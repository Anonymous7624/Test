import logging
import os
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pymongo.database import Database

from app.database import get_db
from app.deps import get_current_user
from app.domain import User
from app.domain import UserSettings as UserSettingsRow
from app.models import UserRole
from app.repositories.listing_repository import ListingRepository
from app.repositories.user_repository import UserRepository
from app.schemas import PipelineCountsOut, WorkerStatus
from app.services.monitoring_validation import readiness_errors

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/worker", tags=["worker"])

# How many seconds since the last heartbeat before we consider the worker dead.
# Must match (or be less than) WORKER_HEARTBEAT_STALE_SECONDS used by the worker.
_HEARTBEAT_STALE_SECONDS = float(os.environ.get("WORKER_HEARTBEAT_STALE_SECONDS", "300"))

# States that indicate an active in-progress batch (not idle, complete, or errored).
_BATCH_ACTIVE_STATES = frozenset({
    "collecting_listings",
    "step2_normalize",
    "step3_match",
    "step4_save_alert",
})


def _read_worker_heartbeat(db: Database) -> tuple[datetime | None, bool]:
    """Return ``(last_ping_at, is_alive)``.

    ``is_alive`` is ``True`` when the worker wrote a heartbeat within the last
    ``WORKER_HEARTBEAT_STALE_SECONDS`` seconds.  Returns ``(None, False)`` if
    the heartbeat document has never been written (worker never started) or if
    a DB read error occurs.
    """
    try:
        doc = db["worker_meta"].find_one({"_id": "heartbeat"})
        if not doc:
            return None, False
        last_ping = doc.get("last_ping_at")
        if not isinstance(last_ping, datetime):
            return None, False
        age = (datetime.utcnow() - last_ping).total_seconds()
        return last_ping, age < _HEARTBEAT_STALE_SECONDS
    except Exception as exc:
        logger.warning("Could not read worker heartbeat: %s", exc)
        return None, False


def _user_settings(db: Database, user: User):
    repo = UserRepository(db)
    s = repo.get_settings(user.id)
    assert s is not None
    return s


def _idle_pipeline_message(monitoring_enabled: bool) -> str:
    return "Monitoring off — no active pipeline." if not monitoring_enabled else "Waiting for worker tick."


def _display_last_error(s: UserSettingsRow) -> str | None:
    """
    ``last_error`` in Mongo may be stale if ``monitoring_state`` was reset to polling after recovery.
    Only treat it as an *active* fatal error while monitoring is in the error state (or when off).
    """
    raw = s.last_error
    if not raw or not str(raw).strip():
        return None
    if not s.monitoring_enabled:
        return str(raw)[:500]
    st = (s.monitoring_state or "").strip().lower()
    if st == "error":
        return str(raw)[:500]
    return None


def _pipeline_counts_last_completed(s: UserSettingsRow) -> PipelineCountsOut:
    return PipelineCountsOut(
        raw_collected=int(getattr(s, "worker_last_completed_raw_collected", 0)),
        step1_kept=int(getattr(s, "worker_last_completed_step1_kept", 0)),
        step2_matched=int(getattr(s, "worker_last_completed_step2_matched", 0)),
        step3_scored=int(getattr(s, "worker_last_completed_step3_scored", 0)),
        step4_saved=int(getattr(s, "worker_last_completed_step4_saved", 0)),
        alerts_sent=int(getattr(s, "worker_last_completed_alerts_sent", 0)),
    )


def _pipeline_counts_current(s: UserSettingsRow) -> PipelineCountsOut:
    return PipelineCountsOut(
        raw_collected=int(getattr(s, "worker_count_raw_collected", 0)),
        step1_kept=int(getattr(s, "worker_count_step1_kept", 0)),
        step2_matched=int(getattr(s, "worker_count_step2_matched", 0)),
        step3_scored=int(getattr(s, "worker_count_step3_scored", 0)),
        step4_saved=int(getattr(s, "worker_count_step4_saved", 0)),
        alerts_sent=int(getattr(s, "worker_count_alerts_sent", 0)),
    )


def _worker_status_payload(db: Database, user: User) -> WorkerStatus:
    s = _user_settings(db, user)
    repo = ListingRepository(db)
    listings_n = repo.count_for_user(user.id)
    alerts_n = repo.count_alerts_sent(user.id)
    state = "idle"
    if s.monitoring_enabled:
        state = (s.monitoring_state or "idle").strip() or "idle"

    heartbeat_at, worker_alive = _read_worker_heartbeat(db)

    current_state_str = str(getattr(s, "worker_current_state", None) or "idle")
    batch_is_active = current_state_str in _BATCH_ACTIVE_STATES

    counts = _pipeline_counts_last_completed(s)
    current_counts = _pipeline_counts_current(s)
    rk = int(getattr(s, "worker_pipeline_step3_rank", 0) or 0)
    tot = int(getattr(s, "worker_pipeline_step3_total", 0) or 0)
    step3_rank_out: int | None = rk if rk > 0 and tot > 0 else None
    step3_total_out: int | None = tot if rk > 0 and tot > 0 else None

    pipeline_msg = (getattr(s, "worker_pipeline_message", None) or "").strip()
    if not pipeline_msg:
        pipeline_msg = _idle_pipeline_message(s.monitoring_enabled)

    admin_snap: dict | None = None
    if user.role == UserRole.admin.value:

        def _iso(dt) -> str | None:
            if dt is None:
                return None
            if hasattr(dt, "isoformat"):
                iso = dt.isoformat()
                return iso if iso.endswith("Z") or "+" in iso else iso + "Z"
            return str(dt)

        admin_snap = {
            "monitoring_state_db": s.monitoring_state,
            "monitoring_enabled_db": bool(s.monitoring_enabled),
            "worker_current_step": getattr(s, "worker_current_step", 0),
            "worker_current_state": getattr(s, "worker_current_state", "idle"),
            "worker_pipeline_message": getattr(s, "worker_pipeline_message", "") or "",
            "worker_last_batch_started_at": _iso(getattr(s, "worker_last_batch_started_at", None)),
            "worker_last_success_at": _iso(getattr(s, "worker_last_success_at", None)),
            "worker_pipeline_error": getattr(s, "worker_pipeline_error", None),
            "worker_collector_warning": getattr(s, "worker_collector_warning", None),
            "last_checked_at": _iso(s.last_checked_at),
            "last_error_db": s.last_error,
            "last_error_active": _display_last_error(s),
            "worker_last_collector_failure_at": _iso(
                getattr(s, "worker_last_collector_failure_at", None)
            ),
            "worker_last_collector_failure_message": getattr(
                s, "worker_last_collector_failure_message", None
            ),
            "worker_configuration_error": getattr(s, "worker_configuration_error", None),
            "backfill_complete": bool(getattr(s, "backfill_complete", True)),
            "counts": counts.model_dump(),
            "counts_scope": "last_completed_batch_steps_1_to_4",
            "current_counts": current_counts.model_dump(),
            "current_counts_scope": "in_progress_batch_steps_1_to_4",
            "pipeline_step3_rank": step3_rank_out,
            "pipeline_step3_total": step3_total_out,
            "stored_listings_count": listings_n,
            "worker_last_heartbeat_at": _iso(heartbeat_at),
            "worker_is_alive": worker_alive,
            "worker_heartbeat_stale_seconds": _HEARTBEAT_STALE_SECONDS,
            "batch_is_active": batch_is_active,
        }

    return WorkerStatus(
        monitoring_enabled=bool(s.monitoring_enabled),
        monitoring_state=state,
        message="Worker process polls the database; run the worker service separately.",
        last_checked_at=s.last_checked_at,
        listings_found_count=listings_n,
        alerts_sent_count=alerts_n,
        backfill_complete=bool(getattr(s, "backfill_complete", True)),
        last_error=_display_last_error(s),
        current_step=int(getattr(s, "worker_current_step", 0)),
        current_state=str(getattr(s, "worker_current_state", None) or "idle"),
        pipeline_message=pipeline_msg,
        last_batch_started_at=getattr(s, "worker_last_batch_started_at", None),
        last_successful_run_at=getattr(s, "worker_last_success_at", None),
        pipeline_error=getattr(s, "worker_pipeline_error", None),
        pipeline_counts=counts,
        pipeline_counts_scope="last_completed_batch",
        current_pipeline_counts=current_counts,
        current_pipeline_scope="in_progress_batch",
        pipeline_step3_rank=step3_rank_out,
        pipeline_step3_total=step3_total_out,
        admin_pipeline_snapshot=admin_snap,
        collector_warning=getattr(s, "worker_collector_warning", None),
        configuration_error=getattr(s, "worker_configuration_error", None),
        worker_last_heartbeat_at=heartbeat_at,
        worker_is_alive=worker_alive,
        batch_is_active=batch_is_active,
    )


def _soft_idle_on_stop(s: UserSettingsRow) -> None:
    """Clear active pipeline indicators when user stops monitoring; keep last success / counts for history."""
    s.worker_current_step = 0
    s.worker_current_state = "idle"
    s.worker_pipeline_message = ""
    s.worker_pipeline_step3_rank = 0
    s.worker_pipeline_step3_total = 0
    s.worker_collector_warning = None
    s.worker_pipeline_error = None
    s.worker_configuration_error = None


@router.post("/run", response_model=WorkerStatus)
def run_monitoring(
    user: User = Depends(get_current_user),
    db: Database = Depends(get_db),
) -> WorkerStatus:
    repo = UserRepository(db)
    s = _user_settings(db, user)
    errors = readiness_errors(s)
    if errors:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"errors": errors},
        )
    logger.info(
        "Start monitoring requested: user_id=%s — writing monitoring_state=starting to DB",
        user.id,
    )
    s.monitoring_enabled = True
    s.monitoring_state = "starting"
    s.backfill_complete = False
    s.last_error = None
    s.worker_pipeline_error = None
    s.worker_collector_warning = None
    s.worker_last_collector_failure_at = None
    s.worker_last_collector_failure_message = None
    s.worker_configuration_error = None
    s.worker_current_step = 0
    s.worker_current_state = "starting"
    s.worker_pipeline_message = "Monitoring requested — waiting for worker to pick up."
    repo.replace_settings(s)
    logger.info(
        "DB updated to starting for user_id=%s — worker must be running separately to pick this up",
        user.id,
    )
    payload = _worker_status_payload(db, user)
    if not payload.worker_is_alive:
        logger.warning(
            "Start monitoring: user_id=%s — no recent worker heartbeat "
            "(last_heartbeat=%s stale_threshold=%ss). "
            "Worker may not be running. Start it with: python worker/main.py",
            user.id,
            payload.worker_last_heartbeat_at,
            _HEARTBEAT_STALE_SECONDS,
        )
    return payload


@router.post("/stop", response_model=WorkerStatus)
def stop_monitoring(
    user: User = Depends(get_current_user),
    db: Database = Depends(get_db),
) -> WorkerStatus:
    repo = UserRepository(db)
    s = _user_settings(db, user)
    s.monitoring_enabled = False
    s.monitoring_state = "idle"
    _soft_idle_on_stop(s)
    repo.replace_settings(s)
    return _worker_status_payload(db, user)


@router.get("/status", response_model=WorkerStatus)
def worker_status(
    user: User = Depends(get_current_user),
    db: Database = Depends(get_db),
) -> WorkerStatus:
    return _worker_status_payload(db, user)
