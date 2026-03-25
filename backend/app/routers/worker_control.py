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

router = APIRouter(prefix="/worker", tags=["worker"])


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


def _worker_status_payload(db: Database, user: User) -> WorkerStatus:
    s = _user_settings(db, user)
    repo = ListingRepository(db)
    listings_n = repo.count_for_user(user.id)
    alerts_n = repo.count_alerts_sent(user.id)
    state = "idle"
    if s.monitoring_enabled:
        state = (s.monitoring_state or "idle").strip() or "idle"

    counts = PipelineCountsOut(
        raw_collected=int(getattr(s, "worker_count_raw_collected", 0)),
        step1_kept=int(getattr(s, "worker_count_step1_kept", 0)),
        step2_matched=int(getattr(s, "worker_count_step2_matched", 0)),
        step3_scored=int(getattr(s, "worker_count_step3_scored", 0)),
        step4_saved=int(getattr(s, "worker_count_step4_saved", 0)),
        alerts_sent=int(getattr(s, "worker_count_alerts_sent", 0)),
    )

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
            "backfill_complete": bool(getattr(s, "backfill_complete", True)),
            "counts": counts.model_dump(),
            "counts_scope": "last_completed_batch_steps_1_to_4",
            "stored_listings_count": listings_n,
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
        pipeline_counts_scope="last_batch",
        admin_pipeline_snapshot=admin_snap,
        collector_warning=getattr(s, "worker_collector_warning", None),
    )


def _soft_idle_on_stop(s: UserSettingsRow) -> None:
    """Clear active pipeline indicators when user stops monitoring; keep last success / counts for history."""
    s.worker_current_step = 0
    s.worker_current_state = "idle"
    s.worker_pipeline_message = ""
    s.worker_collector_warning = None
    s.worker_pipeline_error = None


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
    s.monitoring_enabled = True
    s.monitoring_state = "starting"
    s.backfill_complete = False
    s.last_error = None
    s.worker_pipeline_error = None
    s.worker_collector_warning = None
    s.worker_last_collector_failure_at = None
    s.worker_last_collector_failure_message = None
    s.worker_current_step = 0
    s.worker_current_state = "starting"
    s.worker_pipeline_message = "Monitoring requested — waiting for worker to pick up."
    repo.replace_settings(s)
    return _worker_status_payload(db, user)


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
