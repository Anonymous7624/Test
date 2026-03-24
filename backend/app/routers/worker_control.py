from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import get_current_user
from app.models import User, UserSettings
from app.repositories.listing_repository import ListingRepository
from app.schemas import WorkerStatus
from app.services.monitoring_validation import readiness_errors

router = APIRouter(prefix="/worker", tags=["worker"])


def _user_settings(db: Session, user: User) -> UserSettings:
    s = db.query(UserSettings).filter(UserSettings.user_id == user.id).first()
    assert s is not None
    return s


def _worker_status_payload(db: Session, user: User) -> WorkerStatus:
    s = _user_settings(db, user)
    repo = ListingRepository(db)
    listings_n = repo.count_for_user(user.id)
    alerts_n = repo.count_alerts_sent(user.id)
    state = "idle"
    if s.monitoring_enabled:
        state = (s.monitoring_state or "idle").strip() or "idle"
    return WorkerStatus(
        monitoring_enabled=bool(s.monitoring_enabled),
        monitoring_state=state,
        message="Worker process polls the database; run the worker service separately.",
        last_checked_at=s.last_checked_at,
        listings_found_count=listings_n,
        alerts_sent_count=alerts_n,
        backfill_complete=bool(getattr(s, "backfill_complete", True)),
        last_error=s.last_error,
    )


@router.post("/run", response_model=WorkerStatus)
def run_monitoring(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WorkerStatus:
    s = _user_settings(db, user)
    errors = readiness_errors(db, s)
    if errors:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"errors": errors},
        )
    s.monitoring_enabled = True
    s.monitoring_state = "starting"
    s.backfill_complete = False
    s.last_error = None
    db.add(s)
    db.commit()
    db.refresh(s)
    return _worker_status_payload(db, user)


@router.post("/stop", response_model=WorkerStatus)
def stop_monitoring(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WorkerStatus:
    s = _user_settings(db, user)
    s.monitoring_enabled = False
    s.monitoring_state = "idle"
    db.add(s)
    db.commit()
    db.refresh(s)
    return _worker_status_payload(db, user)


@router.get("/status", response_model=WorkerStatus)
def worker_status(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WorkerStatus:
    return _worker_status_payload(db, user)
