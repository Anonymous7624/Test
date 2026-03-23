from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import get_current_user
from app.models import User, UserSettings
from app.schemas import WorkerStatus

router = APIRouter(prefix="/worker", tags=["worker"])


def _settings(db: Session, user: User) -> UserSettings:
    row = db.query(UserSettings).filter(UserSettings.user_id == user.id).first()
    assert row is not None
    return row


@router.post("/run", response_model=WorkerStatus)
def run_monitoring(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WorkerStatus:
    s = _settings(db, user)
    s.monitoring_enabled = True
    db.add(s)
    db.commit()
    return WorkerStatus(monitoring_enabled=True, message="Monitoring enabled for your account.")


@router.post("/stop", response_model=WorkerStatus)
def stop_monitoring(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WorkerStatus:
    s = _settings(db, user)
    s.monitoring_enabled = False
    db.add(s)
    db.commit()
    return WorkerStatus(monitoring_enabled=False, message="Monitoring stopped for your account.")


@router.get("/status", response_model=WorkerStatus)
def worker_status(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> WorkerStatus:
    s = _settings(db, user)
    return WorkerStatus(
        monitoring_enabled=s.monitoring_enabled,
        message="Worker process polls DB; ensure worker service is running separately.",
    )
