from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import get_current_user
from app.models import User, UserSettings
from app.schemas import UserSettingsOut, UserSettingsUpdate
from app.services.categories_service import validate_category_id

router = APIRouter(prefix="/settings", tags=["settings"])


def _get_settings_row(db: Session, user: User) -> UserSettings:
    row = db.query(UserSettings).filter(UserSettings.user_id == user.id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Settings missing")
    return row


@router.get("/me", response_model=UserSettingsOut)
def get_my_settings(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> UserSettingsOut:
    return UserSettingsOut.model_validate(_get_settings_row(db, user))


@router.put("/me", response_model=UserSettingsOut)
def update_my_settings(
    body: UserSettingsUpdate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> UserSettingsOut:
    row = _get_settings_row(db, user)
    data = body.model_dump(exclude_unset=True)
    if "category_id" in data and data["category_id"] is not None:
        if not validate_category_id(data["category_id"]):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid category")
    for k, v in data.items():
        setattr(row, k, v)
    db.add(row)
    db.commit()
    db.refresh(row)
    return UserSettingsOut.model_validate(row)
