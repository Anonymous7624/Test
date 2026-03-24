import os

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import get_current_user
from app.models import User, UserSettings
from app.schemas import TelegramTestResult, UserSettingsOut, UserSettingsUpdate
from app.services.categories_service import validate_category_id
from app.services.telegram_service import send_test_message

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
    if "telegram_chat_id" in data:
        row.telegram_connected = bool((data.get("telegram_chat_id") or "").strip())
    db.add(row)
    db.commit()
    db.refresh(row)
    return UserSettingsOut.model_validate(row)


@router.post("/telegram/test", response_model=TelegramTestResult)
def send_telegram_test(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> TelegramTestResult:
    row = _get_settings_row(db, user)
    cid = (row.telegram_chat_id or "").strip()
    if not cid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Save a Telegram chat id first.",
        )
    if not os.getenv("TELEGRAM_BOT_TOKEN"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="TELEGRAM_BOT_TOKEN is not configured on the server.",
        )
    ok, err = send_test_message(cid)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=err or "Telegram request failed.",
        )
    return TelegramTestResult(ok=True, message="Test message sent.")
