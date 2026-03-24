import secrets
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.deps import get_current_user
from app.models import User, UserSettings
from app.schemas import (
    TelegramTestResult,
    TelegramVerificationStart,
    UserSettingsOut,
    UserSettingsUpdate,
    user_settings_out_from_row,
)
from app.services.categories_service import validate_category_id
from app.services.location_service import LocationResolutionError, resolve_location_for_save
from app.services.monitoring_validation import (
    readiness_checks,
    readiness_errors,
    validate_max_price_usd,
    validate_radius_miles,
)
from app.services.telegram_service import send_test_message
from app.services.units import km_to_miles, miles_to_km

router = APIRouter(prefix="/settings", tags=["settings"])


def _get_settings_row(db: Session, user: User) -> UserSettings:
    row = db.query(UserSettings).filter(UserSettings.user_id == user.id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Settings missing")
    return row


def _location_subset_changed(
    row: UserSettings,
    data: dict,
) -> bool:
    """True if client sent a change to any location-related field."""
    keys = ("location_text", "center_lat", "center_lon", "geoapify_place_id")
    for k in keys:
        if k not in data:
            continue
        cur = getattr(row, k)
        new = data[k]
        if cur != new:
            return True
    return False


@router.get("/monitoring-readiness")
def monitoring_readiness(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    row = _get_settings_row(db, user)
    errs = readiness_errors(db, row)
    checks = readiness_checks(db, row)
    return {"ready": len(errs) == 0, "errors": errs, "checks": checks}


@router.get("/me", response_model=UserSettingsOut)
def get_my_settings(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> UserSettingsOut:
    return user_settings_out_from_row(_get_settings_row(db, user))


@router.put("/me", response_model=UserSettingsOut)
def update_my_settings(
    body: UserSettingsUpdate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> UserSettingsOut:
    row = _get_settings_row(db, user)
    data = body.model_dump(exclude_unset=True)
    if "radius_miles" in data and data["radius_miles"] is not None:
        validate_radius_miles(float(data["radius_miles"]))
        data["radius_km"] = miles_to_km(float(data["radius_miles"]))
        del data["radius_miles"]
    elif "radius_km" in data and data["radius_km"] is not None:
        validate_radius_miles(km_to_miles(float(data["radius_km"])))
    if "max_price" in data and data["max_price"] is not None:
        validate_max_price_usd(float(data["max_price"]))
    if "category_id" in data and data["category_id"] is not None:
        if not validate_category_id(data["category_id"]):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid category")

    # Resolve Geoapify-backed location when the user edits location fields
    if _location_subset_changed(row, data) or (
        "location_text" in data and (data.get("location_text") or "").strip() and not row.location_text
    ):
        lt = (data.get("location_text") if "location_text" in data else None)
        if lt is None:
            lt = row.location_text
        lt = (lt or "").strip()

        if not lt:
            data["center_lat"] = None
            data["center_lon"] = None
            data["geoapify_place_id"] = None
            data["boundary_context"] = None
            data["location_text"] = ""
        else:
            try:
                resolved = resolve_location_for_save(
                    location_text=lt,
                    center_lat=data.get("center_lat", row.center_lat),
                    center_lon=data.get("center_lon", row.center_lon),
                    geoapify_place_id=data.get("geoapify_place_id", row.geoapify_place_id),
                    fetch_boundaries=True,
                )
            except LocationResolutionError as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=str(exc),
                ) from exc
            except RuntimeError as exc:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=str(exc),
                ) from exc
            data["location_text"] = resolved["location_text"]
            data["center_lat"] = resolved["center_lat"]
            data["center_lon"] = resolved["center_lon"]
            data["geoapify_place_id"] = resolved["geoapify_place_id"]
            data["boundary_context"] = resolved["boundary_context"]

    for k, v in data.items():
        setattr(row, k, v)
    if "telegram_chat_id" in data:
        row.telegram_connected = bool((data.get("telegram_chat_id") or "").strip())
    db.add(row)
    db.commit()
    db.refresh(row)
    return user_settings_out_from_row(row)


@router.post("/telegram/verification/start", response_model=TelegramVerificationStart)
def start_telegram_verification(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> TelegramVerificationStart:
    if not (settings.telegram_bot_token or "").strip():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="TELEGRAM_BOT_TOKEN is not configured on the server.",
        )
    row = _get_settings_row(db, user)
    code = secrets.token_hex(4).upper()[:10]
    exp = datetime.utcnow() + timedelta(minutes=15)
    row.telegram_verify_code = code
    row.telegram_verify_expires_at = exp
    db.add(row)
    db.commit()
    db.refresh(row)
    bot_u = (settings.telegram_bot_username or "").strip().lstrip("@") or "Facebookcatching_bot"
    bot_at = f"@{bot_u}"
    start_cmd = f"/start {code}"
    instructions = (
        f"1) Open Telegram. 2) Search for {bot_at}. 3) Open the chat and send this exact line: {start_cmd}"
    )
    return TelegramVerificationStart(
        code=code,
        expires_at=exp,
        instructions=instructions,
        bot_username=bot_at,
        start_command=start_cmd,
    )


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
            detail="Connect Telegram via verification first.",
        )
    if not (settings.telegram_bot_token or "").strip():
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
