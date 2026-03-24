from datetime import datetime

from typing import Any, Literal

from pydantic import BaseModel, Field

from app.models import UserSettings


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserPublic(BaseModel):
    id: int
    username: str
    role: str

    model_config = {"from_attributes": True}


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserPublic


class UserSettingsOut(BaseModel):
    location_text: str
    center_lat: float | None = None
    center_lon: float | None = None
    radius_km: float
    radius_miles: float
    category_id: str
    max_price: float
    telegram_chat_id: str | None
    telegram_connected: bool
    monitoring_enabled: bool
    monitoring_state: str
    last_checked_at: datetime | None = None
    last_error: str | None = None
    backfill_complete: bool
    geoapify_place_id: str | None = None
    boundary_context: dict[str, Any] | None = None
    telegram_verify_pending: bool = False


def user_settings_out_from_row(row: UserSettings) -> UserSettingsOut:
    from app.services.units import km_to_miles

    pending = bool(
        row.telegram_verify_code
        and row.telegram_verify_expires_at
        and row.telegram_verify_expires_at > datetime.utcnow()
    )
    return UserSettingsOut(
        location_text=row.location_text,
        center_lat=row.center_lat,
        center_lon=row.center_lon,
        radius_km=float(row.radius_km),
        radius_miles=round(km_to_miles(float(row.radius_km)), 4),
        category_id=row.category_id,
        max_price=float(row.max_price),
        telegram_chat_id=row.telegram_chat_id,
        telegram_connected=bool(row.telegram_connected),
        monitoring_enabled=bool(row.monitoring_enabled),
        monitoring_state=getattr(row, "monitoring_state", None) or "idle",
        last_checked_at=getattr(row, "last_checked_at", None),
        last_error=getattr(row, "last_error", None),
        backfill_complete=bool(getattr(row, "backfill_complete", True)),
        geoapify_place_id=row.geoapify_place_id,
        boundary_context=row.boundary_context,
        telegram_verify_pending=pending,
    )


class UserSettingsUpdate(BaseModel):
    location_text: str | None = None
    center_lat: float | None = None
    center_lon: float | None = None
    radius_km: float | None = Field(default=None, ge=0)
    radius_miles: float | None = Field(default=None, ge=0)
    category_id: str | None = None
    max_price: float | None = Field(default=None, ge=0)
    telegram_chat_id: str | None = None
    geoapify_place_id: str | None = None


class TelegramTestResult(BaseModel):
    ok: bool
    message: str


class ListingOut(BaseModel):
    id: int
    title: str
    price: float
    estimated_resale: float
    estimated_profit: float
    category_slug: str
    location: str
    found_at: datetime
    alert_status: str
    source_link: str
    source: str
    discovery_source: str
    profitable: bool

    model_config = {"from_attributes": True}


class AdminUserCreate(BaseModel):
    username: str = Field(min_length=2, max_length=128)
    password: str = Field(min_length=6)
    role: Literal["admin", "user"]


class AdminUserUpdate(BaseModel):
    role: Literal["admin", "user"] | None = None
    password: str | None = Field(default=None, min_length=6)


class AdminUserOut(BaseModel):
    id: int
    username: str
    role: str
    created_at: datetime

    model_config = {"from_attributes": True}


class WorkerStatus(BaseModel):
    monitoring_enabled: bool
    monitoring_state: str
    message: str
    last_checked_at: datetime | None = None
    listings_found_count: int = 0
    alerts_sent_count: int = 0
    backfill_complete: bool = True
    last_error: str | None = None


class TelegramVerificationStart(BaseModel):
    code: str
    expires_at: datetime
    instructions: str
