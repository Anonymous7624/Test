from datetime import datetime

from typing import Any, Literal

from pydantic import BaseModel, Field

from app.domain import Listing, UserSettings as UserSettingsRow
from app.services.marketplace_categories_service import label_for_slug


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


class DeleteAccountRequest(BaseModel):
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
    search_mode: Literal["marketplace_category", "custom_keywords"]
    marketplace_category_label: str | None = None
    marketplace_category_slug: str | None = None
    custom_keywords: list[str] = Field(default_factory=list)
    telegram_bot_username: str
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


def user_settings_out_from_row(row: UserSettingsRow) -> UserSettingsOut:
    from app.config import settings as app_settings
    from app.services.units import km_to_miles

    bot_u = (app_settings.telegram_bot_username or "").strip().lstrip("@") or "Facebookcatching_bot"

    pending = bool(
        row.telegram_verify_code
        and row.telegram_verify_expires_at
        and row.telegram_verify_expires_at > datetime.utcnow()
    )
    has_chat = bool((row.telegram_chat_id or "").strip())
    slug = row.marketplace_category_slug
    label = row.marketplace_category_label
    if slug and not (label or "").strip():
        label = label_for_slug(str(slug)) or str(slug)
    return UserSettingsOut(
        location_text=row.location_text,
        center_lat=row.center_lat,
        center_lon=row.center_lon,
        radius_km=float(row.radius_km),
        radius_miles=round(km_to_miles(float(row.radius_km)), 4),
        search_mode=row.search_mode,  # type: ignore[arg-type]
        marketplace_category_label=label,
        marketplace_category_slug=slug,
        custom_keywords=list(row.custom_keywords or []),
        telegram_bot_username=f"@{bot_u}",
        telegram_chat_id=row.telegram_chat_id,
        telegram_connected=bool(row.telegram_connected) or has_chat,
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
    search_mode: Literal["marketplace_category", "custom_keywords"] | None = None
    marketplace_category_slug: str | None = None
    marketplace_category_label: str | None = None
    custom_keywords: list[str] | None = None
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
    category_id: str
    category_slug: str
    location_text: str
    found_at: datetime
    alert_status: str
    source_link: str
    source: str
    discovery_source: str
    profitable: bool
    origin_type: str
    alert_sent: bool
    ai_result: dict[str, Any] | None = None
    confidence: str | float | None = None
    reasoning: str | None = None
    should_alert: bool | None = None
    description: str | None = None
    matched_keywords: list[str] = Field(default_factory=list)
    scraped_at: datetime | None = None
    alert_sent_at: datetime | None = None
    alert_last_error: str | None = None

    model_config = {"from_attributes": True}

    @classmethod
    def from_listing(cls, row: Listing) -> "ListingOut":
        """Map domain Listing (and legacy field names) to API shape."""
        cid = getattr(row, "category_id", None) or getattr(row, "category_slug", "general")
        lt = getattr(row, "location_text", None) or getattr(row, "location", "")
        mk = getattr(row, "matched_keywords", None)
        keywords = list(mk) if isinstance(mk, list) else []
        return cls(
            id=row.id,
            title=row.title,
            price=row.price,
            estimated_resale=row.estimated_resale,
            estimated_profit=row.estimated_profit,
            category_id=str(cid),
            category_slug=str(cid),
            location_text=str(lt),
            found_at=row.found_at,
            alert_status=row.alert_status,
            source_link=row.source_link,
            source=row.source,
            discovery_source=getattr(row, "discovery_source", row.origin_type),
            profitable=row.profitable,
            origin_type=getattr(row, "origin_type", "live"),
            alert_sent=getattr(row, "alert_sent", False),
            ai_result=getattr(row, "ai_result", None),
            confidence=getattr(row, "confidence", None),
            reasoning=getattr(row, "reasoning", None),
            should_alert=getattr(row, "should_alert", None),
            description=getattr(row, "description", None),
            matched_keywords=keywords,
            scraped_at=getattr(row, "scraped_at", None),
            alert_sent_at=getattr(row, "alert_sent_at", None),
            alert_last_error=getattr(row, "alert_last_error", None),
        )


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


class PipelineCountsOut(BaseModel):
    raw_collected: int = 0
    step1_kept: int = 0
    step2_matched: int = 0
    step3_scored: int = 0
    step4_saved: int = 0
    alerts_sent: int = 0


class WorkerStatus(BaseModel):
    monitoring_enabled: bool
    monitoring_state: str
    message: str
    last_checked_at: datetime | None = None
    listings_found_count: int = 0
    alerts_sent_count: int = 0
    backfill_complete: bool = True
    last_error: str | None = None
    current_step: int = 0
    current_state: str = "idle"
    pipeline_message: str = ""
    last_batch_started_at: datetime | None = None
    last_successful_run_at: datetime | None = None
    pipeline_error: str | None = None
    pipeline_counts: PipelineCountsOut | None = None
    pipeline_counts_scope: str = Field(
        default="last_batch",
        description="Pipeline count fields reflect the last completed batch (Steps 1–4), not lifetime totals.",
    )
    admin_pipeline_snapshot: dict | None = None
    collector_warning: str | None = None


class TelegramVerificationStart(BaseModel):
    code: str
    expires_at: datetime
    instructions: str
    bot_username: str
    start_command: str
