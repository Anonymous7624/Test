"""Domain types for MongoDB-backed persistence (no SQLAlchemy ORM)."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class User:
    id: int
    username: str
    password_hash: str
    role: str
    created_at: datetime


@dataclass
class UserSettings:
    user_id: int
    location_text: str
    center_lat: float | None
    center_lon: float | None
    geoapify_place_id: str | None
    boundary_context: dict | None
    radius_km: float
    category_id: str
    max_price: float
    telegram_chat_id: str | None
    telegram_connected: bool
    telegram_verify_code: str | None
    telegram_verify_expires_at: datetime | None
    monitoring_enabled: bool
    monitoring_state: str
    last_checked_at: datetime | None
    last_error: str | None
    backfill_complete: bool


@dataclass
class Listing:
    id: int
    user_id: int
    source_url: str
    source_id: str | None
    title: str
    price: float
    estimated_resale: float
    estimated_profit: float
    category_id: str
    location_text: str
    found_at: datetime
    alert_status: str
    source_link: str
    source: str
    origin_type: str
    discovery_source: str
    profitable: bool
    alert_sent: bool
    ai_result: dict | None
    confidence: str | None
    reasoning: str | None
    should_alert: bool | None
