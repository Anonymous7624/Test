"""Domain types for MongoDB-backed persistence (no SQLAlchemy ORM)."""

from dataclasses import dataclass, field
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
    # Worker pipeline snapshot (updated by worker process; read by API for live status)
    worker_current_step: int = 0
    worker_current_state: str = "idle"
    worker_pipeline_message: str = ""
    worker_last_batch_started_at: datetime | None = None
    worker_last_success_at: datetime | None = None
    worker_count_raw_collected: int = 0
    worker_count_step1_kept: int = 0
    worker_count_step2_matched: int = 0
    worker_count_step3_scored: int = 0
    worker_count_step4_saved: int = 0
    worker_count_alerts_sent: int = 0
    worker_pipeline_error: str | None = None
    # Set when Step 1 completes but Marketplace advanced UI filters were skipped (e.g. Filters drawer).
    worker_collector_warning: str | None = None


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
    description: str | None = None
    matched_keywords: list[str] = field(default_factory=list)
    scraped_at: datetime | None = None
    alert_sent_at: datetime | None = None
    alert_last_error: str | None = None
