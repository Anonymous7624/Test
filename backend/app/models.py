import enum
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class UserRole(str, enum.Enum):
    admin = "admin"
    user = "user"


class AlertStatus(str, enum.Enum):
    none = "none"
    pending = "pending"
    sent = "sent"
    skipped = "skipped"


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    username: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(32), default=UserRole.user.value)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    settings: Mapped["UserSettings"] = relationship(
        "UserSettings", back_populates="user", uselist=False, cascade="all, delete-orphan"
    )


class UserSettings(Base):
    __tablename__ = "user_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), unique=True)
    location_text: Mapped[str] = mapped_column(String(512), default="")
    center_lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    center_lon: Mapped[float | None] = mapped_column(Float, nullable=True)
    geoapify_place_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    boundary_context: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    radius_km: Mapped[float] = mapped_column(Float, default=25.0)
    category_id: Mapped[str] = mapped_column(String(64), default="general")
    max_price: Mapped[float] = mapped_column(Float, default=10_000.0)
    telegram_chat_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    telegram_connected: Mapped[bool] = mapped_column(Boolean, default=False)
    telegram_verify_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    telegram_verify_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    monitoring_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    monitoring_state: Mapped[str] = mapped_column(String(32), default="idle")
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(512), nullable=True)
    backfill_complete: Mapped[bool] = mapped_column(Boolean, default=True)

    user: Mapped["User"] = relationship("User", back_populates="settings")


class Listing(Base):
    __tablename__ = "listings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    external_id: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(512))
    price: Mapped[float] = mapped_column(Float)
    estimated_resale: Mapped[float] = mapped_column(Float)
    estimated_profit: Mapped[float] = mapped_column(Float)
    category_slug: Mapped[str] = mapped_column(String(64), index=True)
    location: Mapped[str] = mapped_column(String(256))
    found_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    alert_status: Mapped[str] = mapped_column(String(32), default=AlertStatus.none.value)
    source_link: Mapped[str] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(64), default="mock")
    discovery_source: Mapped[str] = mapped_column(String(32), default="live")
    profitable: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
