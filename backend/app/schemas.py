from datetime import datetime

from typing import Literal

from pydantic import BaseModel, Field


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
    location: str
    radius_km: float
    category_id: str
    max_price: float
    telegram_chat_id: str | None
    telegram_connected: bool
    monitoring_enabled: bool

    model_config = {"from_attributes": True}


class UserSettingsUpdate(BaseModel):
    location: str | None = None
    radius_km: float | None = Field(default=None, ge=0)
    category_id: str | None = None
    max_price: float | None = Field(default=None, ge=0)
    telegram_chat_id: str | None = None


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
    message: str
