from dataclasses import asdict
from datetime import datetime

from pymongo.database import Database

from app.domain import User, UserSettings as UserSettingsState
from app.models import UserRole
from app.mongodb import next_sequence
from app.services.search_settings import migrate_settings_doc, normalize_telegram_alert_mode


def _default_settings_doc(user_id: int) -> dict:
    return {
        "user_id": user_id,
        "location_text": "",
        "center_lat": None,
        "center_lon": None,
        "geoapify_place_id": None,
        "boundary_context": None,
        "radius_km": 25.0,
        "search_mode": "marketplace_category",
        "marketplace_category_label": "Electronics",
        "marketplace_category_slug": "electronics",
        "custom_keywords": [],
        "telegram_chat_id": None,
        "telegram_connected": False,
        "telegram_verify_code": None,
        "telegram_verify_expires_at": None,
        "telegram_alert_mode": "profitable_only",
        "monitoring_enabled": False,
        "monitoring_state": "idle",
        "last_checked_at": None,
        "last_error": None,
        "backfill_complete": True,
        "worker_current_step": 0,
        "worker_current_state": "idle",
        "worker_pipeline_message": "",
        "worker_last_batch_started_at": None,
        "worker_last_success_at": None,
        "worker_count_raw_collected": 0,
        "worker_count_step1_kept": 0,
        "worker_count_step2_matched": 0,
        "worker_count_step3_scored": 0,
        "worker_count_step4_saved": 0,
        "worker_count_alerts_sent": 0,
        "worker_pipeline_error": None,
        "worker_collector_warning": None,
        "worker_last_collector_failure_at": None,
        "worker_last_collector_failure_message": None,
        "worker_configuration_error": None,
    }


def _user_from_doc(doc: dict) -> User:
    return User(
        id=int(doc["id"]),
        username=doc["username"],
        password_hash=doc["password_hash"],
        role=doc["role"],
        created_at=doc["created_at"],
    )


def settings_from_doc(doc: dict) -> UserSettingsState:
    d = migrate_settings_doc(dict(doc))
    return UserSettingsState(
        user_id=int(d["user_id"]),
        location_text=str(d.get("location_text") or ""),
        center_lat=d.get("center_lat"),
        center_lon=d.get("center_lon"),
        geoapify_place_id=d.get("geoapify_place_id"),
        boundary_context=d.get("boundary_context"),
        radius_km=float(d.get("radius_km", 25.0)),
        search_mode=str(d.get("search_mode") or "marketplace_category"),
        marketplace_category_label=d.get("marketplace_category_label"),
        marketplace_category_slug=d.get("marketplace_category_slug"),
        custom_keywords=list(d.get("custom_keywords") or []),
        telegram_chat_id=d.get("telegram_chat_id"),
        telegram_connected=bool(d.get("telegram_connected", False)),
        telegram_verify_code=d.get("telegram_verify_code"),
        telegram_verify_expires_at=d.get("telegram_verify_expires_at"),
        telegram_alert_mode=normalize_telegram_alert_mode(str(d.get("telegram_alert_mode"))),
        monitoring_enabled=bool(d.get("monitoring_enabled", False)),
        monitoring_state=str(d.get("monitoring_state") or "idle"),
        last_checked_at=d.get("last_checked_at"),
        last_error=d.get("last_error"),
        backfill_complete=bool(d.get("backfill_complete", True)),
        worker_current_step=int(d.get("worker_current_step", 0)),
        worker_current_state=str(d.get("worker_current_state") or "idle"),
        worker_pipeline_message=str(d.get("worker_pipeline_message") or ""),
        worker_last_batch_started_at=d.get("worker_last_batch_started_at"),
        worker_last_success_at=d.get("worker_last_success_at"),
        worker_count_raw_collected=int(d.get("worker_count_raw_collected", 0)),
        worker_count_step1_kept=int(d.get("worker_count_step1_kept", 0)),
        worker_count_step2_matched=int(d.get("worker_count_step2_matched", 0)),
        worker_count_step3_scored=int(d.get("worker_count_step3_scored", 0)),
        worker_count_step4_saved=int(d.get("worker_count_step4_saved", 0)),
        worker_count_alerts_sent=int(d.get("worker_count_alerts_sent", 0)),
        worker_pipeline_error=d.get("worker_pipeline_error"),
        worker_collector_warning=d.get("worker_collector_warning"),
        worker_last_collector_failure_at=d.get("worker_last_collector_failure_at"),
        worker_last_collector_failure_message=d.get("worker_last_collector_failure_message"),
        worker_configuration_error=d.get("worker_configuration_error"),
    )


class UserRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def get_by_username(self, username: str) -> User | None:
        doc = self.db["users"].find_one({"username": username})
        return _user_from_doc(doc) if doc else None

    def get_by_id(self, user_id: int) -> User | None:
        doc = self.db["users"].find_one({"id": user_id})
        return _user_from_doc(doc) if doc else None

    def get_settings(self, user_id: int) -> UserSettingsState | None:
        doc = self.db["user_settings"].find_one({"user_id": user_id})
        return settings_from_doc(doc) if doc else None

    def create(self, username: str, password_hash: str, role: str = UserRole.user.value) -> User:
        uid = next_sequence(self.db, "users")
        now = datetime.utcnow()
        user_doc = {
            "id": uid,
            "username": username,
            "password_hash": password_hash,
            "role": role,
            "created_at": now,
        }
        self.db["users"].insert_one(user_doc)
        self.db["user_settings"].insert_one(_default_settings_doc(uid))
        return _user_from_doc(user_doc)

    def list_all(self) -> list[User]:
        docs = self.db["users"].find().sort("id", 1)
        return [_user_from_doc(d) for d in docs]

    def delete(self, user: User) -> None:
        self.db["listings"].delete_many({"user_id": user.id})
        self.db["user_settings"].delete_one({"user_id": user.id})
        self.db["users"].delete_one({"id": user.id})

    def replace_settings(self, state: UserSettingsState) -> None:
        doc = asdict(state)
        self.db["user_settings"].replace_one({"user_id": state.user_id}, doc, upsert=True)

    def update_user_fields(self, user: User, *, role: str | None = None, password_hash: str | None = None) -> User:
        patch: dict = {}
        if role is not None:
            patch["role"] = role
        if password_hash is not None:
            patch["password_hash"] = password_hash
        if patch:
            self.db["users"].update_one({"id": user.id}, {"$set": patch})
        u = self.get_by_id(user.id)
        assert u is not None
        return u
