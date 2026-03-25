"""Shared rules for whether monitoring can start (Run)."""

from __future__ import annotations

from typing import TypedDict

from app.domain import UserSettings as UserSettingsRow
from app.services.marketplace_categories_service import validate_marketplace_slug
from app.services.search_settings import normalize_custom_keywords
from app.services.units import km_to_miles, miles_to_km

MIN_RADIUS_MILES = 5.0


class ReadinessCheck(TypedDict):
    id: str
    label: str
    ok: bool


def _radius_miles(s: UserSettingsRow) -> float:
    return km_to_miles(float(s.radius_km))


def _location_complete(s: UserSettingsRow) -> bool:
    return bool(
        (s.geoapify_place_id or "").strip()
        and s.center_lat is not None
        and s.center_lon is not None
        and (s.location_text or "").strip(),
    )


def telegram_is_configured(s: UserSettingsRow) -> bool:
    """True if the user linked Telegram via verification or a saved chat id (manual fallback)."""
    return bool(s.telegram_connected) or bool((s.telegram_chat_id or "").strip())


def _search_config_ok(s: UserSettingsRow) -> bool:
    sm = (s.search_mode or "marketplace_category").strip()
    if sm == "marketplace_category":
        return bool(validate_marketplace_slug(str(s.marketplace_category_slug or "")))
    if sm == "custom_keywords":
        return len(normalize_custom_keywords(s.custom_keywords)) >= 1
    return False


def readiness_checks(s: UserSettingsRow) -> list[ReadinessCheck]:
    """Structured checklist for UI (✅/❌)."""
    loc_ok = _location_complete(s)
    search_ok = _search_config_ok(s)
    rad_ok = _radius_miles(s) >= MIN_RADIUS_MILES
    tg_ok = telegram_is_configured(s)
    return [
        {
            "id": "location",
            "label": "Location selected (valid suggestion)",
            "ok": loc_ok,
        },
        {
            "id": "search",
            "label": "Search mode configured (category or keywords)",
            "ok": search_ok,
        },
        {
            "id": "radius",
            "label": f"Radius ≥ {MIN_RADIUS_MILES:.0f} miles",
            "ok": rad_ok,
        },
        {
            "id": "telegram",
            "label": "Telegram connected or chat ID saved",
            "ok": tg_ok,
        },
    ]


def readiness_errors(s: UserSettingsRow) -> list[str]:
    """Return human-readable blocking reasons (empty if ready)."""
    errors: list[str] = []
    if not (s.location_text or "").strip():
        errors.append("Location is required.")
    elif not _location_complete(s):
        errors.append("Pick a location from the search suggestions (Geoapify).")
    sm = (s.search_mode or "marketplace_category").strip()
    if sm == "marketplace_category":
        if not validate_marketplace_slug(str(s.marketplace_category_slug or "")):
            errors.append("Select a Marketplace category.")
    elif sm == "custom_keywords":
        if len(normalize_custom_keywords(s.custom_keywords)) < 1:
            errors.append("Add at least one custom keyword (up to 15).")
    else:
        errors.append("Invalid search mode.")
    if _radius_miles(s) < MIN_RADIUS_MILES:
        errors.append(f"Radius must be at least {MIN_RADIUS_MILES:.0f} miles.")
    if not telegram_is_configured(s):
        errors.append("Connect Telegram (message the bot with /start CODE) or save a chat ID below.")
    return errors


def is_ready_for_monitoring(s: UserSettingsRow) -> bool:
    return len(readiness_errors(s)) == 0


def validate_radius_miles(miles: float) -> None:
    if miles < MIN_RADIUS_MILES:
        raise ValueError(f"Radius must be at least {MIN_RADIUS_MILES:.0f} miles.")


def radius_km_from_miles(miles: float) -> float:
    validate_radius_miles(miles)
    return miles_to_km(miles)


_ACTIVE_MONITORING_STATES = frozenset({"starting", "backfill", "polling"})


def settings_update_locked(s: UserSettingsRow) -> bool:
    """True while monitoring is enabled and the worker is in an active phase (not idle/error)."""
    if not s.monitoring_enabled:
        return False
    st = (s.monitoring_state or "idle").strip().lower()
    return st in _ACTIVE_MONITORING_STATES
