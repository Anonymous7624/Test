"""Shared rules for whether monitoring can start (Run)."""

from __future__ import annotations

from typing import TypedDict

from app.domain import UserSettings as UserSettingsRow
from app.services.categories_service import validate_category_id
from app.services.units import km_to_miles, miles_to_km

MIN_RADIUS_MILES = 5.0
MIN_MAX_PRICE_USD = 10.0


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


def readiness_checks(s: UserSettingsRow) -> list[ReadinessCheck]:
    """Structured checklist for UI (✅/❌)."""
    loc_ok = _location_complete(s)
    cat_ok = bool(validate_category_id(s.category_id) and (s.category_id or "").strip())
    rad_ok = _radius_miles(s) >= MIN_RADIUS_MILES
    price_ok = float(s.max_price) >= MIN_MAX_PRICE_USD
    tg_ok = telegram_is_configured(s)
    return [
        {
            "id": "location",
            "label": "Location selected (valid suggestion)",
            "ok": loc_ok,
        },
        {
            "id": "category",
            "label": "Category selected",
            "ok": cat_ok,
        },
        {
            "id": "radius",
            "label": f"Radius ≥ {MIN_RADIUS_MILES:.0f} miles",
            "ok": rad_ok,
        },
        {
            "id": "max_price",
            "label": f"Max price ≥ ${MIN_MAX_PRICE_USD:.0f}",
            "ok": price_ok,
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
    if not validate_category_id(s.category_id) or not (s.category_id or "").strip():
        errors.append("Select a valid category.")
    if float(s.max_price) < MIN_MAX_PRICE_USD:
        errors.append(f"Max price must be at least ${MIN_MAX_PRICE_USD:.0f} USD.")
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


def validate_max_price_usd(price: float) -> None:
    if price < MIN_MAX_PRICE_USD:
        raise ValueError(f"Max price must be at least ${MIN_MAX_PRICE_USD:.0f} USD.")


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
