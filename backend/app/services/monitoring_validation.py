"""Shared rules for whether monitoring can start (Run)."""

from sqlalchemy.orm import Session

from app.models import UserSettings
from app.services.categories_service import validate_category_id
from app.services.units import km_to_miles, miles_to_km

MIN_RADIUS_MILES = 5.0
MIN_MAX_PRICE_USD = 10.0


def _radius_miles(s: UserSettings) -> float:
    return km_to_miles(float(s.radius_km))


def readiness_errors(db: Session, s: UserSettings) -> list[str]:
    """Return human-readable blocking reasons (empty if ready)."""
    del db
    errors: list[str] = []
    if not (s.geoapify_place_id or "").strip() or s.center_lat is None or s.center_lon is None:
        errors.append("Pick a location from the search suggestions (Geoapify).")
    if not (s.location_text or "").strip():
        errors.append("Location is required.")
    if not validate_category_id(s.category_id) or not (s.category_id or "").strip():
        errors.append("Select a valid category.")
    if float(s.max_price) < MIN_MAX_PRICE_USD:
        errors.append(f"Max price must be at least ${MIN_MAX_PRICE_USD:.0f} USD.")
    if _radius_miles(s) < MIN_RADIUS_MILES:
        errors.append(f"Radius must be at least {MIN_RADIUS_MILES:.0f} miles.")
    if not s.telegram_connected or not (s.telegram_chat_id or "").strip():
        errors.append("Telegram must be connected (verify with the bot).")
    return errors


def is_ready_for_monitoring(db: Session, s: UserSettings) -> bool:
    return len(readiness_errors(db, s)) == 0


def validate_radius_miles(miles: float) -> None:
    if miles < MIN_RADIUS_MILES:
        raise ValueError(f"Radius must be at least {MIN_RADIUS_MILES:.0f} miles.")


def validate_max_price_usd(price: float) -> None:
    if price < MIN_MAX_PRICE_USD:
        raise ValueError(f"Max price must be at least ${MIN_MAX_PRICE_USD:.0f} USD.")


def radius_km_from_miles(miles: float) -> float:
    validate_radius_miles(miles)
    return miles_to_km(miles)
