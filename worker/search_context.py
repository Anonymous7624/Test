"""
Prepare location hints for scrapers / search using stored Geoapify fields.

center_lat/center_lon/radius_km are available for future radius filtering.
"""

from __future__ import annotations

from typing import Any


def build_search_location_hint(settings: Any) -> str:
    """Prefer a locality name from boundary_context; fall back to location_text."""
    ctx = getattr(settings, "boundary_context", None)
    if isinstance(ctx, dict):
        for feat in ctx.get("part_of") or []:
            if not isinstance(feat, dict):
                continue
            city = feat.get("city")
            if city:
                return str(city)
    text = (getattr(settings, "location_text", None) or "").strip()
    return text or "Unknown"


def search_geo_context(settings: Any) -> dict[str, Any]:
    """Structured snapshot for future worker filters (radius, admin areas)."""
    ctx = getattr(settings, "boundary_context", None) if settings else None
    return {
        "location_text": (getattr(settings, "location_text", None) or "").strip(),
        "center_lat": getattr(settings, "center_lat", None),
        "center_lon": getattr(settings, "center_lon", None),
        "radius_km": getattr(settings, "radius_km", None),
        "geoapify_place_id": getattr(settings, "geoapify_place_id", None),
        "boundary_context": ctx if isinstance(ctx, dict) else None,
    }
