"""
Prepare location hints for scrapers / search using stored Geoapify fields.

center_lat/center_lon/radius_km are available for future radius filtering.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.domain import UserSettings as UserSettingsRow
from app.services.categories_service import keywords_for_category


@dataclass
class CollectionInputs:
    """Per active profile: everything Step 1 needs to run targeted collection."""

    user_id: int
    category_id: str
    keywords: list[str]
    location_text: str
    primary_search_location: str
    search_area_labels: list[str]
    max_price: float
    center_lat: float | None
    center_lon: float | None
    radius_km: float
    radius_hint: str


def _nearby_and_related_areas(profile: UserSettingsRow) -> list[str]:
    """Towns / admin areas from boundary_context part_of, plus primary locality label."""
    seen: set[str] = set()
    out: list[str] = []
    ctx = profile.boundary_context
    if isinstance(ctx, dict):
        for feat in ctx.get("part_of") or []:
            if not isinstance(feat, dict):
                continue
            for k in ("city", "name", "formatted"):
                v = feat.get(k)
                if v and str(v).strip():
                    s = str(v).strip()
                    low = s.lower()
                    if low not in seen:
                        seen.add(low)
                        out.append(s)
    lt = (profile.location_text or "").strip()
    if lt:
        first = lt.split(",")[0].strip()
        if first and first.lower() not in seen:
            out.append(first)
    return out


def build_collection_inputs(profile: UserSettingsRow) -> CollectionInputs:
    """Build targeted collection/search inputs for one monitoring profile."""
    cid = str(profile.category_id or "").strip() or "general"
    kws = keywords_for_category(cid)
    primary = build_search_location_hint(profile)
    areas = _nearby_and_related_areas(profile)
    r_km = float(profile.radius_km)
    hint = f"within {r_km:g} km of {primary}" if primary else f"within {r_km:g} km"
    return CollectionInputs(
        user_id=profile.user_id,
        category_id=cid,
        keywords=kws,
        location_text=(profile.location_text or "").strip(),
        primary_search_location=primary,
        search_area_labels=areas,
        max_price=float(profile.max_price),
        center_lat=profile.center_lat,
        center_lon=profile.center_lon,
        radius_km=r_km,
        radius_hint=hint,
    )


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
