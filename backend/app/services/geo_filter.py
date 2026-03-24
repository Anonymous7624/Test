"""Haversine distance for listing vs user search center + radius (stored in settings)."""

from __future__ import annotations

import math
from typing import Any


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1 - a)))
    return r * c


def _city_tokens_from_boundary(boundary_context: dict[str, Any] | None) -> list[str]:
    if not boundary_context or not isinstance(boundary_context, dict):
        return []
    out: list[str] = []
    for feat in boundary_context.get("part_of") or []:
        if not isinstance(feat, dict):
            continue
        for k in ("city", "name", "formatted"):
            v = feat.get(k)
            if v and isinstance(v, str) and v.strip():
                out.append(v.strip().lower())
    return out


def listing_within_user_radius(
    *,
    user_lat: float | None,
    user_lon: float | None,
    radius_km: float,
    boundary_context: dict[str, Any] | None,
    user_location_text: str,
    listing_lat: float | None,
    listing_lon: float | None,
    listing_location_text: str,
) -> bool:
    """
    If listing has coordinates and user has center, use haversine <= radius_km.
    Otherwise fall back to substring match: user city tokens or location_text in listing_location_text.
    """
    loc = (listing_location_text or "").strip().lower()
    if not loc:
        return False

    if (
        user_lat is not None
        and user_lon is not None
        and listing_lat is not None
        and listing_lon is not None
    ):
        d = haversine_km(user_lat, user_lon, listing_lat, listing_lon)
        return d <= float(radius_km) + 0.5  # small buffer for geocode vs listing precision

    # Text fallback: any known city from boundary or first segment of location_text
    tokens = _city_tokens_from_boundary(boundary_context)
    if not tokens and user_location_text.strip():
        first = user_location_text.split(",")[0].strip().lower()
        if first:
            tokens = [first]
    for t in tokens:
        if len(t) >= 3 and t in loc:
            return True
    # Last resort: require overlap on a significant word from user location_text
    user_words = [w.lower() for w in user_location_text.replace(",", " ").split() if len(w) >= 4]
    for w in user_words:
        if w in loc:
            return True
    return False
