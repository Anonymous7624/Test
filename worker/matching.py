"""Filter collected listings against a user monitoring profile (category, price, radius)."""

from __future__ import annotations

from app.domain import UserSettings as UserSettingsRow
from app.services.categories_service import keywords_for_category
from app.services.geo_filter import listing_within_user_radius

from mock_scraper import RawListing


def raw_matches_profile(raw: RawListing, profile: UserSettingsRow) -> bool:
    if float(raw.price) > float(profile.max_price) + 1e-6:
        return False

    cid = str(profile.category_id or "").strip()
    if raw.category_slug != cid:
        kws = keywords_for_category(cid)
        t = raw.title.lower()
        if not kws or not any(k.lower() in t for k in kws):
            return False

    return listing_within_user_radius(
        user_lat=profile.center_lat,
        user_lon=profile.center_lon,
        radius_km=float(profile.radius_km),
        boundary_context=profile.boundary_context,
        user_location_text=profile.location_text,
        listing_lat=raw.latitude,
        listing_lon=raw.longitude,
        listing_location_text=raw.location,
    )
