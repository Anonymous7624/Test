"""Step 1: raw scrape → CandidateListing + light prefilter (before strict matching)."""

from __future__ import annotations

import math
from datetime import datetime

from app.domain import UserSettings as UserSettingsRow

from candidate_models import CandidateListing
from mock_scraper import RawListing
from search_context import CollectionInputs


def _stable_source_id(raw: RawListing) -> str | None:
    if raw.source_id and str(raw.source_id).strip():
        return str(raw.source_id).strip()
    ext = raw.source_link.rsplit("/", maxsplit=1)[-1]
    return f"{raw.source}:{ext}" if ext else None


def normalize_raw_to_candidate(
    raw: RawListing,
    profile: UserSettingsRow,
    collection_inputs: CollectionInputs,
    *,
    origin_type: str,
) -> CandidateListing:
    """Map scraper output to the internal candidate shape for this user/job."""
    source_url = raw.source_link.strip()
    scraped = datetime.utcnow()
    meta = {
        "collector_category_slug": raw.category_slug,
        "profile_category_id": str(profile.category_id or "").strip(),
        "prefilter_max_price": float(profile.max_price),
        "collection": {
            "category_id": collection_inputs.category_id,
            "keywords": list(collection_inputs.keywords),
            "search_plan": collection_inputs.search_plan.to_log_dict(),
            "primary_search_location": collection_inputs.primary_search_location,
            "search_area_labels": list(collection_inputs.search_area_labels),
            "radius_hint": collection_inputs.radius_hint,
            "max_price": collection_inputs.max_price,
        },
    }
    return CandidateListing(
        user_id=profile.user_id,
        source_url=source_url,
        source_id=_stable_source_id(raw),
        title=raw.title.strip(),
        price=float(raw.price),
        description=(raw.description or "").strip(),
        location_text=(raw.location or "").strip(),
        image_url=raw.image_url,
        scraped_at=scraped,
        origin_type=origin_type,
        category_slug=(raw.category_slug or "general").strip(),
        latitude=raw.latitude,
        longitude=raw.longitude,
        source_link=source_url,
        source=raw.source,
        raw_metadata=meta,
    )


def prefilter_candidate(
    candidate: CandidateListing,
    *,
    max_price: float,
) -> tuple[bool, str | None]:
    """
    Light drops before Step 2: invalid rows, missing critical fields, obvious price violations.
    Returns (keep, reason_code_or_none).
    """
    if not candidate.source_url.strip():
        return False, "missing_source_url"
    if not candidate.title.strip():
        return False, "missing_title"
    loc = candidate.location_text.strip()
    if not loc:
        return False, "missing_location"

    p = candidate.price
    if math.isnan(p) or math.isinf(p):
        return False, "invalid_price"
    if p <= 0:
        return False, "non_positive_price"
    if p > float(max_price) + 1e-6:
        return False, "over_max_price_prefilter"

    return True, None
