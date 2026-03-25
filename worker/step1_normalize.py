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
        "profile_search_mode": str(profile.search_mode or "").strip(),
        "profile_marketplace_category_slug": profile.marketplace_category_slug,
        "listing_location_parsed": getattr(raw, "listing_location_parsed", None),
        "collection": {
            "listing_category_ref": collection_inputs.listing_category_ref,
            "keywords": list(collection_inputs.keywords),
            "search_plan": collection_inputs.search_plan.to_log_dict(),
            "primary_search_location": collection_inputs.primary_search_location,
            "search_area_labels": list(collection_inputs.search_area_labels),
            "radius_hint": collection_inputs.radius_hint,
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
        category_slug=(raw.category_slug or "marketplace").strip(),
        latitude=raw.latitude,
        longitude=raw.longitude,
        source_link=source_url,
        source=raw.source,
        raw_metadata=meta,
    )


def prefilter_candidate(candidate: CandidateListing) -> tuple[bool, str | None]:
    """
    Light drops before Step 2: invalid rows, missing critical fields, non-numeric/non-positive price.

    Step 2 applies search-mode relevance and a pre-AI strength gate (no max-price filter).
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

    return True, None
