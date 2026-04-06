"""Step 1: raw scrape → CandidateListing + light prefilter (before strict matching)."""

from __future__ import annotations

import math
import re
from datetime import datetime

from app.domain import UserSettings as UserSettingsRow

from candidate_models import CandidateListing
from mock_scraper import RawListing
from search_context import CollectionInputs

# ── Junk / sanity patterns ────────────────────────────────────────────────────
# These indicate the scraper captured notification chrome or page noise instead
# of real listing content.  Listings matching these patterns are rejected before
# they reach the DB.

_JUNK_TITLE_WORDS_RE = re.compile(
    r"\b(?:Unread|Mark\s+as\s+read|Today[''`\u2019]?s\s+picks|Sponsored)\b",
    re.I,
)

_JUNK_DESC_SECTIONS_RE = re.compile(
    r"(?:Today[''`\u2019]?s\s+picks|Related\s+listings?|People\s+also\s+(?:viewed|liked)"
    r"|You\s+may\s+also\s+like|More\s+from\s+(?:this\s+seller|Marketplace)"
    r"|Similar\s+(?:items?|listings?)|Sponsored|Recommended\s+for\s+you)",
    re.I,
)


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
    title_effective = (
        (getattr(raw, "title_full", None) or "").strip() or (raw.title or "").strip()
    )
    listing_scrape = {
        "brand": getattr(raw, "brand", None),
        "condition": getattr(raw, "condition", None),
        "listing_location_detail": getattr(raw, "listing_location_detail", None),
        "image_urls": list(getattr(raw, "image_urls", None) or [])[:12],
        "detail_enriched": bool(getattr(raw, "detail_enriched", False)),
    }
    meta = {
        "collector_category_slug": raw.category_slug,
        "profile_search_mode": str(profile.search_mode or "").strip(),
        "profile_marketplace_category_slug": profile.marketplace_category_slug,
        "listing_location_parsed": getattr(raw, "listing_location_parsed", None),
        "listing_scrape": listing_scrape,
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
        title=title_effective,
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

    Also rejects listings whose title or description contains obvious scraper-noise
    patterns (notification chrome, recommendation sections, etc.) that indicate the
    extractor captured page junk rather than real listing content.

    Step 2 applies search-mode relevance and a pre-AI strength gate (no max-price filter).
    """
    if not candidate.source_url.strip():
        return False, "missing_source_url"

    title = candidate.title.strip()
    if not title:
        return False, "missing_title"

    # Reject if the title contains obvious page-noise markers.
    if _JUNK_TITLE_WORDS_RE.search(title):
        return False, f"junk_title:{title[:80]!r}"

    loc = candidate.location_text.strip()
    if not loc:
        return False, "missing_location"

    p = candidate.price
    if math.isnan(p) or math.isinf(p):
        return False, "invalid_price"
    if p <= 0:
        return False, "non_positive_price"

    # Warn (but do not reject) if the description contains recommendation section text.
    # The collector should have already stripped it; this is a last-resort check.
    desc = candidate.description or ""
    if desc and _JUNK_DESC_SECTIONS_RE.search(desc):
        # Strip everything from the junk section onwards rather than discarding.
        m = _JUNK_DESC_SECTIONS_RE.search(desc)
        if m:
            candidate.description = desc[: m.start()].strip() or None  # type: ignore[assignment]

    return True, None
