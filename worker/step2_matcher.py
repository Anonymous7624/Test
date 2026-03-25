"""Step 2: strict matching, dedupe against MongoDB, keyword + geo + price rules."""

from __future__ import annotations

from pymongo.database import Database

from app.domain import UserSettings as UserSettingsRow
from app.repositories.listing_repository import ListingRepository
from app.services.categories_service import keywords_for_category
from app.services.geo_filter import listing_within_user_radius

from candidate_models import CandidateListing, MatchResult
from mock_scraper import RawListing


def _keywords_matched_in_title(title: str, keywords: list[str]) -> list[str]:
    t = title.lower()
    out: list[str] = []
    for k in keywords:
        if not k or not str(k).strip():
            continue
        ks = str(k).strip().lower()
        if ks and ks in t:
            out.append(str(k).strip())
    return out


def category_and_keyword_ok(
    *,
    category_slug: str,
    profile_category_id: str,
    title: str,
) -> tuple[bool, list[str], list[str]]:
    """
    Enforce category / keyword relevance (same semantics as legacy matcher).
    If listing category matches profile, keywords are optional; else title must hit a category keyword.
    """
    cid = str(profile_category_id or "").strip() or "general"
    kws = keywords_for_category(cid)
    matched = _keywords_matched_in_title(title, kws)

    if (category_slug or "").strip() == cid:
        return True, [], matched

    if not kws:
        return False, ["category_mismatch_no_keywords_configured"], matched
    if not matched:
        return False, ["category_keyword_mismatch"], matched
    return True, [], matched


def strict_match(
    candidate: CandidateListing,
    profile: UserSettingsRow,
    db: Database,
) -> MatchResult:
    """Evaluate one candidate against the monitoring profile; Mongo dedupe is part of Step 2."""
    reasons: list[str] = []

    repo = ListingRepository(db)
    if repo.find_by_user_source_url(profile.user_id, candidate.source_url):
        return MatchResult(
            matched=False,
            rejection_reasons=["duplicate_user_source_url"],
            matched_keywords=[],
            candidate_for_ai=None,
        )

    if float(candidate.price) > float(profile.max_price) + 1e-6:
        reasons.append("over_max_price")

    ok_cat, cat_reasons, matched_kw = category_and_keyword_ok(
        category_slug=candidate.category_slug,
        profile_category_id=str(profile.category_id or ""),
        title=candidate.title,
    )
    if not ok_cat:
        reasons.extend(cat_reasons)

    # Geo: reuse listing_within_user_radius (expects RawListing-like fields)
    raw_like = RawListing(
        title=candidate.title,
        price=candidate.price,
        location=candidate.location_text,
        category_slug=candidate.category_slug,
        source_link=candidate.source_link,
        source=candidate.source,
        latitude=candidate.latitude,
        longitude=candidate.longitude,
    )
    if not listing_within_user_radius(
        user_lat=profile.center_lat,
        user_lon=profile.center_lon,
        radius_km=float(profile.radius_km),
        boundary_context=profile.boundary_context,
        user_location_text=profile.location_text,
        listing_lat=raw_like.latitude,
        listing_lon=raw_like.longitude,
        listing_location_text=raw_like.location,
    ):
        reasons.append("location_outside_radius")

    if reasons:
        return MatchResult(
            matched=False,
            rejection_reasons=reasons,
            matched_keywords=matched_kw,
            candidate_for_ai=None,
        )

    return MatchResult(
        matched=True,
        rejection_reasons=[],
        matched_keywords=matched_kw,
        candidate_for_ai=candidate,
    )
