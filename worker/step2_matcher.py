"""Step 2: strict matching, dedupe against MongoDB, geo + search-mode relevance."""

from __future__ import annotations

import math

from pymongo.database import Database

from app.domain import UserSettings as UserSettingsRow
from app.repositories.listing_repository import ListingRepository
from app.services.geo_filter import listing_within_user_radius
from app.services.search_settings import normalize_custom_keywords

from candidate_models import CandidateListing, MatchResult
from mock_scraper import RawListing


def search_mode_relevance_ok(
    *,
    profile: UserSettingsRow,
    title: str,
    description: str,
    listing_category_slug: str,
) -> tuple[bool, list[str], list[str]]:
    """
    Enforce search-mode relevance. Returns (ok, rejection_reasons, matched_keywords for custom mode).
    """
    sm = (profile.search_mode or "marketplace_category").strip()
    title_l = (title or "").lower()
    desc_l = (description or "").lower()
    blob = f"{title_l} {desc_l}"

    if sm == "marketplace_category":
        want = (profile.marketplace_category_slug or "").strip().lower()
        got = (listing_category_slug or "").strip().lower()
        if want and got and got != want:
            return False, ["category_slug_mismatch"], []
        return True, [], []

    if sm == "custom_keywords":
        kws = normalize_custom_keywords(list(profile.custom_keywords or []))
        matched: list[str] = []
        for kw in kws:
            ks = kw.strip().lower()
            if ks and ks in blob:
                matched.append(kw.strip())
        if not matched:
            return False, ["custom_keyword_no_match"], []
        return True, [], matched

    return False, ["unknown_search_mode"], []


def strict_match(
    candidate: CandidateListing,
    profile: UserSettingsRow,
    db: Database,
) -> MatchResult:
    """
    Evaluate one candidate: Mongo dedupe → price validity → geo → search-mode relevance.
    """
    repo = ListingRepository(db)
    if repo.find_by_user_source_url(profile.user_id, candidate.source_url):
        return MatchResult(
            matched=False,
            rejection_reasons=["duplicate_user_source_url"],
            matched_keywords=[],
            candidate_for_ai=None,
        )

    p = float(candidate.price)
    if math.isnan(p) or math.isinf(p):
        return MatchResult(
            matched=False,
            rejection_reasons=["invalid_price"],
            matched_keywords=[],
            candidate_for_ai=None,
        )
    if p <= 0:
        return MatchResult(
            matched=False,
            rejection_reasons=["non_positive_price"],
            matched_keywords=[],
            candidate_for_ai=None,
        )

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
        return MatchResult(
            matched=False,
            rejection_reasons=["location_outside_radius"],
            matched_keywords=[],
            candidate_for_ai=None,
        )

    ok_rel, rel_reasons, matched_kw = search_mode_relevance_ok(
        profile=profile,
        title=candidate.title,
        description=candidate.description or "",
        listing_category_slug=candidate.category_slug,
    )
    if not ok_rel:
        return MatchResult(
            matched=False,
            rejection_reasons=rel_reasons,
            matched_keywords=matched_kw,
            candidate_for_ai=None,
        )

    return MatchResult(
        matched=True,
        rejection_reasons=[],
        matched_keywords=matched_kw,
        candidate_for_ai=candidate,
    )
