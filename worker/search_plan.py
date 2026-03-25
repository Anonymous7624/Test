"""
Structured Step 1 search plans: Marketplace category browse or keyword searches.

Step 1 uses a path-only Marketplace entry URL (category segment when in marketplace_category mode),
then applies location, radius, and sort in the browser UI. Custom keyword mode runs each phrase
via the Marketplace search box (never global Facebook search).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal

from app.domain import UserSettings as UserSettingsRow
from app.services.marketplace_step1_queries import focused_queries_from_custom_keywords

logger = logging.getLogger(__name__)

DEFAULT_MARKETPLACE_SORT = "creation_time_descend"

MARKETPLACE_SORT_UI_LABEL: dict[str, str] = {
    "creation_time_descend": "Newest first",
}


def _radius_km_to_miles(r_km: float) -> float:
    return float(r_km) * 0.621371192237334


class SearchPlanInvalidError(RuntimeError):
    """Raised when a profile cannot produce a valid Step 1 search plan."""


def build_marketplace_entry_url(plan: SearchPlan) -> str:
    """Path-only Marketplace entry URL (no filter query string)."""
    base = "https://www.facebook.com"
    if plan.marketplace_category_slug:
        return f"{base}/marketplace/category/{plan.marketplace_category_slug}/"
    return f"{base}/marketplace/"


def validate_search_plan_for_step1(plan: SearchPlan) -> None:
    if not (plan.location_text or "").strip():
        raise SearchPlanInvalidError(
            "Location is required to set Marketplace location and radius in the browser."
        )
    if plan.search_mode == "custom_keywords":
        qs = [q.strip() for q in plan.focused_queries if q and str(q).strip()]
        if not qs:
            raise SearchPlanInvalidError(
                "Custom keyword mode requires at least one keyword."
            )
    elif plan.search_mode == "marketplace_category":
        if not (plan.marketplace_category_slug or "").strip():
            raise SearchPlanInvalidError("Select a Marketplace category.")
    else:
        raise SearchPlanInvalidError(f"Unknown search_mode={plan.search_mode!r}.")


Step1CollectionMode = Literal["category_feed", "keyword_queries"]


@dataclass
class SearchPlan:
    """Structured inputs for Step 1 Marketplace collection (logged + passed to the collector)."""

    user_id: int
    search_mode: str
    location_text: str
    radius_miles: float
    sort_mode: str
    marketplace_category_slug: str | None
    marketplace_category_label: str | None
    focused_queries: list[str]
    step1_collection_mode: Step1CollectionMode
    listing_category_ref: str
    raw_custom_keywords: list[str] = field(default_factory=list)

    def to_log_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "search_mode": self.search_mode,
            "location_text": self.location_text,
            "radius_miles": round(self.radius_miles, 2),
            "sort_mode": self.sort_mode,
            "marketplace_category_slug": self.marketplace_category_slug,
            "marketplace_category_label": self.marketplace_category_label,
            "focused_queries": list(self.focused_queries),
            "step1_collection_mode": self.step1_collection_mode,
            "listing_category_ref": self.listing_category_ref,
            "raw_custom_keywords": list(self.raw_custom_keywords),
        }


def build_search_plan(profile: UserSettingsRow) -> SearchPlan:
    sm = (profile.search_mode or "marketplace_category").strip()
    loc = (profile.location_text or "").strip()
    r_mi = _radius_km_to_miles(float(profile.radius_km))

    if sm == "custom_keywords":
        raw_kws = list(profile.custom_keywords or [])
        focused = focused_queries_from_custom_keywords(raw_kws)
        slug = None
        label = None
        step1_mode: Step1CollectionMode = "keyword_queries"
        listing_ref = "custom_keywords"
    else:
        raw_kws = []
        slug = (profile.marketplace_category_slug or "").strip() or None
        label = (profile.marketplace_category_label or "").strip() or None
        focused = []
        step1_mode = "category_feed"
        listing_ref = slug or "marketplace"

    logger.info(
        "Step 1 search plan user_id=%s search_mode=%s step1_collection_mode=%s slug=%s focused_queries=%s",
        int(profile.user_id),
        sm,
        step1_mode,
        slug,
        list(focused),
    )
    return SearchPlan(
        user_id=int(profile.user_id),
        search_mode=sm,
        location_text=loc,
        radius_miles=r_mi,
        sort_mode=DEFAULT_MARKETPLACE_SORT,
        marketplace_category_slug=slug,
        marketplace_category_label=label,
        focused_queries=focused,
        step1_collection_mode=step1_mode,
        listing_category_ref=listing_ref,
        raw_custom_keywords=list(raw_kws),
    )
