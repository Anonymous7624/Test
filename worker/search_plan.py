"""
Structured Step 1 search plans: Marketplace filters + focused product queries (no keyword blobs).

Step 1 uses a path-only Marketplace entry URL (category segment when set), then applies
location, radius, price, and sort in the browser UI. Focused terms run via the search box —
not via hand-built ``?maxPrice=...`` URLs.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from app.domain import UserSettings as UserSettingsRow
from app.services.categories_service import keywords_for_category

logger = logging.getLogger(__name__)

# Low-signal marketing / filler terms — never use as search queries unless explicitly allowed later.
DEFAULT_EXCLUDED_QUERY_TOKENS: frozenset[str] = frozenset(
    {
        "deal",
        "deals",
        "sale",
        "sales",
        "local",
        "bundle",
        "bundles",
        "clearance",
        "cheap",
        "cheapest",
        "discount",
        "discounted",
        "near",
        "me",
        "nearby",
        "lot",
        "lots",
        "assorted",
        "misc",
        "various",
        "free",
        "obo",
        "firm",
        "must",
        "go",
        "quick",
        "asap",
        "today",
        "urgent",
    }
)

# Internal category id -> Facebook Marketplace category path segment (browse + filters on category).
# ``None`` = use global ``/marketplace/search/`` (no category path).
FB_MARKETPLACE_CATEGORY_SLUG: dict[str, str | None] = {
    "general": None,
    "electronics": "electronics",
    "furniture": "furniture",
    "vehicles": "vehicles",
}

# Default: newest first (good for monitoring). Internal key; UI shows a label (e.g. "Newest first").
DEFAULT_MARKETPLACE_SORT = "creation_time_descend"

# Map internal sort_mode -> English Marketplace UI label (logged-in web, en locale).
MARKETPLACE_SORT_UI_LABEL: dict[str, str] = {
    "creation_time_descend": "Newest first",
}

_MAX_FOCUS_QUERIES = 6
_TOKEN_SPLIT_RE = re.compile(r"[\s,/]+")


def _radius_km_to_miles(r_km: float) -> float:
    return float(r_km) * 0.621371192237334


def _sanitize_token(t: str) -> str | None:
    s = t.strip().lower()
    if len(s) < 2:
        return None
    if s in DEFAULT_EXCLUDED_QUERY_TOKENS:
        return None
    # strip simple surrounding punctuation
    s = s.strip("'\"")
    if len(s) < 2 or s in DEFAULT_EXCLUDED_QUERY_TOKENS:
        return None
    return s


def focused_queries_from_category_keywords(
    category_id: str,
    raw_keywords: list[str],
    *,
    max_queries: int = _MAX_FOCUS_QUERIES,
) -> list[str]:
    """
    Build a short list of high-signal search strings from config keywords.
    Drops generic marketplace filler words; keeps product nouns and short phrases.
    """
    seen_lower: set[str] = set()
    out: list[str] = []

    for phrase in raw_keywords:
        if not phrase or not str(phrase).strip():
            continue
        parts = [p for p in _TOKEN_SPLIT_RE.split(str(phrase).strip()) if p]
        kept_tokens: list[str] = []
        for p in parts:
            tok = _sanitize_token(p)
            if tok:
                kept_tokens.append(tok)
        if not kept_tokens:
            continue
        # Title-case lightly for readability in logs; URL encoding preserves meaning.
        q = " ".join(kept_tokens)
        low = q.lower()
        if low in seen_lower:
            continue
        seen_lower.add(low)
        out.append(q)
        if len(out) >= max_queries:
            break

    return out


class SearchPlanInvalidError(RuntimeError):
    """Raised when a profile cannot produce a valid Step 1 search plan (no queries, no location, etc.)."""


def build_marketplace_entry_url(plan: SearchPlan) -> str:
    """
    Path-only Marketplace entry URL (no filter query string).

    Category context uses ``/marketplace/category/{slug}/`` when configured; otherwise ``/marketplace/``.
    Filters (location, radius, price, sort) are applied in the browser UI — not via URL params.
    """
    base = "https://www.facebook.com"
    if plan.marketplace_category_slug:
        return f"{base}/marketplace/category/{plan.marketplace_category_slug}/"
    return f"{base}/marketplace/"


def validate_search_plan_for_step1(plan: SearchPlan) -> None:
    """
    Real Playwright collection requires a non-empty location and at least one focused query term.
    Prevents blank searches and empty ``query=`` navigation.
    """
    if not (plan.location_text or "").strip():
        raise SearchPlanInvalidError(
            "Step 1 requires location_text to set Marketplace location/radius in the UI."
        )
    queries = [q.strip() for q in plan.focused_queries if q and str(q).strip()]
    if not queries:
        raise SearchPlanInvalidError(
            "Step 1 requires at least one focused product query term (from category keywords). "
            "Add keywords in config/categories.json or choose a category with keywords."
        )


@dataclass
class SearchPlan:
    """Structured inputs for Step 1 Marketplace collection (logged + passed to the collector)."""

    user_id: int
    category_id: str
    location_text: str
    radius_miles: float
    max_price: float
    sort_mode: str
    marketplace_category_slug: str | None
    focused_queries: list[str]
    raw_category_keywords: list[str] = field(default_factory=list)

    def to_log_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "category_id": self.category_id,
            "location_text": self.location_text,
            "radius_miles": round(self.radius_miles, 2),
            "max_price": self.max_price,
            "sort_mode": self.sort_mode,
            "marketplace_category_slug": self.marketplace_category_slug,
            "focused_queries": list(self.focused_queries),
            "raw_category_keywords": list(self.raw_category_keywords),
        }


def build_search_plan(profile: UserSettingsRow) -> SearchPlan:
    cid = str(profile.category_id or "").strip() or "general"
    raw_kws = keywords_for_category(cid)
    focused = focused_queries_from_category_keywords(cid, raw_kws)
    logger.info(
        "Step 1 search keywords user_id=%s category_id=%s raw_category_keywords=%s focused_queries=%s",
        int(profile.user_id),
        cid,
        list(raw_kws),
        list(focused),
    )
    loc = (profile.location_text or "").strip()
    r_mi = _radius_km_to_miles(float(profile.radius_km))
    slug = FB_MARKETPLACE_CATEGORY_SLUG.get(cid)
    return SearchPlan(
        user_id=int(profile.user_id),
        category_id=cid,
        location_text=loc,
        radius_miles=r_mi,
        max_price=float(profile.max_price),
        sort_mode=DEFAULT_MARKETPLACE_SORT,
        marketplace_category_slug=slug,
        focused_queries=focused,
        raw_category_keywords=list(raw_kws),
    )
