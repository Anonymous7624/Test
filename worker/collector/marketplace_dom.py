"""
Shared Facebook Marketplace DOM hints: result item links and page-state diagnostics.

Facebook changes markup often; keep selector lists and heuristics in one place for Step 1 only.
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Ordered: try specific anchors first, then broader patterns.
# Log which strategy matched so DOM regressions are visible in worker logs.
MARKETPLACE_ITEM_LINK_SELECTORS: list[tuple[str, str]] = [
    ("anchor_href_slash_marketplace_item", 'a[href*="/marketplace/item/"]'),
    ("anchor_href_fb_com_marketplace_item", 'a[href*="facebook.com/marketplace/item"]'),
    ("anchor_href_marketplace_item_relaxed", 'a[href*="marketplace/item"]'),
    (
        "role_link_href_marketplace_item",
        '[role="link"][href*="/marketplace/item/"]',
    ),
]


_LOGIN_OR_SESSION_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"log\s*in", re.I),
    re.compile(r"sign\s*up", re.I),
    re.compile(r"session\s*expired", re.I),
    re.compile(r"confirm\s+it'?s\s+you", re.I),
    re.compile(r"checkpoint", re.I),
)

_EMPTY_RESULTS_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"no\s+results?\s+found", re.I),
    re.compile(r"no\s+listings?\s+found", re.I),
    re.compile(r"nothing\s+for\s+sale", re.I),
    re.compile(r"no\s+items?\s+match", re.I),
    re.compile(r"try\s+adjusting\s+your\s+filters", re.I),
)


def marketplace_search_results_url(query: str) -> str:
    """Canonical Marketplace search URL (query string only; no price/sort params)."""
    from urllib.parse import quote_plus

    q = (query or "").strip()
    return f"https://www.facebook.com/marketplace/search/?query={quote_plus(q)}"


def is_facebook_marketplace_url(url: str) -> bool:
    """True when URL is clearly inside Facebook Marketplace (not global FB search)."""
    u = (url or "").lower()
    if "facebook.com" not in u and "fb.com" not in u:
        return False
    return "/marketplace" in u


def url_looks_like_marketplace_search(url: str) -> bool:
    u = (url or "").lower()
    if "facebook.com" not in u and "fb.com" not in u:
        return False
    if "marketplace" not in u:
        return False
    return "search" in u or "query=" in u


async def wait_for_any_item_link(
    page,
    *,
    timeout_ms: int = 25_000,
) -> tuple[str | None, int]:
    """
    Wait until at least one listing link appears using the first matching selector strategy.
    Returns (selector_name, element_count) or (None, 0) if nothing matched within timeout.
    """
    per = max(3000, min(8000, timeout_ms // max(1, len(MARKETPLACE_ITEM_LINK_SELECTORS))))
    import time as _time

    deadline = _time.monotonic() + (timeout_ms / 1000.0)
    for name, sel in MARKETPLACE_ITEM_LINK_SELECTORS:
        remaining = deadline - _time.monotonic()
        if remaining <= 0:
            break
        try:
            await page.wait_for_selector(
                sel,
                timeout=int(min(per, remaining * 1000)),
                state="attached",
            )
        except Exception:
            continue
        try:
            els = await page.query_selector_all(sel)
            n = len(els)
            if n:
                return name, n
        except Exception:
            continue

    # Full timeout on primary anchor (common case)
    remaining = deadline - _time.monotonic()
    if remaining > 0:
        name, sel = MARKETPLACE_ITEM_LINK_SELECTORS[0]
        try:
            await page.wait_for_selector(
                sel, timeout=int(remaining * 1000), state="attached"
            )
            els = await page.query_selector_all(sel)
            if els:
                return name, len(els)
        except Exception:
            pass
    return None, 0


async def query_all_item_links_with_strategy(
    page,
) -> tuple[str, list]:
    """
    Return all anchor-like listing links using the first strategy that returns elements.
    """
    all_strategies = MARKETPLACE_ITEM_LINK_SELECTORS
    for name, sel in all_strategies:
        try:
            els = await page.query_selector_all(sel)
        except Exception:
            continue
        if els:
            return name, els
    return "none", []


async def read_search_box_value(page) -> str | None:
    """Best-effort: text in a Marketplace-scoped search field."""
    for loc in (
        page.locator('[role="main"]').locator('input[type="search"]'),
        page.locator('[role="main"]').get_by_role("combobox", name=re.compile(r"search", re.I)),
        page.locator('input[type="search"]'),
        page.get_by_role("combobox", name=re.compile(r"search", re.I)),
    ):
        try:
            if await loc.count() < 1:
                continue
            el = loc.first
            v = await el.input_value()
            if v is not None and str(v).strip():
                return str(v).strip()
        except Exception:
            continue
    return None


async def classify_marketplace_page_state(
    page,
    *,
    expected_query: str | None = None,
) -> dict[str, Any]:
    """
    Heuristic page bucket for logs: search results vs feed vs empty vs auth wall vs unknown.
    """
    url = ""
    title = ""
    body_snippet = ""
    try:
        url = page.url or ""
    except Exception:
        pass
    try:
        title = await page.title()
    except Exception:
        pass
    try:
        body = await page.locator("body").inner_text()
        body_snippet = " ".join((body or "").split())[:1200]
    except Exception:
        pass

    state = "unknown"
    low_body = body_snippet.lower()
    if any(p.search(body_snippet) for p in _LOGIN_OR_SESSION_PATTERNS):
        state = "auth_or_checkpoint"
    elif any(p.search(body_snippet) for p in _EMPTY_RESULTS_PATTERNS):
        state = "empty_results_ui"
    elif url_looks_like_marketplace_search(url):
        state = "marketplace_search_url"
    elif "marketplace" in url.lower() and "/marketplace/search" not in url.lower():
        state = "marketplace_non_search"
    elif "marketplace" in url.lower():
        state = "marketplace_other"

    search_val = await read_search_box_value(page)
    q_match: bool | None = None
    if expected_query is not None and search_val is not None:
        q_match = expected_query.strip().lower() in search_val.lower()

    return {
        "page_state": state,
        "url": url,
        "title": title,
        "body_snippet": body_snippet[:800],
        "search_input_value": search_val,
        "search_box_matches_query": q_match,
    }


async def log_no_results_diagnostics(
    page,
    *,
    step_label: str,
    expected_query: str,
    submission_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Structured log when no listing cards were parsed."""
    classification = await classify_marketplace_page_state(
        page, expected_query=expected_query
    )
    line = (
        "Marketplace Step1 no-results diagnostics [%s]: page_state=%s url=%r title=%r "
        "search_input_value=%r search_box_matches_query=%s submission=%s body_snippet=%r"
    )
    logger.warning(
        line,
        step_label,
        classification.get("page_state"),
        classification.get("url"),
        classification.get("title"),
        classification.get("search_input_value"),
        classification.get("search_box_matches_query"),
        submission_meta or {},
        classification.get("body_snippet"),
    )
    return classification
