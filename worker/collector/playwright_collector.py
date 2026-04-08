"""
Facebook Marketplace collection via Playwright (async API).

Uses ``storage_state`` from ``backend/playwright/.auth/facebook.json`` (see
``facebook_login_bootstrap.py`` at repo root).

Modes:
- Default: navigate to Marketplace (path-only entry URL), apply filters in the UI, then browse
  the category feed or run Marketplace keyword searches (no price filter params in the URL).
- ``COLLECTOR_USE_LOCAL_STUB=1``: load local HTML (``COLLECTOR_STUB_HTML`` or bundled stub)
  for offline/CI — same DOM as before.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import replace
from pathlib import Path

from mock_scraper import RawListing

from search_context import CollectionInputs

from .errors import CollectorInterruptedError
from .marketplace_dom import (
    log_no_results_diagnostics,
    query_all_item_links_with_strategy,
    wait_for_any_item_link,
)
from .marketplace_ui import (
    MarketplaceFilterError,
    apply_marketplace_filters_ui,
    ensure_marketplace_context,
    run_focused_marketplace_query,
)

logger = logging.getLogger(__name__)


def _is_benign_playwright_close_error(exc: BaseException) -> bool:
    """True when teardown hit an already-closed handle (idempotent close)."""
    try:
        from playwright._impl._errors import (  # noqa: PLC0415 — after playwright import in fetch
            is_target_closed_error,
        )

        if isinstance(exc, Exception) and is_target_closed_error(exc):
            return True
    except Exception:
        pass
    msg = str(exc).lower()
    return (
        "has been closed" in msg
        or "target closed" in msg
        or "browser has been closed" in msg
        or "context has been closed" in msg
        or "connection closed" in msg
    )


async def _safe_close_playwright(label: str, close_coro) -> None:
    """
    Close a Playwright handle without turning teardown into a fatal fetch error.

    Double-close and \"already closed\" races are common; a raised exception in ``finally``
    would otherwise suppress a successful ``return`` from the collector.
    """
    try:
        await close_coro
    except BaseException as exc:
        if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
            raise
        if _is_benign_playwright_close_error(exc):
            logger.warning(
                "Playwright cleanup: %s already closed or gone (ignored): %s",
                label,
                exc,
            )
            return
        if isinstance(exc, Exception):
            logger.warning(
                "Playwright cleanup: %s close raised (ignored): %s",
                label,
                exc,
            )
            return
        raise


async def _teardown_playwright_session(
    *,
    browser,
    context,
    page,
    user_id: str | None,
    use_stub: bool,
) -> None:
    """Idempotent page → context → browser teardown; logs start and completion."""
    logger.info(
        "Playwright cleanup started (user_id=%s stub=%s)",
        user_id,
        use_stub,
    )
    if page is not None and not page.is_closed():
        await _safe_close_playwright("page", page.close())
    if context is not None:
        await _safe_close_playwright("context", context.close())
    if browser is not None and browser.is_connected():
        await _safe_close_playwright("browser", browser.close())
    logger.info(
        "Playwright cleanup completed safely (user_id=%s stub=%s)",
        user_id,
        use_stub,
    )


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)).strip())
    except ValueError:
        return default


_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_FACEBOOK_AUTH_STATE = (
    _REPO_ROOT / "backend" / "playwright" / ".auth" / "facebook.json"
)
_STUB = Path(__file__).resolve().parent / "static" / "marketplace_stub.html"

# Max raw listings collected per Playwright run unless WORKER_COLLECTOR_BATCH_CAP is set.
_DEFAULT_WORKER_COLLECTOR_BATCH_CAP_LIVE = 30
_DEFAULT_WORKER_COLLECTOR_BATCH_CAP_BACKFILL = 30
# Tighter default for category-feed live runs: early location screening removes obvious
# out-of-radius listings before detail-page enrichment, so a smaller initial batch wastes
# far less work.  Override with WORKER_COLLECTOR_CATEGORY_FEED_BATCH_CAP or the global
# WORKER_COLLECTOR_BATCH_CAP env var.
_DEFAULT_WORKER_COLLECTOR_BATCH_CAP_CATEGORY_FEED_LIVE = 15

_ITEM_HREF_RE = re.compile(r"/marketplace/item/(\d+)", re.I)
_PRICE_RE = re.compile(r"\$\s*([\d,]+(?:\.\d{2})?)")


class FacebookAuthStateMissingError(RuntimeError):
    """Raised when the Facebook Playwright storage state file has not been created."""

    def __init__(self, path: Path) -> None:
        self.path = path
        super().__init__(
            f"Facebook Playwright auth state not found: {path}\n"
            "Create it with a one-time manual login (repository root):\n"
            "  python facebook_login_bootstrap.py\n"
            "Log in in the opened browser, then press Enter to save. "
            "The worker reuses that file on later runs; do not start login from the app."
        )


def facebook_auth_state_path() -> Path:
    """Resolved path to the saved Playwright storage state for Facebook."""
    return _FACEBOOK_AUTH_STATE.resolve()


def _parse_float(val: str | None) -> float | None:
    if val is None or not str(val).strip():
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _normalize_fb_url(href: str) -> str:
    if href.startswith("/"):
        return f"https://www.facebook.com{href.split('?', 1)[0]}"
    if href.startswith("http"):
        return href.split("?", 1)[0]
    return href


def _raw_dedupe_key(raw: RawListing) -> str:
    sid = (raw.source_id or "").strip()
    if sid:
        return sid
    return _normalize_fb_url(raw.source_link)


def _item_id_from_href(href: str) -> str | None:
    m = _ITEM_HREF_RE.search(href)
    return m.group(1) if m else None


def _extract_price(text: str) -> float | None:
    m = _PRICE_RE.search(text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _title_from_card_text(text: str) -> str:
    """First non-trivial, non-junk line from a Marketplace card is the listing title."""
    lines = [ln.strip() for ln in text.replace("\t", " ").splitlines() if ln.strip()]
    for ln in lines:
        # Skip very short tokens
        if len(ln) < 3:
            continue
        # Skip lines that are only a price
        if _extract_price(ln) is not None and len(ln) <= 18:
            continue
        # Skip pure notification / UI chrome lines
        if ln.lower() in _JUNK_STANDALONE_LINES:
            continue
        # Clean any junk prefix/suffix from the chosen line
        cleaned = _clean_card_title(ln)
        if cleaned and len(cleaned) >= 2:
            return cleaned[:500]
    # Fallback: strip price, take first line
    cleaned = _PRICE_RE.sub("", text).strip()
    first = cleaned.split("\n")[0][:500] if cleaned else ""
    return _clean_card_title(first) if first else ""


_MI_DIST_ONLY = re.compile(
    r"^\s*\d+\s*(mi|miles|km|m)\s*$",
    re.I,
)


def _line_looks_like_location(line: str) -> bool:
    s = (line or "").strip()
    if len(s) < 3 or len(s) > 120:
        return False
    if s.startswith("$"):
        return False
    if _extract_price(s) is not None and "$" in s:
        return False
    if _MI_DIST_ONLY.match(s):
        return False
    return bool(re.search(r"[A-Za-z]{2,}", s))


def _split_location_from_middle_dot(line: str) -> str | None:
    """e.g. \"Price · Town, ST\" or \"· Hempstead\"."""
    for sep in ("·", "•", "|"):
        if sep in line:
            parts = [p.strip() for p in line.split(sep) if p.strip()]
            for p in reversed(parts):
                if p.startswith("$") or _extract_price(p) is not None:
                    continue
                if _line_looks_like_location(p):
                    return p
    return None


def _extract_listing_location_from_card_text(
    text: str,
    *,
    title: str,
    primary_search: str,
) -> tuple[str, str | None]:
    """
    Returns ``(location_for_geo, parsed_display_or_none)``.

    ``location_for_geo`` is always non-empty (uses primary search region as last resort).
    ``parsed_display`` is set when we parsed a concrete line from the card (for alerts).
    """
    primary = (primary_search or "").strip() or "Unknown"
    raw = (text or "").replace("\t", " ")
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    if not lines:
        return primary, None

    parsed: str | None = None
    for ln in lines:
        if "·" in ln or "•" in ln or "|" in ln:
            hit = _split_location_from_middle_dot(ln)
            if hit:
                parsed = hit
                break

    if not parsed:
        price_idx = None
        for i, ln in enumerate(lines):
            if "$" in ln and _extract_price(ln) is not None:
                price_idx = i
                break
        if price_idx is not None:
            for j in range(price_idx + 1, min(price_idx + 6, len(lines))):
                ln = lines[j]
                tl = (title or "").strip().lower()
                if tl and ln.lower() == tl[: min(len(ln), len(tl))]:
                    continue
                if _MI_DIST_ONLY.match(ln):
                    continue
                if _line_looks_like_location(ln):
                    parsed = ln
                    break

    if not parsed:
        for ln in reversed(lines[-5:]):
            if _line_looks_like_location(ln):
                parsed = ln
                break

    if not parsed:
        return primary, None

    if primary and parsed.strip().lower() == primary.lower():
        return primary, None

    # Only promote to loc_parsed when the text is a plausible location string.
    # Junk / product text that slipped through the heuristics above (e.g.
    # "3 savedMark as read", "Price dropped", product model numbers) is
    # discarded here so it never reaches the early location screen as a fake
    # "location" and incorrectly rejects a good listing.
    if not _is_valid_visible_location(parsed):
        return primary, None

    return parsed.strip(), parsed.strip()


def _early_location_screen(
    items: list[RawListing],
    *,
    collection_inputs: CollectionInputs,
    category_feed_mode: bool = False,
) -> tuple[list[RawListing], int, int]:
    """
    Pre-enrichment location filter using only text visible on the card.

    Rejects a listing early **only** when all three conditions hold:

    1. ``listing_location_parsed`` is set and passes ``_is_valid_visible_location``
       (i.e. it is a real city/region string, not UI chrome or product text).
    2. That location fails the text-based radius check (``listing_within_user_radius``).
    3. In ``category_feed_mode`` the location must additionally match the
       high-confidence ``'City, ST'`` pattern — because Facebook's own
       location/radius UI filter has already been applied, so the card-level
       text is only a secondary hint and should not override that filter unless
       it is unambiguous.

    Any listing whose parsed location is missing, malformed, or junk text is
    passed through as "unknown" — it continues to detail-page enrichment where
    the full listing page can be inspected.

    Returns ``(passed_items, early_rejected_count, unknown_or_skipped_count)``.
    """
    # Lazy import: backend/ is added to sys.path by main.py before this is ever called.
    from app.services.geo_filter import listing_within_user_radius  # noqa: PLC0415

    passed: list[RawListing] = []
    rejected = 0
    unknown = 0

    for raw in items:
        parsed_loc = (raw.listing_location_parsed or "").strip()

        if not parsed_loc:
            # No distinct card location parsed — use primary-search fallback; pass through.
            unknown += 1
            passed.append(raw)
            continue

        # Belt-and-suspenders guard: reject any junk text that slipped through
        # _extract_listing_location_from_card_text.  Log at DEBUG so logs are not noisy.
        if not _is_valid_visible_location(parsed_loc):
            unknown += 1
            passed.append(raw)
            logger.debug(
                "Step 1 loc-screen user_id=%s card_location=%r "
                "(malformed/junk text — ignored, listing passed through)",
                collection_inputs.user_id,
                parsed_loc,
            )
            continue

        # In category_feed mode the Facebook UI radius filter already ran, so
        # we only early-reject when we are *certain* the card location is a
        # real city outside the radius.  Require the high-confidence
        # "City, ST" pattern; anything less specific is passed through.
        if category_feed_mode and not _is_high_confidence_city_state(parsed_loc):
            unknown += 1
            passed.append(raw)
            logger.debug(
                "Step 1 loc-screen user_id=%s card_location=%r "
                "(category_feed: low-confidence location — passed through; "
                "UI radius filter already applied)",
                collection_inputs.user_id,
                parsed_loc,
            )
            continue

        within = listing_within_user_radius(
            user_lat=collection_inputs.center_lat,
            user_lon=collection_inputs.center_lon,
            radius_km=collection_inputs.radius_km,
            boundary_context=collection_inputs.boundary_context,
            user_location_text=collection_inputs.location_text,
            listing_lat=None,   # cards never carry lat/lon
            listing_lon=None,
            listing_location_text=parsed_loc,
        )
        if within:
            passed.append(raw)
        else:
            rejected += 1
            logger.info(
                "Step 1 early-loc-reject user_id=%s card_location=%r "
                "(valid location, confirmed outside radius — detail-enrich skipped)",
                collection_inputs.user_id,
                parsed_loc,
            )

    return passed, rejected, unknown


def _quick_location_reject_count(
    batch: list[RawListing],
    collection_inputs: CollectionInputs,
    *,
    category_feed_mode: bool = False,
) -> int:
    """
    Count how many cards in ``batch`` would fail the early location screen without logging each one.

    Used for the adaptive scroll-stop heuristic: if a whole scroll round produces only
    out-of-radius cards, continuing to scroll will likely produce more of the same, so we
    stop early.  Returns 0 when no parsed card locations are present (unknown ≠ rejected).

    Applies the same validity guards as ``_early_location_screen`` so junk text
    does not inflate the reject count and trigger a premature scroll-stop.
    """
    # Lazy import; backend/ is on sys.path before this is ever called.
    from app.services.geo_filter import listing_within_user_radius  # noqa: PLC0415

    rejected = 0
    for raw in batch:
        parsed_loc = (raw.listing_location_parsed or "").strip()
        if not parsed_loc:
            continue  # unknown — cannot reject on card text alone
        # Skip junk text — it would not be rejected by _early_location_screen either.
        if not _is_valid_visible_location(parsed_loc):
            continue
        # In category_feed mode only high-confidence "City, ST" triggers early rejection.
        if category_feed_mode and not _is_high_confidence_city_state(parsed_loc):
            continue
        within = listing_within_user_radius(
            user_lat=collection_inputs.center_lat,
            user_lon=collection_inputs.center_lon,
            radius_km=collection_inputs.radius_km,
            boundary_context=collection_inputs.boundary_context,
            user_location_text=collection_inputs.location_text,
            listing_lat=None,
            listing_lon=None,
            listing_location_text=parsed_loc,
        )
        if not within:
            rejected += 1
    return rejected


async def _parse_stub_page(
    page,
    *,
    collection_inputs: CollectionInputs,
    backfill: bool,
    stub_path: Path,
) -> list[RawListing]:
    uri = stub_path.as_uri()
    target_cat = (collection_inputs.listing_category_ref or "marketplace").strip()
    out: list[RawListing] = []

    await page.goto(uri, wait_until="domcontentloaded")
    elements = await page.query_selector_all("article.listing")
    for el in elements:
        url = await el.get_attribute("data-url") or ""
        price_a = _parse_float(await el.get_attribute("data-price"))
        lat = _parse_float(await el.get_attribute("data-lat"))
        lon = _parse_float(await el.get_attribute("data-lon"))
        cat = (await el.get_attribute("data-category") or "general").strip()
        if cat != target_cat:
            continue
        h2 = await el.query_selector("h2")
        loc_el = await el.query_selector(".loc")
        title = ((await h2.inner_text()) if h2 else "").strip()
        loc = ((await loc_el.inner_text()) if loc_el else "").strip()
        if not url or title == "" or price_a is None:
            continue
        ext = url.rsplit("/", maxsplit=1)[-1]
        out.append(
            RawListing(
                title=title,
                price=price_a,
                location=loc or "Unknown",
                category_slug=cat,
                source_link=url,
                source="playwright_stub",
                latitude=lat,
                longitude=lon,
                source_id=f"playwright_stub:{ext}",
            )
        )
    if not backfill:
        out = out[:4]
    elif backfill and out:
        extra: list[RawListing] = []
        for i, r in enumerate(out):
            extra.append(
                RawListing(
                    title=f"[Archive] {r.title}",
                    price=round(r.price * 0.95, 2),
                    location=r.location,
                    category_slug=r.category_slug,
                    source_link=f"https://archive.example.com/item/{i}-{r.source_link.rsplit('/', 1)[-1]}",
                    source="playwright_stub_backfill",
                    latitude=r.latitude,
                    longitude=r.longitude,
                )
            )
        out = out + extra
    return out


async def _harvest_visible_marketplace_cards(
    page,
    collection_inputs: CollectionInputs,
    *,
    max_items: int | None = None,
) -> tuple[str, list[RawListing]]:
    """Parse currently loaded result cards (no scroll). ``max_items`` caps harvest size when set."""
    strategy_name, links = await query_all_item_links_with_strategy(page)
    if not links:
        return strategy_name, []
    logger.debug(
        "Marketplace harvest: strategy=%s link_count=%s",
        strategy_name,
        len(links),
    )
    seen_href: set[str] = set()
    out: list[RawListing] = []
    cat = (collection_inputs.listing_category_ref or "marketplace").strip()
    default_loc = (collection_inputs.primary_search_location or "Unknown").strip() or "Unknown"

    for link in links:
        if max_items is not None and len(out) >= max_items:
            break
        href = await link.get_attribute("href")
        if not href:
            continue
        full = _normalize_fb_url(href)
        iid = _item_id_from_href(full)
        if not iid or full in seen_href:
            continue
        seen_href.add(full)
        text = (await link.inner_text() or "").strip()
        price = _extract_price(text)
        title = _title_from_card_text(text)
        if price is None or not title:
            continue
        loc_geo, loc_parsed = _extract_listing_location_from_card_text(
            text,
            title=title,
            primary_search=default_loc,
        )
        out.append(
            RawListing(
                title=title,
                price=price,
                location=loc_geo,
                category_slug=cat,
                source_link=full,
                source="facebook_marketplace",
                latitude=None,
                longitude=None,
                source_id=f"fb:{iid}",
                listing_location_parsed=loc_parsed,
            )
        )
    if links and not out:
        logger.warning(
            "Marketplace harvest: %s item links matched selector=%s but none passed price/title heuristics url=%r",
            len(links),
            strategy_name,
            page.url,
        )
    return strategy_name, out


# ── Junk / noise patterns ────────────────────────────────────────────────────

# Lines that are pure notification / UI chrome — never a real listing title.
_JUNK_STANDALONE_LINES: frozenset[str] = frozenset(
    {
        "unread",
        "mark as read",
        "today's picks",
        "today\u2019s picks",
        "sponsored",
        "see more",
        "send seller a message",
        "chat with seller",
        "message seller",
        "save",
        "share",
        "report",
        "new",
        "sold",
        "pending",
    }
)

# Prefixes that Facebook prepends to card text for notification/unread badges.
_JUNK_PREFIX_RE = re.compile(
    r"^(?:Unread\s*|New\s+message\s*|Mark\s+as\s+read\s*)+",
    re.I,
)

# Trailing noise appended after a real title: "·1d", ".1dMark as read", etc.
_JUNK_SUFFIX_RE = re.compile(
    r"(?:\s*[\u00b7·]\s*\d+[hdwm]|\s*\.\s*\d+[hdwm]|\s*\d+[hdwm])?(?:\s*Mark\s+as\s+read)?$",
    re.I,
)

# ── Visible-location validation ──────────────────────────────────────────────

# UI/notification strings that are never a real location.
# Covers the patterns seen in logs: "Price dropped", "Mark as read",
# "savedMark as read", "New message", "Message seller", etc.
_LOC_JUNK_RE = re.compile(
    r"(?:"
    r"\bprice\s+drop(?:ped)?\b"           # "Price dropped"
    r"|\bmark\s+as\s+read\b"              # "Mark as read"
    r"|\bsaved\s*mark\b"                  # "savedMark" (concatenated UI chrome)
    r"|\bnew\s+message\b"                 # "New message"
    r"|\bmessage\s+(?:seller|the)\b"      # "Message seller"
    r"|\bchat\s+with\b"                   # "Chat with seller"
    r"|\bsend\s+(?:seller|a)\b"           # "Send seller a message"
    r"|\bsee\s+more\b"                    # "See more"
    r"|\bsponsored\b"                     # "Sponsored"
    r"|\btoday[''`\u2019]?s\s+picks?\b"   # "Today's picks"
    r"|\bunread\b"                        # "Unread"
    r")",
    re.I,
)

# Product-code tokens: uppercase+digit combos, hyphenated SKUs.
# e.g. "256GB", "SV3Z-7W", "A1234B"  (state abbrevs like "NY" do NOT match).
_LOC_PRODUCT_CODE_RE = re.compile(
    r"\b(?:[A-Z]{2,}\d+|\d+[A-Z]{2,}|[A-Z0-9]{3,}-[A-Z0-9]+)\b"
)

# Valid location shape: only letters / spaces / hyphens / apostrophes / dots.
# No digits — real US city names on Marketplace don't contain numerals.
# Optionally followed by ", State" (full name or 2–30-char abbreviation).
_VALID_LOC_SHAPE_RE = re.compile(
    r"^[A-Za-z][A-Za-z\s\-\'\.]{1,49}(?:,\s*[A-Za-z]{2,30})?\s*$"
)

# High-confidence: exactly "City, 2-letter-state" — e.g. "Bellmore, NY".
_HIGH_CONF_CITY_STATE_RE = re.compile(
    r"^[A-Za-z][A-Za-z\s\-\'\.]{1,39},\s*[A-Za-z]{2}\s*$"
)


def _is_valid_visible_location(text: str) -> bool:
    """
    Return True only when ``text`` is plausibly a real location string on a
    Facebook Marketplace card (e.g. ``"Bellmore, NY"``, ``"Plainview, NY"``).

    Rejects:
    - UI chrome / notification text (``"3 savedMark as read"``, ``"Price dropped"``)
    - Product names / model numbers (``"iphone 14 promax 256GB SV3Z-7W"``)
    - Price or distance-only text
    - Anything with a leading digit, product-code tokens, or junk keywords

    This is intentionally conservative: when in doubt it returns False so
    the listing passes through to detail-page enrichment rather than being
    silently dropped by the early location screen.
    """
    s = (text or "").strip()
    if len(s) < 3 or len(s) > 80:
        return False
    # Leading digit → notification badge or product model ("3 savedMark", "14 promax …")
    if s[0].isdigit():
        return False
    # Price text
    if s.startswith("$") or (_extract_price(s) is not None and "$" in s):
        return False
    # Distance-only line (e.g. "12 mi")
    if _MI_DIST_ONLY.match(s):
        return False
    # Known junk / notification keyword patterns
    if _LOC_JUNK_RE.search(s):
        return False
    # Product-code tokens (uppercase+digit combos, hyphenated SKUs)
    if _LOC_PRODUCT_CODE_RE.search(s):
        return False
    # Must match the valid location shape (all letters/spaces — no digits)
    if not _VALID_LOC_SHAPE_RE.match(s):
        return False
    return True


def _is_high_confidence_city_state(text: str) -> bool:
    """
    Return True when ``text`` matches the high-confidence ``'City, ST'`` pattern
    (two-letter US state abbreviation), e.g. ``"Bellmore, NY"``.

    Used in ``category_feed`` mode where Facebook's UI radius filter has
    already been applied — only this strong pattern justifies an early reject.
    """
    return bool(_HIGH_CONF_CITY_STATE_RE.match((text or "").strip()))


# Section headings that mark the start of junk content on a detail page.
# Everything after these headings is recommendation / sponsored / UI chrome.
_JUNK_SECTION_HEADING_RE = re.compile(
    r"(?:^|\n)[ \t]*(?:"
    r"Today[''`\u2019]?s picks"
    r"|Related listings?"
    r"|People also (?:viewed|liked)"
    r"|You may also like"
    r"|More from (?:this seller|Marketplace)"
    r"|Similar (?:items?|listings?)"
    r"|Marketplace listings?"
    r"|Other items? you may like"
    r"|Sponsored"
    r"|Recommended for you"
    r"|See more (?:from|on) Marketplace"
    r"|Send (?:seller )?a message"
    r"|Chat with (?:the )?seller"
    r"|Message (?:the )?seller"
    r")[ \t]*(?:\n|$)",
    re.I,
)


def _strip_noise(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _clean_card_title(raw_title: str) -> str:
    """Strip notification prefixes/suffixes injected by Facebook into card text."""
    t = raw_title.strip()
    # Remove leading "Unread", "New message", etc.
    t = _JUNK_PREFIX_RE.sub("", t).strip()
    # Remove trailing time-ago + "Mark as read" fragments.
    t = _JUNK_SUFFIX_RE.sub("", t).strip()
    return t


def _truncate_before_junk_sections(text: str) -> str:
    """Cut text at the first recommended/sponsored/UI-chrome section heading."""
    m = _JUNK_SECTION_HEADING_RE.search(text)
    if m:
        return text[: m.start()].strip()
    return text


def _brand_condition_from_text(text: str) -> tuple[str | None, str | None]:
    brand = None
    m = re.search(r"Brand\s*[:\s]+\s*([^\n\r]+)", text, re.I)
    if m:
        brand = _strip_noise(m.group(1))[:200]
    cond = None
    m = re.search(r"Condition\s*[:\s]+\s*([^\n\r]+)", text, re.I)
    if m:
        cond = _strip_noise(m.group(1))[:200]
    if not cond:
        for tok in (
            "New",
            "Used - Like New",
            "Used - Good",
            "Used - Fair",
            "Used - Excellent",
            "Open box",
        ):
            if re.search(rf"(?:^|\s){re.escape(tok)}(?:\s|$)", text, re.I):
                cond = tok
                break
    return brand, cond


def _description_blob_from_text(text: str, title: str) -> str:
    # Truncate at the first recommended / sponsored / UI-chrome section so we
    # never include "Today's picks", related listings, or seller contact UI.
    t = _truncate_before_junk_sections((text or "").strip())

    # Strategy 1: look for an explicit "Description" / "About this item" heading.
    for m in re.finditer(
        r"(?:Description|About this item|Details)\s*\n+([\s\S]{40,12000}?)"
        r"(?=\n\s*\n(?:Seller|Location|Condition|Brand|Message|Listed in|Shipping|More details)|$)",
        t,
        re.I,
    ):
        blob = m.group(1).strip()
        if len(blob) >= 40:
            return _clean_description_blob(blob)[:8000]

    # Strategy 2: longest paragraph that is not the title and not all-junk.
    paras = [p.strip() for p in re.split(r"\n{2,}", t) if len(p.strip()) > 60]
    best = ""
    for p in paras:
        if title and len(title) > 8 and title[:30].lower() in p.lower()[:120]:
            continue
        if len(p) > len(best):
            best = p
    return _clean_description_blob(best)[:8000] if best else ""


def _clean_description_blob(text: str) -> str:
    """Remove duplicate lines and obvious junk from a description candidate."""
    lines = text.splitlines()
    seen: set[str] = set()
    out: list[str] = []
    for ln in lines:
        norm = ln.strip()
        # Drop pure junk lines (notification chrome, single-word UI labels)
        if norm.lower() in _JUNK_STANDALONE_LINES:
            continue
        # Drop duplicate lines
        key = norm.lower()
        if key and key in seen:
            continue
        seen.add(key)
        out.append(ln)
    # Collapse runs of more than two blank lines
    result = re.sub(r"\n{3,}", "\n\n", "\n".join(out))
    return result.strip()


async def _collect_image_urls_from_page(page, limit: int = 8) -> list[str]:
    urls: list[str] = []
    try:
        for img in await page.query_selector_all('[role="main"] img, img'):
            src = await img.get_attribute("src")
            if not src or not src.startswith("http"):
                continue
            if "emoji" in src or "/static" in src.lower():
                continue
            if src not in urls:
                urls.append(src)
            if len(urls) >= limit:
                break
    except Exception:
        pass
    return urls[:limit]


async def _enrich_one_raw_listing(page, raw: RawListing) -> RawListing:
    url = (raw.source_link or "").strip()
    if not url or "marketplace/item" not in url:
        return raw
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=55_000)
        await page.wait_for_timeout(650)
        await page.wait_for_load_state("domcontentloaded")
    except Exception:
        return raw

    # ── Locate the listing detail root ───────────────────────────────────────
    # Try progressively broader selectors; log which one was used.
    # We prefer a tight container so recommendation / sponsored sections are
    # not included in the text we feed to the title/description extractors.
    text = ""
    container_label = "none"
    _DETAIL_SELECTORS = [
        # Facebook Marketplace item detail pagelet (most specific)
        '[data-pagelet="MarketplaceItemMainContent"]',
        # Generic "article" inside main (common FB layout)
        '[role="main"] article',
        # Fallback to the full main region
        '[role="main"]',
    ]
    for sel in _DETAIL_SELECTORS:
        try:
            el = page.locator(sel).first
            if await el.count():
                raw_text = (await el.inner_text() or "").strip()
                if len(raw_text) >= 30:
                    text = raw_text[:40000]
                    container_label = sel
                    break
        except Exception:
            continue

    if not text:
        try:
            text = (await page.inner_text("body"))[:40000]
            container_label = "body"
        except Exception:
            text = ""

    logger.debug(
        "Detail enrich: url=%s container_root=%r text_len=%s",
        url,
        container_label,
        len(text),
    )

    # Strip junk sections (recommended items, sponsored content, etc.) from the
    # raw text before handing it to any extractor.
    text = _truncate_before_junk_sections(text)

    # ── Title from h1 ────────────────────────────────────────────────────────
    # Find h1 preferably inside the same container we chose for text.
    title_full = None
    try:
        # Try h1 inside the chosen container first, then fall back to first h1.
        h1_locator = (
            page.locator(f"{container_label} h1").first
            if container_label not in ("body", "none")
            else page.locator("h1").first
        )
        if not await h1_locator.count():
            h1_locator = page.locator("h1").first

        if await h1_locator.count():
            t = ((await h1_locator.inner_text()) or "").strip()
            # Clean notification noise from the h1 text.
            t = _clean_card_title(t)
            # Accept if it looks like a genuine title (not a junk phrase, long enough).
            if t and t.lower() not in _JUNK_STANDALONE_LINES and len(t) >= 2:
                title_full = t[:500]
    except Exception:
        pass

    # ── Description, brand, condition, location ───────────────────────────────
    brand, cond = _brand_condition_from_text(text)
    desc = _description_blob_from_text(text, raw.title)
    loc_detail = None
    m = re.search(r"(?:Location|Listed in)\s*[:\s]+\s*([^\n\r]+)", text, re.I)
    if m:
        loc_detail = _strip_noise(m.group(1))[:200]

    imgs = await _collect_image_urls_from_page(page, limit=8)
    primary_img = imgs[0] if imgs else raw.image_url

    new_desc = (desc or "").strip()
    old_desc = (raw.description or "").strip()
    final_desc = new_desc if len(new_desc) > len(old_desc) else old_desc

    return replace(
        raw,
        title_full=title_full or raw.title_full,
        brand=brand or raw.brand,
        condition=cond or raw.condition,
        listing_location_detail=loc_detail or raw.listing_location_detail,
        description=final_desc,
        image_url=primary_img or raw.image_url,
        image_urls=list(imgs) if imgs else list(raw.image_urls or []),
        detail_enriched=True,
    )


async def _maybe_enrich_listings_from_detail_pages(
    page,
    items: list[RawListing],
    *,
    collection_inputs: CollectionInputs,
) -> tuple[list[RawListing], dict]:
    """
    Screen by card-visible location and pre-enrichment duplicate check, then visit detail pages.

    Returns ``(result_listings, screen_meta)``.  ``screen_meta`` keys:

    - ``collected_from_page``: items entering this stage
    - ``rejected_early_by_visible_location``: dropped because card location text is outside radius
    - ``unknown_location_passed``: no parsed card location → passed without screening
    - ``pre_enrich_known_dupes``: items skipped because their source_id/link is already in MongoDB
    - ``allowed_to_detail_enrich``: items that entered detail-page enrichment
    - ``detail_enriched_ok``: items successfully enriched with detail-page data
    - ``final_candidates``: items returned for pipeline processing
    """
    user_id = collection_inputs.user_id
    max_n = _int_env("WORKER_COLLECTOR_DETAIL_ENRICH_MAX", 25)

    # ── Card-visible location pre-screen (before any detail-page browser visits) ──────────────
    # In category_feed mode Facebook's UI location/radius filter has already run,
    # so card-level location text is only a secondary hint.  Only high-confidence
    # "City, ST" strings can trigger an early reject; everything else passes through.
    _cat_feed = (
        getattr(getattr(collection_inputs, "search_plan", None), "step1_collection_mode", None)
        == "category_feed"
    )
    screened, early_reject_n, unknown_n = _early_location_screen(
        items,
        collection_inputs=collection_inputs,
        category_feed_mode=_cat_feed,
    )
    logger.info(
        "Step 1 card-loc-screen user_id=%s mode=%s: "
        "collected_from_page=%s rejected_early_by_visible_location=%s "
        "unknown_location_passed=%s loc_screened_survivors=%s",
        user_id,
        "category_feed" if _cat_feed else "keyword",
        len(items),
        early_reject_n,
        unknown_n,
        len(screened),
    )

    # ── Pre-enrichment duplicate screen (listings already in MongoDB for this user) ──────────
    known_ids = collection_inputs.known_source_ids
    pre_dedup: list[RawListing] = []
    known_dupes_n = 0
    if known_ids:
        for raw in screened:
            sid = (raw.source_id or "").strip()
            slink = (raw.source_link or "").strip()
            if (sid and sid in known_ids) or (slink and slink in known_ids):
                known_dupes_n += 1
                logger.info(
                    "Step 1 pre-enrich-dedupe user_id=%s source_id=%r "
                    "(already in DB — detail-enrich skipped)",
                    user_id,
                    sid or slink,
                )
            else:
                pre_dedup.append(raw)
    else:
        pre_dedup = screened

    if known_dupes_n:
        logger.info(
            "Step 1 pre-enrich-dedupe summary user_id=%s: "
            "known_dupes_skipped=%s loc_screened_survivors=%s after_dedup=%s",
            user_id,
            known_dupes_n,
            len(screened),
            len(pre_dedup),
        )

    screen_meta: dict = {
        "collected_from_page": len(items),
        "rejected_early_by_visible_location": early_reject_n,
        "unknown_location_passed": unknown_n,
        "pre_enrich_known_dupes": known_dupes_n,
        "allowed_to_detail_enrich": len(pre_dedup),
        "detail_enriched_ok": 0,
        "final_candidates": 0,
    }

    if max_n <= 0 or not pre_dedup:
        screen_meta["final_candidates"] = len(pre_dedup)
        logger.info(
            "Step 1 final-candidates user_id=%s: "
            "collected=%s early_loc_rejected=%s pre_enrich_dupes=%s "
            "final_to_pipeline=%s (enrich disabled or empty)",
            user_id,
            len(items),
            early_reject_n,
            known_dupes_n,
            len(pre_dedup),
        )
        return pre_dedup, screen_meta

    # ── Detail-page enrichment for the screened, deduped survivors ───────────────────────────
    out: list[RawListing] = []
    enriched_n = 0
    for i, raw in enumerate(pre_dedup):
        if i >= max_n:
            out.append(raw)
            continue
        try:
            er = await _enrich_one_raw_listing(page, raw)
            out.append(er)
            if er.detail_enriched:
                enriched_n += 1
        except Exception as exc:
            logger.warning(
                "Detail enrich failed user_id=%s url=%s: %s",
                user_id,
                raw.source_link,
                exc,
            )
            out.append(raw)

    screen_meta["detail_enriched_ok"] = enriched_n
    screen_meta["final_candidates"] = len(out)
    logger.info(
        "Step 1 detail-page enrich: user_id=%s attempted=%s enriched_ok=%s "
        "after_dedup_input=%s",
        user_id,
        min(len(pre_dedup), max_n),
        enriched_n,
        len(pre_dedup),
    )
    logger.info(
        "Step 1 final-candidates user_id=%s: "
        "collected=%s early_loc_rejected=%s unknown_loc=%s "
        "pre_enrich_dupes=%s detail_enriched=%s final_to_pipeline=%s",
        user_id,
        len(items),
        early_reject_n,
        unknown_n,
        known_dupes_n,
        enriched_n,
        len(out),
    )
    return out, screen_meta


async def _collect_marketplace_feed_for_query(
    page,
    *,
    collection_inputs: CollectionInputs,
    expected_query: str,
    submission_meta: dict | None,
    per_query_cap: int,
) -> tuple[list[RawListing], dict]:
    """
    Wait for results, harvest unique cards, scroll until idle or caps.

    Stops when: per-query cap reached, ``WORKER_COLLECTOR_SCROLL_IDLE_ROUNDS`` consecutive scroll
    rounds with no new unique cards, or ``WORKER_COLLECTOR_MAX_SCROLL_ROUNDS`` scroll rounds.
    """
    await page.wait_for_load_state("domcontentloaded")
    probe_name, probe_n = await wait_for_any_item_link(page, timeout_ms=25_000)
    if probe_name and probe_n:
        logger.info(
            "Marketplace parse: item links detected via wait probe selector=%s count=%s",
            probe_name,
            probe_n,
        )

    await ensure_marketplace_context(page, expected_query=expected_query)

    max_scroll_rounds = _int_env("WORKER_COLLECTOR_MAX_SCROLL_ROUNDS", 25)
    idle_limit = _int_env("WORKER_COLLECTOR_SCROLL_IDLE_ROUNDS", 3)
    meta: dict = {
        "scroll_rounds_executed": 0,
        "stopped_reason": None,
        "per_query_cap": per_query_cap,
    }

    out: list[RawListing] = []
    seen: set[str] = set()
    idle = 0
    strategy_name = "none"

    for round_idx in range(max_scroll_rounds):
        await ensure_marketplace_context(page, expected_query=expected_query)
        strategy_name, batch = await _harvest_visible_marketplace_cards(
            page, collection_inputs, max_items=None
        )

        if round_idx == 0 and not batch:
            reason = "selector_miss_or_empty_results"
            if submission_meta and submission_meta.get("item_links_probe"):
                ip = submission_meta["item_links_probe"]
                if isinstance(ip, dict) and not ip.get("selector"):
                    reason = "probe_failed_after_submit"
            await log_no_results_diagnostics(
                page,
                step_label="collect_marketplace_feed_for_query",
                expected_query=expected_query or "",
                submission_meta={**(submission_meta or {}), "parse_reason": reason},
            )
            logger.warning(
                "Marketplace collect: no harvestable cards on first pass strategy=%s query=%r",
                strategy_name,
                expected_query,
            )

        new_this = 0
        new_cards_this_round: list[RawListing] = []
        for r in batch:
            dk = _raw_dedupe_key(r)
            if dk in seen:
                continue
            seen.add(dk)
            out.append(r)
            new_cards_this_round.append(r)
            new_this += 1
            if len(out) >= per_query_cap:
                break

        if round_idx == 0:
            logger.info(
                "Step 1 query=%r cards_seen_before_scroll=%s (unique candidates)",
                expected_query,
                len(out),
            )
        else:
            logger.info(
                "Step 1 query=%r scroll_round=%s cards_seen_cumulative=%s new_this_round=%s strategy=%s",
                expected_query,
                round_idx,
                len(out),
                new_this,
                strategy_name,
            )
        meta["scroll_rounds_executed"] = round_idx

        if len(out) >= per_query_cap:
            meta["stopped_reason"] = "per_query_cap"
            break

        # Adaptive early stop: if every new card this round has a parsed location AND all are
        # outside the user's radius, further scrolling will almost certainly produce more of the
        # same off-target results — stop now to save time.
        # Use the same validity guards as _early_location_screen to avoid premature stops
        # caused by junk text that was never going to trigger a real rejection anyway.
        _feed_mode = (
            getattr(getattr(collection_inputs, "search_plan", None), "step1_collection_mode", None)
            == "category_feed"
        )
        if (
            round_idx >= 1
            and new_this >= 3
            and len(out) >= 3
        ):
            parsed_in_round = sum(
                1 for r in new_cards_this_round
                if _is_valid_visible_location((r.listing_location_parsed or "").strip())
                and (not _feed_mode or _is_high_confidence_city_state((r.listing_location_parsed or "").strip()))
            )
            if parsed_in_round == new_this:
                quick_rejects = _quick_location_reject_count(
                    new_cards_this_round,
                    collection_inputs,
                    category_feed_mode=_feed_mode,
                )
                if quick_rejects == new_this:
                    logger.info(
                        "Step 1 adaptive-stop query=%r round=%s: "
                        "all %s new cards have parsed locations and all are outside radius — "
                        "stopping scroll early (cumulative=%s)",
                        expected_query,
                        round_idx,
                        new_this,
                        len(out),
                    )
                    meta["stopped_reason"] = "adaptive_early_location_reject"
                    break

        if round_idx >= 1 and new_this == 0:
            idle += 1
            if idle >= idle_limit:
                meta["stopped_reason"] = "no_new_cards_after_scroll"
                break
        elif new_this > 0:
            idle = 0

        if round_idx >= max_scroll_rounds - 1:
            meta["stopped_reason"] = meta.get("stopped_reason") or "max_scroll_rounds"
            break

        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1300)

    if not meta.get("stopped_reason"):
        meta["stopped_reason"] = "completed_scroll_loop"

    logger.info(
        "Step 1 query=%r final_candidates_collected=%s stop_reason=%s strategy=%s",
        expected_query,
        len(out),
        meta.get("stopped_reason"),
        strategy_name,
    )
    return out, meta


async def fetch_listings_playwright(
    *,
    collection_inputs: CollectionInputs,
    backfill: bool,
) -> tuple[list[RawListing], dict]:
    """
    Collect listings: real Marketplace via UI filters + focused queries by default, or local stub.

    Returns ``(listings, collector_meta)``. ``collector_meta`` is empty for the stub; for Facebook it
    may include ``degraded_mode`` and ``worker_collector_warning`` for the worker status API.
    """
    try:
        from playwright.async_api import async_playwright
        from playwright._impl._errors import TargetClosedError
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("playwright is not installed") from exc

    auth_path = facebook_auth_state_path()
    if not auth_path.is_file():
        raise FacebookAuthStateMissingError(auth_path)

    use_stub = os.environ.get("COLLECTOR_USE_LOCAL_STUB", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    stub_path = Path(os.environ.get("COLLECTOR_STUB_HTML", str(_STUB))).resolve()
    if use_stub and not stub_path.is_file():
        raise FileNotFoundError(f"Collector stub not found: {stub_path}")

    headless = os.environ.get("PLAYWRIGHT_HEADLESS", "1").strip().lower() not in (
        "0",
        "false",
        "no",
    )

    plan = collection_inputs.search_plan
    logger.info(
        "Playwright collector starting (stub=%s headless=%s backfill=%s user_id=%s)",
        use_stub,
        headless,
        backfill,
        collection_inputs.user_id,
    )
    logger.info(
        "Step 1 search plan (exact) user_id=%s: %s",
        plan.user_id,
        plan.to_log_dict(),
    )

    if not use_stub:
        logger.info(
            "Step 1 strategy user_id=%s: path-only Marketplace entry + UI filters (location, radius, "
            "sort); category feed browse and/or keyword queries (search plan validated before Playwright).",
            plan.user_id,
        )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = None
        page = None
        try:
            context = await browser.new_context(storage_state=str(auth_path))
            page = await context.new_page()
            page.set_default_timeout(60_000)

            if use_stub:
                logger.info("Search target: local stub file %s (no Marketplace URL)", stub_path)
                out = await _parse_stub_page(
                    page,
                    collection_inputs=collection_inputs,
                    backfill=backfill,
                    stub_path=stub_path,
                )
                logger.info(
                    "Collector success: listings=%s source=playwright_stub user_id=%s",
                    len(out),
                    collection_inputs.user_id,
                )
                return out, {}
            else:
                    default_batch = (
                        _DEFAULT_WORKER_COLLECTOR_BATCH_CAP_BACKFILL
                        if backfill
                        else _DEFAULT_WORKER_COLLECTOR_BATCH_CAP_LIVE
                    )
                    # Category-feed live runs use a tighter default to reduce radius-reject waste.
                    # Priority: WORKER_COLLECTOR_BATCH_CAP (global) >
                    #           WORKER_COLLECTOR_CATEGORY_FEED_BATCH_CAP (feed-only) >
                    #           mode-specific built-in default.
                    _global_cap_env = os.environ.get("WORKER_COLLECTOR_BATCH_CAP", "").strip()
                    if _global_cap_env:
                        total_cap = _int_env("WORKER_COLLECTOR_BATCH_CAP", default_batch)
                    elif plan.step1_collection_mode == "category_feed" and not backfill:
                        total_cap = _int_env(
                            "WORKER_COLLECTOR_CATEGORY_FEED_BATCH_CAP",
                            _DEFAULT_WORKER_COLLECTOR_BATCH_CAP_CATEGORY_FEED_LIVE,
                        )
                    else:
                        total_cap = default_batch
                    per_query_cap = _int_env("WORKER_COLLECTOR_PER_QUERY_CAP", 100)
                    logger.info(
                        "Step 1 collector batch cap: max_listings=%s per run "
                        "(mode=%s backfill=%s; global_override=%s)",
                        total_cap,
                        plan.step1_collection_mode,
                        backfill,
                        bool(_global_cap_env),
                    )
                    merged: list[RawListing] = []
                    seen_keys: set[str] = set()

                    ui_applied: dict = {}
                    try:
                        ui_applied = await apply_marketplace_filters_ui(
                            page,
                            plan,
                            collection_inputs=collection_inputs,
                        )
                    except MarketplaceFilterError:
                        logger.exception(
                            "Marketplace UI filters failed user_id=%s (see exception)",
                            plan.user_id,
                        )
                        raise
                    logger.info(
                        "Step 1 UI filters applied (summary) user_id=%s: %s",
                        plan.user_id,
                        ui_applied,
                    )
                    if ui_applied.get("degraded_mode"):
                        logger.warning(
                            "Step 1 collector degraded user_id=%s (advanced filters partial or skipped); "
                            "continuing collection.",
                            plan.user_id,
                        )

                    if plan.step1_collection_mode == "category_feed":
                        fq_label = (
                            plan.marketplace_category_label
                            or plan.marketplace_category_slug
                            or "category"
                        )
                        logger.info(
                            "Step 1 category-feed browse user_id=%s label=%r (no search-box keyword blob)",
                            plan.user_id,
                            fq_label,
                        )
                        pq = min(per_query_cap, total_cap)
                        batch, scroll_meta = await _collect_marketplace_feed_for_query(
                            page,
                            collection_inputs=collection_inputs,
                            expected_query=f"(browse {fq_label})",
                            submission_meta=None,
                            per_query_cap=pq,
                        )
                        logger.info(
                            "Step 1 category feed scroll_meta=%s batch_cap=%s per_query_cap=%s",
                            scroll_meta,
                            total_cap,
                            pq,
                        )
                        for r in batch:
                            dk = _raw_dedupe_key(r)
                            if dk in seen_keys:
                                continue
                            seen_keys.add(dk)
                            merged.append(r)
                            if len(merged) >= total_cap:
                                break
                        out = merged[:total_cap]
                    else:
                        queries = [q.strip() for q in plan.focused_queries if q and str(q).strip()]
                        n_q = len(queries)
                        for idx, fq in enumerate(queries):
                            logger.info(
                                "Step 1 focused query %s/%s user_id=%s term=%r per_query_cap=%s batch_cap=%s",
                                idx + 1,
                                n_q,
                                plan.user_id,
                                fq,
                                per_query_cap,
                                total_cap,
                            )
                            try:
                                sub_meta = await run_focused_marketplace_query(page, fq)
                                logger.info(
                                    "Step 1 focused query submit user_id=%s term=%r meta=%s",
                                    plan.user_id,
                                    fq,
                                    sub_meta,
                                )
                            except MarketplaceFilterError:
                                logger.exception(
                                    "Focused query failed user_id=%s term=%r",
                                    plan.user_id,
                                    fq,
                                )
                                raise
                            batch, scroll_meta = await _collect_marketplace_feed_for_query(
                                page,
                                collection_inputs=collection_inputs,
                                expected_query=fq,
                                submission_meta=sub_meta,
                                per_query_cap=per_query_cap,
                            )
                            logger.info(
                                "Step 1 focused query %s/%s user_id=%s term=%r scroll_meta=%s",
                                idx + 1,
                                n_q,
                                plan.user_id,
                                fq,
                                scroll_meta,
                            )
                            cross_query_dedupe = 0
                            added = 0
                            for r in batch:
                                dk = _raw_dedupe_key(r)
                                if dk in seen_keys:
                                    cross_query_dedupe += 1
                                    continue
                                seen_keys.add(dk)
                                merged.append(r)
                                added += 1
                                if len(merged) >= total_cap:
                                    break
                            if cross_query_dedupe:
                                logger.info(
                                    "Step 1 cross-query dedupe skipped user_id=%s term=%r count=%s",
                                    plan.user_id,
                                    fq,
                                    cross_query_dedupe,
                                )
                            logger.info(
                                "Step 1 focused query %s/%s user_id=%s term=%r new_unique_added=%s merged_total=%s",
                                idx + 1,
                                n_q,
                                plan.user_id,
                                fq,
                                added,
                                len(merged),
                            )
                            if len(merged) >= total_cap:
                                break
                        out = merged[:total_cap]
                    out, enrich_meta = await _maybe_enrich_listings_from_detail_pages(
                        page,
                        out,
                        collection_inputs=collection_inputs,
                    )
                    logger.info(
                        "Step 1 enrich+screen summary user_id=%s: %s",
                        collection_inputs.user_id,
                        enrich_meta,
                    )
                    collector_meta = {
                        "degraded_mode": bool(ui_applied.get("degraded_mode")),
                        "worker_collector_warning": ui_applied.get("worker_collector_warning"),
                        # Surface date-listed filter status so callers can distinguish it from
                        # location_mismatch rejects (which come from Step 2 geo check, not the filter).
                        "date_listed_24h_selected": ui_applied.get("date_listed_24h_selected"),
                        # Enrich/screen summary for batch status reporting in main.py.
                        "screen_summary": enrich_meta,
                    }
                    logger.info(
                        "Collector success: listings=%s source=%s user_id=%s",
                        len(out),
                        "facebook_marketplace",
                        collection_inputs.user_id,
                    )
                    if len(out) == 0:
                        logger.warning(
                            "No listings parsed from Marketplace HTML — check auth state, "
                            "UI filter selectors, search results, or DOM changes."
                        )
                    return out, collector_meta
        except asyncio.CancelledError:
            logger.info(
                "Playwright collector: task cancelled during fetch (user_id=%s stub=%s)",
                collection_inputs.user_id,
                use_stub,
            )
            raise
        except Exception as exc:
            if isinstance(exc, TargetClosedError):
                logger.info(
                    "Playwright collector: target closed during fetch (user_id=%s stub=%s)",
                    collection_inputs.user_id,
                    use_stub,
                )
                raise CollectorInterruptedError(
                    "Browser or context closed during collection"
                ) from exc
            raise
        finally:
            try:
                await asyncio.shield(
                    _teardown_playwright_session(
                        browser=browser,
                        context=context,
                        page=page,
                        user_id=collection_inputs.user_id,
                        use_stub=use_stub,
                    )
                )
            except asyncio.CancelledError:
                logger.warning(
                    "Playwright cleanup: cancellation during shielded teardown; "
                    "best-effort close (user_id=%s)",
                    collection_inputs.user_id,
                )
                await _teardown_playwright_session(
                    browser=browser,
                    context=context,
                    page=page,
                    user_id=collection_inputs.user_id,
                    use_stub=use_stub,
                )
                raise
