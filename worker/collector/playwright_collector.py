"""
Facebook Marketplace collection via Playwright (async API).

Uses ``storage_state`` from ``backend/playwright/.auth/facebook.json`` (see
``facebook_login_bootstrap.py`` at repo root).

Modes:
- Default: navigate to Marketplace (path-only entry URL), apply filters in the UI, then run
  focused product queries in the search box (no weak ``?maxPrice=...``-only URLs).
- ``COLLECTOR_USE_LOCAL_STUB=1``: load local HTML (``COLLECTOR_STUB_HTML`` or bundled stub)
  for offline/CI — same DOM as before.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path

from mock_scraper import RawListing

from search_context import CollectionInputs
from search_plan import SearchPlanInvalidError, validate_search_plan_for_step1

from .marketplace_ui import (
    MarketplaceFilterError,
    apply_marketplace_filters_ui,
    run_focused_marketplace_query,
)

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_FACEBOOK_AUTH_STATE = (
    _REPO_ROOT / "backend" / "playwright" / ".auth" / "facebook.json"
)
_STUB = Path(__file__).resolve().parent / "static" / "marketplace_stub.html"

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
    """First non-trivial line is usually the title on Marketplace cards."""
    lines = [ln.strip() for ln in text.replace("\t", " ").splitlines() if ln.strip()]
    for ln in lines:
        if len(ln) < 3:
            continue
        # Skip lines that are only a price
        if _extract_price(ln) is not None and len(ln) <= 18:
            continue
        return ln[:500]
    cleaned = _PRICE_RE.sub("", text).strip()
    return cleaned.split("\n")[0][:500] if cleaned else ""


async def _parse_stub_page(
    page,
    *,
    collection_inputs: CollectionInputs,
    backfill: bool,
    stub_path: Path,
) -> list[RawListing]:
    uri = stub_path.as_uri()
    target_cat = (collection_inputs.category_id or "general").strip()
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


async def _parse_facebook_marketplace_page(
    page,
    *,
    collection_inputs: CollectionInputs,
    backfill: bool,
    max_items: int | None = None,
) -> list[RawListing]:
    """Parse search results: listing links and visible card text (price/title heuristics)."""
    await page.wait_for_load_state("domcontentloaded")
    try:
        await page.wait_for_selector('a[href*="/marketplace/item/"]', timeout=25_000)
    except Exception as exc:
        logger.warning("No marketplace item links found within timeout: %s", exc)

    links = await page.query_selector_all('a[href*="/marketplace/item/"]')
    if max_items is None:
        max_items = 50 if backfill else 15
    seen: set[str] = set()
    out: list[RawListing] = []
    cat = (collection_inputs.category_id or "general").strip()
    default_loc = (collection_inputs.primary_search_location or "Unknown").strip() or "Unknown"

    for link in links:
        if len(out) >= max_items:
            break
        href = await link.get_attribute("href")
        if not href:
            continue
        full = _normalize_fb_url(href)
        iid = _item_id_from_href(full)
        if not iid or full in seen:
            continue
        seen.add(full)
        text = (await link.inner_text() or "").strip()
        price = _extract_price(text)
        title = _title_from_card_text(text)
        if price is None or not title:
            continue
        out.append(
            RawListing(
                title=title,
                price=price,
                location=default_loc,
                category_slug=cat,
                source_link=full,
                source="facebook_marketplace",
                latitude=None,
                longitude=None,
                source_id=f"fb:{iid}",
            )
        )
    return out


async def fetch_listings_playwright(
    *,
    collection_inputs: CollectionInputs,
    backfill: bool,
) -> list[RawListing]:
    """
    Collect listings: real Marketplace via UI filters + focused queries by default, or local stub.
    """
    try:
        from playwright.async_api import async_playwright
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
        try:
            validate_search_plan_for_step1(plan)
        except SearchPlanInvalidError:
            logger.exception("Invalid search plan for Step 1 (user_id=%s)", plan.user_id)
            raise
        logger.info(
            "Step 1 strategy user_id=%s: path-only Marketplace entry + UI filters (location, radius, "
            "max price, sort) then focused search-box queries — no ?maxPrice= / ?sortBy= URL filter strings.",
            plan.user_id,
        )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        try:
            context = await browser.new_context(storage_state=str(auth_path))
            try:
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
                else:
                    queries = [q.strip() for q in plan.focused_queries if q and str(q).strip()]
                    n_q = len(queries)
                    total_cap = 50 if backfill else 15
                    per_cap = max(1, (total_cap + n_q - 1) // n_q)
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

                    for idx, fq in enumerate(queries):
                        logger.info(
                            "Step 1 focused query %s/%s user_id=%s term=%r per_search_cap=%s",
                            idx + 1,
                            n_q,
                            plan.user_id,
                            fq,
                            per_cap,
                        )
                        try:
                            await run_focused_marketplace_query(page, fq)
                        except MarketplaceFilterError:
                            logger.exception(
                                "Focused query failed user_id=%s term=%r",
                                plan.user_id,
                                fq,
                            )
                            raise
                        batch = await _parse_facebook_marketplace_page(
                            page,
                            collection_inputs=collection_inputs,
                            backfill=backfill,
                            max_items=per_cap,
                        )
                        logger.info(
                            "Step 1 focused query %s/%s user_id=%s term=%r cards_collected=%s (pre-dedupe)",
                            idx + 1,
                            n_q,
                            plan.user_id,
                            fq,
                            len(batch),
                        )
                        for r in batch:
                            dk = _raw_dedupe_key(r)
                            if dk in seen_keys:
                                continue
                            seen_keys.add(dk)
                            merged.append(r)
                            if len(merged) >= total_cap:
                                break
                        if len(merged) >= total_cap:
                            break
                    out = merged[:total_cap]
                logger.info(
                    "Listings found: %s (source=%s)",
                    len(out),
                    "playwright_stub" if use_stub else "facebook_marketplace",
                )
                if not use_stub and len(out) == 0:
                    logger.warning(
                        "No listings parsed from Marketplace HTML — check auth state, "
                        "UI filter selectors, search results, or DOM changes."
                    )
                return out
            finally:
                await context.close()
        finally:
            await browser.close()
