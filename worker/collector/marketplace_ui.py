"""
Facebook Marketplace: apply built-in filters via the logged-in web UI (Playwright async).

Facebook changes markup frequently; selectors use roles/labels with fallbacks. Location and radius
are required for a meaningful search. The Filters drawer (sort, and Date listed in category mode
only) is best-effort: if it cannot be opened or controls fail, the collector continues with the
category feed or keyword queries using location/radius and category path. No user-configurable price
cap is applied in Marketplace UI.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Callable

from search_context import CollectionInputs
from search_plan import (
    MARKETPLACE_SORT_UI_LABEL,
    SearchPlan,
    build_marketplace_entry_url,
)

from .marketplace_dom import (
    is_facebook_marketplace_url,
    marketplace_search_results_url,
    wait_for_any_item_link,
)

logger = logging.getLogger(__name__)


class MarketplaceFilterError(RuntimeError):
    """Raised when a required Marketplace step could not be completed (e.g. location or search box)."""


# US Marketplace radius options commonly seen in the location dialog (miles).
_FB_RADIUS_MILES_OPTIONS: tuple[int, ...] = (1, 2, 5, 10, 20, 40, 65, 100, 250, 500)


def _snap_radius_miles(radius_miles: float) -> int:
    r = max(0.0, float(radius_miles))
    for opt in _FB_RADIUS_MILES_OPTIONS:
        if opt >= r:
            return opt
    return _FB_RADIUS_MILES_OPTIONS[-1]


def _sort_label_for_plan(plan: SearchPlan) -> str:
    label = MARKETPLACE_SORT_UI_LABEL.get(plan.sort_mode)
    if not label:
        raise MarketplaceFilterError(
            f"Unsupported sort_mode={plan.sort_mode!r}; add a MARKETPLACE_SORT_UI_LABEL entry."
        )
    return label


async def _wait_for_marketplace_shell(page) -> None:
    await page.wait_for_load_state("domcontentloaded")
    # Feed / main landmark — avoid blocking on networkidle (Facebook is chatty).
    try:
        await page.wait_for_selector(
            'main, [role="main"], [role="feed"], div[role="article"]',
            timeout=30_000,
        )
    except Exception as exc:
        raise MarketplaceFilterError(
            "Marketplace page did not load a recognizable layout (main/feed)."
        ) from exc


async def _click_first_visible(page, locator, *, step: str, timeout: int = 15_000) -> None:
    loc = locator.first
    await loc.wait_for(state="visible", timeout=timeout)
    await loc.click()
    logger.info("Marketplace UI: clicked %s", step)


async def _open_location_dialog(page, plan: SearchPlan) -> None:
    """Open the location / radius control (header, not the Filters drawer)."""
    loc_text = (plan.location_text or "").strip()
    r_mi = _snap_radius_miles(plan.radius_miles)
    # Try: chip showing miles, or explicit Location control.
    candidates = [
        # Current location + radius summary (common pattern).
        page.get_by_role("button").filter(has_text=re.compile(r"\d+\s*(mi|miles|km)", re.I)),
        page.locator('[aria-label*="Location"]').filter(has_text=re.compile(r".+", re.I)),
        page.get_by_role("button", name=re.compile(r"location|radius|distance", re.I)),
    ]
    last_exc: Exception | None = None
    for loc in candidates:
        try:
            if await loc.count() < 1:
                continue
            await _click_first_visible(page, loc, step="location/radius control")
            await page.wait_for_timeout(500)
            return
        except Exception as exc:
            last_exc = exc
            continue
    raise MarketplaceFilterError(
        f"Could not open Marketplace location / radius UI for {loc_text!r} (~{r_mi} mi). "
        f"Last error: {last_exc!r}"
    ) from last_exc


async def _fill_location_and_radius_in_dialog(page, plan: SearchPlan) -> None:
    loc_text = (plan.location_text or "").strip()
    r_mi = _snap_radius_miles(plan.radius_miles)
    # Prefer the open dialog so we do not fill the main Marketplace search bar.
    dialog = page.locator('[role="dialog"]').first
    roots = [dialog, page]
    search_box = None
    for root in roots:
        try:
            if root is not page and await root.count() < 1:
                continue
        except Exception:
            continue
        for loc in (
            root.get_by_role("combobox", name=re.compile(r"location|city|area", re.I)),
            root.locator('input[placeholder*="City" i]'),
            root.locator('input[placeholder*="location" i]'),
            root.locator('input[aria-label*="location" i]'),
        ):
            try:
                if await loc.count() < 1:
                    continue
                el = loc.first
                await el.wait_for(state="visible", timeout=12_000)
                search_box = el
                break
            except Exception:
                continue
        if search_box is not None:
            break
    if search_box is None:
        raise MarketplaceFilterError("Location dialog open but no location search input found.")

    await search_box.fill("")
    await search_box.fill(loc_text)
    await search_box.press("Enter")
    await page.wait_for_timeout(1200)
    # Pick first suggestion if listbox appears.
    try:
        opt = page.get_by_role("option").first
        if await opt.count():
            await opt.click()
            await page.wait_for_timeout(500)
    except Exception:
        pass

    # Radius: button or combobox with mile options.
    radius_patterns = [
        page.get_by_role("button", name=re.compile(rf"^{r_mi}\s*mi", re.I)),
        page.get_by_role("option", name=re.compile(rf"{r_mi}\s*miles?", re.I)),
        page.get_by_text(re.compile(rf"^{r_mi}\s*miles?$", re.I)),
        page.get_by_role("combobox", name=re.compile(r"radius|distance", re.I)),
    ]
    applied = False
    for loc in radius_patterns:
        try:
            if await loc.count() < 1:
                continue
            await loc.first.click()
            await page.wait_for_timeout(400)
            # If combobox, pick option from list.
            opt = page.get_by_role("option", name=re.compile(rf"{r_mi}", re.I))
            if await opt.count():
                await opt.first.click()
            applied = True
            break
        except Exception:
            continue
    if not applied:
        raise MarketplaceFilterError(
            f"Could not set radius to ~{plan.radius_miles} mi (snap {r_mi} mi) in the location UI."
        )

    # Close dialog: Apply / Done / Save.
    for close in (
        page.get_by_role("button", name=re.compile(r"^apply$|^done$|^save$|^update$", re.I)),
        page.get_by_role("button", name=re.compile(r"apply", re.I)),
    ):
        try:
            if await close.count():
                await close.first.click()
                await page.wait_for_timeout(600)
                break
        except Exception:
            continue
    await page.keyboard.press("Escape")
    await page.wait_for_timeout(400)


async def _left_sidebar_filters_visible(page) -> tuple[bool, str]:
    """
    Desktop category pages often show Filters + Date listed in the main column without a drawer.
    """
    main = page.locator('[role="main"]')
    if await main.count() < 1:
        return False, "no_main_landmark"
    try:
        dl = main.get_by_text(re.compile(r"date\s*listed", re.I)).first
        if await dl.count() > 0 and await dl.is_visible():
            return True, "date_listed_visible_in_main"
    except Exception:
        pass
    try:
        aside = main.locator("aside").first
        if await aside.count() > 0 and await aside.is_visible():
            txt = (await aside.inner_text() or "")[:4000]
            if re.search(r"date\s*listed", txt, re.I) or re.search(
                r"\b(filter|sort|categories)\b", txt, re.I
            ):
                return True, "aside_sidebar_visible"
    except Exception:
        pass
    try:
        col = main.locator('[data-pagelet*="Filter" i], [data-testid*="filter" i]').first
        if await col.count() > 0 and await col.is_visible():
            return True, "filter_container_visible"
    except Exception:
        pass
    return False, "left_sidebar_filters_not_detected"


async def _filters_panel_looks_open(page) -> bool:
    """Heuristic: visible dialog that looks like Marketplace filters (not only location)."""
    dialog = page.locator('[role="dialog"]').first
    if await dialog.count() < 1:
        return False
    try:
        await dialog.wait_for(state="visible", timeout=4000)
    except Exception:
        return False
    for loc in (
        dialog.get_by_text(re.compile(r"\b(sort|price|max|condition|category|delivery|date)\b", re.I)),
        dialog.get_by_label(re.compile(r"max", re.I)),
        dialog.locator('input[inputmode="numeric"]'),
        dialog.get_by_role("combobox", name=re.compile(r"sort", re.I)),
    ):
        try:
            if await loc.count() > 0:
                return True
        except Exception:
            continue
    return False


def _iter_filters_open_locators(page) -> list[tuple[str, Callable[[], Any]]]:
    """Named selector strategies for opening the Filters UI (order matters: try specific first)."""
    return [
        (
            "role_button_name_anchors_filters",
            lambda: page.get_by_role("button", name=re.compile(r"^filters?$", re.I)),
        ),
        (
            "role_button_name_contains_filter",
            lambda: page.get_by_role("button", name=re.compile(r"filter", re.I)),
        ),
        ("role_tab_filter", lambda: page.get_by_role("tab", name=re.compile(r"filter", re.I))),
        (
            "aria_label_filter",
            lambda: page.locator("[aria-label*='Filter' i], [aria-label*='filter' i]").first,
        ),
        (
            "text_filters_exact_line",
            lambda: page.get_by_text(re.compile(r"^filters?$", re.I)),
        ),
        (
            "div_role_button_filter_text",
            lambda: page.locator('div[role="button"]').filter(
                has_text=re.compile(r"filters?", re.I)
            ),
        ),
        (
            "header_toolbar_filter",
            lambda: page.locator('header button, [role="banner"] button').filter(
                has_text=re.compile(r"filter", re.I)
            ),
        ),
        (
            "span_role_filter",
            lambda: page.locator('span[role="button"], span[role="tab"]').filter(
                has_text=re.compile(r"^filters?$", re.I)
            ),
        ),
        (
            "all_filters_text",
            lambda: page.get_by_text(re.compile(r"\ball\s+filters?\b", re.I)),
        ),
    ]


async def _try_open_filters_drawer(page) -> dict[str, Any]:
    """
    Try to open the Filters drawer/panel. Does not raise.

    Returns keys: opened, already_visible, selector_used, attempt_log (list[str]),
    surface (optional: left_sidebar | filters_dialog).
    """
    attempt_log: list[str] = []

    if await _filters_panel_looks_open(page):
        logger.info(
            "Marketplace UI: Filters panel already visible (skipping open click)"
        )
        return {
            "opened": False,
            "already_visible": True,
            "selector_used": None,
            "attempt_log": ["already_visible: dialog matched filters heuristics"],
            "surface": "filters_dialog",
        }

    left_ok, left_reason = await _left_sidebar_filters_visible(page)
    if left_ok:
        logger.info(
            "Marketplace UI: left-side filter section visible (%s) — drawer not required",
            left_reason,
        )
        return {
            "opened": False,
            "already_visible": True,
            "selector_used": left_reason,
            "attempt_log": [f"left_sidebar_filters_visible: {left_reason}"],
            "surface": "left_sidebar",
        }

    selector_used: str | None = None
    for name, factory in _iter_filters_open_locators(page):
        loc = factory()
        try:
            n = await loc.count()
            if n < 1:
                attempt_log.append(f"{name}: no elements matched (count=0)")
                continue
        except Exception as exc:
            attempt_log.append(f"{name}: count() failed: {type(exc).__name__}: {exc!r}")
            continue
        try:
            await _click_first_visible(page, loc, step=f"Filters ({name})")
            await page.wait_for_timeout(700)
            if await _filters_panel_looks_open(page):
                selector_used = name
                logger.info(
                    "Marketplace UI: Filters drawer opened via selector=%s",
                    name,
                )
                return {
                    "opened": True,
                    "already_visible": False,
                    "selector_used": name,
                    "attempt_log": attempt_log + [f"{name}: click succeeded; panel visible"],
                    "surface": "filters_dialog",
                }
            attempt_log.append(
                f"{name}: clicked but filters panel heuristics not visible afterward"
            )
        except Exception as exc:
            attempt_log.append(f"{name}: {type(exc).__name__}: {exc!r}")

    # Nothing worked: summarize (never None-only "Last error")
    summary = (
        "; ".join(attempt_log)
        if attempt_log
        else "internal: no selector entries produced attempt_log (unexpected)"
    )
    logger.warning(
        "Marketplace UI: could not open Filters drawer; attempts=%s",
        summary[:1200],
    )
    return {
        "opened": False,
        "already_visible": False,
        "selector_used": None,
        "attempt_log": attempt_log,
        "failure_summary": summary,
    }


async def _resolve_filters_root(page, drawer_info: dict[str, Any]) -> tuple[Any, str]:
    """Prefer left main column when category page shows inline filters; else Filters dialog."""
    surf = drawer_info.get("surface")
    if surf == "left_sidebar":
        logger.info("Marketplace UI: filters root=left main column (inline sidebar)")
        return page.locator('[role="main"]'), "left_sidebar_main"
    if await _filters_panel_looks_open(page):
        logger.info("Marketplace UI: filters root=dialog overlay")
        return page.locator('[role="dialog"]').first, "filters_dialog"
    if drawer_info.get("opened"):
        logger.info("Marketplace UI: filters root=dialog (opened via UI click)")
        return page.locator('[role="dialog"]').first, "filters_dialog_opened"
    return page.locator('[role="dialog"]').first, "dialog_fallback"


async def _verify_date_listed_24h_applied(page, root) -> tuple[bool, str]:
    """Best-effort confirmation that a 24h-style label is visible after selection."""
    for rx in (
        re.compile(r"last\s*24\s*hours?", re.I),
        re.compile(r"past\s*24\s*hours?", re.I),
        re.compile(r"^\s*24\s*hours?\s*$", re.I),
        re.compile(r"past\s*day", re.I),
    ):
        try:
            loc = root.get_by_text(rx).first
            if await loc.count() and await loc.is_visible():
                return True, f"visible_label:{rx.pattern}"
        except Exception:
            continue
    try:
        page_loc = page.get_by_text(
            re.compile(r"last\s*24\s*hours?|past\s*24|24\s*hours?", re.I)
        ).first
        if await page_loc.count() and await page_loc.is_visible():
            return True, "visible_label:page_fallback"
    except Exception:
        pass
    return False, "no_confirmation_label_visible"


async def _set_date_listed_to_24_hours(page, root, *, surface_label: str) -> None:
    """
    Set Date listed → Last 24 hours (or closest label) within ``root`` (dialog or main).

    Facebook labels vary (\"24 hours\", \"Last 24 hours\", \"Past day\"); we try several.
    """
    try:
        await root.wait_for(state="visible", timeout=8000)
    except Exception as exc:
        raise MarketplaceFilterError(
            f"Filters region not visible ({surface_label})."
        ) from exc

    date_labels = [
        re.compile(r"date\s*listed", re.I),
        re.compile(r"listed\s*date", re.I),
        re.compile(r"\bdate\b.*\blisted\b", re.I),
    ]
    # Open the Date listed control (combobox, button, or row).
    opened = False
    for pat in date_labels:
        for loc in (
            root.get_by_role("combobox", name=pat),
            root.get_by_role("button", name=pat),
            root.get_by_text(pat),
        ):
            try:
                if await loc.count() < 1:
                    continue
                el = loc.first
                await el.scroll_into_view_if_needed()
                await el.click()
                await page.wait_for_timeout(450)
                opened = True
                break
            except Exception:
                continue
        if opened:
            break

    if not opened:
        # Broader: any combobox near "date" text in root
        try:
            rows = root.locator("div[role='button'], button, [role='combobox'], span[role='button']")
            n = await rows.count()
            for i in range(min(n, 60)):
                el = rows.nth(i)
                t = (await el.inner_text() or "").strip()
                if re.search(r"date", t, re.I) and re.search(r"list", t, re.I):
                    await el.scroll_into_view_if_needed()
                    await el.click()
                    await page.wait_for_timeout(450)
                    opened = True
                    break
        except Exception:
            pass

    if not opened:
        raise MarketplaceFilterError(
            f"Date listed control not found in filters region ({surface_label})."
        )

    # Choose a 24-hour option (menu / listbox / dialog).
    hour_patterns = [
        re.compile(r"^\s*24\s*hours?\s*$", re.I),
        re.compile(r"last\s*24\s*hours?", re.I),
        re.compile(r"past\s*24\s*hours?", re.I),
        re.compile(r"past\s*day", re.I),
        re.compile(r"24\s*h\b", re.I),
        re.compile(r"today|last\s*day", re.I),
    ]
    for rx in hour_patterns:
        for loc in (
            page.get_by_role("option", name=rx),
            page.get_by_role("menuitem", name=rx),
            root.get_by_role("option", name=rx),
            page.get_by_text(rx),
        ):
            try:
                if await loc.count() < 1:
                    continue
                await loc.first.click()
                await page.wait_for_timeout(400)
                logger.info(
                    "Marketplace UI: Date listed option matched pattern=%s surface=%s",
                    rx.pattern,
                    surface_label,
                )
                return
            except Exception:
                continue

    raise MarketplaceFilterError(
        f"Could not select a 24-hour Date listed option ({surface_label})."
    )


async def _set_sort_in_filters(page, plan: SearchPlan, root) -> None:
    label = _sort_label_for_plan(plan)
    try:
        await root.wait_for(state="visible", timeout=8000)
    except Exception as exc:
        raise MarketplaceFilterError("Filters region missing for sort.") from exc

    # Sort combobox or list.
    for loc in (
        root.get_by_role("combobox", name=re.compile(r"sort", re.I)),
        root.get_by_label(re.compile(r"sort", re.I)),
    ):
        try:
            if await loc.count() < 1:
                continue
            await loc.first.click()
            await page.wait_for_timeout(400)
            opt = page.get_by_role("option", name=re.compile(re.escape(label), re.I))
            if await opt.count():
                await opt.first.click()
                await page.wait_for_timeout(400)
                logger.info("Marketplace UI: sort set to %r", label)
                return
        except Exception:
            continue
    raise MarketplaceFilterError(f"Could not set sort to {label!r} in Filters.")


async def _apply_filters_confirm(page) -> bool:
    """Click Apply / See results when a modal Filters dialog is open; no-op for inline sidebar."""
    dialog = page.locator('[role="dialog"]').first
    if await dialog.count() < 1:
        logger.info(
            "Marketplace UI: no Filters dialog — skipping Apply (inline / left-column filters)"
        )
        return False
    try:
        vis = await dialog.is_visible()
    except Exception:
        vis = False
    if not vis:
        logger.info("Marketplace UI: Filters dialog not visible — skipping Apply click")
        return False
    for btn in (
        page.get_by_role("button", name=re.compile(r"apply", re.I)),
        page.get_by_role("button", name=re.compile(r"see.*items|show.*results|show.*listings", re.I)),
    ):
        try:
            if await btn.count():
                await btn.first.click()
                await page.wait_for_timeout(800)
                return True
        except Exception:
            continue
    logger.warning(
        "Marketplace UI: no Apply / See results button in Filters dialog (continuing)"
    )
    return False


async def ensure_marketplace_context(page, *, expected_query: str) -> dict[str, Any]:
    """
    If navigation left Marketplace (e.g. global search), recover to canonical Marketplace search URL.
    """
    meta: dict[str, Any] = {
        "recovered": False,
        "url_before": page.url,
        "url_after": page.url,
    }
    if is_facebook_marketplace_url(page.url):
        return meta
    direct = marketplace_search_results_url(expected_query)
    logger.warning(
        "Marketplace UI: page left Marketplace — recovering query=%r from url=%r to %r",
        expected_query,
        page.url,
        direct,
    )
    await page.goto(direct, wait_until="domcontentloaded")
    await page.wait_for_timeout(900)
    meta["recovered"] = True
    meta["url_after"] = page.url
    return meta


async def run_focused_marketplace_query(page, query: str) -> dict[str, Any]:
    """
    Run a focused Marketplace search after filters are applied.

    Prefer the search control inside ``[role="main"]`` so we do not submit the global header
    search (which leaves Marketplace and yields no ``/marketplace/item/`` links).

    If the UI does not navigate to a Marketplace search URL and no result links appear quickly,
    fall back to the canonical ``/marketplace/search/?query=...`` URL (location/radius still apply
    from the session; this is not a hand-built price/sort URL).
    """
    q = (query or "").strip()
    if not q:
        raise MarketplaceFilterError("Internal error: empty focused query.")

    pre_url = page.url
    meta: dict[str, Any] = {
        "query": q,
        "pre_submit_url": pre_url,
        "post_submit_url": None,
        "page_title_after": None,
        "submission_method": None,
        "search_scope_used": None,
        "fallback_direct_search_url": None,
        "item_links_probe": None,
    }

    # Order: Marketplace main first (avoid global header search combobox).
    main = page.locator('[role="main"]')
    search_candidates: list[tuple[str, Any]] = [
        ("main_combobox_search", main.get_by_role("combobox", name=re.compile(r"search", re.I))),
        (
            "main_input_search",
            main.locator('input[type="search"], input[placeholder*="Search" i]'),
        ),
        (
            "main_placeholder_marketplace",
            main.get_by_placeholder(re.compile(r"marketplace|search|buy", re.I)),
        ),
        (
            "page_combobox_search",
            page.get_by_role("combobox", name=re.compile(r"search", re.I)),
        ),
        (
            "page_placeholder",
            page.get_by_placeholder(re.compile(r"marketplace|search", re.I)),
        ),
        ("page_input_search", page.locator('input[type="search"]')),
        ("page_input_placeholder_search", page.locator('input[placeholder*="Search" i]')),
    ]

    last_exc: Exception | None = None
    for scope_name, loc in search_candidates:
        try:
            if await loc.count() < 1:
                continue
            el = loc.first
            await el.wait_for(state="visible", timeout=12_000)
            await el.click()
            await el.fill("")
            await el.fill(q)
            await el.press("Enter")
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(900)
            post = page.url
            meta["post_submit_url"] = post
            try:
                meta["page_title_after"] = await page.title()
            except Exception:
                pass
            meta["submission_method"] = "ui_search_box"
            meta["search_scope_used"] = scope_name
            logger.info(
                "Marketplace UI: focused query submitted scope=%s query=%r url_after=%r title=%r",
                scope_name,
                q,
                post,
                meta.get("page_title_after"),
            )

            probe_name, probe_count = await wait_for_any_item_link(page, timeout_ms=12_000)
            meta["item_links_probe"] = {"selector": probe_name, "count": probe_count}
            if probe_name and probe_count:
                logger.info(
                    "Marketplace UI: search verified (item links visible) scope=%s selector=%s count=%s",
                    scope_name,
                    probe_name,
                    probe_count,
                )
                meta["marketplace_context"] = await ensure_marketplace_context(page, expected_query=q)
                return meta

            # UI may have used header search or SPA did not update URL — try canonical search URL.
            direct = marketplace_search_results_url(q)
            meta["fallback_direct_search_url"] = direct
            logger.warning(
                "Marketplace UI: no item links after UI submit; trying direct search URL query=%r "
                "url_before_fallback=%r probe=%s",
                q,
                page.url,
                meta["item_links_probe"],
            )
            await page.goto(direct, wait_until="domcontentloaded")
            await page.wait_for_timeout(900)
            meta["post_submit_url"] = page.url
            try:
                meta["page_title_after"] = await page.title()
            except Exception:
                pass
            meta["submission_method"] = "direct_marketplace_search_url"
            probe_name, probe_count = await wait_for_any_item_link(page, timeout_ms=15_000)
            meta["item_links_probe"] = {"selector": probe_name, "count": probe_count}
            logger.info(
                "Marketplace UI: direct search URL loaded query=%r url_after=%r title=%r probe=%s",
                q,
                page.url,
                meta.get("page_title_after"),
                meta["item_links_probe"],
            )
            meta["marketplace_context"] = await ensure_marketplace_context(page, expected_query=q)
            return meta
        except Exception as exc:
            last_exc = exc
            continue

    # No search box worked — still try direct Marketplace search (session location may apply).
    direct = marketplace_search_results_url(q)
    meta["fallback_direct_search_url"] = direct
    meta["submission_method"] = "direct_marketplace_search_url_no_input"
    logger.warning(
        "Marketplace UI: no search input matched; navigating to direct search URL query=%r last_err=%r",
        q,
        last_exc,
    )
    await page.goto(direct, wait_until="domcontentloaded")
    await page.wait_for_timeout(900)
    meta["post_submit_url"] = page.url
    try:
        meta["page_title_after"] = await page.title()
    except Exception:
        pass
    probe_name, probe_count = await wait_for_any_item_link(page, timeout_ms=18_000)
    meta["item_links_probe"] = {"selector": probe_name, "count": probe_count}
    logger.info(
        "Marketplace UI: direct search only query=%r url_after=%r probe=%s",
        q,
        page.url,
        meta["item_links_probe"],
    )
    meta["marketplace_context"] = await ensure_marketplace_context(page, expected_query=q)
    return meta


async def apply_marketplace_filters_ui(
    page,
    plan: SearchPlan,
    *,
    collection_inputs: CollectionInputs,
) -> dict[str, Any]:
    """
    Navigate to Marketplace (category path only), then set location, radius, and sort via UI.

    Category context: path-only ``/marketplace/category/{slug}/`` when ``plan.marketplace_category_slug`` is set; otherwise ``/marketplace/``.
    """
    entry_url = build_marketplace_entry_url(plan)
    logger.info(
        "Marketplace UI: navigating to entry (path-only, no filter query string) user_id=%s url=%s",
        plan.user_id,
        entry_url,
    )
    await page.goto(entry_url, wait_until="domcontentloaded")
    await _wait_for_marketplace_shell(page)
    try:
        _title = await page.title()
    except Exception:
        _title = ""
    logger.info(
        "Marketplace UI: category_page_entered user_id=%s url=%s title=%r",
        plan.user_id,
        page.url,
        _title,
    )

    applied: dict[str, Any] = {
        "entry_url": entry_url,
        "category_slug": plan.marketplace_category_slug,
        "location_text": (plan.location_text or "").strip(),
        "radius_miles_requested": round(plan.radius_miles, 2),
        "radius_miles_snapped": _snap_radius_miles(plan.radius_miles),
        "sort_mode": plan.sort_mode,
        "sort_ui_label": _sort_label_for_plan(plan),
        "location_radius_ok": False,
        "filters_drawer_opened": False,
        "filters_drawer_already_visible": False,
        "filters_drawer_selector": None,
        "sort_ui": "not_attempted",
        "filters_confirmed": False,
        "degraded_mode": False,
        "worker_collector_warning": None,
        "date_listed_filter_attempted": False,
        "date_listed_24h_selected": False,
        "date_listed_error": None,
        "date_listed_skipped_reason": None,
        "date_listed_applied_and_panel_confirmed": False,
        "filters_surface": None,
        "date_listed_verify_ok": False,
        "date_listed_verify_detail": None,
        "filters_confirmed": False,
    }

    # Location + radius (required for meaningful geo-scoped search).
    await _open_location_dialog(page, plan)
    await _fill_location_and_radius_in_dialog(page, plan)
    applied["location_radius_ui"] = "applied"
    applied["location_radius_ok"] = True
    logger.info(
        "Marketplace UI: location/radius OK user_id=%s primary=%r snapped=%s mi",
        plan.user_id,
        collection_inputs.primary_search_location,
        applied["radius_miles_snapped"],
    )

    ls_vis, ls_reason = await _left_sidebar_filters_visible(page)
    logger.info(
        "Marketplace UI: after_location left_sidebar_filter_section user_id=%s visible=%s detail=%s",
        plan.user_id,
        ls_vis,
        ls_reason,
    )

    # Price + sort (Filters drawer) — best-effort.
    drawer_info = await _try_open_filters_drawer(page)
    applied["filters_drawer_opened"] = bool(
        drawer_info.get("opened") or drawer_info.get("already_visible")
    )
    applied["filters_drawer_already_visible"] = bool(drawer_info.get("already_visible"))
    applied["filters_drawer_selector"] = drawer_info.get("selector_used")
    applied["filters_drawer_attempts"] = drawer_info.get("attempt_log", [])
    applied["filters_surface"] = drawer_info.get("surface")

    if not applied["filters_drawer_opened"]:
        applied["degraded_mode"] = True
        applied["sort_ui"] = "skipped_no_drawer"
        if (plan.search_mode or "").strip() == "marketplace_category":
            applied["date_listed_skipped_reason"] = "filters_drawer_not_open"
        fs = drawer_info.get("failure_summary") or (
            "; ".join(drawer_info.get("attempt_log") or []) or "unknown"
        )
        applied["worker_collector_warning"] = (
            "Marketplace sort filter was skipped: Filters drawer did not open. "
            f"Details: {fs[:400]}"
        )
        logger.warning(
            "Marketplace UI: degraded mode user_id=%s — continuing with location/radius + category entry only. %s",
            plan.user_id,
            applied["worker_collector_warning"][:500],
        )
    else:
        filters_root, surface_label = await _resolve_filters_root(page, drawer_info)
        applied["filters_surface"] = surface_label
        logger.info(
            "Marketplace UI: filters_root_ready user_id=%s surface=%s drawer_info=%s",
            plan.user_id,
            surface_label,
            {
                "opened": drawer_info.get("opened"),
                "already_visible": drawer_info.get("already_visible"),
                "surface": drawer_info.get("surface"),
            },
        )

        # Date listed = 24 hours — only in marketplace_category mode (not custom keyword searches).
        if (plan.search_mode or "").strip() == "marketplace_category":
            applied["date_listed_filter_attempted"] = True
            logger.info(
                "Marketplace UI: Date listed filter (24h) attempting user_id=%s mode=%s surface=%s",
                plan.user_id,
                plan.search_mode,
                surface_label,
            )
            try:
                await _set_date_listed_to_24_hours(page, filters_root, surface_label=surface_label)
                applied["date_listed_24h_selected"] = True
                ok, detail = await _verify_date_listed_24h_applied(page, filters_root)
                applied["date_listed_verify_ok"] = ok
                applied["date_listed_verify_detail"] = detail
                logger.info(
                    "Marketplace UI: Date listed 24h click done user_id=%s verify_ok=%s verify_detail=%s",
                    plan.user_id,
                    ok,
                    detail,
                )
            except Exception as exc:
                applied["date_listed_error"] = f"{type(exc).__name__}: {str(exc)[:400]}"
                applied["degraded_mode"] = True
                logger.warning(
                    "Marketplace UI: Date listed 24h not applied user_id=%s surface=%s reason=%s",
                    plan.user_id,
                    surface_label,
                    exc,
                    exc_info=True,
                )
                prev = (applied.get("worker_collector_warning") or "").strip()
                extra = (
                    "Date listed filter (24 hours) could not be applied. "
                    f"Surface={surface_label}. Details: {applied['date_listed_error'][:280]}"
                )
                applied["worker_collector_warning"] = (
                    f"{prev} | {extra}" if prev else extra
                )
        else:
            applied["date_listed_skipped_reason"] = "not_marketplace_category_mode"
            logger.info(
                "Marketplace UI: Date listed filter skipped (search_mode=%s) user_id=%s",
                plan.search_mode,
                plan.user_id,
            )

        # Sort (Filters region: dialog or left column)
        try:
            await _set_sort_in_filters(page, plan, filters_root)
            applied["sort_ui"] = "applied"
        except Exception as exc:
            applied["sort_ui"] = f"skipped: {type(exc).__name__}: {exc!r}"
            applied["degraded_mode"] = True
            logger.warning(
                "Marketplace UI: sort not applied user_id=%s surface=%s: %s",
                plan.user_id,
                surface_label,
                exc,
                exc_info=True,
            )

        # Confirm / apply filters panel (modal only)
        try:
            confirmed = await _apply_filters_confirm(page)
            if not confirmed and surface_label == "left_sidebar_main":
                confirmed = True
                logger.info(
                    "Marketplace UI: treating filters as applied (inline left sidebar; no modal Apply) user_id=%s",
                    plan.user_id,
                )
            applied["filters_confirmed"] = bool(confirmed)
        except Exception as exc:
            applied["filters_confirmed"] = False
            applied["degraded_mode"] = True
            logger.warning(
                "Marketplace UI: Filters confirm failed user_id=%s: %s",
                plan.user_id,
                exc,
                exc_info=True,
            )

        applied["date_listed_applied_and_panel_confirmed"] = bool(
            applied.get("date_listed_24h_selected") and applied.get("filters_confirmed")
        )
        if applied.get("date_listed_filter_attempted") and applied.get("date_listed_24h_selected"):
            logger.info(
                "Marketplace UI: Date listed 24h sealed by Apply user_id=%s ok=%s",
                plan.user_id,
                applied["date_listed_applied_and_panel_confirmed"],
            )

        if applied["degraded_mode"] and not applied.get("worker_collector_warning"):
            applied["worker_collector_warning"] = (
                "Marketplace sort filter could not be fully applied (sort and/or confirm). "
                "Search uses location/radius and category."
            )
            logger.warning(
                "Marketplace UI: degraded mode user_id=%s — partial advanced filters",
                plan.user_id,
            )

    logger.info(
        "Marketplace UI: filter cycle summary user_id=%s location_radius_ok=%s drawer_open=%s "
        "degraded=%s sort=%s confirmed=%s surface=%s date_listed_attempted=%s date_listed_24h=%s "
        "date_verify_ok=%s date_err=%s",
        plan.user_id,
        applied["location_radius_ok"],
        applied["filters_drawer_opened"],
        applied["degraded_mode"],
        applied["sort_ui"],
        applied.get("filters_confirmed"),
        applied.get("filters_surface"),
        applied.get("date_listed_filter_attempted"),
        applied.get("date_listed_24h_selected"),
        applied.get("date_listed_verify_ok"),
        (applied.get("date_listed_error") or "")[:120] or None,
    )
    logger.info(
        "Marketplace UI: filters applied user_id=%s: %s",
        plan.user_id,
        {k: v for k, v in applied.items() if k not in ("entry_url",)},
    )
    return applied
