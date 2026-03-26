"""
Facebook Marketplace: apply built-in filters via the logged-in web UI (Playwright async).

Facebook changes markup frequently; selectors use roles/labels with fallbacks. Location and radius
are required for a meaningful search. Sort and Date listed (category mode) resolve from inline
left-column filters, a Filters dialog, or a page-level probe — not only from opening the drawer.
If controls still fail, the collector continues with location/radius and category path. No
user-configurable price cap is applied in Marketplace UI.
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


async def _scroll_main_for_filters(page) -> None:
    """Category pages may mount the filter rail below the fold; nudge layout before detection."""
    main = page.locator('[role="main"]')
    if await main.count() < 1:
        return
    try:
        await main.first.evaluate("el => el.scrollTo(0, 0)")
        await page.wait_for_timeout(200)
        await main.first.evaluate("el => el.scrollTo(0, Math.min(el.scrollHeight, 400))")
        await page.wait_for_timeout(250)
    except Exception:
        pass


async def _locator_visible_and_enabled(loc) -> bool:
    try:
        if await loc.count() < 1:
            return False
        el = loc.first
        vis = await el.is_visible()
        if not vis:
            return False
        try:
            dis = await el.is_disabled()
            if dis:
                return False
        except Exception:
            pass
        return True
    except Exception:
        return False


async def _date_listed_text_locator_in_scope(scope):
    """Match Facebook copy variants (Date listed, Listed date, etc.)."""
    patterns = (
        re.compile(r"date\s*listed", re.I),
        re.compile(r"listed\s*date", re.I),
        re.compile(r"date\s+posted", re.I),
    )
    for pat in patterns:
        loc = scope.get_by_text(pat)
        try:
            if await loc.count() > 0:
                return loc.first, f"text:{pat.pattern}"
        except Exception:
            continue
    return None, "no_date_listed_text_match"


async def _left_sidebar_filters_visible(page) -> tuple[bool, str]:
    """
    Desktop category pages often show Filters + Date listed in the main column without a drawer.
    Uses several layouts: aside, filter rail divs, labels, aria.
    """
    main = page.locator('[role="main"]')
    if await main.count() < 1:
        return False, "no_main_landmark"

    await _scroll_main_for_filters(page)

    try:
        dl, _detail = await _date_listed_text_locator_in_scope(main)
        if dl is not None and await _locator_visible_and_enabled(dl):
            return True, "date_listed_visible_in_main"
    except Exception:
        pass

    try:
        for lab in (
            main.get_by_label(re.compile(r"date\s*listed", re.I)),
            main.locator('label:has-text("Date")').filter(has_text=re.compile(r"listed", re.I)),
        ):
            if await lab.count() > 0 and await lab.first.is_visible():
                return True, "date_listed_label_in_main"
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
        col = main.locator(
            '[data-pagelet*="Filter" i], [data-testid*="filter" i], '
            '[class*="filter" i], [class*="Filter" i]'
        ).first
        if await col.count() > 0 and await col.is_visible():
            t = (await col.inner_text() or "")[:4000]
            if re.search(r"date\s*listed|sort|condition|price", t, re.I):
                return True, "filter_container_visible"
    except Exception:
        pass

    # Whole-page fallback: filter rail sometimes not under [role=main] in SPA shells.
    try:
        dl, _d = await _date_listed_text_locator_in_scope(page)
        if dl is not None and await _locator_visible_and_enabled(dl):
            return True, "date_listed_visible_page_scope"
    except Exception:
        pass

    return False, "left_sidebar_filters_not_detected"


async def _wait_for_left_filters_section(
    page, *, user_id: str | None, max_wait_ms: int = 12_000
) -> tuple[bool, str]:
    """
    After category + location, the filter rail can mount late. Poll before opening the drawer.
    """
    deadline = max_wait_ms
    step = 450
    elapsed = 0
    last_reason = "initial"
    while elapsed < deadline:
        await _scroll_main_for_filters(page)
        ok, reason = await _left_sidebar_filters_visible(page)
        last_reason = reason
        if ok:
            logger.info(
                "Marketplace UI: left_side_filters_found user_id=%s detail=%s wait_ms=%s",
                user_id,
                reason,
                elapsed,
            )
            return True, reason
        await page.wait_for_timeout(step)
        elapsed += step
    logger.warning(
        "Marketplace UI: left_side_filters_not_yet_visible user_id=%s last_detail=%s after_ms=%s",
        user_id,
        last_reason,
        deadline,
    )
    return False, last_reason


async def _find_left_filters_container_scope(page) -> tuple[Any | None, str]:
    """
    Prefer the narrowest DOM scope for the left filter rail (Filters + Date listed), not whole main.
    """
    main = page.locator('[role="main"]')
    if await main.count() < 1:
        return None, "no_main_landmark"

    await _scroll_main_for_filters(page)

    # Aside / complementary regions often hold the left filter column.
    for name, loc in (
        ("aside", main.locator("aside").first),
        ("complementary", main.get_by_role("complementary")),
    ):
        try:
            if await loc.count() < 1:
                continue
            el = loc.first
            if not await el.is_visible():
                continue
            txt = (await el.inner_text() or "")[:6000]
            if not re.search(r"date\s*listed", txt, re.I):
                continue
            dl, pat = await _date_listed_text_locator_in_scope(el)
            if dl is not None and await _locator_visible_and_enabled(dl):
                return el, f"left_rail:{name}:{pat}"
        except Exception:
            continue

    # Any visible block under main that contains a visible "Date listed" label (narrower than full main).
    try:
        blocks = main.locator(
            'div[role="navigation"], section, '
            '[data-pagelet*="Filter" i], [data-testid*="filter" i]'
        )
        n = await blocks.count()
        for i in range(min(n, 25)):
            blk = blocks.nth(i)
            try:
                if not await blk.is_visible():
                    continue
                dl, pat = await _date_listed_text_locator_in_scope(blk)
                if dl is None or not await _locator_visible_and_enabled(dl):
                    continue
                tblk = (await blk.inner_text() or "")[:4000]
                if re.search(r"\bfilter", tblk, re.I) or re.search(
                    r"date\s*listed", tblk, re.I
                ):
                    return blk, f"left_rail:block:{pat}"
            except Exception:
                continue
    except Exception:
        pass

    # Playwright 1.29+: narrowest div in main that contains Date listed text.
    try:
        has_dl = main.get_by_text(re.compile(r"date\s*listed", re.I)).first
        if await has_dl.count() > 0:
            narrow = main.locator("div").filter(has=has_dl).first
            if await narrow.count() > 0 and await narrow.first.is_visible():
                return narrow.first, "left_rail:div_filter_has_date_listed"
    except Exception:
        pass

    return None, "no_narrow_left_container"


async def _resolve_filters_root_for_category_date_filter(
    page, drawer_info: dict[str, Any]
) -> tuple[Any, str, str]:
    """
    For marketplace_category Date listed flow: prefer left rail scope when visible.
    Returns (root, surface_label, narrow_detail).
    """
    container, narrow_detail = await _find_left_filters_container_scope(page)
    if container is not None:
        logger.info(
            "Marketplace UI: filters_root narrowed to left filter container detail=%s",
            narrow_detail,
        )
        return container, "left_filter_rail", narrow_detail

    root, label = await _find_best_filters_root(page, drawer_info)
    return root, label, "used_best_filters_root"


async def _probe_date_listed_root(page) -> tuple[Any, str, str]:
    """
    Find a locator scope that contains a visible Date listed control (main, dialog, or full page).
    Returns (root, short_label, detail) or (None, \"none\", reason).
    """
    await _scroll_main_for_filters(page)

    dialog = page.locator('[role="dialog"]').first
    if await dialog.count() > 0:
        try:
            if await dialog.is_visible():
                dl, _pat = await _date_listed_text_locator_in_scope(dialog)
                if dl is not None and await _locator_visible_and_enabled(dl):
                    return dialog, "dialog", "date_listed_in_visible_dialog"
        except Exception:
            pass

    main = page.locator('[role="main"]')
    if await main.count() > 0:
        try:
            dl, pat = await _date_listed_text_locator_in_scope(main)
            if dl is not None:
                await dl.scroll_into_view_if_needed()
                await page.wait_for_timeout(200)
                if await _locator_visible_and_enabled(dl):
                    return main, "inline_main", f"date_listed_in_main:{pat}"
        except Exception:
            pass

    try:
        dl, pat = await _date_listed_text_locator_in_scope(page)
        if dl is not None:
            await dl.scroll_into_view_if_needed()
            await page.wait_for_timeout(200)
            if await _locator_visible_and_enabled(dl):
                return page, "page", f"date_listed_page_scope:{pat}"
    except Exception:
        pass

    return None, "none", "date_listed_not_found_anywhere"


async def _discover_filters_surface(page) -> dict[str, Any]:
    """
    Resolve where filters live: inline left column, Filters dialog, or probed Date listed region.
    Unlike drawer-only flow, does not give up if the drawer click does not match heuristics.
    """
    discovery_log: list[str] = []

    await _scroll_main_for_filters(page)
    left_ok, left_reason = await _left_sidebar_filters_visible(page)
    discovery_log.append(f"inline_first:{left_ok}:{left_reason}")

    if left_ok:
        return {
            "surface_ready": True,
            "inline_filters_found": True,
            "drawer_mode": False,
            "drawer_info": {
                "opened": False,
                "already_visible": True,
                "selector_used": left_reason,
                "attempt_log": [f"inline_filters: {left_reason}"],
                "surface": "left_sidebar",
            },
            "discovery_log": discovery_log,
        }

    drawer_info = await _try_open_filters_drawer(page)
    opened = bool(drawer_info.get("opened") or drawer_info.get("already_visible"))
    discovery_log.append(
        f"drawer_try:opened={opened}:surface={drawer_info.get('surface')}"
    )

    if opened:
        is_dialog = drawer_info.get("surface") == "filters_dialog"
        return {
            "surface_ready": True,
            "inline_filters_found": drawer_info.get("surface") == "left_sidebar",
            "drawer_mode": is_dialog,
            "drawer_info": drawer_info,
            "discovery_log": discovery_log,
        }

    await page.wait_for_timeout(450)
    await _scroll_main_for_filters(page)
    left_ok2, left_reason2 = await _left_sidebar_filters_visible(page)
    discovery_log.append(f"inline_retry:{left_ok2}:{left_reason2}")

    if left_ok2:
        return {
            "surface_ready": True,
            "inline_filters_found": True,
            "drawer_mode": False,
            "drawer_info": {
                "opened": False,
                "already_visible": True,
                "selector_used": left_reason2,
                "attempt_log": [f"inline_retry: {left_reason2}"],
                "surface": "left_sidebar",
            },
            "discovery_log": discovery_log,
        }

    probe_root, probe_label, probe_detail = await _probe_date_listed_root(page)
    discovery_log.append(f"probe:{probe_label}:{probe_detail}")

    if probe_root is not None:
        surf = "left_sidebar" if probe_label == "inline_main" else (
            "filters_dialog" if probe_label == "dialog" else "page_fallback"
        )
        return {
            "surface_ready": True,
            "inline_filters_found": probe_label == "inline_main",
            "drawer_mode": probe_label == "dialog",
            "drawer_info": {
                "opened": False,
                "already_visible": True,
                "selector_used": probe_detail,
                "attempt_log": [f"probe:{probe_detail}"],
                "surface": surf,
                "probe_root_hint": probe_label,
            },
            "discovery_log": discovery_log,
        }

    return {
        "surface_ready": False,
        "inline_filters_found": False,
        "drawer_mode": False,
        "drawer_info": drawer_info,
        "discovery_log": discovery_log,
    }


async def _find_best_filters_root(page, drawer_info: dict[str, Any]) -> tuple[Any, str]:
    """
    Choose the DOM root for Sort + Date listed. Honors probe hints from _discover_filters_surface.
    """
    surf = drawer_info.get("surface")
    hint = drawer_info.get("probe_root_hint")

    if hint == "page":
        logger.info("Marketplace UI: filters root=page (Date listed probe)")
        return page, "page_fallback"

    if hint == "dialog":
        logger.info("Marketplace UI: filters root=dialog (Date listed probe)")
        return page.locator('[role="dialog"]').first, "filters_dialog_probe"

    if surf == "page_fallback":
        logger.info("Marketplace UI: filters root=page (surface=page_fallback)")
        return page, "page_fallback"

    if surf == "left_sidebar":
        logger.info("Marketplace UI: filters root=left main column (inline sidebar)")
        return page.locator('[role="main"]'), "left_sidebar_main"

    if await _filters_panel_looks_open(page):
        logger.info("Marketplace UI: filters root=dialog overlay")
        return page.locator('[role="dialog"]').first, "filters_dialog"

    if drawer_info.get("opened"):
        logger.info("Marketplace UI: filters root=dialog (opened via UI click)")
        return page.locator('[role="dialog"]').first, "filters_dialog_opened"

    main = page.locator('[role="main"]')
    if await main.count() > 0:
        return main, "main_fallback"

    return page, "page_fallback"


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


async def _verify_date_listed_24h_applied(page, root) -> tuple[bool, str]:
    """Confirm 24h filter: visible chip/label, selected option, or aria state."""
    logger.info("Marketplace UI: date_listed_verify checking Last 24h selection in DOM")
    scopes = [root, page]
    if root is not page:
        scopes.append(page.locator('[role="main"]'))

    # Selected menu/listbox option or radio
    for scope in scopes:
        try:
            for sel in (
                scope.locator('[role="option"][aria-selected="true"]'),
                scope.locator('[role="menuitemradio"][aria-checked="true"]'),
                scope.locator('[aria-selected="true"]'),
            ):
                if await sel.count() < 1:
                    continue
                for i in range(min(await sel.count(), 12)):
                    el = sel.nth(i)
                    try:
                        t = (await el.inner_text() or "")[:200]
                        if re.search(
                            r"24\s*hours?|past\s*24|last\s*24|past\s*day|today",
                            t,
                            re.I,
                        ):
                            logger.info(
                                "Marketplace UI: date_listed_verify OK via aria_selected text=%r",
                                t[:80],
                            )
                            return True, f"aria_selected:{t[:80]!r}"
                    except Exception:
                        continue
        except Exception:
            continue

    for rx in (
        re.compile(r"last\s*24\s*hours?", re.I),
        re.compile(r"past\s*24\s*hours?", re.I),
        re.compile(r"^\s*24\s*hours?\s*$", re.I),
        re.compile(r"past\s*day", re.I),
        re.compile(r"\b24\s*h\b", re.I),
    ):
        for scope in scopes:
            try:
                loc = scope.get_by_text(rx).first
                if await loc.count() and await loc.is_visible():
                    logger.info(
                        "Marketplace UI: date_listed_verify OK visible_label pattern=%s",
                        rx.pattern,
                    )
                    return True, f"visible_label:{rx.pattern}"
            except Exception:
                continue

    try:
        page_loc = page.get_by_text(
            re.compile(r"last\s*24\s*hours?|past\s*24|24\s*hours?", re.I)
        ).first
        if await page_loc.count() and await page_loc.is_visible():
            logger.info("Marketplace UI: date_listed_verify OK page_fallback label visible")
            return True, "visible_label:page_fallback"
    except Exception:
        pass
    logger.warning(
        "Marketplace UI: date_listed_verify FAILED — no Last 24h / 24 hours confirmation in UI"
    )
    return False, "no_confirmation_label_visible"


async def _set_date_listed_to_24_hours(page, root, *, surface_label: str) -> None:
    """
    Set Date listed → Last 24 hours (or closest label) within ``root`` (dialog or main).

    Facebook labels vary (\"24 hours\", \"Last 24 hours\", \"Past day\"); we try several.
    Logs each step: region visible → Date listed found → clicked → Last 24h → clicked.
    """
    try:
        if root is page:
            await page.wait_for_load_state("domcontentloaded")
        else:
            await root.wait_for(state="visible", timeout=8000)
    except Exception as exc:
        raise MarketplaceFilterError(
            f"Filters region not visible ({surface_label})."
        ) from exc

    logger.info(
        "Marketplace UI: date_listed_step filters_region_ready surface=%s",
        surface_label,
    )

    date_labels = [
        re.compile(r"date\s*listed", re.I),
        re.compile(r"listed\s*date", re.I),
        re.compile(r"\bdate\b.*\blisted\b", re.I),
    ]
    dl_found_pat: str | None = None
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
                logger.info(
                    "Marketplace UI: date_listed_step Date listed control found pattern=%s surface=%s",
                    pat.pattern,
                    surface_label,
                )
                await el.click()
                await page.wait_for_timeout(450)
                dl_found_pat = pat.pattern
                opened = True
                logger.info(
                    "Marketplace UI: date_listed_step Date listed clicked pattern=%s surface=%s",
                    pat.pattern,
                    surface_label,
                )
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
                    logger.info(
                        "Marketplace UI: date_listed_step Date listed found (row scan) text=%r surface=%s",
                        t[:120],
                        surface_label,
                    )
                    await el.scroll_into_view_if_needed()
                    await el.click()
                    await page.wait_for_timeout(450)
                    dl_found_pat = "row_scan"
                    opened = True
                    logger.info(
                        "Marketplace UI: date_listed_step Date listed clicked (row scan) surface=%s",
                        surface_label,
                    )
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
                logger.info(
                    "Marketplace UI: date_listed_step Last 24 hours option found pattern=%s surface=%s",
                    rx.pattern,
                    surface_label,
                )
                await loc.first.click()
                await page.wait_for_timeout(400)
                logger.info(
                    "Marketplace UI: date_listed_step Last 24 hours clicked pattern=%s "
                    "date_listed_opened_with=%s surface=%s",
                    rx.pattern,
                    dl_found_pat,
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
        if root is page:
            await page.wait_for_load_state("domcontentloaded")
        else:
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
        "filters_inline_detected": False,
        "filters_drawer_mode": False,
        "filters_surface_resolved": False,
        "filters_surface_discovery_log": [],
        "date_listed_section_found": False,
        "date_listed_last_24h_clicked": False,
        "filters_surface_narrow_detail": None,
        "left_side_filters_wait": None,
    }

    sm_cat = (plan.search_mode or "").strip() == "marketplace_category"

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

    if sm_cat:
        lf_ok, lf_detail = await _wait_for_left_filters_section(
            page, user_id=str(plan.user_id), max_wait_ms=12_000
        )
        applied["left_side_filters_wait"] = {"ok": lf_ok, "detail": lf_detail}
        logger.info(
            "Marketplace UI: marketplace_category mode — left rail wait done user_id=%s ok=%s detail=%s",
            plan.user_id,
            lf_ok,
            lf_detail,
        )

    ls_vis, ls_reason = await _left_sidebar_filters_visible(page)
    logger.info(
        "Marketplace UI: after_location left_sidebar_filter_section user_id=%s visible=%s detail=%s",
        plan.user_id,
        ls_vis,
        ls_reason,
    )

    disc = await _discover_filters_surface(page)
    drawer_info = disc["drawer_info"]
    surface_ready = bool(disc["surface_ready"])
    applied["filters_inline_detected"] = bool(disc.get("inline_filters_found"))
    applied["filters_drawer_mode"] = bool(disc.get("drawer_mode"))
    applied["filters_surface_resolved"] = surface_ready
    applied["filters_surface_discovery_log"] = list(disc.get("discovery_log") or [])

    applied["filters_drawer_opened"] = bool(
        drawer_info.get("opened") or drawer_info.get("already_visible")
    )
    applied["filters_drawer_already_visible"] = bool(drawer_info.get("already_visible"))
    applied["filters_drawer_selector"] = drawer_info.get("selector_used")
    applied["filters_drawer_attempts"] = drawer_info.get("attempt_log", [])
    applied["filters_surface"] = drawer_info.get("surface")

    logger.info(
        "Marketplace UI: filters_surface_discovery user_id=%s resolved=%s inline=%s drawer_mode=%s "
        "surface=%s log=%s",
        plan.user_id,
        surface_ready,
        applied["filters_inline_detected"],
        applied["filters_drawer_mode"],
        applied.get("filters_surface"),
        applied["filters_surface_discovery_log"],
    )

    if not surface_ready:
        applied["sort_ui"] = "skipped_no_filter_surface"
        fs = drawer_info.get("failure_summary") or (
            "; ".join(drawer_info.get("attempt_log") or []) or "unknown"
        )
        applied["worker_collector_warning"] = (
            "Marketplace filter surface not detected (inline, dialog, or Date listed probe). "
            f"Details: {fs[:400]}"
        )
        logger.warning(
            "Marketplace UI: filter surface unresolved user_id=%s — will try Date listed on page "
            "if category mode. %s",
            plan.user_id,
            applied["worker_collector_warning"][:500],
        )

    if sm_cat:
        filters_root, surface_label, narrow_detail = (
            await _resolve_filters_root_for_category_date_filter(page, drawer_info)
        )
        applied["filters_surface_narrow_detail"] = narrow_detail
    else:
        filters_root, surface_label = await _find_best_filters_root(page, drawer_info)
        applied["filters_surface_narrow_detail"] = None

    applied["filters_surface"] = surface_label

    logger.info(
        "Marketplace UI: filters_root_ready user_id=%s surface=%s narrow_detail=%s surface_ready=%s drawer_info=%s",
        plan.user_id,
        surface_label,
        applied.get("filters_surface_narrow_detail"),
        surface_ready,
        {
            "opened": drawer_info.get("opened"),
            "already_visible": drawer_info.get("already_visible"),
            "surface": drawer_info.get("surface"),
            "probe_root_hint": drawer_info.get("probe_root_hint"),
        },
    )

    if sm_cat:
        applied["date_listed_filter_attempted"] = True
        applied["date_listed_skipped_reason"] = None
        logger.info(
            "Marketplace UI: Date listed filter (24h) attempting user_id=%s mode=%s surface=%s "
            "surface_ready=%s",
            plan.user_id,
            plan.search_mode,
            surface_label,
            surface_ready,
        )
        try:
            await _set_date_listed_to_24_hours(page, filters_root, surface_label=surface_label)
            applied["date_listed_section_found"] = True
            applied["date_listed_last_24h_clicked"] = True
            ok, detail = await _verify_date_listed_24h_applied(page, filters_root)
            applied["date_listed_verify_ok"] = ok
            applied["date_listed_verify_detail"] = detail
            # Truth: only claim 24h filter applied when DOM confirms it (do not pretend).
            applied["date_listed_24h_selected"] = bool(ok)
            if ok:
                logger.info(
                    "Marketplace UI: date_listed_filter_success_confirmed user_id=%s verify_detail=%s",
                    plan.user_id,
                    detail,
                )
            else:
                logger.warning(
                    "Marketplace UI: date_listed_clicks_done_but_verify_failed user_id=%s "
                    "verify_detail=%s — not treating as applied",
                    plan.user_id,
                    detail,
                )
            logger.info(
                "Marketplace UI: Date listed 24h done user_id=%s verify_ok=%s verify_detail=%s "
                "section_found=%s last24_clicked=%s selected_truth=%s",
                plan.user_id,
                ok,
                detail,
                applied["date_listed_section_found"],
                applied["date_listed_last_24h_clicked"],
                applied["date_listed_24h_selected"],
            )
        except Exception as exc:
            err_s = f"{type(exc).__name__}: {str(exc)[:400]}"
            applied["date_listed_error"] = err_s
            if "Date listed control not found" in str(exc):
                applied["date_listed_section_found"] = False
            elif "Could not select a 24-hour" in str(exc) or "24-hour" in str(exc):
                applied["date_listed_section_found"] = True
                applied["date_listed_last_24h_clicked"] = False
            logger.warning(
                "Marketplace UI: Date listed 24h not applied user_id=%s surface=%s reason=%s",
                plan.user_id,
                surface_label,
                exc,
                exc_info=True,
            )
            if surface_label not in ("page_fallback",):
                logger.info(
                    "Marketplace UI: retry Date listed with full page root user_id=%s",
                    plan.user_id,
                )
                try:
                    await _set_date_listed_to_24_hours(
                        page, page, surface_label="last_resort_page"
                    )
                    applied["date_listed_section_found"] = True
                    applied["date_listed_last_24h_clicked"] = True
                    ok, detail = await _verify_date_listed_24h_applied(page, page)
                    applied["date_listed_verify_ok"] = ok
                    applied["date_listed_verify_detail"] = detail
                    applied["date_listed_24h_selected"] = bool(ok)
                    applied["date_listed_error"] = None if ok else applied.get("date_listed_error")
                    if ok:
                        logger.info(
                            "Marketplace UI: Date listed 24h confirmed after page-root retry user_id=%s",
                            plan.user_id,
                        )
                    else:
                        logger.warning(
                            "Marketplace UI: page-root retry clicks did not verify user_id=%s detail=%s",
                            plan.user_id,
                            detail,
                        )
                except Exception as exc2:
                    applied["date_listed_error"] = (
                        f"{err_s} | retry: {type(exc2).__name__}: {str(exc2)[:200]}"
                    )
                    applied["date_listed_skipped_reason"] = "date_listed_unreachable"
                    logger.warning(
                        "Marketplace UI: Date listed page-root retry failed user_id=%s: %s",
                        plan.user_id,
                        exc2,
                    )

        # Clicks ran on a narrow root but verification failed — try once at page scope.
        if (
            not applied.get("date_listed_24h_selected")
            and applied.get("date_listed_last_24h_clicked")
            and surface_label not in ("page_fallback", "last_resort_page", "last_resort_page_verify_retry")
        ):
            logger.info(
                "Marketplace UI: retry Date listed with full page root after verify_fail user_id=%s "
                "prior_surface=%s",
                plan.user_id,
                surface_label,
            )
            try:
                await _set_date_listed_to_24_hours(
                    page, page, surface_label="last_resort_page_verify_retry"
                )
                applied["date_listed_section_found"] = True
                applied["date_listed_last_24h_clicked"] = True
                ok2, detail2 = await _verify_date_listed_24h_applied(page, page)
                applied["date_listed_verify_ok"] = ok2
                applied["date_listed_verify_detail"] = detail2
                applied["date_listed_24h_selected"] = bool(ok2)
                if ok2:
                    applied["date_listed_error"] = None
                    logger.info(
                        "Marketplace UI: Date listed 24h confirmed after verify_fail page-root retry "
                        "user_id=%s",
                        plan.user_id,
                    )
                else:
                    applied["date_listed_error"] = (
                        applied.get("date_listed_error")
                        or f"verify_fail_then_page_retry: {detail2}"
                    )
                    logger.warning(
                        "Marketplace UI: verify_fail page-root retry did not confirm user_id=%s detail=%s",
                        plan.user_id,
                        detail2,
                    )
            except Exception as exc_v:
                err_v = f"{type(exc_v).__name__}: {str(exc_v)[:400]}"
                applied["date_listed_error"] = (
                    f"{(applied.get('date_listed_error') or '')} | verify_retry: {err_v}"
                ).strip(" |")
                logger.warning(
                    "Marketplace UI: verify_fail page-root retry raised user_id=%s: %s",
                    plan.user_id,
                    exc_v,
                )

        if not applied.get("date_listed_24h_selected"):
            applied["degraded_mode"] = True
            prev = (applied.get("worker_collector_warning") or "").strip()
            err_part = (applied.get("date_listed_error") or "").strip()
            ver_detail = applied.get("date_listed_verify_detail")
            ver_ok = applied.get("date_listed_verify_ok")
            reason_parts: list[str] = []
            if err_part:
                reason_parts.append(f"exception_or_step_error={err_part[:320]}")
            if not ver_ok and ver_detail:
                reason_parts.append(f"verify_detail={ver_detail}")
            if not reason_parts:
                reason_parts.append(
                    "clicks_or_DOM did not confirm Last 24 hours (see date_listed_verify_detail)"
                )
            extra = (
                "Date listed filter (24 hours) could not be applied or confirmed. "
                f"Surface={surface_label}. " + "; ".join(reason_parts)
            )
            applied["worker_collector_warning"] = f"{prev} | {extra}" if prev else extra
    else:
        applied["date_listed_skipped_reason"] = "not_marketplace_category_mode"
        logger.info(
            "Marketplace UI: Date listed filter skipped (search_mode=%s) user_id=%s",
            plan.search_mode,
            plan.user_id,
        )

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

    try:
        confirmed = await _apply_filters_confirm(page)
        if not confirmed and surface_label in (
            "left_sidebar_main",
            "main_fallback",
            "page_fallback",
            "filters_dialog_probe",
            "left_filter_rail",
        ):
            confirmed = True
            logger.info(
                "Marketplace UI: treating filters as applied (inline or page scope; no modal Apply) "
                "user_id=%s surface=%s",
                plan.user_id,
                surface_label,
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
        applied.get("date_listed_24h_selected")
        and applied.get("date_listed_verify_ok")
        and (
            applied.get("filters_confirmed")
            or surface_label
            in (
                "page_fallback",
                "left_sidebar_main",
                "main_fallback",
                "filters_dialog_probe",
                "left_filter_rail",
            )
        )
    )
    if sm_cat and applied.get("date_listed_24h_selected") and applied.get("date_listed_verify_ok"):
        logger.info(
            "Marketplace UI: Date listed 24h confirmed user_id=%s panel_ok=%s verify=%s",
            plan.user_id,
            applied["date_listed_applied_and_panel_confirmed"],
            applied.get("date_listed_verify_detail"),
        )

    if applied["degraded_mode"] and not applied.get("worker_collector_warning"):
        applied["worker_collector_warning"] = (
            "Marketplace advanced filters could not be fully applied. "
            "Search uses location/radius and category."
        )
        logger.warning(
            "Marketplace UI: degraded mode user_id=%s — partial advanced filters",
            plan.user_id,
        )

    logger.info(
        "Marketplace UI: filter cycle summary user_id=%s location_radius_ok=%s drawer_open=%s "
        "surface_resolved=%s inline_filters=%s drawer_mode=%s degraded=%s sort=%s confirmed=%s "
        "surface=%s date_listed_attempted=%s date_listed_24h=%s section_found=%s last24_clicked=%s "
        "date_verify_ok=%s date_err=%s",
        plan.user_id,
        applied["location_radius_ok"],
        applied["filters_drawer_opened"],
        applied.get("filters_surface_resolved"),
        applied.get("filters_inline_detected"),
        applied.get("filters_drawer_mode"),
        applied["degraded_mode"],
        applied["sort_ui"],
        applied.get("filters_confirmed"),
        applied.get("filters_surface"),
        applied.get("date_listed_filter_attempted"),
        applied.get("date_listed_24h_selected"),
        applied.get("date_listed_section_found"),
        applied.get("date_listed_last_24h_clicked"),
        applied.get("date_listed_verify_ok"),
        (applied.get("date_listed_error") or "")[:120] or None,
    )
    logger.info(
        "Marketplace UI: filters applied user_id=%s: %s",
        plan.user_id,
        {k: v for k, v in applied.items() if k not in ("entry_url",)},
    )
    return applied
