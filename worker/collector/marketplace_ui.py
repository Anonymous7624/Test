"""
Facebook Marketplace: apply built-in filters via the logged-in web UI (Playwright async).

Facebook changes markup frequently; selectors use roles/labels with fallbacks. Location and radius
are required for a meaningful search. The Filters drawer (max price, sort) is best-effort: if it
cannot be opened or controls fail, the collector degrades gracefully and continues with focused
queries using whatever filters Facebook already applied (e.g. location/radius, category path).
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Callable

from search_context import CollectionInputs
from search_plan import (
    MARKETPLACE_SORT_UI_LABEL,
    SearchPlan,
    build_marketplace_entry_url,
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
        dialog.get_by_text(re.compile(r"\b(sort|price|max|condition|category|delivery)\b", re.I)),
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
    ]


async def _try_open_filters_drawer(page) -> dict[str, Any]:
    """
    Try to open the Filters drawer/panel. Does not raise.

    Returns keys: opened, already_visible, selector_used, attempt_log (list[str]).
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


async def _set_max_price_in_filters(page, plan: SearchPlan) -> None:
    """Inside Filters: set maximum price (USD)."""
    cap = int(max(0, min(plan.max_price, 1_000_000)))
    dialog = page.locator('[role="dialog"]').first
    try:
        await dialog.wait_for(state="visible", timeout=12_000)
    except Exception as exc:
        raise MarketplaceFilterError("Filters panel not visible as a dialog.") from exc

    # Prefer labeled "Max" near price.
    max_input = None
    for loc in (
        dialog.get_by_label(re.compile(r"^max$", re.I)),
        dialog.locator('input[placeholder*="Max" i]'),
        dialog.locator('input[aria-label*="Max" i]'),
        dialog.locator('input[inputmode="numeric"]').nth(1),
    ):
        try:
            if await loc.count() < 1:
                continue
            el = loc.first
            await el.wait_for(state="visible", timeout=8000)
            max_input = el
            break
        except Exception:
            continue
    if max_input is None:
        raise MarketplaceFilterError("Could not find Max price input in Filters.")

    await max_input.fill("")
    await max_input.fill(str(cap))
    await asyncio.sleep(0.1)


async def _set_sort_in_filters(page, plan: SearchPlan) -> None:
    label = _sort_label_for_plan(plan)
    dialog = page.locator('[role="dialog"]').first
    try:
        await dialog.wait_for(state="visible", timeout=8000)
    except Exception as exc:
        raise MarketplaceFilterError("Filters dialog missing for sort.") from exc

    # Sort combobox or list.
    for loc in (
        dialog.get_by_role("combobox", name=re.compile(r"sort", re.I)),
        dialog.get_by_label(re.compile(r"sort", re.I)),
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


async def _apply_filters_confirm(page) -> None:
    for btn in (
        page.get_by_role("button", name=re.compile(r"apply", re.I)),
        page.get_by_role("button", name=re.compile(r"see.*items|show.*results|show.*listings", re.I)),
    ):
        try:
            if await btn.count():
                await btn.first.click()
                await page.wait_for_timeout(800)
                return
        except Exception:
            continue
    raise MarketplaceFilterError("Could not confirm Filters (no Apply / See items button).")


async def run_focused_marketplace_query(page, query: str) -> None:
    """Use the search box after filters are applied; does not navigate to URL query strings."""
    q = (query or "").strip()
    if not q:
        raise MarketplaceFilterError("Internal error: empty focused query.")

    search_candidates = [
        page.get_by_role("combobox", name=re.compile(r"search", re.I)),
        page.get_by_placeholder(re.compile(r"marketplace|search", re.I)),
        page.locator('input[type="search"]'),
        page.locator('input[placeholder*="Search"]'),
    ]
    last_exc: Exception | None = None
    for loc in search_candidates:
        try:
            if await loc.count() < 1:
                continue
            el = loc.first
            await el.wait_for(state="visible", timeout=12_000)
            await el.fill("")
            await el.fill(q)
            await el.press("Enter")
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(500)
            return
        except Exception as exc:
            last_exc = exc
            continue
    raise MarketplaceFilterError(
        f"Could not run focused query {q!r} (search input not found). Last: {last_exc!r}"
    ) from last_exc


async def apply_marketplace_filters_ui(
    page,
    plan: SearchPlan,
    *,
    collection_inputs: CollectionInputs,
) -> dict[str, Any]:
    """
    Navigate to Marketplace (category path only), then set location, radius, max price, sort via UI.

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

    applied: dict[str, Any] = {
        "entry_url": entry_url,
        "category_slug": plan.marketplace_category_slug,
        "location_text": (plan.location_text or "").strip(),
        "radius_miles_requested": round(plan.radius_miles, 2),
        "radius_miles_snapped": _snap_radius_miles(plan.radius_miles),
        "max_price_cap": int(max(0, min(plan.max_price, 1_000_000))),
        "sort_mode": plan.sort_mode,
        "sort_ui_label": _sort_label_for_plan(plan),
        "location_radius_ok": False,
        "filters_drawer_opened": False,
        "filters_drawer_already_visible": False,
        "filters_drawer_selector": None,
        "max_price_ui": "not_attempted",
        "sort_ui": "not_attempted",
        "filters_confirmed": False,
        "degraded_mode": False,
        "worker_collector_warning": None,
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

    # Price + sort (Filters drawer) — best-effort.
    drawer_info = await _try_open_filters_drawer(page)
    applied["filters_drawer_opened"] = bool(
        drawer_info.get("opened") or drawer_info.get("already_visible")
    )
    applied["filters_drawer_already_visible"] = bool(drawer_info.get("already_visible"))
    applied["filters_drawer_selector"] = drawer_info.get("selector_used")
    applied["filters_drawer_attempts"] = drawer_info.get("attempt_log", [])

    if not applied["filters_drawer_opened"]:
        applied["degraded_mode"] = True
        applied["max_price_ui"] = "skipped_no_drawer"
        applied["sort_ui"] = "skipped_no_drawer"
        fs = drawer_info.get("failure_summary") or (
            "; ".join(drawer_info.get("attempt_log") or []) or "unknown"
        )
        applied["worker_collector_warning"] = (
            "Advanced Marketplace filters (max price, sort) were skipped: Filters drawer did not open. "
            f"Details: {fs[:400]}"
        )
        logger.warning(
            "Marketplace UI: degraded mode user_id=%s — continuing with location/radius + category entry only. %s",
            plan.user_id,
            applied["worker_collector_warning"][:500],
        )
    else:
        # Max price
        try:
            await _set_max_price_in_filters(page, plan)
            applied["max_price_ui"] = "applied"
            logger.info(
                "Marketplace UI: max price applied user_id=%s cap=%s",
                plan.user_id,
                applied["max_price_cap"],
            )
        except Exception as exc:
            applied["max_price_ui"] = f"skipped: {type(exc).__name__}: {exc!r}"
            applied["degraded_mode"] = True
            logger.warning(
                "Marketplace UI: max price not applied user_id=%s: %s",
                plan.user_id,
                exc,
                exc_info=True,
            )

        # Sort
        try:
            await _set_sort_in_filters(page, plan)
            applied["sort_ui"] = "applied"
        except Exception as exc:
            applied["sort_ui"] = f"skipped: {type(exc).__name__}: {exc!r}"
            applied["degraded_mode"] = True
            logger.warning(
                "Marketplace UI: sort not applied user_id=%s: %s",
                plan.user_id,
                exc,
                exc_info=True,
            )

        # Confirm / apply filters panel
        try:
            await _apply_filters_confirm(page)
            applied["filters_confirmed"] = True
        except Exception as exc:
            applied["filters_confirmed"] = False
            applied["degraded_mode"] = True
            logger.warning(
                "Marketplace UI: Filters confirm failed user_id=%s: %s",
                plan.user_id,
                exc,
                exc_info=True,
            )

        if applied["degraded_mode"] and not applied.get("worker_collector_warning"):
            applied["worker_collector_warning"] = (
                "Some advanced Marketplace filters could not be fully applied (max price, sort, and/or confirm). "
                "Search uses location/radius and category; results may be broader than configured."
            )
            logger.warning(
                "Marketplace UI: degraded mode user_id=%s — partial advanced filters",
                plan.user_id,
            )

    logger.info(
        "Marketplace UI: filter cycle summary user_id=%s location_radius_ok=%s drawer_open=%s "
        "degraded=%s max_price=%s sort=%s confirmed=%s",
        plan.user_id,
        applied["location_radius_ok"],
        applied["filters_drawer_opened"],
        applied["degraded_mode"],
        applied["max_price_ui"],
        applied["sort_ui"],
        applied["filters_confirmed"],
    )
    logger.info(
        "Marketplace UI: filters applied user_id=%s: %s",
        plan.user_id,
        {k: v for k, v in applied.items() if k not in ("entry_url",)},
    )
    return applied
