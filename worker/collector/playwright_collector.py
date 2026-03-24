"""
Load a small local HTML fixture with Playwright and parse listing cards.

Conservative: one navigation, short timeout, no retries beyond caller.
"""

from __future__ import annotations

import os
from pathlib import Path

from mock_scraper import RawListing

_STUB = Path(__file__).resolve().parent / "static" / "marketplace_stub.html"


def _parse_float(val: str | None) -> float | None:
    if val is None or not str(val).strip():
        return None
    try:
        return float(val)
    except ValueError:
        return None


def fetch_listings_playwright(*, backfill: bool) -> list[RawListing]:
    """Parse stub HTML via Playwright. `backfill` selects batch size hint (stub returns all)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("playwright is not installed") from exc

    stub_path = Path(os.environ.get("COLLECTOR_STUB_HTML", str(_STUB))).resolve()
    if not stub_path.is_file():
        raise FileNotFoundError(f"Collector stub not found: {stub_path}")

    uri = stub_path.as_uri()
    out: list[RawListing] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.set_default_timeout(30_000)
            page.goto(uri, wait_until="domcontentloaded")
            for el in page.query_selector_all("article.listing"):
                url = el.get_attribute("data-url") or ""
                price_a = _parse_float(el.get_attribute("data-price"))
                lat = _parse_float(el.get_attribute("data-lat"))
                lon = _parse_float(el.get_attribute("data-lon"))
                cat = (el.get_attribute("data-category") or "general").strip()
                h2 = el.query_selector("h2")
                loc_el = el.query_selector(".loc")
                title = (h2.inner_text() if h2 else "").strip()
                loc = (loc_el.inner_text() if loc_el else "").strip()
                if not url or title == "" or price_a is None:
                    continue
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
        finally:
            browser.close()

    return out
