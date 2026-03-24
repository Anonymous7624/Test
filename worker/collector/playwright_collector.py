"""
Load a small local HTML fixture with Playwright and parse listing cards.

Facebook Marketplace sessions: the browser context is created with Playwright
``storage_state`` loaded from ``backend/playwright/.auth/facebook.json`` (see
``facebook_login_bootstrap.py`` at repo root). Conservative: one navigation,
short timeout, no retries beyond caller.
"""

from __future__ import annotations

import os
from pathlib import Path

from mock_scraper import RawListing

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_FACEBOOK_AUTH_STATE = (
    _REPO_ROOT / "backend" / "playwright" / ".auth" / "facebook.json"
)
_STUB = Path(__file__).resolve().parent / "static" / "marketplace_stub.html"


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


def fetch_listings_playwright(*, backfill: bool) -> list[RawListing]:
    """Parse stub HTML via Playwright. `backfill` selects batch size hint (stub returns all)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("playwright is not installed") from exc

    auth_path = facebook_auth_state_path()
    if not auth_path.is_file():
        raise FacebookAuthStateMissingError(auth_path)

    stub_path = Path(os.environ.get("COLLECTOR_STUB_HTML", str(_STUB))).resolve()
    if not stub_path.is_file():
        raise FileNotFoundError(f"Collector stub not found: {stub_path}")

    uri = stub_path.as_uri()
    out: list[RawListing] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            context = browser.new_context(storage_state=str(auth_path))
            try:
                page = context.new_page()
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
                context.close()
        finally:
            browser.close()

    return out
