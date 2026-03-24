"""One-time Facebook login: writes ``backend/playwright/.auth/facebook.json`` for the worker. Run from repo root."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

_REPO_ROOT = Path(__file__).resolve().parent
_AUTH_OUT = _REPO_ROOT / "backend" / "playwright" / ".auth" / "facebook.json"


def main() -> None:
    _AUTH_OUT.parent.mkdir(parents=True, exist_ok=True)
    print(
        "A Chromium window will open on Facebook login.\n"
        "Log in manually (including 2FA if prompted).\n"
        "When you are fully logged in, return here and press Enter to save auth state.\n"
        f"File: {_AUTH_OUT}\n"
    )
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        try:
            page = context.new_page()
            page.goto("https://www.facebook.com/login", wait_until="domcontentloaded")
            input("Press Enter after you have logged in in the browser... ")
            context.storage_state(path=str(_AUTH_OUT))
            print(f"Saved Playwright storage state to {_AUTH_OUT}")
        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    main()
