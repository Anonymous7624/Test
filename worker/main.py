"""
Worker loop: poll users with monitoring_enabled — backfill then live polling — Playwright + Ollama pipeline.

Run from repository root (see README) so imports resolve:
  PYTHONPATH=backend;.;%CD%  (Windows PowerShell example in README)
"""

from __future__ import annotations

import asyncio
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

# Resolve backend app package and worker directory on sys.path
_ROOT = Path(__file__).resolve().parent
_REPO = _ROOT.parent
_BACKEND = _REPO / "backend"
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(_ROOT))

from pymongo.database import Database  # noqa: E402

from app.config import settings  # noqa: E402
from app.database import get_database  # noqa: E402
from app.domain import UserSettings as UserSettingsRow  # noqa: E402
from app.mongodb import ensure_indexes  # noqa: E402
from app.repositories.user_repository import UserRepository, settings_from_doc  # noqa: E402
from collector.playwright_collector import (  # noqa: E402
    FacebookAuthStateMissingError,
    fetch_listings_playwright,
)
from mock_scraper import RawListing, mock_fetch_backfill, mock_fetch_batch  # noqa: E402
from pipeline import process_batch  # noqa: E402
from search_context import build_collection_inputs  # noqa: E402


def _collect_raws(profile: UserSettingsRow, *, backfill: bool) -> list[RawListing]:
    inputs = build_collection_inputs(profile)
    try:
        return fetch_listings_playwright(collection_inputs=inputs, backfill=backfill)
    except FacebookAuthStateMissingError:
        raise
    except Exception as exc:  # noqa: BLE001
        print(f"Playwright collector failed, using mock data: {exc}", flush=True)
        if backfill:
            return mock_fetch_backfill(
                category_slug=inputs.category_id,
                location=inputs.primary_search_location,
                max_price=inputs.max_price,
                keywords=inputs.keywords,
                search_area_labels=inputs.search_area_labels,
            )
        return mock_fetch_batch(
            category_slug=inputs.category_id,
            location=inputs.primary_search_location,
            max_price=inputs.max_price,
            keywords=inputs.keywords,
            search_area_labels=inputs.search_area_labels,
        )


def _process_monitoring_user(db: Database, s: UserSettingsRow) -> None:
    repo = UserRepository(db)
    now = datetime.utcnow()

    if s.monitoring_state == "starting":
        s.monitoring_state = "backfill"
        repo.replace_settings(s)

    if not s.backfill_complete:
        s.monitoring_state = "backfill"
        repo.replace_settings(s)
        raws = _collect_raws(s, backfill=True)
        if raws:
            process_batch(db, raws, profile=s, origin_type="backfill")
        s.backfill_complete = True
        s.monitoring_state = "polling"
        s.last_checked_at = now
        s.last_error = None
        repo.replace_settings(s)
        return

    s.monitoring_state = "polling"
    repo.replace_settings(s)
    raws = _collect_raws(s, backfill=False)
    if raws:
        process_batch(db, raws, profile=s, origin_type="live")
    s.last_checked_at = now
    s.last_error = None
    repo.replace_settings(s)


def tick() -> None:
    db: Database = get_database()
    try:
        for doc in db["user_settings"].find({"monitoring_enabled": True}):
            s = settings_from_doc(doc)
            try:
                _process_monitoring_user(db, s)
            except Exception as exc:  # noqa: BLE001 — surface errors in MVP worker
                s.monitoring_state = "error"
                s.last_error = str(exc)[:500]
                UserRepository(db).replace_settings(s)
                print(f"Worker user {s.user_id} error: {exc}", flush=True)
                traceback.print_exc()
    finally:
        pass


async def main_loop() -> None:
    ensure_indexes(get_database())
    interval = float(os.environ.get("WORKER_POLL_SECONDS", "300"))
    print(
        f"Worker started. MONGODB_URI={settings.mongodb_uri} db={settings.mongodb_database} "
        f"poll={interval}s (default 300 = 5 min)",
        flush=True,
    )
    while True:
        try:
            tick()
        except Exception as exc:  # noqa: BLE001
            print(f"Worker tick error: {exc}", flush=True)
            traceback.print_exc()
        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(main_loop())
