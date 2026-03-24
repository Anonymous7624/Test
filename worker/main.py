"""
Worker loop: poll users with monitoring_enabled — backfill then live polling — ingest mock listings.

Run from repository root (see README) so imports resolve:
  PYTHONPATH=backend;.;%CD%  (Windows PowerShell example in README)
"""

from __future__ import annotations

import asyncio
import os
import sys
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
from app.mongodb import ensure_indexes  # noqa: E402
from app.domain import UserSettings as UserSettingsRow  # noqa: E402
from app.repositories.user_repository import UserRepository, settings_from_doc  # noqa: E402
from mock_scraper import mock_fetch_backfill, mock_fetch_batch  # noqa: E402
from pipeline import process_batch  # noqa: E402
from search_context import build_search_location_hint  # noqa: E402


def _process_monitoring_user(db: Database, s: UserSettingsRow) -> None:
    repo = UserRepository(db)
    hint = build_search_location_hint(s)
    now = datetime.utcnow()
    if not s.backfill_complete:
        s.monitoring_state = "searching"
        repo.replace_settings(s)
        raws = mock_fetch_backfill(
            category_slug=s.category_id,
            location=hint,
            max_price=float(s.max_price),
        )
        if raws:
            process_batch(
                db,
                raws,
                owner_user_id=s.user_id,
                telegram_chat_id=s.telegram_chat_id,
                origin_type="backfill",
            )
        s.backfill_complete = True
        s.monitoring_state = "monitoring"
        s.last_checked_at = now
        s.last_error = None
        repo.replace_settings(s)
        return

    s.monitoring_state = "monitoring"
    repo.replace_settings(s)
    raws = mock_fetch_batch(
        category_slug=s.category_id,
        location=hint,
        max_price=float(s.max_price),
    )
    if raws:
        process_batch(
            db,
            raws,
            owner_user_id=s.user_id,
            telegram_chat_id=s.telegram_chat_id,
            origin_type="live",
        )
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
    finally:
        pass


async def main_loop() -> None:
    ensure_indexes(get_database())
    interval = float(os.environ.get("WORKER_POLL_SECONDS", "8"))
    print(
        f"Worker started. MONGODB_URI={settings.mongodb_uri} db={settings.mongodb_database} poll={interval}s",
        flush=True,
    )
    while True:
        try:
            tick()
        except Exception as exc:  # noqa: BLE001
            print(f"Worker tick error: {exc}", flush=True)
        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(main_loop())
