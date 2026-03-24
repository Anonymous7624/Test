"""
Worker loop: poll users with monitoring_enabled and ingest mock listings.

Run from repository root (see README) so imports resolve:
  PYTHONPATH=backend;.;%CD%  (Windows PowerShell example in README)
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

# Resolve backend app package and worker directory on sys.path
_ROOT = Path(__file__).resolve().parent
_REPO = _ROOT.parent
_BACKEND = _REPO / "backend"
sys.path.insert(0, str(_BACKEND))
sys.path.insert(0, str(_ROOT))

from sqlalchemy import select  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from app.config import settings  # noqa: E402
from app.database import Base, SessionLocal, engine  # noqa: E402
from app.migrate_sqlite import apply_sqlite_migrations  # noqa: E402
from app.models import User, UserSettings  # noqa: E402
from mock_scraper import mock_fetch_batch  # noqa: E402
from pipeline import process_batch  # noqa: E402
from search_context import build_search_location_hint  # noqa: E402


def tick() -> None:
    db: Session = SessionLocal()
    try:
        stmt = (
            select(UserSettings)
            .join(User, UserSettings.user_id == User.id)
            .where(UserSettings.monitoring_enabled.is_(True))
        )
        rows = list(db.scalars(stmt))
        for s in rows:
            hint = build_search_location_hint(s)
            raws = mock_fetch_batch(
                category_slug=s.category_id,
                location=hint,
                max_price=s.max_price,
            )
            if raws:
                process_batch(
                    db,
                    raws,
                    owner_user_id=s.user_id,
                    telegram_chat_id=s.telegram_chat_id,
                )
    finally:
        db.close()


async def main_loop() -> None:
    # Same cwd convention as API: run worker from `backend/` so SQLite path matches.
    Path("data").mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(bind=engine)
    apply_sqlite_migrations(engine)
    interval = float(__import__("os").environ.get("WORKER_POLL_SECONDS", "8"))
    print(f"Worker started. DATABASE_URL={settings.database_url} poll={interval}s", flush=True)
    while True:
        try:
            tick()
        except Exception as exc:  # noqa: BLE001 — surface errors in MVP worker
            print(f"Worker tick error: {exc}", flush=True)
        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(main_loop())
