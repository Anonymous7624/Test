"""
Worker loop: poll users with monitoring_enabled — backfill then live polling — Playwright + Ollama pipeline.

Run from repository root (see README) so imports resolve:
  PYTHONPATH=backend;.;%CD%  (Windows PowerShell example in README)
"""

from __future__ import annotations

import asyncio
import logging
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

logger = logging.getLogger(__name__)


def _mock_collector_enabled() -> bool:
    return os.environ.get("WORKER_MOCK_COLLECTOR", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


async def _collect_raws(
    profile: UserSettingsRow, *, backfill: bool
) -> tuple[list[RawListing], dict]:
    """Returns (raw listings, collector metadata e.g. degraded UI mode)."""
    inputs = build_collection_inputs(profile)
    if _mock_collector_enabled():
        logger.warning(
            "Using MOCK collector (WORKER_MOCK_COLLECTOR is set); no Playwright / Facebook."
        )
        if backfill:
            raws = mock_fetch_backfill(
                category_slug=inputs.listing_category_ref,
                location=inputs.primary_search_location,
                keywords=inputs.keywords,
                search_area_labels=inputs.search_area_labels,
            )
        else:
            raws = mock_fetch_batch(
                category_slug=inputs.listing_category_ref,
                location=inputs.primary_search_location,
                keywords=inputs.keywords,
                search_area_labels=inputs.search_area_labels,
            )
        return raws, {}

    try:
        return await fetch_listings_playwright(collection_inputs=inputs, backfill=backfill)
    except FacebookAuthStateMissingError:
        raise
    except Exception as exc:
        logger.exception(
            "Playwright collector failed (user_id=%s backfill=%s): %s",
            profile.user_id,
            backfill,
            exc,
        )
        raise


def _reset_pipeline_cycle_counts(s: UserSettingsRow) -> None:
    """Zero per-cycle counters (e.g. empty fetch after a batch that had matches)."""
    s.worker_count_raw_collected = 0
    s.worker_count_step1_kept = 0
    s.worker_count_step2_matched = 0
    s.worker_count_step3_scored = 0
    s.worker_count_step4_saved = 0
    s.worker_count_alerts_sent = 0


def _begin_listing_collection(repo: UserRepository, s: UserSettingsRow, now: datetime) -> None:
    """Persist pipeline state before Playwright/mock fetch (step 1)."""
    s.worker_last_batch_started_at = now
    s.worker_current_step = 1
    s.worker_current_state = "collecting_listings"
    s.worker_pipeline_message = "Step 1: Looking for listings"
    s.worker_pipeline_error = None
    s.worker_collector_warning = None
    # New attempt — do not show the previous tick's fatal error while this cycle runs.
    s.last_error = None
    repo.replace_settings(s)


def _after_listing_collection(
    repo: UserRepository,
    s: UserSettingsRow,
    raws: list[RawListing],
    *,
    collector_meta: dict | None = None,
) -> None:
    s.worker_count_raw_collected = len(raws)
    meta = collector_meta or {}
    msg = f"Step 1: {len(raws)} listings found"
    if meta.get("degraded_mode"):
        msg += " — degraded (some Marketplace UI filters skipped)"
    s.worker_pipeline_message = msg
    warn = meta.get("worker_collector_warning")
    s.worker_collector_warning = (str(warn)[:500] if warn else None)
    repo.replace_settings(s)


async def _process_monitoring_user(db: Database, s: UserSettingsRow) -> None:
    repo = UserRepository(db)
    now = datetime.utcnow()

    if s.monitoring_state == "starting":
        s.monitoring_state = "backfill"
        repo.replace_settings(s)

    if not s.backfill_complete:
        s.monitoring_state = "backfill"
        repo.replace_settings(s)
        _begin_listing_collection(repo, s, now)
        try:
            raws, collector_meta = await _collect_raws(s, backfill=True)
        except Exception as exc:
            s.worker_current_state = "collector_error"
            s.worker_pipeline_error = str(exc)[:500]
            s.worker_pipeline_message = "Step 1: Collector failed"
            s.last_checked_at = now
            repo.replace_settings(s)
            raise
        _after_listing_collection(repo, s, raws, collector_meta=collector_meta)
        if raws:
            stats = process_batch(db, raws, profile=s, origin_type="backfill")
            print(
                f"[user={s.user_id}] backfill batch: saved={stats.step4_saved} alerts_sent={stats.alerts_sent}",
                flush=True,
            )
        else:
            _reset_pipeline_cycle_counts(s)
            s.worker_current_step = 0
            s.worker_current_state = "no_listings_this_cycle"
            s.worker_pipeline_message = "Step 1: No listings returned this cycle"
            s.worker_pipeline_error = None
            s.worker_last_success_at = now
            repo.replace_settings(s)
        s.backfill_complete = True
        s.monitoring_state = "polling"
        s.last_checked_at = now
        s.last_error = None
        repo.replace_settings(s)
        return

    s.monitoring_state = "polling"
    repo.replace_settings(s)
    _begin_listing_collection(repo, s, now)
    try:
        raws, collector_meta = await _collect_raws(s, backfill=False)
    except Exception as exc:
        s.worker_current_state = "collector_error"
        s.worker_pipeline_error = str(exc)[:500]
        s.worker_pipeline_message = "Step 1: Collector failed"
        s.last_checked_at = now
        repo.replace_settings(s)
        raise
    _after_listing_collection(repo, s, raws, collector_meta=collector_meta)
    if raws:
        stats = process_batch(db, raws, profile=s, origin_type="live")
        print(
            f"[user={s.user_id}] live batch: saved={stats.step4_saved} alerts_sent={stats.alerts_sent}",
            flush=True,
        )
    else:
        _reset_pipeline_cycle_counts(s)
        s.worker_current_step = 0
        s.worker_current_state = "no_listings_this_cycle"
        s.worker_pipeline_message = "Step 1: No listings returned this cycle"
        s.worker_pipeline_error = None
        s.worker_last_success_at = now
        repo.replace_settings(s)
    s.last_checked_at = now
    s.last_error = None
    repo.replace_settings(s)


async def tick() -> None:
    db: Database = get_database()
    try:
        for doc in db["user_settings"].find({"monitoring_enabled": True}):
            s = settings_from_doc(doc)
            try:
                await _process_monitoring_user(db, s)
            except Exception as exc:  # noqa: BLE001 — surface errors in MVP worker
                s.monitoring_state = "error"
                s.last_error = str(exc)[:500]
                s.last_checked_at = datetime.utcnow()
                UserRepository(db).replace_settings(s)
                print(f"Worker user {s.user_id} error: {exc}", flush=True)
                traceback.print_exc()
    finally:
        pass


async def main_loop() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("collector.playwright_collector").setLevel(logging.INFO)

    ensure_indexes(get_database())
    interval = float(os.environ.get("WORKER_POLL_SECONDS", "300"))
    print(
        f"Worker started. MONGODB_URI={settings.mongodb_uri} db={settings.mongodb_database} "
        f"poll={interval}s (default 300 = 5 min) mock_collector={_mock_collector_enabled()}",
        flush=True,
    )
    while True:
        try:
            await tick()
        except Exception as exc:  # noqa: BLE001
            print(f"Worker tick error: {exc}", flush=True)
            traceback.print_exc()
        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(main_loop())
