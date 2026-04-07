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

# ---------------------------------------------------------------------------
# Heartbeat stale threshold (seconds).
# The worker writes a liveness ping before each tick; the API uses this
# to determine whether the worker process is actually running.
# Must be > max expected tick duration + poll interval.
# Default: 300 s (5 min). Override with WORKER_HEARTBEAT_STALE_SECONDS.
# ---------------------------------------------------------------------------
_HEARTBEAT_STALE_SECONDS = float(os.environ.get("WORKER_HEARTBEAT_STALE_SECONDS", "300"))

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
from collector.errors import CollectorInterruptedError  # noqa: E402
from collector.playwright_collector import (  # noqa: E402
    FacebookAuthStateMissingError,
    fetch_listings_playwright,
)
from mock_scraper import RawListing, mock_fetch_backfill, mock_fetch_batch  # noqa: E402
from pipeline import process_batch  # noqa: E402
from search_context import build_collection_inputs  # noqa: E402
from search_plan import SearchPlanInvalidError, validate_search_plan_for_step1  # noqa: E402

logger = logging.getLogger(__name__)


def _update_heartbeat(db) -> None:
    """Write a global liveness ping to ``worker_meta`` so the API can detect whether
    the worker process is actually running.  Failures are non-fatal — the worker
    continues even if the heartbeat write fails (e.g. transient network blip).
    """
    try:
        db["worker_meta"].update_one(
            {"_id": "heartbeat"},
            {
                "$set": {
                    "last_ping_at": datetime.utcnow(),
                    "pid": os.getpid(),
                },
                "$setOnInsert": {"first_seen_at": datetime.utcnow()},
            },
            upsert=True,
        )
    except Exception as exc:
        logger.warning("Could not update worker heartbeat: %s", exc)


def _mock_collector_enabled() -> bool:
    return os.environ.get("WORKER_MOCK_COLLECTOR", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _get_known_source_ids(db: Database, user_id: int, limit: int = 500) -> frozenset[str]:
    """
    Fetch recently stored source IDs and source links for this user from MongoDB.

    These are passed to the collector so it can skip detail-page enrichment for listings
    already saved, avoiding wasted browser page-visits before the Step-2 pipeline dedupe.
    Returns an empty frozenset on any DB error (fail-open; dedupe still happens in Step 2).
    """
    try:
        ids: set[str] = set()
        for doc in db["listings"].find(
            {"user_id": user_id},
            {"source_id": 1, "source_link": 1, "_id": 0},
            limit=limit,
            sort=[("_id", -1)],
        ):
            sid = (doc.get("source_id") or "").strip()
            if sid:
                ids.add(sid)
            # Also index the normalised URL so raw source_link hits work.
            slink = (doc.get("source_link") or "").strip()
            if slink:
                ids.add(slink)
        return frozenset(ids)
    except Exception as exc:
        logger.warning(
            "Pre-collector dedupe: could not load known_source_ids user_id=%s: %s",
            user_id,
            exc,
        )
        return frozenset()


async def _collect_raws(
    profile: UserSettingsRow,
    *,
    backfill: bool,
    known_source_ids: frozenset[str] = frozenset(),
) -> tuple[list[RawListing], dict]:
    """Returns (raw listings, collector metadata e.g. degraded UI mode)."""
    from dataclasses import replace as _dc_replace  # noqa: PLC0415

    inputs = build_collection_inputs(profile)
    if known_source_ids:
        inputs = _dc_replace(inputs, known_source_ids=known_source_ids)
    validate_search_plan_for_step1(inputs.search_plan)
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
    except CollectorInterruptedError:
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


def _persist_empty_cycle_last_completed(s: UserSettingsRow) -> None:
    """No listings returned — snapshot last completed batch as an empty cycle."""
    s.worker_last_completed_raw_collected = 0
    s.worker_last_completed_step1_kept = 0
    s.worker_last_completed_step2_matched = 0
    s.worker_last_completed_step3_scored = 0
    s.worker_last_completed_step4_saved = 0
    s.worker_last_completed_alerts_sent = 0


def _begin_listing_collection(
    repo: UserRepository, s: UserSettingsRow, now: datetime, *, backfill: bool
) -> None:
    """Persist pipeline state before Playwright/mock fetch (step 1)."""
    _reset_pipeline_cycle_counts(s)
    s.worker_last_batch_started_at = now
    s.worker_current_step = 1
    s.worker_current_state = "collecting_listings"
    s.worker_pipeline_message = "Step 1: Looking for listings"
    s.worker_pipeline_error = None
    s.worker_collector_warning = None
    # New batch — drop prior collector failure snapshot so the UI is not stuck on an old run.
    s.worker_last_collector_failure_at = None
    s.worker_last_collector_failure_message = None
    s.worker_configuration_error = None
    s.worker_pipeline_step3_rank = 0
    s.worker_pipeline_step3_total = 0
    # New attempt — do not show the previous tick's fatal error while this cycle runs.
    s.last_error = None
    repo.replace_settings(s)
    search_mode = getattr(s, "search_mode", "?")
    category = getattr(s, "marketplace_category_slug", None) or getattr(s, "marketplace_category_label", None)
    keywords = getattr(s, "custom_keywords", None) or []
    logger.info(
        "Batch started: user_id=%s backfill=%s stage=collect_listings "
        "search_mode=%s category=%r keywords=%r mock_collector=%s",
        s.user_id,
        backfill,
        search_mode,
        category,
        list(keywords)[:5],
        _mock_collector_enabled(),
    )


def _persist_batch_interrupted(
    repo: UserRepository,
    s: UserSettingsRow,
    now: datetime,
    *,
    backfill: bool,
) -> None:
    """Manual stop / cancellation / browser closed — not a fatal collector failure."""
    s.worker_current_state = "batch_interrupted"
    s.worker_pipeline_message = "Step 1: Interrupted (collection stopped)"
    s.worker_pipeline_error = None
    s.worker_last_collector_failure_at = None
    s.worker_last_collector_failure_message = None
    s.worker_configuration_error = None
    s.last_error = None
    s.last_checked_at = now
    repo.replace_settings(s)
    logger.info(
        "Batch interrupted: user_id=%s backfill=%s (no collector_error recorded)",
        s.user_id,
        backfill,
    )


def _persist_configuration_error(
    repo: UserRepository,
    s: UserSettingsRow,
    now: datetime,
    *,
    backfill: bool,
    message: str,
) -> None:
    """Invalid search settings — not a Playwright/collector failure."""
    msg = (message or "").strip() or "Search settings are incomplete or invalid."
    s.worker_current_state = "configuration_error"
    s.worker_pipeline_message = msg
    s.worker_configuration_error = msg[:500]
    s.worker_pipeline_error = None
    s.worker_collector_warning = None
    s.last_error = None
    s.monitoring_state = "error"
    s.last_checked_at = now
    repo.replace_settings(s)
    logger.warning(
        "Search configuration invalid user_id=%s backfill=%s: %s",
        s.user_id,
        backfill,
        msg[:200],
    )


def _after_listing_collection(
    repo: UserRepository,
    s: UserSettingsRow,
    raws: list[RawListing],
    *,
    collector_meta: dict | None = None,
    prior_collector_failure_message: str | None = None,
) -> None:
    if prior_collector_failure_message:
        logger.info(
            "Collector recovered: user_id=%s raw_count=%s (after prior failure: %s)",
            s.user_id,
            len(raws),
            prior_collector_failure_message[:200],
        )
    s.worker_count_raw_collected = len(raws)
    meta = collector_meta or {}
    msg = f"Step 1: {len(raws)} listings found"
    if meta.get("degraded_mode"):
        msg += " — degraded (some Marketplace UI filters skipped)"
    s.worker_pipeline_message = msg
    warn = meta.get("worker_collector_warning")
    s.worker_collector_warning = (str(warn)[:500] if warn else None)
    # Collector succeeded — a prior tick may still have left last_error in DB until we clear it here.
    s.last_error = None
    s.worker_configuration_error = None
    repo.replace_settings(s)
    logger.info(
        "Collector success (worker): user_id=%s raw_count=%s",
        s.user_id,
        len(raws),
    )


async def _process_monitoring_user(db: Database, s: UserSettingsRow) -> None:
    repo = UserRepository(db)
    now = datetime.utcnow()

    if s.monitoring_state == "starting":
        s.monitoring_state = "backfill"
        repo.replace_settings(s)

    if not s.backfill_complete:
        s.monitoring_state = "backfill"
        repo.replace_settings(s)
        prior_collector_failure = s.worker_last_collector_failure_message
        _begin_listing_collection(repo, s, now, backfill=True)
        try:
            raws, collector_meta = await _collect_raws(s, backfill=True)
        except SearchPlanInvalidError as exc:
            _persist_configuration_error(
                repo, s, now, backfill=True, message=str(exc)
            )
            return
        except CollectorInterruptedError:
            _persist_batch_interrupted(repo, s, now, backfill=True)
            return
        except asyncio.CancelledError:
            _persist_batch_interrupted(repo, s, now, backfill=True)
            raise
        except KeyboardInterrupt:
            _persist_batch_interrupted(repo, s, now, backfill=True)
            raise
        except Exception as exc:
            s.worker_current_state = "collector_error"
            s.worker_pipeline_error = str(exc)[:500]
            s.worker_pipeline_message = "Step 1: Collector failed"
            s.worker_last_collector_failure_at = now
            s.worker_last_collector_failure_message = str(exc)[:500]
            s.last_checked_at = now
            repo.replace_settings(s)
            raise
        _after_listing_collection(
            repo,
            s,
            raws,
            collector_meta=collector_meta,
            prior_collector_failure_message=prior_collector_failure,
        )
        if raws:
            stats = process_batch(db, raws, profile=s, origin_type="backfill")
            print(
                f"[user={s.user_id}] backfill batch: saved={stats.step4_saved} alerts_sent={stats.alerts_sent}",
                flush=True,
            )
        else:
            _reset_pipeline_cycle_counts(s)
            _persist_empty_cycle_last_completed(s)
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
        logger.info(
            "Batch finished: user_id=%s backfill=True raw_collected=%s monitoring=polling",
            s.user_id,
            s.worker_count_raw_collected,
        )
        return

    s.monitoring_state = "polling"
    repo.replace_settings(s)
    prior_collector_failure = s.worker_last_collector_failure_message
    _begin_listing_collection(repo, s, now, backfill=False)
    # Load persisted source IDs so the collector can skip detail-page enrichment for duplicates
    # before they hit the Step-2 pipeline dedupe.  Fail-open: if the query fails we still collect.
    live_known_ids = _get_known_source_ids(db, s.user_id)
    if live_known_ids:
        logger.info(
            "Pre-collector dedupe: user_id=%s known_source_ids_loaded=%s "
            "(detail-enrich will be skipped for these)",
            s.user_id,
            len(live_known_ids),
        )
    try:
        raws, collector_meta = await _collect_raws(s, backfill=False, known_source_ids=live_known_ids)
    except SearchPlanInvalidError as exc:
        _persist_configuration_error(
            repo, s, now, backfill=False, message=str(exc)
        )
        return
    except CollectorInterruptedError:
        _persist_batch_interrupted(repo, s, now, backfill=False)
        return
    except asyncio.CancelledError:
        _persist_batch_interrupted(repo, s, now, backfill=False)
        raise
    except KeyboardInterrupt:
        _persist_batch_interrupted(repo, s, now, backfill=False)
        raise
    except Exception as exc:
        s.worker_current_state = "collector_error"
        s.worker_pipeline_error = str(exc)[:500]
        s.worker_pipeline_message = "Step 1: Collector failed"
        s.worker_last_collector_failure_at = now
        s.worker_last_collector_failure_message = str(exc)[:500]
        s.last_checked_at = now
        repo.replace_settings(s)
        raise
    _after_listing_collection(
        repo,
        s,
        raws,
        collector_meta=collector_meta,
        prior_collector_failure_message=prior_collector_failure,
    )
    if raws:
        stats = process_batch(db, raws, profile=s, origin_type="live")
        screen = collector_meta.get("screen_summary") or {}
        print(
            f"[user={s.user_id}] live batch: "
            f"collected={screen.get('collected_from_page', len(raws))} "
            f"early_loc_rejected={screen.get('rejected_early_by_visible_location', 0)} "
            f"unknown_loc={screen.get('unknown_location_passed', 0)} "
            f"pre_enrich_dupes={screen.get('pre_enrich_known_dupes', 0)} "
            f"detail_enriched={screen.get('detail_enriched_ok', 0)} "
            f"passed_to_pipeline={len(raws)} "
            f"step2_matched={stats.step2_matched} "
            f"saved={stats.step4_saved} "
            f"alerts_sent={stats.alerts_sent}",
            flush=True,
        )
    else:
        _reset_pipeline_cycle_counts(s)
        _persist_empty_cycle_last_completed(s)
        s.worker_current_step = 0
        s.worker_current_state = "no_listings_this_cycle"
        s.worker_pipeline_message = "Step 1: No listings returned this cycle"
        s.worker_pipeline_error = None
        s.worker_last_success_at = now
        repo.replace_settings(s)
    s.last_checked_at = now
    s.last_error = None
    repo.replace_settings(s)
    logger.info(
        "Batch finished: user_id=%s backfill=False raw_collected=%s monitoring=polling",
        s.user_id,
        s.worker_count_raw_collected,
    )


async def tick() -> None:
    db: Database = get_database()
    now_tick = datetime.utcnow()
    logger.info(
        "Worker tick started at %s UTC — mongodb=%r db=%r",
        now_tick.strftime("%Y-%m-%d %H:%M:%S"),
        settings.mongodb_uri,
        settings.mongodb_database,
    )
    # Update global liveness heartbeat BEFORE processing users so the API can
    # tell the worker is alive even during a long Playwright collection.
    _update_heartbeat(db)
    try:
        docs = list(db["user_settings"].find({"monitoring_enabled": True}))
        if not docs:
            logger.info(
                "Worker tick: no users with monitoring_enabled=True — "
                "nothing to do. Enable monitoring in Settings to start collecting."
            )
            return
        logger.info("Worker tick: found %s user(s) with monitoring_enabled=True", len(docs))
        for doc in docs:
            s = settings_from_doc(doc)
            logger.info(
                "Worker tick: picking up user_id=%s monitoring_state=%s "
                "backfill_complete=%s search_mode=%s",
                s.user_id,
                s.monitoring_state,
                getattr(s, "backfill_complete", True),
                getattr(s, "search_mode", "?"),
            )
            try:
                await _process_monitoring_user(db, s)
            except Exception as exc:  # noqa: BLE001 — surface errors in MVP worker
                s.monitoring_state = "error"
                s.last_error = str(exc)[:500]
                s.last_checked_at = datetime.utcnow()
                UserRepository(db).replace_settings(s)
                print(f"Worker user {s.user_id} error: {exc}", flush=True)
                traceback.print_exc()
            # Refresh heartbeat after each (potentially long) user tick so stale
            # detection stays accurate when multiple users are being processed.
            _update_heartbeat(db)
    finally:
        pass


async def main_loop() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("collector.playwright_collector").setLevel(logging.INFO)

    db = get_database()
    ensure_indexes(db)
    interval = float(os.environ.get("WORKER_POLL_SECONDS", "150"))
    mock_mode = _mock_collector_enabled()

    print("=" * 60, flush=True)
    print("Worker starting up", flush=True)
    print(f"  MongoDB URI  : {settings.mongodb_uri!r}", flush=True)
    print(f"  MongoDB DB   : {settings.mongodb_database!r}", flush=True)
    print(f"  Poll interval: {interval}s", flush=True)
    print(f"  Heartbeat stale threshold: {_HEARTBEAT_STALE_SECONDS}s "
          f"(WORKER_HEARTBEAT_STALE_SECONDS)", flush=True)
    print(f"  Mock collector: {mock_mode} (set WORKER_MOCK_COLLECTOR=1 to enable)", flush=True)
    # Write startup heartbeat immediately so the API can detect this worker within seconds.
    _update_heartbeat(db)
    print(f"  Heartbeat written to worker_meta collection (pid={os.getpid()})", flush=True)
    if not mock_mode:
        try:
            from collector.playwright_collector import facebook_auth_state_path  # noqa: PLC0415
            auth_path = facebook_auth_state_path()
            auth_ok = auth_path.is_file()
            print(f"  Facebook auth : {'OK' if auth_ok else 'MISSING'} ({auth_path})", flush=True)
            if not auth_ok:
                print(
                    "  WARNING: Facebook auth file not found. "
                    "Run `python facebook_login_bootstrap.py` once to create it.",
                    flush=True,
                )
        except Exception as _e:
            print(f"  Facebook auth : (could not check — {_e})", flush=True)
    # Show how many users currently have monitoring enabled
    try:
        enabled_count = db["user_settings"].count_documents({"monitoring_enabled": True})
        print(f"  Users with monitoring_enabled=True: {enabled_count}", flush=True)
        if enabled_count == 0:
            print(
                "  NOTE: No users have monitoring enabled yet. "
                "Enable monitoring in the Settings page to start collecting.",
                flush=True,
            )
    except Exception as _e:
        print(f"  (Could not query user_settings: {_e})", flush=True)
    print(f"  First tick will run immediately, then every {interval}s", flush=True)
    print("=" * 60, flush=True)

    while True:
        try:
            await tick()
        except Exception as exc:  # noqa: BLE001
            print(f"Worker tick error: {exc}", flush=True)
            traceback.print_exc()
        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(main_loop())
