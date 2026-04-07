"""
Post-collection pipeline (steps 2–4). Called after main.py completes step 1 (collection).

Step 2: normalize + light prefilter  (state: step2_normalize, worker_current_step=2)
Step 3: strict match + quality gate  (state: step3_match,     worker_current_step=3)
Step 4: persist to MongoDB + alert   (state: step4_save_alert, worker_current_step=4)
Done:   batch_complete               (state: batch_complete,   worker_current_step=0)

AI scoring (Ollama) is intentionally removed from the active flow.
The code is preserved in backend/app/services/ai_scoring.py and can be re-enabled later.
"""

from __future__ import annotations

import logging
import os
from collections import Counter
from dataclasses import dataclass, fields
from datetime import datetime

from app.domain import UserSettings as UserSettingsRow
from app.models import AlertStatus
from app.repositories.listing_repository import ListingRepository
from app.repositories.user_repository import UserRepository
from app.services.profit_estimation import estimate_profit
from app.services.search_settings import normalize_telegram_alert_mode
from app.services.telegram_service import send_listing_alert
from pymongo.database import Database

from candidate_models import CandidateListing
from search_context import build_collection_inputs
from step1_normalize import normalize_raw_to_candidate, prefilter_candidate
from step2_matcher import strict_match
from step2_pre_ai import pre_ai_should_score  # quality gate: spam/junk/signal checks
from mock_scraper import RawListing

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)).strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)).strip())
    except ValueError:
        return default


@dataclass
class _MatchedJob:
    """Step-2 approved candidate, queued for Step 3 save + alert."""

    cand: CandidateListing
    matched_keywords: list[str]
    heuristic_profit: float
    heuristic_resale: float


def _persisted_listing_location_text(
    cand: CandidateListing,
    *,
    primary_search_location: str,
) -> str:
    """Prefer parsed card location; else candidate location; else search primary."""
    meta = cand.raw_metadata or {}
    parsed = meta.get("listing_location_parsed")
    if isinstance(parsed, str) and parsed.strip():
        return parsed.strip()
    loc = (cand.location_text or "").strip()
    if loc:
        return loc
    prim = (primary_search_location or "").strip()
    return prim or "N/A"


def _flush_pipeline(db: Database, profile: UserSettingsRow) -> None:
    """Persist pipeline fields; bumps last_checked_at so the API/UI show live worker activity."""
    profile.last_checked_at = datetime.utcnow()
    UserRepository(db).replace_settings(profile)


def _write_last_completed_snapshot(profile: UserSettingsRow, stats: "PipelineBatchResult") -> None:
    profile.worker_last_completed_raw_collected = stats.raw_collected
    profile.worker_last_completed_step1_kept = stats.step1_kept
    profile.worker_last_completed_step2_matched = stats.step2_matched
    profile.worker_last_completed_step3_scored = 0  # AI scoring removed; field kept for DB compat
    profile.worker_last_completed_step4_saved = stats.step4_saved
    profile.worker_last_completed_alerts_sent = stats.alerts_sent


@dataclass
class NormalizedListing:
    source_url: str
    source_id: str | None
    title: str
    price: float
    location_text: str
    category_id: str
    source_link: str
    source: str


@dataclass
class PipelineBatchResult:
    """Counters for one worker batch (Steps 1–3)."""

    raw_collected: int
    step1_kept: int
    step1_prefilter_drop: int
    step2_matched: int
    step2_rejected: int
    step3_scored: int  # always 0; kept for DB/UI field compatibility
    step4_saved: int
    alerts_sent: int


def normalized_from_candidate(c: CandidateListing) -> NormalizedListing:
    return NormalizedListing(
        source_url=c.source_url,
        source_id=c.source_id,
        title=c.title,
        price=c.price,
        location_text=c.location_text,
        category_id=c.category_slug,
        source_link=c.source_link,
        source=c.source,
    )


def _condition_from_metadata(raw_metadata: dict) -> str:
    if not isinstance(raw_metadata, dict):
        return ""
    ls = raw_metadata.get("listing_scrape")
    if isinstance(ls, dict):
        v = ls.get("condition")
        if v and str(v).strip():
            return str(v).strip()[:500]
    for key in ("condition", "item_condition", "condition_text"):
        v = raw_metadata.get(key)
        if v and str(v).strip():
            return str(v).strip()[:500]
    return ""


def process_batch(
    db: Database,
    raws: list[RawListing],
    *,
    profile: UserSettingsRow,
    origin_type: str = "live",
) -> PipelineBatchResult:
    """
    Run Step 1–3 (no AI scoring). Step 3 saves to MongoDB before attempting Telegram.
    Per-listing failures are logged; the batch continues.

    profitable_only alert mode: uses heuristic profit estimate (price × category bump).
    Since the heuristic always produces a positive profit for any positive price, profitable_only
    will behave like any_listing in practice while AI scoring is disabled.
    """
    collection_inputs = build_collection_inputs(profile)

    profile.worker_current_step = 2
    profile.worker_current_state = "step2_normalize"
    profile.worker_pipeline_message = "Step 2: Normalizing and prefiltering listings"
    profile.worker_pipeline_error = None
    profile.worker_configuration_error = None
    _flush_pipeline(db, profile)
    logger.info(
        "Batch step 2 start (normalize+prefilter): user_id=%s raw_count=%s",
        profile.user_id,
        len(raws),
    )

    raw_collected = len(raws)
    candidates: list[CandidateListing] = []
    step1_prefilter_drop = 0
    prefilter_reasons: Counter[str] = Counter()

    for raw in raws:
        cand = normalize_raw_to_candidate(
            raw,
            profile,
            collection_inputs,
            origin_type=origin_type,
        )
        ok, reason = prefilter_candidate(cand)
        if not ok:
            step1_prefilter_drop += 1
            if reason:
                prefilter_reasons[reason] += 1
            continue
        candidates.append(cand)

    step1_kept = len(candidates)
    profile.worker_count_raw_collected = raw_collected
    profile.worker_count_step1_kept = step1_kept
    profile.worker_current_step = 3
    profile.worker_current_state = "step3_match"
    profile.worker_pipeline_message = (
        f"Step 3: Matching against filters ({step1_kept} candidates; "
        f"{step1_prefilter_drop} dropped in step 2)"
    )
    _flush_pipeline(db, profile)
    logger.info(
        "Batch step 3 start (match+quality gate): user_id=%s step2_kept=%s step2_prefilter_dropped=%s",
        profile.user_id,
        step1_kept,
        step1_prefilter_drop,
    )

    logger.debug(
        "Step 2 batch input: CandidateListing field names=%s",
        tuple(f.name for f in fields(CandidateListing)),
    )

    repo = ListingRepository(db)
    step2_matched = 0
    step2_rejected = 0
    step2_reason_counter: Counter[str] = Counter()
    step4_saved = 0
    alerts_sent = 0
    pipeline_candidate_errors: Counter[str] = Counter()

    primary_search_loc = collection_inputs.primary_search_location
    matched_jobs: list[_MatchedJob] = []

    for cand in candidates:
        try:
            result = strict_match(cand, profile, db)
            if not result.matched:
                step2_rejected += 1
                for r in result.rejection_reasons:
                    step2_reason_counter[r] += 1
                continue

            c = result.candidate_for_ai
            if c is None:
                continue

            quality_ok, _quality_strength, _quality_rs = pre_ai_should_score(
                c, profile, list(result.matched_keywords)
            )
            if not quality_ok:
                step2_rejected += 1
                step2_reason_counter["quality_gate_failed"] += 1
                continue

            step2_matched += 1
            fb = estimate_profit(c.price, c.category_slug)
            matched_jobs.append(
                _MatchedJob(
                    cand=c,
                    matched_keywords=list(result.matched_keywords),
                    heuristic_profit=fb.estimated_profit,
                    heuristic_resale=fb.estimated_resale,
                )
            )
        except Exception as exc:  # noqa: BLE001
            err_key = f"{type(exc).__name__}:{str(exc)[:160]}"
            pipeline_candidate_errors[err_key] += 1
            if pipeline_candidate_errors[err_key] == 1:
                logger.exception("Pipeline failed for one candidate: %s", exc)
                print(
                    f"[user={profile.user_id}] pipeline: candidate error (continuing): {exc}",
                    flush=True,
                )
            else:
                logger.debug(
                    "Pipeline candidate error (repeat %s for same pattern): %s",
                    pipeline_candidate_errors[err_key],
                    exc,
                )
            continue

    profile.worker_current_step = 4
    profile.worker_current_state = "step4_save_alert"
    profile.worker_count_step2_matched = step2_matched
    profile.worker_count_step3_scored = 0
    profile.worker_pipeline_step3_total = 0
    profile.worker_pipeline_step3_rank = 0
    profile.worker_pipeline_message = (
        f"Step 4: Saving {len(matched_jobs)} matched listings and sending alerts"
    )
    _flush_pipeline(db, profile)
    logger.info(
        "Batch step 4 start (save+alert): user_id=%s step3_matched=%s",
        profile.user_id,
        step2_matched,
    )

    for idx, job in enumerate(matched_jobs):
        c = job.cand
        try:
            profile.worker_pipeline_error = None
            norm = normalized_from_candidate(c)
            listing_loc = _persisted_listing_location_text(
                c,
                primary_search_location=primary_search_loc,
            )

            fb = estimate_profit(c.price, c.category_slug)
            profitable = fb.estimated_profit > 0.0

            has_chat = bool((profile.telegram_chat_id or "").strip())
            tg_mode = normalize_telegram_alert_mode(getattr(profile, "telegram_alert_mode", None))

            profile.worker_pipeline_message = (
                f"Step 4: saving {idx + 1}/{len(matched_jobs)} — {norm.title[:60]}"
            )
            _flush_pipeline(db, profile)

            print(
                f"[user={profile.user_id}] step3: saving {idx + 1}/{len(matched_jobs)} "
                f"title={norm.title[:80]!r} profitable={profitable} url={norm.source_url}",
                flush=True,
            )

            if tg_mode == "none":
                init_alert_status = AlertStatus.skipped.value
                init_alert_err: str | None = "telegram_alert_mode_none"
            elif tg_mode == "profitable_only" and not profitable:
                init_alert_status = AlertStatus.skipped.value
                init_alert_err = None
            elif not has_chat:
                init_alert_status = AlertStatus.pending.value
                init_alert_err = "telegram_chat_not_configured"
            else:
                init_alert_status = AlertStatus.pending.value
                init_alert_err = None

            try:
                _meta = c.raw_metadata or {}
                _ls = _meta.get("listing_scrape")
                _scrape_meta = _ls if isinstance(_ls, dict) else None

                created = repo.create(
                    user_id=profile.user_id,
                    source_url=norm.source_url,
                    source_id=norm.source_id,
                    title=norm.title,
                    price=norm.price,
                    estimated_resale=fb.estimated_resale,
                    estimated_profit=fb.estimated_profit,
                    category_id=norm.category_id,
                    location_text=listing_loc,
                    source_link=norm.source_link,
                    source=norm.source,
                    profitable=profitable,
                    alert_status=init_alert_status,
                    found_at=datetime.utcnow(),
                    origin_type=origin_type,
                    description=(c.description or "").strip() or None,
                    matched_keywords=list(job.matched_keywords),
                    scraped_at=c.scraped_at,
                    ai_result=None,
                    confidence=None,
                    reasoning=None,
                    should_alert=None,
                    alert_sent=False,
                    alert_sent_at=None,
                    alert_last_error=init_alert_err,
                    scrape_metadata=_scrape_meta,
                )
            except Exception as exc:  # noqa: BLE001
                profile.worker_pipeline_error = f"Save listing failed: {str(exc)[:400]}"
                _flush_pipeline(db, profile)
                logger.exception("Step 3 save failed: %s", exc)
                print(f"[user={profile.user_id}] step3: save failed: {exc}", flush=True)
                continue

            if created is None:
                logger.warning(
                    "Duplicate listing skipped (user_id=%s source_url=%s)",
                    profile.user_id,
                    norm.source_url,
                )
                print(
                    f"[user={profile.user_id}] step3: duplicate source_url skipped {norm.source_url}",
                    flush=True,
                )
                continue

            step4_saved += 1
            print(
                f"[user={profile.user_id}] step3: saved listing id={created.id} url={norm.source_url}",
                flush=True,
            )

            should_send_telegram = (
                tg_mode != "none"
                and has_chat
                and (tg_mode == "any_listing" or (tg_mode == "profitable_only" and profitable))
            )
            if should_send_telegram:
                ok, terr = send_listing_alert(
                    chat_id=profile.telegram_chat_id,
                    title=norm.title,
                    price=norm.price,
                    estimated_resale=fb.estimated_resale,
                    estimated_profit=fb.estimated_profit,
                    location_text=listing_loc,
                    description=(c.description or "").strip() or None,
                    source_url=norm.source_url or norm.source_link,
                )
                now = datetime.utcnow()
                if ok:
                    repo.set_alert_delivery(
                        listing_id=created.id,
                        user_id=profile.user_id,
                        alert_sent=True,
                        alert_status=AlertStatus.sent.value,
                        alert_sent_at=now,
                        alert_last_error=None,
                    )
                    alerts_sent += 1
                    print(
                        f"[user={profile.user_id}] step3: telegram sent listing id={created.id}",
                        flush=True,
                    )
                else:
                    repo.set_alert_delivery(
                        listing_id=created.id,
                        user_id=profile.user_id,
                        alert_sent=False,
                        alert_status=AlertStatus.failed.value,
                        alert_sent_at=None,
                        alert_last_error=terr or "telegram_send_failed",
                    )
                    print(
                        f"[user={profile.user_id}] step3: telegram failed listing id={created.id} err={terr}",
                        flush=True,
                    )

        except Exception as exc:  # noqa: BLE001
            err_key = f"{type(exc).__name__}:{str(exc)[:160]}"
            pipeline_candidate_errors[err_key] += 1
            if pipeline_candidate_errors[err_key] == 1:
                logger.exception("Pipeline failed for one candidate: %s", exc)
                print(
                    f"[user={profile.user_id}] pipeline: candidate error (continuing): {exc}",
                    flush=True,
                )
            else:
                logger.debug(
                    "Pipeline candidate error (repeat %s for same pattern): %s",
                    pipeline_candidate_errors[err_key],
                    exc,
                )
            continue

    stats = PipelineBatchResult(
        raw_collected=raw_collected,
        step1_kept=step1_kept,
        step1_prefilter_drop=step1_prefilter_drop,
        step2_matched=step2_matched,
        step2_rejected=step2_rejected,
        step3_scored=0,
        step4_saved=step4_saved,
        alerts_sent=alerts_sent,
    )
    # Snapshot the completed batch results into last_completed fields.
    _write_last_completed_snapshot(profile, stats)
    # Reset current (in-progress) counts to zero: the batch is done.
    # last_completed fields now hold the summary; current counts reflect no active batch.
    profile.worker_count_raw_collected = 0
    profile.worker_count_step1_kept = 0
    profile.worker_count_step2_matched = 0
    profile.worker_count_step3_scored = 0
    profile.worker_count_step4_saved = 0
    profile.worker_count_alerts_sent = 0
    profile.worker_pipeline_step3_rank = 0
    profile.worker_pipeline_step3_total = 0
    profile.worker_current_step = 0
    profile.worker_current_state = "batch_complete"
    profile.worker_last_success_at = datetime.utcnow()
    logger.info(
        "Batch complete: user_id=%s collected=%s step2_kept=%s step3_matched=%s step4_saved=%s alerts=%s",
        profile.user_id,
        stats.raw_collected,
        stats.step1_kept,
        stats.step2_matched,
        stats.step4_saved,
        stats.alerts_sent,
    )
    if pipeline_candidate_errors:
        logger.warning(
            "Pipeline batch: %d candidate-level exception(s) across %d unique error pattern(s): %s",
            sum(pipeline_candidate_errors.values()),
            len(pipeline_candidate_errors),
            dict(pipeline_candidate_errors),
        )
    profile.worker_pipeline_error = None
    profile.worker_pipeline_message = (
        f"Batch complete: collected={stats.raw_collected} matched={stats.step2_matched} "
        f"saved={stats.step4_saved} alerts={stats.alerts_sent}"
    )
    profile.worker_configuration_error = None
    _flush_pipeline(db, profile)

    print(
        f"[user={profile.user_id}] pipeline summary: "
        f"raw={stats.raw_collected} step1_kept={stats.step1_kept} "
        f"prefilter_drop={stats.step1_prefilter_drop} prefilter={dict(prefilter_reasons)} | "
        f"step2_matched={stats.step2_matched} step2_reject={stats.step2_rejected} "
        f"step2_reasons={dict(step2_reason_counter)} | "
        f"step4_saved={stats.step4_saved} alerts_sent={stats.alerts_sent}",
        flush=True,
    )

    def _step2_rejection_bucket(reason: str) -> str:
        if reason == "duplicate_user_source_url":
            return "duplicate_detection"
        if reason in ("invalid_price", "non_positive_price"):
            return "bad_price"
        if reason in ("pre_ai_low_signal", "quality_gate_failed"):
            return "quality_gate"
        if reason == "location_outside_radius":
            return "location_mismatch"
        if reason in (
            "category_keyword_mismatch",
            "category_mismatch_no_keywords_configured",
            "category_slug_mismatch",
            "custom_keyword_no_match",
            "unknown_search_mode",
        ):
            return "weak_keyword_or_category"
        return "other"

    bucket_counts: Counter[str] = Counter()
    for r, n in step2_reason_counter.items():
        bucket_counts[_step2_rejection_bucket(r)] += n
    logger.info(
        "Step 2 rejection buckets user_id=%s: duplicate_detection=%s bad_price=%s quality_gate=%s "
        "location_mismatch=%s weak_keyword_or_category=%s other=%s (raw_reasons=%s)",
        profile.user_id,
        bucket_counts.get("duplicate_detection", 0),
        bucket_counts.get("bad_price", 0),
        bucket_counts.get("quality_gate", 0),
        bucket_counts.get("location_mismatch", 0),
        bucket_counts.get("weak_keyword_or_category", 0),
        bucket_counts.get("other", 0),
        dict(step2_reason_counter),
    )
    loc_miss = bucket_counts.get("location_mismatch", 0)
    if loc_miss > 0:
        logger.info(
            "Step 2 location note user_id=%s: %s location_mismatch reject(s) are from the "
            "Step 2 text-based geo check on listing location text — this is NOT caused by the "
            "Date-listed / 24h UI filter (which is applied at Facebook UI level before collection). "
            "Card-visible location pre-screening (Step 1) should have reduced these; remaining "
            "rejects are listings whose location was not parsed from the card or passed card-screen "
            "but failed the post-enrichment Step 2 check.",
            profile.user_id,
            loc_miss,
        )
    return stats
