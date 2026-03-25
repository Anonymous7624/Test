"""
Step 1 → Step 2 → Step 3 (AI scoring) → Step 4 (MongoDB + optional Telegram).

Step 1: normalize + light prefilter. Step 2: strict match + Mongo dedupe.
Step 3: Ollama scoring for Step-2 matches (priority queue + optional cap). Step 4: persist first, then alert.
"""

from __future__ import annotations

import logging
import os
import threading
from collections import Counter
from dataclasses import dataclass, fields
from datetime import datetime

from app.config import settings
from app.domain import UserSettings as UserSettingsRow
from app.models import AlertStatus
from app.repositories.listing_repository import ListingRepository
from app.repositories.user_repository import UserRepository
from app.services.ai_scoring import MatchedCandidateInput, Step3ScoreResult, score_matched_candidate
from app.services.profit_estimation import estimate_profit
from app.services.search_settings import normalize_telegram_alert_mode
from app.services.telegram_service import send_listing_alert
from pymongo.database import Database

from candidate_models import CandidateListing
from search_context import build_collection_inputs
from step1_normalize import normalize_raw_to_candidate, prefilter_candidate
from step2_matcher import strict_match
from step2_pre_ai import pre_ai_should_score
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
class _AIJob:
    """Step-2 + pre-AI approved candidate, queued for Step 3 with priority hints."""

    cand: CandidateListing
    matched_keywords: list[str]
    pre_strength: float
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


def _write_last_completed_snapshot(profile: UserSettingsRow, stats: PipelineBatchResult) -> None:
    profile.worker_last_completed_raw_collected = stats.raw_collected
    profile.worker_last_completed_step1_kept = stats.step1_kept
    profile.worker_last_completed_step2_matched = stats.step2_matched
    profile.worker_last_completed_step3_scored = stats.step3_scored
    profile.worker_last_completed_step4_saved = stats.step4_saved
    profile.worker_last_completed_alerts_sent = stats.alerts_sent


def _heartbeat_during_blocking(
    db: Database,
    profile: UserSettingsRow,
    *,
    interval_sec: float,
) -> tuple[threading.Event, threading.Thread]:
    """Periodically persist last_checked_at while a blocking call (Ollama HTTP) runs."""
    stop = threading.Event()

    def _run() -> None:
        while not stop.wait(interval_sec):
            profile.last_checked_at = datetime.utcnow()
            try:
                UserRepository(db).replace_settings(profile)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Step 3 heartbeat DB write failed: %s", exc)

    t = threading.Thread(target=_run, daemon=True, name="step3_ai_heartbeat")
    t.start()
    return stop, t


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
    """Counters for one worker batch (Steps 1–4)."""

    raw_collected: int
    step1_kept: int
    step1_prefilter_drop: int
    step2_matched: int
    step2_rejected: int
    step3_scored: int
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
    Run Step 1–4. Step 4 saves to MongoDB before attempting Telegram; updates delivery after send.
    Per-listing failures are logged; the batch continues.
    """
    collection_inputs = build_collection_inputs(profile)

    profile.worker_current_step = 1
    profile.worker_current_state = "step1_normalize"
    profile.worker_pipeline_message = "Step 1: Normalizing and prefiltering listings"
    profile.worker_pipeline_error = None
    profile.worker_configuration_error = None
    _flush_pipeline(db, profile)

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
    profile.worker_current_step = 2
    profile.worker_current_state = "step2_match"
    profile.worker_pipeline_message = (
        f"Step 2: Matching against user filters ({step1_kept} candidates after prefilter; "
        f"{step1_prefilter_drop} dropped in step 1)"
    )
    _flush_pipeline(db, profile)

    logger.debug(
        "Step 2 batch input: CandidateListing field names (Step 1 → Step 2 contract)=%s",
        tuple(f.name for f in fields(CandidateListing)),
    )

    repo = ListingRepository(db)
    step2_matched = 0
    step2_rejected = 0
    step2_reason_counter: Counter[str] = Counter()
    step3_scored = 0
    step4_saved = 0
    alerts_sent = 0
    pipeline_candidate_errors: Counter[str] = Counter()

    primary_search_loc = collection_inputs.primary_search_location
    ai_jobs: list[_AIJob] = []

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

            pre_ok, pre_strength, _pre_rs = pre_ai_should_score(
                c, profile, list(result.matched_keywords)
            )
            if not pre_ok:
                step2_rejected += 1
                step2_reason_counter["pre_ai_low_signal"] += 1
                continue

            step2_matched += 1
            fb = estimate_profit(c.price, c.category_slug)
            ai_jobs.append(
                _AIJob(
                    cand=c,
                    matched_keywords=list(result.matched_keywords),
                    pre_strength=pre_strength,
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

    ai_jobs.sort(
        key=lambda j: (-j.heuristic_profit, -j.pre_strength, -j.heuristic_resale),
    )
    ai_cap = _env_int("WORKER_STEP3_AI_CAP", 50)
    strong_min_strength = _env_float("WORKER_OLLAMA_STRONG_MIN_STRENGTH", 0.72)
    strong_min_hprofit = _env_float("WORKER_OLLAMA_STRONG_MIN_HEURISTIC_PROFIT", 25.0)
    base_timeout = float(settings.ollama_timeout or 180.0)
    strong_bonus = float(getattr(settings, "ollama_timeout_strong_bonus", 30.0))
    per_candidate_max = _env_float("WORKER_STEP3_AI_CANDIDATE_MAX_SECONDS", 300.0)
    heartbeat_sec = _env_float("WORKER_STEP3_HEARTBEAT_SECONDS", 15.0)

    if len(ai_jobs) > ai_cap:
        logger.warning(
            "Step 3: WORKER_STEP3_AI_CAP=%s — Ollama for top %s by heuristic priority; "
            "%s lower-priority listings use heuristic-only scoring",
            ai_cap,
            ai_cap,
            len(ai_jobs) - ai_cap,
        )

    profile.worker_current_step = 3
    profile.worker_current_state = "step3_ai_scoring"
    profile.worker_count_step2_matched = step2_matched
    profile.worker_pipeline_step3_total = len(ai_jobs)
    profile.worker_pipeline_step3_rank = 0
    profile.worker_pipeline_message = (
        f"Step 3: AI queue {len(ai_jobs)} (sorted by heuristic profit; cap={ai_cap})"
    )
    _flush_pipeline(db, profile)

    for idx, job in enumerate(ai_jobs):
        c = job.cand
        try:
            profile.worker_pipeline_error = None
            norm = normalized_from_candidate(c)
            listing_loc = _persisted_listing_location_text(
                c,
                primary_search_location=primary_search_loc,
            )
            use_ollama_slot = idx < ai_cap
            is_strong = job.pre_strength >= strong_min_strength or (
                job.heuristic_profit >= strong_min_hprofit
            )
            timeout = min(
                base_timeout + (strong_bonus if is_strong else 0.0),
                per_candidate_max,
            )

            profile.worker_pipeline_step3_rank = idx + 1
            profile.worker_pipeline_message = (
                f"Step 3: scoring rank {idx + 1}/{len(ai_jobs)} {norm.title[:48]}…"
            )
            _flush_pipeline(db, profile)

            print(
                f"[user={profile.user_id}] step3: rank={idx + 1}/{len(ai_jobs)} "
                f"title={norm.title[:80]!r} url={norm.source_url}",
                flush=True,
            )

            step3_input = MatchedCandidateInput(
                title=c.title,
                price=c.price,
                category_id=c.category_slug,
                description=c.description or "",
                location_text=c.location_text,
                matched_keywords=list(job.matched_keywords),
                source_url=c.source_url,
                condition_text=_condition_from_metadata(c.raw_metadata),
            )

            hb_stop: threading.Event | None = None
            hb_thread: threading.Thread | None = None
            if use_ollama_slot and heartbeat_sec > 0:
                hb_stop, hb_thread = _heartbeat_during_blocking(
                    db, profile, interval_sec=heartbeat_sec
                )

            if use_ollama_slot:
                try:
                    score = score_matched_candidate(
                        step3_input,
                        timeout_seconds=timeout,
                    )
                except Exception as exc:  # noqa: BLE001
                    profile.worker_pipeline_error = (
                        f"AI scoring exception (continuing): {str(exc)[:400]}"
                    )
                    _flush_pipeline(db, profile)
                    logger.exception("Step 3 unexpected error: %s", exc)
                    fb = estimate_profit(c.price, c.category_slug)
                    score = Step3ScoreResult(
                        estimated_resale=fb.estimated_resale,
                        estimated_profit=fb.estimated_profit,
                        confidence="low",
                        reasoning=f"Scoring failed unexpectedly; heuristic only. ({str(exc)[:160]})",
                        should_alert=False,
                        used_ollama=False,
                        ai_result={
                            "estimated_resale": fb.estimated_resale,
                            "estimated_profit": fb.estimated_profit,
                            "confidence": "low",
                            "reasoning": "worker_exception",
                            "should_alert": False,
                            "model": None,
                            "scoring_error": str(exc)[:500],
                            "used_ollama": False,
                        },
                    )
                finally:
                    if hb_stop is not None:
                        hb_stop.set()
                    if hb_thread is not None:
                        hb_thread.join(timeout=2.0)
            else:
                fb = estimate_profit(c.price, c.category_slug)
                score = Step3ScoreResult(
                    estimated_resale=fb.estimated_resale,
                    estimated_profit=fb.estimated_profit,
                    confidence="low",
                    reasoning="Heuristic only: beyond WORKER_STEP3_AI_CAP priority queue.",
                    should_alert=bool(fb.profitable),
                    used_ollama=False,
                    ai_result={
                        "estimated_resale": fb.estimated_resale,
                        "estimated_profit": fb.estimated_profit,
                        "confidence": "low",
                        "reasoning": "ai_cap_exceeded",
                        "should_alert": bool(fb.profitable),
                        "model": None,
                        "used_ollama": False,
                        "skipped_ollama_due_to_cap": True,
                    },
                )

            fallback_used = not score.used_ollama
            logger.info(
                "Step 3 user_id=%s rank=%s/%s priority_pre_strength=%.3f heuristic_profit=%.2f "
                "ollama_slot=%s timeout=%.1f cap=%.1f strong=%s used_ollama=%s fallback=%s",
                profile.user_id,
                idx + 1,
                len(ai_jobs),
                job.pre_strength,
                job.heuristic_profit,
                use_ollama_slot,
                timeout if use_ollama_slot else 0.0,
                per_candidate_max,
                is_strong,
                score.used_ollama,
                fallback_used,
            )

            step3_scored += 1
            profile.worker_count_step3_scored = step3_scored
            profile.worker_pipeline_message = f"Step 3: {step3_scored} scored (latest profit est. ${score.estimated_profit:.2f})"
            _flush_pipeline(db, profile)
            status = "ok" if score.used_ollama else "fallback"
            print(
                f"[user={profile.user_id}] step3: {status} profit={score.estimated_profit:.2f} "
                f"should_alert={score.should_alert} used_ollama={score.used_ollama} "
                f"fallback={fallback_used} timeout={(timeout if use_ollama_slot else 0.0):.1f}s",
                flush=True,
            )

            profitable = score.estimated_profit > 0.0
            step4_fields = score.to_step4_fields()
            has_chat = bool((profile.telegram_chat_id or "").strip())
            tg_mode = normalize_telegram_alert_mode(getattr(profile, "telegram_alert_mode", None))

            profile.worker_current_step = 4
            profile.worker_current_state = "step4_save_alert"
            profile.worker_pipeline_message = "Step 4: Saving results / sending alerts"
            _flush_pipeline(db, profile)

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
                created = repo.create(
                    user_id=profile.user_id,
                    source_url=norm.source_url,
                    source_id=norm.source_id,
                    title=norm.title,
                    price=norm.price,
                    estimated_resale=score.estimated_resale,
                    estimated_profit=score.estimated_profit,
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
                    ai_result=step4_fields["ai_result"],
                    confidence=step4_fields["confidence"],
                    reasoning=step4_fields["reasoning"],
                    should_alert=step4_fields["should_alert"],
                    alert_sent=False,
                    alert_sent_at=None,
                    alert_last_error=init_alert_err,
                )
            except Exception as exc:  # noqa: BLE001
                profile.worker_pipeline_error = f"Save listing failed: {str(exc)[:400]}"
                _flush_pipeline(db, profile)
                logger.exception("Step 4 save failed: %s", exc)
                print(f"[user={profile.user_id}] step4: save failed: {exc}", flush=True)
                continue

            if created is None:
                logger.warning(
                    "Duplicate listing skipped (user_id=%s source_url=%s)",
                    profile.user_id,
                    norm.source_url,
                )
                print(
                    f"[user={profile.user_id}] step4: duplicate source_url skipped {norm.source_url}",
                    flush=True,
                )
                continue

            step4_saved += 1
            print(
                f"[user={profile.user_id}] step4: saved listing id={created.id} url={norm.source_url}",
                flush=True,
            )

            should_send_telegram = (
                tg_mode != "none"
                and has_chat
                and (tg_mode == "any_listing" or (tg_mode == "profitable_only" and profitable))
            )
            if should_send_telegram:
                conf_for_alert = step4_fields.get("confidence")
                ok, terr = send_listing_alert(
                    chat_id=profile.telegram_chat_id,
                    title=norm.title,
                    price=norm.price,
                    estimated_resale=score.estimated_resale,
                    estimated_profit=score.estimated_profit,
                    location_text=listing_loc,
                    description=(c.description or "").strip() or None,
                    source_url=norm.source_url or norm.source_link,
                    confidence=conf_for_alert if conf_for_alert is not None else None,
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
                        f"[user={profile.user_id}] step4: telegram sent listing id={created.id}",
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
                        f"[user={profile.user_id}] step4: telegram failed listing id={created.id} err={terr}",
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
        step3_scored=step3_scored,
        step4_saved=step4_saved,
        alerts_sent=alerts_sent,
    )
    _write_last_completed_snapshot(profile, stats)
    profile.worker_count_raw_collected = stats.raw_collected
    profile.worker_count_step1_kept = stats.step1_kept
    profile.worker_count_step2_matched = stats.step2_matched
    profile.worker_count_step3_scored = stats.step3_scored
    profile.worker_count_step4_saved = stats.step4_saved
    profile.worker_count_alerts_sent = stats.alerts_sent
    profile.worker_pipeline_step3_rank = 0
    profile.worker_pipeline_step3_total = 0
    profile.worker_current_step = 0
    profile.worker_current_state = "batch_complete"
    profile.worker_last_success_at = datetime.utcnow()
    if pipeline_candidate_errors:
        logger.warning(
            "Pipeline batch: %d candidate-level exception(s) across %d unique error pattern(s): %s",
            sum(pipeline_candidate_errors.values()),
            len(pipeline_candidate_errors),
            dict(pipeline_candidate_errors),
        )
    # Batch finished without aborting — clear any per-item step errors so the site does not
    # show stale "AI scoring failed" / "Save listing failed" after a successful completion.
    profile.worker_pipeline_error = None
    profile.worker_pipeline_message = (
        f"Batch complete: collected={stats.raw_collected} matched={stats.step2_matched} "
        f"scored={stats.step3_scored} saved={stats.step4_saved} alerts={stats.alerts_sent}"
    )
    profile.worker_configuration_error = None
    _flush_pipeline(db, profile)

    print(
        f"[user={profile.user_id}] pipeline summary: "
        f"raw={stats.raw_collected} step1_kept={stats.step1_kept} "
        f"prefilter_drop={stats.step1_prefilter_drop} prefilter={dict(prefilter_reasons)} | "
        f"step2_matched={stats.step2_matched} step2_reject={stats.step2_rejected} "
        f"step2_reasons={dict(step2_reason_counter)} | "
        f"step3_scored={stats.step3_scored} step4_saved={stats.step4_saved} alerts_sent={stats.alerts_sent}",
        flush=True,
    )

    def _step2_rejection_bucket(reason: str) -> str:
        if reason == "duplicate_user_source_url":
            return "duplicate_detection"
        if reason in ("invalid_price", "non_positive_price"):
            return "bad_price"
        if reason == "pre_ai_low_signal":
            return "pre_ai_gate"
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
        "Step 2 rejection buckets user_id=%s: duplicate_detection=%s bad_price=%s pre_ai_gate=%s "
        "location_mismatch=%s weak_keyword_or_category=%s other=%s (raw_reasons=%s)",
        profile.user_id,
        bucket_counts.get("duplicate_detection", 0),
        bucket_counts.get("bad_price", 0),
        bucket_counts.get("pre_ai_gate", 0),
        bucket_counts.get("location_mismatch", 0),
        bucket_counts.get("weak_keyword_or_category", 0),
        bucket_counts.get("other", 0),
        dict(step2_reason_counter),
    )
    return stats
