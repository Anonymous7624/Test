"""
Step 1 → Step 2 → Step 3 (AI scoring) → Step 4 (MongoDB + optional Telegram).

Step 1: normalize + light prefilter. Step 2: strict match + Mongo dedupe.
Step 3: Ollama scoring only for Step-2 matches. Step 4: persist first, then alert.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime

from app.domain import UserSettings as UserSettingsRow
from app.models import AlertStatus
from app.repositories.listing_repository import ListingRepository
from app.services.ai_scoring import MatchedCandidateInput, Step3ScoreResult, score_matched_candidate
from app.services.profit_estimation import estimate_profit
from app.services.telegram_service import send_profit_alert
from pymongo.database import Database

from candidate_models import CandidateListing
from search_context import build_collection_inputs
from step1_normalize import normalize_raw_to_candidate, prefilter_candidate
from step2_matcher import strict_match
from mock_scraper import RawListing

logger = logging.getLogger(__name__)


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
        ok, reason = prefilter_candidate(cand, max_price=collection_inputs.max_price)
        if not ok:
            step1_prefilter_drop += 1
            if reason:
                prefilter_reasons[reason] += 1
            continue
        candidates.append(cand)

    step1_kept = len(candidates)
    repo = ListingRepository(db)
    step2_matched = 0
    step2_rejected = 0
    step2_reason_counter: Counter[str] = Counter()
    step3_scored = 0
    step4_saved = 0
    alerts_sent = 0

    for cand in candidates:
        try:
            result = strict_match(cand, profile, db)
            if not result.matched:
                step2_rejected += 1
                for r in result.rejection_reasons:
                    step2_reason_counter[r] += 1
                continue

            step2_matched += 1
            c = result.candidate_for_ai
            if c is None:
                continue

            norm = normalized_from_candidate(c)
            print(
                f"[user={profile.user_id}] step3: scoring title={norm.title[:80]!r} url={norm.source_url}",
                flush=True,
            )

            step3_input = MatchedCandidateInput(
                title=c.title,
                price=c.price,
                category_id=c.category_slug,
                description=c.description or "",
                location_text=c.location_text,
                matched_keywords=list(result.matched_keywords),
                source_url=c.source_url,
                condition_text=_condition_from_metadata(c.raw_metadata),
            )

            try:
                score = score_matched_candidate(step3_input)
            except Exception as exc:  # noqa: BLE001
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

            step3_scored += 1
            status = "ok" if score.used_ollama else "fallback"
            print(
                f"[user={profile.user_id}] step3: {status} profit={score.estimated_profit:.2f} "
                f"should_alert={score.should_alert} used_ollama={score.used_ollama}",
                flush=True,
            )

            profitable = score.estimated_profit > 0.0
            step4_fields = score.to_step4_fields()
            has_chat = bool((profile.telegram_chat_id or "").strip())

            if not score.should_alert:
                init_alert_status = AlertStatus.skipped.value
                init_alert_err: str | None = None
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
                    location_text=norm.location_text,
                    source_link=norm.source_link,
                    source=norm.source,
                    profitable=profitable,
                    alert_status=init_alert_status,
                    found_at=datetime.utcnow(),
                    origin_type=origin_type,
                    description=(c.description or "").strip() or None,
                    matched_keywords=list(result.matched_keywords),
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

            if score.should_alert and has_chat:
                ok, terr = send_profit_alert(
                    chat_id=profile.telegram_chat_id,
                    title=norm.title,
                    source_link=norm.source_link,
                    estimated_profit=score.estimated_profit,
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
            logger.exception("Pipeline failed for one candidate: %s", exc)
            print(f"[user={profile.user_id}] pipeline: candidate error (continuing): {exc}", flush=True)
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
    print(
        f"[user={profile.user_id}] pipeline summary: "
        f"raw={stats.raw_collected} step1_kept={stats.step1_kept} "
        f"prefilter_drop={stats.step1_prefilter_drop} prefilter={dict(prefilter_reasons)} | "
        f"step2_matched={stats.step2_matched} step2_reject={stats.step2_rejected} "
        f"step2_reasons={dict(step2_reason_counter)} | "
        f"step3_scored={stats.step3_scored} step4_saved={stats.step4_saved} alerts_sent={stats.alerts_sent}",
        flush=True,
    )
    return stats
