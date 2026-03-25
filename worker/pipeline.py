"""
Step 1 → Step 2 → Step 3 (Ollama) → MongoDB → optional Telegram.

Step 1: normalize + light prefilter. Step 2: strict match + Mongo dedupe. Step 3+: scoring (unchanged).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime

from app.domain import UserSettings as UserSettingsRow
from app.models import AlertStatus
from app.repositories.listing_repository import ListingRepository
from app.services.ollama_scoring import score_listing_with_ollama
from app.services.telegram_service import send_profit_alert
from pymongo.database import Database

from candidate_models import CandidateListing
from search_context import build_collection_inputs
from step1_normalize import normalize_raw_to_candidate, prefilter_candidate
from step2_matcher import strict_match
from mock_scraper import RawListing


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


def process_batch(
    db: Database,
    raws: list[RawListing],
    *,
    profile: UserSettingsRow,
    origin_type: str = "live",
) -> int:
    """
    Run Step 1 (normalize + prefilter), Step 2 (strict match + dedupe), then AI + persistence.
    Returns count inserted into MongoDB.
    """
    collection_inputs = build_collection_inputs(profile)

    step1_raw = len(raws)
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
    inserted = 0
    step2_pass = 0
    step2_reject = 0
    step2_reason_counter: Counter[str] = Counter()

    for cand in candidates:
        result = strict_match(cand, profile, db)
        if not result.matched:
            step2_reject += 1
            for r in result.rejection_reasons:
                step2_reason_counter[r] += 1
            continue

        step2_pass += 1
        c = result.candidate_for_ai
        if c is None:
            continue
        norm = normalized_from_candidate(c)
        score = score_listing_with_ollama(
            title=norm.title,
            price=norm.price,
            category_id=norm.category_id,
            location_text=norm.location_text,
            source_url=norm.source_url,
        )
        profitable = score.estimated_profit > 0.0
        if score.should_alert:
            sent = send_profit_alert(
                chat_id=profile.telegram_chat_id,
                title=norm.title,
                source_link=norm.source_link,
                estimated_profit=score.estimated_profit,
            )
            alert_status = AlertStatus.sent.value if sent else AlertStatus.pending.value
        else:
            alert_status = AlertStatus.skipped.value

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
            alert_status=alert_status,
            found_at=datetime.utcnow(),
            origin_type=origin_type,
            ai_result=score.ai_result,
            confidence=score.confidence,
            reasoning=score.reasoning,
            should_alert=score.should_alert,
        )
        if created is not None:
            inserted += 1

    print(
        f"[user={profile.user_id}] listing pipeline: "
        f"step1_raw={step1_raw} step1_kept={step1_kept} step1_prefilter_drop={step1_prefilter_drop} "
        f"prefilter={dict(prefilter_reasons)} | "
        f"step2_pass={step2_pass} step2_reject={step2_reject} step2_reasons={dict(step2_reason_counter)} | "
        f"inserted={inserted}",
        flush=True,
    )
    return inserted
