"""Pre-AI gate: only higher-signal listings are sent to Ollama (Step 3)."""

from __future__ import annotations

import os
import re

from app.domain import UserSettings as UserSettingsRow

from candidate_models import CandidateListing

_SPAM_CAPS = re.compile(r"[A-Z]{6,}")
_JUNK_TITLE = (
    "click here",
    "dm me",
    "cashapp",
    "cash app",
    "whatsapp only",
    "text me",
    "link in bio",
    "follow me",
    "crypto",
    "bitcoin",
)
# Titles that look like ISO / bulk listings (weak for resale scoring)
_BULK_LISTING = re.compile(
    r"\b(lot of|assorted|bundle|misc|various|mixed lot|wholesale|pallet)\b",
    re.I,
)
_BRANDISH = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\b")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)).strip())
    except ValueError:
        return default


def pre_ai_should_score(
    candidate: CandidateListing,
    profile: UserSettingsRow,
    matched_keywords: list[str],
) -> tuple[bool, float, list[str]]:
    """
    Returns (send_to_ai, strength 0..1, debug reasons).
    Stricter than Step 1 — reduces noisy listings reaching the LLM after max-price removal.
    """
    min_strength = _env_float("WORKER_PRE_AI_MIN_STRENGTH", 0.42)
    title = (candidate.title or "").strip()
    desc = (candidate.description or "").strip()
    reasons: list[str] = []
    score = 0.0

    if len(title) < 4:
        return False, 0.0, ["title_too_short"]
    if len(title) >= 10:
        score += 0.12
        reasons.append("title_substantial")

    sm = (profile.search_mode or "marketplace_category").strip()
    tl = title.lower()
    dl = desc.lower()
    blob = f"{tl} {dl}"

    if sm == "custom_keywords":
        for mk in matched_keywords:
            m = mk.strip().lower()
            if m and m in tl:
                score += 0.42
                reasons.append("keyword_in_title")
                break
        for mk in matched_keywords:
            m = mk.strip().lower()
            if m and m in dl:
                score += 0.22
                reasons.append("keyword_in_desc")
                break
        # Partial token overlap (e.g. "iphone" in "iPhone 13")
        if score < 0.25 and matched_keywords:
            for mk in matched_keywords:
                parts = [p for p in re.split(r"\s+", mk.strip().lower()) if len(p) >= 3]
                for p in parts:
                    if p in blob:
                        score += 0.28
                        reasons.append("keyword_token_overlap")
                        break
                else:
                    continue
                break
        if score < 0.18 and matched_keywords:
            score += 0.08
            reasons.append("blob_match_carryover")
    else:
        score += 0.22
        reasons.append("marketplace_category_scoped")
        slug = (profile.marketplace_category_slug or "").strip().lower()
        if slug and slug.replace("-", " ") in blob:
            score += 0.12
            reasons.append("category_hint_in_text")

    if _SPAM_CAPS.search(title) and len(title) < 100:
        score -= 0.18
        reasons.append("spam_caps_penalty")

    if any(j in tl for j in _JUNK_TITLE):
        return False, score, ["spam_phrase"]

    if _BULK_LISTING.search(title) and len(title) < 80:
        score -= 0.12
        reasons.append("bulk_listing_penalty")

    if _BRANDISH.search(title):
        score += 0.08
        reasons.append("possible_brand_or_model")

    p = float(candidate.price)
    if p < 3:
        score -= 0.14
        reasons.append("suspicious_low_price")

    if 4 <= p <= 80_000:
        score += 0.12
        reasons.append("price_range_ok")

    if len(desc) >= 40:
        score += 0.12
        reasons.append("substantial_description")
    elif len(desc) >= 20:
        score += 0.06
        reasons.append("has_description")

    strong = score >= min_strength
    if not strong:
        reasons.append("below_pre_ai_threshold")
    return strong, min(1.0, max(0.0, score)), reasons
