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
)


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
    min_strength = _env_float("WORKER_PRE_AI_MIN_STRENGTH", 0.38)
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
    if sm == "custom_keywords":
        tl = title.lower()
        dl = desc.lower()
        for mk in matched_keywords:
            m = mk.strip().lower()
            if m and m in tl:
                score += 0.38
                reasons.append("keyword_in_title")
                break
        for mk in matched_keywords:
            m = mk.strip().lower()
            if m and m in dl:
                score += 0.18
                reasons.append("keyword_in_desc")
                break
        if score < 0.2 and matched_keywords:
            # Phrase matched blob in strict_match but weak title signal — small boost
            score += 0.1
            reasons.append("blob_match_carryover")
    else:
        score += 0.28
        reasons.append("marketplace_category_scoped")

    if _SPAM_CAPS.search(title) and len(title) < 100:
        score -= 0.15
        reasons.append("spam_caps_penalty")

    tl = title.lower()
    if any(j in tl for j in _JUNK_TITLE):
        return False, score, ["spam_phrase"]

    # Very low prices often noise; still allow if custom keyword strong
    p = float(candidate.price)
    if p < 3:
        score -= 0.12
        reasons.append("suspicious_low_price")

    if 4 <= p <= 80_000:
        score += 0.1
        reasons.append("price_range_ok")

    if len(desc) >= 20:
        score += 0.08
        reasons.append("has_description")

    strong = score >= min_strength
    if not strong:
        reasons.append("below_pre_ai_threshold")
    return strong, min(1.0, max(0.0, score)), reasons
