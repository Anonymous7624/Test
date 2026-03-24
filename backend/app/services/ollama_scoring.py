"""
Score a normalized listing with a local Ollama model using structured JSON output.
Falls back to rule-based profit estimation if Ollama is unreachable.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

import httpx

from app.config import settings
from app.services.profit_estimation import estimate_profit

logger = logging.getLogger(__name__)

OLLAMA_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "estimated_resale": {"type": "number", "description": "USD resale estimate"},
        "estimated_profit": {"type": "number", "description": "USD profit after fees"},
        "confidence": {"type": "number", "description": "0-1 confidence"},
        "reasoning": {"type": "string", "description": "Brief rationale"},
        "should_alert": {"type": "boolean", "description": "Whether user should be alerted"},
    },
    "required": ["estimated_resale", "estimated_profit", "confidence", "reasoning", "should_alert"],
}


@dataclass
class OllamaScoreResult:
    estimated_resale: float
    estimated_profit: float
    confidence: float
    reasoning: str
    should_alert: bool
    ai_result: dict[str, Any]
    used_ollama: bool


def _parse_json_content(raw: str) -> dict[str, Any]:
    text = raw.strip()
    if not text:
        raise ValueError("empty model output")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", text)
        if m:
            return json.loads(m.group(0))
        raise


def score_listing_with_ollama(
    *,
    title: str,
    price: float,
    category_id: str,
    location_text: str,
    source_url: str,
) -> OllamaScoreResult:
    """Call Ollama /api/chat with JSON schema; on failure use estimate_profit fallback."""
    base = (settings.ollama_base_url or "").rstrip("/")
    model = (settings.ollama_model or "llama3.2").strip()
    fallback = estimate_profit(price, category_id)
    fb_dict = {
        "estimated_resale": fallback.estimated_resale,
        "estimated_profit": fallback.estimated_profit,
        "confidence": 0.35,
        "reasoning": "Ollama unavailable; used heuristic estimate.",
        "should_alert": fallback.profitable,
    }
    if not base:
        return OllamaScoreResult(
            estimated_resale=fb_dict["estimated_resale"],
            estimated_profit=fb_dict["estimated_profit"],
            confidence=fb_dict["confidence"],
            reasoning=fb_dict["reasoning"],
            should_alert=fb_dict["should_alert"],
            ai_result={**fb_dict, "model": None},
            used_ollama=False,
        )

    system = (
        "You are a resale analyst. Reply ONLY with a JSON object matching the schema. "
        "USD. Be conservative; set should_alert true only for strong deals."
    )
    user_msg = json.dumps(
        {
            "title": title,
            "asking_price_usd": price,
            "category_id": category_id,
            "location": location_text,
            "source_url": source_url,
        },
        ensure_ascii=False,
    )
    url = f"{base}/api/chat"
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ],
        "format": OLLAMA_JSON_SCHEMA,
        "stream": False,
        "options": {"temperature": 0.2},
    }
    try:
        with httpx.Client(timeout=120.0) as client:
            r = client.post(url, json=payload)
            if r.status_code == 400:
                payload_loose = {
                    **payload,
                    "format": "json",
                }
                r = client.post(url, json=payload_loose)
            r.raise_for_status()
            data = r.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Ollama request failed: %s", exc)
        return OllamaScoreResult(
            estimated_resale=fb_dict["estimated_resale"],
            estimated_profit=fb_dict["estimated_profit"],
            confidence=fb_dict["confidence"],
            reasoning=fb_dict["reasoning"],
            should_alert=fb_dict["should_alert"],
            ai_result={**fb_dict, "model": model, "error": str(exc)[:200]},
            used_ollama=False,
        )

    msg = (data.get("message") or {}) if isinstance(data, dict) else {}
    content = msg.get("content") if isinstance(msg, dict) else None
    if not isinstance(content, str):
        logger.warning("Ollama response missing message.content")
        return OllamaScoreResult(
            estimated_resale=fb_dict["estimated_resale"],
            estimated_profit=fb_dict["estimated_profit"],
            confidence=fb_dict["confidence"],
            reasoning=fb_dict["reasoning"],
            should_alert=fb_dict["should_alert"],
            ai_result={**fb_dict, "model": model, "error": "bad_response"},
            used_ollama=False,
        )

    try:
        parsed = _parse_json_content(content)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Ollama JSON parse failed: %s", exc)
        return OllamaScoreResult(
            estimated_resale=fb_dict["estimated_resale"],
            estimated_profit=fb_dict["estimated_profit"],
            confidence=fb_dict["confidence"],
            reasoning=fb_dict["reasoning"],
            should_alert=fb_dict["should_alert"],
            ai_result={**fb_dict, "model": model, "error": str(exc)[:200]},
            used_ollama=False,
        )

    try:
        er = float(parsed["estimated_resale"])
        ep = float(parsed["estimated_profit"])
        conf = float(parsed["confidence"])
        reason = str(parsed.get("reasoning") or "").strip() or "No reasoning provided."
        alert = bool(parsed.get("should_alert"))
    except (KeyError, TypeError, ValueError) as exc:
        logger.warning("Ollama payload invalid: %s", exc)
        return OllamaScoreResult(
            estimated_resale=fb_dict["estimated_resale"],
            estimated_profit=fb_dict["estimated_profit"],
            confidence=fb_dict["confidence"],
            reasoning=fb_dict["reasoning"],
            should_alert=fb_dict["should_alert"],
            ai_result={**fb_dict, "model": model, "error": str(exc)[:200]},
            used_ollama=False,
        )

    conf = max(0.0, min(1.0, conf))
    ai_result = {
        "estimated_resale": er,
        "estimated_profit": ep,
        "confidence": conf,
        "reasoning": reason,
        "should_alert": alert,
        "model": model,
    }
    return OllamaScoreResult(
        estimated_resale=round(er, 2),
        estimated_profit=round(ep, 2),
        confidence=conf,
        reasoning=reason,
        should_alert=alert,
        ai_result=ai_result,
        used_ollama=True,
    )
