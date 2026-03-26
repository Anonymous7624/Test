"""
Step 3: AI scoring for listings that already passed Step 2 (strict matcher).

Calls local Ollama with a concise prompt and strict JSON schema; validates the response
and falls back conservatively without crashing the worker.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Literal

import httpx

from app.config import settings
from app.services.profit_estimation import estimate_profit

logger = logging.getLogger(__name__)

ConfidenceLevel = Literal["low", "medium", "high"]

CONFIDENCE_VALUES: frozenset[str] = frozenset({"low", "medium", "high"})

OLLAMA_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "estimated_resale": {"type": "number", "description": "Conservative USD resale estimate"},
        "estimated_profit": {"type": "number", "description": "USD profit after realistic fees"},
        "confidence": {
            "type": "string",
            "enum": ["low", "medium", "high"],
            "description": "Uncertainty of the estimate",
        },
        "reasoning": {"type": "string", "description": "Short factual rationale"},
        "should_alert": {"type": "boolean", "description": "True only if deal is clearly strong"},
    },
    "required": [
        "estimated_resale",
        "estimated_profit",
        "confidence",
        "reasoning",
        "should_alert",
    ],
}


@dataclass(frozen=True)
class MatchedCandidateInput:
    """
    Normalized input from Step 2 only — no raw scrape blobs.
    Built by the worker pipeline from CandidateListing + matched_keywords.
    """

    title: str
    price: float
    category_id: str
    description: str
    location_text: str
    matched_keywords: list[str]
    source_url: str
    condition_text: str = ""


@dataclass
class Step3ScoreResult:
    """Normalized Step 3 output for Step 4 (persistence / dashboard)."""

    estimated_resale: float
    estimated_profit: float
    confidence: str
    reasoning: str
    should_alert: bool
    used_ollama: bool
    ai_result: dict[str, Any]

    def to_step4_fields(self) -> dict[str, Any]:
        """Flat fields suitable for listing insert and dashboard."""
        return {
            "estimated_resale": self.estimated_resale,
            "estimated_profit": self.estimated_profit,
            "confidence": self.confidence,
            "reasoning": self.reasoning,
            "should_alert": self.should_alert,
            "ai_result": self.ai_result,
        }


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


def _normalize_confidence(raw: Any) -> str:
    if raw is None:
        return "low"
    if isinstance(raw, str):
        s = raw.strip().lower()
        if s in CONFIDENCE_VALUES:
            return s
    if isinstance(raw, (int, float)):
        x = float(raw)
        if x < 0.34:
            return "low"
        if x < 0.67:
            return "medium"
        return "high"
    return "low"


def _trim(s: str, max_len: int) -> str:
    t = (s or "").strip()
    if len(t) <= max_len:
        return t
    return t[: max_len - 1].rstrip() + "…"


def _build_prompt_payload(inp: MatchedCandidateInput) -> dict[str, Any]:
    kw = [k for k in inp.matched_keywords if k and str(k).strip()]
    return {
        "title": _trim(inp.title, 300),
        "asking_price_usd": round(float(inp.price), 2),
        "category": _trim(inp.category_id, 120),
        "description": _trim(inp.description, 1600),
        "condition": _trim(inp.condition_text, 400),
        "location": _trim(inp.location_text, 200),
        "matched_keywords": kw[:24],
        "source_url": _trim(inp.source_url, 500),
    }


def _heuristic_fallback(inp: MatchedCandidateInput) -> tuple[dict[str, Any], bool]:
    fb = estimate_profit(inp.price, inp.category_id)
    doc = {
        "estimated_resale": fb.estimated_resale,
        "estimated_profit": fb.estimated_profit,
        "confidence": "low",
        "reasoning": "Heuristic estimate (Ollama unavailable or not configured).",
        "should_alert": bool(fb.profitable),
    }
    return doc, fb.profitable


def _failure_result(
    *,
    inp: MatchedCandidateInput,
    model: str | None,
    error: str,
    conservative_alert: bool,
    fallback_reason: str = "ollama_request_failed",
) -> Step3ScoreResult:
    fb, profitable_hint = _heuristic_fallback(inp)
    fb["should_alert"] = bool(conservative_alert and profitable_hint)
    err_short = str(error).replace("\n", " ")[:180]
    fb["reasoning"] = f"{fb['reasoning']} ({err_short})"
    ai = {
        **fb,
        "model": model,
        "scoring_error": error[:500],
        "used_ollama": False,
        "scoring_source": "heuristic_fallback",
        "fallback_reason": fallback_reason,
    }
    return Step3ScoreResult(
        estimated_resale=round(float(fb["estimated_resale"]), 2),
        estimated_profit=round(float(fb["estimated_profit"]), 2),
        confidence=str(fb["confidence"]),
        reasoning=str(fb["reasoning"]),
        should_alert=bool(fb["should_alert"]),
        used_ollama=False,
        ai_result=ai,
    )


def score_matched_candidate(
    inp: MatchedCandidateInput,
    *,
    timeout_seconds: float | None = None,
) -> Step3ScoreResult:
    """
    Score a single Step-2–approved candidate via Ollama. Does not raise on model/network errors.

    ``timeout_seconds`` overrides the default ``OLLAMA_TIMEOUT_SECONDS`` / ``OLLAMA_TIMEOUT`` for this
    request (e.g. stronger candidates get a longer budget from the worker).
    """
    base = (settings.ollama_base_url or "").strip().rstrip("/")
    model = (settings.ollama_model or "llama3.2").strip()
    default_timeout = float(settings.ollama_timeout or 300.0)
    timeout = float(timeout_seconds if timeout_seconds is not None else default_timeout)

    if not base:
        fb, _ = _heuristic_fallback(inp)
        ai = {
            **fb,
            "model": None,
            "used_ollama": False,
            "scoring_source": "heuristic_fallback",
            "fallback_reason": "ollama_base_url_not_configured",
        }
        return Step3ScoreResult(
            estimated_resale=round(float(fb["estimated_resale"]), 2),
            estimated_profit=round(float(fb["estimated_profit"]), 2),
            confidence=str(fb["confidence"]),
            reasoning=str(fb["reasoning"]),
            should_alert=bool(fb["should_alert"]),
            used_ollama=False,
            ai_result=ai,
        )

    system = (
        "You are a conservative resale analyst for second-hand marketplace listings. "
        "Estimate realistic resale and profit; avoid hype. "
        "Prefer under-stating resale over over-stating. "
        "Set should_alert to true only when profit is clearly positive after fees and risk. "
        "Reply with one JSON object only, no markdown, matching the requested schema."
    )
    user_obj = {
        "task": "estimate_flip",
        "listing": _build_prompt_payload(inp),
        "rules": [
            "USD only.",
            "Default assumption: the item is USED / pre-owned unless the title or description clearly says it is new, "
            "open-box, sealed, BNIB, NIB, or otherwise unused. Only treat it as new-like when that is explicit in the text.",
            "Assume typical marketplace fees/shipping risk unless description says otherwise.",
            "If data is thin, lower confidence and avoid should_alert.",
        ],
    }
    user_msg = json.dumps(user_obj, ensure_ascii=False)
    url = f"{base}/api/chat"
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ],
        "format": OLLAMA_JSON_SCHEMA,
        "stream": False,
        "options": {"temperature": 0.15, "top_p": 0.9},
    }

    title_hint = _trim(inp.title, 120)
    # Long read for slow local models; connect stays bounded so dead hosts fail fast.
    httpx_timeout = httpx.Timeout(
        connect=min(60.0, max(10.0, timeout * 0.25)),
        read=timeout,
        write=min(120.0, timeout),
        pool=timeout,
    )
    logger.info(
        "Ollama request starting url=%s model=%s timeout_seconds=%.1f httpx_read=%.1f connect=%.1f title=%s",
        url,
        model,
        timeout,
        httpx_timeout.read,
        httpx_timeout.connect,
        title_hint,
    )

    try:
        with httpx.Client(timeout=httpx_timeout) as client:
            r = client.post(url, json=payload)
            if r.status_code == 400:
                loose = {**payload, "format": "json"}
                r = client.post(url, json=loose)
            r.raise_for_status()
            data = r.json()
    except httpx.TimeoutException as exc:
        logger.warning(
            "Ollama request timed out (read/connect budget exhausted) timeout_seconds=%.1f "
            "httpx_read=%.1f url=%s model=%s title=%s: %s",
            timeout,
            float(httpx_timeout.read),
            url,
            model,
            title_hint,
            exc,
        )
        return _failure_result(
            inp=inp,
            model=model,
            error=f"timeout_after_{timeout}s:{exc}",
            conservative_alert=False,
            fallback_reason="ollama_http_timeout",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Ollama request failed (timeout_seconds=%.1f url=%s model=%s): %s",
            timeout,
            url,
            model,
            exc,
        )
        return _failure_result(
            inp=inp,
            model=model,
            error=str(exc),
            conservative_alert=False,
        )

    msg = (data.get("message") or {}) if isinstance(data, dict) else {}
    content = msg.get("content") if isinstance(msg, dict) else None
    if not isinstance(content, str):
        logger.warning("Ollama response missing message.content")
        return _failure_result(
            inp=inp,
            model=model,
            error="bad_response_shape",
            conservative_alert=False,
        )

    try:
        parsed = _parse_json_content(content)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Ollama JSON parse failed: %s", exc)
        return _failure_result(
            inp=inp,
            model=model,
            error=f"json_parse:{exc}",
            conservative_alert=False,
        )

    try:
        er = float(parsed["estimated_resale"])
        conf = _normalize_confidence(parsed.get("confidence"))
        reason = str(parsed.get("reasoning") or "").strip() or "No reasoning provided."
        raw_should_alert = bool(parsed.get("should_alert"))
    except (KeyError, TypeError, ValueError) as exc:
        logger.warning("Ollama payload invalid: %s", exc)
        return _failure_result(
            inp=inp,
            model=model,
            error=f"invalid_fields:{exc}",
            conservative_alert=False,
        )

    price = float(inp.price)
    er = round(er, 2)
    # Ollama resale is authoritative; profit is derived so it matches UI math (resale − price).
    ep = round(er - price, 2)
    if "estimated_profit" in parsed:
        try:
            _model_ep = float(parsed["estimated_profit"])
            logger.debug(
                "Ollama model estimated_profit=%s ignored in favor of resale−price=%s",
                _model_ep,
                ep,
            )
        except (TypeError, ValueError):
            pass

    # Align should_alert with derived profit (resale − price).
    alert = raw_should_alert and ep > 0.0

    logger.info(
        "Ollama request succeeded timeout_seconds=%.1f model=%s title=%s estimated_profit=%.2f",
        timeout,
        model,
        title_hint,
        ep,
    )

    ai_result = {
        "estimated_resale": er,
        "estimated_profit": ep,
        "confidence": conf,
        "reasoning": reason,
        "should_alert": alert,
        "model": model,
        "used_ollama": True,
        "scoring_source": "ollama",
        "profit_derived_from_resale": True,
    }
    return Step3ScoreResult(
        estimated_resale=er,
        estimated_profit=ep,
        confidence=conf,
        reasoning=reason,
        should_alert=alert,
        used_ollama=True,
        ai_result=ai_result,
    )
