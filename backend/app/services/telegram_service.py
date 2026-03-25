"""
Telegram Bot API (sendMessage). Bot token from Settings (TELEGRAM_BOT_TOKEN); chat from user settings.
"""

from __future__ import annotations

from typing import Any

import httpx

from app.config import settings

TELEGRAM_API = "https://api.telegram.org"


def _bot_token() -> str:
    return (settings.telegram_bot_token or "").strip()


def _send_message(bot_token: str, chat_id: str, text: str) -> tuple[bool, str | None]:
    url = f"{TELEGRAM_API}/bot{bot_token}/sendMessage"
    try:
        r = httpx.post(
            url,
            json={"chat_id": chat_id, "text": text},
            timeout=20.0,
        )
        if r.is_success:
            return True, None
        return False, r.text
    except Exception as exc:  # noqa: BLE001 — surface to API caller
        return False, str(exc)


def send_test_message(chat_id: str) -> tuple[bool, str | None]:
    token = _bot_token()
    if not token or not chat_id.strip():
        return False, "Missing bot token or chat id"
    return _send_message(token, chat_id.strip(), "Deal dashboard: Telegram test message OK.")


def send_verification_success(chat_id: str) -> None:
    token = _bot_token()
    if not token or not str(chat_id).strip():
        return
    cid = str(chat_id).strip()
    _send_message(
        token,
        cid,
        (
            "Deal dashboard: Telegram linked successfully. You will receive profit alerts here.\n"
            f"Your chat id (for reference): {cid}"
        ),
    )


def _na_money(value: float | None) -> str:
    if value is None:
        return "N/A"
    try:
        return f"${float(value):.2f}"
    except (TypeError, ValueError):
        return "N/A"


def _na_text(value: str | None, *, max_len: int = 320) -> str:
    t = (value or "").strip()
    if not t:
        return "N/A"
    if len(t) <= max_len:
        return t
    return t[: max_len - 1].rstrip() + "…"


def _first_n_words(text: str | None, *, max_words: int = 30) -> str:
    """First ~N words of listing body; safe on empty / odd whitespace."""
    raw = (text or "").strip()
    if not raw:
        return "N/A"
    words = raw.split()
    if len(words) <= max_words:
        return " ".join(words)
    return " ".join(words[:max_words]) + "…"


def _fmt_confidence(raw: str | float | int | None) -> str:
    if raw is None:
        return "N/A"
    if isinstance(raw, (int, float)):
        x = float(raw)
        if 0.0 <= x <= 1.0:
            return f"{x:.0%}"
        return f"{x:.2f}"
    s = str(raw).strip()
    return s if s else "N/A"


def build_listing_alert_text(
    *,
    title: str,
    price: float | None,
    estimated_resale: float | None,
    estimated_profit: float | None,
    location_text: str | None,
    description: str | None,
    source_url: str,
    confidence: str | float | int | None,
) -> str:
    """Deterministic Telegram body — no LLM text."""
    lines = [
        "Listing alert",
        f"Title: {_na_text(title, max_len=500)}",
        f"Price: {_na_money(price)}",
        f"Est. retail / resale: {_na_money(estimated_resale)}",
        f"Est. profit (retail − price): {_na_money(estimated_profit)}",
        f"Listing location: {_na_text(location_text, max_len=240)}",
        f"Confidence: {_fmt_confidence(confidence)}",
        f"Description (snippet): {_first_n_words(description, max_words=30)}",
        f"URL: {(source_url or '').strip() or 'N/A'}",
    ]
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3997] + "…"
    return text


def send_listing_alert(
    *,
    chat_id: str | None,
    title: str,
    price: float | None,
    estimated_resale: float | None,
    estimated_profit: float | None,
    location_text: str | None,
    description: str | None,
    source_url: str,
    confidence: str | float | int | None,
) -> tuple[bool, str | None]:
    """
    Sends a deterministic template alert to the user's Telegram chat.
    Returns (success, error_message_if_failed).
    """
    token = _bot_token()
    if not token:
        return False, "telegram_bot_token_not_configured"
    if not chat_id or not str(chat_id).strip():
        return False, "telegram_chat_not_configured"
    text = build_listing_alert_text(
        title=title,
        price=price,
        estimated_resale=estimated_resale,
        estimated_profit=estimated_profit,
        location_text=location_text,
        description=description,
        source_url=source_url,
        confidence=confidence,
    )
    return _send_message(token, str(chat_id).strip(), text)


def fetch_updates(*, offset: int | None = None, timeout: int = 0) -> tuple[list[dict[str, Any]], int | None]:
    """Long-poll Telegram updates; returns (results, next_offset)."""
    token = _bot_token()
    if not token:
        return [], offset
    url = f"{TELEGRAM_API}/bot{token}/getUpdates"
    params: dict[str, Any] = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset
    try:
        r = httpx.get(url, params=params, timeout=float(timeout + 25))
        if not r.is_success:
            return [], offset
        data = r.json()
        if not data.get("ok"):
            return [], offset
        results = data.get("result") or []
        next_off = offset
        for u in results:
            uid = u.get("update_id")
            if isinstance(uid, int):
                next_off = uid + 1
        return results, next_off
    except Exception:  # noqa: BLE001
        return [], offset
