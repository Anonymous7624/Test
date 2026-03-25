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


def send_profit_alert(
    *,
    chat_id: str | None,
    title: str,
    source_link: str,
    estimated_profit: float,
) -> tuple[bool, str | None]:
    """
    Sends alert to the given user's chat (multi-user: always pass that user's chat_id).
    Returns (success, error_message_if_failed).
    """
    token = _bot_token()
    if not token:
        return False, "telegram_bot_token_not_configured"
    if not chat_id or not str(chat_id).strip():
        return False, "telegram_chat_not_configured"
    text = (
        f"Profit alert\n{title}\nEst. profit: {estimated_profit:.2f}\n{source_link}"
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
