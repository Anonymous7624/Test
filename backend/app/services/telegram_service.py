"""
Telegram Bot API (sendMessage). Bot token from TELEGRAM_BOT_TOKEN env only; chat from user settings.
"""

from __future__ import annotations

import os

import httpx

TELEGRAM_API = "https://api.telegram.org"


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
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token or not chat_id.strip():
        return False, "Missing bot token or chat id"
    return _send_message(token, chat_id.strip(), "Deal dashboard: Telegram test message OK.")


def send_profit_alert(
    *,
    chat_id: str | None,
    title: str,
    source_link: str,
    estimated_profit: float,
) -> bool:
    """Sends alert to the given chat using TELEGRAM_BOT_TOKEN from the environment."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token or not chat_id or not str(chat_id).strip():
        return False
    text = (
        f"Profit alert\n{title}\nEst. profit: {estimated_profit:.2f}\n{source_link}"
    )
    ok, _ = _send_message(token, str(chat_id).strip(), text)
    return ok
