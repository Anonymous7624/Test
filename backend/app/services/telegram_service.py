"""
Telegram alert abstraction.

TODO: Implement Bot API sendMessage using telegram_bot_token + telegram_chat_id from user settings.
TODO: Add retry/backoff and rate limiting for production.
TODO: Wire Cloudflare Tunnel or public webhook URL if Telegram cannot reach localhost.

For now: read TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID from env in worker or call send_alert stub.
"""

import os
from dataclasses import dataclass


@dataclass
class TelegramConfig:
    bot_token: str | None
    chat_id: str | None


def config_from_env() -> TelegramConfig:
    return TelegramConfig(
        bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
        chat_id=os.getenv("TELEGRAM_CHAT_ID"),
    )


def send_alert(title: str, source_link: str, estimated_profit: float) -> bool:
    """
    Placeholder: returns True if env vars present (simulated send).
    TODO: httpx POST to https://api.telegram.org/bot{token}/sendMessage
    """
    cfg = config_from_env()
    if not cfg.bot_token or not cfg.chat_id:
        return False
    # TODO: implement real API call; keep stub to avoid network in dev by default
    _ = (title, source_link, estimated_profit)
    return True
