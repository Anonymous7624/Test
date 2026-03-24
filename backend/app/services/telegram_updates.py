"""
Poll Telegram getUpdates and bind /start CODE to user settings (single consumer for the bot).
"""

from __future__ import annotations

import re
from datetime import datetime

from pymongo.database import Database

from app.services.telegram_service import fetch_updates, send_verification_success


def _parse_start_code(text: str | None) -> str | None:
    if not text or not isinstance(text, str):
        return None
    t = text.strip()
    m = re.match(r"/start(?:\s+(\S+))?", t)
    if not m:
        return None
    return m.group(1)


def process_telegram_updates(db: Database, offset: int | None) -> int | None:
    """Apply pending verification codes; returns next offset for getUpdates."""
    updates, next_off = fetch_updates(offset=offset, timeout=0)
    for u in updates:
        msg = u.get("message") or u.get("edited_message")
        if not isinstance(msg, dict):
            continue
        chat = msg.get("chat")
        if not isinstance(chat, dict):
            continue
        chat_id = chat.get("id")
        text = msg.get("text")
        code = _parse_start_code(text)
        if not code or chat_id is None:
            continue
        doc = db["user_settings"].find_one(
            {
                "telegram_verify_code": code,
                "telegram_verify_expires_at": {"$gt": datetime.utcnow()},
            }
        )
        if not doc:
            continue
        db["user_settings"].update_one(
            {"user_id": doc["user_id"]},
            {
                "$set": {
                    "telegram_chat_id": str(int(chat_id)),
                    "telegram_connected": True,
                    "telegram_verify_code": None,
                    "telegram_verify_expires_at": None,
                }
            },
        )
        send_verification_success(str(int(chat_id)))
    return next_off
