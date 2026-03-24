"""Lightweight SQLite column migrations for MVP (no Alembic)."""

from sqlalchemy import text
from sqlalchemy.engine import Engine


def apply_sqlite_migrations(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    with engine.begin() as conn:
        us_cols = {r[1] for r in conn.execute(text("PRAGMA table_info(user_settings)")).fetchall()}
        if "telegram_connected" not in us_cols:
            conn.execute(
                text("ALTER TABLE user_settings ADD COLUMN telegram_connected BOOLEAN DEFAULT 0")
            )
        li_cols = {r[1] for r in conn.execute(text("PRAGMA table_info(listings)")).fetchall()}
        if "user_id" not in li_cols:
            conn.execute(text("ALTER TABLE listings ADD COLUMN user_id INTEGER REFERENCES users(id)"))
        row = conn.execute(text("SELECT id FROM users ORDER BY id LIMIT 1")).fetchone()
        if row:
            uid = row[0]
            conn.execute(
                text("UPDATE listings SET user_id = :uid WHERE user_id IS NULL"),
                {"uid": uid},
            )
        conn.execute(
            text(
                "UPDATE user_settings SET telegram_connected = 1 "
                "WHERE telegram_chat_id IS NOT NULL AND TRIM(telegram_chat_id) != ''"
            )
        )
        # Drop legacy per-user bot token column if present (SQLite 3.35+)
        us_cols2 = {r[1] for r in conn.execute(text("PRAGMA table_info(user_settings)")).fetchall()}
        if "telegram_bot_token" in us_cols2:
            try:
                conn.execute(text("ALTER TABLE user_settings DROP COLUMN telegram_bot_token"))
            except Exception:
                pass
