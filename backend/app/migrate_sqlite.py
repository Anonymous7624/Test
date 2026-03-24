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

        # Geoapify-backed location fields
        _geo_cols = [
            ("location_text", "ALTER TABLE user_settings ADD COLUMN location_text VARCHAR(512) DEFAULT ''"),
            ("center_lat", "ALTER TABLE user_settings ADD COLUMN center_lat FLOAT"),
            ("center_lon", "ALTER TABLE user_settings ADD COLUMN center_lon FLOAT"),
            ("geoapify_place_id", "ALTER TABLE user_settings ADD COLUMN geoapify_place_id VARCHAR(128)"),
            ("boundary_context", "ALTER TABLE user_settings ADD COLUMN boundary_context TEXT"),
        ]
        for col_name, ddl in _geo_cols:
            cur = {r[1] for r in conn.execute(text("PRAGMA table_info(user_settings)")).fetchall()}
            if col_name not in cur:
                conn.execute(text(ddl))
        # Migrate legacy `location` column into location_text
        us_cols5 = {r[1] for r in conn.execute(text("PRAGMA table_info(user_settings)")).fetchall()}
        if "location" in us_cols5:
            conn.execute(
                text(
                    "UPDATE user_settings SET location_text = location "
                    "WHERE (location_text IS NULL OR TRIM(location_text) = '') "
                    "AND location IS NOT NULL AND TRIM(location) != ''"
                )
            )
        # Legacy NOT NULL `location` breaks ORM inserts that only set location_text; drop after backfill.
        us_after_loc = {r[1] for r in conn.execute(text("PRAGMA table_info(user_settings)")).fetchall()}
        if "location" in us_after_loc:
            try:
                conn.execute(text("ALTER TABLE user_settings DROP COLUMN location"))
            except Exception:
                pass

        us_cols6 = {r[1] for r in conn.execute(text("PRAGMA table_info(user_settings)")).fetchall()}
        for col_name, ddl in [
            ("telegram_verify_code", "ALTER TABLE user_settings ADD COLUMN telegram_verify_code VARCHAR(64)"),
            (
                "telegram_verify_expires_at",
                "ALTER TABLE user_settings ADD COLUMN telegram_verify_expires_at DATETIME",
            ),
            ("monitoring_state", "ALTER TABLE user_settings ADD COLUMN monitoring_state VARCHAR(32) DEFAULT 'idle'"),
            ("last_checked_at", "ALTER TABLE user_settings ADD COLUMN last_checked_at DATETIME"),
            ("last_error", "ALTER TABLE user_settings ADD COLUMN last_error VARCHAR(512)"),
            ("backfill_complete", "ALTER TABLE user_settings ADD COLUMN backfill_complete BOOLEAN DEFAULT 1"),
        ]:
            if col_name not in us_cols6:
                conn.execute(text(ddl))
                us_cols6.add(col_name)

        li_cols2 = {r[1] for r in conn.execute(text("PRAGMA table_info(listings)")).fetchall()}
        if "discovery_source" not in li_cols2:
            conn.execute(text("ALTER TABLE listings ADD COLUMN discovery_source VARCHAR(32) DEFAULT 'live'"))
