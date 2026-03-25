import logging
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# Resolve backend/.env regardless of process cwd (e.g. uvicorn run from repo root).
_BACKEND_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _BACKEND_ROOT / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Auth / JWT
    secret_key: str = "change-me-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60 * 24

    # Default admin (seeded on startup if missing)
    admin_username: str = "admin"
    admin_password: str = "changeme"

    # MongoDB (users, settings, listings). Use MONGODB_URI from environment.
    mongodb_uri: str = Field(default="mongodb://localhost:27017", validation_alias="MONGODB_URI")
    mongodb_database: str = Field(default="deal_dashboard", validation_alias="MONGODB_DATABASE")

    # Categories JSON (legacy; listings may still reference old ids)
    categories_path: str = str(
        Path(__file__).resolve().parent.parent.parent / "config" / "categories.json"
    )
    marketplace_categories_path: str = str(
        Path(__file__).resolve().parent.parent.parent / "config" / "marketplace_categories.json"
    )

    # CORS (comma-separated). Prefer BACKEND_CORS_ORIGINS; CORS_ORIGINS is accepted for compatibility.
    backend_cors_origins: str = Field(
        default="http://localhost:3000,http://127.0.0.1:3000,http://192.168.1.181:3000",
        validation_alias=AliasChoices("BACKEND_CORS_ORIGINS", "CORS_ORIGINS"),
    )

    # Geoapify (geocoding + boundaries on server; never expose to client beyond autocomplete key)
    geoapify_api_key: str = Field(default="", validation_alias="GEOAPIFY_API_KEY")

    # Telegram Bot API (server only; users link chat via Settings UI)
    telegram_bot_token: str = Field(default="", validation_alias="TELEGRAM_BOT_TOKEN")
    # Public @username for in-app instructions (no secret)
    telegram_bot_username: str = Field(
        default="Facebookcatching_bot",
        validation_alias="TELEGRAM_BOT_USERNAME",
    )

    # Ollama (local LLM for listing scoring; worker reads same env via backend settings)
    ollama_base_url: str = Field(default="http://127.0.0.1:11434", validation_alias="OLLAMA_BASE_URL")
    ollama_model: str = Field(default="llama3.2", validation_alias="OLLAMA_MODEL")
    ollama_timeout: float = Field(default=120.0, validation_alias="OLLAMA_TIMEOUT")


settings = Settings()


def log_telegram_token_diagnostic() -> None:
    """Startup-safe: confirms TELEGRAM_BOT_TOKEN without logging the full secret."""
    t = (settings.telegram_bot_token or "").strip()
    if not t:
        logger.info("TELEGRAM_BOT_TOKEN: not set (env file: %s)", _ENV_FILE)
        return
    logger.info(
        "TELEGRAM_BOT_TOKEN: present (prefix=%s…, length=%d)",
        t[:4],
        len(t),
    )
