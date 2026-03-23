from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Auth / JWT
    secret_key: str = "change-me-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60 * 24

    # Default admin (seeded on startup if missing)
    admin_username: str = "admin"
    admin_password: str = "changeme"

    # SQLite by default; swap for postgresql+asyncpg later via DATABASE_URL
    database_url: str = "sqlite:///./data/app.db"

    # Categories JSON (centralized config)
    categories_path: str = str(
        Path(__file__).resolve().parent.parent.parent / "config" / "categories.json"
    )

    # CORS (Next.js dev server)
    cors_origins: str = "http://localhost:3000"


settings = Settings()
