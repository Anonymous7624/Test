import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import log_telegram_token_diagnostic, settings
from app.database import get_database
from app.mongodb import close_mongo_client, ensure_indexes
from app.routers import admin, auth, categories, listings, settings as settings_router, worker_control
from app.seed import seed_default_admin

_telegram_offset: int | None = None


async def _telegram_poll_loop() -> None:
    global _telegram_offset
    while True:
        if not (settings.telegram_bot_token or "").strip():
            await asyncio.sleep(5)
            continue
        try:
            from app.services.telegram_updates import process_telegram_updates

            db = get_database()
            _telegram_offset = process_telegram_updates(db, _telegram_offset)
        except Exception:
            pass
        await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(_: FastAPI):
    log_telegram_token_diagnostic()
    db = get_database()
    ensure_indexes(db)
    seed_default_admin(db)
    tg_task = asyncio.create_task(_telegram_poll_loop())
    try:
        yield
    finally:
        tg_task.cancel()
        try:
            await tg_task
        except asyncio.CancelledError:
            pass
        close_mongo_client()


app = FastAPI(title="Deal Dashboard API", lifespan=lifespan)

origins = [o.strip() for o in settings.backend_cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api_prefix = "/api"
app.include_router(auth.router, prefix=api_prefix)
app.include_router(settings_router.router, prefix=api_prefix)
app.include_router(categories.router, prefix=api_prefix)
app.include_router(listings.router, prefix=api_prefix)
app.include_router(worker_control.router, prefix=api_prefix)
app.include_router(admin.router, prefix=api_prefix)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
