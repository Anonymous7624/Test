from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Production: run behind Cloudflare Tunnel (cloudflared) or a reverse proxy so the API is reachable
# from the Next.js frontend and from Telegram webhooks without exposing raw ports. Point tunnel
# public hostname to this Uvicorn process (e.g. localhost:8000) and set BACKEND_CORS_ORIGINS to the tunnel URL.

from app.config import settings
from app.database import Base, engine
from app.migrate_sqlite import apply_sqlite_migrations
from app.routers import admin, auth, categories, listings, settings as settings_router, worker_control
from app.seed import seed_default_admin


@asynccontextmanager
async def lifespan(_: FastAPI):
    Path("data").mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(bind=engine)
    from app.database import SessionLocal

    db = SessionLocal()
    try:
        seed_default_admin(db)
    finally:
        db.close()
    apply_sqlite_migrations(engine)
    yield


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
