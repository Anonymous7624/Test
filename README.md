# Deal-finding dashboard (monorepo MVP)

Private dashboard with **Next.js** frontend, **FastAPI** API, and a **Python worker** that simulates scraping, normalizes listings, deduplicates, estimates profit, and flags Telegram-ready alerts.

## Repository layout

| Path | Role |
|------|------|
| `frontend/` | Next.js 14 (App Router), auth, sidebar, listings table, settings, admin |
| `backend/` | FastAPI, JWT auth, SQLite persistence, repositories, categories API |
| `worker/` | Poll loop + mock scraper + pipeline (imports shared `app` from `backend/`) |
| `config/categories.json` | Centralized categories (General + 3 starters) and keywords |

SQLite lives at `backend/data/app.db`. Repositories are written so you can point `DATABASE_URL` at PostgreSQL later without rewriting business logic.

## Prerequisites

- **Python 3.11+** with `pip`
- **Node.js 18+** and npm (for the frontend)

## One-time setup

### 1. Backend

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
# Edit .env — set SECRET_KEY and ADMIN_PASSWORD at minimum.
```

### 2. Frontend

```powershell
cd frontend
copy .env.example .env.local
npm install
```

`NEXT_PUBLIC_API_URL` should match your API (default `http://localhost:8000/api`).

### 3. Worker (optional venv)

The worker reuses backend packages. Either use the same venv as the backend or:

```powershell
cd worker
pip install -r requirements.txt
```

Worker imports `app` from `backend/`; run it with **current working directory = `backend/`** so the SQLite path in `DATABASE_URL` matches the API.

## Running locally (three terminals)

**Terminal A — API** (from `backend/`):

```powershell
cd backend
.\.venv\Scripts\Activate.ps1
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

On first start, tables are created and the default admin is seeded from `ADMIN_USERNAME` / `ADMIN_PASSWORD` in `.env` if that username does not exist.

**Terminal B — Frontend** (from `frontend/`):

```powershell
cd frontend
npm run dev
```

Open [http://localhost:3000](http://localhost:3000), sign in with the admin credentials from `backend/.env`.

**Terminal C — Worker** (from `backend/` so SQLite paths align):

```powershell
cd backend
.\.venv\Scripts\Activate.ps1
python ..\worker\main.py
```

In the UI, **Settings → Run monitoring** sets `monitoring_enabled` for your user. The worker only ingests mock listings while that flag is on. **Stop monitoring** clears the flag.

Optional: `WORKER_POLL_SECONDS` (default `8`) adjusts the loop interval.

## API overview

- `POST /api/auth/login`, `GET /api/auth/me`
- `GET/PUT /api/settings/me`
- `GET /api/categories`
- `GET /api/listings?profitable_only=true&category=electronics`
- `POST /api/worker/run`, `POST /api/worker/stop`, `GET /api/worker/status`
- `GET/POST/PATCH/DELETE /api/admin/users` (admin role)

Interactive docs: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

## Deployment notes (future)

- **PostgreSQL:** Set `DATABASE_URL` to a `postgresql+psycopg2://...` URL (see comments in `backend/app/database.py` and `backend/.env.example`). Run migrations or `create_all` once on the new DB.
- **Cloudflare Tunnel:** Run `cloudflared tunnel` to expose the API (and optionally the Next.js app) on a stable hostname; update `CORS_ORIGINS` and `NEXT_PUBLIC_API_URL` to that hostname. See comment in `backend/app/main.py`.

## Default admin

Configured via environment variables (`ADMIN_USERNAME`, `ADMIN_PASSWORD` in `backend/.env`). Change the password immediately for any non-local deployment.
