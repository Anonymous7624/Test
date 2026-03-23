# Agent notes

- Monorepo layout: `frontend/` (Next.js), `backend/` (FastAPI), `worker/` (Python), `config/` (shared categories).
- Local dev: SQLite at `backend/data/app.db`; repositories use SQLAlchemy — swap `DATABASE_URL` for PostgreSQL later.
- Auth: JWT in `Authorization: Bearer` header; roles `admin` | `user`.
- Do not commit `.env` files; use `.env.example` templates.
