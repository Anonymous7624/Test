# Agent notes

- Monorepo layout: `frontend/` (Next.js), `backend/` (FastAPI), `worker/` (Python), `config/` (shared categories and Marketplace category list).
- Persistence: MongoDB for users, settings, listings (see backend config). Structure code so PostgreSQL can be added later without major rewrites.
- Auth: JWT in `Authorization: Bearer` header; roles `admin` | `user`.
- Do not commit `.env` files; use `.env.example` templates.
- Worker runs separately from API routes; polls `user_settings` for monitoring.
- Search: `search_mode` `marketplace_category` | `custom_keywords`; built-in categories in `config/marketplace_categories.json`.
- Do not hardcode business logic into frontend components; keep filters and category lists in config / backend.
- Implement duplicate detection for listings already seen.
- Dashboard should show listings, profit estimate, alert status, and timestamps.
- Use environment variables for secrets and tokens; never store plaintext passwords.
