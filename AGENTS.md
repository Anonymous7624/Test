# AGENTS.md

Build this project as a production-minded MVP.

Requirements:
- Separate frontend, backend, and worker into distinct folders.
- Frontend uses Next.js.
- Backend uses FastAPI.
- Worker uses Python and is separate from API routes.
- Use role-based auth with two roles: admin and user.
- For MVP, use local persistence (SQLite or JSON) instead of PostgreSQL.
- Structure code so PostgreSQL can be added later without major rewrites.
- Add clear README instructions for local setup and running each service.
- Do not hardcode business logic into frontend components.
- Keep category keywords in a centralized config file.
- Implement duplicate detection for listings already seen.
- Dashboard must show listings, profit estimate, alert status, and timestamps.
- The UI should be clean, modern, and easy to expand.
- Use environment variables for secrets and tokens.
- Never store plaintext passwords.
- Add TODO comments where future database integration will go.
