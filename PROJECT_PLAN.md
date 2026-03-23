<<<<<<< HEAD
# Project plan — deal-finding dashboard MVP

## Scope

Private dashboard: auth, user monitoring settings, mock listing pipeline, profit estimates, Telegram hook (stub), listings UI with filters.

## Future

- Replace SQLite with PostgreSQL (same repository layer, new `DATABASE_URL`).
- Expose services via Cloudflare Tunnel; comments in `README.md` mark where tunnel config fits.
- Real scrapers and Telegram bot delivery.
=======
# Project Plan

App name: Deal Finder Dashboard

Purpose:
A private dashboard where authenticated users configure a profitable-listing monitor.
The backend worker continuously checks listing sources, filters by user preferences,
scores likely flip profit with an AI estimator, sends Telegram alerts, and stores results.

User roles:
- Admin
- User

Core MVP features:
- Login
- User management
- Search settings form
- Start/stop monitoring
- Listing dedupe
- AI profit estimate
- Telegram alerts
- Listings dashboard

Architecture:
- frontend/: Next.js
- backend/: FastAPI
- worker/: Python background service
- shared config for categories and keywords
- lightweight persistence now, PostgreSQL later
>>>>>>> 36f2ace333c82bca20f2ea2718ed7559c7e83072
