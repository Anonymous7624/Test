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
