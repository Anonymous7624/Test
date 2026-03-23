# Project plan — deal-finding dashboard MVP

## Scope

Private dashboard: auth, user monitoring settings, mock listing pipeline, profit estimates, Telegram hook (stub), listings UI with filters.

## Future

- Replace SQLite with PostgreSQL (same repository layer, new `DATABASE_URL`).
- Expose services via Cloudflare Tunnel; comments in `README.md` mark where tunnel config fits.
- Real scrapers and Telegram bot delivery.
