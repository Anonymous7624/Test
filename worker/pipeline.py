"""
Normalize → dedupe → profit estimate → alert flags.
"""

from dataclasses import dataclass
from datetime import datetime

from app.models import AlertStatus
from app.repositories.listing_repository import ListingRepository
from app.services.profit_estimation import estimate_profit
from app.services.telegram_service import config_from_env, send_alert
from sqlalchemy.orm import Session

from mock_scraper import RawListing


@dataclass
class NormalizedListing:
    external_id: str
    title: str
    price: float
    location: str
    category_slug: str
    source_link: str
    source: str


def normalize(raw: RawListing) -> NormalizedListing:
    ext = raw.source_link.rsplit("/", maxsplit=1)[-1]
    return NormalizedListing(
        external_id=f"{raw.source}:{ext}",
        title=raw.title.strip(),
        price=raw.price,
        location=raw.location.strip(),
        category_slug=raw.category_slug,
        source_link=raw.source_link,
        source=raw.source,
    )


def process_batch(db: Session, raws: list[RawListing]) -> int:
    """Insert new listings; returns count inserted."""
    repo = ListingRepository(db)
    inserted = 0
    cfg = config_from_env()
    for raw in raws:
        norm = normalize(raw)
        if repo.get_by_external_id(norm.external_id):
            continue
        est = estimate_profit(norm.price, norm.category_slug)
        alert_status = AlertStatus.none.value
        if est.profitable:
            # TODO: send Telegram using per-user token/chat from UserSettings when wired
            sent = bool(cfg.bot_token and cfg.chat_id and send_alert(norm.title, norm.source_link, est.estimated_profit))
            alert_status = AlertStatus.sent.value if sent else AlertStatus.pending.value
        repo.create(
            external_id=norm.external_id,
            title=norm.title,
            price=norm.price,
            estimated_resale=est.estimated_resale,
            estimated_profit=est.estimated_profit,
            category_slug=norm.category_slug,
            location=norm.location,
            source_link=norm.source_link,
            source=norm.source,
            profitable=est.profitable,
            alert_status=alert_status,
            found_at=datetime.utcnow(),
        )
        inserted += 1
    return inserted
