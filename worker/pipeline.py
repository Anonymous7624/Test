"""
Normalize → dedupe → profit estimate → alert flags.
"""

from dataclasses import dataclass
from datetime import datetime

from app.models import AlertStatus
from app.repositories.listing_repository import ListingRepository
from app.services.profit_estimation import estimate_profit
from app.services.telegram_service import send_profit_alert
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


def normalize(raw: RawListing, owner_user_id: int) -> NormalizedListing:
    ext = raw.source_link.rsplit("/", maxsplit=1)[-1]
    return NormalizedListing(
        external_id=f"u{owner_user_id}:{raw.source}:{ext}",
        title=raw.title.strip(),
        price=raw.price,
        location=raw.location.strip(),
        category_slug=raw.category_slug,
        source_link=raw.source_link,
        source=raw.source,
    )


def process_batch(
    db: Session,
    raws: list[RawListing],
    *,
    owner_user_id: int,
    telegram_chat_id: str | None,
    discovery_source: str = "live",
) -> int:
    """Insert new listings; returns count inserted."""
    repo = ListingRepository(db)
    inserted = 0
    for raw in raws:
        norm = normalize(raw, owner_user_id)
        if repo.get_by_external_id(norm.external_id):
            continue
        est = estimate_profit(norm.price, norm.category_slug)
        alert_status = AlertStatus.none.value
        if est.profitable:
            sent = send_profit_alert(
                chat_id=telegram_chat_id,
                title=norm.title,
                source_link=norm.source_link,
                estimated_profit=est.estimated_profit,
            )
            alert_status = AlertStatus.sent.value if sent else AlertStatus.pending.value
        repo.create(
            user_id=owner_user_id,
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
            discovery_source=discovery_source,
        )
        inserted += 1
    return inserted
