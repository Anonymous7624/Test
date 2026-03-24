"""
Normalize → dedupe → Ollama scoring → MongoDB → optional Telegram (unchanged contract).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from app.domain import UserSettings as UserSettingsRow
from app.models import AlertStatus
from app.repositories.listing_repository import ListingRepository
from app.services.ollama_scoring import score_listing_with_ollama
from app.services.telegram_service import send_profit_alert
from pymongo.database import Database

from matching import raw_matches_profile
from mock_scraper import RawListing


@dataclass
class NormalizedListing:
    source_url: str
    source_id: str | None
    title: str
    price: float
    location_text: str
    category_id: str
    source_link: str
    source: str


def normalize(raw: RawListing, owner_user_id: int) -> NormalizedListing:
    ext = raw.source_link.rsplit("/", maxsplit=1)[-1]
    source_url = raw.source_link.strip()
    return NormalizedListing(
        source_url=source_url,
        source_id=f"{raw.source}:{ext}",
        title=raw.title.strip(),
        price=float(raw.price),
        location_text=raw.location.strip(),
        category_id=raw.category_slug.strip(),
        source_link=raw.source_link,
        source=raw.source,
    )


def process_batch(
    db: Database,
    raws: list[RawListing],
    *,
    profile: UserSettingsRow,
    origin_type: str = "live",
) -> int:
    """Insert new listings for this user; returns count inserted."""
    matched = [r for r in raws if raw_matches_profile(r, profile)]
    repo = ListingRepository(db)
    inserted = 0
    for raw in matched:
        norm = normalize(raw, profile.user_id)
        if repo.find_by_user_source_url(profile.user_id, norm.source_url):
            continue
        score = score_listing_with_ollama(
            title=norm.title,
            price=norm.price,
            category_id=norm.category_id,
            location_text=norm.location_text,
            source_url=norm.source_url,
        )
        profitable = score.estimated_profit > 0.0
        if score.should_alert:
            sent = send_profit_alert(
                chat_id=profile.telegram_chat_id,
                title=norm.title,
                source_link=norm.source_link,
                estimated_profit=score.estimated_profit,
            )
            alert_status = AlertStatus.sent.value if sent else AlertStatus.pending.value
        else:
            alert_status = AlertStatus.skipped.value

        created = repo.create(
            user_id=profile.user_id,
            source_url=norm.source_url,
            source_id=norm.source_id,
            title=norm.title,
            price=norm.price,
            estimated_resale=score.estimated_resale,
            estimated_profit=score.estimated_profit,
            category_id=norm.category_id,
            location_text=norm.location_text,
            source_link=norm.source_link,
            source=norm.source,
            profitable=profitable,
            alert_status=alert_status,
            found_at=datetime.utcnow(),
            origin_type=origin_type,
            ai_result=score.ai_result,
            confidence=score.confidence,
            reasoning=score.reasoning,
            should_alert=score.should_alert,
        )
        if created is not None:
            inserted += 1
    return inserted
