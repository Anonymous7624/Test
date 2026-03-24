from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import AlertStatus, Listing


class ListingRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_by_external_id(self, external_id: str) -> Listing | None:
        return self.db.scalar(select(Listing).where(Listing.external_id == external_id))

    def create(
        self,
        *,
        user_id: int,
        external_id: str,
        title: str,
        price: float,
        estimated_resale: float,
        estimated_profit: float,
        category_slug: str,
        location: str,
        source_link: str,
        source: str,
        profitable: bool,
        alert_status: str,
        found_at: datetime | None = None,
        discovery_source: str = "live",
    ) -> Listing:
        row = Listing(
            user_id=user_id,
            external_id=external_id,
            title=title,
            price=price,
            estimated_resale=estimated_resale,
            estimated_profit=estimated_profit,
            category_slug=category_slug,
            location=location,
            source_link=source_link,
            source=source,
            discovery_source=discovery_source,
            profitable=profitable,
            alert_status=alert_status,
            found_at=found_at or datetime.utcnow(),
        )
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        return row

    def list_filtered(
        self,
        *,
        user_id: int,
        profitable_only: bool | None,
        category_slug: str | None,
        limit: int = 200,
    ) -> list[Listing]:
        q = (
            select(Listing)
            .where(Listing.user_id == user_id)
            .order_by(Listing.found_at.desc())
        )
        if profitable_only:
            q = q.where(Listing.profitable.is_(True))
        if category_slug:
            q = q.where(Listing.category_slug == category_slug)
        return list(self.db.scalars(q.limit(limit)))

    def count_for_user(self, user_id: int) -> int:
        q = select(func.count()).select_from(Listing).where(Listing.user_id == user_id)
        return int(self.db.scalar(q) or 0)

    def count_alerts_sent(self, user_id: int) -> int:
        q = (
            select(func.count())
            .select_from(Listing)
            .where(Listing.user_id == user_id, Listing.alert_status == AlertStatus.sent.value)
        )
        return int(self.db.scalar(q) or 0)
