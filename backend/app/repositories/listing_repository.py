from datetime import datetime

from pymongo import DESCENDING
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from app.domain import Listing
from app.models import AlertStatus
from app.mongodb import next_sequence


def _listing_from_doc(doc: dict) -> Listing:
    origin = str(doc.get("origin_type") or doc.get("discovery_source") or "live")
    return Listing(
        id=int(doc["id"]),
        user_id=int(doc["user_id"]),
        source_url=str(doc.get("source_url") or doc.get("source_link") or ""),
        source_id=doc.get("source_id"),
        title=str(doc["title"]),
        price=float(doc["price"]),
        estimated_resale=float(doc["estimated_resale"]),
        estimated_profit=float(doc["estimated_profit"]),
        category_slug=str(doc["category_slug"]),
        location=str(doc["location"]),
        found_at=doc["found_at"],
        alert_status=str(doc["alert_status"]),
        source_link=str(doc["source_link"]),
        source=str(doc.get("source") or "mock"),
        origin_type=origin,
        discovery_source=str(doc.get("discovery_source") or origin),
        profitable=bool(doc.get("profitable", False)),
        alert_sent=bool(doc.get("alert_sent", doc.get("alert_status") == AlertStatus.sent.value)),
    )


class ListingRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def find_by_user_source_url(self, user_id: int, source_url: str) -> Listing | None:
        doc = self.db["listings"].find_one({"user_id": user_id, "source_url": source_url})
        return _listing_from_doc(doc) if doc else None

    def create(
        self,
        *,
        user_id: int,
        source_url: str,
        source_id: str | None,
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
        origin_type: str = "live",
    ) -> Listing | None:
        """Insert listing; returns None if duplicate (user_id + source_url)."""
        lid = next_sequence(self.db, "listings")
        now = found_at or datetime.utcnow()
        alert_sent = alert_status == AlertStatus.sent.value
        doc = {
            "id": lid,
            "user_id": user_id,
            "source_url": source_url,
            "source_id": source_id,
            "title": title,
            "price": price,
            "estimated_resale": estimated_resale,
            "estimated_profit": estimated_profit,
            "category_slug": category_slug,
            "location": location,
            "found_at": now,
            "alert_status": alert_status,
            "source_link": source_link,
            "source": source,
            "origin_type": origin_type,
            "discovery_source": origin_type,
            "profitable": profitable,
            "alert_sent": alert_sent,
        }
        try:
            self.db["listings"].insert_one(doc)
        except DuplicateKeyError:
            return None
        return _listing_from_doc(doc)

    def list_filtered(
        self,
        *,
        user_id: int,
        profitable_only: bool | None,
        category_slug: str | None,
        limit: int = 200,
    ) -> list[Listing]:
        q: dict = {"user_id": user_id}
        if profitable_only:
            q["profitable"] = True
        if category_slug:
            q["category_slug"] = category_slug
        cur = self.db["listings"].find(q).sort([("found_at", DESCENDING)]).limit(limit)
        return [_listing_from_doc(d) for d in cur]

    def count_for_user(self, user_id: int) -> int:
        return int(self.db["listings"].count_documents({"user_id": user_id}))

    def count_alerts_sent(self, user_id: int) -> int:
        return int(
            self.db["listings"].count_documents(
                {"user_id": user_id, "alert_status": AlertStatus.sent.value}
            )
        )
