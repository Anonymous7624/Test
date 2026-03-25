from datetime import datetime

from pymongo import DESCENDING
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from app.domain import Listing
from app.models import AlertStatus
from app.mongodb import next_sequence


def _listing_from_doc(doc: dict) -> Listing:
    origin = str(doc.get("origin_type") or doc.get("discovery_source") or "live")
    cat = doc.get("category_id") or doc.get("category_slug") or "general"
    loc = doc.get("location_text") or doc.get("location") or ""
    raw_ai = doc.get("ai_result")
    ai_dict = raw_ai if isinstance(raw_ai, dict) else {}
    conf = doc.get("confidence")
    if conf is None and ai_dict:
        conf = ai_dict.get("confidence")
    if isinstance(conf, (int, float)):
        x = float(conf)
        conf = "low" if x < 0.34 else ("medium" if x < 0.67 else "high")
    reason = doc.get("reasoning")
    if reason is None and ai_dict:
        reason = ai_dict.get("reasoning")
    sa = doc.get("should_alert")
    if sa is None and ai_dict:
        sa = ai_dict.get("should_alert")

    mk = doc.get("matched_keywords")
    if isinstance(mk, list):
        matched_kw = [str(x) for x in mk if x is not None and str(x).strip()]
    else:
        matched_kw = []

    desc = doc.get("description")
    desc_out = str(desc).strip() if desc is not None and str(desc).strip() else None

    scraped = doc.get("scraped_at")
    sent_at = doc.get("alert_sent_at")

    return Listing(
        id=int(doc["id"]),
        user_id=int(doc["user_id"]),
        source_url=str(doc.get("source_url") or doc.get("source_link") or ""),
        source_id=doc.get("source_id"),
        title=str(doc["title"]),
        price=float(doc["price"]),
        estimated_resale=float(doc["estimated_resale"]),
        estimated_profit=float(doc["estimated_profit"]),
        category_id=str(cat),
        location_text=str(loc),
        found_at=doc["found_at"],
        alert_status=str(doc["alert_status"]),
        source_link=str(doc["source_link"]),
        source=str(doc.get("source") or "mock"),
        origin_type=origin,
        discovery_source=str(doc.get("discovery_source") or origin),
        profitable=bool(doc.get("profitable", False)),
        alert_sent=bool(doc.get("alert_sent", doc.get("alert_status") == AlertStatus.sent.value)),
        ai_result=raw_ai if isinstance(raw_ai, dict) else None,
        confidence=str(conf) if conf is not None else None,
        reasoning=str(reason) if reason is not None else None,
        should_alert=bool(sa) if sa is not None else None,
        description=desc_out,
        matched_keywords=matched_kw,
        scraped_at=scraped if isinstance(scraped, datetime) else None,
        alert_sent_at=sent_at if isinstance(sent_at, datetime) else None,
        alert_last_error=str(doc["alert_last_error"]) if doc.get("alert_last_error") else None,
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
        category_id: str,
        location_text: str,
        source_link: str,
        source: str,
        profitable: bool,
        alert_status: str,
        found_at: datetime | None = None,
        origin_type: str = "live",
        description: str | None = None,
        matched_keywords: list[str] | None = None,
        scraped_at: datetime | None = None,
        ai_result: dict | None = None,
        confidence: str | float | None = None,
        reasoning: str | None = None,
        should_alert: bool | None = None,
        alert_sent: bool = False,
        alert_sent_at: datetime | None = None,
        alert_last_error: str | None = None,
    ) -> Listing | None:
        """Insert listing; returns None if duplicate (user_id + source_url)."""
        lid = next_sequence(self.db, "listings")
        now = found_at or datetime.utcnow()
        kws = [str(x).strip() for x in (matched_keywords or []) if x and str(x).strip()]
        doc = {
            "id": lid,
            "user_id": user_id,
            "source_url": source_url,
            "source_id": source_id,
            "title": title,
            "price": price,
            "description": (description or "").strip() or None,
            "estimated_resale": estimated_resale,
            "estimated_profit": estimated_profit,
            "category_id": category_id,
            "category_slug": category_id,
            "location_text": location_text,
            "location": location_text,
            "found_at": now,
            "scraped_at": scraped_at,
            "alert_status": alert_status,
            "source_link": source_link,
            "source": source,
            "origin_type": origin_type,
            "discovery_source": origin_type,
            "profitable": profitable,
            "alert_sent": alert_sent,
            "alert_sent_at": alert_sent_at,
            "alert_last_error": alert_last_error,
            "matched_keywords": kws,
            "ai_result": ai_result,
            "confidence": confidence,
            "reasoning": reasoning,
            "should_alert": should_alert,
        }
        try:
            self.db["listings"].insert_one(doc)
        except DuplicateKeyError:
            return None
        return _listing_from_doc(doc)

    def set_alert_delivery(
        self,
        *,
        listing_id: int,
        user_id: int,
        alert_sent: bool,
        alert_status: str,
        alert_sent_at: datetime | None,
        alert_last_error: str | None,
    ) -> None:
        """Update Telegram delivery fields after send attempt (Step 4)."""
        self.db["listings"].update_one(
            {"id": listing_id, "user_id": user_id},
            {
                "$set": {
                    "alert_sent": alert_sent,
                    "alert_status": alert_status,
                    "alert_sent_at": alert_sent_at,
                    "alert_last_error": alert_last_error,
                }
            },
        )

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
            q["$or"] = [
                {"category_id": category_slug},
                {"category_slug": category_slug},
            ]
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
