"""
Mock scraper — replace with real HTTP/HTML parsers per source.
"""

import random
import uuid
from dataclasses import dataclass


@dataclass
class RawListing:
    title: str
    price: float
    location: str
    category_slug: str
    source_link: str
    source: str
    latitude: float | None = None
    longitude: float | None = None
    description: str = ""
    image_url: str | None = None
    source_id: str | None = None


def _pick_location(primary: str, areas: list[str]) -> str:
    if areas:
        return random.choice(areas)
    return primary or "Unknown"


def mock_fetch_batch(
    *,
    category_slug: str,
    location: str,
    max_price: float,
    keywords: list[str] | None = None,
    search_area_labels: list[str] | None = None,
) -> list[RawListing]:
    """Return 0–2 synthetic listings under max_price (live monitoring), profile-targeted."""
    n = random.randint(0, 2)
    out: list[RawListing] = []
    areas = list(search_area_labels or [])
    kws = list(keywords or [])
    for _ in range(n):
        price = round(random.uniform(20, min(max_price, max_price * 0.95)), 2)
        rid = uuid.uuid4().hex[:12]
        loc = _pick_location(location, areas)
        kw_bit = f" {random.choice(kws)}" if kws and random.random() > 0.2 else ""
        title = f"Mock {category_slug}{kw_bit} item {rid}"
        out.append(
            RawListing(
                title=title,
                price=price,
                location=loc,
                category_slug=category_slug,
                source_link=f"https://example.com/listings/live-{rid}",
                source="mock",
                description=f"Synthetic listing for {category_slug}",
                source_id=f"mock:{rid}",
            )
        )
    return out


def mock_fetch_backfill(
    *,
    category_slug: str,
    location: str,
    max_price: float,
    keywords: list[str] | None = None,
    search_area_labels: list[str] | None = None,
    batch_size: int = 12,
) -> list[RawListing]:
    """Synthetic older listings for initial backfill (distinct source_link prefix)."""
    n = min(batch_size, max(6, random.randint(8, 15)))
    out: list[RawListing] = []
    areas = list(search_area_labels or [])
    kws = list(keywords or [])
    for _ in range(n):
        price = round(random.uniform(15, min(max_price, max_price * 0.9)), 2)
        rid = uuid.uuid4().hex[:12]
        loc = _pick_location(location, areas)
        kw_bit = f" {random.choice(kws)}" if kws and random.random() > 0.15 else ""
        title = f"[Archive] {category_slug}{kw_bit} deal {rid}"
        out.append(
            RawListing(
                title=title,
                price=price,
                location=loc,
                category_slug=category_slug,
                source_link=f"https://example.com/archive/listings/{rid}",
                source="mock_backfill",
                description="Archive synthetic row",
                source_id=f"mock_backfill:{rid}",
            )
        )
    return out
