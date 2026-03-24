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


def mock_fetch_batch(*, category_slug: str, location: str, max_price: float) -> list[RawListing]:
    """Return 0–2 synthetic listings under max_price (live monitoring)."""
    n = random.randint(0, 2)
    out: list[RawListing] = []
    for _ in range(n):
        price = round(random.uniform(20, min(max_price, max_price * 0.95)), 2)
        rid = uuid.uuid4().hex[:12]
        title = f"Mock {category_slug} item {rid}"
        out.append(
            RawListing(
                title=title,
                price=price,
                location=location or "Unknown",
                category_slug=category_slug,
                source_link=f"https://example.com/listings/live-{rid}",
                source="mock",
            )
        )
    return out


def mock_fetch_backfill(
    *,
    category_slug: str,
    location: str,
    max_price: float,
    batch_size: int = 12,
) -> list[RawListing]:
    """Synthetic older listings for initial backfill (distinct source_link prefix)."""
    n = min(batch_size, max(6, random.randint(8, 15)))
    out: list[RawListing] = []
    for _ in range(n):
        price = round(random.uniform(15, min(max_price, max_price * 0.9)), 2)
        rid = uuid.uuid4().hex[:12]
        title = f"[Archive] {category_slug} deal {rid}"
        out.append(
            RawListing(
                title=title,
                price=price,
                location=location or "Unknown",
                category_slug=category_slug,
                source_link=f"https://example.com/archive/listings/{rid}",
                source="mock_backfill",
            )
        )
    return out
