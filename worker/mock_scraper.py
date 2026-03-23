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
    """Return 0–2 synthetic listings under max_price."""
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
                source_link=f"https://example.com/listings/{rid}",
                source="mock",
            )
        )
    return out
