"""Normalized listing shape after Step 1; Step 2 match outcome."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class CandidateListing:
    """Clean internal structure produced by Step 1 (targeted collection + normalization)."""

    user_id: int
    source_url: str
    source_id: str | None
    title: str
    price: float
    description: str
    location_text: str
    image_url: str | None
    scraped_at: datetime
    origin_type: str  # backfill | live
    category_slug: str
    latitude: float | None
    longitude: float | None
    source_link: str
    source: str
    raw_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class MatchResult:
    """Step 2 strict matcher output."""

    matched: bool
    rejection_reasons: list[str]
    matched_keywords: list[str]
    candidate_for_ai: CandidateListing | None
