"""Built-in Facebook Marketplace category list (slug + label). Centralized in config JSON."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from app.config import settings


@lru_cache
def load_marketplace_categories() -> dict:
    path = Path(settings.marketplace_categories_path)
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def marketplace_slugs() -> frozenset[str]:
    data = load_marketplace_categories()
    return frozenset(str(c["slug"]).strip() for c in data.get("categories") or [] if c.get("slug"))


def label_for_slug(slug: str) -> str | None:
    s = (slug or "").strip()
    if not s:
        return None
    for c in load_marketplace_categories().get("categories") or []:
        if str(c.get("slug") or "").strip() == s:
            return str(c.get("label") or s).strip() or s
    return None


def validate_marketplace_slug(slug: str) -> bool:
    return (slug or "").strip() in marketplace_slugs()


def list_categories_for_api() -> list[dict[str, str]]:
    """Return {slug, label} for settings UI."""
    out: list[dict[str, str]] = []
    for c in load_marketplace_categories().get("categories") or []:
        slug = str(c.get("slug") or "").strip()
        label = str(c.get("label") or slug).strip()
        if slug:
            out.append({"slug": slug, "label": label})
    return out
