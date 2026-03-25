import json
from functools import lru_cache
from pathlib import Path

from app.config import settings


@lru_cache
def load_categories() -> dict:
    path = Path(settings.categories_path)
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def category_ids() -> list[str]:
    data = load_categories()
    return [c["id"] for c in data["categories"]]


def validate_category_id(category_id: str) -> bool:
    return category_id in set(category_ids())


def keywords_for_category(category_id: str) -> list[str]:
    """Legacy categories.json keywords (unused by the new search pipeline)."""
    data = load_categories()
    for c in data.get("categories") or []:
        if c.get("id") == category_id:
            return list(c.get("keywords") or [])
    return []
