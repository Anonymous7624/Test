"""Search mode, custom keywords, and legacy Mongo settings migration."""

from __future__ import annotations

import re
from typing import Any, Literal

from app.services.marketplace_categories_service import label_for_slug, validate_marketplace_slug
from app.services.marketplace_step1_queries import custom_keyword_mode_search_ready

MAX_CUSTOM_KEYWORDS = 15

SearchMode = Literal["marketplace_category", "custom_keywords"]
SEARCH_MODES: frozenset[str] = frozenset({"marketplace_category", "custom_keywords"})

TelegramAlertMode = Literal["any_listing", "profitable_only", "none"]
TELEGRAM_ALERT_MODES: frozenset[str] = frozenset({"any_listing", "profitable_only", "none"})

_WS_RE = re.compile(r"\s+")


def normalize_custom_keywords(raw: str | list[str] | None) -> list[str]:
    """Trim, drop empties, dedupe case-insensitively, cap at MAX_CUSTOM_KEYWORDS."""
    if raw is None:
        return []
    if isinstance(raw, str):
        parts = [_WS_RE.sub(" ", p).strip() for p in raw.split(",")]
    else:
        parts = [_WS_RE.sub(" ", str(p)).strip() for p in raw]
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        if not p:
            continue
        low = p.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(p)
        if len(out) >= MAX_CUSTOM_KEYWORDS:
            break
    return out


def normalize_search_mode(raw: str | None) -> SearchMode:
    s = (raw or "").strip().lower()
    if s in SEARCH_MODES:
        return s  # type: ignore[return-value]
    return "marketplace_category"


def normalize_telegram_alert_mode(raw: str | None) -> TelegramAlertMode:
    s = (raw or "").strip().lower()
    if s in TELEGRAM_ALERT_MODES:
        return s  # type: ignore[return-value]
    return "profitable_only"


def migrate_settings_doc(doc: dict[str, Any]) -> dict[str, Any]:
    """
    Merge legacy Mongo fields (category_id, max_price) into the new search shape.
    Safe to call on every read — idempotent for already-migrated docs.
    """
    d = dict(doc)
    legacy_category_id = d.pop("category_id", None)
    d.pop("max_price", None)

    # search_mode
    if "search_mode" not in d or not str(d.get("search_mode") or "").strip():
        legacy_cat = str(legacy_category_id or "general").strip()
        if legacy_cat == "general":
            d["search_mode"] = "custom_keywords"
            if "custom_keywords" not in d or d.get("custom_keywords") is None:
                d["custom_keywords"] = []
        else:
            d["search_mode"] = "marketplace_category"
            slug_map = {
                "electronics": "electronics",
                "furniture": "home-goods",
                "vehicles": "electronics",
            }
            d["marketplace_category_slug"] = slug_map.get(legacy_cat, "electronics")
            d["marketplace_category_label"] = label_for_slug(str(d["marketplace_category_slug"])) or "Electronics"

    d["search_mode"] = normalize_search_mode(str(d.get("search_mode")))

    # marketplace fields
    if d["search_mode"] == "marketplace_category":
        slug = str(d.get("marketplace_category_slug") or "").strip()
        if not slug or not validate_marketplace_slug(slug):
            d["marketplace_category_slug"] = "electronics"
        d["marketplace_category_slug"] = str(d["marketplace_category_slug"]).strip()
        lab = label_for_slug(d["marketplace_category_slug"])
        d["marketplace_category_label"] = lab or d["marketplace_category_slug"]
    else:
        d["marketplace_category_slug"] = None
        d["marketplace_category_label"] = None

    # custom keywords
    d["custom_keywords"] = normalize_custom_keywords(d.get("custom_keywords"))

    d["telegram_alert_mode"] = normalize_telegram_alert_mode(str(d.get("telegram_alert_mode")))

    # One-time: last-completed batch snapshot (separate from in-progress worker_count_*)
    if "worker_last_completed_raw_collected" not in d:
        d["worker_last_completed_raw_collected"] = int(d.get("worker_count_raw_collected", 0))
        d["worker_last_completed_step1_kept"] = int(d.get("worker_count_step1_kept", 0))
        d["worker_last_completed_step2_matched"] = int(d.get("worker_count_step2_matched", 0))
        d["worker_last_completed_step3_scored"] = int(d.get("worker_count_step3_scored", 0))
        d["worker_last_completed_step4_saved"] = int(d.get("worker_count_step4_saved", 0))
        d["worker_last_completed_alerts_sent"] = int(d.get("worker_count_alerts_sent", 0))
    if "worker_pipeline_step3_rank" not in d:
        d["worker_pipeline_step3_rank"] = 0
        d["worker_pipeline_step3_total"] = 0

    return d


def validate_settings_for_save(
    *,
    search_mode: str,
    marketplace_category_slug: str | None,
    custom_keywords: list[str],
) -> None:
    """Raise ValueError with user-facing message if invalid."""
    sm = normalize_search_mode(search_mode)
    if sm == "marketplace_category":
        slug = (marketplace_category_slug or "").strip()
        if not validate_marketplace_slug(slug):
            raise ValueError("Select a Marketplace category.")
        return
    kws = normalize_custom_keywords(custom_keywords)
    if not kws:
        raise ValueError("Add at least one keyword to use custom keyword mode.")
    if not custom_keyword_mode_search_ready(custom_keywords):
        raise ValueError(
            "Custom keyword phrases must include at least one specific product term "
            "(generic words like “free” or “sale” alone are removed)."
        )
    if len(kws) > MAX_CUSTOM_KEYWORDS:
        raise ValueError(f"At most {MAX_CUSTOM_KEYWORDS} keywords.")
