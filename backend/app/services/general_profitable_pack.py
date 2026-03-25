"""Centralized general-category profitable search + match keywords (config-driven)."""

from __future__ import annotations

import hashlib
import json
import logging
import random
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.config import settings

logger = logging.getLogger(__name__)


def _pack_path() -> Path:
    return Path(settings.categories_path).resolve().parent / "general_profitable_pack.json"


@lru_cache
def load_general_profitable_pack() -> dict[str, Any]:
    p = _pack_path()
    if not p.is_file():
        logger.warning("general_profitable_pack.json missing at %s — using empty pack", p)
        return {"search_queries": [], "match_keywords": [], "queries_per_cycle": 8}
    with p.open(encoding="utf-8") as f:
        return json.load(f)


def match_keywords_for_general() -> list[str]:
    """Title-match keywords for Step 2 when profile category is general."""
    pack = load_general_profitable_pack()
    raw = pack.get("match_keywords") or []
    out: list[str] = []
    seen: set[str] = set()
    for k in raw:
        if not k or not str(k).strip():
            continue
        s = str(k).strip()
        low = s.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(s)
    return out


def select_general_search_queries_for_cycle(*, user_id: int) -> tuple[list[str], dict[str, Any]]:
    """
    Rotate a subset of ``search_queries`` per day+user for variety across cycles.

    Returns ``(queries, log_meta)`` where ``log_meta`` includes the full pack and selection info.
    """
    pack = load_general_profitable_pack()
    all_q = [str(q).strip() for q in (pack.get("search_queries") or []) if q and str(q).strip()]
    n_per = max(1, int(pack.get("queries_per_cycle") or 8))
    match_kw = list(pack.get("match_keywords") or [])

    meta: dict[str, Any] = {
        "pack_path": str(_pack_path()),
        "general_pack_search_queries_full": list(all_q),
        "general_pack_match_keywords_count": len(match_kw),
        "queries_per_cycle_config": n_per,
        "queries_selected_this_cycle": [],
        "selection_seed": None,
    }

    if not all_q:
        logger.warning(
            "general_profitable_pack has no search_queries user_id=%s — Step 1 needs keywords from categories.json fallback",
            user_id,
        )
        return [], meta

    n_per = min(n_per, len(all_q))
    from datetime import date

    day = date.today().isoformat()
    seed_str = f"{user_id}:{day}:general_profitable_pack"
    seed = int(hashlib.sha256(seed_str.encode()).hexdigest()[:16], 16)
    meta["selection_seed"] = seed_str

    rng = random.Random(seed)
    shuffled = all_q[:]
    rng.shuffle(shuffled)
    selected = shuffled[:n_per]
    meta["queries_selected_this_cycle"] = list(selected)

    logger.info(
        "General profitable pack user_id=%s: full_search_queries=%s selected_this_cycle=%s queries_per_cycle=%s seed=%r",
        user_id,
        all_q,
        selected,
        n_per,
        seed_str,
    )
    return selected, meta
