"""Marketplace Step 1 keyword queries: same rules as worker search_plan (single source of truth)."""

from __future__ import annotations

import re

DEFAULT_EXCLUDED_QUERY_TOKENS: frozenset[str] = frozenset(
    {
        "deal",
        "deals",
        "sale",
        "sales",
        "local",
        "bundle",
        "bundles",
        "clearance",
        "cheap",
        "cheapest",
        "discount",
        "discounted",
        "near",
        "me",
        "nearby",
        "lot",
        "lots",
        "assorted",
        "misc",
        "various",
        "free",
        "obo",
        "firm",
        "must",
        "go",
        "quick",
        "asap",
        "today",
        "urgent",
    }
)

_TOKEN_SPLIT_RE = re.compile(r"[\s,/]+")
_MAX_FOCUS_QUERIES = 15


def _sanitize_token(t: str) -> str | None:
    s = t.strip().lower()
    if len(s) < 2:
        return None
    if s in DEFAULT_EXCLUDED_QUERY_TOKENS:
        return None
    s = s.strip("'\"")
    if len(s) < 2 or s in DEFAULT_EXCLUDED_QUERY_TOKENS:
        return None
    return s


def focused_queries_from_custom_keywords(
    raw_keywords: list[str], *, max_queries: int = _MAX_FOCUS_QUERIES
) -> list[str]:
    """Deduped keyword phrases for Marketplace search (custom_keywords mode)."""
    from app.services.search_settings import normalize_custom_keywords

    seen_lower: set[str] = set()
    out: list[str] = []
    for phrase in normalize_custom_keywords(raw_keywords):
        if not phrase or not str(phrase).strip():
            continue
        parts = [p for p in _TOKEN_SPLIT_RE.split(str(phrase).strip()) if p]
        kept_tokens: list[str] = []
        for p in parts:
            tok = _sanitize_token(p)
            if tok:
                kept_tokens.append(tok)
        if not kept_tokens:
            continue
        q = " ".join(kept_tokens)
        low = q.lower()
        if low in seen_lower:
            continue
        seen_lower.add(low)
        out.append(q)
        if len(out) >= max_queries:
            break
    return out


def custom_keyword_mode_search_ready(raw_keywords: list[str] | None) -> bool:
    """True when custom keyword mode has at least one usable Marketplace search phrase."""
    return len(focused_queries_from_custom_keywords(list(raw_keywords or []))) >= 1
