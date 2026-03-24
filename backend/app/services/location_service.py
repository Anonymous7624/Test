"""
Geoapify: Geocoding (validate/normalize) + Boundaries (part-of / consists-of).

MVP: sync httpx calls; structured boundary_context for future search filtering.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

GEOAPIFY = "https://api.geoapify.com/v1"


class LocationResolutionError(Exception):
    """Raised when Geoapify cannot validate the submitted place."""


def _client() -> httpx.Client:
    return httpx.Client(timeout=20.0)


def _require_key() -> str:
    key = (settings.geoapify_api_key or "").strip()
    if not key:
        raise RuntimeError("GEOAPIFY_API_KEY is not configured")
    return key


def _parse_geocode_hit(hit: dict[str, Any]) -> dict[str, Any]:
    lat = hit.get("lat")
    lon = hit.get("lon")
    if lat is None or lon is None:
        raise LocationResolutionError("Geocoding result missing coordinates")
    raw_pid = hit.get("place_id")
    place_id = str(raw_pid) if raw_pid else None
    formatted = (hit.get("formatted") or hit.get("address_line1") or "").strip() or None
    text = formatted or _fallback_label(hit)
    return {
        "location_text": text,
        "center_lat": float(lat),
        "center_lon": float(lon),
        "geoapify_place_id": place_id,
    }


def _fallback_label(hit: dict[str, Any]) -> str:
    parts = [hit.get("city"), hit.get("state"), hit.get("country")]
    return ", ".join(p for p in parts if p) or "Unknown"


def geocode_validate_and_normalize(
    *,
    location_text: str,
    center_lat: float | None,
    center_lon: float | None,
    geoapify_place_id: str | None,
) -> dict[str, Any]:
    """
    Validate a place using Forward Geocoding (search). Prefer place_id filter, else text + proximity bias.
    """
    key = _require_key()
    text = location_text.strip()
    if not text:
        raise LocationResolutionError("location_text is empty")

    with _client() as client:
        # 1) Strong match by place id when provided
        if geoapify_place_id and geoapify_place_id.strip():
            pid = geoapify_place_id.strip()
            try:
                r = client.get(
                    f"{GEOAPIFY}/geocode/search",
                    params={
                        "text": text,
                        "format": "json",
                        "filter": f"place:{pid}",
                        "limit": 1,
                        "apiKey": key,
                    },
                )
                r.raise_for_status()
            except httpx.HTTPError as exc:
                raise LocationResolutionError(f"Geoapify geocoding request failed: {exc}") from exc
            data = r.json()
            hits = data.get("results") or []
            if hits:
                return _parse_geocode_hit(hits[0])

        # 2) Text + proximity bias
        params: dict[str, Any] = {
            "text": text,
            "format": "json",
            "limit": 5,
            "apiKey": key,
        }
        if center_lat is not None and center_lon is not None:
            params["bias"] = f"proximity:{center_lon},{center_lat}"

        try:
            r = client.get(f"{GEOAPIFY}/geocode/search", params=params)
            r.raise_for_status()
            data = r.json()
        except httpx.HTTPError as exc:
            raise LocationResolutionError(f"Geoapify geocoding request failed: {exc}") from exc
        hits = data.get("results") or []
        if not hits:
            raise LocationResolutionError("No geocoding match for this location")

        best = hits[0]
        return _parse_geocode_hit(best)


def _feature_to_entry(feat: dict[str, Any]) -> dict[str, Any]:
    props = feat.get("properties") or {}
    geom = feat.get("geometry") or {}
    entry: dict[str, Any] = {
        "name": props.get("name"),
        "formatted": props.get("formatted"),
        "city": props.get("city"),
        "state": props.get("state"),
        "country": props.get("country"),
        "country_code": props.get("country_code"),
        "postcode": props.get("postcode"),
        "place_id": props.get("place_id"),
        "result_type": props.get("result_type") or props.get("rank", {}).get("match_type"),
    }
    if geom.get("type") == "Point" and isinstance(geom.get("coordinates"), list):
        c = geom["coordinates"]
        if len(c) >= 2:
            entry["lon"], entry["lat"] = c[0], c[1]
    return {k: v for k, v in entry.items() if v is not None}


def fetch_boundary_context(
    *,
    lat: float,
    lon: float,
    geoapify_place_id: str | None,
) -> dict[str, Any]:
    """part-of for the point; consists-of when we have a place id (contained subdivisions)."""
    key = _require_key()
    out: dict[str, Any] = {"part_of": [], "consists_of": []}

    with _client() as client:
        r = client.get(
            f"{GEOAPIFY}/boundaries/part-of",
            params={
                "lat": lat,
                "lon": lon,
                "geometry": "point",
                "boundaries": "administrative",
                "apiKey": key,
            },
        )
        r.raise_for_status()
        fc = r.json()
        for feat in fc.get("features") or []:
            out["part_of"].append(_feature_to_entry(feat))

        pid = (geoapify_place_id or "").strip()
        if pid:
            r2 = client.get(
                f"{GEOAPIFY}/boundaries/consists-of",
                params={
                    "id": pid,
                    "geometry": "point",
                    "boundary": "administrative",
                    "apiKey": key,
                },
            )
            if r2.is_success:
                fc2 = r2.json()
                for feat in fc2.get("features") or []:
                    out["consists_of"].append(_feature_to_entry(feat))
            else:
                logger.warning("Geoapify consists-of failed: %s %s", r2.status_code, r2.text[:200])

    return out


def resolve_location_for_save(
    *,
    location_text: str,
    center_lat: float | None,
    center_lon: float | None,
    geoapify_place_id: str | None,
    fetch_boundaries: bool = True,
) -> dict[str, Any]:
    """
    Returns keys: location_text, center_lat, center_lon, geoapify_place_id, boundary_context.
    """
    normalized = geocode_validate_and_normalize(
        location_text=location_text,
        center_lat=center_lat,
        center_lon=center_lon,
        geoapify_place_id=geoapify_place_id,
    )
    boundary: dict[str, Any] | None = None
    if fetch_boundaries:
        try:
            boundary = fetch_boundary_context(
                lat=normalized["center_lat"],
                lon=normalized["center_lon"],
                geoapify_place_id=normalized.get("geoapify_place_id"),
            )
        except Exception as exc:  # noqa: BLE001 — keep save usable if boundaries fail
            logger.warning("Boundary fetch failed: %s", exc)
            boundary = {"part_of": [], "consists_of": [], "error": str(exc)}

    normalized["boundary_context"] = boundary
    return normalized
