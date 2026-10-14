"""``list-routes``: bus routes of a GTFS ZIP with trip counts and an optional realtime cross-check.

Pure Python (csv streaming over the ZIP members, no Spark). The realtime cross-check fetches
``VehiclePositions.json`` ONCE and counts distinct vehicles per route; when the fetch or parse fails every
``realtime_vehicle_count`` is ``null`` and the failure is reported - counts are never fabricated.
"""

from __future__ import annotations

import json
import zipfile
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from transitpulse.config import ProjectConfig
from transitpulse.contracts.envelope import iso_utc
from transitpulse.obs.logging import get_logger, log_event
from transitpulse.static.ingest import find_member, iter_csv_member

__all__ = ["bus_routes_from_zip", "count_vehicles_by_route", "fetch_realtime_vehicle_counts"]

_log = get_logger("transitpulse.static.routes")
_MAX_VP_BYTES = 64 * 1024 * 1024


def _sort_key(route: dict[str, Any]) -> tuple[int, int, str]:
    order = route.get("_sort_order")
    try:
        return (0, int(order), route["route_id"]) if order is not None else (1, 0, route["route_id"])
    except ValueError:
        return (1, 0, route["route_id"])


def bus_routes_from_zip(zip_path: Path, route_type: int = 3) -> list[dict[str, Any]]:
    """Routes with ``route_type == route_type`` and their trip counts (sorted by route_sort_order, route_id)."""
    routes: list[dict[str, Any]] = []
    with zipfile.ZipFile(zip_path) as zf:
        member = find_member(zf, "routes.txt")
        if member is None:
            raise ValueError("routes.txt is missing from the GTFS ZIP")
        for row in iter_csv_member(zf, member):
            rid = row.get("route_id")
            if not rid or row.get("route_type") != str(route_type):
                continue
            routes.append({
                "route_id": rid,
                "short_name": row.get("route_short_name"),
                "long_name": row.get("route_long_name"),
                "_sort_order": row.get("route_sort_order"),
            })
        trip_counts: Counter[str] = Counter()
        tmember = find_member(zf, "trips.txt")
        if tmember is not None:
            wanted = {r["route_id"] for r in routes}
            for row in iter_csv_member(zf, tmember):
                rid = row.get("route_id")
                if rid in wanted:
                    trip_counts[rid] += 1
    routes.sort(key=_sort_key)
    return [
        {"route_id": r["route_id"], "short_name": r["short_name"], "long_name": r["long_name"],
         "trip_count": trip_counts.get(r["route_id"], 0)}
        for r in routes
    ]


def count_vehicles_by_route(feed: dict[str, Any]) -> dict[str, int]:
    """Distinct vehicles per ``vehicle.trip.route_id`` in a GTFS-RT JSON VehiclePositions document."""
    seen: dict[str, set[str]] = defaultdict(set)
    for ent in feed.get("entity") or []:
        if not isinstance(ent, dict):
            continue
        veh = ent.get("vehicle")
        if not isinstance(veh, dict):
            continue
        trip = veh.get("trip") if isinstance(veh.get("trip"), dict) else {}
        route_id = trip.get("route_id")
        if not route_id:
            continue
        desc = veh.get("vehicle") if isinstance(veh.get("vehicle"), dict) else {}
        vid = desc.get("id") or ent.get("id")
        if vid:
            seen[str(route_id)].add(str(vid))
    return {rid: len(v) for rid, v in seen.items()}


def fetch_realtime_vehicle_counts(cfg: ProjectConfig) -> tuple[dict[str, int] | None, dict[str, Any]]:
    """GET VehiclePositions once; returns (counts or None, check metadata)."""
    import httpx

    url = cfg.sources.vehicle_positions_url
    h = cfg.collector.http
    timeout = httpx.Timeout(connect=h.connect_timeout_seconds, read=h.read_timeout_seconds,
                            write=h.write_timeout_seconds, pool=h.pool_timeout_seconds)
    checked_at = datetime.now(UTC)
    meta: dict[str, Any] = {"ok": False, "url": url, "checked_at": iso_utc(checked_at), "http_status": None,
                            "header_timestamp": None, "entity_count": None, "error": None}
    try:
        with httpx.Client(timeout=timeout, headers={"User-Agent": cfg.sources.user_agent},
                          follow_redirects=True) as client:
            resp = client.get(url)
        meta["http_status"] = resp.status_code
        if resp.status_code != 200:
            meta["error"] = f"HTTP {resp.status_code}"
            return None, meta
        if len(resp.content) > _MAX_VP_BYTES:
            meta["error"] = f"response larger than {_MAX_VP_BYTES} bytes"
            return None, meta
        doc = json.loads(resp.content)
        if not isinstance(doc, dict) or not isinstance(doc.get("entity", []), list):
            meta["error"] = "unexpected VehiclePositions JSON shape"
            return None, meta
    except (httpx.HTTPError, ValueError) as exc:
        meta["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
        log_event(_log, "realtime_check", "failed", "VehiclePositions cross-check failed", reason=meta["error"])
        return None, meta
    counts = count_vehicles_by_route(doc)
    header = doc.get("header") if isinstance(doc.get("header"), dict) else {}
    meta.update(ok=True, header_timestamp=header.get("timestamp"), entity_count=len(doc.get("entity") or []))
    log_event(_log, "realtime_check", "ok", "VehiclePositions cross-check done", routes=len(counts))
    return counts, meta
