"""``scope_index.json``: the Spark-free view of a static scope used by the collector, replay and API.

Format (interfaces §3)::

    {"schema_version": 1, "gtfs_version": "...", "scope_id": "...", "route_ids": ["..."],
     "route_type_by_route": {"R": 3}, "agency_by_route": {"R": "1"}, "agency_ids": ["1"],
     "agency_timezone": "America/New_York",
     "trip_to_route": {"<every trip_id of the GTFS version>": "<route_id>"},
     "scope_stop_ids": ["...incl. parent stations"], "stops_by_route": {"R": ["..."]},
     "parent_station": {"stop": "parent"}}

``trip_to_route`` covers ALL trips of the GTFS version (not only scoped ones) so that the scope filter can
tell "known trip of another route" (out of scope) from "unknown trip".
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = ["SCOPE_INDEX_FILE", "SCHEMA_VERSION", "ScopeIndex", "load_scope_index", "build_scope_index_doc",
           "write_scope_index"]

SCOPE_INDEX_FILE = "scope_index.json"
SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ScopeIndex:
    """Immutable, Spark-free scope description (one GTFS version + one scope)."""

    gtfs_version: str
    scope_id: str
    route_ids: frozenset[str]
    route_type_by_route: dict[str, int]
    agency_by_route: dict[str, str | None]
    agency_ids: frozenset[str]
    agency_timezone: str
    trip_to_route: dict[str, str]
    scope_stop_ids: frozenset[str]
    stops_by_route: dict[str, frozenset[str]]
    parent_station: dict[str, str]

    def route_of_trip(self, trip_id: str | None) -> str | None:
        """Static route of any trip of the GTFS version (``None`` when unknown)."""
        return self.trip_to_route.get(trip_id) if trip_id else None

    def is_scope_route(self, route_id: str | None) -> bool:
        return route_id is not None and route_id in self.route_ids

    def is_scope_trip(self, trip_id: str | None) -> bool:
        return self.route_of_trip(trip_id) in self.route_ids

    def routes_serving_stop(self, stop_id: str) -> frozenset[str]:
        """Scope routes serving ``stop_id`` (a parent station matches the routes of its children)."""
        return frozenset(r for r, stops in self.stops_by_route.items() if stop_id in stops)

    @classmethod
    def from_json(cls, doc: Mapping[str, Any]) -> ScopeIndex:
        if int(doc.get("schema_version", 0)) != SCHEMA_VERSION:
            raise ValueError(f"unsupported scope_index schema_version {doc.get('schema_version')!r}")
        return cls(
            gtfs_version=str(doc["gtfs_version"]),
            scope_id=str(doc["scope_id"]),
            route_ids=frozenset(str(r) for r in doc["route_ids"]),
            route_type_by_route={str(k): int(v) for k, v in doc["route_type_by_route"].items()},
            agency_by_route={str(k): (None if v is None else str(v)) for k, v in doc["agency_by_route"].items()},
            agency_ids=frozenset(str(a) for a in doc["agency_ids"]),
            agency_timezone=str(doc["agency_timezone"]),
            trip_to_route={str(k): str(v) for k, v in doc["trip_to_route"].items()},
            scope_stop_ids=frozenset(str(s) for s in doc["scope_stop_ids"]),
            stops_by_route={str(k): frozenset(str(s) for s in v) for k, v in doc["stops_by_route"].items()},
            parent_station={str(k): str(v) for k, v in doc["parent_station"].items()},
        )

    def to_json(self) -> dict[str, Any]:
        return build_scope_index_doc(
            gtfs_version=self.gtfs_version,
            scope_id=self.scope_id,
            route_ids=sorted(self.route_ids),
            route_type_by_route=self.route_type_by_route,
            agency_by_route=self.agency_by_route,
            agency_timezone=self.agency_timezone,
            trip_to_route=self.trip_to_route,
            scope_stop_ids=sorted(self.scope_stop_ids),
            stops_by_route={k: sorted(v) for k, v in self.stops_by_route.items()},
            parent_station=self.parent_station,
        )


def build_scope_index_doc(
    *,
    gtfs_version: str,
    scope_id: str,
    route_ids: Sequence[str],
    route_type_by_route: Mapping[str, int],
    agency_by_route: Mapping[str, str | None],
    agency_timezone: str,
    trip_to_route: Mapping[str, str],
    scope_stop_ids: Sequence[str],
    stops_by_route: Mapping[str, Sequence[str]],
    parent_station: Mapping[str, str],
) -> dict[str, Any]:
    """Deterministic JSON document (sorted lists/keys) for ``scope_index.json``."""
    agency_ids = sorted({a for a in agency_by_route.values() if a is not None})
    return {
        "schema_version": SCHEMA_VERSION,
        "gtfs_version": gtfs_version,
        "scope_id": scope_id,
        "route_ids": sorted(set(route_ids)),
        "route_type_by_route": {k: int(route_type_by_route[k]) for k in sorted(route_type_by_route)},
        "agency_by_route": {k: agency_by_route[k] for k in sorted(agency_by_route)},
        "agency_ids": agency_ids,
        "agency_timezone": agency_timezone,
        "trip_to_route": {k: trip_to_route[k] for k in sorted(trip_to_route)},
        "scope_stop_ids": sorted(set(scope_stop_ids)),
        "stops_by_route": {k: sorted(set(stops_by_route[k])) for k in sorted(stops_by_route)},
        "parent_station": {k: parent_station[k] for k in sorted(parent_station)},
    }


def write_scope_index(directory: Path, doc: Mapping[str, Any]) -> Path:
    """Write ``scope_index.json`` atomically (tmp file + ``os.replace``) into ``directory``."""
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / SCOPE_INDEX_FILE
    fd, tmp = tempfile.mkstemp(prefix=".scope_index.", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, separators=(",", ":"), sort_keys=False)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, target)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    return target


def load_scope_index(scope_dir: Path, *, require_complete: bool = False) -> ScopeIndex:
    """Read ``<scope_dir>/scope_index.json`` (interfaces §3).

    ``require_complete=True`` additionally demands the ``_SUCCESS`` marker of a published scope directory
    (callers that pin a scope for a run should pass it; the index file itself is written before
    ``_SUCCESS`` by the loader, so a missing marker means the load did not finish).
    """
    scope_dir = Path(scope_dir)
    if require_complete and not (scope_dir / "_SUCCESS").is_file():
        raise FileNotFoundError(f"static scope directory is incomplete (no _SUCCESS): {scope_dir}")
    path = scope_dir / SCOPE_INDEX_FILE
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    return ScopeIndex.from_json(doc)
