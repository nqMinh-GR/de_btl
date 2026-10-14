"""Cassandra static catalog writer (WP1): ``routes_by_version``, ``stops_by_version``, ``stops_by_route``,
``stop_times_by_trip`` and ``geometries_by_route``.

* Idempotent prepared upserts (``INSERT``) without TTL: a static version is immutable, so writing the same
  rows again is a no-op logically (PDF p15: "Không TTL static version").
* Bounded concurrency through ``cassandra.concurrent.execute_concurrent_with_args`` with
  ``concurrency = cfg.cassandra.write_concurrency`` and ``raise_on_first_error=True``, fed in chunks so the
  driver never materializes a whole table (rows come from Spark ``toLocalIterator`` over the bounded scope).
* ``None`` values are sent as ``UNSET_VALUE`` (no tombstones); sets are Python sets (an empty set is unset).
* ``geometries_by_route.points_json`` = JSON ``[[lon, lat], ...]`` in shape order, uniformly downsampled to at
  most ``cfg.storage.geometry_max_points`` points (first and last point kept) with ``simplified=true`` when
  points were dropped; ``point_count`` is the number of points stored in ``points_json``.

The session comes from :func:`transitpulse.storage.cassandra.get_session` (WP4); this module only builds
statements and parameters.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

from transitpulse.config import ProjectConfig
from transitpulse.obs.logging import get_logger, log_event

__all__ = [
    "STATIC_TABLE_COLUMNS",
    "STATIC_TABLES",
    "downsample_points",
    "geometry_row",
    "insert_cql",
    "row_params",
    "write_static_catalog",
]

_log = get_logger("transitpulse.storage.static_writer")

#: CQL column order per static table (column names == 0001_initial.cql).
STATIC_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "routes_by_version": (
        "gtfs_version", "route_id", "agency_id", "route_short_name", "route_long_name", "route_type",
        "route_color", "route_text_color", "route_sort_order",
    ),
    "stops_by_version": (
        "gtfs_version", "stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon", "location_type",
        "parent_station", "platform_code", "wheelchair_boarding",
    ),
    "stops_by_route": (
        "gtfs_version", "route_id", "stop_id", "stop_name", "stop_lat", "stop_lon", "direction_ids",
        "pattern_count",
    ),
    "stop_times_by_trip": (
        "gtfs_version", "trip_id", "stop_sequence", "stop_id", "arrival_time_raw", "departure_time_raw",
        "arrival_seconds", "departure_seconds", "route_id", "direction_id", "service_id", "shape_id",
        "trip_headsign", "stop_headsign", "pickup_type", "drop_off_type", "timepoint",
    ),
    "geometries_by_route": (
        "gtfs_version", "route_id", "shape_id", "direction_ids", "trip_count", "point_count", "points_json",
        "simplified", "is_representative",
    ),
}
STATIC_TABLES: tuple[str, ...] = tuple(STATIC_TABLE_COLUMNS)

_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "routes_by_version": ("gtfs_version", "route_id"),
    "stops_by_version": ("gtfs_version", "stop_id"),
    "stops_by_route": ("gtfs_version", "route_id", "stop_id"),
    "stop_times_by_trip": ("gtfs_version", "trip_id", "stop_sequence"),
    "geometries_by_route": ("gtfs_version", "route_id", "shape_id"),
}
_SET_COLUMNS = frozenset({"direction_ids"})
_INT_COLUMNS = frozenset({
    "route_type", "route_sort_order", "location_type", "wheelchair_boarding", "pattern_count", "stop_sequence",
    "arrival_seconds", "departure_seconds", "direction_id", "pickup_type", "drop_off_type", "timepoint",
    "trip_count", "point_count",
})
_FLOAT_COLUMNS = frozenset({"stop_lat", "stop_lon"})
_DEFAULT_CHUNK_ROWS = 2000


def insert_cql(keyspace: str, table: str) -> str:
    """Plain upsert (no TTL, no LWT) for one static table."""
    cols = STATIC_TABLE_COLUMNS[table]
    return (
        f"INSERT INTO {keyspace}.{table} ({', '.join(cols)}) "
        f"VALUES ({', '.join('?' for _ in cols)})"
    )


def downsample_points(points: Sequence[Sequence[float]], max_points: int) -> tuple[list[list[float]], bool]:
    """Uniformly keep at most ``max_points`` points (first and last always kept). Returns (points, simplified)."""
    pts = [[float(p[0]), float(p[1])] for p in points]
    n = len(pts)
    if max_points < 2:
        raise ValueError("geometry_max_points must be >= 2")
    if n <= max_points:
        return pts, False
    step = (n - 1) / (max_points - 1)
    idx = sorted({min(n - 1, int(math.floor(i * step + 0.5))) for i in range(max_points)})
    return [pts[i] for i in idx], True


def _point_pair(p: Any) -> list[float]:
    """Accept ``[lon, lat]`` pairs or mappings with ``lon``/``lat`` (or GTFS shape field names)."""
    if isinstance(p, Mapping):
        lon = p.get("lon", p.get("shape_pt_lon"))
        lat = p.get("lat", p.get("shape_pt_lat"))
        return [float(lon), float(lat)]
    return [float(p[0]), float(p[1])]


def geometry_row(row: Mapping[str, Any], max_points: int) -> dict[str, Any]:
    """Turn a catalog geometry row with ``points`` (shape order) into the ``geometries_by_route`` columns."""
    raw_points = [_point_pair(p) for p in (row.get("points") or [])]
    kept, simplified = downsample_points(raw_points, max_points)
    return {
        "gtfs_version": row["gtfs_version"],
        "route_id": row["route_id"],
        "shape_id": row["shape_id"],
        "direction_ids": row.get("direction_ids"),
        "trip_count": row.get("trip_count"),
        "point_count": len(kept),
        "points_json": json.dumps(kept, separators=(",", ":")),
        "simplified": simplified,
        "is_representative": bool(row.get("is_representative")),
    }


def _convert(column: str, value: Any) -> Any:
    if value is None:
        return None
    if column in _SET_COLUMNS:
        s = {int(v) for v in value if v is not None}
        return s or None
    if column in _INT_COLUMNS:
        return int(value)
    if column in _FLOAT_COLUMNS:
        return float(value)
    return value


def row_params(table: str, row: Mapping[str, Any]) -> tuple[Any, ...]:
    """Ordered statement parameters for ``table`` (None kept; replaced by UNSET at execution)."""
    cols = STATIC_TABLE_COLUMNS[table]
    for pk in _PRIMARY_KEYS[table]:
        if row.get(pk) is None:
            raise ValueError(f"{table}: primary key column {pk!r} is null in row {dict(row)!r}")
    return tuple(_convert(c, row.get(c)) for c in cols)


def _unset(params: tuple[Any, ...]) -> tuple[Any, ...]:
    from cassandra.query import UNSET_VALUE

    return tuple(UNSET_VALUE if v is None else v for v in params)


def _chunks(it: Iterable[tuple[Any, ...]], size: int) -> Iterator[list[tuple[Any, ...]]]:
    buf: list[tuple[Any, ...]] = []
    for item in it:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def write_static_catalog(
    cfg: ProjectConfig,
    session: Any,
    tables: Mapping[str, Iterable[Mapping[str, Any]]],
    *,
    chunk_rows: int = _DEFAULT_CHUNK_ROWS,
) -> dict[str, int]:
    """Upsert every given static table; returns rows written per table.

    ``tables`` maps a table name of :data:`STATIC_TABLES` to an iterable of row mappings (CQL column names;
    ``geometries_by_route`` rows carry ``points`` instead of ``points_json``/``point_count``/``simplified``).
    Raises on the first failed write (``raise_on_first_error=True``); re-running is safe (idempotent upserts).
    """
    from cassandra.concurrent import execute_concurrent_with_args

    from transitpulse.storage.cassandra import prepare

    unknown = set(tables) - set(STATIC_TABLES)
    if unknown:
        raise ValueError(f"unknown static tables: {sorted(unknown)}")
    keyspace = cfg.cassandra.keyspace
    max_points = int(cfg.storage.geometry_max_points)
    concurrency = max(1, int(cfg.cassandra.write_concurrency))
    written: dict[str, int] = {}
    for table in STATIC_TABLES:
        if table not in tables:
            continue
        stmt = prepare(session, insert_cql(keyspace, table), idempotent=True)
        if table == "geometries_by_route":
            rows: Iterable[Mapping[str, Any]] = (geometry_row(r, max_points) for r in tables[table])
        else:
            rows = tables[table]
        params = (_unset(row_params(table, r)) for r in rows)
        count = 0
        for chunk in _chunks(params, max(1, chunk_rows)):
            execute_concurrent_with_args(session, stmt, chunk, concurrency=concurrency, raise_on_first_error=True)
            count += len(chunk)
        written[table] = count
        log_event(_log, "static_cassandra_write", "ok", f"{table} upserted", table=table, rows=count)
    return written
