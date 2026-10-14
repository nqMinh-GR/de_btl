"""Static dimension container shared by the static loader (WP1, writer) and processing (WP4/WP5, reader).

Layout: ``<STATIC_ROOT>/<gtfs_version>/<scope_id>/dim_<name>/`` (Parquet written by Spark), plus
``scope_index.json``, ``static_quality_report.json``, ``manifest.json`` and ``_SUCCESS`` (written last).
A scope directory without ``_SUCCESS`` is incomplete and must never be used by a run.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

DIM_NAMES: tuple[str, ...] = (
    "agency",          # agency_id, agency_name, agency_timezone, gtfs_version
    "routes",          # route_id, agency_id, route_short_name, route_long_name, route_type:int, route_color,
                       #   route_text_color, route_sort_order:int, gtfs_version
    "trips",           # trip_id, route_id, service_id, direction_id:int, shape_id, trip_headsign, block_id,
                       #   first_departure_secs:int, last_arrival_secs:int, stop_count:int, schedule_valid:bool,
                       #   has_repeated_stop:bool, gtfs_version     (ALL trips of scope routes)
    "stop_times",      # trip_id, stop_sequence:int, stop_id, arrival_time_raw, departure_time_raw,
                       #   arrival_secs:int, departure_secs:int, timepoint:int, stop_occurrence:int,
                       #   stop_id_unique_in_trip:bool, gtfs_version
    "stops",           # stop_id, stop_code, stop_name, stop_lat:double, stop_lon:double, location_type:int,
                       #   parent_station, platform_code, wheelchair_boarding:int, gtfs_version
    "route_stops",     # route_id, direction_id:int, stop_id, gtfs_version   (incl. parent stations of served stops)
    "shapes",          # shape_id, shape_pt_sequence:int, shape_pt_lat:double, shape_pt_lon:double,
                       #   shape_dist_traveled:double, gtfs_version
    "calendar",        # service_id, monday..sunday:bool, start_date:date, end_date:date, gtfs_version
    "calendar_dates",  # service_id, date:date, exception_type:int, gtfs_version
    "service_dates",   # service_id, service_date:date, service_anchor_utc:timestamp, source, gtfs_version
    "feed_info",       # feed_publisher_name, feed_lang, feed_start_date, feed_end_date, feed_version, gtfs_version
)


@dataclass(frozen=True)
class ScopeIndexInfo:
    gtfs_version: str
    scope_id: str
    route_ids: tuple[str, ...]
    agency_timezone: str


@dataclass
class Dims:
    """Cached Spark DataFrames of one (gtfs_version, scope_id). Loaded once per streaming query."""

    gtfs_version: str
    scope_id: str
    agency_timezone: str
    route_ids: tuple[str, ...]
    frames: dict[str, DataFrame]

    def __getattr__(self, name: str) -> DataFrame:
        frames = self.__dict__.get("frames", {})
        if name in frames:
            return frames[name]
        raise AttributeError(name)

    def unpersist(self) -> None:
        for df in self.frames.values():
            df.unpersist()


def scope_dir_is_complete(scope_dir: Path) -> bool:
    return (scope_dir / "_SUCCESS").is_file() and (scope_dir / "manifest.json").is_file()


def read_scope_manifest(scope_dir: Path) -> dict[str, Any]:
    with open(scope_dir / "manifest.json", encoding="utf-8") as fh:
        return json.load(fh)


def load_dims(spark: SparkSession, scope_dir: Path, *, cache: bool = True) -> Dims:
    """Read every dim_* Parquet dataset of a completed scope directory (explicit failure if incomplete)."""
    if not scope_dir_is_complete(scope_dir):
        raise FileNotFoundError(f"static scope directory is incomplete (no _SUCCESS/manifest.json): {scope_dir}")
    manifest = read_scope_manifest(scope_dir)
    frames: dict[str, DataFrame] = {}
    for name in DIM_NAMES:
        path = scope_dir / f"dim_{name}"
        if not path.exists():
            raise FileNotFoundError(f"missing static dimension {path}")
        df = spark.read.parquet(str(path))
        frames[name] = df.cache() if cache else df
    return Dims(
        gtfs_version=manifest["gtfs_version"],
        scope_id=manifest["scope_id"],
        agency_timezone=manifest["agency_timezone"],
        route_ids=tuple(manifest["verified_route_ids"]),
        frames=frames,
    )
