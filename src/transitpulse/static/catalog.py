"""Spark -> Cassandra static catalog rows, read back from a completed scope directory.

The rows of the five static tables are derived from the scope's Parquet (``dim_*`` plus the extra
``catalog_stop_times`` dataset that keeps ``stop_headsign``/``pickup_type``/``drop_off_type`` for
``stop_times_by_trip``) and streamed to the driver with ``toLocalIterator`` (bounded scope), so the same
code serves a fresh load and a re-run that only (re)writes Cassandra.

* ``stops_by_route``: stops actually served by the route's trips (no parent stations), ``direction_ids`` =
  set of directions serving the stop, ``pattern_count`` = number of distinct ordered stop patterns of the
  route that contain the stop. It is a SET of stops; stop order comes from ``stop_times_by_trip``.
* Trips with ``schedule_valid=false`` are kept everywhere (their usable stop rows are listed; rows without a
  stop_sequence never reach ``catalog_stop_times`` and rows without a stop_id are skipped in
  ``stops_by_route``) - the API shows them, processing must not compute delays from them.
* ``geometries_by_route``: one row per (route, shape) with the directions and trip count using the shape;
  ``is_representative`` marks the most used shape per (route, direction) (ties -> smallest shape_id).
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

__all__ = ["CATALOG_STOP_TIMES", "catalog_frames", "iter_rows", "catalog_row_iterators"]

CATALOG_STOP_TIMES = "catalog_stop_times"


def _read(spark: SparkSession, scope_dir: Path, name: str) -> DataFrame:
    return spark.read.parquet(str(scope_dir / name))


def catalog_frames(spark: SparkSession, scope_dir: Path, gtfs_version: str) -> dict[str, DataFrame]:
    """Lazy frames whose columns are the CQL columns of each static table (geometry: ``points`` array)."""
    gv = F.lit(gtfs_version).alias("gtfs_version")
    routes = _read(spark, scope_dir, "dim_routes")
    stops = _read(spark, scope_dir, "dim_stops")
    trips = _read(spark, scope_dir, "dim_trips")
    st = _read(spark, scope_dir, "dim_stop_times")
    shapes = _read(spark, scope_dir, "dim_shapes")

    out: dict[str, DataFrame] = {}
    out["routes_by_version"] = routes.select(
        gv, "route_id", "agency_id", "route_short_name", "route_long_name", "route_type", "route_color",
        "route_text_color", "route_sort_order")
    out["stops_by_version"] = stops.select(
        gv, "stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station",
        "platform_code", "wheelchair_boarding")

    patterns = st.groupBy("trip_id").agg(
        F.sort_array(F.collect_list(F.struct("stop_sequence", "stop_id"))).alias("_seq")
    ).select("trip_id", F.sha2(F.concat_ws("\u001f", F.expr("transform(_seq, x -> x.stop_id)")), 256)
             .alias("pattern"))
    served = (
        st.select("trip_id", "stop_id")
        .where(F.col("stop_id").isNotNull())        # a defective row (schedule_valid=false trip) has no stop
        .join(trips.select("trip_id", "route_id", "direction_id"), "trip_id")
        .join(patterns, "trip_id")
        .groupBy("route_id", "stop_id")
        .agg(
            F.sort_array(F.collect_set("direction_id")).alias("direction_ids"),
            F.countDistinct("pattern").cast("int").alias("pattern_count"),
        )
    )
    out["stops_by_route"] = served.join(
        stops.select("stop_id", "stop_name", "stop_lat", "stop_lon"), "stop_id", "left"
    ).select(gv, "route_id", "stop_id", "stop_name", "stop_lat", "stop_lon", "direction_ids", "pattern_count")

    out["stop_times_by_trip"] = _read(spark, scope_dir, CATALOG_STOP_TIMES)

    with_shape = trips.where(F.col("shape_id").isNotNull())
    per_dir = with_shape.groupBy("route_id", "direction_id", "shape_id").agg(F.count(F.lit(1)).alias("n"))
    w = Window.partitionBy("route_id", "direction_id").orderBy(F.col("n").desc(), F.col("shape_id").asc())
    rep = per_dir.withColumn("_rk", F.row_number().over(w)).groupBy("route_id", "shape_id").agg(
        F.max((F.col("_rk") == 1).cast("int")).alias("_rep"))
    per_shape = with_shape.groupBy("route_id", "shape_id").agg(
        F.sort_array(F.collect_set("direction_id")).alias("direction_ids"),
        F.count(F.lit(1)).cast("int").alias("trip_count"),
    )
    points = shapes.groupBy("shape_id").agg(
        F.sort_array(F.collect_list(F.struct(
            F.col("shape_pt_sequence").alias("seq"),
            F.col("shape_pt_lon").alias("lon"),
            F.col("shape_pt_lat").alias("lat"),
        ))).alias("points"))
    out["geometries_by_route"] = per_shape.join(rep, ["route_id", "shape_id"], "left").join(
        points, "shape_id", "inner"
    ).select(gv, "route_id", "shape_id", "direction_ids", "trip_count",
             F.coalesce(F.col("_rep") == 1, F.lit(False)).alias("is_representative"), "points")
    return out


def iter_rows(df: DataFrame) -> Iterator[dict[str, Any]]:
    """Stream rows to the driver one partition at a time (``toLocalIterator``) as plain dicts."""
    for row in df.toLocalIterator():
        yield row.asDict(recursive=True)


def catalog_row_iterators(spark: SparkSession, scope_dir: Path, gtfs_version: str) -> dict[str, Iterator[dict]]:
    """``{table: row iterator}`` for :func:`transitpulse.storage.static_writer.write_static_catalog`."""
    return {table: iter_rows(df) for table, df in catalog_frames(spark, scope_dir, gtfs_version).items()}
