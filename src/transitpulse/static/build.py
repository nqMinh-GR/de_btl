"""Spark batch part of the static loader: typing, validation, scope filtering and the ``dim_*`` frames.

Everything stays in DataFrames. Only bounded results reach the driver: agency rows, the configured routes,
aggregated check counts and at most :data:`SAMPLE_LIMIT` sample keys per check. Large inputs are typed once
and staged as Parquet under the job's temp directory so that every check reads columnar data instead of
re-parsing the CSV (and nothing large is cached in the 1.5 GiB driver).

Severity policy (see :mod:`transitpulse.static.validation`): only structural problems are errors in scope;
per-trip/per-row schedule defects are warnings and set ``dim_trips.schedule_valid=false`` for the trip.

Scope (design C §1): configured route_ids that exist with ``route_type == cfg.scope.route_type``; trips of
those routes; their stop_times (semi-join); stops referenced by those stop_times plus their parent stations;
shapes, calendar and calendar_dates of the scoped trips; agencies of the scoped routes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from transitpulse.obs.logging import get_logger, log_event
from transitpulse.static.calendar import WEEKDAY_COLUMNS
from transitpulse.static.gtfs_time import SECONDS_PER_DAY, validate_timezone
from transitpulse.static.ingest import ExtractedFile
from transitpulse.static.sparkio import (
    date_col,
    double_col,
    empty_frame,
    flag_col,
    gtfs_seconds_col,
    int_col,
    project,
    read_gtfs_csv,
    service_anchor_expr,
)
from transitpulse.static.validation import (
    ERROR,
    FILE_SPECS,
    INFO,
    SAMPLE_LIMIT,
    STATUS_FAILED,
    WARNING,
    Check,
    find_duplicate_keys,
    make_check,
    missing_columns,
    peek_csv,
    status_from_checks,
)

__all__ = ["StaticBuild", "build_static", "out_of_scope_checks", "DIM_FILE_OF"]

_log = get_logger("transitpulse.static.build")

#: dim name -> source GTFS table (for the report's source vs scoped row counts)
DIM_FILE_OF: dict[str, str] = {
    "agency": "agency", "routes": "routes", "trips": "trips", "stop_times": "stop_times", "stops": "stops",
    "shapes": "shapes", "calendar": "calendar", "calendar_dates": "calendar_dates", "feed_info": "feed_info",
}


@dataclass
class StaticBuild:
    """Result of :func:`build_static` (frames are lazy; ``dims`` is empty when the status is FAILED)."""

    status: str
    checks: list[Check]
    agency_timezone: str | None
    requested_route_ids: tuple[str, ...]
    verified_route_ids: tuple[str, ...]
    rejected: tuple[dict[str, Any], ...]
    file_stats: dict[str, dict[str, Any]]
    route_info: dict[str, dict[str, Any]] = field(default_factory=dict)
    feed_info: dict[str, Any] | None = None
    dims: dict[str, DataFrame] = field(default_factory=dict)
    catalog_stop_times: DataFrame | None = None


@dataclass(frozen=True)
class _Rule:
    code: str
    expr: Column
    severity: str | None = None
    detail: str | None = None


# --------------------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------------------

def _stage(spark: SparkSession, df: DataFrame, stage_dir: Path, name: str) -> DataFrame:
    """Materialize ``df`` as Parquet under ``stage_dir/name`` and return the re-read frame."""
    path = stage_dir / name
    df.write.mode("overwrite").parquet(str(path))
    return spark.read.parquet(str(path))


def _flag_scope(df: DataFrame, keys: DataFrame, key: str, *, broadcast: bool = True) -> DataFrame:
    """Add boolean ``_in_scope`` = ``df[key]`` is one of ``keys[key]`` (null keys are never in scope)."""
    k = keys.select(key).where(F.col(key).isNotNull()).distinct().withColumn("_in_scope", F.lit(True))
    joined = df.join(F.broadcast(k) if broadcast else k, key, "left")
    return joined.withColumn("_in_scope", F.coalesce(F.col("_in_scope"), F.lit(False)))


def _with_ok(df: DataFrame, ref: DataFrame, ref_key: str, df_key: str, flag: str, *, broadcast: bool) -> DataFrame:
    """Add boolean column ``flag`` (null when ``df[df_key]`` has no match in ``ref[ref_key]``)."""
    r = ref.select(F.col(ref_key).alias(df_key)).where(F.col(df_key).isNotNull()).distinct()
    r = r.withColumn(flag, F.lit(True))
    return df.join(F.broadcast(r) if broadcast else r, df_key, "left")


def _dup(*keys: str) -> Column:
    w = Window.partitionBy(*keys)
    cond = F.lit(True)
    for k in keys:
        cond = cond & F.col(k).isNotNull()
    return cond & (F.count(F.lit(1)).over(w) > 1)


def _coords_valid(lat: str, lon: str) -> Column:
    la, lo = F.col(lat), F.col(lon)
    return (
        la.isNotNull() & lo.isNotNull()
        & la.between(-90.0, 90.0) & lo.between(-180.0, 180.0)
        & ~((la == 0.0) & (lo == 0.0))
    )


def _bad_value(raw: str, typed: str, allowed: Sequence[int] | None = None) -> Column:
    cond = F.col(typed).isNull()
    if allowed is not None:
        cond = cond | ~F.col(typed).isin(*allowed)
    return F.col(raw).isNotNull() & cond


def _evaluate(table: str, df: DataFrame, key: Column, rules: Sequence[_Rule]) -> list[Check]:
    """Count each rule's violations in/out of scope in ONE aggregation; fetch samples only when > 0."""
    if not rules:
        return []
    names = [f"_r{i}" for i in range(len(rules))]
    flagged = df.select(
        key.alias("_key"),
        F.col("_in_scope"),
        *[F.coalesce(r.expr, F.lit(False)).alias(n) for r, n in zip(rules, names, strict=True)],
    )
    aggs: list[Column] = []
    for n in names:
        aggs.append(F.sum(F.when(F.col(n) & F.col("_in_scope"), 1).otherwise(0)).alias(f"{n}_in"))
        aggs.append(F.sum(F.when(F.col(n) & ~F.col("_in_scope"), 1).otherwise(0)).alias(f"{n}_out"))
    row = flagged.agg(*aggs).collect()[0]
    out: list[Check] = []
    for rule, n in zip(rules, names, strict=True):
        for in_scope in (True, False):
            count = int(row[f"{n}_{'in' if in_scope else 'out'}"] or 0)
            if count == 0:
                continue
            scope_cond = F.col("_in_scope") if in_scope else ~F.col("_in_scope")
            samples = [
                r["_key"] for r in flagged.where(F.col(n) & scope_cond).select("_key").limit(SAMPLE_LIMIT).collect()
            ]
            out.append(make_check(rule.code, table, in_scope=in_scope, count=count, sample_keys=samples,
                                  severity=rule.severity, detail=rule.detail))
    return out


def _key(*cols: str) -> Column:
    return F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in cols])


def _as(name: str, spark_type: str) -> Column:
    """``CAST(name AS type)`` keeping the column name (explicit alias: the dim schemas are a contract)."""
    return F.col(name).cast(spark_type).alias(name)


# --------------------------------------------------------------------------------------------------
# reading
# --------------------------------------------------------------------------------------------------

def _read_files(
    spark: SparkSession, files: Mapping[str, ExtractedFile], checks: list[Check], file_stats: dict[str, dict]
) -> tuple[dict[str, DataFrame], dict[str, bool]]:
    raw: dict[str, DataFrame] = {}
    usable: dict[str, bool] = {}
    absent: dict[str, str] = {}
    for name, spec in FILE_SPECS.items():
        ef = files.get(name)
        file_stats[name] = {
            "name": spec.filename, "present": ef is not None, "row_count": None, "scoped_row_count": None,
            "sha256": ef.sha256 if ef else None, "size_bytes": ef.size_bytes if ef else None,
        }
        header, has_rows = peek_csv(ef.path) if ef is not None else (None, False)
        usable[name] = False
        if ef is None or not has_rows:
            if ef is not None:
                file_stats[name]["row_count"] = 0
            absent[name] = "STATIC_FILE_MISSING" if ef is None else "STATIC_FILE_EMPTY"
        else:
            missing = missing_columns(header or [], spec.required_columns)
            if missing:
                checks.append(make_check("MISSING_REQUIRED_COLUMN", spec.filename, in_scope=True,
                                         count=len(missing), sample_keys=missing))
            else:
                usable[name] = True
                raw[name] = project(read_gtfs_csv(spark, ef.path), spec)
        if not usable[name]:
            raw[name] = empty_frame(spark, spec)

    for name, code in absent.items():
        spec = FILE_SPECS[name]
        if name == "feed_info":
            checks.append(make_check("FEED_INFO_MISSING", spec.filename, in_scope=True, count=1))
        elif name in ("calendar", "calendar_dates"):
            other = "calendar_dates" if name == "calendar" else "calendar"
            sev = INFO if usable[other] else ERROR
            checks.append(make_check(code, spec.filename, in_scope=True, count=1, severity=sev,
                                     detail=f"{other}.txt {'provides' if usable[other] else 'cannot provide'} service dates"))
        else:
            checks.append(make_check(code, spec.filename, in_scope=spec.required_file, count=1))
    return raw, usable


def _typed(spark: SparkSession, raw: dict[str, DataFrame], stage_dir: Path, default_agency_id: str | None
           ) -> dict[str, DataFrame]:
    c = F.col
    t: dict[str, DataFrame] = {}
    t["routes"] = _stage(spark, raw["routes"].select(
        "route_id",
        F.coalesce(c("agency_id"), F.lit(default_agency_id).cast("string")).alias("agency_id"),
        "route_short_name", "route_long_name",
        c("route_type").alias("route_type_raw"), int_col(c("route_type")).alias("route_type"),
        "route_color", "route_text_color",
        c("route_sort_order").alias("route_sort_order_raw"), int_col(c("route_sort_order")).alias("route_sort_order"),
    ), stage_dir, "routes")
    t["trips"] = _stage(spark, raw["trips"].select(
        "trip_id", "route_id", "service_id",
        c("direction_id").alias("direction_id_raw"), int_col(c("direction_id")).alias("direction_id"),
        "shape_id", "trip_headsign", "block_id",
    ), stage_dir, "trips")
    t["stops"] = _stage(spark, raw["stops"].select(
        "stop_id", "stop_code", "stop_name",
        c("stop_lat").alias("stop_lat_raw"), double_col(c("stop_lat")).alias("stop_lat"),
        c("stop_lon").alias("stop_lon_raw"), double_col(c("stop_lon")).alias("stop_lon"),
        c("location_type").alias("location_type_raw"), int_col(c("location_type")).alias("location_type"),
        "parent_station", "platform_code",
        c("wheelchair_boarding").alias("wheelchair_boarding_raw"),
        int_col(c("wheelchair_boarding")).alias("wheelchair_boarding"),
    ), stage_dir, "stops")
    t["stop_times"] = _stage(spark, raw["stop_times"].select(
        "trip_id",
        c("stop_sequence").alias("stop_sequence_raw"), int_col(c("stop_sequence")).alias("stop_sequence"),
        "stop_id",
        c("arrival_time").alias("arrival_time_raw"), c("departure_time").alias("departure_time_raw"),
        gtfs_seconds_col(c("arrival_time")).alias("arrival_secs"),
        gtfs_seconds_col(c("departure_time")).alias("departure_secs"),
        c("timepoint").alias("timepoint_raw"), int_col(c("timepoint")).alias("timepoint"),
        "stop_headsign",
        c("pickup_type").alias("pickup_type_raw"), int_col(c("pickup_type")).alias("pickup_type"),
        c("drop_off_type").alias("drop_off_type_raw"), int_col(c("drop_off_type")).alias("drop_off_type"),
        c("shape_dist_traveled").alias("shape_dist_traveled_raw"),
        double_col(c("shape_dist_traveled")).alias("shape_dist_traveled"),
    ), stage_dir, "stop_times")
    day_cols: list[Column] = []
    for day in WEEKDAY_COLUMNS:
        day_cols += [c(day).alias(f"{day}_raw"), flag_col(c(day)).alias(day)]
    t["calendar"] = _stage(spark, raw["calendar"].select(
        "service_id", *day_cols,
        c("start_date").alias("start_date_raw"), date_col(c("start_date")).alias("start_date"),
        c("end_date").alias("end_date_raw"), date_col(c("end_date")).alias("end_date"),
    ), stage_dir, "calendar")
    t["calendar_dates"] = _stage(spark, raw["calendar_dates"].select(
        "service_id",
        c("date").alias("date_raw"), date_col(c("date")).alias("date"),
        c("exception_type").alias("exception_type_raw"), int_col(c("exception_type")).alias("exception_type"),
    ), stage_dir, "calendar_dates")
    t["shapes"] = _stage(spark, raw["shapes"].select(
        "shape_id",
        c("shape_pt_lat").alias("shape_pt_lat_raw"), double_col(c("shape_pt_lat")).alias("shape_pt_lat"),
        c("shape_pt_lon").alias("shape_pt_lon_raw"), double_col(c("shape_pt_lon")).alias("shape_pt_lon"),
        c("shape_pt_sequence").alias("shape_pt_sequence_raw"),
        int_col(c("shape_pt_sequence")).alias("shape_pt_sequence"),
        c("shape_dist_traveled").alias("shape_dist_traveled_raw"),
        double_col(c("shape_dist_traveled")).alias("shape_dist_traveled"),
    ), stage_dir, "shapes")
    t["feed_info"] = raw["feed_info"]
    t["agency"] = raw["agency"]
    return t


# --------------------------------------------------------------------------------------------------
# agency / scope
# --------------------------------------------------------------------------------------------------

def _agency_checks(agency_rows: list[dict[str, Any]], checks: list[Check]) -> str | None:
    missing_tz = [a.get("agency_id") for a in agency_rows if not a.get("agency_timezone")]
    if missing_tz:
        checks.append(make_check("MISSING_REQUIRED_VALUE", "agency.txt", in_scope=True, count=len(missing_tz),
                                 sample_keys=missing_tz, detail="agency_timezone", severity=ERROR))
    zones = sorted({a["agency_timezone"] for a in agency_rows if a.get("agency_timezone")})
    bad: list[str] = []
    for tz in zones:
        try:
            validate_timezone(tz)
        except ValueError:
            bad.append(tz)
    if bad:
        checks.append(make_check("INVALID_TIMEZONE", "agency.txt", in_scope=True, count=len(bad), sample_keys=bad))
    if len(zones) > 1:
        checks.append(make_check("MULTIPLE_AGENCY_TIMEZONES", "agency.txt", in_scope=True, count=len(zones),
                                 sample_keys=zones))
    if len(agency_rows) > 1:
        dups = find_duplicate_keys(agency_rows, ("agency_id",))
        if dups:
            checks.append(make_check("DUPLICATE_KEY", "agency.txt", in_scope=True, count=len(dups), sample_keys=dups))
    if len(zones) == 1 and not bad:
        return zones[0]
    return None


def _verify_routes(
    routes: DataFrame, requested: tuple[str, ...], route_type: int, checks: list[Check]
) -> tuple[list[str], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    found: dict[str, dict[str, Any]] = {}
    if requested:
        for r in (
            routes.where(F.col("route_id").isin(*requested))
            .select("route_id", "route_type", "route_type_raw", "agency_id")
            .collect()
        ):
            found.setdefault(r["route_id"], r.asDict())
    verified: list[str] = []
    rejected: list[dict[str, Any]] = []
    for rid in requested:
        row = found.get(rid)
        if row is None:
            rejected.append({"route_id": rid, "reason": "SCOPE_ROUTE_NOT_FOUND"})
        elif row["route_type"] != route_type:
            rejected.append({"route_id": rid, "reason": "SCOPE_ROUTE_NOT_BUS", "route_type": row["route_type_raw"]})
        else:
            verified.append(rid)
    for reason in ("SCOPE_ROUTE_NOT_FOUND", "SCOPE_ROUTE_NOT_BUS"):
        ids = [r["route_id"] for r in rejected if r["reason"] == reason]
        if ids:
            checks.append(make_check(reason, "routes.txt", in_scope=True, count=len(ids), sample_keys=ids))
    info = {
        rid: {"route_type": int(found[rid]["route_type"]), "agency_id": found[rid]["agency_id"]} for rid in verified
    }
    return verified, rejected, info


# --------------------------------------------------------------------------------------------------
# main entry
# --------------------------------------------------------------------------------------------------

def build_static(
    spark: SparkSession,
    files: Mapping[str, ExtractedFile],
    *,
    gtfs_version: str,
    route_ids: Sequence[str],
    route_type: int,
    stage_dir: Path,
) -> StaticBuild:
    """Validate the extracted GTFS tables and build the scoped dimension frames (lazy)."""
    checks: list[Check] = []
    file_stats: dict[str, dict[str, Any]] = {}
    requested = tuple(dict.fromkeys(r.strip() for r in route_ids if r and r.strip()))
    stage_dir.mkdir(parents=True, exist_ok=True)

    raw, usable = _read_files(spark, files, checks, file_stats)
    fatal_files = any(c.severity == ERROR for c in checks)
    agency_rows = [r.asDict() for r in raw["agency"].collect()] if usable["agency"] else []
    agency_timezone = _agency_checks(agency_rows, checks) if agency_rows else None
    if fatal_files:
        log_event(_log, "static_validate", "failed", "required GTFS files/columns missing",
                  checks=[c.to_json() for c in checks])
        return StaticBuild(STATUS_FAILED, checks, agency_timezone, requested, (), (), file_stats)

    default_agency_id = agency_rows[0].get("agency_id") if len(agency_rows) == 1 else None
    known_agency_ids = sorted({a["agency_id"] for a in agency_rows if a.get("agency_id")})
    t = _typed(spark, raw, stage_dir, default_agency_id)
    for name in ("routes", "trips", "stops", "stop_times", "calendar", "calendar_dates", "shapes"):
        file_stats[name]["row_count"] = t[name].count() if usable[name] else file_stats[name]["row_count"]
    file_stats["agency"]["row_count"] = len(agency_rows)
    feed_rows = t["feed_info"].limit(1).collect() if usable["feed_info"] else []
    feed_info = feed_rows[0].asDict() if feed_rows else None
    file_stats["feed_info"]["row_count"] = t["feed_info"].count() if usable["feed_info"] else None

    routes, trips, stops, st = t["routes"], t["trips"], t["stops"], t["stop_times"]
    cal, cd, shapes = t["calendar"], t["calendar_dates"], t["shapes"]

    verified, rejected, route_info = _verify_routes(routes, requested, route_type, checks)

    # ---- scope sets (small, broadcast)
    scope_trips = trips.where(F.col("route_id").isin(*verified)) if verified else trips.limit(0)
    scope_trip_ids = _stage(spark, scope_trips.select("trip_id").where(F.col("trip_id").isNotNull()).distinct(),
                            stage_dir, "scope_trip_ids")
    scope_services = scope_trips.select("service_id").where(F.col("service_id").isNotNull()).distinct()
    scope_shapes = scope_trips.select("shape_id").where(F.col("shape_id").isNotNull()).distinct()
    scope_st = st.join(F.broadcast(scope_trip_ids), "trip_id", "left_semi")
    served = _stage(spark, scope_st.select("stop_id").where(F.col("stop_id").isNotNull()).distinct(),
                    stage_dir, "served_stops")
    parents = (
        stops.join(F.broadcast(served), "stop_id", "left_semi")
        .select(F.col("parent_station").alias("stop_id"))
        .where(F.col("stop_id").isNotNull())
        .join(stops.select("stop_id"), "stop_id", "left_semi")
        .distinct()
    )
    scope_stop_ids = _stage(spark, served.unionByName(parents).distinct(), stage_dir, "scope_stop_ids")

    # ---- per-row stop_times windows + per-trip statistics (staged: reused by checks and dims)
    w_trip_seq = Window.partitionBy("trip_id").orderBy("stop_sequence")
    w_prev = w_trip_seq.rowsBetween(Window.unboundedPreceding, -1)
    w_trip_stop = Window.partitionBy("trip_id", "stop_id")
    arr, dep = F.col("arrival_secs"), F.col("departure_secs")
    x = (
        st.where(F.col("trip_id").isNotNull() & F.col("stop_sequence").isNotNull())
        .withColumn("_prev_max", F.max(F.greatest(arr, dep)).over(w_prev))
        .withColumn("_bad_order", F.coalesce(
            (arr.isNotNull() & dep.isNotNull() & (dep < arr)) | (F.coalesce(arr, dep) < F.col("_prev_max")),
            F.lit(False)))
        .withColumn("_bad_time", F.coalesce(
            (F.col("arrival_time_raw").isNotNull() & arr.isNull())
            | (F.col("departure_time_raw").isNotNull() & dep.isNull()), F.lit(False)))
        .withColumn("_occ_count", F.count(F.lit(1)).over(w_trip_stop))
        .withColumn("_stop_occurrence", F.row_number().over(w_trip_stop.orderBy("stop_sequence")))
    )
    x = _stage(spark, x, stage_dir, "stop_times_x")
    # per-trip row defects over ALL rows of the trip (rows without a usable stop_sequence are not in x)
    row_defect = (
        F.col("stop_sequence").isNull() | (F.col("stop_sequence") < 0) | F.col("stop_id").isNull()
        | (F.col("arrival_time_raw").isNotNull() & F.col("arrival_secs").isNull())
        | (F.col("departure_time_raw").isNotNull() & F.col("departure_secs").isNull())
    )
    row_defects = st.where(F.col("trip_id").isNotNull()).groupBy("trip_id").agg(
        F.max(F.coalesce(row_defect, F.lit(False)).cast("int")).alias("_row_defect"))
    trip_stats_x = x.groupBy("trip_id").agg(
        F.count(F.lit(1)).alias("stop_count"),
        F.expr("min_by(coalesce(departure_secs, arrival_secs), stop_sequence)").alias("first_departure_secs"),
        F.expr("max_by(coalesce(arrival_secs, departure_secs), stop_sequence)").alias("last_arrival_secs"),
        F.max(F.col("_bad_order").cast("int")).alias("_non_mono"),
        F.max(F.col("_bad_time").cast("int")).alias("_bad_time"),
        F.max((F.col("_occ_count") > 1).cast("int")).alias("_repeated"),
        F.max(F.greatest(arr, dep)).alias("_max_secs"),
    )
    trip_stats = _stage(spark, trip_stats_x.join(row_defects, "trip_id", "full_outer"), stage_dir, "trip_stats")

    # ---- routes
    r = routes.withColumn("_in_scope", F.coalesce(F.col("route_id").isin(*verified), F.lit(False))) \
        if verified else routes.withColumn("_in_scope", F.lit(False))
    route_rules = [
        _Rule("MISSING_REQUIRED_VALUE", F.col("route_id").isNull() | F.col("route_type_raw").isNull(),
              detail="route_id/route_type"),
        _Rule("INVALID_FIELD_VALUE", _bad_value("route_type_raw", "route_type"), detail="route_type"),
        _Rule("INVALID_FIELD_VALUE", _bad_value("route_sort_order_raw", "route_sort_order"), WARNING,
              "route_sort_order"),
        _Rule("DUPLICATE_KEY", _dup("route_id"), detail="route_id"),
    ]
    if known_agency_ids:
        route_rules.append(_Rule("FK_MISSING", F.col("agency_id").isNotNull()
                                 & ~F.col("agency_id").isin(*known_agency_ids), detail="routes.agency_id->agency"))
    checks += _evaluate("routes.txt", r, _key("route_id"), route_rules)

    # ---- trips
    svc_ids = cal.select("service_id").unionByName(cd.select("service_id"))
    tr = _flag_scope(trips, scope_trip_ids, "trip_id")
    tr = _with_ok(tr, routes, "route_id", "route_id", "_route_ok", broadcast=True)
    tr = _with_ok(tr, svc_ids, "service_id", "service_id", "_svc_ok", broadcast=True)
    tr = _with_ok(tr, shapes, "shape_id", "shape_id", "_shape_ok", broadcast=True)
    checks += _evaluate("trips.txt", tr, _key("trip_id"), [
        _Rule("MISSING_REQUIRED_VALUE", F.col("trip_id").isNull() | F.col("route_id").isNull()
              | F.col("service_id").isNull(), detail="trip_id/route_id/service_id"),
        _Rule("DUPLICATE_KEY", _dup("trip_id"), detail="trip_id"),
        _Rule("FK_MISSING", F.col("route_id").isNotNull() & F.col("_route_ok").isNull(), detail="trips.route_id->routes"),
        _Rule("FK_MISSING", F.col("service_id").isNotNull() & F.col("_svc_ok").isNull(),
              detail="trips.service_id->calendar/calendar_dates"),
        _Rule("FK_MISSING", F.col("shape_id").isNotNull() & F.col("_shape_ok").isNull(), WARNING,
              "trips.shape_id->shapes"),
        _Rule("INVALID_FIELD_VALUE", _bad_value("direction_id_raw", "direction_id", (0, 1)), WARNING, "direction_id"),
    ])

    # ---- stop_times (row level)
    s = _flag_scope(st, scope_trip_ids, "trip_id")
    s = _with_ok(s, trips, "trip_id", "trip_id", "_trip_ok", broadcast=False)
    s = _with_ok(s, stops, "stop_id", "stop_id", "_stop_ok", broadcast=True)
    optional_bad = (
        _bad_value("timepoint_raw", "timepoint", (0, 1))
        | _bad_value("pickup_type_raw", "pickup_type", (0, 1, 2, 3))
        | _bad_value("drop_off_type_raw", "drop_off_type", (0, 1, 2, 3))
        | _bad_value("shape_dist_traveled_raw", "shape_dist_traveled")
    )
    checks += _evaluate("stop_times.txt", s, _key("trip_id", "stop_sequence_raw"), [
        _Rule("MISSING_REQUIRED_VALUE", F.col("trip_id").isNull() | F.col("stop_id").isNull()
              | F.col("stop_sequence_raw").isNull(), detail="trip_id/stop_id/stop_sequence"),
        _Rule("INVALID_FIELD_VALUE", _bad_value("stop_sequence_raw", "stop_sequence")
              | (F.col("stop_sequence") < 0), detail="stop_sequence"),
        _Rule("INVALID_GTFS_TIME", (F.col("arrival_time_raw").isNotNull() & F.col("arrival_secs").isNull())
              | (F.col("departure_time_raw").isNotNull() & F.col("departure_secs").isNull())),
        _Rule("DUPLICATE_KEY", _dup("trip_id", "stop_sequence"), detail="(trip_id, stop_sequence)"),
        _Rule("FK_MISSING", F.col("trip_id").isNotNull() & F.col("_trip_ok").isNull(), detail="stop_times.trip_id->trips"),
        _Rule("FK_MISSING", F.col("stop_id").isNotNull() & F.col("_stop_ok").isNull(), detail="stop_times.stop_id->stops"),
        _Rule("INVALID_FIELD_VALUE", optional_bad, WARNING, "timepoint/pickup_type/drop_off_type/shape_dist_traveled"),
    ])

    # ---- stop_times (trip level)
    tl = _flag_scope(trips.select("trip_id").where(F.col("trip_id").isNotNull()).distinct(), scope_trip_ids, "trip_id")
    tl = tl.join(trip_stats, "trip_id", "left")
    checks += _evaluate("stop_times.txt", tl, _key("trip_id"), [
        _Rule("FIRST_LAST_STOP_TIME_MISSING", F.col("first_departure_secs").isNull()
              | F.col("last_arrival_secs").isNull()),
        _Rule("NON_MONOTONIC_STOP_TIMES", F.col("_non_mono") == 1),
        _Rule("REPEATED_STOP_IN_TRIP", F.col("_repeated") == 1),
        _Rule("SERVICE_TIME_GE_24H", F.col("_max_secs") >= SECONDS_PER_DAY),
    ])

    # ---- stops
    sp = _flag_scope(stops, served, "stop_id")
    sp = _with_ok(sp, stops, "stop_id", "parent_station", "_parent_ok", broadcast=True)
    coords_required = F.col("location_type").isNull() | F.col("location_type").isin(0, 1, 2)
    coords_present = F.col("stop_lat_raw").isNotNull() | F.col("stop_lon_raw").isNotNull()
    checks += _evaluate("stops.txt", sp, _key("stop_id"), [
        _Rule("MISSING_REQUIRED_VALUE", F.col("stop_id").isNull(), detail="stop_id"),
        _Rule("DUPLICATE_KEY", _dup("stop_id"), detail="stop_id"),
        _Rule("INVALID_COORDINATES", ~_coords_valid("stop_lat", "stop_lon") & (coords_required | coords_present)),
        _Rule("INVALID_FIELD_VALUE", _bad_value("location_type_raw", "location_type", (0, 1, 2, 3, 4))
              | _bad_value("wheelchair_boarding_raw", "wheelchair_boarding", (0, 1, 2)), WARNING,
              "location_type/wheelchair_boarding"),
        _Rule("FK_MISSING", F.col("parent_station").isNotNull() & F.col("_parent_ok").isNull(), WARNING,
              "stops.parent_station->stops"),
    ])

    # ---- calendar / calendar_dates
    c = _flag_scope(cal, scope_services, "service_id")
    bad_flags = F.lit(False)
    for day in WEEKDAY_COLUMNS:
        bad_flags = bad_flags | F.col(day).isNull()
    checks += _evaluate("calendar.txt", c, _key("service_id"), [
        _Rule("MISSING_REQUIRED_VALUE", F.col("service_id").isNull(), detail="service_id"),
        _Rule("DUPLICATE_KEY", _dup("service_id"), detail="service_id"),
        _Rule("INVALID_CALENDAR", bad_flags | F.col("start_date").isNull() | F.col("end_date").isNull()
              | (F.col("end_date") < F.col("start_date"))),
    ])
    d = _flag_scope(cd, scope_services, "service_id")
    checks += _evaluate("calendar_dates.txt", d, _key("service_id", "date_raw"), [
        _Rule("MISSING_REQUIRED_VALUE", F.col("service_id").isNull(), detail="service_id"),
        _Rule("DUPLICATE_KEY", _dup("service_id", "date_raw"), detail="(service_id, date)"),
        _Rule("INVALID_CALENDAR", F.col("date").isNull() | F.col("exception_type").isNull()
              | ~F.col("exception_type").isin(1, 2)),
    ])

    # ---- shapes (a broken shape is dropped with a warning)
    w_shape = Window.partitionBy("shape_id").orderBy("shape_pt_sequence")
    sh = _flag_scope(shapes, scope_shapes, "shape_id").withColumn(
        "_prev_dist", F.max("shape_dist_traveled").over(w_shape.rowsBetween(Window.unboundedPreceding, -1)))
    shape_point_bad = (
        ~_coords_valid("shape_pt_lat", "shape_pt_lon")
        | F.col("shape_pt_sequence").isNull()
    )
    checks += _evaluate("shapes.txt", sh, _key("shape_id", "shape_pt_sequence_raw"), [
        _Rule("MISSING_REQUIRED_VALUE", F.col("shape_id").isNull() | F.col("shape_pt_sequence_raw").isNull(),
              WARNING, "shape_id/shape_pt_sequence (shape dropped)"),
        _Rule("INVALID_FIELD_VALUE", _bad_value("shape_pt_sequence_raw", "shape_pt_sequence"), WARNING,
              "shape_pt_sequence (shape dropped)"),
        _Rule("DUPLICATE_KEY", _dup("shape_id", "shape_pt_sequence"), detail="(shape_id, shape_pt_sequence)"),
        _Rule("INVALID_COORDINATES", ~_coords_valid("shape_pt_lat", "shape_pt_lon"), WARNING, "shape dropped"),
        _Rule("SHAPE_DIST_DECREASING", F.col("shape_dist_traveled").isNotNull() & F.col("_prev_dist").isNotNull()
              & (F.col("shape_dist_traveled") < F.col("_prev_dist"))),
    ])
    bad_shapes = shapes.where(shape_point_bad & F.col("shape_id").isNotNull()).select("shape_id").distinct()

    # ---- scope-level
    if verified:
        with_trips = {row["route_id"] for row in scope_trips.select("route_id").distinct().collect()}
        no_trips = [rid for rid in verified if rid not in with_trips]
        if no_trips:
            checks.append(make_check("ROUTE_WITHOUT_TRIPS", "trips.txt", in_scope=True, count=len(no_trips),
                                     sample_keys=no_trips))

    pre_status = status_from_checks(checks)
    if pre_status == STATUS_FAILED or agency_timezone is None or not verified:
        if not verified and not any(ch.code.startswith("SCOPE_ROUTE") for ch in checks):
            checks.append(make_check("SCOPE_ROUTE_NOT_FOUND", "routes.txt", in_scope=True, count=1,
                                     sample_keys=["<no route_ids configured>"]))
        log_event(_log, "static_validate", "failed", "static validation failed",
                  errors=[ch.to_json() for ch in checks if ch.severity == ERROR][:20])
        return StaticBuild(STATUS_FAILED, checks, agency_timezone, requested, tuple(verified), tuple(rejected),
                           file_stats, route_info, feed_info)

    # ---- dimensions
    gv = F.lit(gtfs_version).alias("gtfs_version")
    scope_agencies = sorted({i["agency_id"] for i in route_info.values() if i["agency_id"] is not None})
    agency_df = raw["agency"].select(
        F.coalesce(F.col("agency_id"), F.lit(default_agency_id).cast("string")).alias("agency_id"),
        "agency_name", "agency_timezone",
    )
    if len(agency_rows) > 1:
        agency_df = agency_df.where(F.col("agency_id").isin(*scope_agencies))
    dims: dict[str, DataFrame] = {}
    dims["agency"] = agency_df.select("agency_id", "agency_name", "agency_timezone", gv)
    dims["routes"] = routes.where(F.col("route_id").isin(*verified)).select(
        "route_id", "agency_id", "route_short_name", "route_long_name", _as("route_type", "int"),
        "route_color", "route_text_color", _as("route_sort_order", "int"), gv)
    dims["trips"] = scope_trips.join(trip_stats, "trip_id", "left").select(
        "trip_id", "route_id", "service_id", _as("direction_id", "int"), "shape_id", "trip_headsign",
        "block_id",
        F.col("first_departure_secs").cast("int").alias("first_departure_secs"),
        F.col("last_arrival_secs").cast("int").alias("last_arrival_secs"),
        F.coalesce(F.col("stop_count"), F.lit(0)).cast("int").alias("stop_count"),
        # false for every per-trip defect (warning checks): missing first/last time, non-monotonic times,
        # an invalid time, a row without usable stop_sequence/stop_id, or no service_id
        F.coalesce(
            F.col("first_departure_secs").isNotNull() & F.col("last_arrival_secs").isNotNull()
            & (F.col("_non_mono") == 0) & (F.coalesce(F.col("_bad_time"), F.lit(0)) == 0)
            & (F.coalesce(F.col("_row_defect"), F.lit(0)) == 0) & F.col("service_id").isNotNull(),
            F.lit(False)).alias("schedule_valid"),
        F.coalesce(F.col("_repeated") == 1, F.lit(False)).alias("has_repeated_stop"),
        gv)
    dims["stop_times"] = x.join(F.broadcast(scope_trip_ids), "trip_id", "left_semi").select(
        "trip_id", _as("stop_sequence", "int"), "stop_id", "arrival_time_raw", "departure_time_raw",
        _as("arrival_secs", "int"), _as("departure_secs", "int"), _as("timepoint", "int"),
        F.col("_stop_occurrence").cast("int").alias("stop_occurrence"),
        (F.col("_occ_count") == 1).alias("stop_id_unique_in_trip"),
        gv)
    dims["stops"] = stops.join(F.broadcast(scope_stop_ids), "stop_id", "left_semi").dropDuplicates(["stop_id"]).select(
        "stop_id", "stop_code", "stop_name", _as("stop_lat", "double"), _as("stop_lon", "double"),
        _as("location_type", "int"), "parent_station", "platform_code",
        _as("wheelchair_boarding", "int"), gv)
    served_rs = scope_st.join(scope_trips.select("trip_id", "route_id", "direction_id"), "trip_id").select(
        "route_id", "direction_id", "stop_id").where(F.col("stop_id").isNotNull())
    parent_of = stops.select("stop_id", "parent_station").where(F.col("parent_station").isNotNull()) \
        .dropDuplicates(["stop_id"]).join(stops.select(F.col("stop_id").alias("parent_station")), "parent_station",
                                          "left_semi")
    parents_rs = served_rs.join(parent_of, "stop_id").select(
        "route_id", "direction_id", F.col("parent_station").alias("stop_id"))
    dims["route_stops"] = served_rs.unionByName(parents_rs).distinct().select(
        "route_id", _as("direction_id", "int"), "stop_id", gv)
    dims["shapes"] = shapes.join(F.broadcast(scope_shapes), "shape_id", "left_semi") \
        .join(bad_shapes, "shape_id", "left_anti").select(
        "shape_id", _as("shape_pt_sequence", "int"), _as("shape_pt_lat", "double"),
        _as("shape_pt_lon", "double"), _as("shape_dist_traveled", "double"), gv)
    dims["calendar"] = cal.join(F.broadcast(scope_services), "service_id", "left_semi").select(
        "service_id", *[_as(day, "boolean") for day in WEEKDAY_COLUMNS],
        _as("start_date", "date"), _as("end_date", "date"), gv)
    dims["calendar_dates"] = cd.join(F.broadcast(scope_services), "service_id", "left_semi").select(
        "service_id", _as("date", "date"), _as("exception_type", "int"), gv)
    dims["service_dates"] = _service_dates(dims["calendar"], dims["calendar_dates"], agency_timezone, gtfs_version)
    dims["feed_info"] = t["feed_info"].select(
        "feed_publisher_name", "feed_lang", "feed_start_date", "feed_end_date", "feed_version", gv)

    no_dates = scope_services.join(dims["service_dates"].select("service_id").distinct(), "service_id", "left_anti")
    nd = [row["service_id"] for row in no_dates.limit(SAMPLE_LIMIT).collect()]
    if nd:
        checks.append(make_check("SERVICE_NO_ACTIVE_DATES", "calendar.txt", in_scope=True,
                                 count=no_dates.count(), sample_keys=nd))

    catalog_stop_times = scope_st.where(F.col("stop_sequence").isNotNull()).join(
        scope_trips.select("trip_id", "route_id", "direction_id", "service_id", "shape_id", "trip_headsign"),
        "trip_id",
    ).select(
        gv, "trip_id", _as("stop_sequence", "int"), "stop_id", "arrival_time_raw", "departure_time_raw",
        F.col("arrival_secs").cast("int").alias("arrival_seconds"),
        F.col("departure_secs").cast("int").alias("departure_seconds"),
        "route_id", _as("direction_id", "int"), "service_id", "shape_id", "trip_headsign", "stop_headsign",
        _as("pickup_type", "int"), _as("drop_off_type", "int"), _as("timepoint", "int"),
    )

    status = status_from_checks(checks)
    log_event(_log, "static_validate", "ok", "static validation finished", status=status,
              verified_route_ids=verified, check_count=len(checks))
    return StaticBuild(status, checks, agency_timezone, requested, tuple(verified), tuple(rejected), file_stats,
                       route_info, feed_info, dims, catalog_stop_times)


def _service_dates(calendar: DataFrame, calendar_dates: DataFrame, agency_timezone: str, gtfs_version: str
                   ) -> DataFrame:
    """``dim_service_dates``: weekly calendar dates + added dates - removed dates, with the GTFS anchor."""
    # an INVALID_CALENDAR row contributes no service dates at all (a null flag = missing/invalid 0/1 value)
    flags_ok = F.lit(True)
    for day in WEEKDAY_COLUMNS:
        flags_ok = flags_ok & F.col(day).isNotNull()
    valid = calendar.where(
        F.col("start_date").isNotNull() & F.col("end_date").isNotNull()
        & (F.col("end_date") >= F.col("start_date")) & flags_ok
    )
    days = valid.select(
        "service_id",
        F.explode(F.expr("sequence(start_date, end_date, interval 1 day)")).alias("service_date"),
        *WEEKDAY_COLUMNS,
    ).where(F.expr(
        "element_at(array(sunday, monday, tuesday, wednesday, thursday, friday, saturday), dayofweek(service_date))"
    ))
    weekly = days.select("service_id", "service_date").distinct().withColumn("source", F.lit("calendar"))
    added = (
        calendar_dates.where((F.col("exception_type") == 1) & F.col("date").isNotNull())
        .select("service_id", F.col("date").alias("service_date"))
        .distinct()
        .join(weekly.select("service_id", "service_date"), ["service_id", "service_date"], "left_anti")
        .withColumn("source", F.lit("added"))
    )
    removed = calendar_dates.where(F.col("exception_type") == 2).select(
        "service_id", F.col("date").alias("service_date"))
    active = weekly.unionByName(added).join(removed, ["service_id", "service_date"], "left_anti")
    return active.select(
        "service_id",
        F.col("service_date").cast("date").alias("service_date"),
        service_anchor_expr("service_date", agency_timezone).alias("service_anchor_utc"),
        "source",
        F.lit(gtfs_version).alias("gtfs_version"),
    )


def out_of_scope_checks(file_stats: Mapping[str, Mapping[str, Any]]) -> list[Check]:
    """OUT_OF_SCOPE_FILTERED infos (source rows - scoped rows) per file, once scoped counts are known."""
    out: list[Check] = []
    for name in ("agency", "routes", "trips", "stop_times", "stops", "shapes", "calendar", "calendar_dates"):
        st = file_stats.get(name) or {}
        src, scoped = st.get("row_count"), st.get("scoped_row_count")
        if src is None or scoped is None or src <= scoped:
            continue
        out.append(make_check("OUT_OF_SCOPE_FILTERED", FILE_SPECS[name].filename, in_scope=False,
                              count=int(src) - int(scoped), detail=f"{src} source rows -> {scoped} scoped rows"))
    return out
