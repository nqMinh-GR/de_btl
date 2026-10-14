"""Spark schedule resolution for TripUpdates (design C §2-§3, decisions D-BIZ-03).

Time anchor: ``service_anchor_utc = UTC(local noon of service_date in the agency timezone) - 12 h`` is
precomputed by the static loader in ``dim_service_dates.service_anchor_utc``. Here it is only read as whole
epoch seconds (``CAST(ts AS BIGINT)``), so ``scheduled = anchor_epoch + gtfs_seconds`` of the SAME event type
is computed with integer arithmetic and converted with ``timestamp_seconds`` - nothing depends on the session
time zone (which is UTC anyway). Calendar activity = existence of ``(service_id, service_date)`` in
``dim_service_dates`` (calendar + added - removed exceptions, built by WP1).

Trip-instance resolution (per TripUpdate entity, first rule that applies):
  1. ADDED/NEW -> not_applicable (KPI reason ADDED_TRIP). Other unsupported relationships
     (UNSCHEDULED, DUPLICATED, REPLACEMENT, DELETED) -> not_applicable + UNRESOLVED_TRIP.
  2. trip_id not in the scoped static trips -> unresolved, UNKNOWN_ROUTE_OR_TRIP.
  3. descriptor route_id / direction_id contradict static -> unresolved, DESCRIPTOR_STATIC_MISMATCH.
     A static trip with ``dim_trips.schedule_valid = false`` (per-trip static defect kept by WP1) ->
     unresolved, UNRESOLVED_TRIP: predictions stay visible, scheduled_at/delay null, KPI-excluded.
     A null schedule_valid counts as true (same order as static/schedule.py resolve_trip_instance).
  4. start_date present: unparsable -> DESCRIPTOR_STATIC_MISMATCH; service not active on it ->
     SERVICE_NOT_ACTIVE_ON_START_DATE (never guess); start_time present and != first departure ->
     START_TIME_MISMATCH; else resolved / method descriptor.
  5. start_date absent: candidates D in {L-1, L, L+1} (L = local date of metrics_as_of) that are active and
     whose window [anchor+first_dep-W, anchor+last_arr+W] contains metrics_as_of (and start_time matches
     when given). 1 -> resolved/inferred (SERVICE_DATE_INFERRED info); >=2 -> ambiguous
     (AMBIGUOUS_SERVICE_DATE); 0 -> unresolved (START_TIME_MISMATCH when only start_time failed, else
     UNRESOLVED_TRIP).
``trip_instance_key = trip_id|<service date yyyy-MM-dd>`` when resolved, else
``trip_id|?<start_date as received, or none>``. ``service_date_text`` is ISO ``yyyy-MM-dd`` (golden expected.json).

Stop resolution (independent of the service date, only for statically known, non-ADDED trips):
  stop_sequence present -> join (trip_id, stop_sequence); absent row -> UNKNOWN_STOP_FOR_TRIP; differing
  stop_id -> STOP_ID_SEQUENCE_MISMATCH. Else stop_id present -> join (trip_id, stop_id) only when the stop
  occurs exactly once in the trip; repeated -> UNRESOLVED_STOP_SEQUENCE; absent -> UNKNOWN_STOP_FOR_TRIP.
  Neither -> UNRESOLVED_STOP_SEQUENCE.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from transitpulse.processing import quality as Q

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

    from transitpulse.static.dims import Dims

GTFS_TIME_REGEX = r"^\s*(\d{1,3}):([0-5]\d):([0-5]\d)\s*$"
YYYYMMDD_REGEX = r"^[0-9]{8}$"

ADDED_RELATIONSHIPS: tuple[str, ...] = ("ADDED", "NEW")
UNSUPPORTED_RELATIONSHIPS: tuple[str, ...] = ("UNSCHEDULED", "DUPLICATED", "REPLACEMENT", "DELETED")
CANCELED_RELATIONSHIPS: tuple[str, ...] = ("CANCELED", "CANCELLED")

RESOLUTION_REASONS: tuple[str, ...] = (
    "UNKNOWN_ROUTE_OR_TRIP", "DESCRIPTOR_STATIC_MISMATCH", "SERVICE_NOT_ACTIVE_ON_START_DATE",
    "START_TIME_MISMATCH", "SERVICE_DATE_INFERRED", "AMBIGUOUS_SERVICE_DATE", "UNRESOLVED_TRIP",
)
STOP_REASONS: tuple[str, ...] = ("UNKNOWN_STOP_FOR_TRIP", "STOP_ID_SEQUENCE_MISMATCH", "UNRESOLVED_STOP_SEQUENCE")
Q.check_registered(RESOLUTION_REASONS + STOP_REASONS)


# --------------------------------------------------------------------------------------------------
# Column helpers
# --------------------------------------------------------------------------------------------------

def gtfs_seconds_col(c: Column) -> Column:
    """'HH:MM:SS' (hours may exceed 23) -> seconds after the service-day anchor; null/invalid -> null."""
    ok = F.coalesce(c.rlike(GTFS_TIME_REGEX), F.lit(False))
    h = F.regexp_extract(c, GTFS_TIME_REGEX, 1).cast("long")
    m = F.regexp_extract(c, GTFS_TIME_REGEX, 2).cast("long")
    s = F.regexp_extract(c, GTFS_TIME_REGEX, 3).cast("long")
    return F.when(ok, h * 3600 + m * 60 + s)


def yyyymmdd_to_date_col(c: Column) -> Column:
    """GTFS 'yyyyMMdd' -> DateType; malformed or impossible dates -> null (make_date, non-ANSI)."""
    ok = F.coalesce(c.rlike(YYYYMMDD_REGEX), F.lit(False))
    return F.when(
        ok,
        F.make_date(c.substr(1, 4).cast("int"), c.substr(5, 2).cast("int"), c.substr(7, 2).cast("int")),
    )


def date_to_iso_col(d: Column) -> Column:
    """DateType -> 'yyyy-MM-dd' (CAST(date AS STRING) is ISO and time-zone independent)."""
    return d.cast("string")


def scheduled_epoch_col(anchor_epoch: Column, gtfs_seconds: Column) -> Column:
    """Scheduled UTC epoch seconds = service anchor + GTFS seconds of the same event type."""
    return F.when(anchor_epoch.isNotNull() & gtfs_seconds.isNotNull(), anchor_epoch + gtfs_seconds)


def local_service_date(metrics_as_of: datetime, agency_timezone: str):
    """Local calendar date of the generation clock in the agency time zone (driver side, zoneinfo)."""
    return metrics_as_of.astimezone(ZoneInfo(agency_timezone)).date()


def _service_dates(dims: Dims) -> DataFrame:
    return (
        dims.service_dates.select(
            F.col("service_id").cast("string").alias("_sd_service_id"),
            F.col("service_date").cast("date").alias("_sd_date"),
            F.col("service_anchor_utc").cast("timestamp").cast("long").alias("_sd_anchor"),
        )
        .distinct()
    )


def _res(resolution: str, method: str | None, reason: str | None) -> Column:
    return F.struct(
        F.lit(resolution).cast("string").alias("resolution"),
        F.lit(method).cast("string").alias("method"),
        F.lit(reason).cast("string").alias("reason"),
    )


# --------------------------------------------------------------------------------------------------
# Trip instances
# --------------------------------------------------------------------------------------------------

def resolve_trip_instances(
    spark: SparkSession,
    inst: DataFrame,
    dims: Dims,
    *,
    metrics_as_of: datetime,
    agency_timezone: str,
    window_seconds: int,
) -> DataFrame:
    """Resolve TripUpdate trip instances against the static schedule.

    ``inst`` must have: ``_key`` (unique per row), ``trip_id`` (non-null), ``d_route_id``, ``d_direction_id``
    (descriptor values), ``start_date``, ``start_time`` (blank -> null already) and ``trip_rel`` (upper-case
    TripDescriptor.schedule_relationship, default SCHEDULED). All other columns are kept.

    Adds: s_route_id, s_direction_id, service_id, first_departure_secs, last_arrival_secs, trip_known,
    static_schedule_valid (null schedule_valid -> true), schedule_resolution, resolution_method,
    resolution_reason, service_date (date), service_date_text, anchor_epoch (long, set only when resolved),
    route_id (static, else descriptor), direction_id (descriptor, else static), trip_instance_key.
    """
    asof_us = Q.epoch_micros(metrics_as_of)
    w_s = int(window_seconds)

    valid_src = (F.col("schedule_valid").cast("boolean") if "schedule_valid" in dims.trips.columns
                 else F.lit(None).cast("boolean"))
    trips = dims.trips.select(
        F.col("trip_id").cast("string").alias("_s_trip_id"),
        F.col("route_id").cast("string").alias("s_route_id"),
        F.col("service_id").cast("string").alias("service_id"),
        F.col("direction_id").cast("int").alias("s_direction_id"),
        F.col("first_departure_secs").cast("long").alias("first_departure_secs"),
        F.col("last_arrival_secs").cast("long").alias("last_arrival_secs"),
        valid_src.alias("_s_schedule_valid"),
    ).dropDuplicates(["_s_trip_id"])
    sdates = _service_dates(dims)

    t = inst.join(F.broadcast(trips), F.col("trip_id") == F.col("_s_trip_id"), "left")
    t = (
        t.withColumn("trip_known", F.col("_s_trip_id").isNotNull())
        .withColumn("static_schedule_valid", F.coalesce(F.col("_s_schedule_valid"), F.lit(True)))
        .withColumn("_st_secs", gtfs_seconds_col(F.col("start_time")))
        .withColumn("_sd_req", yyyymmdd_to_date_col(F.col("start_date")))
        .drop("_s_trip_id", "_s_schedule_valid")
    )

    # descriptor start_date: active iff (service_id, start_date) is in dim_service_dates
    desc = sdates.select(
        F.col("_sd_service_id").alias("_d_service_id"),
        F.col("_sd_date").alias("_d_date"),
        F.col("_sd_anchor").alias("_desc_anchor"),
    )
    t = t.join(
        F.broadcast(desc),
        (F.col("service_id") == F.col("_d_service_id")) & (F.col("_sd_req") == F.col("_d_date")),
        "left",
    ).drop("_d_service_id", "_d_date")

    # inference for instances without start_date
    local = local_service_date(metrics_as_of, agency_timezone)
    cands = spark.createDataFrame(
        [(local - timedelta(days=1),), (local,), (local + timedelta(days=1),)],
        T.StructType([T.StructField("_cand_date", T.DateType(), False)]),
    )
    need = t.where(
        F.col("trip_known") & F.col("static_schedule_valid") & F.col("start_date").isNull()
        & ~F.col("trip_rel").isin(*ADDED_RELATIONSHIPS)
    ).select("_key", "service_id", "first_departure_secs", "last_arrival_secs", "start_time", "_st_secs")
    c = need.crossJoin(cands).join(
        F.broadcast(sdates),
        (F.col("service_id") == F.col("_sd_service_id")) & (F.col("_cand_date") == F.col("_sd_date")),
        "inner",
    )
    asof = F.lit(asof_us)
    lo = (F.col("_sd_anchor") + F.col("first_departure_secs") - F.lit(w_s)) * F.lit(1_000_000)
    hi = (F.col("_sd_anchor") + F.col("last_arrival_secs") + F.lit(w_s)) * F.lit(1_000_000)
    in_window = F.coalesce((asof >= lo) & (asof <= hi), F.lit(False))
    st_ok = F.col("start_time").isNull() | F.coalesce(F.col("_st_secs") == F.col("first_departure_secs"), F.lit(False))
    inferred = c.where(in_window).groupBy("_key").agg(
        F.count(F.lit(1)).alias("_n_window"),
        F.count(F.when(st_ok, 1)).alias("_n_match"),
        F.min(F.when(st_ok, F.col("_cand_date"))).alias("_inf_date"),
        F.min(F.when(st_ok, F.col("_sd_anchor"))).alias("_inf_anchor"),
    )
    t = t.join(inferred, "_key", "left")

    rel = F.col("trip_rel")
    has_sd = F.col("start_date").isNotNull()
    st_given = F.col("start_time").isNotNull()
    st_mismatch = st_given & ~F.coalesce(F.col("_st_secs") == F.col("first_departure_secs"), F.lit(False))
    desc_mismatch = F.coalesce(
        (F.col("d_route_id").isNotNull() & (F.col("d_route_id") != F.col("s_route_id"))), F.lit(False)
    ) | F.coalesce(
        (F.col("d_direction_id").isNotNull() & (F.col("d_direction_id") != F.col("s_direction_id"))), F.lit(False)
    )
    n_match = F.coalesce(F.col("_n_match"), F.lit(0))
    n_window = F.coalesce(F.col("_n_window"), F.lit(0))

    res = (
        F.when(rel.isin(*ADDED_RELATIONSHIPS), _res("not_applicable", None, None))
        .when(rel.isin(*UNSUPPORTED_RELATIONSHIPS), _res("not_applicable", None, "UNRESOLVED_TRIP"))
        .when(~F.col("trip_known"), _res("unresolved", None, "UNKNOWN_ROUTE_OR_TRIP"))
        .when(desc_mismatch, _res("unresolved", None, "DESCRIPTOR_STATIC_MISMATCH"))
        .when(~F.col("static_schedule_valid"), _res("unresolved", None, "UNRESOLVED_TRIP"))
        .when(has_sd & F.col("_sd_req").isNull(), _res("unresolved", None, "DESCRIPTOR_STATIC_MISMATCH"))
        .when(has_sd & F.col("_desc_anchor").isNull(), _res("unresolved", None, "SERVICE_NOT_ACTIVE_ON_START_DATE"))
        .when(has_sd & st_mismatch, _res("unresolved", None, "START_TIME_MISMATCH"))
        .when(has_sd, _res("resolved", "descriptor", None))
        .when(n_match == 1, _res("resolved", "inferred", "SERVICE_DATE_INFERRED"))
        .when(n_match >= 2, _res("ambiguous", None, "AMBIGUOUS_SERVICE_DATE"))
        .when(st_given & (n_window > 0), _res("unresolved", None, "START_TIME_MISMATCH"))
        .otherwise(_res("unresolved", None, "UNRESOLVED_TRIP"))
    )
    t = t.withColumn("_res", res)
    resolved = F.col("_res")["resolution"] == "resolved"
    by_desc = resolved & (F.col("_res")["method"] == "descriptor")
    by_inf = resolved & (F.col("_res")["method"] == "inferred")
    t = (
        t.withColumn("schedule_resolution", F.col("_res")["resolution"])
        .withColumn("resolution_method", F.col("_res")["method"])
        .withColumn("resolution_reason", F.col("_res")["reason"])
        .withColumn("service_date", F.when(by_desc, F.col("_sd_req")).when(by_inf, F.col("_inf_date")))
        .withColumn("anchor_epoch", F.when(by_desc, F.col("_desc_anchor")).when(by_inf, F.col("_inf_anchor")))
    )
    t = (
        t.withColumn("service_date_text", date_to_iso_col(F.col("service_date")))
        .withColumn("route_id", F.coalesce(F.col("s_route_id"), F.col("d_route_id")))
        .withColumn("direction_id", F.coalesce(F.col("d_direction_id"), F.col("s_direction_id")))
        .withColumn(
            "trip_instance_key",
            F.when(
                F.col("schedule_resolution") == "resolved",
                F.concat(F.col("trip_id"), F.lit("|"), F.col("service_date_text")),
            ).otherwise(
                F.concat(F.col("trip_id"), F.lit("|?"), F.coalesce(F.col("start_date"), F.lit("none")))
            ),
        )
    )
    return t.drop("_res", "_sd_req", "_desc_anchor", "_n_window", "_n_match", "_inf_date", "_inf_anchor", "_st_secs")


# --------------------------------------------------------------------------------------------------
# Stops
# --------------------------------------------------------------------------------------------------

def resolve_stops(stus: DataFrame, dims: Dims) -> DataFrame:
    """Resolve stop-time updates against ``dim_stop_times``.

    ``stus`` must have ``trip_id``, ``stu_seq`` (int, nullable), ``stu_stop_id`` (blank -> null) and
    ``_stop_lookup`` (boolean: trip statically known and not ADDED). Adds: ``static_stop_id``,
    ``resolved_seq`` (static stop_sequence, null unless the stop is resolved), ``arr_secs``, ``dep_secs``
    (long, null unless resolved), ``stop_resolved`` (boolean, never null), ``stop_reason`` (STOP_REASONS or
    null; null when no lookup is attempted).
    """
    st = dims.stop_times
    by_seq = st.select(
        F.col("trip_id").cast("string").alias("_q_trip"),
        F.col("stop_sequence").cast("int").alias("_q_seq"),
        F.col("stop_id").cast("string").alias("_q_stop"),
        F.col("arrival_secs").cast("long").alias("_q_arr"),
        F.col("departure_secs").cast("long").alias("_q_dep"),
    ).dropDuplicates(["_q_trip", "_q_seq"])
    by_stop = st.groupBy(
        F.col("trip_id").cast("string").alias("_i_trip"), F.col("stop_id").cast("string").alias("_i_stop")
    ).agg(
        F.count(F.lit(1)).alias("_i_n"),
        F.min(F.col("stop_sequence").cast("int")).alias("_i_seq"),
        F.min(F.col("arrival_secs").cast("long")).alias("_i_arr"),
        F.min(F.col("departure_secs").cast("long")).alias("_i_dep"),
    )
    lookup = F.coalesce(F.col("_stop_lookup"), F.lit(False))
    x = stus.join(
        F.broadcast(by_seq),
        lookup & (F.col("trip_id") == F.col("_q_trip")) & (F.col("stu_seq") == F.col("_q_seq")),
        "left",
    ).join(
        F.broadcast(by_stop),
        lookup & F.col("stu_seq").isNull() & (F.col("trip_id") == F.col("_i_trip"))
        & (F.col("stu_stop_id") == F.col("_i_stop")),
        "left",
    )
    seq = F.col("stu_seq")
    sid = F.col("stu_stop_id")
    unique = F.coalesce(F.col("_i_n") == 1, F.lit(False))

    def r(ok: bool, reason: str | None) -> Column:
        return F.struct(F.lit(ok).alias("ok"), F.lit(reason).cast("string").alias("reason"))

    ev = (
        F.when(~lookup, r(False, None))
        .when(seq.isNotNull() & F.col("_q_seq").isNull(), r(False, "UNKNOWN_STOP_FOR_TRIP"))
        # null-safe: a static row kept with a null stop_id never "equals" a source stop_id (Python reference
        # static/schedule.resolve_stop: ``stop_id != None`` -> STOP_ID_SEQUENCE_MISMATCH)
        .when(seq.isNotNull() & sid.isNotNull() & F.coalesce(sid != F.col("_q_stop"), F.col("_q_stop").isNull()),
              r(False, "STOP_ID_SEQUENCE_MISMATCH"))
        .when(seq.isNotNull(), r(True, None))
        .when(sid.isNotNull() & F.col("_i_n").isNull(), r(False, "UNKNOWN_STOP_FOR_TRIP"))
        .when(sid.isNotNull() & ~unique, r(False, "UNRESOLVED_STOP_SEQUENCE"))
        .when(sid.isNotNull(), r(True, None))
        .otherwise(r(False, "UNRESOLVED_STOP_SEQUENCE"))
    )
    x = x.withColumn("_stop_eval", ev)
    ok = F.col("_stop_eval")["ok"]
    x = (
        x.withColumn("stop_resolved", F.coalesce(ok, F.lit(False)))
        .withColumn("stop_reason", F.col("_stop_eval")["reason"])
        .withColumn("static_stop_id", F.when(seq.isNotNull(), F.col("_q_stop")).when(unique, F.col("_i_stop")))
        .withColumn("resolved_seq", F.when(ok & seq.isNotNull(), seq).when(ok, F.col("_i_seq")))
        .withColumn("arr_secs", F.when(ok & seq.isNotNull(), F.col("_q_arr")).when(ok, F.col("_i_arr")))
        .withColumn("dep_secs", F.when(ok & seq.isNotNull(), F.col("_q_dep")).when(ok, F.col("_i_dep")))
    )
    return x.drop("_stop_eval", "_q_trip", "_q_seq", "_q_stop", "_q_arr", "_q_dep",
                  "_i_trip", "_i_stop", "_i_n", "_i_seq", "_i_arr", "_i_dep")
