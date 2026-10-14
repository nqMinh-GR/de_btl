"""VehiclePositions generation builder (WP5): validity, IDs, membership, freshness, history, quality.

Per snapshot (design C §6, design B §2/§5, decisions D-ID-03, D-BIZ-04, D-BIZ-07, D-PUB-07, D-X-03):
  * Entity rejects (DLQ): corrupt JSON / no ``vehicle`` -> INVALID_ENTITY; missing/blank vehicle id ->
    MISSING_VEHICLE_ID.
  * Route: descriptor ``trip.route_id``, else ``trip_id`` -> ``dim_trips.route_id``; unresolvable or not a
    scope route -> UNKNOWN_ROUTE_OR_TRIP (warning, out_of_scope, quality only).
  * Invalid (frozen in ``invalid_reason``, ``position_valid=false``; first of): INVALID_COORDINATES (null,
    NaN, out of range or (0,0)), INVALID_TIMESTAMP_UNIT (timestamp > policy.timestamp_ms_threshold or
    <= 0, never auto-converted), FUTURE_TIMESTAMP (obs - metrics_as_of > future_tolerance). Small future
    skew -> CLOCK_SKEW warning, displayed age 0. No timestamp -> snapshot_state_sample (``ss_`` id keyed by
    the header timestamp, ``event_time_origin=feed_header``, ``freshness_known=false``,
    MISSING_OBSERVATION_TIMESTAMP warning).
  * Membership: one served row per distinct vehicle id (dedup ranking: valid first, observation_time desc
    nulls last, observation_id, entity position). Invalid vehicles WITH an id stay in
    vehicles_by_route_snapshot (frozen invalid decision).
  * Parent (frozen in the manifest): ``observation_id`` already served by the parent -> duplicate (not
    re-written to history); a valid current observation OLDER than the parent's served valid observation ->
    OBSERVATION_TIME_REGRESSION: the parent observation is served (``is_inherited``, original
    observation_time so the age keeps growing, ``inherited_from_snapshot_id``), the regressed source
    observation still goes to history, flagged. Vehicles absent from the snapshot are never carried over.
  * Freshness classes at ``params.metrics_as_of`` mirror ``contracts.policy.vehicle_freshness``.
  * route_metrics_by_snapshot: one row per ``run.route_ids`` entry; ``snapshot_membership_count`` and
    fresh/stale/unknown count distinct vehicle ids; ``invalid_vehicle_count`` counts invalid ENTITIES
    (served invalid ids + MISSING_VEHICLE_ID entities of that route). Identity per route:
    membership == fresh + stale + unknown + invalid_with_id.
Curated outputs never depend on ``parent`` (WP4 may build them before reading parent rows).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from transitpulse.processing import quality as Q
from transitpulse.processing import spark_ids as S
from transitpulse.processing.context import GenerationOutput
from transitpulse.processing.schemas import CORRUPT_COL, JSON_OPTIONS, VP_ENTITY_SCHEMA

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

    from transitpulse.contracts.policy import Policy
    from transitpulse.processing.context import BuildParams, RunInfo
    from transitpulse.static.dims import Dims

FEED_TYPE = "vehicle_positions"
DLQ_STAGE = "vp_entity_validation"
BUILDER_VERSION = "wp5-vp-v1"

Q.check_registered((
    "INVALID_ENTITY", "MISSING_VEHICLE_ID", "INVALID_COORDINATES", "INVALID_TIMESTAMP_UNIT", "FUTURE_TIMESTAMP",
    "CLOCK_SKEW", "MISSING_OBSERVATION_TIMESTAMP", "UNKNOWN_ROUTE_OR_TRIP", "DUPLICATE_OBSERVATION",
    "OBSERVATION_TIME_REGRESSION", "PARENT_UNAVAILABLE",
))

VEHICLE_COLUMNS: tuple[str, ...] = (
    "run_id", "snapshot_id", "route_id", "vehicle_id", "observation_id", "record_kind", "observation_time",
    "event_time", "event_time_origin", "freshness_known", "position_valid", "invalid_reason", "latitude",
    "longitude", "bearing", "speed_mps", "vehicle_label", "trip_id", "direction_id", "start_date", "start_time",
    "trip_schedule_relationship", "stop_id", "current_stop_sequence", "current_status", "occupancy_status",
    "feed_timestamp", "is_inherited", "inherited_from_snapshot_id", "quality_flags",
)

HISTORY_COLUMNS: tuple[str, ...] = (
    "run_id", "vehicle_id", "utc_date", "event_time", "observation_id", "record_kind", "event_time_origin",
    "route_id", "trip_id", "direction_id", "start_date", "latitude", "longitude", "bearing", "speed_mps",
    "stop_id", "current_stop_sequence", "current_status", "quality_flags",
)

VP_ROUTE_METRIC_COLUMNS: tuple[str, ...] = (
    "run_id", "feed_type", "snapshot_id", "route_id", "metrics_as_of", "feed_timestamp", "is_partial",
    "policy_version", "snapshot_membership_count", "fresh_vehicle_count", "stale_vehicle_count",
    "unknown_age_vehicle_count", "invalid_vehicle_count", "inherited_vehicle_count", "stale_threshold_seconds",
    "quality_flags",
)

# Parent rows = parent generation's vehicles_by_route_snapshot (CQL names, TimestampType); missing -> null.
PARENT_VEHICLE_SPEC: dict[str, str] = {
    "snapshot_id": "string", "route_id": "string", "vehicle_id": "string", "observation_id": "string",
    "record_kind": "string", "observation_time": "timestamp", "event_time": "timestamp",
    "event_time_origin": "string", "freshness_known": "boolean", "position_valid": "boolean",
    "invalid_reason": "string", "latitude": "double", "longitude": "double", "bearing": "double",
    "speed_mps": "double", "vehicle_label": "string", "trip_id": "string", "direction_id": "int",
    "start_date": "string", "start_time": "string", "trip_schedule_relationship": "string", "stop_id": "string",
    "current_stop_sequence": "int", "current_status": "string", "occupancy_status": "string",
    "is_inherited": "boolean", "inherited_from_snapshot_id": "string",
}

# Observation content served from the parent on OBSERVATION_TIME_REGRESSION.
INHERITED_FIELDS: tuple[str, ...] = (
    "observation_id", "record_kind", "observation_time", "event_time", "event_time_origin", "freshness_known",
    "latitude", "longitude", "bearing", "speed_mps", "vehicle_label", "trip_id", "direction_id", "start_date",
    "start_time", "trip_schedule_relationship", "stop_id", "current_stop_sequence", "current_status",
    "occupancy_status",
)


def vehicle_freshness_cols(
    *, position_valid: Column, observation_epoch_s: Column, metrics_as_of_us: int, policy: Policy
) -> tuple[Column, Column, Column]:
    """Spark mirror of ``contracts.policy.vehicle_freshness`` at ``metrics_as_of``.

    Returns ``(freshness, age_seconds, clock_skew)``: freshness in {fresh, stale, unknown, invalid};
    age_seconds is the displayed age (double, null for unknown/invalid, 0.0 under clock skew);
    clock_skew is true for -future_tolerance <= age < 0.
    ``observation_epoch_s`` = whole epoch seconds of the observation (null for state samples).
    """
    obs = observation_epoch_s.cast("long")
    age_us = F.lit(int(metrics_as_of_us)) - obs * F.lit(1_000_000)
    tol_us = int(policy.future_tolerance_seconds) * 1_000_000
    stale_us = int(policy.vehicle_stale_seconds) * 1_000_000
    valid = F.coalesce(position_valid, F.lit(False))
    known = obs.isNotNull()
    freshness = (
        F.when(~valid, F.lit("invalid"))
        .when(~known, F.lit("unknown"))
        .when(age_us < F.lit(-tol_us), F.lit("invalid"))
        .when(age_us <= F.lit(stale_us), F.lit("fresh"))
        .otherwise(F.lit("stale"))
    )
    shown = valid & known & (age_us >= F.lit(-tol_us))
    age_seconds = F.when(shown, F.greatest(age_us, F.lit(0).cast("long")).cast("double") / F.lit(1_000_000.0))
    skew = F.coalesce(shown & (age_us < F.lit(0)), F.lit(False))
    return freshness, age_seconds, skew


def _parse_entities(snap: DataFrame, dims: Dims, scope_routes: list[str]) -> DataFrame:
    ex = snap.select(
        F.posexplode(F.col("entities")).alias("entity_pos", "entity_json"),
        F.coalesce(F.col("header")["timestamp"], F.col("feed_timestamp")).cast("long").alias("header_ts"),
    )
    ex = ex.withColumn("e", F.from_json(F.col("entity_json"), VP_ENTITY_SCHEMA, JSON_OPTIONS))
    e = F.col("e")
    v = e["vehicle"]
    raw_id = F.coalesce(e["id"], F.get_json_object(F.col("entity_json"), "$.id"))
    ents = ex.select(
        F.col("entity_pos").cast("int").alias("entity_pos"),
        F.col("entity_json"),
        F.col("header_ts"),
        (e.isNull() | e[CORRUPT_COL].isNotNull()).alias("corrupt"),
        v.isNull().alias("no_vehicle"),
        S.entity_ref_col(raw_id, F.col("entity_json")).alias("entity_ref"),
        Q.blank_to_null(v["vehicle"]["id"]).alias("vehicle_id"),
        v["vehicle"]["label"].alias("vehicle_label"),
        v["trip"]["trip_id"].alias("trip_id"),
        v["trip"]["route_id"].alias("desc_route_id"),
        v["trip"]["direction_id"].alias("direction_id"),
        v["trip"]["start_date"].alias("start_date"),
        v["trip"]["start_time"].alias("start_time"),
        v["trip"]["schedule_relationship"].alias("trip_schedule_relationship"),
        v["position"]["latitude"].alias("latitude"),
        v["position"]["longitude"].alias("longitude"),
        v["position"]["bearing"].alias("bearing"),
        v["position"]["speed"].alias("speed_mps"),
        v["timestamp"].alias("vehicle_timestamp"),
        v["stop_id"].alias("stop_id"),
        v["current_stop_sequence"].alias("current_stop_sequence"),
        v["current_status"].alias("current_status"),
        v["occupancy_status"].alias("occupancy_status"),
    )
    trips = dims.trips.select(
        F.col("trip_id").cast("string").alias("_t_trip_id"), F.col("route_id").cast("string").alias("_t_route_id")
    ).dropDuplicates(["_t_trip_id"])
    ents = ents.join(F.broadcast(trips), Q.blank_to_null(F.col("trip_id")) == F.col("_t_trip_id"), "left")
    ents = ents.withColumn(
        "route_id", F.coalesce(Q.blank_to_null(F.col("desc_route_id")), F.col("_t_route_id"))
    ).drop("_t_trip_id", "_t_route_id")
    in_scope = F.col("route_id").isin(*scope_routes) if scope_routes else F.lit(False)
    return ents.withColumn("in_scope", F.coalesce(in_scope, F.lit(False)))


def build_vehicle_generation(
    spark: SparkSession,
    snap: DataFrame,
    params: BuildParams,
    run: RunInfo,
    dims: Dims,
    parent: DataFrame | None,
    *,
    dlq_excerpt_max_chars: int = Q.DEFAULT_DLQ_EXCERPT_MAX_CHARS,
) -> GenerationOutput:
    pol = run.policy
    asof_us = Q.epoch_micros(params.metrics_as_of)
    run_id, snapshot_id = params.run_id, params.snapshot_id
    scope_routes = sorted(set(run.route_ids))
    parent_unavailable = params.parent_snapshot_id is not None and not params.parent_available
    use_parent = parent is not None and params.parent_available

    ents = _parse_entities(snap, dims, scope_routes)

    # ---------------------------------------------------------------- validity
    lat, lon, ts = F.col("latitude"), F.col("longitude"), F.col("vehicle_timestamp")
    coord_bad = (
        lat.isNull() | lon.isNull() | F.isnan(lat) | F.isnan(lon)
        | (F.abs(lat) > 90) | (F.abs(lon) > 180) | ((lat == 0) & (lon == 0))
    )
    unit_bad = ts.isNotNull() & ((ts > F.lit(int(pol.timestamp_ms_threshold))) | (ts <= 0))
    ts_ok = ts.isNotNull() & ~unit_bad
    age_us = F.lit(asof_us) - ts * F.lit(1_000_000)
    tol_us = int(pol.future_tolerance_seconds) * 1_000_000
    future = ts_ok & (age_us < F.lit(-tol_us))
    skew = ts_ok & (age_us < 0) & (age_us >= F.lit(-tol_us))
    ents = (
        ents.withColumn("_coord_bad", F.coalesce(coord_bad, F.lit(True)))
        .withColumn("_unit_bad", F.coalesce(unit_bad, F.lit(False)))
        .withColumn("_future", F.coalesce(future, F.lit(False)))
        .withColumn("_skew", F.coalesce(skew, F.lit(False)))
        .withColumn(
            "rejected_reason",
            F.when(F.col("corrupt") | F.col("no_vehicle"), F.lit("INVALID_ENTITY"))
            .when(F.col("vehicle_id").isNull(), F.lit("MISSING_VEHICLE_ID")),
        )
    )
    ents = ents.withColumn(
        "invalid_reason",
        F.when(F.col("rejected_reason").isNotNull(), F.lit(None).cast("string"))
        .when(F.col("_coord_bad"), F.lit("INVALID_COORDINATES"))
        .when(F.col("_unit_bad"), F.lit("INVALID_TIMESTAMP_UNIT"))
        .when(F.col("_future"), F.lit("FUTURE_TIMESTAMP")),
    )
    own_flags = Q.flags_array(
        F.when(F.col("_coord_bad"), F.lit("INVALID_COORDINATES")),
        F.when(F.col("_unit_bad"), F.lit("INVALID_TIMESTAMP_UNIT")),
        F.when(F.col("_future"), F.lit("FUTURE_TIMESTAMP")),
        F.when(F.col("_skew"), F.lit("CLOCK_SKEW")),
        F.when(ts.isNull(), F.lit("MISSING_OBSERVATION_TIMESTAMP")),
        F.when(~F.col("in_scope"), F.lit("UNKNOWN_ROUTE_OR_TRIP")),
    )
    ents = ents.withColumn(
        "entity_flags",
        F.when(F.col("rejected_reason").isNotNull(), F.array(F.col("rejected_reason"))).otherwise(own_flags),
    ).withColumn(
        "entity_status",
        F.when(F.col("rejected_reason").isNotNull(), F.lit("rejected"))
        .when(F.size(F.col("entity_flags")) > 0, F.lit("partially_valid"))
        .otherwise(F.lit("fully_valid")),
    )

    # ---------------------------------------------------------------- ids and times
    common = dict(
        vehicle_id=F.col("vehicle_id"), trip_id=F.col("trip_id"), route_id=F.col("desc_route_id"),
        direction_id=F.col("direction_id"), start_date=F.col("start_date"), start_time=F.col("start_time"),
        trip_schedule_relationship=F.col("trip_schedule_relationship"), latitude=lat, longitude=lon,
        bearing=F.col("bearing"), speed=F.col("speed_mps"), stop_id=F.col("stop_id"),
        current_stop_sequence=F.col("current_stop_sequence"), current_status=F.col("current_status"),
    )
    obs_id = S.observation_id_col(vehicle_timestamp=ts, **common)
    ss_id = S.state_sample_id_col(header_timestamp=F.col("header_ts"), **common)
    ents = (
        ents.withColumn(
            "observation_id",
            F.when(F.col("rejected_reason").isNull(), F.when(ts.isNull(), ss_id).otherwise(obs_id)),
        )
        .withColumn("record_kind", F.when(ts.isNull(), F.lit("snapshot_state_sample")).otherwise(F.lit("observation")))
        .withColumn("observation_epoch", F.when(ts.isNotNull() & ~F.col("_unit_bad"), ts).cast("long"))
    )
    ents = (
        ents.withColumn("event_epoch", F.coalesce(F.col("observation_epoch"), F.col("header_ts")))
        .withColumn("observation_time", F.timestamp_seconds(F.col("observation_epoch")))
        .withColumn("event_time", F.timestamp_seconds(F.col("event_epoch")))
        .withColumn(
            "event_time_origin",
            F.when(F.col("observation_epoch").isNotNull(), F.lit("vehicle_timestamp")).otherwise(F.lit("feed_header")),
        )
        .withColumn("freshness_known", F.col("observation_epoch").isNotNull())
        .withColumn("position_valid", F.col("invalid_reason").isNull())
    )

    # ---------------------------------------------------------------- membership candidates + dedup
    cand = ents.where(F.col("rejected_reason").isNull() & F.col("in_scope"))
    rank_w = Window.partitionBy("vehicle_id").orderBy(
        F.when(F.col("invalid_reason").isNull(), 0).otherwise(1).asc(),
        F.col("observation_epoch").desc_nulls_last(),
        F.col("observation_id").asc(),
        F.col("entity_pos").asc(),
    )
    cand = cand.withColumn("_rn", F.row_number().over(rank_w))
    valid_c = F.col("invalid_reason").isNull()

    # ---------------------------------------------------------------- parent (frozen)
    if use_parent:
        p = Q.ensure_columns(parent, PARENT_VEHICLE_SPEC)
        p_ids = (
            p.where(F.col("observation_id").isNotNull())
            .select(F.col("observation_id").alias("_p_seen_obs")).distinct()
        )
        p_valid = p.where(
            F.col("invalid_reason").isNull() & F.coalesce(F.col("position_valid"), F.lit(True))
            & F.col("observation_time").isNotNull() & F.col("vehicle_id").isNotNull()
        )
        p_w = Window.partitionBy("vehicle_id").orderBy(
            F.col("observation_time").desc(), F.col("observation_id").asc_nulls_last()
        )
        p_veh = p_valid.withColumn("_prn", F.row_number().over(p_w)).where(F.col("_prn") == 1).select(
            *[F.col(c).alias("_p_" + c) for c in PARENT_VEHICLE_SPEC]
        )
        cand = cand.join(p_ids, F.col("observation_id") == F.col("_p_seen_obs"), "left").withColumn(
            "dup_parent", F.col("_p_seen_obs").isNotNull()
        ).drop("_p_seen_obs")
        cand = cand.join(p_veh, F.col("vehicle_id") == F.col("_p_vehicle_id"), "left")
        regress = (
            (F.col("_rn") == 1) & valid_c & F.col("observation_epoch").isNotNull()
            & F.col("_p_observation_time").isNotNull()
            & (F.col("observation_epoch") < F.col("_p_observation_time").cast("long"))
        )
        cand = cand.withColumn("regress", F.coalesce(regress, F.lit(False)))
    else:
        cand = cand.withColumn("dup_parent", F.lit(False)).withColumn("regress", F.lit(False))
        for c, t in PARENT_VEHICLE_SPEC.items():
            cand = cand.withColumn("_p_" + c, F.lit(None).cast(t))

    cand = cand.withColumn(
        "is_duplicate", valid_c & ((F.col("_rn") > 1) | F.col("dup_parent"))
    ).withColumn(
        "obs_flags",
        Q.merge_flags(F.col("entity_flags"), F.when(F.col("regress"), F.lit("OBSERVATION_TIME_REGRESSION"))),
    )

    # ---------------------------------------------------------------- served rows (one per vehicle id)
    reg = F.col("regress")
    served = cand.where(F.col("_rn") == 1)
    for c in INHERITED_FIELDS:
        served = served.withColumn("_srv_" + c, F.when(reg, F.col("_p_" + c)).otherwise(F.col(c)))
    served = (
        served.withColumn("_srv_position_valid", F.when(reg, F.lit(True)).otherwise(F.col("position_valid")))
        .withColumn("_srv_invalid_reason", F.when(reg, F.lit(None).cast("string")).otherwise(F.col("invalid_reason")))
        .withColumn(
            "_srv_obs_epoch",
            F.when(reg, F.col("_p_observation_time").cast("long")).otherwise(F.col("observation_epoch")),
        )
        .withColumn("is_inherited", reg)
        .withColumn(
            "inherited_from_snapshot_id",
            F.when(reg, F.coalesce(F.col("_p_inherited_from_snapshot_id"), F.col("_p_snapshot_id"),
                                   Q.str_lit(params.parent_snapshot_id))),
        )
    )
    freshness, age_s, _ = vehicle_freshness_cols(
        position_valid=F.col("_srv_position_valid"), observation_epoch_s=F.col("_srv_obs_epoch"),
        metrics_as_of_us=asof_us, policy=pol,
    )
    served = served.withColumn("freshness", freshness).withColumn("age_seconds", age_s)

    feed_ts = Q.ts_lit(params.feed_timestamp)
    vehicles = served.select(
        Q.str_lit(run_id).alias("run_id"),
        Q.str_lit(snapshot_id).alias("snapshot_id"),
        F.col("route_id"),
        F.col("vehicle_id"),
        F.col("_srv_observation_id").alias("observation_id"),
        F.col("_srv_record_kind").alias("record_kind"),
        F.col("_srv_observation_time").alias("observation_time"),
        F.col("_srv_event_time").alias("event_time"),
        F.col("_srv_event_time_origin").alias("event_time_origin"),
        F.col("_srv_freshness_known").alias("freshness_known"),
        F.col("_srv_position_valid").alias("position_valid"),
        F.col("_srv_invalid_reason").alias("invalid_reason"),
        F.col("_srv_latitude").alias("latitude"),
        F.col("_srv_longitude").alias("longitude"),
        F.col("_srv_bearing").alias("bearing"),
        F.col("_srv_speed_mps").alias("speed_mps"),
        F.col("_srv_vehicle_label").alias("vehicle_label"),
        F.col("_srv_trip_id").alias("trip_id"),
        F.col("_srv_direction_id").cast("int").alias("direction_id"),
        F.col("_srv_start_date").alias("start_date"),
        F.col("_srv_start_time").alias("start_time"),
        F.col("_srv_trip_schedule_relationship").alias("trip_schedule_relationship"),
        F.col("_srv_stop_id").alias("stop_id"),
        F.col("_srv_current_stop_sequence").cast("int").alias("current_stop_sequence"),
        F.col("_srv_current_status").alias("current_status"),
        F.col("_srv_occupancy_status").alias("occupancy_status"),
        feed_ts.alias("feed_timestamp"),
        F.col("is_inherited"),
        F.col("inherited_from_snapshot_id"),
        F.col("obs_flags").alias("quality_flags"),
    )

    # ---------------------------------------------------------------- history (unique valid observations)
    hist_src = cand.where(valid_c & ~F.col("is_duplicate"))
    history = hist_src.select(
        Q.str_lit(run_id).alias("run_id"),
        F.col("vehicle_id"),
        F.expr("date_from_unix_date(CAST(floor(event_epoch / 86400) AS INT))").alias("utc_date"),
        F.col("event_time"),
        F.col("observation_id"),
        F.col("record_kind"),
        F.col("event_time_origin"),
        F.col("route_id"),
        F.col("trip_id"),
        F.col("direction_id").cast("int").alias("direction_id"),
        F.col("start_date"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("bearing"),
        F.col("speed_mps"),
        F.col("stop_id"),
        F.col("current_stop_sequence").cast("int").alias("current_stop_sequence"),
        F.col("current_status"),
        F.col("obs_flags").alias("quality_flags"),
    )

    # ---------------------------------------------------------------- route metrics
    fr = F.col("freshness")
    srv_agg = served.groupBy("route_id").agg(
        F.count(F.lit(1)).alias("_members"),
        F.sum(F.when(fr == "fresh", 1).otherwise(0)).alias("_fresh"),
        F.sum(F.when(fr == "stale", 1).otherwise(0)).alias("_stale"),
        F.sum(F.when(fr == "unknown", 1).otherwise(0)).alias("_unknown"),
        F.sum(F.when(fr == "invalid", 1).otherwise(0)).alias("_invalid_id"),
        F.sum(F.when(F.col("is_inherited"), 1).otherwise(0)).alias("_inherited"),
        F.array_sort(F.array_distinct(F.flatten(F.collect_list("obs_flags")))).alias("_flags"),
    )
    missing_id = (
        ents.where((F.col("rejected_reason") == "MISSING_VEHICLE_ID") & F.col("in_scope"))
        .groupBy("route_id").agg(F.count(F.lit(1)).alias("_missing_id"))
    )
    scope = Q.scope_routes_frame(spark, scope_routes)
    rm = scope.join(srv_agg, "route_id", "left").join(missing_id, "route_id", "left")
    z = lambda c: F.coalesce(F.col(c), F.lit(0)).cast("long")  # noqa: E731
    rm = rm.select(
        F.col("route_id"),
        z("_members").alias("_members"), z("_fresh").alias("_fresh"), z("_stale").alias("_stale"),
        z("_unknown").alias("_unknown"), z("_invalid_id").alias("_invalid_id"), z("_inherited").alias("_inherited"),
        z("_missing_id").alias("_missing_id"),
        Q.merge_flags(
            F.col("_flags"),
            F.when(F.coalesce(F.col("_missing_id"), F.lit(0)) > 0, F.lit("MISSING_VEHICLE_ID")),
        ).alias("_flags"),
    )
    counts = Q.entity_status_counts(ents)
    partial = counts.select((F.col("rejected_entity_count") > 0).alias("is_partial"))
    route_metrics = rm.crossJoin(partial).select(
        Q.str_lit(run_id).alias("run_id"),
        F.lit(FEED_TYPE).alias("feed_type"),
        Q.str_lit(snapshot_id).alias("snapshot_id"),
        F.col("route_id"),
        Q.ts_lit(params.metrics_as_of).alias("metrics_as_of"),
        feed_ts.alias("feed_timestamp"),
        F.col("is_partial"),
        Q.str_lit(pol.policy_version).alias("policy_version"),
        F.col("_members").cast("int").alias("snapshot_membership_count"),
        F.col("_fresh").cast("int").alias("fresh_vehicle_count"),
        F.col("_stale").cast("int").alias("stale_vehicle_count"),
        F.col("_unknown").cast("int").alias("unknown_age_vehicle_count"),
        (F.col("_invalid_id") + F.col("_missing_id")).cast("int").alias("invalid_vehicle_count"),
        F.col("_inherited").cast("int").alias("inherited_vehicle_count"),
        F.lit(int(pol.vehicle_stale_seconds)).cast("int").alias("stale_threshold_seconds"),
        F.col("_flags").alias("quality_flags"),
    )

    # ---------------------------------------------------------------- quality issues
    issue_extra = [
        F.col("entity_pos"), F.col("entity_ref"), F.lit(None).cast("string").alias("sub_ref"),
        F.col("observation_id").alias("record_id"), F.col("route_id"), F.col("event_time"),
        F.col("entity_status"),
    ]
    own_issues = ents.select(
        F.explode(F.col("entity_flags")).alias("reason_code"), F.col("entity_ref").alias("ref"),
        F.when(F.col("rejected_reason").isNotNull(), F.lit("rejected")).alias("disposition"), *issue_extra,
    )
    snap_dups = cand.where(valid_c & (F.col("_rn") > 1)).select(
        F.lit("DUPLICATE_OBSERVATION").alias("reason_code"), F.col("entity_ref").alias("ref"),
        F.lit("duplicate").alias("disposition"), *issue_extra,
    )
    parent_dups = cand.where(valid_c & (F.col("_rn") == 1) & F.col("dup_parent")).select(
        F.lit("DUPLICATE_OBSERVATION").alias("reason_code"), F.col("entity_ref").alias("ref"),
        F.lit("duplicate").alias("disposition"),
    )
    regress_issues = cand.where(F.col("regress")).select(
        F.lit("OBSERVATION_TIME_REGRESSION").alias("reason_code"), F.col("entity_ref").alias("ref"),
        F.lit(None).cast("string").alias("disposition"),
    )
    q_frames = [own_issues, snap_dups, parent_dups, regress_issues]
    if parent_unavailable:
        q_frames.append(Q.snapshot_issue(spark, "PARENT_UNAVAILABLE", params.parent_snapshot_id))
    quality = Q.quality_rows(Q.union_issues(q_frames), run_id=run_id, feed_type=FEED_TYPE,
                             snapshot_id=snapshot_id, policy_version=pol.policy_version)
    entity_quality = Q.entity_quality_rows(own_issues.unionByName(snap_dups), params=params, feed_type=FEED_TYPE)

    # ---------------------------------------------------------------- stats / accounting
    cstats = cand.agg(
        F.coalesce(F.sum(F.when(valid_c, 1).otherwise(0)), F.lit(0)).alias("valid_candidate_observations"),
        F.coalesce(F.sum(F.when(valid_c & ~F.col("is_duplicate"), 1).otherwise(0)), F.lit(0))
        .alias("unique_observations"),
        F.coalesce(F.sum(F.when(F.col("is_duplicate"), 1).otherwise(0)), F.lit(0)).alias("duplicate_observations"),
        F.coalesce(F.sum(F.when(valid_c & (F.col("_rn") > 1), 1).otherwise(0)), F.lit(0))
        .alias("in_snapshot_duplicates"),
        F.coalesce(F.sum(F.when(valid_c & (F.col("_rn") == 1) & F.col("dup_parent"), 1).otherwise(0)), F.lit(0))
        .alias("parent_duplicates"),
        F.coalesce(F.sum(F.when(F.col("regress"), 1).otherwise(0)), F.lit(0)).alias("regressions"),
    )
    estats = ents.agg(
        F.coalesce(F.sum(F.when(F.col("rejected_reason").isNull() & ~F.col("in_scope"), 1).otherwise(0)), F.lit(0))
        .alias("out_of_scope_entities"),
        F.coalesce(F.sum(F.when(F.col("rejected_reason") == "MISSING_VEHICLE_ID", 1).otherwise(0)), F.lit(0))
        .alias("missing_vehicle_id_entities"),
        F.coalesce(F.sum(F.when(F.col("rejected_reason") == "INVALID_ENTITY", 1).otherwise(0)), F.lit(0))
        .alias("invalid_json_entities"),
    )
    rcheck = rm.agg(
        F.coalesce(F.sum(F.when(
            F.col("_members") != F.col("_fresh") + F.col("_stale") + F.col("_unknown") + F.col("_invalid_id"), 1
        ).otherwise(0)), F.lit(0)).alias("_route_violations"),
        F.map_from_entries(F.array_sort(F.collect_list(F.struct(
            F.col("route_id").alias("k"),
            F.struct(
                F.col("_members").alias("membership"), F.col("_fresh").alias("fresh"),
                F.col("_stale").alias("stale"), F.col("_unknown").alias("unknown_age"),
                F.col("_invalid_id").alias("invalid_with_vehicle_id"),
                F.col("_missing_id").alias("invalid_without_vehicle_id"),
                F.col("_inherited").alias("inherited"),
            ).alias("v"),
        )))).alias("_per_route"),
    )
    base = counts.crossJoin(cstats).crossJoin(estats).crossJoin(rcheck)
    accounting = F.struct(
        F.lit(FEED_TYPE).alias("feed_type"),
        F.lit(BUILDER_VERSION).alias("builder"),
        # top-level ints (read by WP4 batch.accounting_metrics)
        F.col("valid_candidate_observations").alias("valid_candidate_observations"),
        F.col("unique_observations").alias("unique_observations"),
        F.col("duplicate_observations").alias("duplicate_observations"),
        F.struct(
            F.col("selected_entity_count").alias("selected"),
            F.col("fully_valid_entity_count").alias("fully_valid"),
            F.col("partially_valid_entity_count").alias("partially_valid"),
            F.col("rejected_entity_count").alias("rejected"),
            F.col("out_of_scope_entities").alias("out_of_scope"),
            F.col("missing_vehicle_id_entities").alias("missing_vehicle_id"),
            F.col("invalid_json_entities").alias("invalid_entity"),
        ).alias("entities"),
        F.struct(
            F.col("valid_candidate_observations").alias("valid_candidate"),
            F.col("unique_observations").alias("unique"),
            F.col("duplicate_observations").alias("duplicate"),
            F.col("in_snapshot_duplicates").alias("in_snapshot_duplicate"),
            F.col("parent_duplicates").alias("parent_duplicate"),
            F.col("regressions").alias("time_regressions"),
        ).alias("observations"),
        F.col("_per_route").alias("per_route"),
        F.lit(params.parent_snapshot_id).cast("string").alias("parent_snapshot_id"),
        F.lit(bool(use_parent)).alias("parent_used"),
        F.lit(pol.policy_version).alias("policy_version"),
        F.lit("entity|observation|vehicle_id").alias("units"),
    )
    stats = Q.build_stats(base, accounting=accounting, identities=[
        ("valid_candidate_observations == unique_observations + duplicate_observations",
         F.col("valid_candidate_observations") == F.col("unique_observations") + F.col("duplicate_observations")),
        ("per route: snapshot_membership_count == fresh + stale + unknown_age + invalid_with_vehicle_id",
         F.col("_route_violations") == 0),
    ])

    # ---------------------------------------------------------------- curated (parent independent) + DLQ
    own_fresh, own_age, _ = vehicle_freshness_cols(
        position_valid=F.col("position_valid"), observation_epoch_s=F.col("observation_epoch"),
        metrics_as_of_us=asof_us, policy=pol,
    )
    observations = cand.select(
        Q.str_lit(run_id).alias("run_id"),
        Q.str_lit(snapshot_id).alias("snapshot_id"),
        F.lit(FEED_TYPE).alias("feed_type"),
        F.col("observation_id"), F.col("record_kind"), F.col("vehicle_id"), F.col("route_id"),
        F.col("event_time"), F.col("event_time_origin"), F.col("observation_time"), F.col("freshness_known"),
        F.col("position_valid"), F.col("invalid_reason"),
        F.col("latitude"), F.col("longitude"), F.col("bearing"), F.col("speed_mps"), F.col("vehicle_label"),
        F.col("trip_id"), F.col("direction_id").cast("int").alias("direction_id"), F.col("start_date"),
        F.col("start_time"), F.col("trip_schedule_relationship"), F.col("stop_id"),
        F.col("current_stop_sequence").cast("int").alias("current_stop_sequence"), F.col("current_status"),
        F.col("occupancy_status"),
        own_fresh.alias("freshness_at_build"), own_age.alias("age_seconds_at_build"),
        (F.col("_rn") > 1).alias("in_snapshot_duplicate"),
        F.col("entity_status"), F.col("entity_flags").alias("quality_flags"),
        F.col("entity_ref"), F.col("entity_pos"),
        Q.str_lit(params.capture_id).alias("capture_id"),
        Q.ts_lit(params.metrics_as_of).alias("metrics_as_of"),
        feed_ts.alias("feed_timestamp"),
    )
    dlq = Q.entity_dlq_rows(
        ents.where(F.col("rejected_reason").isNotNull()).select(
            "entity_ref", F.lit(None).cast("string").alias("sub_ref"), F.col("rejected_reason").alias("reason_code"),
            "entity_json", F.lit("rejected").alias("disposition"),
        ),
        params=params, run=run, feed_type=FEED_TYPE, stage=DLQ_STAGE, max_excerpt_chars=dlq_excerpt_max_chars,
    )

    return GenerationOutput(
        tables={
            "vehicles_by_route_snapshot": vehicles.select(*VEHICLE_COLUMNS),
            "vehicle_observations_by_day": history.select(*HISTORY_COLUMNS),
            "route_metrics_by_snapshot": route_metrics.select(*VP_ROUTE_METRIC_COLUMNS),
            "quality_by_snapshot": quality.select(*Q.QUALITY_COLUMNS),
        },
        stats=stats,
        curated={"observations": observations, "entity_quality": entity_quality},
        dlq=dlq,
        notes={"builder": BUILDER_VERSION, "feed_type": FEED_TYPE, "parent_used": bool(use_parent),
               "parent_unavailable": bool(parent_unavailable)},
    )
