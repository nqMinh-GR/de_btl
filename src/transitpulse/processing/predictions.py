"""TripUpdates generation builder (WP5): predictions, schedule resolution, delay and target-stop KPI.

Pipeline for ONE snapshot (design C §3-§5, design B §2/§6):
  1. ``posexplode(entities)`` + ``from_json(TU_ENTITY_SCHEMA)``. Corrupt entity / no ``trip_update`` ->
     INVALID_ENTITY; missing trip_id -> INVALID_PREDICTION (quarantined, no key is invented). Both are
     rejected entities and go to the DLQ.
  2. Trip instances resolved with :mod:`processing.schedule`. Several entities resolving to the same
     ``trip_instance_key`` keep the highest ``trip_update.timestamp`` then the smallest entity id; the others
     are DUPLICATE_OBSERVATION and their predictions are not_applicable.
  3. Each stop_time_update yields up to two candidate predictions (arrival / departure objects).
     Disposition (accounting unit = prediction): ``retained`` (stored in predictions_by_stop_snapshot),
     ``rejected`` (rejected entity, time > policy.timestamp_ms_threshold or <= 0 -> INVALID_TIMESTAMP_UNIT,
     no stop id at all) or ``not_applicable`` (duplicate entity, neither usable time nor delay ->
     NO_PREDICTION_DATA).
  4. ``predicted_delay_seconds = event_time - (anchor + seconds of the SAME event type)`` only when the trip
     is resolved and not CANCELED/ADDED, the stop is resolved, the STU is SCHEDULED, the class is not
     schedule_only/invalid. Never clamped (negative = early).
  5. Target stop + KPI exclusion per trip instance (:mod:`processing.kpi`), aggregated per scope route.
Everything is evaluated at ``params.metrics_as_of``; no output depends on the parent generation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from transitpulse.processing import kpi as K
from transitpulse.processing import quality as Q
from transitpulse.processing import schedule as SCH
from transitpulse.processing import spark_ids as S
from transitpulse.processing.context import GenerationOutput
from transitpulse.processing.schemas import CORRUPT_COL, JSON_OPTIONS, TU_ENTITY_SCHEMA

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

    from transitpulse.processing.context import BuildParams, RunInfo
    from transitpulse.static.dims import Dims

FEED_TYPE = "trip_updates"
DLQ_STAGE = "tu_entity_validation"
BUILDER_VERSION = "wp5-tu-v1"

Q.check_registered((
    "INVALID_ENTITY", "INVALID_PREDICTION", "DUPLICATE_OBSERVATION", "INVALID_TIMESTAMP_UNIT",
    "NO_PREDICTION_DATA", "UNRESOLVED_STOP_SEQUENCE", "CANCELED_TRIP", "ADDED_TRIP", "NO_TARGET_STOP",
    "NO_SCHEDULED_EVENT",
))

PREDICTION_COLUMNS: tuple[str, ...] = (
    "run_id", "snapshot_id", "stop_id", "predicted_at", "prediction_id", "event_type", "trip_id", "route_id",
    "direction_id", "start_date", "start_time", "service_date", "trip_instance_key", "stop_sequence",
    "trip_schedule_relationship", "stop_schedule_relationship", "vehicle_id", "scheduled_at",
    "predicted_delay_seconds", "schedule_resolution", "resolution_method", "uncertainty", "prediction_quality",
    "kpi_eligible", "is_target", "feed_timestamp", "quality_flags",
)

TU_ROUTE_METRIC_COLUMNS: tuple[str, ...] = (
    "run_id", "feed_type", "snapshot_id", "route_id", "metrics_as_of", "feed_timestamp", "is_partial",
    "policy_version", "stale_threshold_seconds", "trip_instance_count", "eligible_trip_count",
    "predicted_late_trip_count", "predicted_late_rate", "mean_predicted_delay_seconds", "excluded_trip_counts",
    "unknown_uncertainty_count", "late_threshold_seconds", "target_policy", "quality_flags",
)

# trip-level reasons that the KPI step itself reports in quality_by_snapshot (others are prediction-level or
# already reported by schedule resolution)
_KPI_ONLY_QUALITY = ("NO_TARGET_STOP", "NO_SCHEDULED_EVENT")


def _parse_entities(snap: DataFrame) -> DataFrame:
    ex = snap.select(F.posexplode(F.col("entities")).alias("entity_pos", "entity_json"))
    ex = ex.withColumn("e", F.from_json(F.col("entity_json"), TU_ENTITY_SCHEMA, JSON_OPTIONS))
    e = F.col("e")
    tu = e["trip_update"]
    trip = tu["trip"]
    corrupt = e.isNull() | e[CORRUPT_COL].isNotNull()
    raw_id = F.coalesce(e["id"], F.get_json_object(F.col("entity_json"), "$.id"))
    ents = ex.select(
        F.col("entity_pos").cast("int").alias("entity_pos"),
        F.col("entity_json"),
        corrupt.alias("corrupt"),
        tu.isNotNull().alias("has_tu"),
        Q.blank_to_null(e["id"]).alias("entity_id"),
        S.entity_ref_col(raw_id, F.col("entity_json")).alias("entity_ref"),
        # source values (prediction_id uses them unchanged)
        trip["trip_id"].alias("src_trip_id"),
        trip["route_id"].alias("src_route_id"),
        trip["start_date"].alias("src_start_date"),
        trip["start_time"].alias("src_start_time"),
        trip["direction_id"].alias("desc_direction_id"),
        trip["schedule_relationship"].alias("trip_schedule_relationship"),
        # normalized values (blank -> null)
        Q.blank_to_null(trip["trip_id"]).alias("trip_id"),
        Q.blank_to_null(trip["route_id"]).alias("desc_route_id"),
        Q.blank_to_null(trip["start_date"]).alias("start_date"),
        Q.blank_to_null(trip["start_time"]).alias("start_time"),
        F.upper(F.coalesce(Q.blank_to_null(trip["schedule_relationship"]), F.lit("SCHEDULED"))).alias("trip_rel"),
        tu["timestamp"].alias("tu_timestamp"),
        tu["vehicle"]["id"].alias("vehicle_id"),
        tu["stop_time_update"].alias("stus"),
    )
    rejected = (
        F.when(F.col("corrupt") | ~F.col("has_tu"), F.lit("INVALID_ENTITY"))
        .when(F.col("trip_id").isNull(), F.lit("INVALID_PREDICTION"))
    )
    return ents.withColumn("rejected_reason", rejected)


def _sub_ref(seq: Column, stop_id: Column, stu_pos: Column, event_type: Column | None) -> Column:
    loc = F.coalesce(seq.cast("string"), F.concat(F.lit("sid:"), stop_id), F.concat(F.lit("pos:"), stu_pos.cast("string")))
    return F.concat_ws("|", loc, event_type) if event_type is not None else loc


def build_trip_update_generation(
    spark: SparkSession,
    snap: DataFrame,
    params: BuildParams,
    run: RunInfo,
    dims: Dims,
    *,
    dlq_excerpt_max_chars: int = Q.DEFAULT_DLQ_EXCERPT_MAX_CHARS,
) -> GenerationOutput:
    pol = run.policy
    asof_us = Q.epoch_micros(params.metrics_as_of)
    tz = run.agency_timezone or dims.agency_timezone
    run_id, snapshot_id = params.run_id, params.snapshot_id

    ents = _parse_entities(snap)
    rejected_ent = F.col("rejected_reason").isNotNull()

    # ---------------------------------------------------------------- trip instances
    inst_in = ents.where(~rejected_ent).select(
        F.col("entity_pos").alias("_key"),
        F.col("trip_id"),
        F.col("desc_route_id").alias("d_route_id"),
        F.col("desc_direction_id").alias("d_direction_id"),
        F.col("start_date"), F.col("start_time"), F.col("trip_rel"), F.col("tu_timestamp"),
        F.col("entity_id"), F.col("entity_ref"), F.col("vehicle_id"), F.col("trip_schedule_relationship"),
    )
    inst = SCH.resolve_trip_instances(
        spark, inst_in, dims, metrics_as_of=params.metrics_as_of, agency_timezone=tz,
        window_seconds=pol.service_date_inference_window_seconds,
    )
    dup_w = Window.partitionBy("trip_instance_key").orderBy(
        F.col("tu_timestamp").desc_nulls_last(), F.col("entity_id").asc_nulls_last(), F.col("_key").asc()
    )
    inst = inst.withColumn("is_dup_entity", F.row_number().over(dup_w) > 1)

    # ---------------------------------------------------------------- stop time updates
    stus = ents.where(~F.col("corrupt") & F.col("has_tu")).select(
        "entity_pos", "entity_ref", "rejected_reason", "trip_id", "src_trip_id", "src_route_id",
        "desc_direction_id", "src_start_date", "src_start_time",
        F.posexplode(F.col("stus")).alias("stu_pos", "stu"),
    )
    inst_cols = inst.select(
        F.col("_key").alias("_ik"), "trip_known", "trip_rel", "schedule_resolution", "resolution_method",
        "resolution_reason", "service_date_text", "anchor_epoch", "route_id", "direction_id",
        "trip_instance_key", "is_dup_entity", "vehicle_id", "trip_schedule_relationship", "start_date",
        "start_time",
    )
    stus = stus.join(inst_cols, F.col("entity_pos") == F.col("_ik"), "left").drop("_ik")
    stu = F.col("stu")
    stus = (
        stus.withColumn("stu_seq", stu["stop_sequence"])
        .withColumn("src_stu_stop_id", stu["stop_id"])
        .withColumn("stu_stop_id", Q.blank_to_null(stu["stop_id"]))
        .withColumn("stu_rel_raw", stu["schedule_relationship"])
        .withColumn("stu_rel", F.upper(F.coalesce(Q.blank_to_null(stu["schedule_relationship"]), F.lit("SCHEDULED"))))
        .withColumn("is_dup", F.coalesce(F.col("is_dup_entity"), F.lit(False)))
        .withColumn(
            "_stop_lookup",
            F.coalesce(F.col("trip_known"), F.lit(False))
            & ~F.coalesce(F.col("trip_rel").isin(*SCH.ADDED_RELATIONSHIPS), F.lit(False)),
        )
    )
    stus = SCH.resolve_stops(stus, dims)

    # ---------------------------------------------------------------- candidate predictions
    evs = stus.select(
        "*",
        F.explode(F.array(
            F.struct(F.lit("arrival").alias("event_type"), stu["arrival"].alias("ev")),
            F.struct(F.lit("departure").alias("event_type"), stu["departure"].alias("ev")),
        )).alias("_x"),
    ).where(F.col("_x")["ev"].isNotNull())
    evs = (
        evs.withColumn("event_type", F.col("_x")["event_type"])
        .withColumn("ev_time", F.col("_x")["ev"]["time"])
        .withColumn("ev_delay", F.col("_x")["ev"]["delay"])
        .withColumn("uncertainty", F.col("_x")["ev"]["uncertainty"])
        .drop("_x")
    )
    ev_time = F.col("ev_time")
    trip_resolved = F.coalesce(F.col("schedule_resolution") == "resolved", F.lit(False))
    sched_secs = F.when(F.col("event_type") == "arrival", F.col("arr_secs")).otherwise(F.col("dep_secs"))
    evs = (
        evs.withColumn(
            "_time_bad",
            ev_time.isNotNull() & ((ev_time > F.lit(int(pol.timestamp_ms_threshold))) | (ev_time <= 0)),
        )
        .withColumn(
            "scheduled_epoch",
            F.when(trip_resolved & F.col("stop_resolved"), SCH.scheduled_epoch_col(F.col("anchor_epoch"), sched_secs)),
        )
    )
    evs = evs.withColumn(
        "event_epoch",
        F.when(ev_time.isNotNull(), F.when(~F.col("_time_bad"), ev_time))
        .when(F.col("ev_delay").isNotNull() & F.col("scheduled_epoch").isNotNull(),
              F.col("scheduled_epoch") + F.col("ev_delay"))
        .cast("long"),
    )
    evs = (
        evs.withColumn(
            "time_origin",
            F.when(ev_time.isNotNull() & ~F.col("_time_bad"), F.lit("source_time"))
            .when(F.col("event_epoch").isNotNull(), F.lit("derived_from_delay")),
        )
        .withColumn("uc", K.uncertainty_class_col(F.col("uncertainty")))
        .withColumn("_out_stop_id", F.coalesce(F.col("stu_stop_id"), F.col("static_stop_id")))
        .withColumn("sort_seq", F.col("resolved_seq"))
        .withColumn(
            "prediction_id",
            S.prediction_id_col(
                trip_id=F.col("src_trip_id"), start_date=F.col("src_start_date"), start_time=F.col("src_start_time"),
                route_id=F.col("src_route_id"), direction_id=F.col("desc_direction_id"),
                stop_sequence=F.col("stu_seq"), stop_id=F.col("src_stu_stop_id"), event_type=F.col("event_type"),
            ),
        )
    )
    disp0 = (
        F.when(rejected_ent, F.lit("rejected"))
        .when(F.col("is_dup"), F.lit("not_applicable"))
        .when(F.col("_time_bad"), F.lit("rejected"))
        .when(F.col("event_epoch").isNull(), F.lit("not_applicable"))
        .when(F.col("_out_stop_id").isNull(), F.lit("rejected"))
        .otherwise(F.lit("retained"))
    )
    evs = evs.withColumn("_disp0", disp0)
    # defensive primary-key guard (stop_id, predicted_at, prediction_id) among retained rows
    pk_w = Window.partitionBy("_disp0", "_out_stop_id", "event_epoch", "prediction_id").orderBy(
        F.col("entity_pos").asc(), F.col("stu_pos").asc(), F.col("event_type").asc()
    )
    evs = evs.withColumn("_pk_rank", F.row_number().over(pk_w))
    pk_dup = (F.col("_disp0") == "retained") & (F.col("_pk_rank") > 1)
    evs = evs.withColumn("disposition", F.when(pk_dup, F.lit("not_applicable")).otherwise(F.col("_disp0")))
    evs = evs.withColumn(
        "pred_issue",
        F.when(rejected_ent | F.col("is_dup"), F.lit(None).cast("string"))
        .when(F.col("_time_bad"), F.lit("INVALID_TIMESTAMP_UNIT"))
        .when(F.col("event_epoch").isNull(), F.lit("NO_PREDICTION_DATA"))
        .when(F.col("_out_stop_id").isNull() & F.col("stop_reason").isNull(), F.lit("UNRESOLVED_STOP_SEQUENCE"))
        .when(pk_dup, F.lit("DUPLICATE_OBSERVATION")),
    )
    rel = F.col("trip_rel")
    delay_ok = (
        trip_resolved
        & ~F.coalesce(rel.isin(*(SCH.CANCELED_RELATIONSHIPS + SCH.ADDED_RELATIONSHIPS)), F.lit(True))
        & F.col("stop_resolved")
        & (F.col("stu_rel") == "SCHEDULED")
        & F.col("event_epoch").isNotNull()
        & F.col("scheduled_epoch").isNotNull()
        & ~F.col("uc").isin(K.UC_SCHEDULE_ONLY, K.UC_INVALID)
        & (F.col("disposition") == "retained")
    )
    evs = evs.withColumn(
        "delay_s", F.when(delay_ok, F.col("event_epoch") - F.col("scheduled_epoch")).cast("int")
    )

    # ---------------------------------------------------------------- target stop + KPI
    retained = evs.where(F.col("disposition") == "retained")
    targets, unresolved = K.select_targets(
        retained.select(
            F.col("entity_pos").alias("_inst"), "stu_pos", "event_type", "event_epoch", "stu_rel",
            "stop_resolved", "sort_seq", "prediction_id", "uc", "delay_s", F.col("_out_stop_id").alias("stop_id"),
        ),
        asof_us,
    )
    survivors = inst.where(~F.col("is_dup_entity")).withColumn("_inst", F.col("_key"))
    trips = K.trip_kpi(survivors, targets, unresolved)

    tgt_keys = trips.select(
        F.col("_key").alias("_rk"), F.col("kpi_reason").alias("trip_kpi_reason"),
        F.col("_tg_stu_pos").alias("_rk_stu"), F.col("_tg_event_type").alias("_rk_ev"),
    )
    evs = evs.join(tgt_keys, F.col("entity_pos") == F.col("_rk"), "left").drop("_rk")
    is_target = F.coalesce(
        (F.col("stu_pos") == F.col("_rk_stu")) & (F.col("event_type") == F.col("_rk_ev")), F.lit(False)
    )
    evs = (
        evs.withColumn("is_target", is_target)
        .withColumn("kpi_eligible", F.col("is_target") & F.col("trip_kpi_reason").isNull())
        .drop("_rk_stu", "_rk_ev")
    )
    evs = evs.withColumn(
        "quality_flags",
        Q.flags_array(
            F.col("resolution_reason"),
            F.when(rel.isin(*SCH.CANCELED_RELATIONSHIPS), F.lit("CANCELED_TRIP")),
            F.when(rel.isin(*SCH.ADDED_RELATIONSHIPS), F.lit("ADDED_TRIP")),
            F.col("stop_reason"),
            F.col("pred_issue"),
            K.uncertainty_reason_col(F.col("uc")),
            F.when(F.col("is_target"), F.col("trip_kpi_reason")),
        ),
    )

    feed_ts = Q.ts_lit(params.feed_timestamp)
    preds = evs.where(F.col("disposition") == "retained").select(
        Q.str_lit(run_id).alias("run_id"),
        Q.str_lit(snapshot_id).alias("snapshot_id"),
        F.col("_out_stop_id").alias("stop_id"),
        F.timestamp_seconds(F.col("event_epoch")).alias("predicted_at"),
        F.col("prediction_id"),
        F.col("event_type"),
        F.col("trip_id"),
        F.col("route_id").cast("string").alias("route_id"),
        F.col("direction_id").cast("int").alias("direction_id"),
        F.col("start_date"),
        F.col("start_time"),
        F.col("service_date_text").alias("service_date"),
        F.col("trip_instance_key"),
        F.col("sort_seq").cast("int").alias("stop_sequence"),
        F.col("trip_schedule_relationship"),
        F.col("stu_rel_raw").alias("stop_schedule_relationship"),
        F.col("vehicle_id"),
        F.timestamp_seconds(F.col("scheduled_epoch")).alias("scheduled_at"),
        F.col("delay_s").alias("predicted_delay_seconds"),
        F.col("schedule_resolution"),
        F.col("resolution_method"),
        F.col("uncertainty").cast("int").alias("uncertainty"),
        F.col("uc").alias("prediction_quality"),
        F.col("kpi_eligible"),
        F.col("is_target"),
        feed_ts.alias("feed_timestamp"),
        F.col("quality_flags"),
    )

    # ---------------------------------------------------------------- issues (all parent-independent)
    rej_issues = ents.where(rejected_ent).select(
        F.col("rejected_reason").alias("reason_code"), F.col("entity_ref").alias("ref"),
        F.lit("rejected").alias("disposition"), F.col("entity_pos"), F.col("entity_ref"),
        F.lit(None).cast("string").alias("sub_ref"), F.lit(None).cast("string").alias("record_id"),
        F.col("desc_route_id").alias("route_id"), F.lit(None).cast("timestamp").alias("event_time"),
    )
    dup_issues = inst.where(F.col("is_dup_entity")).select(
        F.lit("DUPLICATE_OBSERVATION").alias("reason_code"), F.col("entity_ref").alias("ref"),
        F.lit("duplicate").alias("disposition"), F.col("_key").alias("entity_pos"), F.col("entity_ref"),
        F.lit(None).cast("string").alias("sub_ref"), F.col("trip_instance_key").alias("record_id"),
        F.col("route_id"), F.lit(None).cast("timestamp").alias("event_time"),
    )
    trip_codes = Q.flags_array(
        F.col("resolution_reason"),
        F.when(rel.isin(*SCH.CANCELED_RELATIONSHIPS), F.lit("CANCELED_TRIP")),
        F.when(rel.isin(*SCH.ADDED_RELATIONSHIPS), F.lit("ADDED_TRIP")),
        F.when(F.col("kpi_reason").isin(*_KPI_ONLY_QUALITY), F.col("kpi_reason")),
    )
    trip_issues = trips.select(
        F.explode(trip_codes).alias("reason_code"), F.col("entity_ref").alias("ref"),
        F.lit(None).cast("string").alias("disposition"), F.col("_key").alias("entity_pos"), F.col("entity_ref"),
        F.lit(None).cast("string").alias("sub_ref"), F.col("trip_instance_key").alias("record_id"),
        F.col("route_id"), F.lit(None).cast("timestamp").alias("event_time"),
    )
    pred_codes = Q.flags_array(
        F.col("pred_issue"),
        F.col("stop_reason"),
        F.when(F.col("disposition") == "retained", K.uncertainty_reason_col(F.col("uc"))),
    )
    pred_disp = F.when(F.col("disposition") != "retained", F.col("disposition"))
    pred_issues = evs.where(~rejected_ent & ~F.col("is_dup")).select(
        F.explode(pred_codes).alias("reason_code"),
        F.coalesce(F.col("prediction_id"), F.col("entity_ref")).alias("ref"),
        pred_disp.alias("disposition"), F.col("entity_pos"), F.col("entity_ref"),
        _sub_ref(F.col("stu_seq"), F.col("stu_stop_id"), F.col("stu_pos"), F.col("event_type")).alias("sub_ref"),
        F.col("prediction_id").alias("record_id"), F.col("route_id"),
        F.timestamp_seconds(F.col("event_epoch")).alias("event_time"),
    )
    no_event_stus = stus.where(
        ~rejected_ent & ~F.col("is_dup") & F.col("stu")["arrival"].isNull() & F.col("stu")["departure"].isNull()
        & (F.col("stu_rel") != "SKIPPED")
    ).select(
        F.lit("NO_PREDICTION_DATA").alias("reason_code"), F.col("entity_ref").alias("ref"),
        F.lit("not_applicable").alias("disposition"), F.col("entity_pos"), F.col("entity_ref"),
        _sub_ref(F.col("stu_seq"), F.col("stu_stop_id"), F.col("stu_pos"), None).alias("sub_ref"),
        F.lit(None).cast("string").alias("record_id"), F.col("route_id"),
        F.lit(None).cast("timestamp").alias("event_time"),
    )
    issues = (
        rej_issues.unionByName(dup_issues).unionByName(trip_issues).unionByName(pred_issues)
        .unionByName(no_event_stus)
    )

    # entity status: rejected / partially_valid (own warning or error) / fully_valid
    bad = issues.groupBy(F.col("entity_pos").alias("_b_pos")).agg(
        F.max(F.when(Q.is_warning_or_error(F.col("reason_code")), 1).otherwise(0)).alias("_bad")
    )
    ents_st = ents.join(bad, F.col("entity_pos") == F.col("_b_pos"), "left").withColumn(
        "entity_status",
        F.when(rejected_ent, F.lit("rejected"))
        .when(F.coalesce(F.col("_bad"), F.lit(0)) == 1, F.lit("partially_valid"))
        .otherwise(F.lit("fully_valid")),
    )
    status_by_pos = ents_st.select(F.col("entity_pos").alias("_s_pos"), "entity_status")
    issues_st = issues.join(status_by_pos, F.col("entity_pos") == F.col("_s_pos"), "left").drop("_s_pos")

    quality = Q.quality_rows(issues, run_id=run_id, feed_type=FEED_TYPE, snapshot_id=snapshot_id,
                             policy_version=pol.policy_version)
    entity_quality = Q.entity_quality_rows(issues_st, params=params, feed_type=FEED_TYPE)

    # ---------------------------------------------------------------- route metrics
    counts = Q.entity_status_counts(ents_st)
    partial = counts.select((F.col("rejected_entity_count") > 0).alias("is_partial"))
    scope = Q.scope_routes_frame(spark, run.route_ids)
    rk = K.route_kpi_metrics(trips, scope, late_threshold_seconds=pol.predicted_late_seconds)
    route_metrics = rk.crossJoin(partial).select(
        Q.str_lit(run_id).alias("run_id"),
        F.lit(FEED_TYPE).alias("feed_type"),
        Q.str_lit(snapshot_id).alias("snapshot_id"),
        F.col("route_id"),
        Q.ts_lit(params.metrics_as_of).alias("metrics_as_of"),
        feed_ts.alias("feed_timestamp"),
        F.col("is_partial"),
        Q.str_lit(pol.policy_version).alias("policy_version"),
        F.lit(int(pol.prediction_stale_seconds)).cast("int").alias("stale_threshold_seconds"),
        F.col("trip_instance_count"),
        F.col("eligible_trip_count"),
        F.col("predicted_late_trip_count"),
        F.col("predicted_late_rate"),
        F.col("mean_predicted_delay_seconds"),
        F.col("excluded_trip_counts"),
        F.col("unknown_uncertainty_count"),
        F.lit(int(pol.predicted_late_seconds)).cast("int").alias("late_threshold_seconds"),
        Q.str_lit(pol.target_policy).alias("target_policy"),
        F.col("kpi_flags").alias("quality_flags"),
    )

    # ---------------------------------------------------------------- stats / accounting
    disp = F.col("disposition")
    cand = evs.agg(
        F.count(F.lit(1)).alias("candidate_predictions"),
        F.coalesce(F.sum(F.when(disp == "retained", 1).otherwise(0)), F.lit(0)).alias("retained_predictions"),
        F.coalesce(F.sum(F.when(disp == "rejected", 1).otherwise(0)), F.lit(0)).alias("rejected_predictions"),
        F.coalesce(F.sum(F.when(disp == "not_applicable", 1).otherwise(0)), F.lit(0))
        .alias("not_applicable_predictions"),
        F.coalesce(F.sum(F.when(F.col("is_target"), 1).otherwise(0)), F.lit(0)).alias("target_predictions"),
    )
    tcounts = inst.agg(
        F.count(F.lit(1)).alias("trip_instances_total"),
        F.coalesce(F.sum(F.when(F.col("is_dup_entity"), 1).otherwise(0)), F.lit(0)).alias("duplicate_trip_updates"),
    )
    in_scope_routes = sorted(set(run.route_ids))
    scnt = trips.agg(
        F.count(F.lit(1)).alias("surviving_trip_instances"),
        F.coalesce(F.sum(F.when(F.col("route_id").isin(*in_scope_routes), 1).otherwise(0)), F.lit(0))
        .alias("in_scope_trip_instances"),
    ) if in_scope_routes else trips.agg(
        F.count(F.lit(1)).alias("surviving_trip_instances"), F.lit(0).cast("long").alias("in_scope_trip_instances"),
    )
    rcheck = rk.agg(
        F.coalesce(
            F.sum(F.when(F.col("trip_instance_count") != F.col("eligible_trip_count") + F.col("_excluded_total"), 1)
                  .otherwise(0)),
            F.lit(0),
        ).alias("_route_violations"),
        F.map_from_entries(F.array_sort(F.collect_list(F.struct(
            F.col("route_id").alias("k"),
            F.struct(
                F.col("trip_instance_count"), F.col("eligible_trip_count"), F.col("predicted_late_trip_count"),
                F.col("_excluded_total").alias("excluded_trip_count"), F.col("unknown_uncertainty_count"),
            ).alias("v"),
        )))).alias("_per_route"),
    )
    base = counts.crossJoin(cand).crossJoin(tcounts).crossJoin(scnt).crossJoin(rcheck)
    accounting = F.struct(
        F.lit(FEED_TYPE).alias("feed_type"),
        F.lit(BUILDER_VERSION).alias("builder"),
        # top-level ints (read by WP4 batch.accounting_metrics)
        F.col("candidate_predictions").alias("candidate_predictions"),
        F.col("retained_predictions").alias("retained_predictions"),
        F.col("rejected_predictions").alias("rejected_predictions"),
        F.col("not_applicable_predictions").alias("not_applicable_predictions"),
        F.col("duplicate_trip_updates").alias("duplicate_trip_updates"),
        F.struct(
            F.col("selected_entity_count").alias("selected"),
            F.col("fully_valid_entity_count").alias("fully_valid"),
            F.col("partially_valid_entity_count").alias("partially_valid"),
            F.col("rejected_entity_count").alias("rejected"),
        ).alias("entities"),
        F.struct(
            F.col("candidate_predictions").alias("candidate"),
            F.col("retained_predictions").alias("retained"),
            F.col("rejected_predictions").alias("rejected"),
            F.col("not_applicable_predictions").alias("not_applicable"),
            F.col("target_predictions").alias("targets"),
        ).alias("predictions"),
        F.struct(
            F.col("trip_instances_total").alias("valid_trip_update_entities"),
            F.col("duplicate_trip_updates").alias("duplicate_trip_updates"),
            F.col("surviving_trip_instances").alias("trip_instances"),
            F.col("in_scope_trip_instances").alias("in_scope_trip_instances"),
        ).alias("trip_instances"),
        F.col("_per_route").alias("per_route"),
        F.lit(pol.policy_version).alias("policy_version"),
        F.lit(pol.target_policy).alias("target_policy"),
        F.lit(pol.uncertainty_policy).alias("uncertainty_policy"),
        F.lit("entity|prediction|trip_instance").alias("units"),
    )
    stats = Q.build_stats(base, accounting=accounting, identities=[
        ("candidate_prediction_records == retained + rejected + not_applicable",
         F.col("candidate_predictions")
         == F.col("retained_predictions") + F.col("rejected_predictions") + F.col("not_applicable_predictions")),
        ("per route: trip_instance_count == eligible_trip_count + sum(excluded_trip_counts)",
         F.col("_route_violations") == 0),
    ])

    # ---------------------------------------------------------------- curated + DLQ
    curated_predictions = evs.select(
        Q.str_lit(run_id).alias("run_id"),
        Q.str_lit(snapshot_id).alias("snapshot_id"),
        F.lit(FEED_TYPE).alias("feed_type"),
        F.col("prediction_id"),
        F.col("entity_ref"), F.col("entity_pos"), F.col("stu_pos"),
        F.col("event_type"), F.col("trip_id"), F.col("route_id").cast("string").alias("route_id"),
        F.col("direction_id").cast("int").alias("direction_id"), F.col("start_date"), F.col("start_time"),
        F.col("service_date_text").alias("service_date"), F.col("trip_instance_key"),
        F.col("_out_stop_id").alias("stop_id"), F.col("sort_seq").cast("int").alias("stop_sequence"),
        F.timestamp_seconds(F.col("event_epoch")).alias("predicted_at"),
        F.col("time_origin"),
        F.timestamp_seconds(F.col("scheduled_epoch")).alias("scheduled_at"),
        F.col("delay_s").alias("predicted_delay_seconds"),
        F.col("uncertainty").cast("int").alias("uncertainty"),
        F.col("uc").alias("prediction_quality"),
        F.col("schedule_resolution"), F.col("resolution_method"),
        F.col("trip_schedule_relationship"), F.col("stu_rel_raw").alias("stop_schedule_relationship"),
        F.col("stop_resolved"), F.col("disposition"), F.col("is_target"), F.col("kpi_eligible"),
        F.when(F.col("is_target"), F.col("trip_kpi_reason")).alias("kpi_exclusion_reason"),
        F.col("quality_flags"),
        Q.ts_lit(params.metrics_as_of).alias("metrics_as_of"),
        feed_ts.alias("feed_timestamp"),
    )
    kpi_ok = F.col("kpi_reason").isNull()
    curated_trips = trips.select(
        Q.str_lit(run_id).alias("run_id"),
        Q.str_lit(snapshot_id).alias("snapshot_id"),
        F.lit(FEED_TYPE).alias("feed_type"),
        F.col("trip_instance_key"),
        F.col("trip_id"),
        F.col("route_id").cast("string").alias("route_id"),
        F.col("direction_id").cast("int").alias("direction_id"),
        F.col("start_date"), F.col("start_time"),
        F.col("trip_rel").alias("trip_relationship"),
        F.col("trip_schedule_relationship"),
        F.col("schedule_resolution"), F.col("resolution_method"), F.col("resolution_reason"),
        F.col("service_date_text").alias("service_date"),
        F.col("entity_ref"), F.col("vehicle_id"),
        F.col("_tg_prediction_id").alias("target_prediction_id"),
        F.col("_tg_stop_id").alias("target_stop_id"),
        F.col("_tg_seq").cast("int").alias("target_stop_sequence"),
        F.col("_tg_event_type").alias("target_event_type"),
        F.timestamp_seconds(F.col("_tg_epoch")).alias("predicted_at"),
        F.col("_tg_uc").alias("target_uncertainty_class"),
        F.col("_tg_delay").cast("int").alias("target_delay_seconds"),
        F.coalesce(F.col("_n_unres"), F.lit(0)).cast("int").alias("unresolved_future_stops"),
        F.col("kpi_reason"),
        kpi_ok.alias("kpi_eligible"),
        F.when(kpi_ok, F.col("_tg_delay") >= F.lit(int(pol.predicted_late_seconds))).alias("predicted_late"),
        F.when(F.col("route_id").isin(*in_scope_routes) if in_scope_routes else F.lit(False), F.lit(True))
        .otherwise(F.lit(False)).alias("in_scope"),
        Q.merge_flags(trip_codes, F.col("kpi_reason")).alias("quality_flags"),
        Q.ts_lit(params.metrics_as_of).alias("metrics_as_of"),
        feed_ts.alias("feed_timestamp"),
    )
    dlq = Q.entity_dlq_rows(
        ents.where(rejected_ent).select(
            "entity_ref", F.lit(None).cast("string").alias("sub_ref"), F.col("rejected_reason").alias("reason_code"),
            "entity_json", F.lit("rejected").alias("disposition"),
        ),
        params=params, run=run, feed_type=FEED_TYPE, stage=DLQ_STAGE, max_excerpt_chars=dlq_excerpt_max_chars,
    )

    return GenerationOutput(
        tables={
            "predictions_by_stop_snapshot": preds.select(*PREDICTION_COLUMNS),
            "route_metrics_by_snapshot": route_metrics.select(*TU_ROUTE_METRIC_COLUMNS),
            "quality_by_snapshot": quality.select(*Q.QUALITY_COLUMNS),
        },
        stats=stats,
        curated={"predictions": curated_predictions, "trip_instances": curated_trips,
                 "entity_quality": entity_quality},
        dlq=dlq,
        notes={"builder": BUILDER_VERSION, "feed_type": FEED_TYPE, "parent_dependent": False},
    )
