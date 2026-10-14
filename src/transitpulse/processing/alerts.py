"""Alerts generation builder (WP5): selector -> scope-route projection, active state, partial carry-over.

Rules (design C §7, design B §5, decisions D-BIZ-06):
  * Entity rejects: corrupt JSON / no ``alert`` -> INVALID_ENTITY (DLQ). ``alert_id`` = entity id (or
    ``h:<sha256[:16]>`` of the entity JSON when the id is missing). Duplicate ids keep the first entity.
  * A selector (informed_entity) matches scope route R when EVERY present field holds: agency_id ==
    R.agency; route_type == R.route_type; route_id == R; trip -> static route of trip_id (else trip.route_id)
    == R, trip.direction_id narrows; stop_id (parent stations expand to children) served by R in
    ``dim_route_stops`` (for the selector direction when given); direction_id narrows to (R, direction).
    The alert applies to R if ANY selector matches. Empty selector -> INVALID_SELECTOR; unknown stop_id, or
    unknown trip_id without route_id -> UNRESOLVED_ALERT_SELECTOR (ignored, never mapped at random);
    resolved selector matching no scope route -> NO_SCOPE_MATCH.
  * ``active_state_at_build`` at ``metrics_as_of`` mirrors ``contracts.policy.alert_active_state``
    (ALWAYS / ACTIVE / UPCOMING / EXPIRED); the API recomputes it at serving time.
  * Text: language ``en`` preferred, else the first translation. Present alerts are CONFIRMED and keep the
    parent's ``first_seen_snapshot_id``.
  * Partial snapshot (rejected entities > 0) with a usable parent: every parent row whose alert id is not
    present among the valid alerts is carried as UNCONFIRMED_INHERITED with
    ``inherited_from_snapshot_id = coalesce(parent.inherited_from_snapshot_id, parent.snapshot_id)``.
    A full snapshot carries nothing (absent alerts end; counted as ``closed_absent_from_full_snapshot``).
Curated outputs never depend on ``parent``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from transitpulse.contracts.enums import AlertConfirmation
from transitpulse.contracts.envelope import iso_utc
from transitpulse.processing import quality as Q
from transitpulse.processing import spark_ids as S
from transitpulse.processing.context import GenerationOutput
from transitpulse.processing.schemas import ALERT_ENTITY_SCHEMA, CORRUPT_COL, JSON_OPTIONS

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

    from transitpulse.processing.context import BuildParams, RunInfo
    from transitpulse.static.dims import Dims

FEED_TYPE = "alerts"
DLQ_STAGE = "alert_entity_validation"
BUILDER_VERSION = "wp5-alerts-v1"
CONFIRMED = str(AlertConfirmation.CONFIRMED)
UNCONFIRMED_INHERITED = str(AlertConfirmation.UNCONFIRMED_INHERITED)
MAX_CLOSED_SAMPLE = 20

Q.check_registered((
    "INVALID_ENTITY", "DUPLICATE_OBSERVATION", "UNRESOLVED_ALERT_SELECTOR", "INVALID_SELECTOR",
    "NO_SCOPE_MATCH", "PARENT_UNAVAILABLE",
))

ALERT_COLUMNS: tuple[str, ...] = (
    "run_id", "snapshot_id", "route_id", "alert_id", "cause", "effect", "severity_level", "header_text",
    "description_text", "url", "translations_json", "active_periods_json", "informed_entities_json",
    "match_kinds", "selector_resolution", "active_state_at_build", "confirmation_status",
    "inherited_from_snapshot_id", "first_seen_snapshot_id", "feed_timestamp", "quality_flags",
)

PARENT_ALERT_SPEC: dict[str, str] = {
    "snapshot_id": "string", "route_id": "string", "alert_id": "string", "cause": "string", "effect": "string",
    "severity_level": "string", "header_text": "string", "description_text": "string", "url": "string",
    "translations_json": "string", "active_periods_json": "string", "informed_entities_json": "string",
    "match_kinds": "array<string>", "selector_resolution": "string", "active_state_at_build": "string",
    "confirmation_status": "string", "inherited_from_snapshot_id": "string", "first_seen_snapshot_id": "string",
    "quality_flags": "array<string>",
}

PERIODS_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("start", T.LongType(), True),
    T.StructField("end", T.LongType(), True),
]))


def alert_active_state_col(periods: Column, clock_epoch: int) -> Column:
    """Spark mirror of ``contracts.policy.alert_active_state(active_periods, clock_epoch)``.

    ``periods``: array<struct<start: long, end: long>> (null start = -inf, null end = +inf).
    """
    c = F.lit(int(clock_epoch)).cast("long")
    active = F.exists(
        periods, lambda p: (p["start"].isNull() | (p["start"] <= c)) & (p["end"].isNull() | (c < p["end"]))
    )
    upcoming = F.exists(periods, lambda p: p["start"].isNotNull() & (p["start"] > c))
    no_periods = periods.isNull() | (F.size(periods) == 0)
    return (
        F.when(no_periods, F.lit("ALWAYS"))
        .when(F.coalesce(active, F.lit(False)), F.lit("ACTIVE"))
        .when(F.coalesce(upcoming, F.lit(False)), F.lit("UPCOMING"))
        .otherwise(F.lit("EXPIRED"))
    )


def pick_text_col(translated: Column) -> Column:
    """TranslatedString -> text: language 'en' preferred, else the first translation."""
    tr = translated["translation"]
    en = F.filter(tr, lambda t: F.lower(t["language"]) == F.lit("en"))
    return F.coalesce(
        F.when(F.size(en) > 0, en.getItem(0)["text"]),
        F.when(F.size(tr) > 0, tr.getItem(0)["text"]),
    )


def _contains(arr: Column, value: Column) -> Column:
    return F.coalesce(F.exists(arr, lambda x: x == value), F.lit(False))


def _parse_entities(snap: DataFrame) -> DataFrame:
    ex = snap.select(F.posexplode(F.col("entities")).alias("entity_pos", "entity_json"))
    ex = ex.withColumn("e", F.from_json(F.col("entity_json"), ALERT_ENTITY_SCHEMA, JSON_OPTIONS))
    e = F.col("e")
    raw_id = F.coalesce(e["id"], F.get_json_object(F.col("entity_json"), "$.id"))
    ents = ex.select(
        F.col("entity_pos").cast("int").alias("entity_pos"),
        F.col("entity_json"),
        (e.isNull() | e[CORRUPT_COL].isNotNull()).alias("corrupt"),
        e["alert"].isNull().alias("no_alert"),
        S.entity_ref_col(raw_id, F.col("entity_json")).alias("entity_ref"),
        e["alert"].alias("alert"),
    )
    return ents.withColumn("alert_id", F.col("entity_ref")).withColumn(
        "rejected_reason", F.when(F.col("corrupt") | F.col("no_alert"), F.lit("INVALID_ENTITY"))
    )


def build_alert_generation(
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
    clock = Q.epoch_seconds_floor(params.metrics_as_of)
    run_id, snapshot_id = params.run_id, params.snapshot_id
    scope_routes = sorted(set(run.route_ids))
    parent_unavailable = params.parent_snapshot_id is not None and not params.parent_available
    use_parent = parent is not None and params.parent_available
    feed_ts = Q.ts_lit(params.feed_timestamp)

    ents = _parse_entities(snap)
    rejected = F.col("rejected_reason").isNotNull()
    dup_w = Window.partitionBy("alert_id").orderBy(F.col("entity_pos").asc())
    valid = ents.where(~rejected).withColumn("_rn", F.row_number().over(dup_w))

    a = F.col("alert")
    alerts = valid.where(F.col("_rn") == 1).select(
        F.col("entity_pos"), F.col("entity_ref"), F.col("alert_id"),
        a["cause"].alias("cause"),
        a["effect"].alias("effect"),
        a["severity_level"].alias("severity_level"),
        pick_text_col(a["header_text"]).alias("header_text"),
        pick_text_col(a["description_text"]).alias("description_text"),
        pick_text_col(a["url"]).alias("url"),
        F.to_json(F.struct(
            a["header_text"].alias("header_text"),
            a["description_text"].alias("description_text"),
            a["url"].alias("url"),
            a["cause_detail"].alias("cause_detail"),
            a["effect_detail"].alias("effect_detail"),
        )).alias("translations_json"),
        F.coalesce(F.to_json(a["active_period"]), F.lit("[]")).alias("active_periods_json"),
        F.coalesce(F.to_json(a["informed_entity"]), F.lit("[]")).alias("informed_entities_json"),
        alert_active_state_col(a["active_period"], clock).alias("active_state_at_build"),
        a["informed_entity"].alias("_ies"),
    )

    # ---------------------------------------------------------------- selectors
    sels = alerts.select(
        "alert_id", "entity_pos", "entity_ref", F.posexplode_outer(F.col("_ies")).alias("sel_pos", "sel"),
    )
    sel = F.col("sel")
    sels = sels.select(
        "alert_id", "entity_pos", "entity_ref", "sel_pos",
        sel.isNull().alias("_sel_null"),
        Q.blank_to_null(sel["agency_id"]).alias("s_agency"),
        Q.blank_to_null(sel["route_id"]).alias("s_route"),
        sel["route_type"].alias("s_rtype"),
        sel["direction_id"].alias("s_dir"),
        Q.blank_to_null(sel["trip"]["trip_id"]).alias("s_trip_id"),
        Q.blank_to_null(sel["trip"]["route_id"]).alias("s_trip_route"),
        sel["trip"]["direction_id"].alias("s_trip_dir"),
        Q.blank_to_null(sel["stop_id"]).alias("s_stop"),
    )
    trips = dims.trips.select(
        F.col("trip_id").cast("string").alias("_st_trip"),
        F.col("route_id").cast("string").alias("_st_route"),
        F.col("direction_id").cast("int").alias("_st_dir"),
    ).dropDuplicates(["_st_trip"])
    stops_known = dims.stops.select(F.col("stop_id").cast("string").alias("_sk_stop")).distinct()
    sels = sels.join(F.broadcast(trips), F.col("s_trip_id") == F.col("_st_trip"), "left").join(
        F.broadcast(stops_known), F.col("s_stop") == F.col("_sk_stop"), "left"
    )
    has_trip = F.col("s_trip_id").isNotNull() | F.col("s_trip_route").isNotNull()
    empty = F.col("_sel_null") | (
        F.col("s_agency").isNull() & F.col("s_route").isNull() & F.col("s_rtype").isNull()
        & F.col("s_dir").isNull() & ~has_trip & F.col("s_stop").isNull()
    )
    unresolved = (F.col("s_stop").isNotNull() & F.col("_sk_stop").isNull()) | (
        F.col("s_trip_id").isNotNull() & F.col("_st_route").isNull() & F.col("s_trip_route").isNull()
    )
    sels = (
        sels.withColumn("has_trip", has_trip)
        .withColumn("trip_route", F.coalesce(F.col("_st_route"), F.col("s_trip_route")))
        .withColumn(
            "sel_status",
            F.when(empty, F.lit("invalid")).when(unresolved, F.lit("unresolved")).otherwise(F.lit("resolved")),
        )
    )

    # scope routes with agency, route_type and served directions
    routes = dims.routes.where(F.col("route_id").isin(*scope_routes) if scope_routes else F.lit(False)).select(
        F.col("route_id").cast("string").alias("R_route"),
        F.col("agency_id").cast("string").alias("R_agency"),
        F.col("route_type").cast("int").alias("R_rtype"),
    ).dropDuplicates(["R_route"])
    rdirs = dims.trips.groupBy(F.col("route_id").cast("string").alias("_rd_route")).agg(
        F.array_sort(F.collect_set(F.col("direction_id").cast("int"))).alias("R_dirs")
    )
    routes = routes.join(rdirs, F.col("R_route") == F.col("_rd_route"), "left").drop("_rd_route")
    rs = dims.route_stops.select(
        F.col("route_id").cast("string").alias("_rs_route"),
        F.col("direction_id").cast("int").alias("_rs_dir"),
        F.col("stop_id").cast("string").alias("_rs_stop"),
    )
    parents = dims.stops.where(F.length(F.trim(F.col("parent_station"))) > 0).select(
        F.col("stop_id").cast("string").alias("_c_stop"), F.col("parent_station").cast("string").alias("_parent")
    )
    rs_par = rs.join(parents, F.col("_rs_stop") == F.col("_c_stop"), "inner").select(
        "_rs_route", "_rs_dir", F.col("_parent").alias("_rs_stop")
    )
    rs_all = rs.unionByName(rs_par).groupBy("_rs_route", "_rs_stop").agg(
        F.array_sort(F.collect_set("_rs_dir")).alias("_rs_dirs")
    )

    res = (
        sels.where(F.col("sel_status") == "resolved")
        .crossJoin(F.broadcast(routes))
        .join(F.broadcast(rs_all),
              (F.col("R_route") == F.col("_rs_route")) & (F.col("s_stop") == F.col("_rs_stop")), "left")
    )
    s_dir, s_trip_dir = F.col("s_dir"), F.col("s_trip_dir")
    c_agency = F.col("s_agency").isNull() | (F.col("R_agency") == F.col("s_agency"))
    c_rtype = F.col("s_rtype").isNull() | (F.col("R_rtype") == F.col("s_rtype"))
    c_route = F.col("s_route").isNull() | (F.col("R_route") == F.col("s_route"))
    c_trip = ~F.col("has_trip") | (
        (F.col("trip_route") == F.col("R_route"))
        & (s_trip_dir.isNull() | F.coalesce(F.col("_st_dir") == s_trip_dir, _contains(F.col("R_dirs"), s_trip_dir)))
    )
    c_stop = F.col("s_stop").isNull() | (
        F.col("_rs_dirs").isNotNull() & (s_dir.isNull() | _contains(F.col("_rs_dirs"), s_dir))
    )
    c_dir = s_dir.isNull() | _contains(F.col("R_dirs"), s_dir)
    match = F.lit(True)
    for cond in (c_agency, c_rtype, c_route, c_trip, c_stop, c_dir):
        match = match & F.coalesce(cond, F.lit(False))
    kinds = Q.flags_array(
        F.when(F.col("s_agency").isNotNull(), F.lit("agency")),
        F.when(F.col("s_rtype").isNotNull(), F.lit("route_type")),
        F.when(F.col("s_route").isNotNull(), F.lit("route")),
        F.when(F.col("has_trip"), F.lit("trip")),
        F.when(F.col("s_stop").isNotNull(), F.lit("stop")),
        F.when(s_dir.isNotNull(), F.concat(F.lit("direction:"), s_dir.cast("string"))),
    )
    matched = res.where(match).withColumn("_kinds", kinds)
    pairs = matched.groupBy("alert_id", F.col("R_route").alias("route_id")).agg(
        F.array_sort(F.array_distinct(F.flatten(F.collect_list("_kinds")))).alias("match_kinds")
    )
    matched_sel = matched.select(F.col("alert_id").alias("_m_alert"), F.col("sel_pos").alias("_m_pos")).distinct()
    no_scope = sels.where(F.col("sel_status") == "resolved").join(
        matched_sel, (F.col("alert_id") == F.col("_m_alert")) & (F.col("sel_pos") == F.col("_m_pos")), "left_anti"
    )

    sel_code = (
        F.when(F.col("sel_status") == "unresolved", F.lit("UNRESOLVED_ALERT_SELECTOR"))
        .when(F.col("sel_status") == "invalid", F.lit("INVALID_SELECTOR"))
    )
    ssum = sels.groupBy(F.col("entity_pos").alias("_ss_pos")).agg(
        F.count(F.lit(1)).alias("_n_sel"),
        F.count(F.when(F.col("sel_status") != "resolved", 1)).alias("_n_bad"),
        F.array_sort(F.collect_set(sel_code)).alias("_sel_flags"),
    )
    selector_resolution = (
        F.when(F.col("_n_bad") == 0, F.lit("resolved"))
        .when(F.col("_n_bad") >= F.col("_n_sel"), F.lit("unresolved"))
        .otherwise(F.lit("partial"))
    )

    # ---------------------------------------------------------------- confirmed rows
    conf_base = (
        pairs.join(alerts.drop("_ies"), "alert_id", "inner")
        .join(ssum, F.col("entity_pos") == F.col("_ss_pos"), "left")
        .withColumn("selector_resolution", selector_resolution)
        .withColumn("quality_flags", F.coalesce(F.col("_sel_flags"), Q.empty_string_array()))
    )
    if use_parent:
        p = Q.ensure_columns(parent, PARENT_ALERT_SPEC)
        p_snap = F.coalesce(F.col("snapshot_id"), Q.str_lit(params.parent_snapshot_id))
        p_first = p.groupBy(F.col("alert_id").alias("_pf_alert")).agg(
            F.min(F.coalesce(F.col("first_seen_snapshot_id"), p_snap)).alias("_p_first")
        )
        conf = conf_base.join(p_first, F.col("alert_id") == F.col("_pf_alert"), "left").withColumn(
            "first_seen_snapshot_id", F.coalesce(F.col("_p_first"), Q.str_lit(snapshot_id))
        )
    else:
        conf = conf_base.withColumn("first_seen_snapshot_id", Q.str_lit(snapshot_id))
    confirmed_rows = conf.select(
        Q.str_lit(run_id).alias("run_id"),
        Q.str_lit(snapshot_id).alias("snapshot_id"),
        F.col("route_id"), F.col("alert_id"), F.col("cause"), F.col("effect"), F.col("severity_level"),
        F.col("header_text"), F.col("description_text"), F.col("url"), F.col("translations_json"),
        F.col("active_periods_json"), F.col("informed_entities_json"), F.col("match_kinds"),
        F.col("selector_resolution"), F.col("active_state_at_build"),
        F.lit(CONFIRMED).alias("confirmation_status"),
        F.lit(None).cast("string").alias("inherited_from_snapshot_id"),
        F.col("first_seen_snapshot_id"),
        feed_ts.alias("feed_timestamp"),
        F.col("quality_flags"),
    )

    # ---------------------------------------------------------------- partial carry-over / closures
    counts_pre = ents.agg(
        F.coalesce(F.sum(F.when(rejected, 1).otherwise(0)), F.lit(0)).alias("_n_rejected")
    ).select((F.col("_n_rejected") > 0).alias("_is_partial"))
    present = ents.where(~rejected).select(F.col("alert_id").alias("_pr_alert")).distinct()
    if use_parent:
        p_snap = F.coalesce(F.col("snapshot_id"), Q.str_lit(params.parent_snapshot_id))
        carried = (
            p.where(F.col("route_id").isin(*scope_routes) if scope_routes else F.lit(False))
            .join(present, F.col("alert_id") == F.col("_pr_alert"), "left_anti")
            .crossJoin(counts_pre)
            .where(F.col("_is_partial"))
        )
        inherited_rows = carried.select(
            Q.str_lit(run_id).alias("run_id"),
            Q.str_lit(snapshot_id).alias("snapshot_id"),
            F.col("route_id"), F.col("alert_id"), F.col("cause"), F.col("effect"), F.col("severity_level"),
            F.col("header_text"), F.col("description_text"), F.col("url"), F.col("translations_json"),
            F.col("active_periods_json"), F.col("informed_entities_json"),
            F.coalesce(F.col("match_kinds"), Q.empty_string_array()).alias("match_kinds"),
            F.col("selector_resolution"),
            alert_active_state_col(F.from_json(F.col("active_periods_json"), PERIODS_SCHEMA), clock)
            .alias("active_state_at_build"),
            F.lit(UNCONFIRMED_INHERITED).alias("confirmation_status"),
            F.coalesce(F.col("inherited_from_snapshot_id"), p_snap).alias("inherited_from_snapshot_id"),
            F.coalesce(F.col("first_seen_snapshot_id"), p_snap).alias("first_seen_snapshot_id"),
            feed_ts.alias("feed_timestamp"),
            F.coalesce(F.col("quality_flags"), Q.empty_string_array()).alias("quality_flags"),
        )
        closed = (
            p.select(F.col("alert_id").alias("_c_alert")).where(F.col("_c_alert").isNotNull()).distinct()
            .join(present, F.col("_c_alert") == F.col("_pr_alert"), "left_anti")
            .crossJoin(counts_pre)
            .where(~F.col("_is_partial"))
        )
        closed_stats = closed.agg(
            F.count(F.lit(1)).alias("_n_closed"),
            F.slice(F.array_sort(F.collect_list("_c_alert")), 1, MAX_CLOSED_SAMPLE).alias("_closed_sample"),
        )
        served = confirmed_rows.unionByName(inherited_rows)
    else:
        inherited_rows = None
        closed_stats = counts_pre.select(
            F.lit(0).cast("long").alias("_n_closed"), Q.empty_string_array().alias("_closed_sample")
        )
        served = confirmed_rows

    # ---------------------------------------------------------------- issues / quality
    issue_cols = ("reason_code", "ref", "disposition", "entity_pos", "entity_ref", "sub_ref", "record_id",
                  "route_id", "event_time")
    rej_issues = ents.where(rejected).select(
        F.col("rejected_reason").alias("reason_code"), F.col("entity_ref").alias("ref"),
        F.lit("rejected").alias("disposition"), F.col("entity_pos"), F.col("entity_ref"),
        F.lit(None).cast("string").alias("sub_ref"), F.lit(None).cast("string").alias("record_id"),
        F.lit(None).cast("string").alias("route_id"), F.lit(None).cast("timestamp").alias("event_time"),
    )
    dup_issues = valid.where(F.col("_rn") > 1).select(
        F.lit("DUPLICATE_OBSERVATION").alias("reason_code"), F.col("alert_id").alias("ref"),
        F.lit("duplicate").alias("disposition"), F.col("entity_pos"), F.col("entity_ref"),
        F.lit(None).cast("string").alias("sub_ref"), F.col("alert_id").alias("record_id"),
        F.lit(None).cast("string").alias("route_id"), F.lit(None).cast("timestamp").alias("event_time"),
    )
    sel_sub_ref = F.concat(F.lit("ie:"), F.coalesce(F.col("sel_pos").cast("string"), F.lit("none")))
    bad_sel_issues = sels.where(F.col("sel_status") != "resolved").select(
        sel_code.alias("reason_code"), F.col("alert_id").alias("ref"),
        F.lit(None).cast("string").alias("disposition"), F.col("entity_pos"), F.col("entity_ref"),
        sel_sub_ref.alias("sub_ref"), F.col("alert_id").alias("record_id"),
        F.lit(None).cast("string").alias("route_id"), F.lit(None).cast("timestamp").alias("event_time"),
    )
    no_scope_issues = no_scope.select(
        F.lit("NO_SCOPE_MATCH").alias("reason_code"), F.col("alert_id").alias("ref"),
        F.lit(None).cast("string").alias("disposition"), F.col("entity_pos"), F.col("entity_ref"),
        sel_sub_ref.alias("sub_ref"), F.col("alert_id").alias("record_id"),
        F.lit(None).cast("string").alias("route_id"), F.lit(None).cast("timestamp").alias("event_time"),
    )
    issues = (
        rej_issues.unionByName(dup_issues).unionByName(bad_sel_issues).unionByName(no_scope_issues)
        .select(*issue_cols)
    )
    q_frames = [issues]
    if parent_unavailable:
        q_frames.append(Q.snapshot_issue(spark, "PARENT_UNAVAILABLE", params.parent_snapshot_id))
    quality = Q.quality_rows(Q.union_issues(q_frames), run_id=run_id, feed_type=FEED_TYPE,
                             snapshot_id=snapshot_id, policy_version=pol.policy_version)

    bad = issues.groupBy(F.col("entity_pos").alias("_b_pos")).agg(
        F.max(F.when(Q.is_warning_or_error(F.col("reason_code")), 1).otherwise(0)).alias("_bad")
    )
    ents_st = ents.join(bad, F.col("entity_pos") == F.col("_b_pos"), "left").withColumn(
        "entity_status",
        F.when(rejected, F.lit("rejected"))
        .when(F.coalesce(F.col("_bad"), F.lit(0)) == 1, F.lit("partially_valid"))
        .otherwise(F.lit("fully_valid")),
    )
    status_by_pos = ents_st.select(F.col("entity_pos").alias("_s_pos"), "entity_status")
    entity_quality = Q.entity_quality_rows(
        issues.join(status_by_pos, F.col("entity_pos") == F.col("_s_pos"), "left").drop("_s_pos"),
        params=params, feed_type=FEED_TYPE,
    )

    # ---------------------------------------------------------------- stats / accounting
    counts = Q.entity_status_counts(ents_st)
    sstats = sels.agg(
        F.coalesce(F.sum(F.when(~F.col("_sel_null"), 1).otherwise(0)), F.lit(0)).alias("candidate_selectors"),
        F.coalesce(F.sum(F.when(~F.col("_sel_null") & (F.col("sel_status") == "unresolved"), 1).otherwise(0)),
                   F.lit(0)).alias("unresolved_selectors"),
        F.coalesce(F.sum(F.when(~F.col("_sel_null") & (F.col("sel_status") == "invalid"), 1).otherwise(0)),
                   F.lit(0)).alias("invalid_selectors"),
        F.coalesce(F.sum(F.when(F.col("_sel_null"), 1).otherwise(0)), F.lit(0)).alias("alerts_without_selectors"),
    )
    mstats = matched_sel.agg(F.count(F.lit(1)).alias("matched_selectors"))
    nstats = no_scope.agg(F.count(F.lit(1)).alias("no_scope_match_selectors"))
    astats = alerts.agg(F.count(F.lit(1)).alias("valid_alerts"))
    cstats = confirmed_rows.agg(F.count(F.lit(1)).alias("confirmed_rows"))
    if inherited_rows is not None:
        istats = inherited_rows.agg(F.count(F.lit(1)).alias("inherited_rows"))
    else:
        istats = counts_pre.select(F.lit(0).cast("long").alias("inherited_rows"))
    vstats = served.agg(F.count(F.lit(1)).alias("served_alert_rows"))
    base = (
        counts.crossJoin(sstats).crossJoin(mstats).crossJoin(nstats).crossJoin(astats).crossJoin(cstats)
        .crossJoin(istats).crossJoin(vstats).crossJoin(closed_stats)
    )
    accounting = F.struct(
        F.lit(FEED_TYPE).alias("feed_type"),
        F.lit(BUILDER_VERSION).alias("builder"),
        # top-level ints (read by WP4 batch.accounting_metrics)
        F.col("candidate_selectors").alias("candidate_selectors"),
        F.struct(
            F.col("selected_entity_count").alias("selected"),
            F.col("fully_valid_entity_count").alias("fully_valid"),
            F.col("partially_valid_entity_count").alias("partially_valid"),
            F.col("rejected_entity_count").alias("rejected"),
            F.col("valid_alerts").alias("distinct_valid_alerts"),
        ).alias("entities"),
        F.struct(
            F.col("candidate_selectors").alias("candidate"),
            F.col("matched_selectors").alias("matched"),
            F.col("no_scope_match_selectors").alias("no_scope_match"),
            F.col("unresolved_selectors").alias("unresolved"),
            F.col("invalid_selectors").alias("invalid"),
            F.col("alerts_without_selectors").alias("alerts_without_selectors"),
        ).alias("selectors"),
        F.struct(
            F.col("served_alert_rows").alias("served"),
            F.col("confirmed_rows").alias("confirmed"),
            F.col("inherited_rows").alias("inherited"),
            F.col("_n_closed").alias("closed_absent_from_full_snapshot"),
            F.col("_closed_sample").alias("closed_sample"),
            F.lit("ABSENT_FROM_FULL_SNAPSHOT").alias("closed_reason"),
            F.lit(iso_utc(params.feed_timestamp)).alias("closed_at"),
        ).alias("alert_route_rows"),
        F.lit(params.parent_snapshot_id).cast("string").alias("parent_snapshot_id"),
        F.lit(bool(use_parent)).alias("parent_used"),
        F.lit(pol.policy_version).alias("policy_version"),
        F.lit("entity|selector|alert_route_row").alias("units"),
    )
    stats = Q.build_stats(base, accounting=accounting, identities=[
        ("served_alerts == confirmed + inherited",
         F.col("served_alert_rows") == F.col("confirmed_rows") + F.col("inherited_rows")),
        ("candidate_selectors == matched + no_scope_match + unresolved + invalid",
         F.col("candidate_selectors") == F.col("matched_selectors") + F.col("no_scope_match_selectors")
         + F.col("unresolved_selectors") + F.col("invalid_selectors")),
    ])

    # ---------------------------------------------------------------- curated (parent independent) + DLQ
    curated_alerts = conf_base.select(
        Q.str_lit(run_id).alias("run_id"),
        Q.str_lit(snapshot_id).alias("snapshot_id"),
        F.lit(FEED_TYPE).alias("feed_type"),
        F.col("alert_id"), F.col("route_id"),
        feed_ts.alias("event_time"),
        Q.ts_lit(params.metrics_as_of).alias("metrics_as_of"),
        F.col("cause"), F.col("effect"), F.col("severity_level"), F.col("header_text"), F.col("description_text"),
        F.col("active_state_at_build"), F.col("selector_resolution"), F.col("match_kinds"),
        F.col("active_periods_json"), F.col("informed_entities_json"),
        F.lit(CONFIRMED).alias("confirmation_status"),
        F.col("quality_flags"),
        feed_ts.alias("feed_timestamp"),
    )
    dlq = Q.entity_dlq_rows(
        ents.where(rejected).select(
            "entity_ref", F.lit(None).cast("string").alias("sub_ref"), F.col("rejected_reason").alias("reason_code"),
            "entity_json", F.lit("rejected").alias("disposition"),
        ),
        params=params, run=run, feed_type=FEED_TYPE, stage=DLQ_STAGE, max_excerpt_chars=dlq_excerpt_max_chars,
    )

    return GenerationOutput(
        tables={
            "alerts_by_route_snapshot": served.select(*ALERT_COLUMNS),
            "quality_by_snapshot": quality.select(*Q.QUALITY_COLUMNS),
        },
        stats=stats,
        curated={"alerts": curated_alerts, "entity_quality": entity_quality},
        dlq=dlq,
        notes={"builder": BUILDER_VERSION, "feed_type": FEED_TYPE, "parent_used": bool(use_parent),
               "parent_unavailable": bool(parent_unavailable)},
    )
