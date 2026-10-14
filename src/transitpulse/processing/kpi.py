"""Predicted-delay KPI (design C §4-§5, decisions D-BIZ-01/D-BIZ-02, PDF p17-18).

* Uncertainty adapter ``mbta_bus_uncertainty_v1`` (mirror of ``contracts.policy.classify_uncertainty``):
  absent -> unknown_uncertainty (eligible, counted separately); 0 <= u < 300 -> realtime; 300 ->
  schedule_only; 301 -> degraded; > 301 or negative -> invalid. (Non-integer JSON values never reach this
  point: TU_ENTITY_SCHEMA declares ``uncertainty`` INT, so they make the entity INVALID_ENTITY.)
* Target policy ``first_future_resolved_stop_v1``: per trip instance, each stop-time update contributes ONE
  target event (ARRIVAL if it has a usable time, else DEPARTURE); candidates are events with time strictly
  after ``metrics_as_of``, stop relationship not SKIPPED/NO_DATA and a resolved stop; the target is the
  candidate with the smallest stop_sequence. Validity is evaluated on that target only.
* Exclusion reason = first match in ``contracts.enums.KPI_EXCLUSION_ORDER``.
* Route aggregation joins FROM the scope routes, so routes without trips get a row with zero counts and
  null rate/mean (a zero denominator never becomes 0 %).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from transitpulse.contracts.enums import KPI_EXCLUSION_ORDER, UncertaintyClass
from transitpulse.processing import quality as Q
from transitpulse.processing.schedule import ADDED_RELATIONSHIPS, CANCELED_RELATIONSHIPS

UC_REALTIME = str(UncertaintyClass.REALTIME)
UC_UNKNOWN = str(UncertaintyClass.UNKNOWN_UNCERTAINTY)
UC_SCHEDULE_ONLY = str(UncertaintyClass.SCHEDULE_ONLY)
UC_DEGRADED = str(UncertaintyClass.DEGRADED)
UC_INVALID = str(UncertaintyClass.INVALID)

# quality reason recorded for each uncertainty class (realtime has none)
UNCERTAINTY_REASON: dict[str, str] = {
    UC_UNKNOWN: "UNKNOWN_UNCERTAINTY",
    UC_SCHEDULE_ONLY: "SCHEDULE_ONLY_PREDICTION",
    UC_DEGRADED: "DEGRADED_PREDICTION",
    UC_INVALID: "INVALID_PREDICTION",
}
Q.check_registered(tuple(UNCERTAINTY_REASON.values()) + KPI_EXCLUSION_ORDER)

NON_TARGET_STOP_RELATIONSHIPS: tuple[str, ...] = ("SKIPPED", "NO_DATA")


def uncertainty_class_col(u: Column) -> Column:
    """Spark mirror of ``contracts.policy.classify_uncertainty`` (lower-case UncertaintyClass values)."""
    return (
        F.when(u.isNull(), F.lit(UC_UNKNOWN))
        .when(u < 0, F.lit(UC_INVALID))
        .when(u < 300, F.lit(UC_REALTIME))
        .when(u == 300, F.lit(UC_SCHEDULE_ONLY))
        .when(u == 301, F.lit(UC_DEGRADED))
        .otherwise(F.lit(UC_INVALID))
    )


def uncertainty_reason_col(uc: Column) -> Column:
    out = None
    for cls, code in UNCERTAINTY_REASON.items():
        out = F.when(uc == cls, F.lit(code)) if out is None else out.when(uc == cls, F.lit(code))
    return out


def select_targets(events: DataFrame, metrics_as_of_us: int) -> tuple[DataFrame, DataFrame]:
    """Pick the target event of every trip instance.

    ``events``: retained predictions of the surviving trip instances with columns ``_inst`` (instance key),
    ``stu_pos``, ``event_type``, ``event_epoch`` (long seconds), ``stu_rel`` (normalized), ``stop_resolved``,
    ``sort_seq`` (resolved stop_sequence), ``prediction_id``, ``uc``, ``delay_s``.

    Returns ``(targets, unresolved)``: ``targets`` has one row per instance that has a target
    (``_tg_inst, _tg_stu_pos, _tg_event_type, _tg_prediction_id, _tg_uc, _tg_delay, _tg_seq, _tg_stop_id,
    _tg_epoch``; ``_tg_stop_id`` is null when ``events`` has no ``stop_id`` column);
    ``unresolved`` has ``_u_inst, _n_unres`` = number of future target events whose stop is unresolved.
    """
    stop_id = F.col("stop_id") if "stop_id" in events.columns else F.lit(None).cast("string")
    per_stu = Window.partitionBy("_inst", "stu_pos").orderBy(
        F.when(F.col("event_type") == "arrival", 0).otherwise(1).asc()
    )
    tev = events.withColumn("_ev_rank", F.row_number().over(per_stu)).where(F.col("_ev_rank") == 1)
    future = tev.where(
        (F.col("event_epoch") * F.lit(1_000_000) > F.lit(int(metrics_as_of_us)))
        & ~F.col("stu_rel").isin(*NON_TARGET_STOP_RELATIONSHIPS)
    )
    unresolved = future.where(~F.col("stop_resolved")).groupBy(F.col("_inst").alias("_u_inst")).agg(
        F.count(F.lit(1)).alias("_n_unres")
    )
    first = Window.partitionBy("_inst").orderBy(F.col("sort_seq").asc_nulls_last(), F.col("stu_pos").asc())
    targets = (
        future.where(F.col("stop_resolved"))
        .withColumn("_t_rank", F.row_number().over(first))
        .where(F.col("_t_rank") == 1)
        .select(
            F.col("_inst").alias("_tg_inst"),
            F.col("stu_pos").alias("_tg_stu_pos"),
            F.col("event_type").alias("_tg_event_type"),
            F.col("prediction_id").alias("_tg_prediction_id"),
            F.col("uc").alias("_tg_uc"),
            F.col("delay_s").alias("_tg_delay"),
            F.col("sort_seq").alias("_tg_seq"),
            stop_id.alias("_tg_stop_id"),
            F.col("event_epoch").alias("_tg_epoch"),
        )
    )
    return targets, unresolved


def exclusion_reason_col(conditions: dict[str, Column]) -> Column:
    """First matching reason in KPI_EXCLUSION_ORDER (null = eligible)."""
    if set(conditions) != set(KPI_EXCLUSION_ORDER):
        raise ValueError(f"KPI conditions must cover exactly {KPI_EXCLUSION_ORDER}")
    out = None
    for code in KPI_EXCLUSION_ORDER:
        cond = F.coalesce(conditions[code], F.lit(False))
        out = F.when(cond, F.lit(code)) if out is None else out.when(cond, F.lit(code))
    return out


def trip_kpi(instances: DataFrame, targets: DataFrame, unresolved: DataFrame) -> DataFrame:
    """Attach target + ``kpi_reason`` to every surviving trip instance.

    ``instances`` needs ``_inst``, ``trip_rel``, ``schedule_resolution``. Adds the ``_tg_*`` columns,
    ``_n_unres`` and ``kpi_reason`` (null when eligible).
    """
    t = instances.join(targets, F.col("_inst") == F.col("_tg_inst"), "left").join(
        unresolved, F.col("_inst") == F.col("_u_inst"), "left"
    )
    rel = F.col("trip_rel")
    res = F.col("schedule_resolution")
    conds = {
        "CANCELED_TRIP": rel.isin(*CANCELED_RELATIONSHIPS),
        "ADDED_TRIP": rel.isin(*ADDED_RELATIONSHIPS),
        "AMBIGUOUS_SERVICE_DATE": res == "ambiguous",
        "UNRESOLVED_TRIP": res != "resolved",
        "UNRESOLVED_STOP_SEQUENCE": F.coalesce(F.col("_n_unres"), F.lit(0)) > 0,
        "NO_TARGET_STOP": F.col("_tg_inst").isNull(),
        "SCHEDULE_ONLY_PREDICTION": F.col("_tg_uc") == UC_SCHEDULE_ONLY,
        "DEGRADED_PREDICTION": F.col("_tg_uc") == UC_DEGRADED,
        "INVALID_PREDICTION": F.col("_tg_uc") == UC_INVALID,
        "NO_SCHEDULED_EVENT": F.col("_tg_delay").isNull(),
    }
    return t.withColumn("kpi_reason", exclusion_reason_col(conds)).drop("_u_inst")


def route_kpi_metrics(trips: DataFrame, scope_routes: DataFrame, *, late_threshold_seconds: int) -> DataFrame:
    """Per scope route: trip_instance_count, eligible/late counts, rate, mean, exclusions, unknown uncertainty.

    ``trips`` = output of :func:`trip_kpi` with ``route_id``. Returns one row per ``scope_routes`` row with
    ``route_id, trip_instance_count, eligible_trip_count, predicted_late_trip_count, predicted_late_rate,
    mean_predicted_delay_seconds, excluded_trip_counts, unknown_uncertainty_count, _excluded_total,
    kpi_flags`` (``_excluded_total`` is for the accounting identity only).
    """
    in_scope = trips.join(scope_routes.select("route_id"), "route_id", "inner")
    ok = F.col("kpi_reason").isNull()
    delay = F.col("_tg_delay")
    agg = in_scope.groupBy("route_id").agg(
        F.count(F.lit(1)).alias("_n"),
        F.count(F.when(ok, 1)).alias("_elig"),
        F.count(F.when(ok & (delay >= F.lit(int(late_threshold_seconds))), 1)).alias("_late"),
        F.avg(F.when(ok, delay)).alias("_mean"),
        F.count(F.when(ok & (F.col("_tg_uc") == UC_UNKNOWN), 1)).alias("_unk"),
    )
    excl = (
        in_scope.where(~ok)
        .groupBy("route_id", "kpi_reason")
        .agg(F.count(F.lit(1)).alias("_c"))
        .groupBy("route_id")
        .agg(
            Q.sorted_int_map(F.col("kpi_reason"), F.col("_c")).alias("_excl"),
            F.sum("_c").alias("_excl_total"),
            F.array_sort(F.collect_set("kpi_reason")).alias("_kpi_flags"),
        )
    )
    out = scope_routes.select("route_id").join(agg, "route_id", "left").join(excl, "route_id", "left")
    n = F.coalesce(F.col("_n"), F.lit(0))
    elig = F.coalesce(F.col("_elig"), F.lit(0))
    late = F.coalesce(F.col("_late"), F.lit(0))
    return out.select(
        F.col("route_id"),
        n.cast("int").alias("trip_instance_count"),
        elig.cast("int").alias("eligible_trip_count"),
        late.cast("int").alias("predicted_late_trip_count"),
        F.when(elig > 0, late.cast("double") / elig.cast("double")).alias("predicted_late_rate"),
        F.when(elig > 0, F.col("_mean").cast("double")).alias("mean_predicted_delay_seconds"),
        F.coalesce(F.col("_excl"), Q.empty_int_map()).alias("excluded_trip_counts"),
        F.coalesce(F.col("_unk"), F.lit(0)).cast("int").alias("unknown_uncertainty_count"),
        F.coalesce(F.col("_excl_total"), F.lit(0)).cast("long").alias("_excluded_total"),
        F.coalesce(F.col("_kpi_flags"), Q.empty_string_array()).alias("kpi_flags"),
    )
