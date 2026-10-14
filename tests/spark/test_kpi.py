"""Target-stop KPI building blocks (processing/kpi.py): uncertainty adapter parity with
contracts.policy.classify_uncertainty, KPI_EXCLUSION_ORDER precedence, first-future-resolved-stop target
selection, and the per-route aggregation (zero denominators -> null rate/mean, late boundary >= 300 s).
"""

from __future__ import annotations

import pytest

from transitpulse.contracts.enums import KPI_EXCLUSION_ORDER
from transitpulse.contracts.policy import classify_uncertainty

ASOF = 1791201630  # 2026-10-05T12:00:30Z


def test_uncertainty_adapter_matches_policy(spark):
    from pyspark.sql import functions as F

    from transitpulse.processing.kpi import uncertainty_class_col

    values = [None, -2_147_483_648, -1, 0, 1, 60, 299, 300, 301, 302, 360, 2_147_483_647]
    df = spark.createDataFrame([(v,) for v in values], "u int")
    got = {r["u"]: r["c"] for r in df.select("u", uncertainty_class_col(F.col("u")).alias("c")).collect()}
    for v in values:
        assert got[v] == str(classify_uncertainty(v)), v


def test_exclusion_reason_precedence(spark):
    from pyspark.sql import functions as F

    from transitpulse.processing.kpi import exclusion_reason_col

    cols = [f"c{i}" for i in range(len(KPI_EXCLUSION_ORDER))]
    idx = {code: i for i, code in enumerate(KPI_EXCLUSION_ORDER)}

    def row(*codes, null_for=()):
        vals = [code in codes for code in KPI_EXCLUSION_ORDER]
        for code in null_for:
            vals[idx[code]] = None
        return tuple(vals)

    cases = [
        (row(), None),
        (row("CANCELED_TRIP", "AMBIGUOUS_SERVICE_DATE", "NO_TARGET_STOP"), "CANCELED_TRIP"),
        (row("ADDED_TRIP", "UNRESOLVED_TRIP"), "ADDED_TRIP"),
        (row("AMBIGUOUS_SERVICE_DATE", "UNRESOLVED_TRIP"), "AMBIGUOUS_SERVICE_DATE"),
        (row("UNRESOLVED_STOP_SEQUENCE", "NO_TARGET_STOP"), "UNRESOLVED_STOP_SEQUENCE"),
        (row("DEGRADED_PREDICTION", "INVALID_PREDICTION", "NO_SCHEDULED_EVENT"), "DEGRADED_PREDICTION"),
        (row("SCHEDULE_ONLY_PREDICTION", "NO_SCHEDULED_EVENT"), "SCHEDULE_ONLY_PREDICTION"),
        (row("NO_SCHEDULED_EVENT"), "NO_SCHEDULED_EVENT"),
        (row("NO_SCHEDULED_EVENT", null_for=("CANCELED_TRIP",)), "NO_SCHEDULED_EVENT"),
    ]
    df = spark.createDataFrame([c for c, _ in cases], ", ".join(f"{c} boolean" for c in cols))
    conds = {code: F.col(cols[i]) for i, code in enumerate(KPI_EXCLUSION_ORDER)}
    got = [r["reason"] for r in df.select(exclusion_reason_col(conds).alias("reason")).collect()]
    assert got == [e for _, e in cases]


def test_exclusion_reason_requires_every_code():
    pytest.importorskip("pyspark")
    from transitpulse.processing.kpi import exclusion_reason_col

    with pytest.raises(ValueError):
        exclusion_reason_col({"CANCELED_TRIP": None})


def test_target_is_first_future_resolved_stop(spark):
    from transitpulse.processing.kpi import select_targets

    schema = ("_inst string, stu_pos int, event_type string, event_epoch long, stu_rel string, "
              "stop_resolved boolean, sort_seq int, prediction_id string, uc string, delay_s int")
    rows = [
        # a: past departure-only first stop, then a future stop with arrival+departure -> arrival of seq 2
        ("a", 0, "departure", ASOF - 30, "SCHEDULED", True, 1, "a0d", "unknown_uncertainty", 120),
        ("a", 1, "departure", ASOF + 400, "SCHEDULED", True, 2, "a1d", "unknown_uncertainty", 70),
        ("a", 1, "arrival", ASOF + 300, "SCHEDULED", True, 2, "a1a", "unknown_uncertainty", 60),
        ("a", 2, "arrival", ASOF + 600, "SCHEDULED", True, 3, "a2a", "unknown_uncertainty", 60),
        # b: SKIPPED stop is never a target; departure-only stop is
        ("b", 0, "arrival", ASOF + 100, "SKIPPED", True, 1, "b0a", "realtime", 10),
        ("b", 1, "departure", ASOF + 200, "SCHEDULED", True, 2, "b1d", "degraded", 20),
        # c: an unresolved future stop is counted; the resolved one is still the target candidate
        ("c", 0, "arrival", ASOF + 100, "SCHEDULED", False, None, "c0a", "unknown_uncertainty", None),
        ("c", 1, "arrival", ASOF + 200, "SCHEDULED", True, 5, "c1a", "unknown_uncertainty", 40),
        # d: everything in the past
        ("d", 0, "arrival", ASOF - 1, "SCHEDULED", True, 1, "d0a", "realtime", 5),
        # e: arrival exactly at metrics_as_of is not "future" (strict >); arrival wins over departure
        ("e", 0, "arrival", ASOF, "SCHEDULED", True, 1, "e0a", "realtime", 5),
        ("e", 0, "departure", ASOF + 30, "SCHEDULED", True, 1, "e0d", "realtime", 5),
        # f: NO_DATA relationship is never a target
        ("f", 0, "arrival", ASOF + 100, "NO_DATA", True, 1, "f0a", "realtime", 5),
    ]
    events = spark.createDataFrame(rows, schema)
    targets, unresolved = select_targets(events, ASOF * 1_000_000)
    t = {r["_tg_inst"]: r for r in targets.collect()}
    assert set(t) == {"a", "b", "c"}
    assert (t["a"]["_tg_prediction_id"], t["a"]["_tg_seq"], t["a"]["_tg_delay"]) == ("a1a", 2, 60)
    assert (t["b"]["_tg_prediction_id"], t["b"]["_tg_event_type"], t["b"]["_tg_uc"]) == ("b1d", "departure", "degraded")
    assert t["c"]["_tg_prediction_id"] == "c1a"
    u = {r["_u_inst"]: r["_n_unres"] for r in unresolved.collect()}
    assert u == {"c": 1}


def test_trip_kpi_and_route_metrics(spark):
    from transitpulse.processing.kpi import route_kpi_metrics, trip_kpi

    inst = spark.createDataFrame(
        [("i1", "R_A", "SCHEDULED", "resolved"), ("i2", "R_A", "SCHEDULED", "resolved"),
         ("i3", "R_A", "CANCELED", "resolved"), ("i4", "R_B", "ADDED", "not_applicable"),
         ("i5", "R_B", "SCHEDULED", "resolved"), ("i6", "R_Z", "SCHEDULED", "resolved"),
         ("i7", "R_A", "SCHEDULED", "unresolved")],
        "_inst string, route_id string, trip_rel string, schedule_resolution string",
    )
    targets = spark.createDataFrame(
        [("i1", 0, "arrival", "p1", "unknown_uncertainty", 300, 2),
         ("i2", 0, "arrival", "p2", "realtime", 299, 2),
         ("i5", 0, "arrival", "p5", "realtime", None, 4),
         ("i6", 0, "arrival", "p6", "realtime", 999, 1)],
        "_tg_inst string, _tg_stu_pos int, _tg_event_type string, _tg_prediction_id string, _tg_uc string, "
        "_tg_delay int, _tg_seq int",
    )
    unresolved = spark.createDataFrame([], "_u_inst string, _n_unres long")
    trips = trip_kpi(inst, targets, unresolved)
    reasons = {r["_inst"]: r["kpi_reason"] for r in trips.select("_inst", "kpi_reason").collect()}
    assert reasons == {"i1": None, "i2": None, "i3": "CANCELED_TRIP", "i4": "ADDED_TRIP",
                       "i5": "NO_SCHEDULED_EVENT", "i6": None, "i7": "UNRESOLVED_TRIP"}

    scope = spark.createDataFrame([("R_A",), ("R_B",), ("R_C",)], "route_id string")
    m = {r["route_id"]: r for r in route_kpi_metrics(trips, scope, late_threshold_seconds=300).collect()}
    assert set(m) == {"R_A", "R_B", "R_C"}  # out-of-scope R_Z ignored, zero-trip R_C present
    a = m["R_A"]
    assert (a["trip_instance_count"], a["eligible_trip_count"], a["predicted_late_trip_count"]) == (4, 2, 1)
    assert a["predicted_late_rate"] == 0.5 and a["mean_predicted_delay_seconds"] == 299.5
    assert a["excluded_trip_counts"] == {"CANCELED_TRIP": 1, "UNRESOLVED_TRIP": 1}
    assert a["unknown_uncertainty_count"] == 1 and a["_excluded_total"] == 2
    b = m["R_B"]
    assert (b["trip_instance_count"], b["eligible_trip_count"], b["predicted_late_rate"],
            b["mean_predicted_delay_seconds"]) == (2, 0, None, None)
    assert b["excluded_trip_counts"] == {"ADDED_TRIP": 1, "NO_SCHEDULED_EVENT": 1}
    c = m["R_C"]
    assert (c["trip_instance_count"], c["eligible_trip_count"], c["predicted_late_trip_count"],
            c["predicted_late_rate"], c["mean_predicted_delay_seconds"], c["excluded_trip_counts"],
            c["unknown_uncertainty_count"]) == (0, 0, 0, None, None, {}, 0)
    for r in m.values():
        assert r["trip_instance_count"] == r["eligible_trip_count"] + r["_excluded_total"]
    types = dict(route_kpi_metrics(trips, scope, late_threshold_seconds=300).dtypes)
    assert types["excluded_trip_counts"] == "map<string,int>" and types["predicted_late_rate"] == "double"
