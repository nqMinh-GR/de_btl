"""WP5 TripUpdates builder + schedule resolution against the golden fixture (design C §3-§5, §8).

Golden S1 (TU1 at 12:00:30Z): R_A eligible 2, late 1, rate 0.5, mean 210 s, unknown_uncertainty 2;
targets T1 seq3 delay 360, T2 seq2 delay 60; nine predictions. IT07 / IT08 per design C §8.
"""

from __future__ import annotations

import copy
from datetime import UTC, date, datetime

import pytest

from transitpulse.contracts import ids

S1 = datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC)
TU = "trip_updates"
ANCHOR_OCT5 = 1791172800  # 2026-10-05T04:00:00Z = UTC(local noon EDT) - 12 h


def _build(tp, label, clock=S1, feed=None):
    from transitpulse.processing.predictions import build_trip_update_generation

    if feed is None:
        snap, params = tp.snapshot(label, TU, metrics_as_of=clock)
    else:
        snap, params = tp.snapshot_from_feed(feed, TU, metrics_as_of=clock, label=label)
    out = build_trip_update_generation(tp.spark, snap, params, tp.run(clock=clock), tp.dims)
    return out, params


def _metrics(tp, out):
    return tp.by(out.tables["route_metrics_by_snapshot"], "route_id")


def _kpi(m):
    return (m["trip_instance_count"], m["eligible_trip_count"], m["predicted_late_trip_count"],
            m["predicted_late_rate"], m["mean_predicted_delay_seconds"], m["excluded_trip_counts"],
            m["unknown_uncertainty_count"])


# --------------------------------------------------------------------------------------------------
# golden S1
# --------------------------------------------------------------------------------------------------

def test_golden_tu1_kpi_and_predictions(tp):
    out, params = _build(tp, "TU1")
    m = _metrics(tp, out)
    assert set(m) == {"R_A", "R_B"}
    assert _kpi(m["R_A"]) == (2, 2, 1, 0.5, 210.0, {}, 2)
    assert _kpi(m["R_B"]) == (0, 0, 0, None, None, {}, 0)
    ra = m["R_A"]
    assert ra["feed_type"] == TU and ra["late_threshold_seconds"] == 300 and ra["stale_threshold_seconds"] == 90
    assert ra["target_policy"] == "first_future_resolved_stop_v1" and ra["policy_version"] == "policy-v1"
    assert ra["metrics_as_of"] == S1 and ra["feed_timestamp"] == datetime.fromtimestamp(1791201620, UTC)
    assert ra["is_partial"] is False

    preds = tp.by(out.tables["predictions_by_stop_snapshot"], "trip_id", "stop_sequence", "event_type")
    expected_delays = {
        ("T1", 3, "arrival"): 360, ("T1", 3, "departure"): 360, ("T1", 4, "arrival"): 360,
        ("T2", 1, "departure"): 120, ("T2", 2, "arrival"): 60, ("T2", 2, "departure"): 60,
        ("T2", 3, "arrival"): 60, ("T2", 3, "departure"): 60, ("T2", 4, "arrival"): 60,
    }
    assert {k: v["predicted_delay_seconds"] for k, v in preds.items()} == expected_delays
    targets = {k for k, v in preds.items() if v["is_target"]}
    assert targets == {("T1", 3, "arrival"), ("T2", 2, "arrival")}
    assert {k for k, v in preds.items() if v["kpi_eligible"]} == targets

    t1 = preds[("T1", 3, "arrival")]
    assert t1["scheduled_at"] == datetime(2026, 10, 5, 12, 5, 0, tzinfo=UTC)
    assert t1["predicted_at"] == datetime(2026, 10, 5, 12, 11, 0, tzinfo=UTC)
    assert t1["stop_id"] == "S3" and t1["route_id"] == "R_A" and t1["direction_id"] == 0
    assert t1["schedule_resolution"] == "resolved" and t1["resolution_method"] == "descriptor"
    assert t1["service_date"] == "2026-10-05" and t1["trip_instance_key"] == "T1|2026-10-05"
    assert t1["prediction_quality"] == "unknown_uncertainty" and t1["uncertainty"] is None
    assert t1["vehicle_id"] == "V1" and t1["stop_schedule_relationship"] == "SCHEDULED"
    assert "UNKNOWN_UNCERTAINTY" in t1["quality_flags"]
    assert t1["prediction_id"] == ids.prediction_id(
        trip_id="T1", start_date="20261005", start_time="07:55:00", route_id="R_A", direction_id=0,
        stop_sequence=3, stop_id="S3", event_type="arrival",
    )
    t2 = preds[("T2", 2, "arrival")]
    assert t2["scheduled_at"] == datetime(2026, 10, 5, 12, 5, 0, tzinfo=UTC)
    assert t2["predicted_at"] == datetime(2026, 10, 5, 12, 6, 0, tzinfo=UTC)

    assert tp.quality_counts(out.tables["quality_by_snapshot"]) == {"UNKNOWN_UNCERTAINTY": 9}
    st = tp.stats(out)
    assert st["accounting_ok"] is True, st["accounting_violations"]
    assert (st["selected_entity_count"], st["fully_valid_entity_count"], st["partially_valid_entity_count"],
            st["rejected_entity_count"]) == (2, 2, 0, 0)
    pa = st["accounting"]["predictions"]
    assert (pa["candidate"], pa["retained"], pa["rejected"], pa["not_applicable"], pa["targets"]) == (9, 9, 0, 0, 2)
    assert tp.rows(out.dlq) == []


def test_tu_output_columns_and_types(tp):
    from transitpulse.processing.context import STATS_COLUMNS
    from transitpulse.processing.predictions import PREDICTION_COLUMNS, TU_ROUTE_METRIC_COLUMNS

    out, _ = _build(tp, "TU1")
    assert tuple(out.tables["predictions_by_stop_snapshot"].columns) == PREDICTION_COLUMNS
    assert tuple(out.tables["route_metrics_by_snapshot"].columns) == TU_ROUTE_METRIC_COLUMNS
    assert tuple(out.stats.columns) == STATS_COLUMNS
    pt = dict(out.tables["predictions_by_stop_snapshot"].dtypes)
    assert pt["predicted_at"] == "timestamp" and pt["predicted_delay_seconds"] == "int"
    assert pt["stop_sequence"] == "int" and pt["uncertainty"] == "int" and pt["quality_flags"] == "array<string>"
    mt = dict(out.tables["route_metrics_by_snapshot"].dtypes)
    assert mt["excluded_trip_counts"] == "map<string,int>" and mt["predicted_late_rate"] == "double"
    assert mt["eligible_trip_count"] == "int" and mt["mean_predicted_delay_seconds"] == "double"


# --------------------------------------------------------------------------------------------------
# IT07 / IT08
# --------------------------------------------------------------------------------------------------

@pytest.mark.it_case("IT07")
def test_it07_ambiguous_service_date_and_repeated_stop(tp):
    out, _ = _build(tp, "IT07_TU")
    m = _metrics(tp, out)
    assert _kpi(m["R_A"]) == (1, 0, 0, None, None, {"AMBIGUOUS_SERVICE_DATE": 1}, 0)
    assert _kpi(m["R_B"]) == (2, 1, 0, 0.0, 240.0, {"UNRESOLVED_STOP_SEQUENCE": 1}, 1)

    preds = tp.by(out.tables["predictions_by_stop_snapshot"], "trip_id", "stop_id", "event_type")
    t4 = preds[("T4", "S1", "departure")]
    assert t4["schedule_resolution"] == "ambiguous" and t4["predicted_delay_seconds"] is None
    assert t4["trip_instance_key"] == "T4|?none" and t4["service_date"] is None
    assert "AMBIGUOUS_SERVICE_DATE" in t4["quality_flags"]
    s5 = preds[("T6", "S5", "arrival")]
    assert s5["stop_sequence"] is None and s5["predicted_delay_seconds"] is None
    assert "UNRESOLVED_STOP_SEQUENCE" in s5["quality_flags"]
    s7 = preds[("T6", "S7", "arrival")]
    assert s7["stop_sequence"] == 3 and s7["predicted_delay_seconds"] == 60
    assert s7["is_target"] is True and s7["kpi_eligible"] is False
    t5 = preds[("T5", "S6", "arrival")]
    assert t5["stop_sequence"] == 2 and t5["predicted_delay_seconds"] == 240 and t5["kpi_eligible"] is True

    q = tp.quality_counts(out.tables["quality_by_snapshot"])
    assert q["AMBIGUOUS_SERVICE_DATE"] == 1 and q["UNRESOLVED_STOP_SEQUENCE"] == 1
    assert q["UNKNOWN_UNCERTAINTY"] == 4
    st = tp.stats(out)
    assert st["accounting_ok"] is True, st["accounting_violations"]
    assert (st["fully_valid_entity_count"], st["partially_valid_entity_count"]) == (1, 2)


@pytest.mark.it_case("IT07")
def test_it07_t4_resolves_by_inference_late_evening(tp):
    """Unit vector of design C §8: the same T4 update at 2026-10-06T03:00Z -> service date 2026-10-05."""
    clock = datetime(2026, 10, 6, 3, 0, 0, tzinfo=UTC)
    out, _ = _build(tp, "IT07_TU", clock=clock)
    preds = tp.by(out.tables["predictions_by_stop_snapshot"], "trip_id", "stop_id", "event_type")
    t4 = preds[("T4", "S1", "departure")]
    assert t4["schedule_resolution"] == "resolved" and t4["resolution_method"] == "inferred"
    assert t4["service_date"] == "2026-10-05" and t4["trip_instance_key"] == "T4|2026-10-05"
    assert t4["scheduled_at"] == datetime(2026, 10, 5, 23, 50, 0, tzinfo=UTC)  # 19:50 EDT
    assert t4["predicted_delay_seconds"] == 0
    assert "SERVICE_DATE_INFERRED" in t4["quality_flags"]
    q = tp.quality_counts(out.tables["quality_by_snapshot"])
    assert q["SERVICE_DATE_INFERRED"] == 1
    assert _metrics(tp, out)["R_A"]["excluded_trip_counts"] == {"NO_TARGET_STOP": 1}  # its only stop is past


@pytest.mark.it_case("IT08")
def test_it08_uncertainty_cancel_added(tp):
    out, _ = _build(tp, "IT08_TU")
    m = _metrics(tp, out)
    assert _kpi(m["R_A"]) == (
        3, 0, 0, None, None, {"CANCELED_TRIP": 1, "DEGRADED_PREDICTION": 1, "SCHEDULE_ONLY_PREDICTION": 1}, 0,
    )
    assert _kpi(m["R_B"]) == (4, 2, 1, 0.5, 120.0, {"ADDED_TRIP": 1, "INVALID_PREDICTION": 1}, 1)

    preds = tp.by(out.tables["predictions_by_stop_snapshot"], "trip_id")
    assert set(preds) == {"T1", "T2", "T5", "T6", "T7", "ADDED_1"}  # T3 is CANCELED without stop updates
    t1 = preds["T1"]
    assert t1["prediction_quality"] == "degraded" and t1["uncertainty"] == 301
    assert t1["predicted_delay_seconds"] == 360 and t1["is_target"] is True and t1["kpi_eligible"] is False
    assert preds["T2"]["prediction_quality"] == "schedule_only" and preds["T2"]["predicted_delay_seconds"] is None
    t5 = preds["T5"]
    assert t5["prediction_quality"] == "invalid" and t5["predicted_delay_seconds"] is None
    assert t5["kpi_eligible"] is False  # stored for audit, never eligible
    t6 = preds["T6"]
    assert t6["predicted_delay_seconds"] == 300 and t6["kpi_eligible"] is True  # boundary: late (>= 300)
    assert t6["prediction_quality"] == "unknown_uncertainty"
    t7 = preds["T7"]
    assert t7["predicted_delay_seconds"] == -60 and t7["prediction_quality"] == "realtime" and t7["kpi_eligible"]
    added = preds["ADDED_1"]
    assert added["schedule_resolution"] == "not_applicable" and added["stop_id"] == "S5"
    assert added["predicted_delay_seconds"] is None and added["scheduled_at"] is None
    assert added["route_id"] == "R_B" and "ADDED_TRIP" in added["quality_flags"]

    q = tp.quality_counts(out.tables["quality_by_snapshot"])
    for code, n in {"CANCELED_TRIP": 1, "ADDED_TRIP": 1, "DEGRADED_PREDICTION": 1, "SCHEDULE_ONLY_PREDICTION": 1,
                    "INVALID_PREDICTION": 1, "UNKNOWN_UNCERTAINTY": 2}.items():
        assert q.get(code) == n, (code, q)
    st = tp.stats(out)
    assert st["accounting_ok"] is True, st["accounting_violations"]
    assert (st["selected_entity_count"], st["fully_valid_entity_count"], st["partially_valid_entity_count"],
            st["rejected_entity_count"]) == (7, 5, 2, 0)


def test_rejected_trip_updates_go_to_dlq_and_mark_partial(tp):
    feed, _ = tp.feed("TU1")
    feed = copy.deepcopy(feed)
    feed["entity"].append({"id": "tu-no-trip", "trip_update": {
        "trip": {"route_id": "R_A"},
        "stop_time_update": [{"stop_sequence": 1, "arrival": {"time": 1791202000}}]}})
    feed["entity"].append({"id": "tu-corrupt", "trip_update": {
        "trip": {"trip_id": "T3"}, "stop_time_update": [{"stop_sequence": "x"}]}})
    out, params = _build(tp, "TU1_rejects", feed=feed)
    st = tp.stats(out)
    assert st["is_partial"] is True and st["rejected_entity_count"] == 2 and st["accounting_ok"] is True
    pa = st["accounting"]["predictions"]
    assert (pa["candidate"], pa["retained"], pa["rejected"], pa["not_applicable"]) == (10, 9, 1, 0)
    dlq = tp.by(out.dlq, "entity_ref")
    assert {k: v["reason_code"] for k, v in dlq.items()} == {
        "tu-no-trip": "INVALID_PREDICTION", "tu-corrupt": "INVALID_ENTITY",
    }
    assert dlq["tu-no-trip"]["error_id"] == ids.error_id_entity(
        params.run_id, TU, params.snapshot_id, "tu-no-trip", None, "INVALID_PREDICTION")
    m = _metrics(tp, out)
    assert _kpi(m["R_A"]) == (2, 2, 1, 0.5, 210.0, {}, 2)
    assert m["R_A"]["is_partial"] is True


# --------------------------------------------------------------------------------------------------
# schedule resolution building blocks
# --------------------------------------------------------------------------------------------------

def test_resolve_trip_instances_rules(tp):
    from transitpulse.processing.schedule import resolve_trip_instances

    rows = [
        (1, "T1", "R_A", 0, "20261005", "07:55:00", "SCHEDULED"),
        (2, "T1", None, None, "20261012", None, "SCHEDULED"),       # WKDY removed by calendar_dates
        (3, "T1", None, None, "20261010", None, "SCHEDULED"),       # Saturday added by calendar_dates
        (4, "T1", None, None, "20261005", "08:00:00", "SCHEDULED"),
        (5, "TZZ", "R_A", None, "20261005", None, "SCHEDULED"),
        (6, "T1", "R_B", None, "20261005", None, "SCHEDULED"),
        (7, "T1", None, None, None, None, "SCHEDULED"),
        (8, "ADDED_9", "R_B", None, None, None, "ADDED"),
        (9, "T1", None, 1, "20261005", None, "SCHEDULED"),
        (10, "T1", None, None, "2026105", None, "SCHEDULED"),
        (11, "T1", None, None, None, "08:00:00", "SCHEDULED"),
        (12, "T3", None, None, "20261005", "08:10:00", "CANCELED"),
    ]
    inst = tp.spark.createDataFrame(
        rows, "_key int, trip_id string, d_route_id string, d_direction_id int, start_date string, "
              "start_time string, trip_rel string")
    res = tp.by(resolve_trip_instances(tp.spark, inst, tp.dims, metrics_as_of=S1,
                                       agency_timezone="America/New_York", window_seconds=43200), "_key")

    def r(k):
        x = res[k]
        return x["schedule_resolution"], x["resolution_method"], x["resolution_reason"]

    assert r(1) == ("resolved", "descriptor", None)
    assert res[1]["anchor_epoch"] == ANCHOR_OCT5 and res[1]["trip_instance_key"] == "T1|2026-10-05"
    assert res[1]["service_date"] == date(2026, 10, 5)
    assert r(2) == ("unresolved", None, "SERVICE_NOT_ACTIVE_ON_START_DATE")
    assert res[2]["trip_instance_key"] == "T1|?20261012" and res[2]["anchor_epoch"] is None
    assert r(3) == ("resolved", "descriptor", None)
    assert res[3]["anchor_epoch"] == int(datetime(2026, 10, 10, 4, 0, tzinfo=UTC).timestamp())
    assert r(4) == ("unresolved", None, "START_TIME_MISMATCH")
    assert r(5) == ("unresolved", None, "UNKNOWN_ROUTE_OR_TRIP") and res[5]["route_id"] == "R_A"
    assert r(6) == ("unresolved", None, "DESCRIPTOR_STATIC_MISMATCH") and res[6]["route_id"] == "R_A"
    assert r(7) == ("resolved", "inferred", "SERVICE_DATE_INFERRED")
    assert res[7]["service_date_text"] == "2026-10-05" and res[7]["trip_instance_key"] == "T1|2026-10-05"
    assert r(8) == ("not_applicable", None, None) and res[8]["route_id"] == "R_B"
    assert r(9) == ("unresolved", None, "DESCRIPTOR_STATIC_MISMATCH")
    assert r(10) == ("unresolved", None, "DESCRIPTOR_STATIC_MISMATCH")
    assert r(11) == ("unresolved", None, "START_TIME_MISMATCH")
    assert r(12) == ("resolved", "descriptor", None)


def test_resolve_stops_rules(tp):
    from transitpulse.processing.schedule import resolve_stops

    rows = [
        (1, "T6", None, "S5", True),    # S5 occurs twice in T6
        (2, "T6", None, "S7", True),
        (3, "T6", 2, "S6", True),
        (4, "T6", 2, "S7", True),
        (5, "T6", 9, None, True),
        (6, "T6", None, "S1", True),
        (7, "T6", None, None, True),
        (8, "T6", 2, "S6", False),
    ]
    stus = tp.spark.createDataFrame(rows, "k int, trip_id string, stu_seq int, stu_stop_id string, _stop_lookup boolean")
    res = tp.by(resolve_stops(stus, tp.dims), "k")

    def r(k):
        return res[k]["stop_resolved"], res[k]["stop_reason"], res[k]["resolved_seq"]

    assert r(1) == (False, "UNRESOLVED_STOP_SEQUENCE", None)
    assert r(2) == (True, None, 3) and res[2]["arr_secs"] == 8 * 3600 + 6 * 60
    assert r(3) == (True, None, 2) and res[3]["dep_secs"] == 8 * 3600 + 3 * 60
    assert r(4) == (False, "STOP_ID_SEQUENCE_MISMATCH", None)
    assert r(5) == (False, "UNKNOWN_STOP_FOR_TRIP", None)
    assert r(6) == (False, "UNKNOWN_STOP_FOR_TRIP", None)
    assert r(7) == (False, "UNRESOLVED_STOP_SEQUENCE", None)
    assert r(8)[:2] == (False, None)


def test_gtfs_time_and_date_helpers(spark):
    from pyspark.sql import functions as F

    from transitpulse.processing.schedule import gtfs_seconds_col, yyyymmdd_to_date_col

    times = [("25:10:00", 90600), ("08:05:00", 29100), (" 7:05:00 ", 25500), ("24:00:00", 86400),
             ("24:60:00", None), ("abc", None), ("", None), (None, None)]
    df = spark.createDataFrame([(t,) for t, _ in times], "t string")
    got = [r["s"] for r in df.select(gtfs_seconds_col(F.col("t")).alias("s")).collect()]
    assert got == [e for _, e in times]
    dates = [("20261005", date(2026, 10, 5)), ("20261332", None), ("2026-10-05", None), (None, None),
             ("20240229", date(2024, 2, 29))]
    df = spark.createDataFrame([(d,) for d, _ in dates], "d string")
    got = [r["x"] for r in df.select(yyyymmdd_to_date_col(F.col("d")).alias("x")).collect()]
    assert got == [e for _, e in dates]


# --------------------------------------------------------------------------------------------------
# exact match with the hand-written tests/fixtures/golden/expected.json (WP0b)
# --------------------------------------------------------------------------------------------------

def _num_eq(actual, expected) -> bool:
    if expected is None or actual is None:
        return actual is None and expected is None
    return abs(float(actual) - float(expected)) <= 1e-9


@pytest.mark.parametrize(("run", "step", "label"), [
    ("golden", "S1", "TU1"), ("IT07", "S1", "IT07_TU"), ("IT08", "S1", "IT08_TU"),
])
def test_trip_updates_match_expected_json(tp, run, step, label):
    exp = tp.expect(run, step)
    clock = tp.instant(exp["feeds"]["trip_updates"]["metrics_as_of"])
    out, _ = _build(tp, label, clock=clock)

    # prediction_metrics as of metrics_as_of ("stale" is recomputed at serving time, not stored)
    m = _metrics(tp, out)
    assert set(m) == set(exp["prediction_metrics"])
    for route, e in exp["prediction_metrics"].items():
        got = m[route]
        assert got["trip_instance_count"] == e["trip_instances"], route
        assert got["eligible_trip_count"] == e["eligible"], route
        assert got["predicted_late_trip_count"] == e["late"], route
        assert _num_eq(got["predicted_late_rate"], e["rate"]), (route, got["predicted_late_rate"])
        assert _num_eq(got["mean_predicted_delay_seconds"], e["mean"]), (route, got["mean_predicted_delay_seconds"])
        assert got["excluded_trip_counts"] == e["excluded"], route
        assert sum(got["excluded_trip_counts"].values()) == e["excluded_total"], route
        assert got["unknown_uncertainty_count"] == e["unknown_uncertainty"], route

    # targets: one entry per trip instance (curated trip_instances)
    trips = tp.by(out.curated["trip_instances"], "trip_id")
    assert set(trips) == set(exp["targets"])
    for tid, e in exp["targets"].items():
        t = trips[tid]
        got = {
            "route_id": t["route_id"], "trip_relationship": t["trip_relationship"],
            "schedule_resolution": t["schedule_resolution"], "resolution_method": t["resolution_method"],
            "service_date": t["service_date"], "stop_sequence": t["target_stop_sequence"],
            "stop_id": t["target_stop_id"], "event_type": t["target_event_type"],
            "delay_s": t["target_delay_seconds"], "uncertainty_class": t["target_uncertainty_class"],
            "kpi_reason": t["kpi_reason"], "late": t["predicted_late"],
        }
        assert got == e, tid

    # stored predictions
    resolved_by_id = {r["prediction_id"]: r["stop_resolved"]
                      for r in tp.rows(out.curated["predictions"].where("disposition = 'retained'"))}
    got_preds = []
    for p in tp.rows(out.tables["predictions_by_stop_snapshot"]):
        got_preds.append({
            "trip_id": p["trip_id"], "route_id": p["route_id"], "stop_id": p["stop_id"],
            "stop_sequence": p["stop_sequence"], "stop_resolved": resolved_by_id[p["prediction_id"]],
            "event_type": p["event_type"], "predicted_at": p["predicted_at"], "scheduled_at": p["scheduled_at"],
            "delay_s": p["predicted_delay_seconds"], "uncertainty": p["uncertainty"],
            "uncertainty_class": p["prediction_quality"], "schedule_resolution": p["schedule_resolution"],
        })
    want = []
    for e in exp["predictions"]:
        e = dict(e)
        e["predicted_at"] = tp.instant(e["predicted_at"])
        e["scheduled_at"] = tp.instant(e["scheduled_at"])
        want.append(e)

    def key(d):
        return (d["trip_id"], d["stop_id"], d["event_type"], d["predicted_at"])

    assert sorted(got_preds, key=key) == sorted(want, key=key)
    assert tp.stats(out)["accounting_ok"] is True


def _dims_with_schedule_valid(tp, overrides: dict):
    """Golden dims where dim_trips.schedule_valid is replaced for some trips (True/False/None)."""
    from pyspark.sql import functions as F

    from transitpulse.static.dims import Dims

    base = tp.dims
    col = F.col("schedule_valid")
    for trip_id, value in overrides.items():
        col = F.when(F.col("trip_id") == trip_id, F.lit(value).cast("boolean")).otherwise(col)
    trips = base.trips.withColumn("schedule_valid", col)
    return Dims(gtfs_version=base.gtfs_version, scope_id=base.scope_id, agency_timezone=base.agency_timezone,
                route_ids=base.route_ids, frames={**base.frames, "trips": trips})


def test_invalid_static_trip_schedule_is_unresolved_and_kpi_excluded(tp):
    """WP1 keeps defective trips with schedule_valid=false: unresolved/UNRESOLVED_TRIP, predictions visible
    without scheduled_at/delay, trip excluded from the KPI. A null schedule_valid counts as true."""
    from transitpulse.processing.predictions import build_trip_update_generation
    from transitpulse.processing.schedule import resolve_trip_instances

    dims = _dims_with_schedule_valid(tp, {"T1": False, "T2": None})

    snap, params = tp.snapshot("TU1", TU, metrics_as_of=S1)
    out = build_trip_update_generation(tp.spark, snap, params, tp.run(clock=S1), dims)
    m = _metrics(tp, out)
    # T1 excluded (UNRESOLVED_TRIP); T2 (null schedule_valid -> valid) still eligible with delay 60
    assert _kpi(m["R_A"]) == (2, 1, 0, 0.0, 60.0, {"UNRESOLVED_TRIP": 1}, 1)
    assert _kpi(m["R_B"]) == (0, 0, 0, None, None, {}, 0)

    preds = tp.by(out.tables["predictions_by_stop_snapshot"], "trip_id", "stop_sequence", "event_type")
    t1 = [v for k, v in preds.items() if k[0] == "T1"]
    assert len(t1) == 3  # still visible
    for p in t1:
        assert p["schedule_resolution"] == "unresolved" and p["resolution_method"] is None
        assert p["scheduled_at"] is None and p["predicted_delay_seconds"] is None
        assert p["kpi_eligible"] is False and p["trip_instance_key"] == "T1|?20261005"
        assert "UNRESOLVED_TRIP" in p["quality_flags"]
    assert preds[("T1", 3, "arrival")]["is_target"] is True  # target still reported, not eligible
    t2 = preds[("T2", 2, "arrival")]
    assert t2["schedule_resolution"] == "resolved" and t2["predicted_delay_seconds"] == 60 and t2["kpi_eligible"]

    trips = tp.by(out.curated["trip_instances"], "trip_id")
    assert trips["T1"]["kpi_reason"] == "UNRESOLVED_TRIP" and trips["T1"]["resolution_reason"] == "UNRESOLVED_TRIP"
    assert trips["T2"]["kpi_reason"] is None
    q = tp.quality_counts(out.tables["quality_by_snapshot"])
    assert q["UNRESOLVED_TRIP"] == 1
    st = tp.stats(out)
    assert st["accounting_ok"] is True, st["accounting_violations"]
    assert (st["fully_valid_entity_count"], st["partially_valid_entity_count"]) == (1, 1)

    # precedence: descriptor mismatch is checked first; inference never rescues an invalid trip
    rows = [(1, "T1", "R_B", None, "20261005", None, "SCHEDULED"),
            (2, "T1", None, None, None, None, "SCHEDULED"),
            (3, "T1", None, None, "20261005", None, "CANCELED"),
            (4, "T2", None, None, "20261005", None, "SCHEDULED")]
    inst = tp.spark.createDataFrame(
        rows, "_key int, trip_id string, d_route_id string, d_direction_id int, start_date string, "
              "start_time string, trip_rel string")
    res = tp.by(resolve_trip_instances(tp.spark, inst, dims, metrics_as_of=S1, agency_timezone="America/New_York",
                                       window_seconds=43200), "_key")
    assert (res[1]["schedule_resolution"], res[1]["resolution_reason"]) == ("unresolved", "DESCRIPTOR_STATIC_MISMATCH")
    assert (res[2]["schedule_resolution"], res[2]["resolution_reason"]) == ("unresolved", "UNRESOLVED_TRIP")
    assert res[2]["anchor_epoch"] is None and res[2]["service_date"] is None
    assert (res[3]["schedule_resolution"], res[3]["resolution_reason"]) == ("unresolved", "UNRESOLVED_TRIP")
    assert (res[4]["schedule_resolution"], res[4]["resolution_method"]) == ("resolved", "descriptor")
    assert res[4]["static_schedule_valid"] is True and res[1]["static_schedule_valid"] is False


def test_resolve_stops_parity_with_python_reference_incl_null_static_stop(tp):
    """Spark resolve_stops == static.schedule.resolve_stop, including static stop_times rows kept by WP1 with a
    null stop_id (T5 seq 2 nulled here): a source stop_id never matches a null static stop_id."""
    from pyspark.sql import functions as F

    from transitpulse.processing.schedule import resolve_stops
    from transitpulse.static.dims import Dims
    from transitpulse.static.schedule import resolve_stop

    base = tp.dims
    st = base.stop_times.withColumn(
        "stop_id",
        F.when((F.col("trip_id") == "T5") & (F.col("stop_sequence") == 2), F.lit(None).cast("string"))
        .otherwise(F.col("stop_id")),
    )
    dims = Dims(gtfs_version=base.gtfs_version, scope_id=base.scope_id, agency_timezone=base.agency_timezone,
                route_ids=base.route_ids, frames={**base.frames, "stop_times": st})
    trip_stops: dict[str, list[tuple[int, str | None]]] = {}
    for r in tp.rows(st.select("trip_id", "stop_sequence", "stop_id").orderBy("trip_id", "stop_sequence")):
        trip_stops.setdefault(r["trip_id"], []).append((r["stop_sequence"], r["stop_id"]))

    cases = [
        ("T5", 2, "S6"),     # static stop_id null -> STOP_ID_SEQUENCE_MISMATCH (the fixed case)
        ("T5", 2, None),     # static stop_id null, no source stop -> resolved, stop_id None
        ("T5", None, "S6"),  # S6 only existed at the nulled row -> UNKNOWN_STOP_FOR_TRIP
        ("T5", 1, "S5"), ("T5", 3, None), ("T5", 3, "S5"), ("T5", 9, "S6"), ("T5", None, None),
        ("T6", None, "S5"), ("T6", None, "S7"), ("T6", 4, "S5"), ("T6", 1, None), ("T6", 2, "S7"),
        ("T1", 9, None), ("T1", None, "S9"), ("T4", None, "S4"),
    ]
    rows = [(i, t, s, sid, True) for i, (t, s, sid) in enumerate(cases)]
    stus = tp.spark.createDataFrame(rows, "k int, trip_id string, stu_seq int, stu_stop_id string, _stop_lookup boolean")
    res = tp.by(resolve_stops(stus, dims), "k")
    for i, (trip, seq, sid) in enumerate(cases):
        ref = resolve_stop(stop_sequence=seq, stop_id=sid, trip_stops=trip_stops.get(trip, []))
        r = res[i]
        got = (r["stop_resolved"], r["resolved_seq"],
               r["stu_stop_id"] if r["stu_stop_id"] is not None else r["static_stop_id"], r["stop_reason"])
        assert got == (ref.resolved, ref.stop_sequence, ref.stop_id, ref.reason), (trip, seq, sid)
    assert res[0]["stop_reason"] == "STOP_ID_SEQUENCE_MISMATCH" and res[0]["stop_resolved"] is False


def test_resolution_vectors_from_expected_json(tp):
    from transitpulse.processing.schedule import resolve_trip_instances

    vectors = tp.expected()["static"]["resolution_vectors"]
    by_clock: dict[str, list] = {}
    for i, v in enumerate(vectors):
        by_clock.setdefault(v["clock"], []).append((i, v))
    for clock_s, items in by_clock.items():
        rows = []
        for i, v in items:
            t = v["trip"]
            rel = (t.get("schedule_relationship") or "SCHEDULED").upper()
            rows.append((i, t["trip_id"], t.get("route_id"), t.get("direction_id"), t.get("start_date"),
                         t.get("start_time"), rel))
        inst = tp.spark.createDataFrame(
            rows, "_key int, trip_id string, d_route_id string, d_direction_id int, start_date string, "
                  "start_time string, trip_rel string")
        res = tp.by(resolve_trip_instances(tp.spark, inst, tp.dims, metrics_as_of=tp.instant(clock_s),
                                           agency_timezone="America/New_York", window_seconds=43200), "_key")
        for i, v in items:
            r = res[i]
            got = {"schedule_resolution": r["schedule_resolution"], "resolution_method": r["resolution_method"],
                   "service_date": r["service_date_text"], "reason": r["resolution_reason"]}
            want = {k: v["expected"][k] for k in got}
            assert got == want, v["name"]


def test_stop_resolution_vectors_from_expected_json(tp):
    from transitpulse.processing.schedule import resolve_stops

    vectors = tp.expected()["static"]["stop_resolution_vectors"]
    rows = [(i, v["trip_id"], v["stop_sequence"], v["stop_id"], True) for i, v in enumerate(vectors)]
    stus = tp.spark.createDataFrame(rows, "k int, trip_id string, stu_seq int, stu_stop_id string, _stop_lookup boolean")
    res = tp.by(resolve_stops(stus, tp.dims), "k")
    for i, v in enumerate(vectors):
        r = res[i]
        got = {"resolved": r["stop_resolved"], "stop_sequence": r["resolved_seq"],
               "stop_id": r["stu_stop_id"] if r["stu_stop_id"] is not None else r["static_stop_id"],
               "reason": r["stop_reason"]}
        assert got == v["expected"], v
