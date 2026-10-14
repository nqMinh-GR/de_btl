"""WP5 vehicle builder against the golden fixture (design C §6/§8, PDF p17-18) and the policy reference.

Golden S1 (clock 2026-10-05T12:00:30Z, VP1): R_A fresh 1 (V1 age 20), stale 1 (V2 age 210), unknown 1 (V3),
invalid 0, membership 3; R_B all zero. IT06: fresh 2, stale 0, unknown 1, invalid 5, membership 7, partial.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from transitpulse.contracts import ids
from transitpulse.contracts.policy import vehicle_freshness

S1 = datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC)
S2 = datetime(2026, 10, 5, 12, 1, 0, tzinfo=UTC)
VP = "vehicle_positions"


def _build(tp, label, clock, parent=None, **kw):
    from transitpulse.processing.vehicles import build_vehicle_generation

    snap, params = tp.snapshot(label, VP, metrics_as_of=clock, **kw)
    out = build_vehicle_generation(tp.spark, snap, params, tp.run(clock=clock), tp.dims, parent)
    return out, params


# --------------------------------------------------------------------------------------------------
# freshness parity with contracts.policy.vehicle_freshness
# --------------------------------------------------------------------------------------------------

def test_freshness_matches_policy_reference(spark, tp):
    from pyspark.sql import functions as F

    from transitpulse.processing.quality import epoch_micros
    from transitpulse.processing.vehicles import vehicle_freshness_cols

    policy = tp.policy()
    clocks = [S1, S1 + timedelta(microseconds=250_000), S1 + timedelta(seconds=160)]
    offsets = [None, -100_000, -121, -120, -119, -60, -1, 0, 1, 4, 5, 6, 10, 3600]
    for clock in clocks:
        base = int(clock.timestamp())
        rows = []
        for valid in (True, False, None):
            for off in offsets:
                rows.append((valid, None if off is None else base + off))
        df = spark.createDataFrame(rows, "valid boolean, obs long")
        fresh, age, skew = vehicle_freshness_cols(
            position_valid=F.col("valid"), observation_epoch_s=F.col("obs"),
            metrics_as_of_us=epoch_micros(clock), policy=policy,
        )
        got = df.select("valid", "obs", fresh.alias("f"), age.alias("a"), skew.alias("s")).collect()
        assert len(got) == len(rows)
        for r in got:
            obs_dt = None if r["obs"] is None else datetime.fromtimestamp(r["obs"], UTC)
            ref = vehicle_freshness(position_valid=bool(r["valid"]), observation_time=obs_dt,
                                    application_clock=clock, policy=policy)
            assert r["f"] == str(ref.freshness), (clock, r)
            assert r["a"] == ref.age_seconds, (clock, r)
            assert r["s"] == ("CLOCK_SKEW" in ref.flags), (clock, r)


# --------------------------------------------------------------------------------------------------
# golden S1 / S2
# --------------------------------------------------------------------------------------------------

def test_golden_s1_vehicle_metrics(tp):
    out, params = _build(tp, "VP1", S1)
    metrics = tp.by(out.tables["route_metrics_by_snapshot"], "route_id")
    assert set(metrics) == {"R_A", "R_B"}
    ra, rb = metrics["R_A"], metrics["R_B"]
    assert (ra["fresh_vehicle_count"], ra["stale_vehicle_count"], ra["unknown_age_vehicle_count"],
            ra["invalid_vehicle_count"], ra["snapshot_membership_count"]) == (1, 1, 1, 0, 3)
    assert (rb["fresh_vehicle_count"], rb["stale_vehicle_count"], rb["unknown_age_vehicle_count"],
            rb["invalid_vehicle_count"], rb["snapshot_membership_count"]) == (0, 0, 0, 0, 0)
    assert ra["feed_type"] == VP and ra["snapshot_id"] == params.snapshot_id
    assert ra["metrics_as_of"] == S1 and ra["stale_threshold_seconds"] == 120 and ra["is_partial"] is False
    assert ra["inherited_vehicle_count"] == 0 and ra["policy_version"] == "policy-v1"

    obs = tp.by(out.curated["observations"], "vehicle_id")
    assert obs["V1"]["freshness_at_build"] == "fresh" and obs["V1"]["age_seconds_at_build"] == 20.0
    assert obs["V2"]["freshness_at_build"] == "stale" and obs["V2"]["age_seconds_at_build"] == 210.0
    assert obs["V3"]["freshness_at_build"] == "unknown" and obs["V3"]["age_seconds_at_build"] is None

    veh = tp.by(out.tables["vehicles_by_route_snapshot"], "vehicle_id")
    assert set(veh) == {"V1", "V2", "V3"}
    v1, v3 = veh["V1"], veh["V3"]
    assert v1["route_id"] == "R_A" and v1["record_kind"] == "observation"
    assert v1["observation_time"] == datetime.fromtimestamp(1791201610, UTC)
    assert v1["event_time_origin"] == "vehicle_timestamp" and v1["freshness_known"] is True
    assert v1["position_valid"] is True and v1["invalid_reason"] is None and v1["is_inherited"] is False
    assert v1["observation_id"] == ids.observation_id(
        vehicle_id="V1", trip_id="T1", route_id="R_A", direction_id=0, start_date="20261005", start_time="07:55:00",
        trip_schedule_relationship="SCHEDULED", vehicle_timestamp=1791201610, latitude=42.3515, longitude=-71.0585,
        bearing=None, speed=None, stop_id="S3", current_stop_sequence=3, current_status="IN_TRANSIT_TO",
    )
    assert v3["record_kind"] == "snapshot_state_sample" and v3["observation_time"] is None
    assert v3["event_time"] == datetime.fromtimestamp(1791201620, UTC)
    assert v3["event_time_origin"] == "feed_header" and v3["freshness_known"] is False
    assert v3["observation_id"].startswith("ss_")
    assert "MISSING_OBSERVATION_TIMESTAMP" in v3["quality_flags"]
    assert v1["feed_timestamp"] == datetime.fromtimestamp(1791201620, UTC)

    hist = tp.rows(out.tables["vehicle_observations_by_day"])
    assert sorted(h["vehicle_id"] for h in hist) == ["V1", "V2", "V3"]
    assert all(str(h["utc_date"]) == "2026-10-05" for h in hist)

    q = tp.quality_counts(out.tables["quality_by_snapshot"])
    assert q == {"MISSING_OBSERVATION_TIMESTAMP": 1}

    st = tp.stats(out)
    assert st["accounting_ok"] is True, st["accounting_violations"]
    assert (st["selected_entity_count"], st["fully_valid_entity_count"], st["partially_valid_entity_count"],
            st["rejected_entity_count"]) == (3, 2, 1, 0)
    assert st["is_partial"] is False and st["is_empty"] is False
    obs_acc = st["accounting"]["observations"]
    assert (obs_acc["valid_candidate"], obs_acc["unique"], obs_acc["duplicate"]) == (3, 3, 0)
    assert tp.rows(out.dlq) == []


def test_golden_s2_vp2_with_parent_vp1(tp):
    out1, p1 = _build(tp, "VP1", S1)
    out2, _ = _build(tp, "VP2", S2, parent=tp.materialize(out1.tables["vehicles_by_route_snapshot"]),
                     parent_snapshot_id=p1.snapshot_id)
    ra = tp.by(out2.tables["route_metrics_by_snapshot"], "route_id")["R_A"]
    assert (ra["fresh_vehicle_count"], ra["stale_vehicle_count"], ra["unknown_age_vehicle_count"],
            ra["invalid_vehicle_count"], ra["snapshot_membership_count"]) == (1, 0, 1, 0, 2)
    veh = tp.by(out2.tables["vehicles_by_route_snapshot"], "vehicle_id")
    assert set(veh) == {"V1", "V3"}  # V2 absent: never carried over
    assert (veh["V1"]["latitude"], veh["V1"]["longitude"]) == (42.3519, -71.0581)
    assert veh["V1"]["is_inherited"] is False
    obs = tp.by(out2.curated["observations"], "vehicle_id")
    assert obs["V1"]["age_seconds_at_build"] == 5.0
    hist = tp.rows(out2.tables["vehicle_observations_by_day"])
    assert sorted(h["vehicle_id"] for h in hist) == ["V1", "V3"]
    st = tp.stats(out2)
    assert st["accounting_ok"] is True
    assert (st["accounting"]["observations"]["unique"], st["accounting"]["observations"]["duplicate"]) == (2, 0)


def test_repoll_of_same_observations_creates_no_history(tp):
    out1, p1 = _build(tp, "VP1", S1)
    out2, _ = _build(tp, "VP1", S1, parent=tp.materialize(out1.tables["vehicles_by_route_snapshot"]),
                     parent_snapshot_id=p1.snapshot_id)
    assert tp.rows(out2.tables["vehicle_observations_by_day"]) == []
    q = tp.quality_counts(out2.tables["quality_by_snapshot"])
    assert q["DUPLICATE_OBSERVATION"] == 3
    st = tp.stats(out2)
    assert st["accounting_ok"] is True
    assert (st["accounting"]["observations"]["valid_candidate"], st["accounting"]["observations"]["unique"],
            st["accounting"]["observations"]["duplicate"]) == (3, 0, 3)
    ra = tp.by(out2.tables["route_metrics_by_snapshot"], "route_id")["R_A"]
    assert ra["snapshot_membership_count"] == 3  # duplicates of history are still members of the snapshot


def test_observation_time_regression_serves_parent_observation(tp):
    out_vp2, p2 = _build(tp, "VP2", S2)
    out, _ = _build(tp, "VP1", S2, parent=tp.materialize(out_vp2.tables["vehicles_by_route_snapshot"]),
                    parent_snapshot_id=p2.snapshot_id)
    veh = tp.by(out.tables["vehicles_by_route_snapshot"], "vehicle_id")
    v1 = veh["V1"]
    assert v1["is_inherited"] is True and v1["inherited_from_snapshot_id"] == p2.snapshot_id
    assert (v1["latitude"], v1["longitude"]) == (42.3519, -71.0581)
    assert v1["observation_time"] == datetime.fromtimestamp(1791201655, UTC)  # parent's time: age keeps growing
    assert "OBSERVATION_TIME_REGRESSION" in v1["quality_flags"]
    assert veh["V2"]["is_inherited"] is False and veh["V3"]["is_inherited"] is False
    ra = tp.by(out.tables["route_metrics_by_snapshot"], "route_id")["R_A"]
    assert (ra["fresh_vehicle_count"], ra["stale_vehicle_count"], ra["unknown_age_vehicle_count"],
            ra["inherited_vehicle_count"], ra["snapshot_membership_count"]) == (1, 1, 1, 1, 3)
    q = tp.quality_counts(out.tables["quality_by_snapshot"])
    assert q["OBSERVATION_TIME_REGRESSION"] == 1
    hist = tp.by(out.tables["vehicle_observations_by_day"], "vehicle_id")
    assert hist["V1"]["event_time"] == datetime.fromtimestamp(1791201610, UTC)  # regressed source still in history
    assert "OBSERVATION_TIME_REGRESSION" in hist["V1"]["quality_flags"]
    assert tp.stats(out)["accounting_ok"] is True


def test_parent_unavailable_is_reported_without_inheritance(tp):
    out, _ = _build(tp, "VP1", S1, parent=None, parent_snapshot_id="sn_" + "f" * 32, parent_available=False)
    q = tp.by(out.tables["quality_by_snapshot"], "reason_code")
    assert q["PARENT_UNAVAILABLE"]["record_count"] == 1
    assert q["PARENT_UNAVAILABLE"]["unit"] == "snapshot"
    assert q["PARENT_UNAVAILABLE"]["sample_refs"] == ["sn_" + "f" * 32]
    assert len(tp.rows(out.tables["vehicle_observations_by_day"])) == 3


# --------------------------------------------------------------------------------------------------
# IT06
# --------------------------------------------------------------------------------------------------

@pytest.mark.it_case("IT06")
def test_it06_invalid_vehicle_handling(tp):
    out, params = _build(tp, "IT06_VP", S1)
    metrics = tp.by(out.tables["route_metrics_by_snapshot"], "route_id")
    ra = metrics["R_A"]
    assert (ra["fresh_vehicle_count"], ra["stale_vehicle_count"], ra["unknown_age_vehicle_count"],
            ra["invalid_vehicle_count"], ra["snapshot_membership_count"]) == (2, 0, 1, 5, 7)
    assert ra["is_partial"] is True
    assert metrics["R_B"]["snapshot_membership_count"] == 0

    q = tp.quality_counts(out.tables["quality_by_snapshot"])
    assert q == {
        "INVALID_COORDINATES": 2, "FUTURE_TIMESTAMP": 1, "INVALID_TIMESTAMP_UNIT": 1, "MISSING_VEHICLE_ID": 1,
        "MISSING_OBSERVATION_TIMESTAMP": 1, "CLOCK_SKEW": 1,
    }
    qrows = tp.by(out.tables["quality_by_snapshot"], "reason_code")
    assert qrows["MISSING_VEHICLE_ID"]["severity"] == "error"
    assert qrows["MISSING_VEHICLE_ID"]["disposition_counts"] == {"rejected": 1}
    assert qrows["CLOCK_SKEW"]["severity"] == "warning"
    assert qrows["INVALID_COORDINATES"]["sample_refs"] == ["vp-V4", "vp-V5"]

    veh = tp.by(out.tables["vehicles_by_route_snapshot"], "vehicle_id")
    assert set(veh) == {"V1", "V3", "V4", "V5", "V6", "V7", "V8"}
    expected_invalid = {"V4": "INVALID_COORDINATES", "V5": "INVALID_COORDINATES", "V6": "FUTURE_TIMESTAMP",
                        "V8": "INVALID_TIMESTAMP_UNIT"}
    for vid, row in veh.items():
        assert row["invalid_reason"] == expected_invalid.get(vid), vid
        assert row["position_valid"] is (vid not in expected_invalid), vid
    assert veh["V8"]["observation_time"] is None  # never auto-converted from milliseconds
    assert "CLOCK_SKEW" in veh["V7"]["quality_flags"]

    obs = tp.by(out.curated["observations"], "vehicle_id")
    assert obs["V7"]["freshness_at_build"] == "fresh" and obs["V7"]["age_seconds_at_build"] == 0.0
    assert obs["V6"]["freshness_at_build"] == "invalid"

    st = tp.stats(out)
    assert st["is_partial"] is True and st["accounting_ok"] is True, st["accounting_violations"]
    assert (st["selected_entity_count"], st["fully_valid_entity_count"], st["partially_valid_entity_count"],
            st["rejected_entity_count"]) == (8, 1, 6, 1)
    route_acc = st["accounting"]["per_route"]["R_A"]
    assert route_acc["membership"] == route_acc["fresh"] + route_acc["stale"] + route_acc["unknown_age"] + \
        route_acc["invalid_with_vehicle_id"]

    dlq = tp.rows(out.dlq)
    assert len(dlq) == 1
    d = dlq[0]
    assert d["reason_code"] == "MISSING_VEHICLE_ID" and d["entity_ref"] == "vp-noid"
    assert d["error_id"] == ids.error_id_entity(params.run_id, VP, params.snapshot_id, "vp-noid", None,
                                                "MISSING_VEHICLE_ID")
    assert d["schema_version"] == 1 and d["severity"] == "error" and d["disposition"] == "rejected"
    assert d["excerpt"].startswith("{") and d["excerpt_truncated"] is False
    assert d["detected_at"] == "2026-10-05T12:00:30.000Z"
    from transitpulse.contracts.dlq import DLQ_FIELDS

    assert tuple(out.dlq.columns) == DLQ_FIELDS


def test_empty_snapshot_is_valid_and_routes_get_zero_rows(tp):
    from transitpulse.processing.vehicles import build_vehicle_generation

    feed = {"header": {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET", "timestamp": 1791201620},
            "entity": []}
    snap, params = tp.snapshot_from_feed(feed, VP, metrics_as_of=S1)
    out = build_vehicle_generation(tp.spark, snap, params, tp.run(), tp.dims, None)
    assert tp.rows(out.tables["vehicles_by_route_snapshot"]) == []
    metrics = tp.by(out.tables["route_metrics_by_snapshot"], "route_id")
    assert set(metrics) == {"R_A", "R_B"}
    assert all(m["snapshot_membership_count"] == 0 and m["invalid_vehicle_count"] == 0 for m in metrics.values())
    st = tp.stats(out)
    assert st["is_empty"] is True and st["is_partial"] is False and st["accounting_ok"] is True


def test_output_columns_are_cql_columns(tp):
    from transitpulse.processing.vehicles import HISTORY_COLUMNS, VEHICLE_COLUMNS, VP_ROUTE_METRIC_COLUMNS

    out, _ = _build(tp, "VP1", S1)
    assert tuple(out.tables["vehicles_by_route_snapshot"].columns) == VEHICLE_COLUMNS
    assert tuple(out.tables["vehicle_observations_by_day"].columns) == HISTORY_COLUMNS
    assert tuple(out.tables["route_metrics_by_snapshot"].columns) == VP_ROUTE_METRIC_COLUMNS
    from transitpulse.processing.context import STATS_COLUMNS

    assert tuple(out.stats.columns) == STATS_COLUMNS
    types = dict(out.tables["vehicles_by_route_snapshot"].dtypes)
    assert types["observation_time"] == "timestamp" and types["direction_id"] == "int"
    assert types["quality_flags"] == "array<string>" and types["latitude"] == "double"
    assert dict(out.tables["vehicle_observations_by_day"].dtypes)["utc_date"] == "date"
    qtypes = dict(out.tables["quality_by_snapshot"].dtypes)
    assert qtypes["disposition_counts"] == "map<string,int>" and qtypes["record_count"] == "int"


# --------------------------------------------------------------------------------------------------
# exact match with the hand-written tests/fixtures/golden/expected.json (WP0b)
# --------------------------------------------------------------------------------------------------

@pytest.mark.parametrize(("run", "step", "chain"), [
    ("golden", "S1", ["VP1"]),
    ("golden", "S2", ["VP1", "VP2"]),   # VP2 built with the VP1 generation as frozen parent
    ("IT06", "S1", ["IT06_VP"]),
])
def test_vehicle_generation_matches_expected_json(tp, run, step, chain):
    expected = tp.expected()
    exp = tp.expect(run, step)
    parent, parent_sid, history = None, None, []
    out = params = None
    for label in chain:
        clock = tp.instant(expected["captures"][label]["fetched_at"])
        out, params = _build(tp, label, clock, parent=parent, parent_snapshot_id=parent_sid)
        history.extend(tp.rows(out.tables["vehicle_observations_by_day"]))
        parent, parent_sid = tp.materialize(out.tables["vehicles_by_route_snapshot"]), params.snapshot_id
    assert params.metrics_as_of == tp.instant(exp["feeds"]["vehicle_positions"]["metrics_as_of"])

    # route_metrics_by_snapshot == vehicle_generation_metrics (classes at metrics_as_of)
    gm = tp.by(out.tables["route_metrics_by_snapshot"], "route_id")
    assert set(gm) == set(exp["vehicle_generation_metrics"])
    for route, e in exp["vehicle_generation_metrics"].items():
        g = gm[route]
        assert {"fresh": g["fresh_vehicle_count"], "stale": g["stale_vehicle_count"],
                "unknown": g["unknown_age_vehicle_count"], "invalid": g["invalid_vehicle_count"],
                "membership": g["snapshot_membership_count"]} == e, route

    # served vehicles (valid rows full record; invalid rows frozen reason + flags)
    veh = tp.by(out.tables["vehicles_by_route_snapshot"], "vehicle_id")
    obs = tp.by(out.curated["observations"].where("NOT in_snapshot_duplicate"), "vehicle_id")
    assert set(veh) == set(exp["vehicles"])
    for vid, e in exp["vehicles"].items():
        v, o = veh[vid], obs[vid]
        got = {"route_id": v["route_id"], "class": o["freshness_at_build"], "age_s": o["age_seconds_at_build"],
               "invalid_reason": v["invalid_reason"], "flags": sorted(v["quality_flags"])}
        if v["invalid_reason"] is None:
            got.update(trip_id=v["trip_id"], record_kind=v["record_kind"], observation_time=v["observation_time"],
                       event_time=v["event_time"], latitude=v["latitude"], longitude=v["longitude"])
        want = dict(e)
        want["flags"] = sorted(want["flags"])
        for k in ("observation_time", "event_time"):
            if k in want:
                want[k] = tp.instant(want[k])
        assert got == want, vid

    assert tp.quality_counts(out.tables["quality_by_snapshot"]) == exp["vehicle_quality"]

    # cumulative history (Cassandra PK upsert semantics)
    pks = {(h["vehicle_id"], h["utc_date"], h["event_time"], h["observation_id"]) for h in history}
    got_hist: dict[str, list] = {}
    for vid, _d, et, _oid in pks:
        got_hist.setdefault(vid, []).append(et)
    assert {vid: (len(ets), sorted(ets)) for vid, ets in got_hist.items()} == {
        vid: (e["rows"], sorted(tp.instant(x) for x in e["event_times"])) for vid, e in exp["history"].items()
    }

    st = tp.stats(out)
    feed = exp["feeds"]["vehicle_positions"]
    assert (st["is_partial"], st["is_empty"], st["rejected_entity_count"]) == (
        feed["is_partial"], feed["is_empty"], feed["rejected_entity_count"])
    assert st["accounting_ok"] is True
    dlq_counts: dict[str, int] = {}
    for d in tp.rows(out.dlq):
        dlq_counts[d["reason_code"]] = dlq_counts.get(d["reason_code"], 0) + 1
    want_dlq = {k: v for k, v in exp["dlq"].items() if k != "INVALID_JSON"}  # INVALID_JSON is message level (WP4)
    assert dlq_counts == want_dlq


def test_freshness_policy_vectors_from_expected_json(spark, tp):
    from pyspark.sql import functions as F

    from transitpulse.processing.quality import epoch_micros
    from transitpulse.processing.vehicles import vehicle_freshness_cols

    vectors = tp.expected()["policy_vectors"]["freshness"]
    rows = [(i, v["position_valid"], v["observation_epoch"]) for i, v in enumerate(vectors)]
    clocks = {v["clock"] for v in vectors}
    for clock_s in clocks:
        clock = tp.instant(clock_s)
        df = spark.createDataFrame([r for r in rows if vectors[r[0]]["clock"] == clock_s],
                                   "i int, valid boolean, obs long")
        f, a, s = vehicle_freshness_cols(position_valid=F.col("valid"), observation_epoch_s=F.col("obs"),
                                         metrics_as_of_us=epoch_micros(clock), policy=tp.policy())
        for r in df.select("i", f.alias("f"), a.alias("a"), s.alias("s")).collect():
            e = vectors[r["i"]]["expected"]
            assert (r["f"], r["a"]) == (e["class"], e["age_s"]), vectors[r["i"]]
            assert r["s"] == ("CLOCK_SKEW" in e["flags"]), vectors[r["i"]]
