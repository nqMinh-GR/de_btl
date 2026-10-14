"""WP5 alerts builder against the golden fixture (design C §7-§8, IT09) and the policy reference.

ALERTS1 at 12:00:30Z: R_A = {A1 ACTIVE, A3 ALWAYS, A4 EXPIRED, A5 ACTIVE, A8 ALWAYS (direction 1)};
R_B = {A2 UPCOMING, A4 EXPIRED, A5 ACTIVE, A8 ALWAYS (via trip T5)}; A6 NO_SCOPE_MATCH; A7 UNRESOLVED.
IT09: ALERTS2 (full, without A3) closes A3; ALERTS3 (partial: A5 malformed, A4/A8 omitted) carries
A4, A5, A8 as UNCONFIRMED_INHERITED from ALERTS2.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime

import pytest

from transitpulse.contracts import ids
from transitpulse.contracts.policy import alert_active_state

AL = "alerts"
S1 = datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC)
S2 = datetime(2026, 10, 5, 12, 1, 0, tzinfo=UTC)
S3 = datetime(2026, 10, 5, 12, 2, 0, tzinfo=UTC)

ALERTS1_EXPECTED = {
    ("R_A", "A1"): "ACTIVE", ("R_A", "A3"): "ALWAYS", ("R_A", "A4"): "EXPIRED", ("R_A", "A5"): "ACTIVE",
    ("R_A", "A8"): "ALWAYS",
    ("R_B", "A2"): "UPCOMING", ("R_B", "A4"): "EXPIRED", ("R_B", "A5"): "ACTIVE", ("R_B", "A8"): "ALWAYS",
}


def _build(tp, label, clock, parent=None, **kw):
    from transitpulse.processing.alerts import build_alert_generation

    snap, params = tp.snapshot(label, AL, metrics_as_of=clock, **kw)
    out = build_alert_generation(tp.spark, snap, params, tp.run(clock=clock), tp.dims, parent)
    return out, params


def test_active_state_matches_policy_reference(spark):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    from transitpulse.processing.alerts import PERIODS_SCHEMA, alert_active_state_col

    rng = random.Random(7)
    clock = 1791201630
    points = [None, clock - 3600, clock - 1, clock, clock + 1, clock + 3600]
    cases = [None, []]
    for _ in range(150):
        cases.append([(rng.choice(points), rng.choice(points)) for _ in range(rng.randint(1, 3))])
    schema = T.StructType([T.StructField("i", T.IntegerType()), T.StructField("p", PERIODS_SCHEMA)])
    df = spark.createDataFrame([(i, c) for i, c in enumerate(cases)], schema)
    got = {r["i"]: r["s"] for r in df.select("i", alert_active_state_col(F.col("p"), clock).alias("s")).collect()}
    for i, c in enumerate(cases):
        periods = None if c is None else [{"start": s, "end": e} for s, e in c]
        assert got[i] == str(alert_active_state(periods, clock)), (i, c)


def test_alerts1_route_projection(tp):
    out, params = _build(tp, "ALERTS1", S1)
    rows = tp.by(out.tables["alerts_by_route_snapshot"], "route_id", "alert_id")
    assert {k: v["active_state_at_build"] for k, v in rows.items()} == ALERTS1_EXPECTED
    for r in rows.values():
        assert r["confirmation_status"] == "CONFIRMED" and r["inherited_from_snapshot_id"] is None
        assert r["first_seen_snapshot_id"] == params.snapshot_id
        assert r["selector_resolution"] == "resolved" and r["snapshot_id"] == params.snapshot_id
        assert r["feed_timestamp"] == datetime.fromtimestamp(1791201620, UTC)
    assert rows[("R_A", "A8")]["match_kinds"] == ["direction:1", "route"]
    assert rows[("R_B", "A8")]["match_kinds"] == ["trip"]
    assert rows[("R_A", "A1")]["match_kinds"] == ["agency", "route", "route_type"]
    assert rows[("R_A", "A3")]["match_kinds"] == ["stop"]
    a1 = rows[("R_A", "A1")]
    assert a1["header_text"] == "Synthetic alert A1: route R_A detour 11:00-13:00Z (NOT MBTA)"
    assert a1["description_text"].startswith("Synthetic test alert")
    assert a1["cause"] == "CONSTRUCTION" and a1["effect"] == "DETOUR" and a1["severity_level"] == "WARNING"
    assert a1["active_periods_json"] == '[{"start":1791198000,"end":1791205200}]'
    assert rows[("R_A", "A3")]["active_periods_json"] == "[]"
    assert rows[("R_B", "A2")]["description_text"] is None

    q = tp.by(out.tables["quality_by_snapshot"], "reason_code")
    assert set(q) == {"UNRESOLVED_ALERT_SELECTOR", "NO_SCOPE_MATCH"}
    assert q["UNRESOLVED_ALERT_SELECTOR"]["record_count"] == 1 and q["UNRESOLVED_ALERT_SELECTOR"]["sample_refs"] == ["A7"]
    assert q["UNRESOLVED_ALERT_SELECTOR"]["unit"] == "selector" and q["UNRESOLVED_ALERT_SELECTOR"]["severity"] == "warning"
    assert q["NO_SCOPE_MATCH"]["record_count"] == 1 and q["NO_SCOPE_MATCH"]["sample_refs"] == ["A6"]

    st = tp.stats(out)
    assert st["accounting_ok"] is True, st["accounting_violations"]
    assert (st["selected_entity_count"], st["fully_valid_entity_count"], st["partially_valid_entity_count"],
            st["rejected_entity_count"]) == (8, 7, 1, 0)
    sel = st["accounting"]["selectors"]
    assert (sel["candidate"], sel["matched"], sel["no_scope_match"], sel["unresolved"], sel["invalid"]) == (9, 7, 1, 1, 0)
    rr = st["accounting"]["alert_route_rows"]
    assert (rr["served"], rr["confirmed"], rr["inherited"], rr["closed_absent_from_full_snapshot"]) == (9, 9, 0, 0)
    assert tp.rows(out.dlq) == []

    from transitpulse.processing.alerts import ALERT_COLUMNS

    assert tuple(out.tables["alerts_by_route_snapshot"].columns) == ALERT_COLUMNS
    assert dict(out.tables["alerts_by_route_snapshot"].dtypes)["match_kinds"] == "array<string>"
    cur = tp.rows(out.curated["alerts"])
    assert len(cur) == 9 and all(c["feed_type"] == AL and c["alert_id"] for c in cur)


@pytest.mark.it_case("IT09")
def test_it09_full_snapshot_closes_partial_snapshot_inherits(tp):
    out1, p1 = _build(tp, "ALERTS1", S1)
    out2, p2 = _build(tp, "ALERTS2", S2, parent=tp.materialize(out1.tables["alerts_by_route_snapshot"]),
                      parent_snapshot_id=p1.snapshot_id)
    rows2 = tp.by(out2.tables["alerts_by_route_snapshot"], "route_id", "alert_id")
    assert set(rows2) == {("R_A", "A1"), ("R_A", "A4"), ("R_A", "A5"), ("R_A", "A8"),
                          ("R_B", "A2"), ("R_B", "A4"), ("R_B", "A5"), ("R_B", "A8")}  # A3 closed
    assert all(r["confirmation_status"] == "CONFIRMED" for r in rows2.values())
    assert all(r["first_seen_snapshot_id"] == p1.snapshot_id for r in rows2.values())
    st2 = tp.stats(out2)
    assert st2["is_partial"] is False and st2["accounting_ok"] is True
    assert st2["accounting"]["alert_route_rows"]["closed_absent_from_full_snapshot"] == 1
    assert st2["accounting"]["alert_route_rows"]["closed_sample"] == ["A3"]

    out3, p3 = _build(tp, "ALERTS3", S3, parent=tp.materialize(out2.tables["alerts_by_route_snapshot"]),
                      parent_snapshot_id=p2.snapshot_id)
    st3 = tp.stats(out3)
    assert st3["is_partial"] is True and st3["rejected_entity_count"] == 1
    assert st3["accounting_ok"] is True, st3["accounting_violations"]
    rows3 = tp.by(out3.tables["alerts_by_route_snapshot"], "route_id", "alert_id")
    by_route: dict[str, set[str]] = {}
    for (route, alert) in rows3:
        by_route.setdefault(route, set()).add(alert)
    assert by_route == {"R_A": {"A1", "A4", "A5", "A8"}, "R_B": {"A2", "A4", "A5", "A8"}}
    for (route, alert), r in rows3.items():
        if alert in ("A1", "A2"):
            assert r["confirmation_status"] == "CONFIRMED" and r["inherited_from_snapshot_id"] is None
        else:
            assert r["confirmation_status"] == "UNCONFIRMED_INHERITED", (route, alert)
            assert r["inherited_from_snapshot_id"] == p2.snapshot_id, (route, alert)
            assert r["snapshot_id"] == p3.snapshot_id
        assert r["first_seen_snapshot_id"] == p1.snapshot_id, (route, alert)
    assert rows3[("R_A", "A4")]["active_state_at_build"] == "EXPIRED"
    assert rows3[("R_B", "A5")]["active_state_at_build"] == "ACTIVE"
    assert rows3[("R_A", "A8")]["active_state_at_build"] == "ALWAYS"
    rr = st3["accounting"]["alert_route_rows"]
    assert (rr["served"], rr["confirmed"], rr["inherited"], rr["closed_absent_from_full_snapshot"]) == (8, 2, 6, 0)
    assert tp.quality_counts(out3.tables["quality_by_snapshot"]) == {"INVALID_ENTITY": 1}
    dlq = tp.rows(out3.dlq)
    assert len(dlq) == 1 and dlq[0]["entity_ref"] == "A5" and dlq[0]["reason_code"] == "INVALID_ENTITY"
    assert dlq[0]["error_id"] == ids.error_id_entity(p3.run_id, AL, p3.snapshot_id, "A5", None, "INVALID_ENTITY")


def test_partial_snapshot_without_usable_parent_reports_and_does_not_inherit(tp):
    out, _ = _build(tp, "ALERTS3", S3, parent=None, parent_snapshot_id="sn_" + "e" * 32, parent_available=False)
    rows = tp.by(out.tables["alerts_by_route_snapshot"], "route_id", "alert_id")
    assert set(rows) == {("R_A", "A1"), ("R_B", "A2")}
    q = tp.quality_counts(out.tables["quality_by_snapshot"])
    assert q == {"INVALID_ENTITY": 1, "PARENT_UNAVAILABLE": 1}
    assert tp.stats(out)["accounting_ok"] is True


# --------------------------------------------------------------------------------------------------
# exact match with the hand-written tests/fixtures/golden/expected.json (WP0b)
# --------------------------------------------------------------------------------------------------

@pytest.mark.it_case("IT09")
@pytest.mark.parametrize("step", ["S1", "S2", "S3"])
def test_alerts_match_expected_json(tp, step):
    expected = tp.expected()
    chain = {"S1": ["ALERTS1"], "S2": ["ALERTS1", "ALERTS2"], "S3": ["ALERTS1", "ALERTS2", "ALERTS3"]}[step]
    label_of: dict[str, str] = {}
    closed: dict[str, dict] = {}
    parent, parent_sid = None, None
    out = None
    for label in chain:
        clock = tp.instant(expected["captures"][label]["fetched_at"])
        out, params = _build(tp, label, clock, parent=parent, parent_snapshot_id=parent_sid)
        label_of[params.snapshot_id] = label
        acc = tp.stats(out)["accounting"]["alert_route_rows"]
        for alert_id in acc["closed_sample"]:
            closed[alert_id] = {"closed_reason": acc["closed_reason"], "closed_at": tp.instant(acc["closed_at"]),
                                "closed_by": label}
        parent, parent_sid = tp.materialize(out.tables["alerts_by_route_snapshot"]), params.snapshot_id
    exp = tp.expect("IT09", step)

    got: dict[str, dict] = {}
    for r in tp.rows(out.tables["alerts_by_route_snapshot"]):
        got.setdefault(r["route_id"], {})[r["alert_id"]] = {
            "state": r["active_state_at_build"],
            "confirmation": r["confirmation_status"],
            "inherited_from": label_of.get(r["inherited_from_snapshot_id"]) if r["inherited_from_snapshot_id"] else None,
            "first_seen": label_of.get(r["first_seen_snapshot_id"]),
        }
    assert got == exp["alerts_by_route"]

    confirmed = {r["alert_id"]: r["selector_resolution"] for r in tp.rows(out.tables["alerts_by_route_snapshot"])
                 if r["confirmation_status"] == "CONFIRMED"}
    for alert_id, resolution in exp["alert_selector_resolution"].items():
        if alert_id in confirmed:
            assert confirmed[alert_id] == resolution, alert_id

    quality_by_alert: dict[str, list] = {}
    for q in tp.rows(out.tables["quality_by_snapshot"]):
        for ref in q["sample_refs"]:
            quality_by_alert.setdefault(ref, []).append(q["reason_code"])
    assert {k: sorted(v) for k, v in quality_by_alert.items()} == {
        k: sorted(v) for k, v in exp["alert_quality"].items()
    }

    assert closed == {k: {**v, "closed_at": tp.instant(v["closed_at"])} for k, v in exp["alerts_closed"].items()}
    dlq_counts: dict[str, int] = {}
    for d in tp.rows(out.dlq):
        dlq_counts[d["reason_code"]] = dlq_counts.get(d["reason_code"], 0) + 1
    assert dlq_counts == exp["dlq"]
    st = tp.stats(out)
    feed = exp["feeds"]["alerts"]
    assert (st["is_partial"], st["rejected_entity_count"]) == (feed["is_partial"], feed["rejected_entity_count"])
    assert st["accounting_ok"] is True


def test_alert_state_policy_vectors_from_expected_json(spark, tp):
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    from transitpulse.processing.alerts import PERIODS_SCHEMA, alert_active_state_col

    vectors = tp.expected()["policy_vectors"]["alert_state"]
    schema = T.StructType([T.StructField("p", PERIODS_SCHEMA)])
    for v in vectors:
        periods = None if v["active_period"] is None else [(p.get("start"), p.get("end")) for p in v["active_period"]]
        df = spark.createDataFrame([(periods,)], schema)
        got = df.select(alert_active_state_col(F.col("p"), v["clock_epoch"]).alias("s")).collect()[0]["s"]
        assert got == v["state"], v
        assert str(alert_active_state(v["active_period"], v["clock_epoch"])) == v["state"], v
