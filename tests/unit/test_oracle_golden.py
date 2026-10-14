"""oracle(fixture) == expected.json, for every run/step and every static/policy vector.

``expected.json`` is hand-written (see tests/fixtures/golden/PROVENANCE.md); the oracle in ``tests/oracle``
re-implements the rules independently. Agreement of the two is the first leg of the three-way check
(hand values == oracle == pipeline). Floats are compared with absolute tolerance 1e-9; null is strict;
booleans are never equal to numbers.
"""

from __future__ import annotations

import json
import math
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pytest

TESTS_DIR = Path(__file__).resolve().parents[1]
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from oracle import gtfs, realtime, scenario  # noqa: E402

GOLDEN = TESTS_DIR / "fixtures" / "golden"
EXPECTED: dict[str, Any] = json.loads((GOLDEN / "expected.json").read_text(encoding="utf-8"))
TIMELINE: dict[str, Any] = json.loads((GOLDEN / "timeline.json").read_text(encoding="utf-8"))
TOL = 1e-9
RUN_NAMES = ("golden", "IT06", "IT07", "IT08", "IT09")


def diff(expected: Any, actual: Any, path: str = "$") -> list[str]:
    """Structural difference list (empty == equal)."""
    if expected is None or actual is None:
        return [] if expected is None and actual is None else [f"{path}: expected {expected!r}, got {actual!r}"]
    if isinstance(expected, bool) or isinstance(actual, bool):
        ok = isinstance(expected, bool) and isinstance(actual, bool) and expected == actual
        return [] if ok else [f"{path}: expected {expected!r}, got {actual!r}"]
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        if isinstance(expected, int) and isinstance(actual, int):
            ok = expected == actual
        else:
            ok = math.isclose(float(expected), float(actual), rel_tol=0.0, abs_tol=TOL)
        return [] if ok else [f"{path}: expected {expected!r}, got {actual!r}"]
    if isinstance(expected, dict) and isinstance(actual, dict):
        out: list[str] = []
        for key in sorted(set(expected) | set(actual), key=str):
            if key not in actual:
                out.append(f"{path}.{key}: missing in actual (expected {expected[key]!r})")
            elif key not in expected:
                out.append(f"{path}.{key}: unexpected key (actual {actual[key]!r})")
            else:
                out.extend(diff(expected[key], actual[key], f"{path}.{key}"))
        return out
    if isinstance(expected, list) and isinstance(actual, list):
        out = []
        if len(expected) != len(actual):
            out.append(f"{path}: length {len(expected)} != {len(actual)}")
        for i, (e, a) in enumerate(zip(expected, actual)):
            out.extend(diff(e, a, f"{path}[{i}]"))
        return out
    if type(expected) is not type(actual) or expected != actual:
        return [f"{path}: expected {expected!r} ({type(expected).__name__}), got {actual!r} ({type(actual).__name__})"]
    return []


def _assert_same(expected: Any, actual: Any, where: str) -> None:
    problems = diff(expected, actual, where)
    assert not problems, "\n".join(problems[:60])


def test_diff_helper_semantics():
    assert diff({"a": 210.0}, {"a": 210}) == []
    assert diff({"a": 0.5}, {"a": 0.5 + 1e-12}) == []
    assert diff({"a": None}, {"a": 0})
    assert diff({"a": False}, {"a": 0})
    assert diff([1, 2], [1, 2, 3])
    assert diff({"a": 1}, {"a": 1, "b": 2})


def test_run_names_align():
    assert set(EXPECTED["runs"]) == set(RUN_NAMES)
    assert set(TIMELINE["runs"]) == set(RUN_NAMES)


def test_policy_matches_oracle():
    _assert_same(EXPECTED["policy"], realtime.DEFAULT_POLICY, "$.policy")


def test_static_summary():
    actual = gtfs.static_summary(GOLDEN / "static", EXPECTED["scope_route_ids"])
    _assert_same(EXPECTED["static"]["summary"], actual, "$.static.summary")


def test_active_checks():
    feed = gtfs.GtfsFeed.load(GOLDEN / "static")
    for item in EXPECTED["static"]["active_checks"]:
        got = feed.calendar.active(item["service_id"], date.fromisoformat(item["date"]))
        assert got is item["active"], item


def test_time_vectors():
    tz = EXPECTED["agency_timezone"]
    for vector in EXPECTED["static"]["time_vectors"]:
        actual = gtfs.time_vector(vector["service_date"], vector["gtfs_time"], tz)
        _assert_same(vector["expected"], actual, f"$.time_vectors[{vector['service_date']} {vector['gtfs_time']}]")


def test_gtfs_time_round_trip():
    assert gtfs.parse_gtfs_time("25:10:00") == 90600
    assert gtfs.parse_gtfs_time("24:00:00") == 86400
    assert gtfs.parse_gtfs_time("") is None
    assert gtfs.parse_gtfs_time(None) is None
    assert gtfs.format_gtfs_seconds(90600) == "25:10:00"
    for bad in ("8:60:00", "08:05", "ab:cd:ef", "08:05:61"):
        with pytest.raises(ValueError):
            gtfs.parse_gtfs_time(bad)


def test_resolution_vectors():
    scope = gtfs.build_scope(gtfs.GtfsFeed.load(GOLDEN / "static"), EXPECTED["scope_route_ids"])
    window = EXPECTED["policy"]["service_date_inference_window_seconds"]
    for vector in EXPECTED["static"]["resolution_vectors"]:
        res = gtfs.resolve_trip_instance(scope, vector["trip"], gtfs.parse_iso(vector["clock"]), window)
        _assert_same(vector["expected"], gtfs.resolution_summary(res), f"$.resolution_vectors[{vector['name']}]")


def test_stop_resolution_vectors():
    scope = gtfs.build_scope(gtfs.GtfsFeed.load(GOLDEN / "static"), EXPECTED["scope_route_ids"])
    for vector in EXPECTED["static"]["stop_resolution_vectors"]:
        actual = gtfs.resolve_stop(scope.trips[vector["trip_id"]], vector["stop_sequence"], vector["stop_id"])
        _assert_same(vector["expected"], actual, f"$.stop_resolution_vectors[{vector}]")


def test_uncertainty_vectors():
    for vector in EXPECTED["policy_vectors"]["uncertainty"]:
        assert realtime.classify_uncertainty(vector["uncertainty"]) == vector["class"], vector


def test_freshness_vectors():
    policy = EXPECTED["policy"]
    for vector in EXPECTED["policy_vectors"]["freshness"]:
        clock = gtfs.to_epoch(gtfs.parse_iso(vector["clock"]))
        cls, age, flags = realtime.vehicle_freshness(vector["position_valid"], vector["observation_epoch"], clock, policy)
        _assert_same(vector["expected"], {"class": cls, "age_s": age, "flags": flags}, f"$.freshness[{vector}]")


def test_alert_state_vectors():
    for vector in EXPECTED["policy_vectors"]["alert_state"]:
        assert realtime.alert_active_state(vector["active_period"], vector["clock_epoch"]) == vector["state"], vector


@pytest.mark.parametrize("run_name", RUN_NAMES)
def test_oracle_matches_expected_run(run_name: str):
    actual = scenario.run_scenario(GOLDEN, run_name)
    _assert_same(EXPECTED["runs"][run_name], actual, f"$.runs.{run_name}")


# --------------------------------------------------------------------------------------------------
# Headline values restated from the spec (md §17.1, PDF p18/p26) and design C §8 — they guard against the
# hand-written file and the oracle drifting together.
# --------------------------------------------------------------------------------------------------

def _step(run: str, step: str) -> dict[str, Any]:
    return next(s for s in EXPECTED["runs"][run]["steps"] if s["step"] == step)["expect"]


def test_spec_golden_headline_values():
    s1 = _step("golden", "S1")
    assert s1["vehicle_metrics"]["R_A"] == {"fresh": 1, "stale": 1, "unknown": 1, "invalid": 0, "membership": 3}
    assert s1["vehicles"]["V1"]["age_s"] == 20 and s1["vehicles"]["V2"]["age_s"] == 210
    assert s1["vehicles"]["V3"]["age_s"] is None and s1["vehicles"]["V3"]["class"] == "unknown"
    kpi = s1["prediction_metrics"]["R_A"]
    assert (kpi["eligible"], kpi["late"], kpi["rate"], kpi["mean"]) == (2, 1, 0.5, 210.0)
    assert s1["targets"]["T1"]["delay_s"] == 360 and s1["targets"]["T2"]["delay_s"] == 60
    assert s1["prediction_metrics"]["R_B"]["rate"] is None and s1["prediction_metrics"]["R_B"]["mean"] is None
    assert len(s1["predictions"]) == 9

    s2 = _step("golden", "S2")
    assert s2["vehicle_metrics"]["R_A"]["membership"] == 2 and "V2" not in s2["vehicles"]
    assert {v: h["rows"] for v, h in s2["history"].items()} == {"V1": 2, "V2": 1, "V3": 2}

    s3, s4 = _step("golden", "S3"), _step("golden", "S4")
    assert s3["history"] == s2["history"] and s4["history"] == s2["history"]
    assert s4["pointer"]["vehicle_positions"] == "VP2" and s4["vehicles"]["V1"]["latitude"] == 42.3519
    assert s4["deliveries"][0]["late"] is True and s4["lineage_totals"]["late_labels"] == ["VP1"]

    s5 = _step("golden", "S5")
    assert s5["vehicles"]["V1"] == dict(s4["vehicles"]["V1"], **{"class": "stale", "age_s": 135})
    assert s5["vehicle_metrics"]["R_A"]["fresh"] == 0 and s5["vehicle_metrics"]["R_A"]["stale"] == 1
    assert s5["prediction_metrics"]["R_A"]["stale"] is True and s5["feeds"]["trip_updates"]["age_s"] == 170


def test_design_c_scenario_headline_values():
    it06 = _step("IT06", "S1")
    assert it06["vehicle_metrics"]["R_A"] == {"fresh": 2, "stale": 0, "unknown": 1, "invalid": 5, "membership": 7}
    assert it06["feeds"]["vehicle_positions"]["is_partial"] is True
    assert _step("IT06", "S2")["dlq"] == {"INVALID_JSON": 1, "MISSING_VEHICLE_ID": 1}

    it07 = _step("IT07", "S1")["prediction_metrics"]
    assert it07["R_A"]["excluded"] == {"AMBIGUOUS_SERVICE_DATE": 1} and it07["R_A"]["rate"] is None
    assert (it07["R_B"]["eligible"], it07["R_B"]["rate"], it07["R_B"]["mean"]) == (1, 0.0, 240.0)

    it08 = _step("IT08", "S1")["prediction_metrics"]
    assert it08["R_A"]["excluded_total"] == 3 and it08["R_A"]["eligible"] == 0
    assert (it08["R_B"]["eligible"], it08["R_B"]["late"], it08["R_B"]["rate"], it08["R_B"]["mean"]) == (2, 1, 0.5, 120.0)
    assert it08["R_B"]["unknown_uncertainty"] == 1

    it09 = _step("IT09", "S3")
    assert set(it09["alerts_by_route"]["R_A"]) == {"A1", "A4", "A5", "A8"}
    assert set(it09["alerts_by_route"]["R_B"]) == {"A2", "A4", "A5", "A8"}
    assert "A3" in it09["alerts_closed"] and it09["feeds"]["alerts"]["is_partial"] is True
