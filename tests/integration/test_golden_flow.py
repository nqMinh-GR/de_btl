"""IT01-IT04: the golden run (tests/fixtures/golden, timeline run ``golden``) through Kafka -> Spark -> Cassandra -> API.

One module-scoped run, steps applied in order (IT01 S1; IT02 new VP + duplicate delivery; IT03 late re-delivery;
IT04 clock-only advance). A failed predecessor makes the later cases NOT_RUN (they share the run state).
Each step asserts API == expected.json and Cassandra == expected.json; oracle(fixture) == expected.json is asserted
by tests/unit/test_oracle_*.py.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from integration.harness import FixtureRun, api_json, assert_outcome, classify_golden_steps, oracle_check

pytestmark = [pytest.mark.integration, pytest.mark.slow]

VP = "vehicle_positions"
KPI_KEYS = ("snapshot_id", "trip_instance_count", "eligible_trip_count", "predicted_late_trip_count",
            "predicted_late_rate", "mean_predicted_delay_seconds", "excluded_by_reason", "unknown_uncertainty_count",
            "metrics_as_of")


@pytest.fixture(scope="module")
def golden(module_harness: Any, stack_ctx: Any) -> Any:
    fr = FixtureRun.prepare(module_harness, stack_ctx, "golden")
    state = {"fr": fr, "classes": classify_golden_steps(fr.steps, stack_ctx.golden_dir), "ok": {}, "snap": {}}
    module_harness.write_evidence("golden_step_classes.json", {"run_id": fr.run.run_id, **state["classes"]})
    yield state
    module_harness.stop_processor()


def _require(state: dict[str, Any], *previous: str) -> None:
    for p in previous:
        if not state["ok"].get(p):
            pytest.skip(f"NOT_RUN: predecessor {p} did not pass (IT01-IT04 share one run)")


def _kpis(fr: FixtureRun, routes: list[str]) -> dict[str, Any]:
    out = {}
    for r in routes:
        status, body = api_json(fr.h, f"routes/{r}/metrics", run_id=fr.run.run_id)
        pm = ((body.get("data") or {}).get("prediction_metrics") or {}) if status == 200 else {"status": status}
        out[r] = {k: pm.get(k) for k in KPI_KEYS}
    return out


def _vehicle(fr: FixtureRun, vehicle_id: str) -> dict[str, Any] | None:
    status, body = api_json(fr.h, "vehicles", run_id=fr.run.run_id, limit=2000)
    if status != 200:
        return None
    for v in (body.get("data") or {}).get("vehicles") or []:
        if v.get("vehicle_id") == vehicle_id:
            return v
    return None


def _history_oids(fr: FixtureRun, vehicle_id: str, day: date) -> list[str]:
    return [r["observation_id"] for r in fr.h.history_rows(fr.run.run_id, vehicle_id, [day])]


@pytest.mark.it_case("IT01")
def test_it01_golden_initial(golden: dict[str, Any], case_record: Any) -> None:
    fr: FixtureRun = golden["fr"]
    case_record.run_id = fr.run.run_id
    steps = golden["classes"]["IT01"]
    assert steps, "timeline run 'golden' has no first step"
    case_record.expected(three_way="API == expected.json, Cassandra == expected.json, oracle(fixture) == expected.json")
    oracle_check(fr.ctx.golden_dir, "golden", fr.ctx.expected, case_record)
    for step in steps:
        out = fr.run_step(step)
        assert_outcome(out, case_record)
        case_record.evidence(str(fr.h.evidence_dir / f"{fr.run_key}_{fr.run.run_id[:8]}_{step}.json"))
    ptr = fr.h.pointer(fr.run.run_id, VP)
    assert ptr is not None, "no vehicle_positions pointer after S1"
    for route in fr.run.route_ids:
        rows = fr.h.vehicle_rows(fr.run.run_id, ptr["snapshot_id"], route)
        man = fr.h.manifest(fr.run.run_id, VP, ptr["snapshot_id"]) or {}
        expected_rows = (man.get("expected_counts") or {}).get(f"vehicles_by_route_snapshot/{route}", 0)
        assert len(rows) == expected_rows, (
            f"vehicles_by_route_snapshot[{route}] has {len(rows)} rows, manifest expected_counts says {expected_rows}")
    golden["snap"]["kpi"] = _kpis(fr, list(fr.run.route_ids))
    case_record.actual(kpi_after_IT01=golden["snap"]["kpi"], labels=dict(fr.labels))
    golden["ok"]["IT01"] = True


@pytest.mark.it_case("IT02")
def test_it02_duplicate_snapshot_and_observation(golden: dict[str, Any], case_record: Any) -> None:
    _require(golden, "IT01")
    fr: FixtureRun = golden["fr"]
    case_record.run_id = fr.run.run_id
    steps = golden["classes"]["IT02"]
    assert steps, "timeline run 'golden' has no new-snapshot/duplicate step"
    for step in steps:
        assert_outcome(fr.run_step(step), case_record)
    run_id = fr.run.run_id
    vp_labels = [label for label, env in fr.envelopes.items() if env["feed_type"] == VP]
    distinct = {fr.labels[label] for label in vp_labels}
    manifests = {m["snapshot_id"] for m in fr.h.manifests(run_id, VP)}
    assert manifests == distinct, f"one manifest per VP snapshot_id expected: manifests={manifests} published={distinct}"
    republished = [str(item["label"]) for step in steps for item in fr.items(step)
                   if item.get("kind") == "republish" and item.get("label")]
    dup_evidence = {}
    for label in republished:
        q = fr.h.quality_rows(run_id, VP, fr.labels[label])
        dup_evidence[label] = sorted(q)
        assert "DUPLICATE_SNAPSHOT_DELIVERY" in q, (
            f"duplicate delivery of {label} left no DUPLICATE_SNAPSHOT_DELIVERY quality row (rows: {sorted(q)})")
    clock = fr.h.current_clock(run_id)
    assert clock is not None
    for vid in ("V1", "V2", "V3"):
        oids = _history_oids(fr, vid, clock.date())
        assert len(oids) == len(set(oids)), f"duplicate history observation ids for {vid}: {oids}"
    kpi_now = _kpis(fr, list(fr.run.route_ids))
    assert kpi_now == golden["snap"]["kpi"], f"KPI changed without new trip updates: {golden['snap']['kpi']} -> {kpi_now}"
    v1 = _vehicle(fr, "V1")
    assert v1 is not None, "V1 missing from /vehicles after IT02"
    golden["snap"]["v1_position"] = (v1.get("latitude"), v1.get("longitude"))
    golden["snap"]["vp_pointer"] = (fr.h.pointer(run_id, VP) or {}).get("snapshot_id")
    golden["snap"]["vp_manifests"] = len(manifests)
    case_record.actual(vp_manifests=len(manifests), duplicate_quality=dup_evidence, kpi_unchanged=True)
    golden["ok"]["IT02"] = True


@pytest.mark.it_case("IT03")
def test_it03_late_snapshot_no_rollback(golden: dict[str, Any], case_record: Any) -> None:
    _require(golden, "IT01", "IT02")
    fr: FixtureRun = golden["fr"]
    run_id = fr.run.run_id
    case_record.run_id = run_id
    steps = golden["classes"]["IT03"]
    assert steps, "timeline run 'golden' has no late-snapshot step"
    published_before = {label for _, label, _ in fr.published}
    for step in steps:
        assert_outcome(fr.run_step(step), case_record)
    ptr = (fr.h.pointer(run_id, VP) or {}).get("snapshot_id")
    assert ptr == golden["snap"]["vp_pointer"], (
        f"pointer moved on a late snapshot: {fr.label_of(golden['snap']['vp_pointer'])} -> {fr.label_of(ptr)}")
    v1 = _vehicle(fr, "V1")
    assert v1 is not None and (v1.get("latitude"), v1.get("longitude")) == golden["snap"]["v1_position"], (
        f"V1 position rolled back: {golden['snap']['v1_position']} -> {v1 and (v1.get('latitude'), v1.get('longitude'))}")
    late = [label for (published_step, label, _delivery) in fr.published if published_step in steps]
    audit = {}
    for label in late:
        sid = fr.labels[label]
        q = fr.h.quality_rows(run_id, VP, sid)
        man = fr.h.manifest(run_id, VP, sid) or {}
        audit[label] = {"quality": sorted(q), "status": man.get("status"), "outcome": man.get("publish_outcome")}
        assert "LATE_SNAPSHOT_NOT_PUBLISHED" in q, f"late {label} has no LATE_SNAPSHOT_NOT_PUBLISHED row: {sorted(q)}"
        assert man.get("status") == "READY", f"late {label} manifest is {man.get('status')}, expected READY"
        if label not in published_before:
            assert man.get("publish_outcome") == "LATE_NOT_PUBLISHED", audit[label]
    case_record.actual(late_audit=audit, pointer=fr.label_of(ptr))
    golden["ok"]["IT03"] = True


@pytest.mark.it_case("IT04")
def test_it04_clock_advance_without_positions(golden: dict[str, Any], case_record: Any) -> None:
    _require(golden, "IT01", "IT02", "IT03")
    fr: FixtureRun = golden["fr"]
    run_id = fr.run.run_id
    case_record.run_id = run_id
    steps = golden["classes"]["IT04"]
    assert steps, "timeline run 'golden' has no clock-only step"
    vp_manifests_before = len(fr.h.manifests(run_id, VP))
    for step in steps:
        assert_outcome(fr.run_step(step), case_record)
    row = fr.h.run_row(run_id) or {}
    assert row.get("application_clock") == fr.h.current_clock(run_id), (
        f"run_metadata.application_clock {row.get('application_clock')} != last tick {fr.h.current_clock(run_id)}")
    assert len(fr.h.manifests(run_id, VP)) == vp_manifests_before, "a clock tick created a vehicle generation"
    case_record.actual(application_clock=row.get("application_clock"), run_state=row.get("run_state"))
    golden["ok"]["IT04"] = True
