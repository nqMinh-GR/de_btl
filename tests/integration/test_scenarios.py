"""IT06-IT09: synthetic fixture scenarios (timeline runs IT06..IT09, expected.json runs IT06..IT09).

Every step is compared with expected.json through the API and Cassandra; each case adds the invariants of design E
§7.4 that the API cannot show (DLQ records, accounting identities, frozen dispositions, inheritance lineage).
"""

from __future__ import annotations

from typing import Any

import pytest

from integration.harness import FixtureRun, api_json, assert_outcome, dlq_records, oracle_check, scope_stop_ids

pytestmark = [pytest.mark.integration, pytest.mark.slow]

VP, TU, AL = "vehicle_positions", "trip_updates", "alerts"


def _run_all_steps(fr: FixtureRun, case_record: Any) -> None:
    if not fr.has_expectations():
        pytest.fail(f"expected.json has no steps for run {fr.run_key}; the scenario cannot be verified")
    case_record.run_id = fr.run.run_id
    oracle_check(fr.ctx.golden_dir, fr.run_key, fr.ctx.expected, case_record)
    for step in fr.step_names:
        out = fr.run_step(step)
        assert_outcome(out, case_record)


def _prepare(harness: Any, stack_ctx: Any, key: str) -> tuple[FixtureRun, dict[tuple[str, int], int]]:
    from transitpulse.contracts.topics import DLQ

    dlq_start = harness.high_watermarks([DLQ])
    return FixtureRun.prepare(harness, stack_ctx, key), dlq_start


def _ended_before(alert_row: dict[str, Any], as_of: Any) -> bool:
    """True when every active period of the alert ended at or before ``as_of`` (may lawfully stop being carried)."""
    import json
    from datetime import datetime

    if not isinstance(as_of, datetime):
        return False
    try:
        periods = json.loads(alert_row.get("active_periods_json") or "[]")
    except ValueError:
        return False
    if not periods:
        return False
    epoch = as_of.timestamp()
    ends = [p.get("end") for p in periods if isinstance(p, dict)]
    return bool(ends) and all(isinstance(e, (int, float)) and e <= epoch for e in ends)


def _prediction_rows(fr: FixtureRun) -> list[dict[str, Any]]:
    ptr = fr.h.pointer(fr.run.run_id, TU)
    assert ptr is not None, "no trip_updates pointer"
    rows: list[dict[str, Any]] = []
    for stop in scope_stop_ids(fr.static, fr.run.route_ids):
        rows.extend(fr.h.query("SELECT * FROM predictions_by_stop_snapshot WHERE run_id = %s AND snapshot_id = %s "
                               "AND stop_id = %s", (fr.run.run_id, ptr["snapshot_id"], stop)))
    return rows


@pytest.mark.it_case("IT06")
def test_it06_missing_timestamp_and_invalid_gps(harness: Any, stack_ctx: Any, case_record: Any) -> None:
    fr, dlq_start = _prepare(harness, stack_ctx, "IT06")
    _run_all_steps(fr, case_record)
    run_id = fr.run.run_id
    ptr = harness.pointer(run_id, VP)
    assert ptr is not None, "no vehicle_positions pointer for IT06"
    man = harness.manifest(run_id, VP, ptr["snapshot_id"]) or {}
    sel = man.get("selected_entity_count")
    parts = [man.get(k) for k in ("fully_valid_entity_count", "partially_valid_entity_count", "rejected_entity_count")]
    assert sel is not None and None not in parts and sel == sum(parts), (
        f"accounting identity selected == fully_valid + partially_valid + rejected violated: {sel} vs {parts}")
    identities = {}
    for route in fr.run.route_ids:
        rows = harness.query("SELECT * FROM route_metrics_by_snapshot WHERE run_id = %s AND feed_type = %s AND "
                             "snapshot_id = %s AND route_id = %s", (run_id, VP, ptr["snapshot_id"], route))
        assert rows, f"no route_metrics_by_snapshot row for {route}"
        r = rows[0]
        # contracts.accounting: membership == fresh + stale + unknown_age + invalid WITH a vehicle id (D-BIZ-07:
        # invalid_vehicle_count counts entities, including the one without an id, so it is not used here).
        vrows = harness.vehicle_rows(run_id, ptr["snapshot_id"], route)
        invalid_with_id = sum(1 for v in vrows if v.get("invalid_reason"))
        total = sum(int(r.get(k) or 0) for k in ("fresh_vehicle_count", "stale_vehicle_count",
                                                  "unknown_age_vehicle_count")) + invalid_with_id
        identities[route] = {"membership": r.get("snapshot_membership_count"), "classes_with_id": total,
                             "distinct_vehicle_rows": len({v["vehicle_id"] for v in vrows})}
        assert r.get("snapshot_membership_count") == total == len({v["vehicle_id"] for v in vrows}), (
            f"membership identity violated on {route}: {identities[route]}")
    records = dlq_records(harness, run_id, dlq_start)
    missing_id = [d for d in records if d.get("reason_code") == "MISSING_VEHICLE_ID"]
    assert missing_id, f"no DLQ record MISSING_VEHICLE_ID for run {run_id}: {[d.get('reason_code') for d in records]}"
    assert all(d.get("error_id") and d.get("raw_path") for d in missing_id), missing_id
    malformed = [i for s in fr.step_names for i in fr.items(s) if i.get("kind") == "malformed_envelope"]
    if malformed:
        bad = [d for d in records if d.get("reason_code") == "INVALID_JSON"]
        assert bad, "the malformed message on the VP topic produced no INVALID_JSON DLQ record"
        assert (harness.pointer(run_id, VP) or {}).get("snapshot_id") == ptr["snapshot_id"], "view changed"
    case_record.actual(accounting={"selected": sel, "parts": parts, "membership_identity": identities},
                       dlq_reasons=sorted({d.get("reason_code") for d in records}))


@pytest.mark.it_case("IT07")
def test_it07_ambiguous_date_and_repeated_stop(harness: Any, stack_ctx: Any, case_record: Any) -> None:
    fr, _ = _prepare(harness, stack_ctx, "IT07")
    _run_all_steps(fr, case_record)
    rows = _prediction_rows(fr)
    ambiguous = [r for r in rows if r.get("schedule_resolution") == "ambiguous"]
    assert ambiguous, "no prediction with schedule_resolution=ambiguous (T4 without start_date)"
    assert all(r.get("predicted_delay_seconds") is None for r in ambiguous), (
        "an ambiguous service date must never produce a delay: "
        f"{[(r['trip_id'], r['predicted_delay_seconds']) for r in ambiguous]}")
    assert all(r.get("kpi_eligible") is not True for r in ambiguous)
    assert all(r.get("predicted_at") is not None for r in ambiguous), "predicted time must still be shown"
    ptr = harness.pointer(fr.run.run_id, TU) or {}
    reasons = harness.quality_rows(fr.run.run_id, TU, ptr.get("snapshot_id", ""))
    # KPI exclusion reasons are asserted through prediction_metrics.excluded (expected.json); the quality rows are
    # recorded as evidence only (their presence per reason is not part of the frozen contract).
    case_record.actual(ambiguous_predictions=len(ambiguous), tu_quality_reasons=sorted(reasons))


@pytest.mark.it_case("IT08")
def test_it08_uncertainty_and_cancelled_trip(harness: Any, stack_ctx: Any, case_record: Any) -> None:
    fr, _ = _prepare(harness, stack_ctx, "IT08")
    _run_all_steps(fr, case_record)
    rows = _prediction_rows(fr)
    classes = {}
    for r in rows:
        u = r.get("uncertainty")
        q = r.get("prediction_quality")
        classes.setdefault(str(u), set()).add(q)
        if u is not None and u > 301:
            assert q == "invalid", f"uncertainty {u} must be invalid, got {q} ({r.get('trip_id')})"
            assert r.get("kpi_eligible") is not True
        elif u == 301:
            assert q == "degraded", f"uncertainty 301 must be degraded, got {q}"
        elif u == 300:
            assert q == "schedule_only", f"uncertainty 300 must be schedule_only, got {q}"
        elif u is None:
            assert q != "realtime", f"absent uncertainty must stay unknown (never treated as 0), got {q}"
    invalid = [r for r in rows if r.get("prediction_quality") == "invalid"]
    for stop in sorted({r["stop_id"] for r in invalid}):
        status, body = api_json(harness, f"stops/{stop}/predictions", run_id=fr.run.run_id, include="displayable")
        assert status == 200, f"/stops/{stop}/predictions -> {status}"
        shown = {p.get("prediction_id") for p in (body.get("data") or {}).get("predictions") or []}
        leaked = shown & {r["prediction_id"] for r in invalid}
        assert not leaked, f"invalid predictions displayed as ETA at {stop}: {leaked}"
    case_record.actual(uncertainty_classes={k: sorted(str(x) for x in v) for k, v in classes.items()},
                       invalid_predictions=len(invalid))


@pytest.mark.it_case("IT09")
def test_it09_partial_alerts_inheritance(harness: Any, stack_ctx: Any, case_record: Any) -> None:
    fr = FixtureRun.prepare(harness, stack_ctx, "IT09")
    if not fr.has_expectations():
        pytest.fail("expected.json has no steps for run IT09")
    case_record.run_id = fr.run.run_id
    oracle_check(fr.ctx.golden_dir, fr.run_key, fr.ctx.expected, case_record)
    run_id = fr.run.run_id
    previous: dict[str, dict[str, dict[str, Any]]] | None = None
    lineage: dict[str, Any] = {}
    for step in fr.step_names:
        assert_outcome(fr.run_step(step), case_record)
        ptr = harness.pointer(run_id, AL)
        assert ptr is not None, f"no alerts pointer after {step}"
        man = harness.manifest(run_id, AL, ptr["snapshot_id"]) or {}
        as_of = man.get("metrics_as_of")
        current: dict[str, dict[str, dict[str, Any]]] = {}
        inherited_rows = []
        for route in fr.run.route_ids:
            rows = harness.query("SELECT * FROM alerts_by_route_snapshot WHERE run_id = %s AND snapshot_id = %s AND "
                                 "route_id = %s", (run_id, ptr["snapshot_id"], route))
            current[route] = {r["alert_id"]: r for r in rows}
            inherited_rows += [r for r in rows if r.get("confirmation_status") == "UNCONFIRMED_INHERITED"]
        lineage[step] = {"label": fr.label_of(ptr["snapshot_id"]), "is_partial": man.get("is_partial"),
                         "alerts": {k: sorted(v) for k, v in current.items()},
                         "inherited": sorted({r["alert_id"] for r in inherited_rows})}
        if man.get("is_partial") and previous is not None:
            for route, prev_rows in previous.items():
                missing = set(prev_rows) - set(current.get(route, {}))
                closed = sorted(a for a in missing if not _ended_before(prev_rows[a], as_of))
                lineage[step].setdefault("dropped_expired_on_partial", {})[route] = sorted(missing - set(closed))
                assert not closed, f"partial snapshot closed still-relevant alerts on {route}: {closed}"
            for r in inherited_rows:
                assert r.get("inherited_from_snapshot_id"), f"inherited alert {r['alert_id']} has no lineage"
        if not man.get("is_partial"):
            assert not inherited_rows, f"a full snapshot must not carry inherited alerts: {inherited_rows}"
        previous = current
    assert any(v["is_partial"] for v in lineage.values()), "the IT09 timeline produced no partial alerts snapshot"
    case_record.actual(lineage=lineage)
