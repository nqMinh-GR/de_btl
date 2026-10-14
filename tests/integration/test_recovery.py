"""IT10/IT11: processor crash at fault points of the publish protocol, restart on the same run and checkpoint.

The processor runs with ``--enable-fault-injection``; faults are one-shot arm files (``W/faults/<run>/<point>.json``,
D-PUB-06) matching the golden VP2 snapshot; the process exits with 86 when a fault fires and is restarted with
``StackHarness.restart_processor`` (same argv, run and checkpoint), as ``demo.py restart-processor`` does.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from integration.harness import FAULT_EXIT_CODE, FixtureRun, StepOutcome, api_json, assert_outcome, wait_until

pytestmark = [pytest.mark.integration, pytest.mark.slow]

VP = "vehicle_positions"
FROZEN_MANIFEST = ("status", "generation_id", "source_order", "parent_snapshot_id", "parent_available",
                   "built_as_late", "metrics_as_of",
                   "first_materialized_at", "expires_at", "history_expires_at", "ready_at", "expected_counts",
                   "content_checksum", "first_visible_at", "visibility_latency_ms", "publish_outcome", "first_batch_id")
FROZEN_POINTER = ("snapshot_id", "generation_id", "source_order", "view_published_at", "expires_at", "metrics_as_of")


def _second_vp_step(fr: FixtureRun) -> tuple[str, dict[str, Any]]:
    for step in fr.step_names[1:]:
        cap = fr.first_capture(step, VP)
        if cap is not None:
            return step, dict(cap)
    pytest.fail("the golden timeline has no second vehicle_positions capture")


def _vehicle_ids(harness: Any, run_id: str) -> list[str]:
    status, body = api_json(harness, "vehicles", run_id=run_id, limit=2000)
    assert status == 200, f"/vehicles -> {status} {body.get('error')}"
    return sorted(v["vehicle_id"] for v in (body.get("data") or {}).get("vehicles") or [])


def _crash_on(fr: FixtureRun, point: str, case_record: Any) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
    """Run S1, arm ``point`` for VP2, publish the VP2 step and wait for the exit. Returns (step, sid2, before, info)."""
    h, run_id = fr.h, fr.run.run_id
    first = fr.step_names[0]
    assert_outcome(fr.run_step(first), case_record, label=f"{first} (before the fault)")
    vp1 = str(fr.first_capture(first, VP)["label"])
    step, cap = _second_vp_step(fr)
    sid2 = str(fr.envelope_for(str(cap["label"]), cap)["snapshot_id"])
    before = fr.lineage_state()
    arm = h.arm_fault(run_id, point, {"feed_type": VP, "snapshot_id": sid2})
    for item in fr.items(step):
        fr.apply_item(step, item)
    code = h.wait_processor_exit(420)
    assert code == FAULT_EXIT_CODE, f"processor exit code {code}, expected {FAULT_EXIT_CODE} after {point}; " \
                                    f"log:\n{h.processor_log_tail()[-3000:]}"
    fired = h.fired_faults(run_id)
    assert fired and not arm.exists(), f"fault {point} did not fire one-shot (fired={fired}, armed={arm.exists()})"
    return step, sid2, before, {"vp1": vp1, "fired": [str(p) for p in fired], "exit_code": code}


@pytest.mark.it_case("IT10")
@pytest.mark.ct_case("CT09")
def test_it10_crash_after_partial_write(harness: Any, stack_ctx: Any, case_record: Any) -> None:
    from transitpulse.contracts.ids import generation_id

    fr = FixtureRun.prepare(harness, stack_ctx, "golden", fault_injection=True)
    run_id = fr.run.run_id
    case_record.run_id = run_id
    ids_s1 = None
    step, sid2, before, info = _crash_on(fr, "after_partial_write", case_record)
    ptr = harness.pointer(run_id, VP) or {}
    assert ptr.get("snapshot_id") == fr.labels[info["vp1"]], (
        f"after the crash the API pointer must still be {info['vp1']}, got {fr.label_of(ptr.get('snapshot_id'))}")
    ids_s1 = _vehicle_ids(harness, run_id)
    assert set(ids_s1) == {e["vehicle"]["vehicle"]["id"] for e in fr.envelopes[info["vp1"]]["entities"]
                           if (e.get("vehicle") or {}).get("vehicle", {}).get("id")}, ids_s1
    man = harness.manifest(run_id, VP, sid2)
    assert man is None or man.get("status") != "READY", f"partially written generation is visible: {man}"
    t0 = time.monotonic()
    harness.restart_processor(run_id, fault_injection=True)
    harness.wait_caught_up(run_id, timeout=420)
    harness.wait_pointer(run_id, VP, sid2, timeout=300)
    recovery_s = time.monotonic() - t0
    api, cass = fr.check(step)
    lineage = fr.check_lineage(step, before)
    assert_outcome(StepOutcome(step, [], api, cass, fr.expected_step(step), recovery_s, lineage), case_record,
                   label=f"{step} (after restart)")
    man = harness.manifest(run_id, VP, sid2) or {}
    assert man.get("status") == "READY"
    rows_vs_counts = {}
    for key, n in (man.get("expected_counts") or {}).items():
        if key.startswith("vehicles_by_route_snapshot/"):
            route = key.split("/", 1)[1]
            got = len(harness.vehicle_rows(run_id, sid2, route))
            rows_vs_counts[route] = (n, got)
            assert got == n, f"vehicles_by_route_snapshot/{route}: {got} rows, manifest expected_counts {n}"
    want_gen = generation_id(run_id, VP, sid2, fr.run.pipeline_version)
    ptr = harness.pointer(run_id, VP) or {}
    assert man.get("generation_id") == want_gen == ptr.get("generation_id"), (
        f"generation id changed by the retry: manifest {man.get('generation_id')}, pointer {ptr.get('generation_id')},"
        f" expected {want_gen}")
    case_record.actual(fault=info, vehicles_while_crashed=ids_s1, recovery_seconds=round(recovery_s, 1),
                       rows_vs_expected_counts=rows_vs_counts, generation_id=want_gen)


@pytest.mark.it_case("IT11")
@pytest.mark.ct_case("CT09")
def test_it11_crash_after_cas_before_commit(harness: Any, stack_ctx: Any, case_record: Any) -> None:
    fr = FixtureRun.prepare(harness, stack_ctx, "golden", fault_injection=True)
    run_id = fr.run.run_id
    case_record.run_id = run_id
    step, sid2, before, info = _crash_on(fr, "after_cas_before_commit", case_record)
    ptr_before = harness.pointer(run_id, VP) or {}
    assert ptr_before.get("snapshot_id") == sid2, "the CAS happened before the fault, the pointer must be VP2"
    man_before = harness.manifest(run_id, VP, sid2) or {}
    assert man_before.get("status") == "READY"
    clock = harness.current_clock(run_id)
    hist_before = {v: sorted(r["observation_id"] for r in harness.history_rows(run_id, v, [clock.date()]))
                   for v in ("V1", "V2", "V3")}
    restart_wall = time.time()
    t0 = time.monotonic()
    harness.restart_processor(run_id, fault_injection=True)
    harness.wait_caught_up(run_id, timeout=420)
    recovery_s = time.monotonic() - t0
    ptr_after = harness.pointer(run_id, VP) or {}
    man_after = harness.manifest(run_id, VP, sid2) or {}
    changed_ptr = {k: (ptr_before.get(k), ptr_after.get(k)) for k in FROZEN_POINTER if ptr_before.get(k) != ptr_after.get(k)}
    changed_man = {k: (man_before.get(k), man_after.get(k)) for k in FROZEN_MANIFEST
                   if man_before.get(k) != man_after.get(k)}
    assert not changed_ptr, f"reprocessing changed the pointer: {changed_ptr}"
    assert not changed_man, f"reprocessing changed the READY manifest: {changed_man}"
    hist_after = {v: sorted(r["observation_id"] for r in harness.history_rows(run_id, v, [clock.date()]))
                  for v in ("V1", "V2", "V3")}
    assert hist_after == hist_before, f"history changed on reprocessing: {hist_before} -> {hist_after}"
    prom = harness.paths.metrics_dir / "processor.prom"

    def reused() -> tuple[bool, Any]:
        fresh = prom.is_file() and prom.stat().st_mtime > restart_wall
        val = harness.processor_metric("tp_processor_generation_publish_total", feed=VP, result="reused")
        return bool(fresh and val is not None and val >= 1), {"exported_after_restart": fresh, "value": val}

    metric = wait_until(reused, 120, 'tp_processor_generation_publish_total{result="reused"} >= 1 after restart')
    api, cass = fr.check(step)
    lineage = fr.check_lineage(step, before)
    assert_outcome(StepOutcome(step, [], api, cass, fr.expected_step(step), recovery_s, lineage), case_record,
                   label=f"{step} (after restart)")
    case_record.actual(fault=info, pointer_unchanged=True, manifest_unchanged=True, history_unchanged=True,
                       reused_metric=metric, recovery_seconds=round(recovery_s, 1))
