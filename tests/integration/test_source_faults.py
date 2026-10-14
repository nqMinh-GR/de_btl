"""IT05: valid empty snapshot vs malformed response vs timeout, through the REAL collector and a fake source.

The run is ``source_type=synthetic`` with ``scenario_id="simulated_source_fault:IT05"`` (D-E28: a fake server is never
presented as MBTA); the collector drives the run clock with its wall-clock control events. Phases are switched
explicitly: valid (2 vehicles) -> malformed JSON -> timeout -> valid empty.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

import pytest

from integration.fake_source import FakeMbtaSource, FakeResponse, empty_doc, golden_vehicles_doc, now_epoch
from integration.harness import (
    api_json,
    capture_file_count,
    create_fault_run,
    raw_capture_shas,
    start_collector,
    wait_for_outbox,
    wait_until,
)

pytestmark = [pytest.mark.integration, pytest.mark.slow]

VP = "vehicle_positions"
MALFORMED = b'{"header": {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET", "timestamp": '


def _vehicles(harness: Any, run_id: str) -> tuple[int, dict[str, Any]]:
    return api_json(harness, "vehicles", run_id=run_id, limit=2000)


def _warning_codes(body: dict[str, Any]) -> list[str]:
    return sorted({str(w.get("code")) for w in body.get("warnings") or [] if isinstance(w, dict)})


@pytest.mark.it_case("IT05")
def test_it05_empty_vs_malformed_vs_timeout(harness: Any, stack_ctx: Any, case_record: Any) -> None:
    run = create_fault_run(harness, stack_ctx, "IT05")
    run_id = run.run_id
    case_record.run_id = run_id
    case_record.expected(
        after_malformed="same pointer, 200 with the previous vehicles, outbox row rejected INVALID_JSON, raw kept",
        after_timeout="same pointer, 200 with the previous vehicles, SOURCE_FETCH_FAILED warning, no raw capture",
        after_valid_empty="new pointer, 200 with [] and feed state empty")
    harness.start_processor(run_id)
    phases: dict[str, Any] = {}
    with FakeMbtaSource() as fake:
        t = now_epoch()
        fake.set("trip_updates", FakeResponse.json(empty_doc(t - 1)))
        fake.set("alerts", FakeResponse.json(empty_doc(t - 1)))
        p1 = fake.set(VP, FakeResponse.json(golden_vehicles_doc(t - 1)))
        start_collector(harness, run, fake)
        try:
            fake.wait_for(VP, served="body", phase=p1, timeout=90)
            ptr1 = wait_until(lambda: ((harness.pointer(run_id, VP) or {}).get("snapshot_id") is not None,
                                       (harness.pointer(run_id, VP) or {}).get("snapshot_id")),
                              300, "first vehicle generation from the collector")
            status, body = _vehicles(harness, run_id)
            ids = sorted(v["vehicle_id"] for v in (body.get("data") or {}).get("vehicles") or [])
            assert status == 200 and ids == ["V1", "V2"], f"valid phase: {status} {ids}"
            phases["valid"] = {"pointer": ptr1, "vehicles": ids}

            # ---- malformed JSON (persistent): rejected at transport level, raw kept, view unchanged
            p2 = fake.set(VP, FakeResponse.malformed(MALFORMED))
            fake.wait_for(VP, served="malformed", phase=p2, completed=True, timeout=90)
            rows = wait_for_outbox(
                harness.paths, run_id,
                lambda rs: any(r.get("kind") == "snapshot" and r.get("state") == "rejected"
                               and r.get("reason") == "INVALID_JSON" for r in rs),
                "outbox row rejected with INVALID_JSON", feed=VP)
            assert hashlib.sha256(MALFORMED).hexdigest() in raw_capture_shas(harness.paths, run_id, VP), (
                "the malformed body was not archived (raw must be kept for rejected captures)")
            harness.wait_caught_up(run_id)
            status, body = _vehicles(harness, run_id)
            ptr = (harness.pointer(run_id, VP) or {}).get("snapshot_id")
            ids = sorted(v["vehicle_id"] for v in (body.get("data") or {}).get("vehicles") or [])
            assert ptr == ptr1, "a malformed response moved the pointer"
            assert status == 200 and ids == ["V1", "V2"], f"after malformed: {status} {ids}"
            phases["malformed"] = {"pointer_unchanged": True, "warnings": _warning_codes(body),
                                   "outbox_rejected": [(r.get("capture_id"), r.get("reason")) for r in rows
                                                       if r.get("state") == "rejected"][-3:]}

            # ---- timeout (persistent): no capture at all, source warning, view unchanged
            p3 = fake.set(VP, FakeResponse.timeout(delay_seconds=6.0))
            fake.wait_for(VP, served="timeout", phase=p3, timeout=90)
            time.sleep(4.0)          # captures of requests that started before the switch have landed
            count_before = capture_file_count(harness.paths, run_id, VP)
            fake.wait_for(VP, served="timeout", phase=p3, n=3, timeout=120)
            time.sleep(3.0)
            count_after = capture_file_count(harness.paths, run_id, VP)
            assert count_after == count_before, f"a timed-out fetch created a raw capture ({count_before} -> {count_after})"

            def warned() -> tuple[bool, Any]:
                st, b = _vehicles(harness, run_id)
                return st == 200 and "SOURCE_FETCH_FAILED" in _warning_codes(b), (st, _warning_codes(b))

            wait_until(warned, 180, "SOURCE_FETCH_FAILED warning on /vehicles", interval=3.0)
            status, body = _vehicles(harness, run_id)
            ids = sorted(v["vehicle_id"] for v in (body.get("data") or {}).get("vehicles") or [])
            assert (harness.pointer(run_id, VP) or {}).get("snapshot_id") == ptr1, "a timeout moved the pointer"
            assert ids == ["V1", "V2"], f"after timeout the previous vehicles must still be served: {ids}"
            phases["timeout"] = {"raw_captures_unchanged": count_before, "warnings": _warning_codes(body)}

            # ---- valid empty: a real (empty) generation replaces the view
            t4 = now_epoch()
            p4 = fake.set(VP, FakeResponse.json(empty_doc(t4 - 1)))
            fake.wait_for(VP, served="body", phase=p4, timeout=90)
            ptr2 = wait_until(lambda: ((harness.pointer(run_id, VP) or {}).get("snapshot_id") not in (None, ptr1),
                                       (harness.pointer(run_id, VP) or {}).get("snapshot_id")),
                              300, "pointer moved to the valid empty generation")
            status, body = _vehicles(harness, run_id)
            data = body.get("data") or {}
            state = ((body.get("meta") or {}).get("feeds") or {}).get(VP, {}).get("state")
            assert status == 200, f"valid empty snapshot must be served with 200, got {status} {body.get('error')}"
            assert data.get("vehicles") == [], f"valid empty snapshot must serve [], got {data.get('vehicles')}"
            assert state == "empty", f"feed state must be 'empty', got {state}"
            phases["valid_empty"] = {"pointer": ptr2, "state": state}
        finally:
            harness.stop_component("collector")
    case_record.actual(phases=phases, requests=[(r.served, r.phase) for r in fake.requests(VP)][-40:])
