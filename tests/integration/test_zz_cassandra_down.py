"""IT12: Cassandra (the required sink) becomes unavailable while a batch is pending.

Disruptive: stops the Cassandra process OWNED by this work directory's supervisor (never anything else) and is
ordered last by ``pytest_collection_modifyitems``. Expected: the processor fails the batch and exits non-zero (no
self-restart, D-PUB-06), the failed batch is planned in ``offsets/`` but absent from ``commits/``, the API reports
``503 database_unavailable`` and ``/health/ready`` 503 while ``/health/live`` stays 200; after Cassandra is back and
the processor is restarted on the same checkpoint, the backlog is processed exactly as in the golden run.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from integration.harness import FixtureRun, StepOutcome, api_json, assert_outcome, wait_until

pytestmark = [pytest.mark.integration, pytest.mark.slow]

VP = "vehicle_positions"


@pytest.mark.it_case("IT12")
def test_it12_cassandra_unavailable(harness: Any, stack_ctx: Any, case_record: Any) -> None:
    from transitpulse.contracts.envelope import parse_iso_utc
    from transitpulse.contracts.topics import CONTROL
    from transitpulse.verify.progress import checkpoint_batches, checkpoint_offsets

    fr = FixtureRun.prepare(harness, stack_ctx, "golden")
    run_id = fr.run.run_id
    case_record.run_id = run_id
    first = fr.step_names[0]
    assert_outcome(fr.run_step(first), case_record, label=f"{first} (before the outage)")
    step = next((s for s in fr.step_names[1:] if fr.first_capture(s, VP) is not None), None)
    assert step is not None, "the golden timeline has no second vehicle_positions capture"
    cap = dict(fr.first_capture(step, VP) or {})
    env2 = fr.envelope_for(str(cap["label"]), cap)
    sid2 = str(env2["snapshot_id"])
    before = fr.lineage_state()
    cfg = harness.cfg
    ckpt = harness.paths.checkpoint_dir(cfg.project.pipeline_version, run_id, cfg.spark.query_name)
    batches_before = checkpoint_batches(ckpt)
    control_high = harness.high_watermarks([CONTROL])
    evidence: dict[str, Any] = {"checkpoint_before": batches_before}

    harness.stop_cassandra()
    outage_t0 = time.monotonic()
    try:
        delivery = None
        for item in fr.items(step):
            if item.get("kind") == "tick":
                harness.publish_clock(fr.run, parse_iso_utc(str(item["at"])), wait_applied=False)
            elif item.get("kind") == "capture":
                at = parse_iso_utc(str(item["at"]))
                cur = harness.current_clock(run_id)
                if cur is None or at > cur:
                    harness.publish_clock(fr.run, at, wait_applied=False)
                delivery = harness.publish_envelope(fr.envelope_for(str(item["label"]), item))
        assert delivery is not None
        code = harness.wait_processor_exit(900)
        assert code is not None and code != 0, f"processor did not fail the batch (exit code {code})"
        evidence["processor_exit_code"] = code
        batches = checkpoint_batches(ckpt)
        uncommitted = sorted(set(batches["offsets"]) - set(batches["commits"]))
        assert uncommitted, f"no planned-but-uncommitted batch in {ckpt}: {batches}"
        planned = checkpoint_offsets(ckpt, uncommitted[-1])
        covers_new = (planned.get((delivery.topic, delivery.partition), 0) > delivery.offset
                      or any(planned.get(tp, 0) > high for tp, high in control_high.items()))
        assert covers_new, f"uncommitted batch {uncommitted[-1]} does not contain the new messages: {planned}"
        evidence.update(checkpoint_after=batches, uncommitted_batch=uncommitted[-1],
                        planned_offsets={f"{t}[{p}]": o for (t, p), o in planned.items()})

        def unavailable() -> tuple[bool, Any]:
            st, body = api_json(harness, "vehicles", run_id=run_id)
            code_ = (body.get("error") or {}).get("code")
            return st == 503 and code_ == "database_unavailable", (st, code_)

        wait_until(unavailable, 180, "API 503 database_unavailable", interval=3.0)
        ready_status, _ = api_json(harness, "health/ready")
        live_status, _ = api_json(harness, "health/live")
        assert ready_status == 503, f"/health/ready must be 503 while Cassandra is down, got {ready_status}"
        assert live_status == 200, f"/health/live must stay 200, got {live_status}"
        evidence["api"] = {"vehicles": 503, "health_ready": ready_status, "health_live": live_status}
    finally:
        harness.start_cassandra()
    evidence["outage_seconds"] = round(time.monotonic() - outage_t0, 1)
    t0 = time.monotonic()
    harness.restart_processor(run_id)
    harness.wait_caught_up(run_id, timeout=600)
    harness.wait_pointer(run_id, VP, sid2, timeout=300)
    recovery_s = time.monotonic() - t0
    api, cass = fr.check(step)
    lineage = fr.check_lineage(step, before)
    assert_outcome(StepOutcome(step, [], api, cass, fr.expected_step(step), recovery_s, lineage), case_record,
                   label=f"{step} (after recovery)")
    final = checkpoint_batches(ckpt)
    assert set(evidence["checkpoint_after"]["offsets"]) <= set(final["commits"]), (
        f"a batch planned during the outage was never committed after recovery: {final}")
    evidence["recovery_seconds"] = round(recovery_s, 1)
    case_record.actual(**evidence)
