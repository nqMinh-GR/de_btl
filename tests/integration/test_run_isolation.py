"""IT14: two synthetic runs replay the SAME golden recording (identical vehicle/trip ids) one after the other through
the real replay publisher; run B is paused.

Asserts: no cross-run rows (each run's generations exist only under its own run_id, pointers differ, run A's views
and history are unchanged by run B); while B is paused its application clock stays equal for >= 10 s while the
publisher's wall heartbeat advances and run_state is PAUSED; resume advances the clock again.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from integration.harness import api_json, golden_static, wait_until

pytestmark = [pytest.mark.integration, pytest.mark.slow]

VP = "vehicle_positions"
FEEDS = ("vehicle_positions", "trip_updates", "alerts")


def _start_publisher(harness: Any, run: Any, manifest: Any, session: str, speed: float) -> Any:
    session_dir = harness.paths.session_dir(session)
    session_dir.mkdir(parents=True, exist_ok=True)
    argv = [str(harness.runtime.core_python), "-m", "transitpulse.replay", "publish", "--workdir",
            str(harness.paths.root), "--run-id", run.run_id, "--manifest", str(manifest), "--session-id", session,
            "--speed", str(speed)]
    state = session_dir / "publisher_state.json"
    harness.start_component("publisher", argv, run_id=run.run_id, env=harness.env, ready=state.exists,
                            ready_timeout=120)
    return session_dir


def _publisher_state(session_dir: Any) -> dict[str, Any]:
    from transitpulse.replay.control import read_publisher_state

    return read_publisher_state(session_dir) or {}


def _snapshot(harness: Any, run: Any) -> dict[str, Any]:
    pointers = {f: (harness.pointer(run.run_id, f) or {}).get("snapshot_id") for f in FEEDS}
    status, body = api_json(harness, f"routes/{run.route_ids[0]}/metrics", run_id=run.run_id)
    data = body.get("data") or {}
    vm = (data.get("vehicle_metrics") or {}).get("counts")
    pm = data.get("prediction_metrics") or {}
    day = harness.current_clock(run.run_id) or run.initial_clock
    history = {v: sorted(r["observation_id"] for r in harness.history_rows(run.run_id, v, [day.date()]))
               for v in ("V1", "V2", "V3")}
    return {"pointers": pointers, "status": status, "vehicle_counts": vm,
            "kpi": {k: pm.get(k) for k in ("snapshot_id", "eligible_trip_count", "predicted_late_trip_count",
                                           "predicted_late_rate", "mean_predicted_delay_seconds")},
            "history": history}


@pytest.mark.it_case("IT14")
def test_it14_run_isolation_and_pause(harness: Any, stack_ctx: Any, case_record: Any) -> None:
    from transitpulse.replay.control import send_command

    rec = stack_ctx.materialize("golden")
    static = golden_static(stack_ctx)
    # ---------------- run A: full replay at speed 20
    run_a = harness.create_run(static=static, source_type="synthetic", initial_clock=rec.start_clock,
                               scenario_id="IT14_run_A", fixture_kind=rec.fixture_kind, recording_id=rec.recording_id,
                               speed=20.0, label=f"{harness.session_id}:IT14:A")
    case_record.run_id = run_a.run_id
    harness.start_processor(run_a.run_id)
    sess_a = _start_publisher(harness, run_a, rec.manifest_path, f"{harness.session_id}-it14a", 20.0)

    def finished(sdir: Any) -> Any:
        def pred() -> tuple[bool, Any]:
            st = _publisher_state(sdir)
            if st.get("state") == "FAILED":
                raise AssertionError(f"publisher failed: {st}")
            return st.get("state") == "FINISHED", st.get("state")
        return pred

    wait_until(finished(sess_a), 600, "publisher of run A FINISHED", interval=2.0)
    harness.wait_caught_up(run_a.run_id, timeout=300)
    harness.stop_component("publisher")
    snap_a = _snapshot(harness, run_a)
    assert all(snap_a["pointers"].values()), f"run A has no generation for some feed: {snap_a['pointers']}"
    harness.stop_processor()

    # ---------------- run B: same recording at speed 1, paused mid-way
    run_b = harness.create_run(static=static, source_type="synthetic", initial_clock=rec.start_clock,
                               scenario_id="IT14_run_B", fixture_kind=rec.fixture_kind, recording_id=rec.recording_id,
                               speed=1.0, label=f"{harness.session_id}:IT14:B")
    harness.start_processor(run_b.run_id)
    sess_b = _start_publisher(harness, run_b, rec.manifest_path, f"{harness.session_id}-it14b", 1.0)
    pause: dict[str, Any] = {}
    try:
        wait_until(lambda: ((harness.pointer(run_b.run_id, VP) or {}).get("snapshot_id") is not None, None), 300,
                   "first vehicle generation of run B")
        ack = send_command(sess_b, "pause", timeout=30)
        assert ack.get("state") == "PAUSED", f"publisher did not acknowledge pause: {ack}"

        def run_paused() -> tuple[bool, Any]:
            row = harness.run_row(run_b.run_id) or {}
            return row.get("run_state") == "PAUSED", row.get("run_state")

        wait_until(run_paused, 120, "run_metadata.run_state == PAUSED for run B")
        harness.wait_caught_up(run_b.run_id, timeout=120)
        row1 = harness.run_row(run_b.run_id) or {}
        hb1 = _publisher_state(sess_b).get("wall_heartbeat_at")
        time.sleep(12.0)
        harness.wait_caught_up(run_b.run_id, timeout=120)
        row2 = harness.run_row(run_b.run_id) or {}
        st2 = _publisher_state(sess_b)
        hb2 = st2.get("wall_heartbeat_at")
        assert row1.get("application_clock") == row2.get("application_clock"), (
            f"clock moved while paused: {row1.get('application_clock')} -> {row2.get('application_clock')}")
        assert hb1 and hb2 and hb2 > hb1, f"publisher wall heartbeat did not advance while paused: {hb1} -> {hb2}"
        assert st2.get("state") == "PAUSED" and row2.get("run_state") == "PAUSED"
        status, body = api_json(harness, f"runs/{run_b.run_id}")
        api_state = (body.get("meta") or {}).get("run_state") or (body.get("data") or {}).get("run_state")
        assert status == 200 and api_state == "PAUSED", f"API run state {status} {api_state}"
        pause = {"clock": row1.get("application_clock"), "heartbeat_before": hb1, "heartbeat_after": hb2,
                 "api_run_state": api_state}
        send_command(sess_b, "resume", timeout=30)

        def advanced() -> tuple[bool, Any]:
            cur = (harness.run_row(run_b.run_id) or {}).get("application_clock")
            return cur is not None and cur > row2["application_clock"], cur

        pause["clock_after_resume"] = wait_until(advanced, 120, "run B clock advancing after resume")
        send_command(sess_b, "finish", timeout=30)
        wait_until(finished(sess_b), 180, "publisher of run B FINISHED", interval=2.0)
        harness.wait_caught_up(run_b.run_id, timeout=300)
    finally:
        harness.stop_component("publisher")
    snap_b = _snapshot(harness, run_b)

    # ---------------- isolation
    snap_a_after = _snapshot(harness, run_a)
    assert snap_a_after == snap_a, f"run A changed while run B was processed: {snap_a} -> {snap_a_after}"
    shared = {f for f in FEEDS if snap_a["pointers"][f] and snap_a["pointers"][f] == snap_b["pointers"][f]}
    assert not shared, f"runs A and B share snapshot ids for {shared}"
    leaks = {}
    for route in run_a.route_ids:
        for own, other in ((run_a, snap_b), (run_b, snap_a)):
            sid = other["pointers"][VP]
            if sid:
                n = len(harness.vehicle_rows(own.run_id, sid, route))
                if n:
                    leaks[f"{own.run_id[:8]}/{sid[:12]}/{route}"] = n
    assert not leaks, f"cross-run serving rows found: {leaks}"
    ids_a = {r["vehicle_id"] for r in harness.vehicle_rows(run_a.run_id, snap_a["pointers"][VP], run_a.route_ids[0])}
    for run, snap in ((run_a, snap_a), (run_b, snap_b)):
        status, body = api_json(harness, "vehicles", run_id=run.run_id, limit=2000)
        served = (((body.get("meta") or {}).get("feeds") or {}).get(VP) or {}).get("snapshot_id")
        assert status == 200 and served == snap["pointers"][VP], f"API serves {served} for run {run.run_id}"
    case_record.actual(run_a=run_a.run_id, run_b=run_b.run_id, pointers_a=snap_a["pointers"],
                       pointers_b=snap_b["pointers"], identical_vehicle_ids=sorted(ids_a), pause=pause)
