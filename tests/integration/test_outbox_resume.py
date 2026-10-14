"""FR03 (design E §7.4, additional integration test): the collector's durable outbox survives a SIGKILL.

The collector first runs with an unreachable bootstrap (``127.0.0.1:1``) so captures stay ``pending``; it is
killed with SIGKILL (owned child pid, identity verified — never pkill), then restarted with the real bootstrap.
The Kafka messages of the run must carry exactly the outbox ``capture_id``/``snapshot_id`` values.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from integration.fake_source import FakeMbtaSource, FakeResponse, empty_doc, golden_vehicles_doc, now_epoch
from integration.harness import create_fault_run, outbox_rows, start_collector, wait_for_outbox, wait_until

pytestmark = [pytest.mark.integration, pytest.mark.slow]

VP = "vehicle_positions"


def test_outbox_resume_after_sigkill(harness: Any, stack_ctx: Any, case_record: Any) -> None:
    from transitpulse.contracts.topics import SNAPSHOT_TOPICS

    run = create_fault_run(harness, stack_ctx, "outbox_resume")
    run_id = run.run_id
    case_record.run_id = run_id
    start = harness.high_watermarks(SNAPSHOT_TOPICS)
    with FakeMbtaSource() as fake:
        t = now_epoch()
        fake.set("trip_updates", FakeResponse.json(empty_doc(t - 1)))
        fake.set("alerts", FakeResponse.json(empty_doc(t - 1)))
        p1 = fake.set(VP, FakeResponse.json(golden_vehicles_doc(t - 2)))
        start_collector(harness, run, fake, extra_env={"TP_KAFKA_BOOTSTRAP": "127.0.0.1:1"})
        try:
            fake.wait_for(VP, served="body", phase=p1, timeout=90)
            wait_for_outbox(harness.paths, run_id,
                            lambda rs: any(r.get("kind") == "snapshot" and r.get("state") == "pending" for r in rs),
                            "a pending VP snapshot row", feed=VP)
            p2 = fake.set(VP, FakeResponse.json(golden_vehicles_doc(t + 3, ages=(5, 15))))
            fake.wait_for(VP, served="body", phase=p2, timeout=90)
            pending = wait_for_outbox(
                harness.paths, run_id,
                lambda rs: sum(1 for r in rs if r.get("kind") == "snapshot" and r.get("state") == "pending") >= 2,
                "two pending VP snapshot rows", feed=VP)
            for feed in ("vehicle_positions", "trip_updates", "alerts"):
                fake.set(feed, FakeResponse.not_modified())
            time.sleep(3.0)
            pid = harness.kill_component_hard("collector", "transitpulse.ingestion")
            wait_until(lambda: (not harness.component_alive("collector"), None), 60, "collector process gone")
            before_kill = [r for r in outbox_rows(harness.paths, run_id) if r.get("kind") == "snapshot"]
            pending_before = {(r["capture_id"], r["snapshot_id"]) for r in before_kill if r.get("state") == "pending"}
            assert pending_before, f"no pending rows survived the kill: {[(r['state'], r['feed_type']) for r in before_kill]}"
            harness.stop_component("collector")
            start_collector(harness, run, fake)
            rows = wait_for_outbox(
                harness.paths, run_id,
                lambda rs: all(r.get("state") == "published" for r in rs
                               if (r.get("capture_id"), r.get("snapshot_id")) in pending_before),
                "pending rows published after restart", timeout=180)
        finally:
            harness.stop_component("collector")
    end = harness.high_watermarks(SNAPSHOT_TOPICS)
    kafka: dict[tuple[str, str], int] = {}
    for (topic, part), high in end.items():
        for m in harness.run_messages(run_id, topic, start.get((topic, part), 0), high, part):
            env = m.get("json") or {}
            headers = {k: (v or b"").decode() for k, v in (m.get("headers") or {}).items()}
            key = (env.get("capture_id") or headers.get("capture_id"), env.get("snapshot_id") or headers.get("snapshot_id"))
            kafka[key] = kafka.get(key, 0) + 1
    outbox_keys = {(r["capture_id"], r["snapshot_id"]) for r in rows if r.get("kind") == "snapshot" and r.get("snapshot_id")}
    missing = pending_before - set(kafka)
    foreign = set(kafka) - outbox_keys
    assert not missing, f"pending outbox rows never reached Kafka: {sorted(missing)}"
    assert not foreign, f"Kafka carries capture/snapshot ids that are not outbox rows: {sorted(foreign)}"
    case_record.actual(killed_pid=pid, pending_before_kill=sorted(pending_before), pending_rows=len(pending),
                       kafka_deliveries={f"{c}|{s}": n for (c, s), n in kafka.items()},
                       duplicates=sum(n - 1 for n in kafka.values() if n > 1))
