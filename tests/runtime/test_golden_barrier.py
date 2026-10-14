"""Golden-smoke drain must not chase publisher heartbeats written after the barrier."""

from __future__ import annotations

from types import SimpleNamespace

from transitpulse.contracts.topics import CONTROL, VEHICLE_POSITIONS
from transitpulse.runtime import golden_smoke


def test_wait_processed_freezes_the_watermark(monkeypatch) -> None:
    samples = {"n": 0}

    def fake_end(_bootstrap, _topics):
        samples["n"] += 1
        return {VEHICLE_POSITIONS: {0: 3}, CONTROL: {0: samples["n"]}}

    progress = {"doc": None}

    def fake_progress(_paths, _run_id):
        return progress["doc"]

    clock = {"t": 0.0}

    def monotonic():
        return clock["t"]

    def sleep(seconds):
        clock["t"] += seconds
        if samples["n"] >= 1 and progress["doc"] is None:
            progress["doc"] = {"endOffsets": {VEHICLE_POSITIONS: {"0": 3}, CONTROL: {"0": 1}}}

    monkeypatch.setattr(golden_smoke.time, "monotonic", monotonic)
    monkeypatch.setattr(golden_smoke.time, "sleep", sleep)
    monkeypatch.setattr("transitpulse.runtime.kafka_native.end_offsets", fake_end)
    monkeypatch.setattr(golden_smoke, "last_query_progress", fake_progress)

    ctx = SimpleNamespace(
        cfg=SimpleNamespace(kafka=SimpleNamespace(bootstrap_servers="127.0.0.1:9092")),
        paths=None,
    )
    assert golden_smoke.wait_processed(ctx, "run-1", timeout=30) is True
    assert samples["n"] == 1
