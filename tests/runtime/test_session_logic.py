"""Session state derivation, drain barrier (Spark end offsets vs Kafka high watermarks), parameter identity."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from transitpulse.runtime.session import derive_state, extract_end_offsets, offsets_caught_up, params_key

VP = "mbta.vehicle_positions.snapshots.v1"
CTRL = "mbta.control.v1"
TOPICS = {VP, CTRL, "mbta.trip_updates.snapshots.v1", "mbta.alerts.snapshots.v1", "mbta.dlq.v1"}
NOW = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)


def _session(state: str = "RUNNING", deadline_in: int = 300) -> dict:
    return {"session_id": "s-1", "state": state, "deadline_at": (NOW + timedelta(seconds=deadline_in)).isoformat()}


def test_running_paused_failed_exhausted() -> None:
    assert derive_state(_session(), processor_alive=True, input_alive=True, publisher_state={"state": "RUNNING"},
                        now=NOW) == "RUNNING"
    assert derive_state(_session(), processor_alive=True, input_alive=True, publisher_state={"state": "PAUSED"},
                        now=NOW) == "PAUSED"
    assert derive_state(_session(), processor_alive=False, input_alive=True, publisher_state=None, now=NOW) == "FAILED"
    assert derive_state(_session(deadline_in=-10), processor_alive=True, input_alive=False, publisher_state=None,
                        now=NOW) == "BUDGET_EXHAUSTED"
    assert derive_state(_session("STOPPED"), processor_alive=True, input_alive=True, publisher_state=None,
                        now=NOW) == "STOPPED"
    assert derive_state(_session("STARTING"), processor_alive=False, input_alive=False, publisher_state=None,
                        now=NOW) == "STARTING"


def test_extract_end_offsets_from_spark_progress_shapes() -> None:
    nested = {"batchId": 7, "sources": [{"description": "KafkaV2[Subscribe[...]]",
                                         "endOffset": json.dumps({VP: {"0": 12}, CTRL: {"0": 40}})}]}
    assert extract_end_offsets(nested, TOPICS) == {VP: {0: 12}, CTRL: {0: 40}}
    flat = {"batchId": 8, "endOffsets": {VP: {"0": 13}}}
    assert extract_end_offsets(flat, TOPICS) == {VP: {0: 13}}
    listed = {"batchId": 9, "end_offsets": [{"topic": "x"}, {VP: {"0": 14}}]}
    assert extract_end_offsets(listed, TOPICS) == {VP: {0: 14}}


def test_offsets_caught_up() -> None:
    end = {VP: {0: 12}, CTRL: {0: 40}, "mbta.dlq.v1": {0: 0}}
    assert offsets_caught_up({VP: {0: 12}, CTRL: {0: 40}}, end)
    assert not offsets_caught_up({VP: {0: 11}, CTRL: {0: 40}}, end)
    assert not offsets_caught_up({VP: {0: 12}}, end)


def test_params_key_ignores_budget_but_not_inputs() -> None:
    a = {"mode": "synthetic", "seed": 42, "route_ids": ["R_A"], "speed": 1.0}
    assert params_key(a) == params_key({**a, "duration_seconds": 900})
    assert params_key(a) != params_key({**a, "seed": 43})
    assert params_key(a) != params_key({**a, "fault_injection": True})
