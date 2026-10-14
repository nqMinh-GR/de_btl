"""Publisher control file protocol (D-E14): atomic request file + state-file ACK."""

from __future__ import annotations

import threading
import time

import pytest

from transitpulse.replay import control as pc


def _fake_publisher(session_dir, channel: pc.FileControlChannel, stop: threading.Event,
                    applied: list[dict]) -> None:
    while not stop.is_set():
        req = channel.poll()
        if req is not None:
            applied.append(req)
            pc.write_publisher_state(session_dir, {
                "state": "PAUSED" if req["command"] == "pause" else "RUNNING",
                "applied_request_id": req["request_id"],
                "clock_sequence": len(applied),
                "source_clock": "2026-10-05T12:00:30.000Z",
                "wall_heartbeat_at": None,
                "items_published": 0,
                "items_total": 3,
                "next_index": 0,
                "last_barrier": None,
            })
        time.sleep(0.01)


def test_send_command_waits_for_the_matching_ack(tmp_path):
    stop = threading.Event()
    applied: list[dict] = []
    channel = pc.FileControlChannel(tmp_path)               # created before any request is written
    worker = threading.Thread(target=_fake_publisher, args=(tmp_path, channel, stop, applied), daemon=True)
    worker.start()
    try:
        state = pc.send_command(tmp_path, "pause", timeout=5.0)
        assert state["state"] == "PAUSED"
        assert state["applied_request_id"] == applied[0]["request_id"]
        state2 = pc.send_command(tmp_path, "advance-clock", {"to": "2026-10-05T12:03:10Z"}, timeout=5.0)
        assert state2["applied_request_id"] == applied[1]["request_id"]
        assert applied[1]["args"] == {"to": "2026-10-05T12:03:10Z"}
        assert applied[1]["requested_at"].endswith("Z")
    finally:
        stop.set()
        worker.join(timeout=2)
    assert len(applied) == 2
    assert pc.read_publisher_state(tmp_path)["clock_sequence"] == 2


def test_send_command_times_out_without_publisher(tmp_path):
    with pytest.raises(pc.ControlTimeout) as info:
        pc.send_command(tmp_path, "resume", timeout=0.2)
    assert info.value.request_id.startswith("rq_")
    request = pc.read_control_request(tmp_path)
    assert request is not None and request["command"] == "resume"


def test_unknown_command_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        pc.send_command(tmp_path, "rewind", timeout=0.1)


def test_channel_ignores_requests_older_than_the_publisher(tmp_path):
    with pytest.raises(pc.ControlTimeout):
        pc.send_command(tmp_path, "finish", timeout=0.05)
    channel = pc.FileControlChannel(tmp_path)               # fresh publisher: stale 'finish' ignored
    assert channel.poll() is None
    resumed = pc.FileControlChannel(tmp_path, last_applied="rq_other")   # restarted publisher: not yet applied
    req = resumed.poll()
    assert req is not None and req["command"] == "finish"
    assert resumed.poll() is None                           # each request is returned once


def test_read_publisher_state_absent_or_corrupt(tmp_path):
    assert pc.read_publisher_state(tmp_path) is None
    (tmp_path / pc.STATE_FILE).write_text("{truncated", encoding="utf-8")
    assert pc.read_publisher_state(tmp_path) is None
