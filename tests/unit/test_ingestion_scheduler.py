"""Unit tests for the per-feed polling scheduler (ingestion.scheduler)."""

from __future__ import annotations

import threading
import time

import pytest

from transitpulse.ingestion.scheduler import PollScheduler, advance_slot


def test_next_slot_on_time() -> None:
    assert advance_slot(0.0, 30.0, 5.0, None) == (30.0, 0)


def test_overrun_skips_missed_slots_without_bursting() -> None:
    # Poll due at 0 finished at 65 s: slots 30 and 60 are skipped, the next poll is at 90.
    assert advance_slot(0.0, 30.0, 65.0, None) == (90.0, 2)
    # Finishing exactly on the next slot is not a miss.
    assert advance_slot(0.0, 30.0, 30.0, None) == (30.0, 0)


def test_long_retry_after_defers_the_next_poll() -> None:
    assert advance_slot(0.0, 30.0, 5.0, 120.0) == (125.0, 0)
    assert advance_slot(0.0, 30.0, 5.0, 10.0) == (30.0, 0)    # shorter than the interval: ignored


def test_invalid_interval() -> None:
    with pytest.raises(ValueError):
        advance_slot(0.0, 0.0, 1.0, None)
    with pytest.raises(ValueError):
        PollScheduler(["alerts"], {"alerts": 0.0}, lambda f: None, threading.Event())
    with pytest.raises(ValueError):
        PollScheduler([], {}, lambda f: None, threading.Event())


def test_feeds_poll_independently_and_errors_do_not_stop_other_feeds() -> None:
    counts = {"vehicle_positions": 0, "alerts": 0}
    errors: list[str] = []
    lock = threading.Lock()
    stop = threading.Event()

    def task(feed: str) -> float | None:
        with lock:
            counts[feed] += 1
        if feed == "alerts":
            raise RuntimeError("boom")
        return None

    sched = PollScheduler(["vehicle_positions", "alerts"], {"vehicle_positions": 0.05, "alerts": 0.05}, task, stop,
                          on_error=lambda feed, exc: errors.append(feed))
    sched.start()
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        with lock:
            if counts["vehicle_positions"] >= 3 and counts["alerts"] >= 3:
                break
        time.sleep(0.02)
    stop.set()
    assert sched.join(5.0) == []
    assert counts["vehicle_positions"] >= 3 and counts["alerts"] >= 3
    assert errors and set(errors) == {"alerts"}
    with pytest.raises(RuntimeError):
        sched.start()
