"""Control events on ``mbta.control.v1`` (key ``run_id|control``).

The processor accepts a clock event only if ``clock_sequence`` is strictly greater than the stored
value AND ``source_clock`` is not earlier than the stored clock (LWT on run_metadata). Late or stale
ticks are ignored and logged as ``clock_regression_ignored``. The clock is never derived from
prediction times.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from transitpulse.contracts.envelope import iso_utc, utcnow
from transitpulse.contracts.ids import event_id

SCHEMA_VERSION = 1
MESSAGE_TYPE_CONTROL = "control"

# event types
RUN_STARTED = "run_started"
CLOCK_TICK = "clock_tick"
RUN_PAUSED = "run_paused"
RUN_RESUMED = "run_resumed"
RUN_FINISHED = "run_finished"
PUBLISHER_HEARTBEAT = "publisher_heartbeat"
COLLECTOR_STATUS = "collector_status"
COLLECTOR_HEARTBEAT = "collector_heartbeat"
FAULT_ARMED = "fault_armed"

CLOCK_EVENTS = frozenset({RUN_STARTED, CLOCK_TICK, RUN_PAUSED, RUN_RESUMED, RUN_FINISHED})
ALL_EVENTS = CLOCK_EVENTS | {PUBLISHER_HEARTBEAT, COLLECTOR_STATUS, COLLECTOR_HEARTBEAT, FAULT_ARMED}

# run_state implied by each clock event (processor state machine, PDF p30)
STATE_FOR_EVENT = {
    RUN_STARTED: "RUNNING",
    CLOCK_TICK: None,          # keeps the current state unless the tick carries run_state
    RUN_PAUSED: "PAUSED",
    RUN_RESUMED: "RUNNING",
    RUN_FINISHED: "FINISHED",
}


def _base(event_type: str, run_id: str, source_type: str, component: str, version: str) -> dict[str, Any]:
    if event_type not in ALL_EVENTS:
        raise ValueError(f"unknown control event type {event_type!r}")
    return {
        "schema_version": SCHEMA_VERSION,
        "message_type": MESSAGE_TYPE_CONTROL,
        "event_type": event_type,
        "event_id": event_id(),
        "run_id": run_id,
        "source_type": source_type,
        "emitted_at": iso_utc(utcnow()),
        "producer": {"component": component, "version": version},
    }


def clock_event(
    event_type: str,
    *,
    run_id: str,
    source_type: str,
    clock_sequence: int,
    source_clock: datetime,
    component: str,
    version: str,
    speed: float | None = None,
    run_state: str | None = None,
    reason: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """run_started / clock_tick / run_paused / run_resumed / run_finished."""
    if event_type not in CLOCK_EVENTS:
        raise ValueError(f"{event_type!r} is not a clock event")
    ev = _base(event_type, run_id, source_type, component, version)
    ev.update(
        clock_sequence=int(clock_sequence),
        source_clock=iso_utc(source_clock),
        speed=speed,
        run_state=run_state or STATE_FOR_EVENT.get(event_type),
        reason=reason,
    )
    if extra:
        ev.update(extra)
    return ev


def status_event(
    event_type: str,
    *,
    run_id: str,
    source_type: str,
    component: str,
    version: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    """publisher_heartbeat / collector_status / collector_heartbeat / fault_armed (fire-and-forget)."""
    if event_type in CLOCK_EVENTS:
        raise ValueError("use clock_event() for clock events")
    ev = _base(event_type, run_id, source_type, component, version)
    ev.update(fields)
    return ev
