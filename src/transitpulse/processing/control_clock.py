"""Run clock and run state from control events (design B §1/§4, decisions D-E13, PDF p30).

Per micro-batch, BEFORE any snapshot is processed:
  1. valid clock events (``run_started``, ``clock_tick``, ``run_paused``, ``run_resumed``, ``run_finished``)
     are collected newest-first with a hard bound (``spark.max_control_events_per_batch``) and folded in
     ``clock_sequence`` order on top of the stored ``run_metadata`` row (read at SERIAL);
  2. ONE lightweight transaction writes the folded result:
       source clock: ``IF clock_sequence < ? AND application_clock <= ?``
       wall clock  : ``IF clock_sequence < ?``  (``application_clock`` stays NULL for live runs)
     A non-applied LWT means the stored state is already newer (e.g. replay of the same batch) and is
     logged, never treated as an error;
  3. collector / publisher status events become plain upserts of ``pipeline_status`` rows
     ``collector:<feed_type>``, ``collector`` and ``publisher`` (never LWT rows);
  4. :func:`read_run_info` reads the run back at SERIAL for the snapshot work of the batch.

Folding rules: an event is applied only when its ``clock_sequence`` is strictly greater than the current
one and (source mode) its ``source_clock`` is not earlier than the current clock; otherwise it is ignored
and logged (``clock_regression_ignored`` / ``stale_sequence``). State machine:
CREATED -> RUNNING | PAUSED | FINISHED | FAILED; RUNNING <-> PAUSED; RUNNING/PAUSED -> FINISHED | FAILED;
FAILED -> RUNNING (restart of the same run); FINISHED is terminal (later clock events are ignored).
Assumption: every clock event (including pause/resume) carries a strictly increasing ``clock_sequence``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from transitpulse.contracts.control import (
    COLLECTOR_HEARTBEAT,
    COLLECTOR_STATUS,
    PUBLISHER_HEARTBEAT,
    RUN_FINISHED,
    RUN_PAUSED,
    RUN_RESUMED,
    RUN_STARTED,
)
from transitpulse.contracts.enums import RunState
from transitpulse.contracts.policy import Policy
from transitpulse.obs.logging import get_logger, log_event
from transitpulse.processing.context import RunInfo
from transitpulse.storage.cassandra import execute, normalize_row, to_utc, was_applied

_log = get_logger("transitpulse.processing.control_clock")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

CREATED, RUNNING, PAUSED, FINISHED, FAILED = (str(s) for s in (
    RunState.CREATED, RunState.RUNNING, RunState.PAUSED, RunState.FINISHED, RunState.FAILED))
ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    CREATED: frozenset({RUNNING, PAUSED, FINISHED, FAILED}),
    RUNNING: frozenset({RUNNING, PAUSED, FINISHED, FAILED}),
    PAUSED: frozenset({PAUSED, RUNNING, FINISHED, FAILED}),
    FAILED: frozenset({FAILED, RUNNING, FINISHED}),
    FINISHED: frozenset({FINISHED}),
}
_STATES = frozenset(ALLOWED_TRANSITIONS)

SELECT_RUN = "SELECT * FROM run_metadata WHERE run_id = ?"
UPDATE_CLOCK_SOURCE = (
    "UPDATE run_metadata SET application_clock = ?, clock_sequence = ?, run_state = ?, clock_updated_at = ?, "
    "state_reason = ? WHERE run_id = ? IF clock_sequence < ? AND application_clock <= ?"
)
UPDATE_CLOCK_WALL = (
    "UPDATE run_metadata SET clock_sequence = ?, run_state = ?, clock_updated_at = ?, state_reason = ? "
    "WHERE run_id = ? IF clock_sequence < ?"
)
UPDATE_STATE_IF = (
    "UPDATE run_metadata SET run_state = ?, clock_updated_at = ?, state_reason = ? WHERE run_id = ? IF run_state = ?"
)
STATUS_UPSERT_COLUMNS: tuple[str, ...] = (
    "run_id", "component", "status", "last_heartbeat_at", "last_success_at", "last_rejected_at",
    "last_rejected_reason", "progress_json", "last_error",
)
UPSERT_STATUS = (
    f"INSERT INTO pipeline_status ({', '.join(STATUS_UPSERT_COLUMNS)}) "
    f"VALUES ({', '.join('?' for _ in STATUS_UPSERT_COLUMNS)})"
)


class RunNotFound(RuntimeError):
    """run_metadata has no row for the run (create the run before starting the processor)."""


@dataclass(frozen=True)
class ClockEvent:
    event_type: str
    clock_sequence: int
    source_clock: datetime | None
    run_state: str | None = None
    reason: str | None = None
    event_id: str | None = None
    kafka_offset: int | None = None


@dataclass(frozen=True)
class ClockState:
    clock_sequence: int
    application_clock: datetime | None
    run_state: str
    state_reason: str | None = None


@dataclass(frozen=True)
class FoldResult:
    state: ClockState
    changed: bool
    applied: tuple[ClockEvent, ...] = ()
    ignored: tuple[tuple[ClockEvent, str], ...] = ()


@dataclass
class ControlResult:
    events_seen: int = 0
    events_truncated: bool = False
    applied_events: int = 0
    ignored_events: int = 0
    lwt_applied: bool | None = None
    state: ClockState | None = None
    status_rows: int = 0
    notes: list[str] = field(default_factory=list)


def ms_to_dt(ms: int | None) -> datetime | None:
    if ms is None:
        return None
    return _EPOCH + timedelta(milliseconds=int(ms))


# --------------------------------------------------------------------------------------------------
# Pure folding
# --------------------------------------------------------------------------------------------------

def target_state(current: str, ev: ClockEvent) -> str:
    if ev.event_type == RUN_FINISHED:
        return FAILED if ev.run_state == FAILED else FINISHED
    if ev.event_type == RUN_PAUSED:
        return PAUSED
    if ev.event_type in (RUN_STARTED, RUN_RESUMED):
        return RUNNING
    # clock_tick: carries the publisher's state; a bare tick on a CREATED run means it is running
    if ev.run_state in _STATES:
        return str(ev.run_state)
    return RUNNING if current == CREATED else current


def fold_clock_events(state: ClockState, events: Iterable[ClockEvent], *, clock_mode: str) -> FoldResult:
    """Fold clock events (ascending ``clock_sequence``) on top of ``state`` (pure; unit-tested)."""
    if clock_mode not in ("source", "wall"):
        raise ValueError(f"unknown clock_mode {clock_mode!r}")
    cur = state
    applied: list[ClockEvent] = []
    ignored: list[tuple[ClockEvent, str]] = []
    for ev in sorted(events, key=lambda e: (e.clock_sequence, e.kafka_offset if e.kafka_offset is not None else -1)):
        if ev.clock_sequence <= cur.clock_sequence:
            ignored.append((ev, "stale_sequence"))
            continue
        new_clock = cur.application_clock
        if clock_mode == "source":
            if ev.source_clock is None:
                ignored.append((ev, "missing_source_clock"))
                continue
            if cur.application_clock is not None and ev.source_clock < cur.application_clock:
                ignored.append((ev, "clock_regression_ignored"))
                continue
            new_clock = ev.source_clock
        nxt = target_state(cur.run_state, ev)
        if nxt not in ALLOWED_TRANSITIONS.get(cur.run_state, frozenset()):
            ignored.append((ev, f"invalid_transition:{cur.run_state}->{nxt}"))
            continue
        reason = ev.reason or ev.event_type
        cur = ClockState(int(ev.clock_sequence), new_clock, nxt, reason)
        applied.append(ev)
    return FoldResult(cur, cur != state, tuple(applied), tuple(ignored))


def events_from_rows(rows: Sequence[Any]) -> list[ClockEvent]:
    """Rows of ``parse.clock_event_frame`` (``source_clock_ms`` epoch-ms) -> ClockEvent."""
    out: list[ClockEvent] = []
    for r in rows:
        d = r.asDict() if hasattr(r, "asDict") else dict(r)
        out.append(ClockEvent(
            event_type=d["event_type"], clock_sequence=int(d["clock_sequence"]),
            source_clock=ms_to_dt(d.get("source_clock_ms")), run_state=d.get("run_state"),
            reason=d.get("reason"), event_id=d.get("event_id"), kafka_offset=d.get("kafka_offset"),
        ))
    return out


# --------------------------------------------------------------------------------------------------
# Cassandra side
# --------------------------------------------------------------------------------------------------

def read_run_row(session, run_id: str) -> dict[str, Any]:
    row = normalize_row(execute(session, SELECT_RUN, (run_id,), serial=True).one())
    if row is None:
        raise RunNotFound(f"run_metadata has no row for run_id={run_id}; create the run first")
    return row


def read_run_info(session, run_id: str) -> RunInfo:
    """``run_metadata`` (SERIAL) as :class:`RunInfo` (after this batch's control events)."""
    row = read_run_row(session, run_id)
    clock_mode = row.get("clock_mode") or "source"
    policy_json = row.get("policy_json")
    if not policy_json:
        raise RuntimeError(f"run_metadata.policy_json is empty for run {run_id}")
    if row.get("clock_sequence") is None:
        raise RuntimeError(f"run_metadata.clock_sequence is NULL for run {run_id}; run bootstrap is incomplete")
    app_clock = to_utc(row.get("application_clock")) if clock_mode == "source" else None
    if clock_mode == "source" and app_clock is None:
        raise RuntimeError(f"source-clock run {run_id} has no application_clock (initial_clock missing)")
    return RunInfo(
        run_id=row["run_id"],
        project_id=row.get("project_id") or "",
        scope_id=row.get("scope_id") or "",
        source_type=row.get("source_type") or "",
        clock_mode=clock_mode,
        gtfs_version=row.get("gtfs_version") or "",
        agency_timezone=row.get("agency_timezone") or "",
        route_ids=tuple(row.get("route_ids") or ()),
        pipeline_version=row.get("pipeline_version") or "",
        policy=Policy.from_json(policy_json),
        application_clock=app_clock,
        clock_sequence=int(row["clock_sequence"]),
        run_state=row.get("run_state") or CREATED,
        scenario_id=row.get("scenario_id"),
    )


def _state_of(row: dict[str, Any]) -> ClockState:
    if row.get("clock_sequence") is None:
        raise RuntimeError(f"run_metadata.clock_sequence is NULL for run {row.get('run_id')}")
    return ClockState(int(row["clock_sequence"]), to_utc(row.get("application_clock")),
                      row.get("run_state") or CREATED, row.get("state_reason"))


def write_clock(session, run_id: str, old: ClockState, new: ClockState, *, clock_mode: str, now: datetime) -> bool:
    """The single per-batch clock LWT. Returns ``was_applied``."""
    if clock_mode == "source":
        params = (new.application_clock, new.clock_sequence, new.run_state, now, new.state_reason, run_id,
                  new.clock_sequence, new.application_clock)
        return was_applied(execute(session, UPDATE_CLOCK_SOURCE, params))
    params = (new.clock_sequence, new.run_state, now, new.state_reason, run_id, new.clock_sequence)
    return was_applied(execute(session, UPDATE_CLOCK_WALL, params))


def mark_running_on_first_data(session, run_id: str, *, now: datetime) -> bool:
    """Wall-clock (live) runs have no publisher ``run_started``: CREATED -> RUNNING when data arrives."""
    return was_applied(execute(session, UPDATE_STATE_IF, (RUNNING, now, "first_data", run_id, CREATED)))


CLEAR_KEY = "_clear"   # row key: columns bound as explicit NULL (clears a stale value) instead of UNSET
# Collector/publisher outcomes that are NOT failures; any other non-null outcome (http_error, connect_error,
# too_large, timeout, collector_error, disk_budget_exceeded, ...) is a failure.
NON_FAILURE_OUTCOMES: frozenset[str] = frozenset({"ok", "not_modified", "rejected", "disk_budget_warning"})
SUCCESS_OUTCOMES: frozenset[str] = frozenset({"ok", "not_modified"})
CLEARED_ON_SUCCESS: tuple[str, ...] = ("last_error", "last_rejected_at", "last_rejected_reason")
CLEARED_ON_REJECTED: tuple[str, ...] = ("last_error",)


def status_params(row: dict[str, Any]) -> list[Any]:
    """Bind values: ``None`` -> UNSET (column untouched) except columns listed in ``row['_clear']`` -> NULL."""
    from cassandra.query import UNSET_VALUE

    clear = set(row.get(CLEAR_KEY) or ())
    out: list[Any] = []
    for c in STATUS_UPSERT_COLUMNS:
        v = row.get(c)
        out.append(None if c in clear else (UNSET_VALUE if v is None else v))
    return out


def upsert_status(session, row: dict[str, Any]) -> None:
    """Plain upsert of a NON-LWT pipeline_status row (collector:*, collector, publisher, heartbeat)."""
    comp = str(row.get("component") or "")
    if comp == "processor" or comp.startswith("feed:"):
        raise ValueError(f"pipeline_status component {comp!r} is LWT-only")
    execute(session, UPSERT_STATUS, status_params(row), idempotent=True)


def status_rows_from_events(run_id: str, rows: Sequence[Any]) -> list[dict[str, Any]]:
    """Latest collector/publisher status events -> pipeline_status rows (pure).

    * failure: any non-null outcome outside :data:`NON_FAILURE_OUTCOMES`; ``last_error = reason_code or
      outcome``;
    * rejected (fetch ok, body rejected): ``last_rejected_at`` / ``last_rejected_reason`` and an explicit
      NULL ``last_error`` (clears a stale fetch error);
    * success (``ok`` / ``not_modified``): ``last_error``, ``last_rejected_at`` and ``last_rejected_reason``
      are written as explicit NULLs so stale errors are cleared.
    """
    out: list[dict[str, Any]] = []
    for r in rows:
        d = r.asDict() if hasattr(r, "asDict") else dict(r)
        et = d.get("event_type")
        emitted = ms_to_dt(d.get("emitted_at_ms"))
        if et == COLLECTOR_STATUS:
            if not d.get("feed_type"):
                continue
            component = f"collector:{d['feed_type']}"
            status = d.get("feed_state") or d.get("outcome")
        elif et == COLLECTOR_HEARTBEAT:
            component, status = "collector", "HEARTBEAT"
        elif et == PUBLISHER_HEARTBEAT:
            component, status = "publisher", d.get("run_state") or "HEARTBEAT"
        else:
            continue
        outcome = (d.get("outcome") or "").strip().lower() or None
        rejected = outcome == "rejected"
        failed = outcome is not None and outcome not in NON_FAILURE_OUTCOMES
        succeeded = outcome in SUCCESS_OUTCOMES
        progress = d.get("progress_text")
        row: dict[str, Any] = {
            "run_id": run_id,
            "component": component,
            "status": status,
            "last_heartbeat_at": emitted,
            "last_success_at": ms_to_dt(d.get("last_success_fetched_at_ms")) or (emitted if succeeded else None),
            "last_rejected_at": emitted if rejected else None,
            "last_rejected_reason": d.get("reason_code") if rejected else None,
            "progress_json": progress if progress else json.dumps({"event_type": et}),
            "last_error": (d.get("reason_code") or d.get("outcome")) if failed else None,
        }
        if succeeded:
            row[CLEAR_KEY] = CLEARED_ON_SUCCESS
        elif rejected:
            row[CLEAR_KEY] = CLEARED_ON_REJECTED      # the fetch itself worked: drop a stale fetch error
        out.append(row)
    return out


def apply_control(
    session,
    run_id: str,
    *,
    clock_mode: str,
    clock_rows: Sequence[Any],
    truncated: bool,
    status_rows: Sequence[Any],
    now: datetime,
    batch_id: int,
) -> ControlResult:
    """Fold + single LWT + status upserts for one micro-batch."""
    res = ControlResult(events_seen=len(clock_rows), events_truncated=truncated)
    if truncated:
        log_event(_log, "control", "skipped", "more clock events than the per-batch bound; only the newest are "
                  "folded (older ones are superseded by higher clock_sequence values)", level=logging.WARNING,
                  batch_id=batch_id, kept=len(clock_rows))
    if clock_rows:
        row = read_run_row(session, run_id)
        old = _state_of(row)
        fold = fold_clock_events(old, events_from_rows(clock_rows), clock_mode=clock_mode)
        res.applied_events, res.ignored_events = len(fold.applied), len(fold.ignored)
        for ev, why in fold.ignored:
            log_event(_log, "control", "skipped", why, level=logging.INFO, batch_id=batch_id,
                      event_type=ev.event_type, clock_sequence=ev.clock_sequence, stored_sequence=old.clock_sequence)
        res.state = fold.state
        if fold.changed:
            applied = write_clock(session, run_id, old, fold.state, clock_mode=clock_mode, now=now)
            res.lwt_applied = applied
            if applied:
                log_event(_log, "control", "ok", "run clock advanced", batch_id=batch_id,
                          clock_sequence=fold.state.clock_sequence, run_state=fold.state.run_state,
                          application_clock=str(fold.state.application_clock))
            else:
                log_event(_log, "control", "skipped", "clock LWT not applied (stored state already newer)",
                          batch_id=batch_id, clock_sequence=fold.state.clock_sequence)
    rows = status_rows_from_events(run_id, status_rows)
    for r in rows:
        upsert_status(session, r)
    res.status_rows = len(rows)
    return res
