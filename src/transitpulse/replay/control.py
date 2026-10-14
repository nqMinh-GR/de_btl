"""Publisher control channel: atomic control file + state-file ACK (D-E14, D-X-06, interfaces §5).

``send_command`` atomically writes ``<session_dir>/publisher_control.json``::

    {"request_id": "...", "command": "pause", "args": {}, "requested_at": "...Z"}

The publisher polls that file (``replay.control_poll_seconds``), applies each ``request_id`` exactly once,
emits the matching control events and writes ``<session_dir>/publisher_state.json``::

    {"state": "RUNNING|PAUSED|STEP_WAIT|FINISHED|FAILED", "applied_request_id": "...",
     "clock_sequence": 7, "source_clock": "...Z", "wall_heartbeat_at": "...Z", "items_published": 3,
     "items_total": 10, "next_index": 3, "last_barrier": "S1", "last_command": {...}, ...}

``send_command`` waits until ``applied_request_id == request_id`` and returns that state.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any

from transitpulse.contracts.envelope import iso_utc, utcnow
from transitpulse.replay.fileio import atomic_write_json

CONTROL_FILE = "publisher_control.json"
STATE_FILE = "publisher_state.json"
PUBLISH_LOG_FILE = "publish_log.jsonl"
INJECTIONS_FILE = "injections.jsonl"

COMMANDS: tuple[str, ...] = ("pause", "resume", "finish", "step", "advance-clock", "inject")
# Valid only for source-clock runs (replay/synthetic); a live run answers exit code 6.
SOURCE_CLOCK_COMMANDS = frozenset({"pause", "resume", "step", "advance-clock", "inject"})
INJECT_SCENARIOS: tuple[str, ...] = ("duplicate_snapshot", "late_snapshot", "invalid_gps")

STATE_RUNNING = "RUNNING"
STATE_PAUSED = "PAUSED"
STATE_STEP_WAIT = "STEP_WAIT"
STATE_FINISHED = "FINISHED"
STATE_FAILED = "FAILED"
TERMINAL_STATES = frozenset({STATE_FINISHED, STATE_FAILED})


class ControlTimeout(TimeoutError):
    """The publisher did not acknowledge the request in time (not running or blocked)."""

    def __init__(self, request_id: str, timeout: float, last_state: dict | None) -> None:
        super().__init__(f"publisher did not acknowledge request {request_id} within {timeout:.1f}s")
        self.request_id = request_id
        self.timeout = timeout
        self.last_state = last_state


def control_path(session_dir: Path) -> Path:
    return Path(session_dir) / CONTROL_FILE


def state_path(session_dir: Path) -> Path:
    return Path(session_dir) / STATE_FILE


def _read_json_object(path: Path) -> dict | None:
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def read_publisher_state(session_dir: Path) -> dict | None:
    """Current publisher state (None when the publisher never wrote one)."""
    return _read_json_object(state_path(session_dir))


def write_publisher_state(session_dir: Path, state: dict[str, Any], *, fsync: bool = True) -> None:
    atomic_write_json(state_path(session_dir), state, fsync=fsync)


def read_control_request(session_dir: Path) -> dict | None:
    """The last control request written by :func:`send_command` (None when absent or unreadable)."""
    req = _read_json_object(control_path(session_dir))
    if req is None or not isinstance(req.get("request_id"), str) or not isinstance(req.get("command"), str):
        return None
    return req


def new_request_id() -> str:
    return "rq_" + uuid.uuid4().hex


def send_command(session_dir: Path, command: str, args: dict | None = None, timeout: float = 5.0) -> dict:
    """Write a control request atomically and wait for the publisher's ACK; returns the ACKed state.

    Raises ``ValueError`` for an unknown command and :class:`ControlTimeout` when no ACK arrives.
    """
    if command not in COMMANDS:
        raise ValueError(f"unknown publisher command {command!r}; expected one of {list(COMMANDS)}")
    session_dir = Path(session_dir)
    request_id = new_request_id()
    payload = {
        "request_id": request_id,
        "command": command,
        "args": dict(args or {}),
        "requested_at": iso_utc(utcnow()),
    }
    atomic_write_json(control_path(session_dir), payload)
    deadline = time.monotonic() + max(0.0, float(timeout))
    last: dict | None = None
    while True:
        last = read_publisher_state(session_dir)
        if last is not None and last.get("applied_request_id") == request_id:
            return last
        if time.monotonic() >= deadline:
            raise ControlTimeout(request_id, timeout, last)
        time.sleep(0.05)


class FileControlChannel:
    """Publisher side of the protocol: returns each new request once."""

    def __init__(self, session_dir: Path, *, ignore_existing: bool = True, last_applied: str | None = None) -> None:
        self.session_dir = Path(session_dir)
        self._last_seen: str | None = last_applied
        if ignore_existing and last_applied is None:
            # A request written before this publisher started belongs to a previous process.
            existing = read_control_request(self.session_dir)
            if existing is not None:
                self._last_seen = existing["request_id"]

    def poll(self) -> dict | None:
        req = read_control_request(self.session_dir)
        if req is None or req["request_id"] == self._last_seen:
            return None
        self._last_seen = req["request_id"]
        return req
