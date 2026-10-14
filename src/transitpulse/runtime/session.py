"""Demo session records and shared helpers for the demo/infra CLIs.

``W/sessions/<sid>/session.json``::

    {session_id, run_id, mode, source_type, started_at, deadline_at, duration_seconds, recording_manifest,
     speed, route_ids, state: STARTING|RUNNING|STOPPED|FAILED, params, scope, components, ...}

The registry key ``current_session`` points at the active session. The derived state reported by
``demo.py status`` adds ``PAUSED`` (publisher paused) and ``BUDGET_EXHAUSTED`` (deadline passed and the
input component has finished).
"""

from __future__ import annotations

import json
import secrets
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.util import atomic_write_json, parse_iso, read_json_or_none, tail_lines, utc_now

MODES = ("synthetic", "replay", "live")
ACTIVE_STATES = ("STARTING", "RUNNING")
LOG_FILES: dict[str, dict[str, str]] = {
    "kafka": {"stdout": "stdout.log", "server": "server.log", "controller": "controller.log", "gc": "kafkaServer-gc.log"},
    "cassandra": {"stdout": "stdout.log", "system": "system.log", "debug": "debug.log", "gc": "gc.log"},
    "processor": {"stdout": "stdout.log"},
    "publisher": {"stdout": "stdout.log"},
    "collector": {"stdout": "stdout.log"},
    "api": {"stdout": "stdout.log"},
    "dashboard": {"stdout": "stdout.log"},
    "tunnel": {"stdout": "stdout.log"},
    "tunnel-api": {"stdout": "stdout.log"},
    "bootstrap": {"stdout": "uv.log", "uv": "uv.log", "spark_probe": "spark_probe.log"},
}


def new_session_id(prefix: str = "s") -> str:
    return f"{prefix}-{utc_now().strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(2)}"


def session_json_path(paths: WorkPaths, session_id: str) -> Path:
    return paths.session_dir(session_id) / "session.json"


def read_session(paths: WorkPaths, session_id: str | None) -> dict[str, Any] | None:
    if not session_id:
        return None
    data = read_json_or_none(session_json_path(paths, session_id))
    return data if isinstance(data, dict) else None


def write_session(paths: WorkPaths, session: Mapping[str, Any]) -> None:
    atomic_write_json(session_json_path(paths, str(session["session_id"])), dict(session))


def update_session(paths: WorkPaths, session_id: str, **fields: Any) -> dict[str, Any]:
    data = read_session(paths, session_id) or {"session_id": session_id}
    data.update(fields)
    write_session(paths, data)
    return data


def list_sessions(paths: WorkPaths) -> list[dict[str, Any]]:
    root = paths.root / "sessions"
    out: list[dict[str, Any]] = []
    if not root.is_dir():
        return out
    for d in sorted(root.iterdir()):
        data = read_json_or_none(d / "session.json")
        if isinstance(data, dict):
            out.append(data)
    out.sort(key=lambda s: str(s.get("started_at") or ""))
    return out


def params_key(params: Mapping[str, Any]) -> str:
    """Stable identity of the user-visible start parameters (duration excluded: it is a budget)."""
    import hashlib

    keep = {k: params.get(k) for k in ("mode", "seed", "recording_manifest", "static_zip", "speed", "route_ids",
                                       "fault_injection")}
    return hashlib.sha256(json.dumps(keep, sort_keys=True, default=str).encode()).hexdigest()[:16]


def derive_state(session: Mapping[str, Any], *, processor_alive: bool, input_alive: bool,
                 publisher_state: Mapping[str, Any] | None, now: datetime | None = None) -> str:
    """RUNNING | PAUSED | BUDGET_EXHAUSTED | STOPPED | FAILED | STARTING."""
    state = str(session.get("state") or "")
    if state in ("STOPPED", "FAILED"):
        return state
    now = now or utc_now()
    deadline = session.get("deadline_at")
    past_deadline = False
    if deadline:
        try:
            past_deadline = now >= parse_iso(str(deadline))
        except ValueError:
            past_deadline = False
    if past_deadline and not input_alive:
        return "BUDGET_EXHAUSTED"
    if state == "STARTING":
        return "STARTING"
    if not processor_alive:
        return "FAILED"
    if publisher_state and str(publisher_state.get("state")) == "PAUSED":
        return "PAUSED"
    if publisher_state and str(publisher_state.get("state")) == "FINISHED" and not input_alive:
        return "RUNNING"
    return "RUNNING"


def log_path(paths: WorkPaths, component: str, file: str = "stdout") -> Path:
    files = LOG_FILES.get(component)
    if files is None:
        raise ValueError(f"unknown component {component!r}")
    name = files.get(file)
    if name is None:
        raise ValueError(f"unknown log file {file!r} for {component}; choose one of {sorted(files)}")
    return paths.log_dir(component) / name


def read_log(paths: WorkPaths, component: str, file: str, lines: int) -> dict[str, Any]:
    path = log_path(paths, component, file)
    lines = max(1, min(int(lines), 5000))
    text = tail_lines(path, lines, max_bytes=512 * 1024)
    return {"component": component, "file": file, "path": str(path), "exists": path.is_file(),
            "lines": text.splitlines() if text else []}


def last_query_progress(paths: WorkPaths, run_id: str) -> dict[str, Any] | None:
    """Last JSON line of ``logs/runs/<run_id>/query_progress.jsonl`` (written by the processor)."""
    path = paths.query_progress_log(run_id)
    text = tail_lines(path, 5, max_bytes=256 * 1024)
    for line in reversed(text.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            doc = json.loads(line)
        except ValueError:
            continue
        if isinstance(doc, dict):
            return doc
    return None


def progress_batch_id(progress: Mapping[str, Any] | None) -> int | None:
    if not progress:
        return None
    for key in ("batchId", "batch_id"):
        value = progress.get(key)
        if isinstance(value, int):
            return value
    return None


def extract_end_offsets(progress: Any, topics: set[str]) -> dict[str, dict[int, int]]:
    """Collect ``{topic: {partition: offset}}`` from any nesting / JSON-string encoding of Spark end offsets."""
    out: dict[str, dict[int, int]] = {}

    def visit(node: Any) -> None:
        if isinstance(node, str):
            s = node.strip()
            if s.startswith("{") and any(t in s for t in topics):
                try:
                    visit(json.loads(s))
                except ValueError:
                    return
            return
        if isinstance(node, list):
            for item in node:
                visit(item)
            return
        if not isinstance(node, dict):
            return
        for key, value in node.items():
            if key in topics and isinstance(value, dict):
                for part, off in value.items():
                    try:
                        p, o = int(part), int(off)
                    except (TypeError, ValueError):
                        continue
                    prev = out.setdefault(key, {}).get(p)
                    if prev is None or o > prev:
                        out[key][p] = o
            elif key in ("endOffset", "endOffsets", "end_offsets", "sources"):
                visit(value)
            elif isinstance(value, (dict, list)) or (isinstance(value, str) and value.lstrip().startswith("{")):
                visit(value)

    visit(progress)
    return out


def offsets_caught_up(processed: Mapping[str, Mapping[int, int]], end: Mapping[str, Mapping[int, int]]) -> bool:
    """True when every (topic, partition) with data has been processed up to its high watermark."""
    for topic, parts in end.items():
        for part, high in parts.items():
            if high <= 0:
                continue
            if processed.get(topic, {}).get(part, -1) < high:
                return False
    return True
