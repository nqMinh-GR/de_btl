"""One-shot fault injection for recovery tests (decisions D-PUB-06, design E §7.3, design B §4).

Honoured ONLY when the processor runs with ``--enable-fault-injection``; otherwise every check is a no-op
that never touches the filesystem.

Arm file: ``W/faults/<run_id>/<point>.json`` as written by the WP3 publisher / test harness::

    {"fault_id": "f-it10", "point": "after_partial_write", "run_id": "...",
     "match": {"feed_type": "vehicle_positions", "snapshot_id": "sn_...", "snapshot_ordinal": 2,
               "capture_id": "...", "timeline_seq": 7},
     "fraction": null, "armed_at": "...Z", "armed_by": "publisher", "session_id": "...", ...}

* Matching (lead decision): the file's ``point`` must be the evaluated point, and either
  ``match.snapshot_id`` equals the snapshot being processed, or ``match.snapshot_id`` is null and
  ``match.feed_type`` equals the snapshot's feed. ``match.snapshot_id`` is already resolved for the run by
  the publisher, so ``snapshot_ordinal`` / ``capture_id`` / ``timeline_seq`` and every other unknown key
  are ignored. An empty match (no snapshot_id, no feed_type) fires at the first evaluation of the point.
* ``fail_after_rows`` (``executor_write_error`` only, optional, default 1): the executor-side writer raises
  after it has written that many rows of one partition.
* ``fraction`` (design E) is accepted and ignored: ``after_partial_write`` always fires after the first
  write group (entity/history tables) and before metrics/quality/READY.

Firing is one-shot and crash-safe: the arm file is renamed to ``fired-<fault_id>.json`` (file + directory
fsync) BEFORE the process exits with code 86 (distinct from the OOM killer's 137), so a replay of the
uncommitted batch after ``demo.py restart-processor`` cannot fire it again. ``executor_write_error`` is
consumed the same way on the driver, then raises inside the Spark writer task, failing the batch with a
normal non-zero exit.
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from transitpulse.contracts.envelope import iso_utc, utcnow
from transitpulse.obs.logging import get_logger, log_event
from transitpulse.runtime.paths import WorkPaths, safe_component

AFTER_PARTIAL_WRITE = "after_partial_write"
AFTER_READY_BEFORE_CAS = "after_ready_before_cas"
AFTER_CAS_BEFORE_COMMIT = "after_cas_before_commit"
EXECUTOR_WRITE_ERROR = "executor_write_error"
FAULT_POINTS: frozenset[str] = frozenset({
    AFTER_PARTIAL_WRITE, AFTER_READY_BEFORE_CAS, AFTER_CAS_BEFORE_COMMIT, EXECUTOR_WRITE_ERROR,
})
FAULT_EXIT_CODE = 86

_log = get_logger("transitpulse.processing.faults")


class InjectedWriteError(RuntimeError):
    """Raised inside the executor writer when ``executor_write_error`` is armed."""


def _check_point(point: str) -> str:
    if point not in FAULT_POINTS:
        raise ValueError(f"unknown fault point {point!r}; expected one of {sorted(FAULT_POINTS)}")
    return point


def arm_file(paths: WorkPaths, run_id: str, point: str) -> Path:
    return paths.fault_dir(run_id) / f"{_check_point(point)}.json"


def _fsync_dir(d: Path) -> None:
    try:
        fd = os.open(str(d), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def arm_fault(
    paths: WorkPaths,
    run_id: str,
    point: str,
    *,
    match: dict[str, Any] | None = None,
    fault_id: str | None = None,
    fail_after_rows: int | None = None,
) -> Path:
    """Write an arm file atomically (helper for tests, the harness and the synthetic publisher)."""
    fid = safe_component(fault_id or f"f-{uuid.uuid4().hex[:12]}", "fault_id")
    spec: dict[str, Any] = {"fault_id": fid, "point": _check_point(point), "match": dict(match or {}),
                            "armed_at": iso_utc(utcnow())}
    if fail_after_rows is not None:
        spec["fail_after_rows"] = int(fail_after_rows)
    path = arm_file(paths, run_id, point)
    _atomic_write_json(path, spec)
    return path


def _flush_logs() -> None:
    for h in logging.getLogger().handlers:
        try:
            h.flush()
        except Exception:  # noqa: BLE001 - exiting anyway
            pass


class FaultInjector:
    """Evaluates fault points on the driver (``processing/batch.py``)."""

    def __init__(self, paths: WorkPaths, run_id: str, *, enabled: bool) -> None:
        self.paths = paths
        self.run_id = run_id
        self.enabled = bool(enabled)

    # ---------------------------------------------------------------- internals
    def _load(self, point: str) -> tuple[Path, dict[str, Any]] | None:
        path = arm_file(self.paths, self.run_id, point)
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        try:
            spec = json.loads(raw)
            if not isinstance(spec, dict):
                raise ValueError("arm file must contain a JSON object")
        except ValueError as exc:
            log_event(_log, "fault", "failed", f"ignoring unreadable arm file {path}: {exc}", level=logging.ERROR)
            return None
        if spec.get("point", point) != point:
            log_event(_log, "fault", "failed", f"arm file {path} names point {spec.get('point')!r}",
                      level=logging.ERROR)
            return None
        if not spec.get("fault_id"):
            spec["fault_id"] = f"{point}-{int(path.stat().st_mtime)}"
        return path, spec

    @staticmethod
    def _matches(spec: dict[str, Any], candidates: Iterable[tuple[str | None, str | None]]) -> bool:
        """snapshot_id equality; or, when match.snapshot_id is null, feed_type equality."""
        match = spec.get("match") or {}
        if not isinstance(match, dict):
            return False
        want_sid = match.get("snapshot_id")
        want_feed = match.get("feed_type")
        if want_sid is None and want_feed is None:
            return True      # unconditional fault (also fires on batches without snapshots)
        for feed_type, snapshot_id in candidates:
            if want_sid is not None:
                if snapshot_id == want_sid:
                    return True
            elif feed_type == want_feed:
                return True
        return False

    def _consume(self, path: Path, spec: dict[str, Any]) -> Path:
        fid = re.sub(r"[^A-Za-z0-9._-]", "-", str(spec["fault_id"])).lstrip("._-")[:120] or "fault"
        fired = path.with_name(f"fired-{fid}.json")
        record = dict(spec)
        record["fired_at"] = iso_utc(utcnow())
        record["pid"] = os.getpid()
        os.replace(path, fired)              # one-shot: the arm file no longer exists
        _atomic_write_json(fired, record)    # enrich + fsync the fired record
        _fsync_dir(path.parent)
        return fired

    # ---------------------------------------------------------------- API
    def check(
        self,
        point: str,
        *,
        feed_type: str | None = None,
        snapshot_id: str | None = None,
        candidates: Iterable[tuple[str | None, str | None]] | None = None,
    ) -> None:
        """Fire ``point`` (exit 86) when armed and matching; otherwise return immediately."""
        _check_point(point)
        if not self.enabled or point == EXECUTOR_WRITE_ERROR:
            return
        loaded = self._load(point)
        if loaded is None:
            return
        path, spec = loaded
        cands = list(candidates) if candidates is not None else [(feed_type, snapshot_id)]
        if not self._matches(spec, cands):
            return
        fired = self._consume(path, spec)
        log_event(_log, "fault", "failed", f"fault injection {point} fired; exiting with {FAULT_EXIT_CODE}",
                  level=logging.ERROR, feed=feed_type, snapshot_id=snapshot_id, fault_id=spec["fault_id"],
                  fired_file=str(fired))
        _flush_logs()
        os._exit(FAULT_EXIT_CODE)

    def take_executor_fault(self, *, feed_type: str | None, snapshot_id: str | None) -> dict[str, Any] | None:
        """Consume an armed ``executor_write_error`` for this snapshot; returns the writer fault spec."""
        if not self.enabled:
            return None
        loaded = self._load(EXECUTOR_WRITE_ERROR)
        if loaded is None:
            return None
        path, spec = loaded
        if not self._matches(spec, [(feed_type, snapshot_id)]):
            return None
        self._consume(path, spec)
        try:
            n = max(1, int(spec.get("fail_after_rows") or 1))
        except (TypeError, ValueError):
            n = 1
        log_event(_log, "fault", "failed", "fault injection executor_write_error armed for this write",
                  level=logging.ERROR, feed=feed_type, snapshot_id=snapshot_id, fault_id=spec["fault_id"])
        return {"fault_id": str(spec["fault_id"]), "fail_after_rows": n}
