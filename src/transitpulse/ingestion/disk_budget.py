"""Raw-archive disk budget with a fail-stop policy (design E §1.7, spec §8.4).

At start the run's raw bytes are scanned once, then tracked incrementally. Before every raw write
the collector reserves the bytes it is about to write:

* ``run_raw_bytes + n > raw_budget_bytes``  -> ``raw_budget_exceeded``
* ``free_bytes - n < min_free_bytes``         -> ``min_free_bytes``

Either condition is sticky (fail-stop): no further capture is written, the collector emits
``collector_status{reason_code: DISK_BUDGET_EXCEEDED, feed_state: failed}``, drains the outbox for a
bounded time, finalizes the recording with ``complete=false`` and exits with code 3. Crossing
``disk_warning_fraction`` of the budget produces a single warning (``DISK_BUDGET_WARNING``).
Raw data is never deleted to make room.
"""

from __future__ import annotations

import logging
import shutil
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from transitpulse.ingestion.archive import STATIC_LINK_DIR, directory_bytes

log = logging.getLogger(__name__)

REASON_RAW_BUDGET = "raw_budget_exceeded"
REASON_MIN_FREE = "min_free_bytes"
DiskUsageFn = Callable[[str], Any]   # shutil.disk_usage-compatible: returns an object with .total/.used/.free


@dataclass(frozen=True)
class BudgetDecision:
    ok: bool
    reason: str | None
    warning: bool
    run_bytes: int
    budget_bytes: int
    free_bytes: int | None
    min_free_bytes: int
    requested_bytes: int

    def to_json(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "reason": self.reason,
            "warning": self.warning,
            "run_bytes": self.run_bytes,
            "budget_bytes": self.budget_bytes,
            "free_bytes": self.free_bytes,
            "min_free_bytes": self.min_free_bytes,
            "requested_bytes": self.requested_bytes,
        }


def existing_ancestor(path: Path) -> Path:
    p = Path(path)
    while not p.exists():
        if p.parent == p:
            break
        p = p.parent
    return p


class DiskBudget:
    """Thread-safe byte budget for one run's raw directory."""

    def __init__(
        self,
        run_dir: Path,
        *,
        budget_bytes: int,
        min_free_bytes: int,
        warning_fraction: float = 0.8,
        disk_usage: DiskUsageFn = shutil.disk_usage,
        initial_bytes: int | None = None,
    ) -> None:
        if budget_bytes <= 0:
            raise ValueError("budget_bytes must be positive")
        if not 0.0 < warning_fraction <= 1.0:
            raise ValueError("warning_fraction must be in (0, 1]")
        self.run_dir = Path(run_dir)
        self.budget_bytes = int(budget_bytes)
        self.min_free_bytes = int(min_free_bytes)
        self.warning_fraction = float(warning_fraction)
        self._disk_usage = disk_usage
        self._lock = threading.Lock()
        self._run_bytes = int(initial_bytes) if initial_bytes is not None else self.scan()
        self._warned = self._run_bytes >= self.warning_threshold
        self._exceeded: str | None = None

    # ------------------------------------------------------------------ measurement
    def scan(self) -> int:
        """Bytes currently on disk under the run directory (hard-linked static ZIP excluded)."""
        return directory_bytes(self.run_dir, exclude_top=(STATIC_LINK_DIR,))

    @property
    def warning_threshold(self) -> int:
        return int(self.budget_bytes * self.warning_fraction)

    @property
    def run_bytes(self) -> int:
        with self._lock:
            return self._run_bytes

    @property
    def exceeded(self) -> str | None:
        """Reason of the sticky fail-stop, or None while writes are allowed."""
        return self._exceeded

    def free_bytes(self) -> int | None:
        try:
            return int(self._disk_usage(str(existing_ancestor(self.run_dir))).free)
        except OSError as exc:
            log.warning("disk_usage failed for %s: %s", self.run_dir, exc)
            return None

    # ------------------------------------------------------------------ decisions
    def _decide_locked(self, nbytes: int) -> BudgetDecision:
        free = self.free_bytes()
        projected = self._run_bytes + nbytes
        reason: str | None = self._exceeded
        if reason is None:
            if projected > self.budget_bytes:
                reason = REASON_RAW_BUDGET
            elif free is not None and free - nbytes < self.min_free_bytes:
                reason = REASON_MIN_FREE
        if reason is not None:
            self._exceeded = reason
            return BudgetDecision(False, reason, False, self._run_bytes, self.budget_bytes, free,
                                  self.min_free_bytes, nbytes)
        warning = False
        if projected >= self.warning_threshold and not self._warned:
            self._warned = True
            warning = True
        self._run_bytes = projected
        return BudgetDecision(True, None, warning, projected, self.budget_bytes, free, self.min_free_bytes, nbytes)

    def try_reserve(self, nbytes: int) -> BudgetDecision:
        """Reserve ``nbytes`` before a write; a refusal is sticky (fail-stop)."""
        if nbytes < 0:
            raise ValueError("nbytes must be non-negative")
        with self._lock:
            return self._decide_locked(int(nbytes))

    def check(self) -> BudgetDecision:
        """Evaluate the budget without reserving anything (start-up / disk-check)."""
        with self._lock:
            before = self._run_bytes
            decision = self._decide_locked(0)
            self._run_bytes = before
            return decision

    def settle(self, reserved: int, actual: int) -> None:
        """Replace a reservation by the bytes actually written (may be smaller or larger)."""
        with self._lock:
            self._run_bytes = max(0, self._run_bytes - int(reserved) + int(actual))

    def add(self, nbytes: int) -> None:
        """Account bytes written outside a reservation (error bodies, sidecar rewrites)."""
        with self._lock:
            self._run_bytes += max(0, int(nbytes))

    def status(self) -> dict[str, Any]:
        with self._lock:
            run_bytes = self._run_bytes
        free = self.free_bytes()
        if self._exceeded:
            state = "exceeded"
        elif run_bytes >= self.warning_threshold:
            state = "warning"
        else:
            state = "ok"
        return {
            "run_dir": str(self.run_dir),
            "run_raw_bytes": run_bytes,
            "budget_bytes": self.budget_bytes,
            "used_fraction": round(run_bytes / self.budget_bytes, 6),
            "warning_fraction": self.warning_fraction,
            "free_bytes": free,
            "min_free_bytes": self.min_free_bytes,
            "state": state,
            "exceeded_reason": self._exceeded,
        }
