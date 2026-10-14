"""Unit tests for the raw disk budget and its fail-stop policy (ingestion.disk_budget)."""

from __future__ import annotations

from collections import namedtuple
from pathlib import Path
from typing import Any

import pytest

from transitpulse.ingestion.disk_budget import REASON_MIN_FREE, REASON_RAW_BUDGET, DiskBudget

Usage = namedtuple("Usage", "total used free")


def usage(free: int) -> Any:
    def fn(path: str) -> Usage:
        return Usage(10**12, 10**12 - free, free)
    return fn


def make(tmp_path: Path, *, budget: int = 1000, min_free: int = 100, free: int = 10**9,
         fraction: float = 0.8) -> DiskBudget:
    return DiskBudget(tmp_path / "run", budget_bytes=budget, min_free_bytes=min_free, warning_fraction=fraction,
                      disk_usage=usage(free))


def test_initial_scan_counts_existing_bytes_but_not_static(tmp_path: Path) -> None:
    run = tmp_path / "run"
    (run / "alerts" / "2026-10-05").mkdir(parents=True)
    (run / "alerts" / "2026-10-05" / "a.json.gz").write_bytes(b"x" * 300)
    (run / "static").mkdir()
    (run / "static" / "MBTA_GTFS.zip").write_bytes(b"z" * 5000)
    assert make(tmp_path).run_bytes == 300


def test_missing_run_dir_scans_as_zero_and_uses_existing_ancestor(tmp_path: Path) -> None:
    budget = make(tmp_path)
    assert budget.run_bytes == 0
    assert budget.free_bytes() == 10**9


def test_reservations_accumulate_until_the_budget(tmp_path: Path) -> None:
    b = make(tmp_path)
    assert b.try_reserve(400).ok
    assert b.try_reserve(600).ok             # exactly at the budget is allowed
    assert b.run_bytes == 1000
    refused = b.try_reserve(1)
    assert not refused.ok and refused.reason == REASON_RAW_BUDGET
    assert b.exceeded == REASON_RAW_BUDGET


def test_fail_stop_is_sticky(tmp_path: Path) -> None:
    b = make(tmp_path, budget=1000)
    assert not b.try_reserve(2000).ok
    assert not b.try_reserve(0).ok           # even a tiny write is refused afterwards
    assert b.run_bytes == 0                  # a refused reservation is not counted
    assert b.status()["state"] == "exceeded"


def test_min_free_bytes(tmp_path: Path) -> None:
    b = make(tmp_path, budget=10**9, min_free=1000, free=1500)
    assert b.try_reserve(400).ok
    refused = b.try_reserve(600)
    assert not refused.ok and refused.reason == REASON_MIN_FREE
    assert refused.free_bytes == 1500


def test_warning_is_emitted_once_at_the_threshold(tmp_path: Path) -> None:
    b = make(tmp_path, budget=1000, fraction=0.8)
    assert b.try_reserve(790).warning is False
    assert b.try_reserve(20).warning is True     # 810 >= 800
    assert b.try_reserve(10).warning is False
    assert b.status()["state"] == "warning"


def test_settle_and_add_adjust_usage(tmp_path: Path) -> None:
    b = make(tmp_path)
    b.try_reserve(500)
    b.settle(500, 200)
    assert b.run_bytes == 200
    b.add(50)
    assert b.run_bytes == 250
    b.settle(10_000, 0)
    assert b.run_bytes == 0


def test_check_does_not_reserve(tmp_path: Path) -> None:
    b = make(tmp_path)
    b.try_reserve(100)
    decision = b.check()
    assert decision.ok and decision.run_bytes == 100 and b.run_bytes == 100
    assert decision.to_json()["budget_bytes"] == 1000


def test_check_fails_when_free_space_is_already_too_low(tmp_path: Path) -> None:
    b = make(tmp_path, min_free=1000, free=10)
    decision = b.check()
    assert not decision.ok and decision.reason == REASON_MIN_FREE


def test_unknown_free_space_does_not_block(tmp_path: Path) -> None:
    def broken(path: str) -> Usage:
        raise OSError("statvfs failed")
    b = DiskBudget(tmp_path / "run", budget_bytes=1000, min_free_bytes=100, disk_usage=broken)
    decision = b.try_reserve(10)
    assert decision.ok and decision.free_bytes is None


def test_invalid_parameters() -> None:
    with pytest.raises(ValueError):
        DiskBudget(Path("/nonexistent"), budget_bytes=0, min_free_bytes=0, disk_usage=usage(1))
    with pytest.raises(ValueError):
        DiskBudget(Path("/nonexistent"), budget_bytes=10, min_free_bytes=0, warning_fraction=1.5, disk_usage=usage(1))
    b = DiskBudget(Path("/nonexistent"), budget_bytes=10, min_free_bytes=0, disk_usage=usage(1), initial_bytes=3)
    with pytest.raises(ValueError):
        b.try_reserve(-1)
