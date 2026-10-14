"""One-shot arm-file fault injection (D-PUB-06): matching, rename-before-exit, exit code 86."""

from __future__ import annotations

import json
import os

import pytest

from transitpulse.processing import faults
from transitpulse.processing.faults import (
    AFTER_CAS_BEFORE_COMMIT,
    AFTER_PARTIAL_WRITE,
    AFTER_READY_BEFORE_CAS,
    EXECUTOR_WRITE_ERROR,
    FAULT_EXIT_CODE,
    FaultInjector,
    arm_fault,
    arm_file,
)
from transitpulse.runtime.paths import WorkPaths

RUN = "run-1"


class Exited(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


@pytest.fixture()
def paths(tmp_path) -> WorkPaths:
    return WorkPaths.from_workdir(tmp_path)


@pytest.fixture()
def exits(monkeypatch):
    calls: list[int] = []

    def fake_exit(code: int) -> None:
        calls.append(code)
        raise Exited(code)

    monkeypatch.setattr(os, "_exit", fake_exit)
    return calls


def test_disabled_injector_never_fires_nor_consumes(paths, exits):
    p = arm_fault(paths, RUN, AFTER_PARTIAL_WRITE, fault_id="f1")
    inj = FaultInjector(paths, RUN, enabled=False)
    inj.check(AFTER_PARTIAL_WRITE, feed_type="vehicle_positions", snapshot_id="sn_a")
    assert inj.take_executor_fault(feed_type="vehicle_positions", snapshot_id="sn_a") is None
    assert p.exists() and exits == []


def test_matching_fault_renames_then_exits_86(paths, exits):
    p = arm_fault(paths, RUN, AFTER_PARTIAL_WRITE, fault_id="f-it10",
                  match={"feed_type": "vehicle_positions", "snapshot_id": "sn_s2"})
    inj = FaultInjector(paths, RUN, enabled=True)
    inj.check(AFTER_PARTIAL_WRITE, feed_type="vehicle_positions", snapshot_id="sn_s1")   # no match
    assert p.exists() and exits == []
    with pytest.raises(Exited) as ei:
        inj.check(AFTER_PARTIAL_WRITE, feed_type="vehicle_positions", snapshot_id="sn_s2")
    assert ei.value.code == FAULT_EXIT_CODE == 86
    assert not p.exists()
    fired = paths.fault_dir(RUN) / "fired-f-it10.json"
    doc = json.loads(fired.read_text(encoding="utf-8"))
    assert doc["point"] == AFTER_PARTIAL_WRITE and doc["fired_at"] and doc["fault_id"] == "f-it10"
    # one-shot: the replay of the same batch does not fire again
    inj2 = FaultInjector(paths, RUN, enabled=True)
    inj2.check(AFTER_PARTIAL_WRITE, feed_type="vehicle_positions", snapshot_id="sn_s2")
    assert exits == [86]


def test_points_are_independent(paths, exits):
    arm_fault(paths, RUN, AFTER_READY_BEFORE_CAS, fault_id="f2")
    inj = FaultInjector(paths, RUN, enabled=True)
    inj.check(AFTER_PARTIAL_WRITE, feed_type="alerts", snapshot_id="sn_x")
    assert exits == []
    with pytest.raises(Exited):
        inj.check(AFTER_READY_BEFORE_CAS, feed_type="alerts", snapshot_id="sn_x")


def _write_publisher_arm(paths, point: str, match: dict) -> None:
    """Arm file exactly as the WP3 publisher writes it (extra keys must be ignored)."""
    d = paths.fault_dir(RUN)
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{point}.json").write_text(json.dumps({
        "fault_id": "ft_" + "a" * 32, "point": point, "run_id": RUN,
        "match": {"feed_type": None, "snapshot_ordinal": 2, "snapshot_id": None, "capture_id": "vp-x",
                  "timeline_seq": 7, **match},
        "fraction": None, "armed_at": "2026-10-05T12:00:30.000Z", "armed_by": "publisher", "session_id": "s1",
        "source_clock": "2026-10-05T12:00:30.000Z", "timeline_seq": 7,
    }), encoding="utf-8")


def test_resolved_snapshot_id_wins_over_feed_and_ordinal(paths, exits):
    _write_publisher_arm(paths, AFTER_PARTIAL_WRITE, {"feed_type": "trip_updates", "snapshot_id": "sn_t2"})
    inj = FaultInjector(paths, RUN, enabled=True)
    inj.check(AFTER_PARTIAL_WRITE, feed_type="trip_updates", snapshot_id="sn_t1")    # same feed, other snapshot
    assert exits == []
    with pytest.raises(Exited):
        inj.check(AFTER_PARTIAL_WRITE, feed_type="trip_updates", snapshot_id="sn_t2")
    assert (paths.fault_dir(RUN) / f"fired-ft_{'a' * 32}.json").exists()


def test_null_snapshot_id_matches_on_feed_type(paths, exits):
    _write_publisher_arm(paths, AFTER_READY_BEFORE_CAS, {"feed_type": "alerts"})
    inj = FaultInjector(paths, RUN, enabled=True)
    inj.check(AFTER_READY_BEFORE_CAS, feed_type="vehicle_positions", snapshot_id="sn_v")
    assert exits == []
    with pytest.raises(Exited):
        inj.check(AFTER_READY_BEFORE_CAS, feed_type="alerts", snapshot_id="sn_any")


def test_batch_level_point_with_candidates_and_empty_match(paths, exits):
    arm_fault(paths, RUN, AFTER_CAS_BEFORE_COMMIT, fault_id="f4", match={"snapshot_id": "sn_s2"})
    inj = FaultInjector(paths, RUN, enabled=True)
    inj.check(AFTER_CAS_BEFORE_COMMIT, candidates=[])
    inj.check(AFTER_CAS_BEFORE_COMMIT, candidates=[("vehicle_positions", "sn_s1")])
    assert exits == []
    with pytest.raises(Exited):
        inj.check(AFTER_CAS_BEFORE_COMMIT, candidates=[("vehicle_positions", "sn_s1"), ("vehicle_positions", "sn_s2")])
    arm_fault(paths, RUN, AFTER_CAS_BEFORE_COMMIT, fault_id="f5")          # unconditional
    with pytest.raises(Exited):
        FaultInjector(paths, RUN, enabled=True).check(AFTER_CAS_BEFORE_COMMIT, candidates=[])


def test_executor_write_error_is_consumed_on_the_driver(paths, exits):
    arm_fault(paths, RUN, EXECUTOR_WRITE_ERROR, fault_id="f6", match={"feed_type": "alerts"}, fail_after_rows=3)
    inj = FaultInjector(paths, RUN, enabled=True)
    inj.check(EXECUTOR_WRITE_ERROR, feed_type="alerts", snapshot_id="x")    # never exits via check()
    assert inj.take_executor_fault(feed_type="vehicle_positions", snapshot_id="x") is None
    spec = inj.take_executor_fault(feed_type="alerts", snapshot_id="x")
    assert spec == {"fault_id": "f6", "fail_after_rows": 3}
    assert not arm_file(paths, RUN, EXECUTOR_WRITE_ERROR).exists()
    assert (paths.fault_dir(RUN) / "fired-f6.json").exists()
    assert inj.take_executor_fault(feed_type="alerts", snapshot_id="x") is None
    assert exits == []


def test_unreadable_arm_file_is_ignored(paths, exits):
    p = arm_file(paths, RUN, AFTER_PARTIAL_WRITE)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("{not json", encoding="utf-8")
    FaultInjector(paths, RUN, enabled=True).check(AFTER_PARTIAL_WRITE, feed_type="alerts", snapshot_id="x")
    assert exits == [] and p.exists()


def test_unknown_point_is_rejected(paths):
    with pytest.raises(ValueError):
        arm_fault(paths, RUN, "after_everything")
    with pytest.raises(ValueError):
        FaultInjector(paths, RUN, enabled=True).check("after_everything")


def test_arm_file_layout(paths):
    p = arm_fault(paths, RUN, AFTER_PARTIAL_WRITE, fault_id="f7", match={"feed_type": "alerts"})
    assert p == paths.root / "faults" / RUN / "after_partial_write.json"
    doc = json.loads(p.read_text(encoding="utf-8"))
    assert doc["fault_id"] == "f7" and doc["match"] == {"feed_type": "alerts"}
    assert set(faults.FAULT_POINTS) == {AFTER_PARTIAL_WRITE, AFTER_READY_BEFORE_CAS, AFTER_CAS_BEFORE_COMMIT,
                                        EXECUTOR_WRITE_ERROR}
