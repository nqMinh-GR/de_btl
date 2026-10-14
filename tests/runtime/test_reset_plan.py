"""reset: dry-run plans list only real paths strictly inside W; the confirm token binds the exact list."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.reset import confirm_token, execute_reset, plan_reset, target_paths


@pytest.fixture()
def work(tmp_path: Path) -> WorkPaths:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    paths.ensure_base_dirs()
    (paths.kafka_data / "meta.properties").write_text("node.id=1\n")
    (paths.root / "checkpoints" / "p1" / "run-1").mkdir(parents=True)
    (paths.root / "checkpoints" / "p1" / "run-1" / "offsets").write_text("0")
    return paths


def test_every_planned_path_is_inside_the_workdir(work: WorkPaths) -> None:
    plan = plan_reset(work, ["all"], include_caches=True)
    root = os.path.realpath(work.root)
    assert plan["paths"]
    for e in plan["paths"]:
        assert os.path.commonpath([root, e["path"]]) == root
        assert e["path"] != root


def test_dry_run_deletes_nothing_and_token_is_deterministic(work: WorkPaths) -> None:
    plan1 = plan_reset(work, ["kafka"])
    plan2 = plan_reset(work, ["kafka"])
    assert plan1["confirm_token"] == plan2["confirm_token"]
    assert (work.kafka_data / "meta.properties").is_file()
    assert plan1["paths"][0]["path"].endswith("/infra/kafka")
    assert plan1["paths"][0]["files"] == 1


def test_token_changes_with_targets(work: WorkPaths) -> None:
    assert plan_reset(work, ["kafka"])["confirm_token"] != plan_reset(work, ["checkpoints"])["confirm_token"]
    assert confirm_token(["/a"], ["x"], False) != confirm_token(["/a"], ["x"], True)


def test_symlink_pointing_outside_is_removed_as_link_not_followed(work: WorkPaths, tmp_path: Path) -> None:
    outside = tmp_path / "precious"
    outside.mkdir()
    (outside / "keep.txt").write_text("user data")
    ckpt = work.root / "checkpoints"
    for child in ckpt.iterdir():
        if child.is_dir():
            import shutil

            shutil.rmtree(child)
    (ckpt / "link").symlink_to(outside, target_is_directory=True)
    plan = plan_reset(work, ["checkpoints"])
    deleted = execute_reset(plan)
    assert deleted == [os.path.realpath(ckpt)]
    assert (outside / "keep.txt").read_text() == "user data"


def test_workdir_symlinked_target_outside_is_rejected(work: WorkPaths, tmp_path: Path) -> None:
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    sessions = work.root / "sessions"
    sessions.rmdir()
    sessions.symlink_to(outside, target_is_directory=True)
    plan = plan_reset(work, ["session"])
    real_sessions = os.path.join(os.path.realpath(work.root), "sessions")
    listed = {e["path"] for e in plan["paths"]}
    assert real_sessions in listed  # the link itself (inside W), never its target
    assert all(not p.startswith(str(outside)) for p in listed)
    execute_reset(plan)
    assert outside.is_dir()


def test_unknown_target_rejected(work: WorkPaths) -> None:
    with pytest.raises(ValueError):
        target_paths(work, "everything")
