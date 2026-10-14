"""Second Run All / rerun of the start cell: the same session is resumed without CONFLICT (CT04)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from transitpulse.runtime import cli_demo
from transitpulse.runtime.cli_demo import input_duration, publisher_argv, reuse_alive
from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.session import read_session, write_session
from transitpulse.runtime.supervisor import compute_fingerprint

pytestmark = pytest.mark.ct_case("CT04")


def _ctx(tmp_path: Path) -> SimpleNamespace:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    paths.ensure_base_dirs()
    return SimpleNamespace(paths=paths, python="/r/.venv/bin/python")


def _session(ctx: SimpleNamespace) -> dict:
    deadline = datetime(2026, 10, 7, 12, 10, tzinfo=UTC)
    session = {"session_id": "s-20261007T120000Z-ab12", "run_id": "run-1", "mode": "synthetic",
               "deadline_at": deadline.isoformat()}
    write_session(ctx.paths, session)
    return session


def test_input_duration_is_computed_once_and_persisted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _ctx(tmp_path)
    session = _session(ctx)
    deadline = datetime(2026, 10, 7, 12, 10, tzinfo=UTC).timestamp()
    monkeypatch.setattr(cli_demo.time, "time", lambda: deadline - 600)
    first = input_duration(ctx, session)
    assert first == 600
    # Later rerun (a new CLI process reading session.json again): the wall clock moved, the value did not.
    monkeypatch.setattr(cli_demo.time, "time", lambda: deadline - 420)
    reread = read_session(ctx.paths, session["session_id"])
    assert input_duration(ctx, reread) == 600


def test_publisher_fingerprint_identical_on_resume(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _ctx(tmp_path)
    session = _session(ctx)
    deadline = datetime(2026, 10, 7, 12, 10, tzinfo=UTC).timestamp()
    env = {"TP_WORKDIR": str(ctx.paths.root)}

    def argv_at(now: float) -> list[str]:
        monkeypatch.setattr(cli_demo.time, "time", lambda: now)
        current = read_session(ctx.paths, session["session_id"])
        return publisher_argv(ctx, run_id="run-1", session_id=current["session_id"], manifest="/m.json", speed=1.0,
                              step_mode=False, duration_seconds=input_duration(ctx, current))

    first = argv_at(deadline - 590)
    second = argv_at(deadline - 300)
    assert first == second
    assert compute_fingerprint(first, env, "x") == compute_fingerprint(second, env, "x")


class _Sup:
    def __init__(self, entries: dict[str, dict], alive: set[str]) -> None:
        self.entries = entries
        self.alive = alive

    def entry(self, component: str) -> dict | None:
        return self.entries.get(component)

    def is_alive(self, component: str) -> bool:
        return component in self.alive

    def start(self, *args: object, **kwargs: object) -> dict:
        raise AssertionError("an alive component of the same session must not be restarted")


def test_alive_component_of_the_same_session_is_reused_without_start() -> None:
    sup = _Sup({"publisher": {"session_id": "s-1", "state": "READY", "pid": 10, "stdout_log": "/l"},
                "processor": {"session_id": "s-other", "state": "READY"}}, alive={"publisher", "processor"})
    view = reuse_alive(sup, "publisher", "s-1")  # type: ignore[arg-type]
    assert view is not None and view["reused"] is True and view["pid"] == 10
    assert reuse_alive(sup, "processor", "s-1") is None  # type: ignore[arg-type]
    sup.alive.discard("publisher")
    assert reuse_alive(sup, "publisher", "s-1") is None  # type: ignore[arg-type]


def test_deadline_in_the_past_still_gives_a_positive_duration(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _ctx(tmp_path)
    session = _session(ctx)
    monkeypatch.setattr(cli_demo.time, "time",
                        lambda: (datetime(2026, 10, 7, 12, 10, tzinfo=UTC) + timedelta(minutes=5)).timestamp())
    assert input_duration(ctx, session) == 1
