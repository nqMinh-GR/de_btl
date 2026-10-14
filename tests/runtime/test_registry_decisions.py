"""Start-or-reuse decisions (CT03), port/memory gates (CT06) and ownership-scoped stops (CT15).

No process is started and no real signal is sent: /proc is faked and os.kill/os.killpg are recorded.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from transitpulse.runtime import supervisor as supervisor_mod
from transitpulse.runtime.cli_demo import stop_components
from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.supervisor import (
    StartAction,
    Supervisor,
    SupervisorError,
    classify_port,
    compute_fingerprint,
    decide_start,
    spawn_gate_bytes,
    workdir_tag,
)

from .fakes import FakeProcess, FakeProcFS

CT03 = pytest.mark.ct_case("CT03")
CT06 = pytest.mark.ct_case("CT06")
CT15 = pytest.mark.ct_case("CT15")


@CT03
def test_no_entry_spawns() -> None:
    action, _ = decide_start(None, owned=False, fingerprint="a")
    assert action is StartAction.SPAWN


@CT03
def test_owned_same_fingerprint_reuses() -> None:
    action, _ = decide_start({"fingerprint": "a", "wrapper_pid": 10}, owned=True, fingerprint="a")
    assert action is StartAction.REUSE


@CT03
def test_owned_different_fingerprint_conflicts() -> None:
    action, reason = decide_start({"fingerprint": "a", "session_id": "s-1", "run_id": "r-1"}, owned=True,
                                  fingerprint="b")
    assert action is StartAction.CONFLICT
    assert "s-1" in reason and "r-1" in reason


@CT03
def test_dead_or_reused_pid_spawns_again() -> None:
    action, _ = decide_start({"fingerprint": "a", "state": "READY"}, owned=False, fingerprint="a")
    assert action is StartAction.SPAWN


@CT03
def test_fingerprint_depends_on_argv_env_and_extra() -> None:
    base = compute_fingerprint(["a", "b"], {"X": "1"}, "conf=1")
    assert base == compute_fingerprint(["a", "b"], {"X": "1"}, "conf=1")
    assert base != compute_fingerprint(["a", "c"], {"X": "1"}, "conf=1")
    assert base != compute_fingerprint(["a", "b"], {"X": "2"}, "conf=1")
    assert base != compute_fingerprint(["a", "b"], {"X": "1"}, "conf=2")


def _proc_with_listener(cmdline: list[str], environ: dict[str, str] | None = None) -> FakeProcFS:
    proc = FakeProcFS()
    proc.add(FakeProcess(500, cmdline, environ=environ or {}))
    proc.listen(9092, 777, 500)
    return proc


@CT06
def test_classify_free_port() -> None:
    assert classify_port(9092, FakeProcFS(), wtag="t", registered_tokens={})["state"] == "FREE"


@CT06
def test_classify_registered_owner() -> None:
    token = "tp:aaaaaaaaaaaa:kafka:1"
    proc = _proc_with_listener(["java", f"-Dtransitpulse.owner={token}"])
    info = classify_port(9092, proc, wtag="aaaaaaaaaaaa", registered_tokens={"kafka": token})
    assert info["state"] == "OWNED" and info["component"] == "kafka" and info["pid"] == 500


@CT06
def test_classify_ours_but_unregistered_is_adoptable() -> None:
    proc = _proc_with_listener(["java", "-Dtransitpulse.owner=tp:aaaaaaaaaaaa:kafka:old"])
    assert classify_port(9092, proc, wtag="aaaaaaaaaaaa", registered_tokens={})["state"] == "OURS_UNREGISTERED"


@CT06
def test_classify_token_of_another_workdir_is_foreign() -> None:
    proc = _proc_with_listener(["java", "-Dtransitpulse.owner=tp:bbbbbbbbbbbb:kafka:x"])
    assert classify_port(9092, proc, wtag="aaaaaaaaaaaa", registered_tokens={})["state"] == "FOREIGN"


@CT06
def test_port_gate_raises_without_killing(tmp_path: Path) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    proc = _proc_with_listener(["/usr/bin/some-other-server"])
    sup = Supervisor(paths, proc=proc)
    reg = sup.registry()
    with pytest.raises(SupervisorError) as exc:
        sup._check_ports((9092,), reg)
    assert exc.value.code == "PORT_IN_USE"
    assert exc.value.exit_code == 3
    assert 500 in proc.procs  # nothing was signalled


@CT06
def test_port_gate_suggests_adopt_for_our_unregistered_listener(tmp_path: Path) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    tag = workdir_tag(paths.root)
    proc = _proc_with_listener(["java", f"-Dtransitpulse.owner=tp:{tag}:kafka:old"])
    sup = Supervisor(paths, proc=proc)
    with pytest.raises(SupervisorError) as exc:
        sup._check_ports((9092,), sup.registry())
    assert exc.value.code == "PORT_IN_USE_OURS"
    assert "adopt" in (exc.value.hint or "")


@CT06
def test_spawn_gate_includes_one_gib_reserve() -> None:
    assert spawn_gate_bytes("processor") == int(2.8 * 1024**3) + 1024**3
    assert spawn_gate_bytes("kafka") == int(1.0 * 1024**3) + 1024**3


@CT06
def test_memory_gate_refuses_spawn(tmp_path: Path) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    proc = FakeProcFS()
    proc.mem["MemAvailable"] = 2 * 1024**3
    sup = Supervisor(paths, proc=proc)
    with pytest.raises(SupervisorError) as exc:
        sup.start("processor", ["python", "-m", "x"], env={}, scope="session",
                  min_mem_available_bytes=spawn_gate_bytes("processor"))
    assert exc.value.code == "INSUFFICIENT_MEMORY" and exc.value.exit_code == 3
    assert sup.entry("processor") is None


@CT03
def test_conflicting_start_raises_conflict_without_spawning(tmp_path: Path) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    token = f"tp:{workdir_tag(paths.root)}:processor:abc"
    proc = FakeProcFS()
    proc.add(FakeProcess(4200, ["python", "-m", "transitpulse.runtime.runner", "--owner", token], starttime=42))
    sup = Supervisor(paths, proc=proc)
    with sup._locked():
        reg = sup._load()
        reg["components"]["processor"] = {
            "component": "processor", "wrapper_pid": 4200, "start_ticks": 42, "owner_token": token, "boot_id": "boot-1",
            "fingerprint": "old-fingerprint", "state": "READY", "session_id": "s-1", "run_id": "run-1"}
        sup._save(reg)
    with pytest.raises(SupervisorError) as exc:
        sup.start("processor", ["python", "-m", "x", "--run-id", "run-2"], env={}, scope="session")
    assert exc.value.code == "CONFLICT"
    assert exc.value.exit_code == 4
    assert sup.entry("processor")["run_id"] == "run-1"


@CT03
def test_reuse_returns_existing_entry(tmp_path: Path) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    token = f"tp:{workdir_tag(paths.root)}:api:abc"
    proc = FakeProcFS()
    proc.add(FakeProcess(4300, ["python", "-m", "transitpulse.runtime.runner", "--owner", token], starttime=7))
    sup = Supervisor(paths, proc=proc)
    argv, env = ["python", "-m", "uvicorn"], {"TP_WORKDIR": str(paths.root)}
    with sup._locked():
        reg = sup._load()
        reg["components"]["api"] = {"component": "api", "wrapper_pid": 4300, "start_ticks": 7, "owner_token": token,
                                    "boot_id": "boot-1", "fingerprint": compute_fingerprint(argv, env, "x"),
                                    "state": "READY"}
        sup._save(reg)
    entry = sup.start("api", argv, env=env, scope="app", ready=lambda: True, fingerprint_extra="x")
    assert entry["reused"] is True
    assert entry["wrapper_pid"] == 4300


# ------------------------------------------------------------------------------------------ CT15 stop scope

@pytest.fixture()
def signals(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int, int]]:
    sent: list[tuple[str, int, int]] = []
    monkeypatch.setattr(supervisor_mod.os, "kill", lambda pid, sig: sent.append(("kill", pid, sig)))
    monkeypatch.setattr(supervisor_mod.os, "killpg", lambda pgid, sig: sent.append(("killpg", pgid, sig)))
    return sent


@CT15
def test_mixed_process_group_only_token_carriers_are_signalled(tmp_path: Path, signals: list) -> None:
    token = "tp:aaaaaaaaaaaa:processor:1"
    proc = FakeProcFS()
    proc.add(FakeProcess(900001, ["python", "runner", "--owner", token], pgrp=900001))
    proc.add(FakeProcess(900002, ["java", "spark"], pgrp=900001, environ={"TP_OWNER_TOKEN": token}))
    proc.add(FakeProcess(900003, ["/usr/bin/unrelated"], pgrp=900001))
    sup = Supervisor(WorkPaths.from_workdir(tmp_path / "w"), proc=proc)
    sup._kill_group(900001, token, 9, wrapper_pid=900001)
    assert ("killpg", 900001, 9) not in signals
    assert {s[1] for s in signals} == {900001, 900002}


@CT15
def test_group_of_token_carriers_is_killed_as_a_group(tmp_path: Path, signals: list) -> None:
    token = "tp:aaaaaaaaaaaa:kafka:1"
    proc = FakeProcFS()
    proc.add(FakeProcess(900011, ["python", "runner", "--owner", token], pgrp=900011))
    proc.add(FakeProcess(900012, ["java", f"-Dtransitpulse.owner={token}"], pgrp=900011))
    sup = Supervisor(WorkPaths.from_workdir(tmp_path / "w"), proc=proc)
    sup._kill_group(900011, token, 9, wrapper_pid=900011)
    assert signals == [("killpg", 900011, 9)]


@CT15
def test_stop_of_dead_process_sends_no_signal(tmp_path: Path, signals: list) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    sup = Supervisor(paths, proc=FakeProcFS())
    with sup._locked():
        reg = sup._load()
        reg["components"]["kafka"] = {"component": "kafka", "wrapper_pid": 4242, "start_ticks": 1,
                                      "owner_token": "tp:x:kafka:1", "boot_id": "boot-1", "state": "READY",
                                      "exit_file": str(paths.exit_file("kafka"))}
        sup._save(reg)
    res = sup.stop("kafka")
    assert res["stopped"] is False and res.get("already_stopped") is True
    assert signals == []
    assert sup.entry("kafka")["state"] == "EXITED"


class _RecordingSupervisor:
    def __init__(self, entries: dict[str, dict]) -> None:
        self.entries = entries
        self.stopped: list[str] = []

    def entry(self, component: str) -> dict | None:
        return self.entries.get(component)

    def stop(self, component: str) -> dict:
        self.stopped.append(component)
        return {"component": component, "stopped": True}


@CT15
def test_stop_components_never_stops_another_sessions_processes() -> None:
    sup = _RecordingSupervisor({"publisher": {"session_id": "s-other"}, "processor": {"session_id": "s-mine"},
                                "api": {"session_id": None}})
    out = stop_components(None, sup, ("publisher", "processor", "api"), session_id="s-mine")  # type: ignore[arg-type]
    assert sup.stopped == ["processor", "api"]
    assert out["publisher"]["stopped"] is False
