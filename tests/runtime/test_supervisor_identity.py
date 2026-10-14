"""Supervisor process identity (/proc starttime + boot id + owner token) with a fake /proc."""

from __future__ import annotations

from pathlib import Path

import pytest

from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.proc import parse_net_tcp, parse_stat
from transitpulse.runtime.supervisor import Supervisor, is_owned, workdir_tag

from .fakes import FakeProcess, FakeProcFS

TOKEN = "tp:abcdef012345:kafka:00112233445566778899"


def _entry(**over: object) -> dict:
    e = {"component": "kafka", "wrapper_pid": 4100, "start_ticks": 812345, "owner_token": TOKEN,
         "boot_id": "boot-1", "fingerprint": "fp", "state": "READY"}
    e.update(over)
    return e


@pytest.fixture()
def proc() -> FakeProcFS:
    fake = FakeProcFS()
    fake.add(FakeProcess(4100, ["/w/.venv/bin/python", "-I", "-m", "transitpulse.runtime.runner", "--owner", TOKEN,
                                "--", "/w/kafka/bin/kafka-server-start.sh"], starttime=812345))
    return fake


def test_owned_when_all_identity_fields_match(proc: FakeProcFS) -> None:
    assert is_owned(_entry(), proc, "boot-1")


def test_not_owned_when_pid_missing(proc: FakeProcFS) -> None:
    assert not is_owned(_entry(wrapper_pid=9999), proc, "boot-1")


def test_zombie_counts_as_dead(proc: FakeProcFS) -> None:
    proc.procs[4100].state = "Z"
    assert not is_owned(_entry(), proc, "boot-1")


def test_reused_pid_detected_by_start_ticks(proc: FakeProcFS) -> None:
    proc.procs[4100].starttime = 999999
    assert not is_owned(_entry(), proc, "boot-1")


def test_boot_id_change_invalidates(proc: FakeProcFS) -> None:
    assert not is_owned(_entry(), proc, "boot-2")


def test_token_must_be_in_cmdline(proc: FakeProcFS) -> None:
    proc.procs[4100].cmdline = ["/usr/bin/python3", "unrelated.py"]
    assert not is_owned(_entry(), proc, "boot-1")


@pytest.mark.parametrize("bad", [{"wrapper_pid": None}, {"wrapper_pid": 1}, {"owner_token": ""},
                                 {"start_ticks": None}])
def test_incomplete_entries_never_owned(proc: FakeProcFS, bad: dict) -> None:
    assert not is_owned(_entry(**bad), proc, "boot-1")


def test_parse_stat_with_parentheses_and_spaces_in_comm() -> None:
    fields = ["S", "1", "4100", "4100"] + ["0"] * 15 + ["812345", "0"]
    text = "4102 (java (main) x) " + " ".join(fields) + "\n"
    st = parse_stat(text)
    assert st.pid == 4102
    assert st.comm == "java (main) x"
    assert st.state == "S" and st.ppid == 1 and st.pgrp == 4100
    assert st.starttime == 812345


def test_parse_net_tcp_listen_entries_only() -> None:
    text = (
        "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
        "   0: 0100007F:2384 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 5555 1\n"
        "   1: 0100007F:2384 0100007F:C350 01 00000000:00000000 00:00000000 00000000     0        0 6666 1\n"
    )
    assert parse_net_tcp(text) == [(0x2384, 5555)]


def test_registry_boot_change_marks_entries_exited(tmp_path: Path) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    old = FakeProcFS(boot_id="boot-old")
    sup_old = Supervisor(paths, proc=old)
    with sup_old._locked():
        reg = sup_old._load()
        reg["components"]["kafka"] = _entry(boot_id="boot-old")
        reg["current_session"] = "s-1"
        sup_old._save(reg)
    sup_new = Supervisor(paths, proc=FakeProcFS(boot_id="boot-new"))
    reg = sup_new.registry()
    assert reg["boot_id"] == "boot-new"
    assert reg["components"]["kafka"]["state"] == "EXITED"
    assert reg["components"]["kafka"]["exit"]["terminated_by"] == "vm_reboot"
    assert reg["current_session"] is None


def test_workdir_tag_is_stable(tmp_path: Path) -> None:
    assert workdir_tag(tmp_path) == workdir_tag(tmp_path / "." )
    assert len(workdir_tag(tmp_path)) == 12
