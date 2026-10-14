"""Public access: quick-tunnel URL parsing, log offsets, expose start-or-reuse, wrapper-based ownership."""

from __future__ import annotations

from pathlib import Path

import pytest

from transitpulse.runtime import supervisor as supervisor_mod
from transitpulse.runtime.access import decide_expose, parse_tunnel_url, read_log_since, targets_of
from transitpulse.runtime.cli_common import CliError
from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.supervisor import ACCESS_COMPONENTS, COMPONENTS, Supervisor, is_owned_child

from .fakes import FakeProcess, FakeProcFS

SAMPLE_LOG = """\
2026-10-07T10:00:00Z INF Thank you for trying Cloudflare Tunnel. Doing so, without a Cloudflare account, is a quick way \
to experiment and try it out.
2026-10-07T10:00:00Z INF Requesting new quick Tunnel on trycloudflare.com...
2026-10-07T10:00:01Z ERR failed to request quick Tunnel: Post "https://api.trycloudflare.com/tunnel": EOF
2026-10-07T10:00:03Z INF +--------------------------------------------------------------------------------------------+
2026-10-07T10:00:03Z INF |  Your quick Tunnel has been created! Visit it at (it may take some time to be reachable):  |
2026-10-07T10:00:03Z INF |  https://seasonal-deck-organisms-sf.trycloudflare.com                                      |
2026-10-07T10:00:03Z INF +--------------------------------------------------------------------------------------------+
2026-10-07T10:00:04Z INF Registered tunnel connection connIndex=0 location=iad08 protocol=quic
"""


def test_parse_quick_tunnel_url_ignores_the_api_host() -> None:
    assert parse_tunnel_url(SAMPLE_LOG) == "https://seasonal-deck-organisms-sf.trycloudflare.com"


def test_parse_returns_none_without_a_tunnel_url() -> None:
    assert parse_tunnel_url("ERR failed to request quick Tunnel: Post \"https://api.trycloudflare.com/tunnel\"") is None
    assert parse_tunnel_url("") is None


def test_log_offset_hides_previous_incarnations(tmp_path: Path) -> None:
    log = tmp_path / "stdout.log"
    old = "|  https://old-tunnel-words.trycloudflare.com  |\n===== supervisor start component=tunnel\n"
    log.write_text(old)
    offset = len(old.encode())
    assert parse_tunnel_url(read_log_since(log, offset)) is None
    with open(log, "a") as fh:
        fh.write(SAMPLE_LOG)
    assert parse_tunnel_url(read_log_since(log, offset)) == "https://seasonal-deck-organisms-sf.trycloudflare.com"
    assert read_log_since(tmp_path / "missing.log", 0) == ""


def test_expose_start_or_reuse_decision() -> None:
    assert decide_expose(None, alive=False) == "START"
    assert decide_expose({"public_url": "https://a-b.trycloudflare.com"}, alive=False) == "START"
    assert decide_expose({"public_url": "https://a-b.trycloudflare.com"}, alive=True) == "REUSE"
    assert decide_expose({"public_url": None}, alive=True) == "WAIT_URL"


def test_targets() -> None:
    assert targets_of("dashboard") == ["dashboard"]
    assert targets_of("both") == ["dashboard", "api"]
    with pytest.raises(CliError):
        targets_of("everything")


def test_access_components_are_supervised() -> None:
    assert set(ACCESS_COMPONENTS) <= set(COMPONENTS)


TOKEN = "tp:aaaaaaaaaaaa:tunnel:0011"


def _tunnel_entry() -> dict:
    return {"component": "tunnel", "wrapper_pid": 7000, "start_ticks": 50, "owner_token": TOKEN, "boot_id": "boot-1",
            "child_start_ticks": 60, "pgid": 7000, "state": "READY"}


def _proc() -> FakeProcFS:
    proc = FakeProcFS()
    proc.add(FakeProcess(7000, ["python", "-I", "-m", "transitpulse.runtime.runner", "--owner", TOKEN, "--",
                                "/w/tools/cloudflared/2026.10.0/cloudflared", "tunnel"], starttime=50, pgrp=7000))
    # our cloudflared: no token on its command line, child of the verified wrapper
    proc.add(FakeProcess(7001, ["/w/tools/cloudflared/2026.10.0/cloudflared", "tunnel", "--no-autoupdate", "--url",
                                "http://127.0.0.1:8501"], starttime=60, ppid=7000, pgrp=7000))
    # somebody else's cloudflared in another group
    proc.add(FakeProcess(8001, ["/usr/local/bin/cloudflared", "tunnel", "run"], starttime=61, ppid=1, pgrp=8001))
    return proc


def test_wrapper_based_ownership_of_cloudflared() -> None:
    proc = _proc()
    entry = _tunnel_entry()
    assert is_owned_child(entry, 7001, proc, "boot-1")
    assert not is_owned_child(entry, 8001, proc, "boot-1")          # not a child of our wrapper
    assert not is_owned_child(entry, 7001, proc, "boot-2")          # VM rebooted
    proc.procs[7001].starttime = 999                                  # pid reused by another process
    assert not is_owned_child(entry, 7001, proc, "boot-1")
    proc.procs[7001].starttime = 60
    proc.procs[7000].cmdline = ["python", "unrelated.py"]             # wrapper no longer carries the token
    assert not is_owned_child(entry, 7001, proc, "boot-1")


def test_force_kill_signals_only_our_cloudflared(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[tuple[str, int, int]] = []
    monkeypatch.setattr(supervisor_mod.os, "kill", lambda pid, sig: sent.append(("kill", pid, sig)))
    monkeypatch.setattr(supervisor_mod.os, "killpg", lambda pgid, sig: sent.append(("killpg", pgid, sig)))
    proc = _proc()
    sup = Supervisor(WorkPaths.from_workdir(tmp_path / "w"), proc=proc)
    sup._kill_group(7000, TOKEN, 9, wrapper_pid=7000, entry=_tunnel_entry())
    assert sent == [("killpg", 7000, 9)]          # wrapper (token) + verified child: the whole group is ours
    sent.clear()
    sup._kill_group(8001, TOKEN, 9, wrapper_pid=7000, entry=_tunnel_entry())
    assert sent == []                              # a foreign cloudflared is never signalled
