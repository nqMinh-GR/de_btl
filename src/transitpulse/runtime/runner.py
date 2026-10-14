"""Process wrapper spawned by the supervisor (stdlib only, must stay import-light).

    python -I -m transitpulse.runtime.runner --owner TOKEN --component C --exit-file F
           [--deadline EPOCH] [--term-timeout SECONDS] -- argv...

* Runs in its own session/process group (the supervisor uses ``start_new_session=True``), so it
  survives a notebook-kernel restart or interrupt.
* Spawns ``argv`` as its child (same process group, inherits the log file as stdout/stderr) and
  forwards SIGTERM/SIGINT to it.
* Optional ``--deadline``: when the wall clock passes it, the child receives SIGTERM (session budget);
  after ``--term-timeout`` seconds without exit it receives SIGKILL. Only the direct child is signalled.
* Writes ``F`` atomically: ``{"state": "RUNNING", child_pid, ...}`` after the spawn and
  ``{"state": "EXITED", exit_code, signal, ended_at, terminated_by}`` at the end, so exit codes stay
  visible after the CLI that started the component has exited.
* Its own command line carries the owner token, which is how the supervisor verifies ownership.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _start_ticks(pid: int) -> int | None:
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
        rest = text[text.rindex(")") + 1 :].split()
        return int(rest[19])
    except (OSError, ValueError, IndexError):
        return None


def _write_state(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _stderr(msg: str) -> None:
    # The runner's stderr is the component log file; one plain line per lifecycle event.
    sys.stderr.write(f"[tp-runner {_now_iso()}] {msg}\n")
    sys.stderr.flush()


def _parse(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    if "--" not in argv:
        raise SystemExit("runner: missing '--' before the command")
    split = argv.index("--")
    own, cmd = argv[:split], argv[split + 1 :]
    p = argparse.ArgumentParser(prog="transitpulse.runtime.runner", add_help=False)
    p.add_argument("--owner", required=True)
    p.add_argument("--component", required=True)
    p.add_argument("--exit-file", required=True)
    p.add_argument("--deadline", type=float, default=None)
    p.add_argument("--term-timeout", type=float, default=60.0)
    args = p.parse_args(own)
    if not cmd:
        raise SystemExit("runner: empty command after '--'")
    return args, cmd


def main(argv: list[str] | None = None) -> int:
    args, cmd = _parse(list(sys.argv[1:] if argv is None else argv))
    exit_file = Path(args.exit_file)
    base: dict[str, Any] = {
        "owner": args.owner,
        "component": args.component,
        "runner_pid": os.getpid(),
        "started_at": _now_iso(),
        "deadline_epoch": args.deadline,
    }
    state: dict[str, Any] = {"child": None, "pending": None, "terminated_by": None}

    def _forward(signum: int, _frame: Any) -> None:
        child = state["child"]
        if state["terminated_by"] is None:
            state["terminated_by"] = f"signal:{signal.Signals(signum).name}"
        if child is None:
            state["pending"] = signum
            return
        try:
            child.send_signal(signum)
        except ProcessLookupError:
            pass

    signal.signal(signal.SIGTERM, _forward)
    signal.signal(signal.SIGINT, _forward)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    _write_state(exit_file, {**base, "state": "STARTING", "child_pid": None})
    _stderr(f"starting component={args.component} argv0={cmd[0]}")
    try:
        child = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, close_fds=True)
    except OSError as exc:
        _stderr(f"spawn failed: {exc}")
        _write_state(exit_file, {**base, "state": "EXITED", "child_pid": None, "exit_code": 127, "signal": None,
                                 "ended_at": _now_iso(), "terminated_by": None, "error": str(exc)[:500]})
        return 127
    state["child"] = child
    _write_state(exit_file, {**base, "state": "RUNNING", "child_pid": child.pid,
                             "child_start_ticks": _start_ticks(child.pid)})
    if state["pending"] is not None:
        try:
            child.send_signal(state["pending"])
        except ProcessLookupError:
            pass

    deadline_sent_at: float | None = None
    while True:
        try:
            rc = child.wait(timeout=1.0)
            break
        except subprocess.TimeoutExpired:
            pass
        now = time.time()
        if args.deadline is not None and deadline_sent_at is None and now >= args.deadline:
            deadline_sent_at = now
            state["terminated_by"] = "deadline"
            _stderr("session deadline reached: sending SIGTERM to the component")
            try:
                child.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass
        if deadline_sent_at is not None and now - deadline_sent_at > max(1.0, args.term_timeout):
            _stderr("component did not stop after the deadline grace period: sending SIGKILL")
            try:
                child.kill()
            except ProcessLookupError:
                pass
            deadline_sent_at = float("inf")  # kill once

    exit_code = rc if rc >= 0 else None
    sig = -rc if rc < 0 else None
    _write_state(exit_file, {**base, "state": "EXITED", "child_pid": child.pid, "exit_code": exit_code,
                             "signal": sig, "ended_at": _now_iso(), "terminated_by": state["terminated_by"]})
    _stderr(f"component exited exit_code={exit_code} signal={sig} terminated_by={state['terminated_by']}")
    return rc if rc >= 0 else 128 + (-rc)


if __name__ == "__main__":
    sys.exit(main())
