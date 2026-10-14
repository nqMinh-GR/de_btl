"""Native process supervisor (design D §6): PID registry, ownership-checked start-or-reuse and stop.

Registry ``W/processes/registry.json`` (every write under ``fcntl.flock(registry.lock)``, atomic)::

    {"schema_version": 1, "workdir": "...", "boot_id": "...", "current_session": "s-..."|null,
     "components": {"kafka": {"component", "scope", "session_id", "run_id", "wrapper_pid", "pid", "pgid",
        "start_ticks", "child_start_ticks", "boot_id", "owner_token", "argv", "fingerprint", "stdout_log",
        "exit_file", "ports", "state": STARTING|READY|STOPPING|EXITED|FAILED, "started_at", "started_epoch",
        "ready_at", "exit", "deadline_epoch"}}}

Ownership ``owned(e)``: ``/proc/<wrapper_pid>/stat`` exists, state is not zombie, ``starttime`` equals the
recorded ticks, the boot id is unchanged and ``/proc/<wrapper_pid>/cmdline`` contains the owner token.
Signals are only ever sent to a verified wrapper, and ``killpg`` is used only when every member of the
process group carries the owner token (command line or environment). Never ``pkill``/``killall``.

Each component is spawned through :mod:`transitpulse.runtime.runner` in a new session, with stdout and
stderr appended to ``W/logs/<component>/stdout.log`` and stdin from /dev/null (a long-lived child must
never inherit the caller's pipes, otherwise the notebook cell that started it would never finish).
The literal ``@OWNER_TOKEN@`` in argv/env values is replaced by the token at spawn time, so JVM
command lines (``-Dtransitpulse.owner=...``) carry it too.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import hashlib
import json
import logging
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from enum import StrEnum
from pathlib import Path
from typing import Any

from transitpulse.runtime.paths import WorkPaths, safe_component
from transitpulse.runtime.proc import GIB, ProcFS, carries_token
from transitpulse.runtime.util import atomic_write_json, iso_now, read_json_or_none, tail_lines

log = logging.getLogger("transitpulse.runtime.supervisor")

COMPONENTS: tuple[str, ...] = ("kafka", "cassandra", "processor", "publisher", "collector", "api", "dashboard",
                                "tunnel", "tunnel-api")
INFRA_COMPONENTS: tuple[str, ...] = ("kafka", "cassandra")
APP_COMPONENTS: tuple[str, ...] = ("processor", "publisher", "collector", "api")
# Scope "access": the dashboard and the public quick tunnels outlive demo sessions (stable public URL).
ACCESS_COMPONENTS: tuple[str, ...] = ("dashboard", "tunnel", "tunnel-api")
GRACEFUL_TIMEOUTS: dict[str, float] = {
    "kafka": 60.0, "cassandra": 60.0, "processor": 90.0, "publisher": 30.0, "collector": 30.0,
    "api": 10.0, "dashboard": 10.0, "tunnel": 15.0, "tunnel-api": 15.0,
}
# Expected RSS budget per component (design D §9); spawn gate = budget + RESERVE of MemAvailable.
SPAWN_BUDGET_BYTES: dict[str, int] = {
    "kafka": int(1.0 * GIB), "cassandra": int(2.6 * GIB), "processor": int(2.8 * GIB),
    "api": int(0.3 * GIB), "publisher": int(0.2 * GIB), "collector": int(0.2 * GIB), "dashboard": int(0.3 * GIB),
    "tunnel": int(0.1 * GIB), "tunnel-api": int(0.1 * GIB),
}
SPAWN_RESERVE_BYTES = 1 * GIB
OWNER_PLACEHOLDER = "@OWNER_TOKEN@"
LOG_ROTATE_BYTES = 64 * 1024 * 1024
LIVE_STATES = ("STARTING", "READY", "STOPPING")

EXIT_CODES: dict[str, int] = {
    "CONFLICT": 4, "PORT_IN_USE": 3, "PORT_IN_USE_OURS": 4, "INSUFFICIENT_MEMORY": 3, "READINESS_TIMEOUT": 5,
    "PROCESS_EXITED": 5, "SPAWN_FAILED": 5, "REGISTRY_LOCKED": 4, "UNKNOWN_COMPONENT": 2, "NOT_OWNED": 4,
    "ADOPT_NOT_FOUND": 3,
}


def _w(workdir: Any, command: str) -> str:
    return f"`{command} --workdir {workdir}`" if workdir is not None else f"`{command} --workdir W`"


def conflict_hint(component: str, workdir: Any = None) -> str:
    """How to free a component slot. `demo.py stop` deliberately keeps the API and the access components."""
    if component in INFRA_COMPONENTS:
        return f"stop everything first: {_w(workdir, 'scripts/demo.py stop --all')}, then start again"
    if component in ACCESS_COMPONENTS or component == "api":
        return (f"the API/dashboard/tunnels outlive sessions: stop them with "
                f"{_w(workdir, 'scripts/demo.py expose stop --target both')} (then "
                f"{_w(workdir, 'scripts/demo.py stop')} for the API) or {_w(workdir, 'scripts/demo.py stop --all')}, "
                "then start again")
    return f"stop the current session first: {_w(workdir, 'scripts/demo.py stop')}, then start again"


def spawn_gate_bytes(component: str) -> int:
    return SPAWN_BUDGET_BYTES.get(component, int(0.2 * GIB)) + SPAWN_RESERVE_BYTES


class SupervisorError(Exception):
    def __init__(self, code: str, message: str, hint: str | None = None, log_excerpt: str | None = None,
                 *, exit_code: int | None = None, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint
        self.log_excerpt = log_excerpt
        self.exit_code = exit_code if exit_code is not None else EXIT_CODES.get(code, 1)
        self.details = dict(details or {})


class StartAction(StrEnum):
    SPAWN = "SPAWN"
    REUSE = "REUSE"
    CONFLICT = "CONFLICT"


def workdir_tag(root: Path) -> str:
    return hashlib.sha256(str(Path(root).resolve()).encode("utf-8")).hexdigest()[:12]


def compute_fingerprint(argv: Sequence[str], env: Mapping[str, str], extra: str = "") -> str:
    payload = json.dumps({"argv": list(argv), "env": dict(sorted(env.items())), "extra": extra},
                         sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def is_owned(entry: Mapping[str, Any] | None, proc: ProcFS, boot_id: str) -> bool:
    """Identity check of a registry entry against the live process table (see module docstring)."""
    if not entry:
        return False
    pid = entry.get("wrapper_pid")
    token = entry.get("owner_token")
    ticks = entry.get("start_ticks")
    if not isinstance(pid, int) or pid <= 1 or not token or ticks is None:
        return False
    recorded_boot = entry.get("boot_id")
    if recorded_boot and boot_id and recorded_boot != boot_id:
        return False
    st = proc.stat(pid)
    if st is None or st.state in ("Z", "X", "x"):
        return False
    if st.starttime != ticks:
        return False
    cmd = proc.cmdline(pid)
    return bool(cmd) and any(token in arg for arg in cmd)


def is_owned_child(entry: Mapping[str, Any] | None, pid: int, proc: ProcFS, boot_id: str) -> bool:
    """Wrapper-based ownership of a component child that cannot carry the token on its own command line
    (e.g. ``cloudflared``): the wrapper itself must be owned (start ticks, boot id, token in ITS cmdline), the
    child's PPid must be the recorded wrapper pid and, when recorded, the child's start ticks must match."""
    if not entry or not is_owned(entry, proc, boot_id):
        return False
    st = proc.stat(pid)
    if st is None or st.state in ("Z", "X", "x"):
        return False
    if st.ppid != entry.get("wrapper_pid"):
        return False
    recorded = entry.get("child_start_ticks")
    return recorded is None or st.starttime == recorded


def decide_start(entry: Mapping[str, Any] | None, *, owned: bool, fingerprint: str) -> tuple[StartAction, str]:
    """Pure start-or-reuse decision (the port/memory gates are applied only for SPAWN)."""
    if entry is None:
        return StartAction.SPAWN, "no registry entry"
    if owned:
        if entry.get("fingerprint") == fingerprint:
            return StartAction.REUSE, f"owned process {entry.get('wrapper_pid')} with the same fingerprint"
        return StartAction.CONFLICT, (
            f"owned process {entry.get('wrapper_pid')} runs with a different configuration "
            f"(session={entry.get('session_id')}, run={entry.get('run_id')})"
        )
    return StartAction.SPAWN, "previous process is not running (exited, killed, PID reused or VM rebooted)"


def classify_port(port: int, proc: ProcFS, *, wtag: str, registered_tokens: Mapping[str, str]) -> dict[str, Any]:
    """FREE | OWNED (registered component) | OURS_UNREGISTERED (adoptable) | FOREIGN | UNKNOWN_OWNER."""
    inodes = proc.tcp_listeners().get(port, [])
    if not inodes:
        return {"port": port, "state": "FREE", "pid": None, "cmdline": None, "component": None}
    pid = None
    for inode in inodes:
        pid = proc.socket_owner(inode)
        if pid is not None:
            break
    if pid is None:
        return {"port": port, "state": "UNKNOWN_OWNER", "pid": None, "cmdline": None, "component": None}
    cmd = proc.cmdline(pid) or []
    joined = " ".join(cmd)
    for component, token in registered_tokens.items():
        if token and carries_token(proc, pid, token):
            return {"port": port, "state": "OWNED", "pid": pid, "cmdline": joined[:300], "component": component}
    prefix = f"tp:{wtag}:"
    env = proc.environ(pid) or {}
    if prefix in joined or any(prefix in v for v in env.values()):
        return {"port": port, "state": "OURS_UNREGISTERED", "pid": pid, "cmdline": joined[:300], "component": None}
    return {"port": port, "state": "FOREIGN", "pid": pid, "cmdline": joined[:300], "component": None}


def _signal_name(sig: int | None) -> str | None:
    if sig is None:
        return None
    try:
        return signal.Signals(sig).name
    except ValueError:
        return str(sig)


class Supervisor:
    def __init__(self, paths: WorkPaths, *, proc: ProcFS | None = None, python: str | None = None) -> None:
        self.paths = paths
        self.proc = proc or ProcFS()
        self.python = python or sys.executable
        self.wtag = workdir_tag(paths.root)
        self._handles: dict[int, subprocess.Popen[bytes]] = {}
        (paths.processes / "exits").mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ registry
    @contextlib.contextmanager
    def _locked(self, timeout: float = 30.0) -> Iterator[None]:
        self.paths.processes.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.paths.registry_lock, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            deadline = time.monotonic() + timeout
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError as exc:
                    if exc.errno not in (errno.EAGAIN, errno.EACCES):
                        raise
                    if time.monotonic() > deadline:
                        raise SupervisorError("REGISTRY_LOCKED", f"{self.paths.registry_lock} is held by another "
                                              "command for more than %.0f s" % timeout,
                                              hint="wait for the other infra/demo command to finish") from exc
                    time.sleep(0.1)
            yield
        finally:
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _empty_registry(self, boot_id: str) -> dict[str, Any]:
        return {"schema_version": 1, "workdir": str(self.paths.root), "boot_id": boot_id, "components": {},
                "current_session": None}

    def _load(self) -> dict[str, Any]:
        boot = self.proc.boot_id()
        reg = read_json_or_none(self.paths.registry_json)
        if not isinstance(reg, dict) or not isinstance(reg.get("components"), dict):
            reg = self._empty_registry(boot)
        if boot and reg.get("boot_id") and reg.get("boot_id") != boot:
            for entry in reg["components"].values():
                if entry.get("state") in LIVE_STATES:
                    entry["state"] = "EXITED"
                    entry["exit"] = {"exit_code": None, "signal": None, "terminated_by": "vm_reboot",
                                     "detected_at": iso_now()}
            reg["boot_id"] = boot
            reg["previous_session"] = reg.get("current_session") or reg.get("previous_session")
            reg["current_session"] = None
        reg.setdefault("boot_id", boot)
        reg.setdefault("current_session", None)
        return reg

    def _save(self, reg: Mapping[str, Any]) -> None:
        atomic_write_json(self.paths.registry_json, reg)

    def registry(self) -> dict[str, Any]:
        with self._locked():
            reg = self._load()
            self._save(reg)
            return reg

    def entry(self, component: str) -> dict[str, Any] | None:
        reg = read_json_or_none(self.paths.registry_json)
        if not isinstance(reg, dict):
            return None
        e = (reg.get("components") or {}).get(component)
        return dict(e) if isinstance(e, dict) else None

    # ------------------------------------------------------------------ sessions pointer
    def current_session(self) -> str | None:
        reg = read_json_or_none(self.paths.registry_json)
        if not isinstance(reg, dict):
            return None
        if reg.get("boot_id") and self.proc.boot_id() and reg.get("boot_id") != self.proc.boot_id():
            return None
        return reg.get("current_session")

    def set_current_session(self, session_id: str | None) -> None:
        with self._locked():
            reg = self._load()
            if session_id is None and reg.get("current_session"):
                reg["previous_session"] = reg["current_session"]
            reg["current_session"] = session_id
            self._save(reg)

    # ------------------------------------------------------------------ liveness
    def _alive(self, entry: Mapping[str, Any] | None) -> bool:
        if not entry:
            return False
        pid = entry.get("wrapper_pid")
        handle = self._handles.get(pid) if isinstance(pid, int) else None
        if handle is not None:
            handle.poll()  # reap our own direct child so it never lingers as a zombie
        return is_owned(entry, self.proc, self.proc.boot_id())

    def is_alive(self, component: str) -> bool:
        return self._alive(self.entry(component))

    def owned(self, entry: Mapping[str, Any] | None) -> bool:
        return self._alive(entry)

    def _read_exit(self, entry: Mapping[str, Any]) -> dict[str, Any] | None:
        path = Path(entry.get("exit_file") or self.paths.exit_file(entry["component"]))
        data = read_json_or_none(path)
        if not isinstance(data, dict) or data.get("owner") != entry.get("owner_token"):
            return entry.get("exit") if isinstance(entry.get("exit"), dict) else None
        if data.get("state") != "EXITED":
            return None
        sig = data.get("signal")
        out = {
            "exit_code": data.get("exit_code"), "signal": sig, "signal_name": _signal_name(sig),
            "ended_at": data.get("ended_at"), "terminated_by": data.get("terminated_by"),
        }
        if sig == signal.SIGKILL and not data.get("terminated_by"):
            out["note"] = "killed by SIGKILL without a supervisor request: possible OOM kill"
        return out

    def child_pid(self, entry: Mapping[str, Any]) -> int | None:
        data = read_json_or_none(Path(entry.get("exit_file") or self.paths.exit_file(entry["component"])))
        if isinstance(data, dict) and data.get("owner") == entry.get("owner_token"):
            pid = data.get("child_pid")
            return pid if isinstance(pid, int) else None
        return None

    def group_rss_bytes(self, entry: Mapping[str, Any]) -> int | None:
        pgid = entry.get("pgid")
        if not isinstance(pgid, int) or not self._alive(entry):
            return None
        total = 0
        for pid in self.proc.group_members(pgid):
            total += self.proc.rss_bytes(pid) or 0
        return total

    def log_tail(self, component: str, lines: int = 60, max_bytes: int = 8192) -> str:
        e = self.entry(component)
        path = Path(e["stdout_log"]) if e and e.get("stdout_log") else self.paths.log_dir(component) / "stdout.log"
        return tail_lines(path, lines, max_bytes)

    # ------------------------------------------------------------------ start
    def start(self, component: str, argv: list[str], *, env: dict, scope: str, session_id: str | None = None,
              run_id: str | None = None, ports: tuple[int, ...] = (), ready: Callable[[], bool] | None = None,
              ready_timeout: float = 240, fingerprint_extra: str = "", min_mem_available_bytes: int = 0,
              deadline_epoch: float | None = None, graceful_timeout: float | None = None,
              cwd: str | os.PathLike[str] | None = None) -> dict:
        """Start ``component`` or reuse the owned process with the same fingerprint (see module docstring).

        ``cwd`` defaults to the work directory. Returns the registry entry plus ``{"reused": bool}``; raises
        :class:`SupervisorError`.
        """
        if component not in COMPONENTS:
            raise SupervisorError("UNKNOWN_COMPONENT", f"unknown component {component!r}",
                                  hint=f"one of {', '.join(COMPONENTS)}")
        safe_component(component, "component")
        fp = compute_fingerprint(argv, env, fingerprint_extra)
        with self._locked():
            reg = self._load()
            entry = reg["components"].get(component)
            owned = self._alive(entry)
            action, reason = decide_start(entry, owned=owned, fingerprint=fp)
            log.info("%s: %s (%s)", component, action.value.lower(), reason)
            if action is StartAction.CONFLICT:
                raise SupervisorError("CONFLICT", f"{component}: {reason}",
                                      hint=conflict_hint(component, self.paths.root),
                                      details={"entry": entry})
            if action is StartAction.REUSE:
                reused_entry = dict(entry)
            else:
                if entry is not None and entry.get("state") in LIVE_STATES:
                    entry["exit"] = self._read_exit(entry) or {"exit_code": None, "signal": None,
                                                               "terminated_by": "unknown (process vanished)"}
                    entry["state"] = "EXITED"
                    entry["exited_detected_at"] = iso_now()
                    self._save(reg)
                self._check_ports(ports, reg)
                if min_mem_available_bytes:
                    avail = self.proc.meminfo().get("MemAvailable")
                    if avail is not None and avail < min_mem_available_bytes:
                        raise SupervisorError(
                            "INSUFFICIENT_MEMORY",
                            f"{component}: MemAvailable {avail / GIB:.2f} GiB < required "
                            f"{min_mem_available_bytes / GIB:.2f} GiB (budget + 1 GiB reserve)",
                            hint="stop other sessions/benchmarks or use a VM with more RAM; check "
                                 f"{_w(self.paths.root, 'scripts/demo.py status')}",
                        )
                new_entry = self._spawn(component, argv, env, fp, scope=scope, session_id=session_id,
                                        run_id=run_id, ports=ports, deadline_epoch=deadline_epoch,
                                        graceful_timeout=graceful_timeout, cwd=cwd)
                reg["components"][component] = new_entry
                self._save(reg)
        if action is StartAction.REUSE:
            if reused_entry.get("state") == "READY" and (ready is None or _safe_call(ready)):
                return {**reused_entry, "reused": True}
            log.info("%s: reused process is not ready yet; waiting", component)
            return self._wait_ready(component, reused_entry["owner_token"], ready, ready_timeout, reused=True)
        return self._wait_ready(component, new_entry["owner_token"], ready, ready_timeout, reused=False)

    def _check_ports(self, ports: Sequence[int], reg: Mapping[str, Any]) -> None:
        if not ports:
            return
        tokens = {c: e.get("owner_token", "") for c, e in reg["components"].items()
                  if e.get("state") in LIVE_STATES and self._alive(e)}
        for port in ports:
            info = classify_port(int(port), self.proc, wtag=self.wtag, registered_tokens=tokens)
            if info["state"] == "FREE":
                continue
            if info["state"] == "OWNED":
                raise SupervisorError("PORT_IN_USE_OURS", f"port {port} is used by our {info['component']} process",
                                      hint="stop that component first", details={"port": info})
            if info["state"] == "OURS_UNREGISTERED":
                raise SupervisorError(
                    "PORT_IN_USE_OURS",
                    f"port {port} is held by pid {info['pid']} started for this work directory but missing from "
                    "the registry (registry deleted or written by another boot)",
                    hint=f"run {_w(self.paths.root, 'scripts/infra.py adopt')} to re-register it "
                         "(or stop it via its own tooling)",
                    details={"port": info},
                )
            raise SupervisorError(
                "PORT_IN_USE",
                f"port {port} is already in use by pid {info['pid']}: {info['cmdline'] or 'unknown process'}",
                hint="free the port (this tool never kills foreign processes) or change the configuration",
                details={"port": info},
            )

    def _spawn(self, component: str, argv: Sequence[str], env: Mapping[str, str], fingerprint: str, *, scope: str,
               session_id: str | None, run_id: str | None, ports: Sequence[int], deadline_epoch: float | None,
               graceful_timeout: float | None, cwd: str | os.PathLike[str] | None = None) -> dict[str, Any]:
        token = f"tp:{self.wtag}:{component}:{uuid.uuid4().hex[:16]}"
        log_dir = self.paths.log_dir(component)
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "stdout.log"
        with contextlib.suppress(OSError):
            if log_path.stat().st_size > LOG_ROTATE_BYTES:
                os.replace(log_path, log_dir / "stdout.log.1")
        exit_file = self.paths.exit_file(component)
        real_argv = [str(a).replace(OWNER_PLACEHOLDER, token) for a in argv]
        real_env = {str(k): str(v).replace(OWNER_PLACEHOLDER, token) for k, v in env.items()}
        real_env["TP_OWNER_TOKEN"] = token
        real_env["TP_COMPONENT"] = component
        wrapper = [self.python, "-I", "-m", "transitpulse.runtime.runner", "--owner", token,
                   "--component", component, "--exit-file", str(exit_file),
                   "--term-timeout", str(graceful_timeout or GRACEFUL_TIMEOUTS.get(component, 30.0))]
        if deadline_epoch is not None:
            wrapper += ["--deadline", f"{deadline_epoch:.3f}"]
        wrapper += ["--", *real_argv]
        header = (f"\n===== {iso_now()} supervisor start component={component} session={session_id} "
                  f"run={run_id} token={token}\n===== argv: {' '.join(real_argv)}\n").encode()
        try:
            with open(log_path, "ab", buffering=0) as logf:
                logf.write(header)
                # Byte offset where THIS incarnation's output starts (log-based readiness ignores older runs).
                log_offset = logf.tell()
                p = subprocess.Popen(wrapper, stdin=subprocess.DEVNULL, stdout=logf, stderr=subprocess.STDOUT,
                                     cwd=str(cwd or self.paths.root), env=real_env, start_new_session=True,
                                     close_fds=True)
        except OSError as exc:
            raise SupervisorError("SPAWN_FAILED", f"cannot spawn {component}: {exc}",
                                  hint="check runtime.json paths (re-run bootstrap)") from exc
        self._handles[p.pid] = p
        ticks = None
        for _ in range(20):
            st = self.proc.stat(p.pid)
            if st is not None:
                ticks = st.starttime
                break
            if p.poll() is not None:
                break
            time.sleep(0.05)
        log.info("%s: spawned wrapper pid %s (log %s)", component, p.pid, log_path)
        return {
            "component": component, "scope": scope, "session_id": session_id, "run_id": run_id,
            "wrapper_pid": p.pid, "pid": None, "pgid": p.pid, "start_ticks": ticks, "child_start_ticks": None,
            "boot_id": self.proc.boot_id(), "owner_token": token, "argv": real_argv, "fingerprint": fingerprint,
            "stdout_log": str(log_path), "log_offset": log_offset, "exit_file": str(exit_file),
            "ports": [int(x) for x in ports], "cwd": str(cwd or self.paths.root),
            "state": "STARTING", "started_at": iso_now(), "started_epoch": time.time(), "ready_at": None,
            "exit": None, "deadline_epoch": deadline_epoch,
            "graceful_timeout": graceful_timeout or GRACEFUL_TIMEOUTS.get(component, 30.0),
        }

    def annotate(self, component: str, token: str, **fields: Any) -> dict[str, Any] | None:
        """Record extra facts (e.g. a tunnel's public URL) on the entry owned by ``token``; None if replaced."""
        return self._update(component, token, **fields)

    def owned_child_pid(self, component: str) -> int | None:
        """The component's direct child pid, verified through its wrapper (see :func:`is_owned_child`)."""
        entry = self.entry(component)
        if not entry:
            return None
        pid = entry.get("pid") or self.child_pid(entry)
        if isinstance(pid, int) and is_owned_child(entry, pid, self.proc, self.proc.boot_id()):
            return pid
        return None

    def _update(self, component: str, token: str, **fields: Any) -> dict[str, Any] | None:
        with self._locked():
            reg = self._load()
            e = reg["components"].get(component)
            if not e or e.get("owner_token") != token:
                return None
            e.update(fields)
            self._save(reg)
            return dict(e)

    def _wait_ready(self, component: str, token: str, ready: Callable[[], bool] | None, timeout: float,
                    *, reused: bool) -> dict[str, Any]:
        t0 = time.monotonic()
        last_progress = t0
        child_recorded = False
        while True:
            entry = self.entry(component)
            if not entry or entry.get("owner_token") != token:
                raise SupervisorError("CONFLICT", f"{component}: registry entry was replaced by another command",
                                      hint=f"check {_w(self.paths.root, 'scripts/demo.py status')}")
            if not child_recorded:
                cpid = self.child_pid(entry)
                if cpid is not None:
                    st = self.proc.stat(cpid)
                    self._update(component, token, pid=cpid, child_start_ticks=st.starttime if st else None)
                    child_recorded = True
            alive = self._alive(entry)
            ok = (alive and time.monotonic() - t0 >= 2.0) if ready is None else _safe_call(ready)
            if ok:
                elapsed = round(time.monotonic() - t0, 1)
                updated = self._update(component, token, state="READY", ready_at=entry.get("ready_at") or iso_now())
                log.info("%s: READY after %.1f s", component, elapsed)
                return {**(updated or entry), "reused": reused, "ready_seconds": elapsed}
            if not alive:
                time.sleep(0.3)  # let the runner finish writing its exit file
                exit_info = self._read_exit(entry) or {"exit_code": None, "signal": None}
                if not reused:
                    self._update(component, token, state="FAILED", exit=exit_info)
                code = exit_info.get("exit_code")
                sig = exit_info.get("signal_name") or exit_info.get("signal")
                raise SupervisorError(
                    "PROCESS_EXITED",
                    f"{component} exited before becoming ready (exit code {code}"
                    f"{', signal ' + str(sig) if sig else ''}{'; ' + exit_info['note'] if exit_info.get('note') else ''})",
                    hint=f"see {entry.get('stdout_log')} "
                         f"({_w(self.paths.root, 'scripts/demo.py logs --component ' + component)})",
                    log_excerpt=self.log_tail(component),
                    details={"exit": exit_info},
                )
            elapsed = time.monotonic() - t0
            if elapsed > timeout:
                excerpt = self.log_tail(component)
                if not reused:
                    log.warning("%s: not ready after %.0f s; stopping the process this command started", component,
                                elapsed)
                    self._terminate(entry, GRACEFUL_TIMEOUTS.get(component, 30.0))
                    self._update(component, token, state="FAILED",
                                 exit=self._read_exit(entry) or {"exit_code": None, "terminated_by": "readiness_timeout"})
                raise SupervisorError(
                    "READINESS_TIMEOUT", f"{component} did not become ready within {int(timeout)} s",
                    hint=f"inspect {entry.get('stdout_log')}; increase --ready-timeout if the VM is slow",
                    log_excerpt=excerpt,
                )
            if time.monotonic() - last_progress >= 5.0:
                log.info("waiting for %s to become ready (%.0f/%.0f s)", component, elapsed, timeout)
                last_progress = time.monotonic()
            time.sleep(1.0)

    # ------------------------------------------------------------------ stop
    def stop(self, component: str, *, timeout: float | None = None) -> dict:
        with self._locked():
            reg = self._load()
            entry = reg["components"].get(component)
            if entry is None:
                return {"component": component, "state": "NOT_REGISTERED", "stopped": False}
            if not self._alive(entry):
                if entry.get("state") in LIVE_STATES:
                    entry["exit"] = self._read_exit(entry) or entry.get("exit")
                    entry["state"] = "EXITED"
                    self._save(reg)
                return {"component": component, "state": entry.get("state"), "stopped": False,
                        "already_stopped": True, "exit": entry.get("exit")}
            entry["state"] = "STOPPING"
            self._save(reg)
            entry = dict(entry)
        t0 = time.monotonic()
        grace = float(timeout if timeout is not None else
                      entry.get("graceful_timeout") or GRACEFUL_TIMEOUTS.get(component, 30.0))
        log.info("%s: stopping (SIGTERM, grace %.0f s)", component, grace)
        forced = self._terminate(entry, grace)
        exit_info = self._read_exit(entry)
        if exit_info is None:
            exit_info = {"exit_code": None, "signal": signal.SIGKILL if forced else None,
                         "terminated_by": "supervisor_force_kill" if forced else "supervisor"}
        self._update(component, entry["owner_token"], state="EXITED", exit=exit_info, stopped_at=iso_now())
        return {"component": component, "state": "STOPPED", "stopped": True, "forced": forced,
                "seconds": round(time.monotonic() - t0, 1), "exit": exit_info}

    def _terminate(self, entry: Mapping[str, Any], grace: float) -> bool:
        """SIGTERM the verified wrapper, wait, then force-kill the token-carrying group. Returns forced flag."""
        token = str(entry.get("owner_token"))
        pid = int(entry["wrapper_pid"])
        pgid = entry.get("pgid")
        forced = False
        if self._alive(entry):
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGTERM)
            if not self._wait_dead(entry, grace):
                forced = True
                log.warning("%s: still running after %.0f s; force-killing its process group",
                            entry.get("component"), grace)
                self._kill_group(pgid, token, signal.SIGKILL, wrapper_pid=pid, entry=entry)
                self._wait_dead(entry, 10.0)
        # Leftover group members (e.g. a JVM whose parent died) that carry our token.
        if isinstance(pgid, int) and pgid > 1:
            leftovers = [p for p in self.proc.group_members(pgid) if carries_token(self.proc, p, token)]
            if leftovers:
                log.info("%s: terminating %d leftover process(es) of the group", entry.get("component"), len(leftovers))
                self._kill_group(pgid, token, signal.SIGTERM, wrapper_pid=pid, entry=entry)
                end = time.monotonic() + 10.0
                while time.monotonic() < end and any(
                        carries_token(self.proc, p, token) for p in self.proc.group_members(pgid)):
                    time.sleep(0.25)
                if any(carries_token(self.proc, p, token) for p in self.proc.group_members(pgid)):
                    forced = True
                    self._kill_group(pgid, token, signal.SIGKILL, wrapper_pid=pid, entry=entry)
        return forced

    def _kill_group(self, pgid: Any, token: str, sig: int, *, wrapper_pid: int,
                    entry: Mapping[str, Any] | None = None) -> None:
        """Signal our processes in ``pgid``: token carriers (cmdline/environ) and, for children that cannot
        carry the token (cloudflared), direct children of the verified wrapper. Foreign members are never
        signalled; ``killpg`` is used only when every member is ours."""
        if not isinstance(pgid, int) or pgid <= 1 or pgid == os.getpgrp():
            return
        members = self.proc.group_members(pgid)
        if not members:
            return
        boot = self.proc.boot_id()
        carriers = [p for p in members if carries_token(self.proc, p, token)
                    or (entry is not None and is_owned_child(entry, p, self.proc, boot))]
        if len(carriers) == len(members):
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(pgid, sig)
            return
        # Mixed group: never signal processes that do not carry our token.
        for p in carriers:
            if p > 1 and p != os.getpid():
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.kill(p, sig)

    def _wait_dead(self, entry: Mapping[str, Any], timeout: float) -> bool:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if not self._alive(entry):
                return True
            time.sleep(0.25)
        return not self._alive(entry)

    def wait_exit(self, component: str, timeout: float) -> int | None:
        entry = self.entry(component)
        if entry is None:
            return None
        if not self._wait_dead(entry, timeout):
            return None
        time.sleep(0.2)
        info = self._read_exit(entry) or {}
        with self._locked():
            reg = self._load()
            e = reg["components"].get(component)
            if e and e.get("owner_token") == entry.get("owner_token") and e.get("state") in LIVE_STATES:
                e["state"] = "EXITED"
                e["exit"] = info or e.get("exit")
                self._save(reg)
        code = info.get("exit_code")
        if code is None and info.get("signal"):
            return 128 + int(info["signal"])
        return code

    # ------------------------------------------------------------------ status / adopt
    def status(self) -> dict:
        with self._locked():
            reg = self._load()
            changed = False
            out: dict[str, Any] = {}
            for name, e in reg["components"].items():
                alive = self._alive(e)
                if not alive and e.get("state") in LIVE_STATES:
                    e["exit"] = self._read_exit(e) or e.get("exit")
                    e["state"] = "EXITED"
                    changed = True
                view = dict(e)
                view["alive"] = alive
                view["rss_bytes"] = self.group_rss_bytes(e) if alive else None
                out[name] = view
            if changed:
                self._save(reg)
        return {"workdir": reg.get("workdir"), "boot_id": reg.get("boot_id"),
                "current_session": reg.get("current_session"), "components": out}

    def adopt(self, component: str, *, ports: Sequence[int], fingerprint_argv: Sequence[str],
              fingerprint_env: Mapping[str, str], fingerprint_extra: str = "", scope: str = "infra") -> dict:
        """Re-register a process of THIS work directory (token prefix match) that listens on ``ports``."""
        prefix = f"tp:{self.wtag}:{component}:"
        found_pid = None
        token = None
        listeners = self.proc.tcp_listeners()
        for port in ports:
            for inode in listeners.get(int(port), []):
                pid = self.proc.socket_owner(inode)
                if pid is None:
                    continue
                cmd = " ".join(self.proc.cmdline(pid) or [])
                env = self.proc.environ(pid) or {}
                candidates = [cmd, *env.values()]
                for text in candidates:
                    idx = text.find(prefix)
                    if idx >= 0:
                        token = text[idx:].split()[0].split(",")[0]
                        found_pid = pid
                        break
                if found_pid:
                    break
            if found_pid:
                break
        if found_pid is None or token is None:
            raise SupervisorError("ADOPT_NOT_FOUND", f"no {component} process of this work directory listens on "
                                  f"{list(ports)}", hint=f"start it with {_w(self.paths.root, 'scripts/infra.py start')}")
        wrapper = found_pid
        st = self.proc.stat(found_pid)
        if st is not None and st.ppid > 1:
            parent_cmd = self.proc.cmdline(st.ppid) or []
            if any(token in a for a in parent_cmd):
                wrapper = st.ppid
        wst = self.proc.stat(wrapper)
        if wst is None:
            raise SupervisorError("ADOPT_NOT_FOUND", f"process {wrapper} vanished during adoption")
        entry = {
            "component": component, "scope": scope, "session_id": None, "run_id": None, "wrapper_pid": wrapper,
            "pid": found_pid, "pgid": wst.pgrp, "start_ticks": wst.starttime,
            "child_start_ticks": st.starttime if st else None, "boot_id": self.proc.boot_id(), "owner_token": token,
            "argv": self.proc.cmdline(found_pid) or [],
            "fingerprint": compute_fingerprint(fingerprint_argv, fingerprint_env, fingerprint_extra),
            "stdout_log": str(self.paths.log_dir(component) / "stdout.log"),
            "exit_file": str(self.paths.exit_file(component)), "ports": [int(p) for p in ports], "state": "READY",
            "started_at": None, "started_epoch": None, "ready_at": iso_now(), "exit": None, "adopted": True,
            "adopted_at": iso_now(), "graceful_timeout": GRACEFUL_TIMEOUTS.get(component, 30.0),
        }
        if wrapper == found_pid and not any(token in a for a in (self.proc.cmdline(wrapper) or [])):
            # Token only in the environment: identity must still be checkable through the cmdline.
            raise SupervisorError("ADOPT_NOT_FOUND", f"pid {found_pid} carries the token only in its environment; "
                                  "cannot verify identity safely", hint="stop it manually and start again")
        with self._locked():
            reg = self._load()
            current = reg["components"].get(component)
            if current is not None and self._alive(current):
                raise SupervisorError("CONFLICT", f"{component} is already registered and alive")
            reg["components"][component] = entry
            self._save(reg)
        log.info("%s: adopted wrapper pid %s (listener pid %s)", component, wrapper, found_pid)
        return entry


def _safe_call(fn: Callable[[], bool]) -> bool:
    try:
        return bool(fn())
    except Exception as exc:  # noqa: BLE001 - readiness probes fail until the service is up
        log.debug("readiness probe raised %s: %s", type(exc).__name__, exc)
        return False
