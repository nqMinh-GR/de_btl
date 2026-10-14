"""In-memory /proc used by the runtime tests (no real processes are inspected or signalled)."""

from __future__ import annotations

from dataclasses import dataclass, field

from transitpulse.runtime.proc import ProcFS, ProcStat


@dataclass
class FakeProcess:
    pid: int
    cmdline: list[str]
    starttime: int = 1000
    state: str = "S"
    ppid: int = 1
    pgrp: int | None = None
    environ: dict[str, str] = field(default_factory=dict)
    fds: list[str] = field(default_factory=list)
    rss: int = 100 * 1024 * 1024


class FakeProcFS(ProcFS):
    def __init__(self, boot_id: str = "boot-1") -> None:
        super().__init__("/nonexistent-proc")
        self.procs: dict[int, FakeProcess] = {}
        self._boot = boot_id
        self.listeners: dict[int, list[int]] = {}
        self.mem: dict[str, int] = {"MemTotal": 12 * 1024**3, "MemAvailable": 10 * 1024**3}

    def add(self, proc: FakeProcess) -> FakeProcess:
        self.procs[proc.pid] = proc
        return proc

    def stat(self, pid: int) -> ProcStat | None:
        p = self.procs.get(pid)
        if p is None:
            return None
        return ProcStat(pid=p.pid, comm=p.cmdline[0] if p.cmdline else "?", state=p.state, ppid=p.ppid,
                        pgrp=p.pgrp if p.pgrp is not None else p.pid, session=p.pid, starttime=p.starttime)

    def cmdline(self, pid: int) -> list[str] | None:
        p = self.procs.get(pid)
        return list(p.cmdline) if p else None

    def environ(self, pid: int) -> dict[str, str] | None:
        p = self.procs.get(pid)
        return dict(p.environ) if p else None

    def rss_bytes(self, pid: int) -> int | None:
        p = self.procs.get(pid)
        return p.rss if p else None

    def fd_targets(self, pid: int) -> list[str]:
        p = self.procs.get(pid)
        return list(p.fds) if p else []

    def pids(self) -> list[int]:
        return sorted(self.procs)

    def boot_id(self) -> str:
        return self._boot

    def meminfo(self) -> dict[str, int]:
        return dict(self.mem)

    def tcp_listeners(self) -> dict[int, list[int]]:
        return {k: list(v) for k, v in self.listeners.items()}

    def listen(self, port: int, inode: int, pid: int) -> None:
        self.listeners.setdefault(port, []).append(inode)
        self.procs[pid].fds.append(f"socket:[{inode}]")
