"""Linux ``/proc`` helpers behind a replaceable reader (tests inject a fake).

Used by the supervisor (process identity, process groups, listeners), the doctor (memory, cgroup,
ports) and the status commands. Everything here is read-only: nothing in this module sends signals.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path

GIB = 1024**3
MIB = 1024**2

LISTEN_STATE = "0A"


@dataclass(frozen=True)
class ProcStat:
    """Subset of ``/proc/<pid>/stat`` (field numbers as in proc(5))."""

    pid: int
    comm: str
    state: str        # field 3
    ppid: int         # field 4
    pgrp: int         # field 5
    session: int      # field 6
    starttime: int    # field 22, clock ticks since boot


def parse_stat(text: str) -> ProcStat:
    """Parse ``/proc/<pid>/stat``. ``comm`` may contain spaces and parentheses, so split after the LAST ')'."""
    lp = text.index("(")
    rp = text.rindex(")")
    pid = int(text[:lp].strip())
    comm = text[lp + 1 : rp]
    rest = text[rp + 1 :].split()
    # rest[0] = field 3 (state) ... rest[19] = field 22 (starttime)
    if len(rest) < 20:
        raise ValueError(f"truncated /proc stat line for pid {pid}")
    return ProcStat(
        pid=pid,
        comm=comm,
        state=rest[0],
        ppid=int(rest[1]),
        pgrp=int(rest[2]),
        session=int(rest[3]),
        starttime=int(rest[19]),
    )


def parse_net_tcp(text: str) -> list[tuple[int, int]]:
    """Return ``(port, socket inode)`` for every LISTEN socket of a ``/proc/net/tcp{,6}`` table."""
    out: list[tuple[int, int]] = []
    for line in text.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 10 or parts[3] != LISTEN_STATE:
            continue
        local = parts[1]
        try:
            port = int(local.rsplit(":", 1)[1], 16)
            inode = int(parts[9])
        except (IndexError, ValueError):
            continue
        out.append((port, inode))
    return out


def parse_meminfo(text: str) -> dict[str, int]:
    """``/proc/meminfo`` -> bytes per key (values reported in kB are converted)."""
    out: dict[str, int] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, rest = line.split(":", 1)
        fields = rest.split()
        if not fields:
            continue
        try:
            value = int(fields[0])
        except ValueError:
            continue
        if len(fields) > 1 and fields[1].lower() == "kb":
            value *= 1024
        out[key.strip()] = value
    return out


class ProcFS:
    """Thin reader over a procfs root. Every method returns ``None``/empty instead of raising for vanished pids."""

    def __init__(self, root: str | os.PathLike[str] = "/proc") -> None:
        self.root = Path(root)

    # ---- per-process
    def stat(self, pid: int) -> ProcStat | None:
        try:
            return parse_stat((self.root / str(int(pid)) / "stat").read_text(encoding="utf-8", errors="replace"))
        except (OSError, ValueError, IndexError):
            return None

    def cmdline(self, pid: int) -> list[str] | None:
        try:
            raw = (self.root / str(int(pid)) / "cmdline").read_bytes()
        except OSError:
            return None
        parts = raw.split(b"\0")
        if parts and parts[-1] == b"":
            parts = parts[:-1]
        return [p.decode("utf-8", errors="surrogateescape") for p in parts]

    def environ(self, pid: int) -> dict[str, str] | None:
        try:
            raw = (self.root / str(int(pid)) / "environ").read_bytes()
        except OSError:
            return None
        env: dict[str, str] = {}
        for item in raw.split(b"\0"):
            if not item or b"=" not in item:
                continue
            k, v = item.split(b"=", 1)
            env[k.decode("utf-8", errors="surrogateescape")] = v.decode("utf-8", errors="surrogateescape")
        return env

    def rss_bytes(self, pid: int) -> int | None:
        try:
            text = (self.root / str(int(pid)) / "status").read_text(encoding="utf-8", errors="replace")
        except OSError:
            return None
        for line in text.splitlines():
            if line.startswith("VmRSS:"):
                fields = line.split()
                try:
                    return int(fields[1]) * 1024
                except (IndexError, ValueError):
                    return None
        return None

    def fd_targets(self, pid: int) -> list[str]:
        fd_dir = self.root / str(int(pid)) / "fd"
        try:
            names = os.listdir(fd_dir)
        except OSError:
            return []
        out: list[str] = []
        for name in names:
            try:
                out.append(os.readlink(fd_dir / name))
            except OSError:
                continue
        return out

    # ---- system wide
    def pids(self) -> list[int]:
        try:
            return sorted(int(n) for n in os.listdir(self.root) if n.isdigit())
        except OSError:
            return []

    def boot_id(self) -> str:
        try:
            return (self.root / "sys" / "kernel" / "random" / "boot_id").read_text(encoding="utf-8").strip()
        except OSError:
            return ""

    def meminfo(self) -> dict[str, int]:
        try:
            return parse_meminfo((self.root / "meminfo").read_text(encoding="utf-8", errors="replace"))
        except OSError:
            return {}

    def tcp_listeners(self) -> dict[int, list[int]]:
        """port -> socket inodes in LISTEN state (IPv4 and IPv6)."""
        out: dict[int, list[int]] = {}
        for name in ("tcp", "tcp6"):
            try:
                text = (self.root / "net" / name).read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            for port, inode in parse_net_tcp(text):
                out.setdefault(port, []).append(inode)
        return out

    def socket_owner(self, inode: int) -> int | None:
        """PID holding ``socket:[inode]`` (needs permission to read other processes' fd tables)."""
        target = f"socket:[{inode}]"
        for pid in self.pids():
            if target in self.fd_targets(pid):
                return pid
        return None

    def group_members(self, pgid: int) -> list[int]:
        out: list[int] = []
        for pid in self.pids():
            st = self.stat(pid)
            if st is not None and st.pgrp == pgid and st.state not in ("Z", "X", "x"):
                out.append(pid)
        return out

    def vm_max_map_count(self) -> int | None:
        try:
            return int((self.root / "sys" / "vm" / "max_map_count").read_text().strip())
        except (OSError, ValueError):
            return None


def carries_token(proc: ProcFS, pid: int, token: str) -> bool:
    """True when the process command line or its initial environment contains ``token``."""
    cmd = proc.cmdline(pid)
    if cmd and any(token in arg for arg in cmd):
        return True
    env = proc.environ(pid)
    if env and any(token in v for v in env.values()):
        return True
    return False


def cgroup_memory_limit(root: str | os.PathLike[str] = "/sys/fs/cgroup") -> int | None:
    """Effective cgroup memory limit in bytes (v2 ``memory.max`` or v1 ``memory.limit_in_bytes``); None = unlimited."""
    base = Path(root)
    candidates = [base / "memory.max", base / "memory" / "memory.limit_in_bytes"]
    for path in candidates:
        try:
            text = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if text == "max":
            return None
        try:
            value = int(text)
        except ValueError:
            continue
        # cgroup v1 reports "unlimited" as a huge page-aligned number.
        if value >= 1 << 60:
            return None
        return value
    return None


@dataclass(frozen=True)
class DiskInfo:
    path: str
    total_bytes: int
    free_bytes: int
    inodes_free: int | None


def disk_info(path: str | os.PathLike[str]) -> DiskInfo:
    """Free space of the filesystem holding ``path`` (walks up to the first existing parent)."""
    p = Path(path)
    while not p.exists() and p != p.parent:
        p = p.parent
    usage = shutil.disk_usage(p)
    inodes: int | None
    try:
        st = os.statvfs(p)
        inodes = int(st.f_favail) if st.f_files else None
    except OSError:
        inodes = None
    return DiskInfo(path=str(p), total_bytes=int(usage.total), free_bytes=int(usage.free), inodes_free=inodes)


def cpu_count() -> int:
    try:
        return len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        return os.cpu_count() or 1
