"""Small shared helpers for the native runtime (atomic files, hashing, time, git, log tails)."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CHUNK = 1024 * 1024


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return iso(utc_now())


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso(text: str) -> datetime:
    value = text.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError(f"timestamp without timezone: {text!r}")
    return dt.astimezone(UTC)


def atomic_write_bytes(path: Path, data: bytes, *, mode: int | None = None) -> None:
    """Write ``data`` to ``path`` via a temp file in the same directory + fsync + os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    if mode is not None:
        os.chmod(tmp, mode)
    os.replace(tmp, path)


def atomic_write_text(path: Path, text: str, *, mode: int | None = None) -> None:
    atomic_write_bytes(path, text.encode("utf-8"), mode=mode)


def atomic_write_json(path: Path, data: Any) -> None:
    atomic_write_text(path, json.dumps(data, indent=2, sort_keys=True, default=str, ensure_ascii=False) + "\n")


def read_json(path: Path) -> Any:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def read_json_or_none(path: Path) -> Any:
    try:
        return read_json(path)
    except (OSError, ValueError):
        return None


def hash_file(path: Path, algorithm: str = "sha256") -> str:
    h = hashlib.new(algorithm)
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sha256_file(path: Path) -> str:
    return hash_file(path, "sha256")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def tail_lines(path: Path, lines: int = 60, max_bytes: int = 8192) -> str:
    """Last ``lines`` lines of a text file, bounded to ``max_bytes`` (empty string when unreadable)."""
    try:
        size = path.stat().st_size
        with open(path, "rb") as fh:
            read = min(size, max(max_bytes * 4, 65536))
            fh.seek(size - read)
            data = fh.read()
    except OSError:
        return ""
    text = data.decode("utf-8", errors="replace")
    dq: deque[str] = deque(text.splitlines(), maxlen=max(1, lines))
    out = "\n".join(dq)
    if len(out.encode("utf-8")) > max_bytes:
        out = out.encode("utf-8")[-max_bytes:].decode("utf-8", errors="replace")
    return out


def git_info(repo_dir: Path) -> dict[str, Any]:
    """``{commit, dirty, reason}`` for a checkout; commit is None (with a reason) when unavailable."""
    git = shutil.which("git")
    if git is None:
        return {"commit": None, "dirty": None, "reason": "git executable not found"}
    if not (repo_dir / ".git").exists():
        return {"commit": None, "dirty": None, "reason": "not a git checkout"}
    try:
        head = subprocess.run([git, "-C", str(repo_dir), "rev-parse", "HEAD"], capture_output=True, text=True,
                              timeout=20, check=False, stdin=subprocess.DEVNULL)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"commit": None, "dirty": None, "reason": f"git failed: {exc}"}
    commit = head.stdout.strip()
    if head.returncode != 0 or not re.fullmatch(r"[0-9a-f]{40}", commit):
        return {"commit": None, "dirty": None, "reason": "not a git checkout"}
    dirty: bool | None
    changed: list[str] = []
    try:
        st = subprocess.run([git, "-C", str(repo_dir), "status", "--porcelain", "--untracked-files=no"],
                            capture_output=True, text=True, timeout=30, check=False, stdin=subprocess.DEVNULL)
        changed = [ln[3:] for ln in st.stdout.splitlines() if ln.strip()]
        dirty = bool(changed) if st.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        dirty = None
    return {"commit": commit, "dirty": dirty, "changed_files": changed[:50], "reason": None}


def git_is_tracked(repo_dir: Path, relpath: str) -> bool | None:
    git = shutil.which("git")
    if git is None or not (repo_dir / ".git").exists():
        return None
    try:
        r = subprocess.run([git, "-C", str(repo_dir), "ls-files", "--error-unmatch", relpath], capture_output=True,
                           text=True, timeout=20, check=False, stdin=subprocess.DEVNULL)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return r.returncode == 0


_SAFE_PATH = re.compile(r"^/[A-Za-z0-9._+/-]*$")


def check_safe_path(path: Path, what: str) -> None:
    """Native launchers (bash scripts, Ivy URIs) break on whitespace/quotes: require a conservative charset."""
    text = str(path)
    if not _SAFE_PATH.match(text):
        raise ValueError(
            f"{what} {text!r} contains characters that the native launchers cannot handle "
            "(allowed: letters, digits, '.', '_', '+', '-', '/'); choose another directory"
        )


def human_bytes(n: int | None) -> str:
    if n is None:
        return "n/a"
    value = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TiB"
