"""Small file-system helpers for the static package (atomic writes, JSON documents)."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

__all__ = ["atomic_write_bytes", "atomic_write_json", "read_json", "fsync_dir"]


def fsync_dir(directory: Path) -> None:
    """Best-effort fsync of a directory entry (makes a rename durable on POSIX)."""
    try:
        fd = os.open(str(directory), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes) -> Path:
    """Write ``data`` to ``path`` through a temp file in the same directory and ``os.replace``."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    fsync_dir(path.parent)
    return path


def atomic_write_json(path: Path, doc: Any, *, indent: int | None = 2) -> Path:
    """Atomically write a JSON document (UTF-8, sorted keys for deterministic diffs)."""
    text = json.dumps(doc, ensure_ascii=False, indent=indent, sort_keys=True, default=str)
    return atomic_write_bytes(path, (text + "\n").encode("utf-8"))


def read_json(path: Path) -> Any:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)
