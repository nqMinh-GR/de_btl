"""Small file helpers shared by the replay publisher, control channel and synthetic generators.

* Atomic writes: temp file in the same directory, fsync, ``os.replace``, directory fsync.
* Deterministic gzip (``mtime=0``, empty file name, level 6) as the collector does (D-E02).
* ``manifest_sha256``: sha256 of the canonical manifest JSON without the ``manifest_sha256`` field.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from transitpulse.contracts.ids import canonical_json, sha256_hex

_CHUNK = 1 << 20


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(_CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def fsync_dir(directory: Path) -> None:
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes, *, fsync: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex[:8]}")
    try:
        with open(tmp, "xb") as fh:
            fh.write(data)
            fh.flush()
            if fsync:
                os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
        raise
    if fsync:
        fsync_dir(path.parent)


def json_bytes(obj: Any, *, pretty: bool = True) -> bytes:
    if pretty:
        text = json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
    else:
        text = json.dumps(obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False, allow_nan=False)
    return text.encode("utf-8")


def atomic_write_json(path: Path, obj: Any, *, fsync: bool = True, pretty: bool = True) -> None:
    atomic_write_bytes(path, json_bytes(obj, pretty=pretty), fsync=fsync)


def read_json(path: Path) -> Any:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def gzip_deterministic(body: bytes) -> bytes:
    """Gzip with mtime=0 and no file name so identical bodies give identical bytes on one platform."""
    buf = io.BytesIO()
    with gzip.GzipFile(filename="", fileobj=buf, mode="wb", mtime=0, compresslevel=6) as gz:
        gz.write(body)
    return buf.getvalue()


def manifest_sha256(manifest: Mapping[str, Any]) -> str:
    """sha256 hex of the canonical manifest JSON without the ``manifest_sha256`` field."""
    body = {k: v for k, v in manifest.items() if k != "manifest_sha256"}
    return sha256_hex(canonical_json(body))


def append_jsonl(path: Path, obj: Mapping[str, Any]) -> None:
    """Append one compact JSON line (no fsync: logs, not state)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str) + "\n"
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(line)
