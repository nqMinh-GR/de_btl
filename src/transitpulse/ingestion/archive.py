"""Raw archive of source responses (design E §1.2, decisions D-E02/D-E03).

Layout under ``RAW_ROOT`` (``W/data/raw``)::

    <run_id>/<feed_type>/<YYYY-MM-DD fetched_at UTC>/<capture_id>.json.gz      gzip(decoded body), mtime=0
    <run_id>/<feed_type>/<YYYY-MM-DD>/<capture_id>.meta.json                   sidecar metadata
    <run_id>/<feed_type>/<YYYY-MM-DD>/errors/<attempt_id>.body.gz (+ .meta.json) non-2xx bodies (<= 64 KiB)
    <run_id>/capture_index.jsonl                                               one line per capture/fetch event
    <run_id>/quarantine/                                                       orphan temp files (never deleted)

* ``raw_sha256`` is the sha256 of the decoded entity body (Content-Encoding removed); ``stored_sha256``
  is the sha256 of the ``.gz`` file. The gzip header carries no file name and ``mtime=0`` so the
  stored bytes are deterministic.
* Every file is written ``tmp -> fsync -> os.replace -> fsync(dir)``; index lines are appended with
  ``O_APPEND`` + fsync. Raw files are never overwritten with different content and never deleted.
* ``raw_path`` values are POSIX paths relative to ``RAW_ROOT`` built only from validated identifiers.
  :meth:`RawArchive.resolve` rejects absolute paths, traversal and any component that does not match
  the layout (it also accepts the ``<feed>/<date>/<file>`` form used inside recording directories).
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import re
import threading
import time
import uuid
import zlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from transitpulse.contracts.enums import FEED_ABBR
from transitpulse.contracts.ids import sha256_hex
from transitpulse.runtime.paths import resolve_under, safe_component

log = logging.getLogger(__name__)

CAPTURE_SUFFIX = ".json.gz"
META_SUFFIX = ".meta.json"
ERROR_BODY_SUFFIX = ".body.gz"
ERRORS_DIR = "errors"
INDEX_NAME = "capture_index.jsonl"
QUARANTINE_DIR = "quarantine"
STATIC_LINK_DIR = "static"
ERROR_BODY_MAX_BYTES = 64 * 1024
DEFAULT_MAX_DECODED_BYTES = 64 * 1024 * 1024
GZIP_LEVEL = 6

_FEEDS = frozenset(FEED_ABBR)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CAPTURE_RE = re.compile(r"^(vp|tu|al)-\d{8}T\d{12}Z-[0-9a-f]{12}$")
_ATTEMPT_RE = re.compile(r"^(vp|tu|al)-\d{8}T\d{12}Z-e\d{1,3}$")
_TMP_MARKER = ".tmp-"


class RawIntegrityError(ValueError):
    """A raw file is unreadable or its sha256 does not match the recorded value."""


@dataclass(frozen=True)
class RawRef:
    raw_path: str
    raw_sha256: str
    raw_bytes: int
    stored_sha256: str
    stored_bytes: int


# --------------------------------------------------------------------------------------------------
# Durable file helpers (shared with recording/collector state files)
# --------------------------------------------------------------------------------------------------

def iso_us(dt: datetime) -> str:
    """ISO-8601 UTC with microseconds and ``Z`` (sidecars keep full capture precision)."""
    if dt.tzinfo is None:
        raise ValueError("naive datetime is not allowed")
    return dt.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def fsync_dir(path: Path) -> None:
    """fsync a directory so that a rename inside it is durable (best effort on non-POSIX)."""
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` via a unique temp file, fsync, ``os.replace`` and directory fsync."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}{_TMP_MARKER}{os.getpid()}-{uuid.uuid4().hex[:8]}")
    try:
        with open(tmp, "xb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
        raise
    fsync_dir(path.parent)


def atomic_write_json(path: Path, obj: Any, *, indent: int | None = None) -> None:
    text = json.dumps(obj, sort_keys=True, indent=indent, ensure_ascii=False, allow_nan=False,
                      separators=None if indent else (",", ":"))
    atomic_write_bytes(path, (text + "\n").encode("utf-8"))


def gzip_bytes(body: bytes) -> bytes:
    """Deterministic gzip (no file name, mtime 0)."""
    buf = io.BytesIO()
    with gzip.GzipFile(filename="", fileobj=buf, mode="wb", mtime=0, compresslevel=GZIP_LEVEL) as gz:
        gz.write(body)
    return buf.getvalue()


def gunzip_bounded(data_or_path: bytes | Path, max_bytes: int) -> bytes:
    """Decompress gzip data with a hard output cap; raises :class:`RawIntegrityError` on failure."""
    try:
        if isinstance(data_or_path, Path):
            with gzip.open(data_or_path, "rb") as fh:
                out = fh.read(max_bytes + 1)
        else:
            with gzip.GzipFile(fileobj=io.BytesIO(data_or_path), mode="rb") as fh:
                out = fh.read(max_bytes + 1)
    except (OSError, EOFError, zlib.error) as exc:
        raise RawIntegrityError(f"cannot decompress raw file: {exc}") from exc
    if len(out) > max_bytes:
        raise RawIntegrityError(f"decompressed raw file exceeds {max_bytes} bytes")
    return out


# --------------------------------------------------------------------------------------------------
# Path validation
# --------------------------------------------------------------------------------------------------

def _check_date(date: str) -> str:
    if not _DATE_RE.match(date):
        raise ValueError(f"invalid date directory {date!r}")
    return date


def _check_feed(feed_type: str) -> str:
    if feed_type not in _FEEDS:
        raise ValueError(f"invalid feed_type {feed_type!r}")
    return feed_type


def check_capture_id(capture_id: str, feed_type: str | None = None) -> str:
    m = _CAPTURE_RE.match(capture_id or "")
    if not m:
        raise ValueError(f"invalid capture_id {capture_id!r}")
    if feed_type is not None and FEED_ABBR[feed_type] != m.group(1):
        raise ValueError(f"capture_id {capture_id!r} does not belong to feed {feed_type!r}")
    return capture_id


def validate_raw_relpath(raw_path: str) -> PurePosixPath:
    """Validate the structure of a stored raw path; returns it as a PurePosixPath.

    Accepted forms (optionally prefixed by a run_id component)::

        [<run_id>/]<feed>/<date>/<capture_id>.json.gz | <capture_id>.meta.json
        [<run_id>/]<feed>/<date>/errors/<attempt_id>.body.gz | <attempt_id>.meta.json
    """
    if not isinstance(raw_path, str) or not raw_path or "\\" in raw_path or "\x00" in raw_path:
        raise ValueError(f"unsafe raw path {raw_path!r}")
    rel = PurePosixPath(raw_path)
    parts = rel.parts
    if rel.is_absolute() or any(p in ("", ".", "..") for p in parts):
        raise ValueError(f"unsafe raw path {raw_path!r}")
    if len(parts) < 3:
        raise ValueError(f"raw path {raw_path!r} does not match the archive layout")
    name = parts[-1]
    if len(parts) >= 4 and parts[-2] == ERRORS_DIR:
        base = parts[:-2]
        if name.endswith(ERROR_BODY_SUFFIX):
            stem = name[: -len(ERROR_BODY_SUFFIX)]
        elif name.endswith(META_SUFFIX):
            stem = name[: -len(META_SUFFIX)]
        else:
            raise ValueError(f"invalid error file name in {raw_path!r}")
        m = _ATTEMPT_RE.match(stem)
    else:
        base = parts[:-1]
        if name.endswith(CAPTURE_SUFFIX):
            stem = name[: -len(CAPTURE_SUFFIX)]
        elif name.endswith(META_SUFFIX):
            stem = name[: -len(META_SUFFIX)]
        else:
            raise ValueError(f"invalid capture file name in {raw_path!r}")
        m = _CAPTURE_RE.match(stem)
    if not m:
        raise ValueError(f"invalid capture/attempt id in {raw_path!r}")
    if len(base) == 3:
        safe_component(base[0], "run_id")
        feed, date = base[1], base[2]
    elif len(base) == 2:
        feed, date = base
    else:
        raise ValueError(f"raw path {raw_path!r} does not match the archive layout")
    _check_feed(feed)
    _check_date(date)
    if FEED_ABBR[feed] != m.group(1):
        raise ValueError(f"raw path {raw_path!r} mixes feed directory and id prefix")
    return rel


def meta_path_for(raw_path: str) -> str:
    if raw_path.endswith(CAPTURE_SUFFIX):
        return raw_path[: -len(CAPTURE_SUFFIX)] + META_SUFFIX
    if raw_path.endswith(ERROR_BODY_SUFFIX):
        return raw_path[: -len(ERROR_BODY_SUFFIX)] + META_SUFFIX
    raise ValueError(f"not a raw body path: {raw_path!r}")


# --------------------------------------------------------------------------------------------------
# Archive
# --------------------------------------------------------------------------------------------------

class RawArchive:
    """Durable, append-only raw capture store rooted at ``raw_root`` (``W/data/raw``)."""

    def __init__(self, raw_root: Path) -> None:
        self.raw_root = Path(raw_root)
        self._index_lock = threading.Lock()

    # ---- paths
    def run_dir(self, run_id: str) -> Path:
        return self.raw_root / safe_component(run_id, "run_id")

    def index_path(self, run_id: str) -> Path:
        return self.run_dir(run_id) / INDEX_NAME

    def capture_relpath(self, run_id: str, feed_type: str, fetched_at: datetime, capture_id: str) -> str:
        safe_component(run_id, "run_id")
        _check_feed(feed_type)
        check_capture_id(capture_id, feed_type)
        date = fetched_at.astimezone(UTC).strftime("%Y-%m-%d")
        return f"{run_id}/{feed_type}/{date}/{capture_id}{CAPTURE_SUFFIX}"

    def resolve(self, raw_path: str) -> Path:
        """Absolute path of a stored raw/meta file; rejects anything outside the archive layout."""
        validate_raw_relpath(raw_path)
        return resolve_under(self.raw_root, raw_path)

    # ---- captures
    def write_capture(
        self,
        run_id: str,
        feed_type: str,
        capture_id: str,
        fetched_at: datetime,
        body: bytes,
        meta: dict[str, Any],
    ) -> RawRef:
        """Write ``<capture_id>.json.gz`` then its ``.meta.json`` sidecar (both durable) and return the ref.

        Idempotent for the same body; raises ``FileExistsError`` if a different body already exists
        under the same capture id (raw files are never overwritten).
        """
        rel = self.capture_relpath(run_id, feed_type, fetched_at, capture_id)
        raw_sha = sha256_hex(body)
        if not capture_id.endswith(raw_sha[:12]):
            raise ValueError(f"capture_id {capture_id!r} does not match the body sha256 {raw_sha[:12]}")
        gz = gzip_bytes(body)
        stored_sha = sha256_hex(gz)
        final = self.resolve(rel)
        if final.exists():
            existing = final.read_bytes()
            if sha256_hex(existing) != stored_sha:
                raise FileExistsError(f"raw file {rel} already exists with different content")
            log.info("raw capture %s already archived (idempotent re-write skipped)", capture_id)
        else:
            atomic_write_bytes(final, gz)
        ref = RawRef(raw_path=rel, raw_sha256=raw_sha, raw_bytes=len(body), stored_sha256=stored_sha,
                     stored_bytes=len(gz))
        sidecar = dict(meta)
        sidecar.update(
            meta_version=1,
            run_id=run_id,
            feed_type=feed_type,
            capture_id=capture_id,
            fetched_at=iso_us(fetched_at),
            raw_path=rel,
            raw_sha256=raw_sha,
            raw_bytes=len(body),
            stored_sha256=stored_sha,
            stored_bytes=len(gz),
        )
        atomic_write_json(self.resolve(meta_path_for(rel)), sidecar)
        return ref

    def read_meta(self, raw_path: str) -> dict[str, Any] | None:
        path = self.resolve(meta_path_for(raw_path) if not raw_path.endswith(META_SUFFIX) else raw_path)
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        except FileNotFoundError:
            return None
        return data if isinstance(data, dict) else None

    def update_meta(self, raw_path: str, updates: dict[str, Any]) -> dict[str, Any]:
        """Merge ``updates`` into a capture sidecar (atomic rewrite). Identity fields cannot change."""
        current = self.read_meta(raw_path)
        if current is None:
            raise FileNotFoundError(f"sidecar for {raw_path} not found")
        protected = {"run_id", "feed_type", "capture_id", "fetched_at", "raw_path", "raw_sha256", "raw_bytes",
                     "stored_sha256", "stored_bytes"}
        merged = dict(current)
        merged.update({k: v for k, v in updates.items() if k not in protected})
        atomic_write_json(self.resolve(meta_path_for(raw_path)), merged)
        return merged

    def read_capture(
        self,
        raw_path: str,
        *,
        expected_sha256: str | None = None,
        max_bytes: int = DEFAULT_MAX_DECODED_BYTES,
    ) -> bytes:
        """Return the decoded body after verifying its sha256.

        The expected value is ``expected_sha256`` when given, else the sidecar's ``raw_sha256``; with
        neither available the 12-hex prefix embedded in the capture id is checked.
        """
        if not raw_path.endswith(CAPTURE_SUFFIX):
            raise ValueError(f"not a capture path: {raw_path!r}")
        path = self.resolve(raw_path)
        if not path.is_file():
            raise FileNotFoundError(f"raw capture {raw_path} not found")
        body = gunzip_bounded(path, max_bytes)
        actual = sha256_hex(body)
        expected = expected_sha256
        if expected is None:
            meta = None
            try:
                meta = self.read_meta(raw_path)
            except (OSError, ValueError):
                meta = None
            if meta and isinstance(meta.get("raw_sha256"), str):
                expected = meta["raw_sha256"]
        if expected is not None:
            if actual != expected:
                raise RawIntegrityError(f"sha256 mismatch for {raw_path}: expected {expected}, got {actual}")
        else:
            prefix = PurePosixPath(raw_path).name[: -len(CAPTURE_SUFFIX)].rsplit("-", 1)[-1]
            if not actual.startswith(prefix):
                raise RawIntegrityError(f"sha256 of {raw_path} does not match its capture id")
        return body

    # ---- error bodies (non-2xx audit)
    def write_error_body(
        self,
        run_id: str,
        feed_type: str,
        fetched_at: datetime,
        attempt_id: str,
        body: bytes,
        meta: dict[str, Any],
    ) -> str:
        """Archive at most 64 KiB of a non-2xx body under ``errors/``; returns its raw path."""
        safe_component(run_id, "run_id")
        _check_feed(feed_type)
        if not _ATTEMPT_RE.match(attempt_id):
            raise ValueError(f"invalid attempt id {attempt_id!r}")
        truncated = body[:ERROR_BODY_MAX_BYTES]
        date = fetched_at.astimezone(UTC).strftime("%Y-%m-%d")
        rel = f"{run_id}/{feed_type}/{date}/{ERRORS_DIR}/{attempt_id}{ERROR_BODY_SUFFIX}"
        final = self.resolve(rel)
        gz = gzip_bytes(truncated)
        if not final.exists():
            atomic_write_bytes(final, gz)
        sidecar = dict(meta)
        sidecar.update(
            meta_version=1,
            run_id=run_id,
            feed_type=feed_type,
            attempt_id=attempt_id,
            fetched_at=iso_us(fetched_at),
            raw_path=rel,
            body_sha256=sha256_hex(truncated),
            body_bytes_kept=len(truncated),
            body_truncated=len(body) > len(truncated),
            stored_bytes=len(gz),
        )
        atomic_write_json(self.resolve(meta_path_for(rel)), sidecar)
        return rel

    # ---- capture index
    def append_index(self, run_id: str, record: dict[str, Any]) -> None:
        """Append one JSON line to ``capture_index.jsonl`` (O_APPEND + fsync, serialized per process)."""
        path = self.index_path(run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = (json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                           allow_nan=False) + "\n").encode("utf-8")
        with self._index_lock:
            created = not path.exists()
            fd = os.open(str(path), os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
            try:
                view = memoryview(line)
                while view:
                    written = os.write(fd, view)
                    view = view[written:]
                os.fsync(fd)
            finally:
                os.close(fd)
            if created:
                fsync_dir(path.parent)

    def read_index(self, run_id: str) -> list[dict[str, Any]]:
        """All parseable index records in file order (a torn last line after a crash is skipped)."""
        path = self.index_path(run_id)
        if not path.is_file():
            return []
        records: list[dict[str, Any]] = []
        bad = 0
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except ValueError:
                    bad += 1
                    continue
                if isinstance(obj, dict):
                    records.append(obj)
                else:
                    bad += 1
        if bad:
            log.warning("capture index %s: skipped %d unparseable line(s)", path, bad)
        return records

    def indexed_capture_ids(self, run_id: str) -> set[str]:
        return {r["capture_id"] for r in self.read_index(run_id)
                if r.get("kind") == "capture" and isinstance(r.get("capture_id"), str)}

    # ---- restart helpers
    def recent_sidecars(self, run_id: str, feed_type: str, n_dates: int = 2) -> list[Path]:
        """Capture sidecars of the ``n_dates`` newest date directories of a feed (oldest first)."""
        feed_dir = self.run_dir(run_id) / _check_feed(feed_type)
        if not feed_dir.is_dir():
            return []
        dates = sorted((d for d in feed_dir.iterdir() if d.is_dir() and _DATE_RE.match(d.name)),
                       key=lambda d: d.name, reverse=True)[: max(0, n_dates)]
        out: list[Path] = []
        for d in sorted(dates, key=lambda d: d.name):
            for p in sorted(d.glob(f"*{META_SUFFIX}")):
                if _CAPTURE_RE.match(p.name[: -len(META_SUFFIX)]):
                    out.append(p)
        return out

    def relpath_of(self, path: Path) -> str:
        """POSIX path of ``path`` relative to the raw root."""
        return path.resolve().relative_to(self.raw_root.resolve()).as_posix()

    def quarantine_orphans(self, run_id: str, *, older_than_seconds: float = 3600.0,
                           now: float | None = None) -> list[str]:
        """Move orphan temp files older than the threshold into ``<run>/quarantine/`` (never deleted)."""
        run_dir = self.run_dir(run_id)
        if not run_dir.is_dir():
            return []
        qdir = run_dir / QUARANTINE_DIR
        ts = time.time() if now is None else now
        moved: list[str] = []
        for dirpath, dirnames, filenames in os.walk(run_dir):
            dpath = Path(dirpath)
            if dpath == qdir or qdir in dpath.parents:
                dirnames[:] = []
                continue
            for name in filenames:
                if not (name.startswith(".") and _TMP_MARKER in name):
                    continue
                src = dpath / name
                try:
                    age = ts - src.stat().st_mtime
                except FileNotFoundError:
                    continue
                if age < older_than_seconds:
                    continue
                rel = src.relative_to(run_dir).as_posix()
                qdir.mkdir(parents=True, exist_ok=True)
                dst = qdir / rel.replace("/", "__").lstrip(".")
                if dst.exists():
                    dst = dst.with_name(f"{dst.name}-{uuid.uuid4().hex[:8]}")
                os.replace(src, dst)
                moved.append(rel)
        if moved:
            fsync_dir(qdir)
            log.warning("quarantined %d orphan temp file(s) under %s", len(moved), qdir)
        return moved

    def run_bytes(self, run_id: str) -> int:
        """Bytes on disk under the run's raw directory (the hard-linked static ZIP is excluded)."""
        run_dir = self.run_dir(run_id)
        return directory_bytes(run_dir, exclude_top=(STATIC_LINK_DIR,))


def directory_bytes(root: Path, *, exclude_top: tuple[str, ...] = ()) -> int:
    """Sum of regular-file sizes below ``root`` (top-level names in ``exclude_top`` are skipped)."""
    if not root.is_dir():
        return 0
    total = 0
    for dirpath, dirnames, filenames in os.walk(root):
        if Path(dirpath) == root and exclude_top:
            dirnames[:] = [d for d in dirnames if d not in exclude_top]
        for name in filenames:
            try:
                total += os.lstat(os.path.join(dirpath, name)).st_size
            except FileNotFoundError:
                continue
    return total


def hard_link(src: Path, dst: Path) -> bool:
    """Hard-link ``src`` to ``dst`` (no extra disk). Returns False when linking is impossible."""
    if dst.exists():
        try:
            return os.path.samefile(src, dst)
        except OSError:
            return False
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.link(src, dst)
    except OSError as exc:
        log.info("cannot hard-link %s -> %s (%s); static ZIP stays referenced by gtfs_version", src, dst, exc)
        return False
    fsync_dir(dst.parent)
    return True


__all__ = [
    "RawArchive",
    "RawIntegrityError",
    "RawRef",
    "atomic_write_bytes",
    "atomic_write_json",
    "check_capture_id",
    "directory_bytes",
    "fsync_dir",
    "gunzip_bounded",
    "gzip_bytes",
    "iso_us",
    "hard_link",
    "meta_path_for",
    "validate_raw_relpath",
]
