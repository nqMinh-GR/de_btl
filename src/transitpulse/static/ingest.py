"""Immutable GTFS ZIP store, download, extraction and Spark-free inspection.

Layout: ``W/data/static/<gtfs_version>/{MBTA_GTFS.zip, version.json}`` where ``gtfs_version`` is the full
sha256 hex of the ZIP bytes (never the file name; the MBTA URL serves new content under the same name).
The stored ZIP is written once (temp file + ``os.replace``) and made read-only; a later ingest of the same
bytes reuses it, and a stored file whose checksum no longer matches is reported, never overwritten.
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import shlex
import shutil
import tempfile
import uuid
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from transitpulse.contracts.envelope import iso_utc, parse_iso_utc
from transitpulse.obs.logging import get_logger, log_event
from transitpulse.runtime.paths import WorkPaths
from transitpulse.static.fsutil import atomic_write_json, fsync_dir, read_json
from transitpulse.static.validation import FILE_SPECS, normalize_header

__all__ = [
    "PARSER_VERSION",
    "ZIP_NAME",
    "VERSION_FILE",
    "MAX_ZIP_BYTES",
    "StaticError",
    "UsageError",
    "ArtifactError",
    "SourceFetchError",
    "StaticVersion",
    "ExtractedFile",
    "ingest_zip",
    "download_zip",
    "get_static_version",
    "sha256_file",
    "find_member",
    "extract_gtfs",
    "read_feed_info",
    "iter_csv_member",
    "inspect_zip",
]

PARSER_VERSION = "tp-static-v1"
ZIP_NAME = "MBTA_GTFS.zip"
VERSION_FILE = "version.json"
MAX_ZIP_BYTES = 512 * 1024 * 1024
_CHUNK = 1024 * 1024

_log = get_logger("transitpulse.static.ingest")


class StaticError(Exception):
    """Static-loader failure carrying a machine code, a CLI exit code and a hint."""

    exit_code = 1

    def __init__(self, code: str, message: str, hint: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint


class UsageError(StaticError):
    exit_code = 2


class ArtifactError(StaticError):
    exit_code = 7


class SourceFetchError(StaticError):
    exit_code = 1


@dataclass(frozen=True)
class StaticVersion:
    """Identity and provenance of one immutable GTFS ZIP (interfaces §3)."""

    gtfs_version: str
    zip_path: Path
    zip_sha256: str
    feed_version: str | None
    source_url: str | None
    downloaded_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "gtfs_version": self.gtfs_version,
            "zip_relpath": ZIP_NAME,
            "zip_sha256": self.zip_sha256,
            "feed_version": self.feed_version,
            "source_url": self.source_url,
            "downloaded_at": iso_utc(self.downloaded_at),
            "parser_version": PARSER_VERSION,
        }

    @classmethod
    def from_json(cls, doc: dict[str, Any], version_dir: Path) -> StaticVersion:
        return cls(
            gtfs_version=str(doc["gtfs_version"]),
            zip_path=version_dir / str(doc.get("zip_relpath") or ZIP_NAME),
            zip_sha256=str(doc["zip_sha256"]),
            feed_version=doc.get("feed_version"),
            source_url=doc.get("source_url"),
            downloaded_at=parse_iso_utc(str(doc["downloaded_at"])),
        )


@dataclass(frozen=True)
class ExtractedFile:
    name: str            # GTFS table name (e.g. "stop_times")
    path: Path
    sha256: str
    size_bytes: int


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def download_zip(
    url: str,
    dest_dir: Path,
    *,
    user_agent: str,
    connect_timeout: float = 10.0,
    read_timeout: float = 120.0,
    max_bytes: int = MAX_ZIP_BYTES,
) -> Path:
    """Stream ``url`` into a new file under ``dest_dir`` (bounded size, explicit timeouts, User-Agent)."""
    import httpx

    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / f"static-{uuid.uuid4().hex}.zip"
    timeout = httpx.Timeout(connect=connect_timeout, read=read_timeout, write=connect_timeout, pool=connect_timeout)
    total = 0
    try:
        with httpx.Client(timeout=timeout, headers={"User-Agent": user_agent}, follow_redirects=True) as client:
            with client.stream("GET", url) as resp:
                if resp.status_code != 200:
                    raise SourceFetchError(
                        "SOURCE_FETCH_FAILED", f"GET {url} returned HTTP {resp.status_code}",
                        "check the URL / network; or pass --zip with a local copy",
                    )
                with open(target, "wb") as fh:
                    for chunk in resp.iter_bytes(chunk_size=_CHUNK):
                        total += len(chunk)
                        if total > max_bytes:
                            raise SourceFetchError(
                                "SOURCE_FETCH_FAILED", f"static ZIP exceeds {max_bytes} bytes", None
                            )
                        fh.write(chunk)
                    fh.flush()
                    os.fsync(fh.fileno())
    except httpx.HTTPError as exc:
        target.unlink(missing_ok=True)
        raise SourceFetchError(
            "SOURCE_FETCH_FAILED", f"GET {url} failed: {type(exc).__name__}: {exc}",
            "no Internet access? use --zip with a local GTFS ZIP",
        ) from exc
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    log_event(_log, "static_download", "ok", "static ZIP downloaded", url=url, bytes=total)
    return target


def find_member(zf: zipfile.ZipFile, filename: str) -> zipfile.ZipInfo | None:
    """ZIP member whose base name is ``filename`` (the shallowest one when nested)."""
    hits = [
        info for info in zf.infolist()
        if not info.is_dir() and info.filename.replace("\\", "/").rsplit("/", 1)[-1] == filename
    ]
    if not hits:
        return None
    return min(hits, key=lambda i: (i.filename.count("/"), i.filename))


def read_feed_info(zip_path: Path) -> dict[str, str | None] | None:
    """First data row of ``feed_info.txt`` (trimmed, blank -> None) or ``None`` when absent/empty."""
    with zipfile.ZipFile(zip_path) as zf:
        member = find_member(zf, "feed_info.txt")
        if member is None:
            return None
        for row in iter_csv_member(zf, member):
            return row
    return None


def iter_csv_member(zf: zipfile.ZipFile, member: zipfile.ZipInfo) -> Iterable[dict[str, str | None]]:
    """Stream rows of a ZIP CSV member as dicts (BOM/whitespace-stripped header, blank values -> None)."""
    with zf.open(member) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        reader = csv.reader(text)
        header = normalize_header(next(reader, []))
        for cells in reader:
            if not any(c.strip() for c in cells):
                continue
            row: dict[str, str | None] = {}
            for i, name in enumerate(header):
                v = cells[i].strip() if i < len(cells) else ""
                row[name] = v if v else None
            yield row


def _validate_zip(path: Path) -> None:
    if not zipfile.is_zipfile(path):
        raise ArtifactError("STATIC_ZIP_INVALID", f"{path} is not a ZIP archive", "pass a GTFS ZIP")
    with zipfile.ZipFile(path) as zf:
        bad = zf.testzip()
        if bad is not None:
            raise ArtifactError("STATIC_ZIP_INVALID", f"CRC error in ZIP member {bad!r}", "re-download the ZIP")


def ingest_zip(
    paths: WorkPaths,
    zip_path: Path | None,
    *,
    download_url: str | None,
    user_agent: str,
    connect_timeout: float = 10.0,
    read_timeout: float = 120.0,
) -> StaticVersion:
    """Copy (or download) a GTFS ZIP into ``data/static/<gtfs_version>/MBTA_GTFS.zip`` + ``version.json``.

    Exactly one of ``zip_path`` / ``download_url`` must be given. Idempotent: the same bytes map to the
    same version directory, and the first ``version.json`` (provenance of the first ingest) is kept.
    """
    if (zip_path is None) == (download_url is None):
        raise UsageError("USAGE", "pass exactly one of --zip PATH or --download", None)
    downloaded: Path | None = None
    if download_url is not None:
        downloaded = download_zip(download_url, paths.downloads / ".partial", user_agent=user_agent,
                                  connect_timeout=connect_timeout, read_timeout=read_timeout)
        src, source_url = downloaded, download_url
    else:
        src = Path(zip_path).expanduser().resolve()  # type: ignore[arg-type]
        if not src.is_file():
            raise ArtifactError("STATIC_ZIP_NOT_FOUND", f"static ZIP not found: {src}", "check --zip PATH")
        source_url = src.as_uri()
    try:
        _validate_zip(src)
        digest = sha256_file(src)
        vdir = paths.static_version_dir(digest)
        vdir.mkdir(parents=True, exist_ok=True)
        target = vdir / ZIP_NAME
        if target.exists():
            existing = sha256_file(target)
            if existing != digest:
                raise ArtifactError(
                    "STATIC_ZIP_CHECKSUM_MISMATCH",
                    f"stored {target} has sha256 {existing}, expected {digest}",
                    "the immutable static store was modified; restore it or use a fresh work directory",
                )
            reused = True
        else:
            fd, tmp = tempfile.mkstemp(prefix=f".{ZIP_NAME}.", suffix=".tmp", dir=vdir)
            try:
                with os.fdopen(fd, "wb") as out, open(src, "rb") as inp:
                    shutil.copyfileobj(inp, out, _CHUNK)
                    out.flush()
                    os.fsync(out.fileno())
                if sha256_file(Path(tmp)) != digest:
                    raise ArtifactError("STATIC_ZIP_CHECKSUM_MISMATCH", "ZIP changed while being copied", None)
                os.replace(tmp, target)
            except BaseException:
                if os.path.exists(tmp):
                    os.unlink(tmp)
                raise
            fsync_dir(vdir)
            try:
                os.chmod(target, 0o444)
            except OSError:
                pass
            reused = False
        vfile = vdir / VERSION_FILE
        if vfile.is_file():
            version = StaticVersion.from_json(read_json(vfile), vdir)
        else:
            info = read_feed_info(target)
            version = StaticVersion(
                gtfs_version=digest,
                zip_path=target,
                zip_sha256=digest,
                feed_version=(info or {}).get("feed_version"),
                source_url=source_url,
                downloaded_at=datetime.now(UTC),
            )
            atomic_write_json(vfile, version.to_json())
            version = StaticVersion.from_json(read_json(vfile), vdir)   # same precision as later re-reads
        log_event(_log, "static_ingest", "reused" if reused else "ok", "static ZIP stored",
                  gtfs_version=digest, zip_path=str(target), feed_version=version.feed_version)
        return version
    finally:
        if downloaded is not None:
            downloaded.unlink(missing_ok=True)


def get_static_version(paths: WorkPaths, gtfs_version: str) -> StaticVersion:
    """Read ``version.json`` of an ingested version (``ArtifactError`` when it was never ingested)."""
    vdir = paths.static_version_dir(gtfs_version)
    vfile = vdir / VERSION_FILE
    if not vfile.is_file() or not (vdir / ZIP_NAME).is_file():
        workdir = shlex.quote(str(paths.root))
        raise ArtifactError(
            "STATIC_VERSION_NOT_FOUND", f"GTFS version {gtfs_version} is not ingested under {vdir}",
            f"run `python -m transitpulse.static load --workdir {workdir} --zip PATH --route-ids A,B` "
            "(or `--download` instead of `--zip PATH`) first",
        )
    return StaticVersion.from_json(read_json(vfile), vdir)


def extract_gtfs(zip_path: Path, dest_dir: Path) -> dict[str, ExtractedFile]:
    """Extract the known GTFS tables (``FILE_SPECS``) into ``dest_dir/<name>.txt`` with their sha256.

    Member names are never used as output paths (no traversal); unknown members are ignored.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    out: dict[str, ExtractedFile] = {}
    with zipfile.ZipFile(zip_path) as zf:
        for name in FILE_SPECS:
            member = find_member(zf, f"{name}.txt")
            if member is None:
                continue
            target = dest_dir / f"{name}.txt"
            h = hashlib.sha256()
            size = 0
            with zf.open(member) as inp, open(target, "wb") as outf:
                for chunk in iter(lambda: inp.read(_CHUNK), b""):
                    h.update(chunk)
                    size += len(chunk)
                    outf.write(chunk)
            out[name] = ExtractedFile(name, target, h.hexdigest(), size)
    return out


def inspect_zip(zip_path: Path) -> dict[str, Any]:
    """Spark-free summary: files present/empty/row counts, feed_info, service date range, bus routes."""
    zip_path = Path(zip_path).expanduser().resolve()
    if not zip_path.is_file():
        raise ArtifactError("STATIC_ZIP_NOT_FOUND", f"static ZIP not found: {zip_path}", "check --zip PATH")
    _validate_zip(zip_path)
    digest = sha256_file(zip_path)
    files: list[dict[str, Any]] = []
    min_date: str | None = None
    max_date: str | None = None
    bus_routes = 0
    feed_info: dict[str, str | None] | None = None
    with zipfile.ZipFile(zip_path) as zf:
        for name, spec in FILE_SPECS.items():
            member = find_member(zf, spec.filename)
            if member is None:
                files.append({"name": spec.filename, "present": False, "empty": None, "row_count": None,
                              "columns": None, "required": spec.required_file})
                continue
            count = 0
            columns: list[str] | None = None
            for row in iter_csv_member(zf, member):
                if columns is None:
                    columns = list(row.keys())
                count += 1
                if name == "calendar":
                    for d in (row.get("start_date"), row.get("end_date")):
                        if d and len(d) == 8 and d.isdigit():
                            min_date = d if min_date is None or d < min_date else min_date
                            max_date = d if max_date is None or d > max_date else max_date
                elif name == "calendar_dates":
                    d = row.get("date")
                    if d and len(d) == 8 and d.isdigit() and row.get("exception_type") == "1":
                        min_date = d if min_date is None or d < min_date else min_date
                        max_date = d if max_date is None or d > max_date else max_date
                elif name == "routes" and row.get("route_type") == "3":
                    bus_routes += 1
                elif name == "feed_info" and feed_info is None:
                    feed_info = row
            files.append({"name": spec.filename, "present": True, "empty": count == 0, "row_count": count,
                          "columns": columns, "required": spec.required_file})
    return {
        "zip_path": str(zip_path),
        "zip_sha256": digest,
        "gtfs_version": digest,
        "files": files,
        "feed_info": feed_info,
        "feed_version": (feed_info or {}).get("feed_version"),
        "service_date_range": {"min": min_date, "max": max_date},
        "bus_route_count": bus_routes,
    }
