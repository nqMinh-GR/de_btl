"""Artifact download, checksum verification and safe extraction (bootstrap steps B4-B6).

Rules (decision D-ENV-05/06, design D §1-§2):
* Checksums come only from the publisher's official file of the SAME origin as the archive
  (``downloads.apache.org`` or ``archive.apache.org``, the GitHub release, the Adoptium API).
  A pinned manifest value must match both the official file and the downloaded bytes.
* Downloads stream to ``cache/downloads/.partial/<name>.<uuid>`` while hashing, are fsynced and are
  renamed into ``cache/downloads/`` only after verification. A mismatch deletes the partial file and
  fails with exit code 7; nothing is extracted from an unverified archive.
* Extraction checks every tar member first (single top directory, no absolute paths, no ``..``,
  no devices/FIFOs, links resolving inside the top directory, member-count and size caps) and then
  uses ``tarfile`` with ``filter="data"``. The tree is extracted into a staging directory and moved
  into place with ``os.replace``. An install directory without a valid marker is moved to
  ``tools/.trash/`` (never deleted).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import posixpath
import re
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.util import atomic_write_json, hash_file, iso_now, read_json_or_none

log = logging.getLogger("transitpulse.runtime.artifacts")

USER_AGENT = "TransitPulse-MBTA-bootstrap/0.1 (+course project; checksum-verified downloads)"
DOWNLOAD_CHUNK = 1024 * 1024
MAX_MEMBERS = 50_000
MAX_TOTAL_BYTES = 2 * 1024**3
_HEX_LEN = {"sha1": 40, "sha256": 64, "sha512": 128}


class ArtifactError(Exception):
    """Download / checksum / extraction failure (CLI exit code 7)."""

    def __init__(self, code: str, message: str, hint: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint


# --------------------------------------------------------------------------------------------------
# Official checksum file parsing
# --------------------------------------------------------------------------------------------------

def parse_checksum_text(text: str, algorithm: str, *, filename: str | None = None) -> str:
    """Parse an official checksum file and return the lowercase hex digest.

    Supported layouts:
    * ``gpg --print-md`` (Apache Kafka ``.sha512``)::

          kafka_2.13-4.2.2.tgz: ACB9961F 7EC72F5F ...
                                6CF961BD ...

      -> take everything after the first ``:``, remove all whitespace, lowercase.
    * ``sha256sum`` (``<hex>  <file>`` or ``<hex> *<file>``, possibly several lines; when ``filename`` is
      given the matching line is used).
    * bare hex (Apache Cassandra ``.sha256``).
    """
    expected_len = _HEX_LEN.get(algorithm)
    if expected_len is None:
        raise ArtifactError("CHECKSUM_UNSUPPORTED", f"unsupported checksum algorithm {algorithm!r}")
    body = text.strip().lstrip("﻿")
    if not body:
        raise ArtifactError("CHECKSUM_UNPARSEABLE", "official checksum file is empty")
    candidates: list[str] = []
    first_line = body.splitlines()[0]
    if ":" in first_line and not re.match(r"^[0-9A-Fa-f]{%d}\s" % expected_len, first_line):
        # gpg --print-md: the digest may be wrapped over several lines and grouped by 8 hex chars.
        candidates.append(re.sub(r"\s+", "", body.split(":", 1)[1]).lower())
    else:
        lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
        chosen: list[str] = []
        if filename is not None:
            for ln in lines:
                parts = ln.split()
                if len(parts) >= 2 and parts[-1].lstrip("*").split("/")[-1] == filename:
                    chosen.append(parts[0])
        if not chosen:
            chosen = [lines[0].split()[0]]
        candidates.extend(c.lower() for c in chosen)
    digest = candidates[0]
    if not re.fullmatch(r"[0-9a-f]{%d}" % expected_len, digest):
        raise ArtifactError(
            "CHECKSUM_UNPARSEABLE",
            f"official checksum file does not contain a {algorithm} digest (parsed {digest[:20]!r}...)",
        )
    return digest


def parse_adoptium_checksum(payload: Any, package_name: str) -> tuple[str, str | None]:
    """Find the binary whose ``package.name`` equals ``package_name`` in an Adoptium v3 assets response.

    Returns ``(sha256, checksum_link)``. The response is a JSON array of releases, each with
    ``binaries[].package.{name, link, checksum, checksum_link}``.
    """
    if not isinstance(payload, list):
        raise ArtifactError("CHECKSUM_UNPARSEABLE", "Adoptium API response is not a JSON array")
    for release in payload:
        for binary in (release or {}).get("binaries", []) or []:
            pkg = (binary or {}).get("package") or {}
            if pkg.get("name") == package_name:
                checksum = str(pkg.get("checksum") or "").strip().lower()
                if not re.fullmatch(r"[0-9a-f]{64}", checksum):
                    raise ArtifactError("CHECKSUM_UNPARSEABLE",
                                        f"Adoptium API entry for {package_name} has no valid sha256")
                return checksum, pkg.get("checksum_link")
    raise ArtifactError("CHECKSUM_UNAVAILABLE", f"Adoptium API response has no package named {package_name}")


# --------------------------------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------------------------------

def _request(url: str, *, method: str = "GET") -> urllib.request.Request:
    if not url.startswith("https://"):
        raise ArtifactError("INSECURE_URL", f"refusing non-https URL {url}")
    return urllib.request.Request(url, method=method, headers={"User-Agent": USER_AGENT})


def fetch_bytes(url: str, *, timeout: float = 60.0, retries: int = 3, max_bytes: int = 4 * 1024 * 1024) -> bytes:
    """Small GET (checksum files, API JSON) with retries and a size cap."""
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(_request(url), timeout=timeout) as resp:  # noqa: S310 - https only
                data = resp.read(max_bytes + 1)
            if len(data) > max_bytes:
                raise ArtifactError("RESPONSE_TOO_LARGE", f"{url} returned more than {max_bytes} bytes")
            return data
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code in (400, 401, 403, 404, 410):
                break
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last = exc
        if attempt < retries:
            time.sleep(2 ** attempt)
    raise ArtifactError("DOWNLOAD_FAILED", f"GET {url} failed: {last}")


def head_ok(url: str, *, timeout: float = 15.0) -> tuple[bool, str]:
    try:
        with urllib.request.urlopen(_request(url, method="HEAD"), timeout=timeout) as resp:  # noqa: S310
            return True, f"HTTP {resp.status}"
    except urllib.error.HTTPError as exc:
        # Reachable host; some CDNs reject HEAD on directory URLs.
        return exc.code < 500, f"HTTP {exc.code}"
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return False, str(exc)[:200]


def download_to(url: str, dest: Path, partial_dir: Path, *, algorithm: str, expected: str | None,
                timeout: float = 120.0, retries: int = 3,
                progress: Callable[[str], None] | None = None) -> str:
    """Stream ``url`` into ``partial_dir`` while hashing; verify; fsync; ``os.replace`` to ``dest``.

    Returns the hex digest. On mismatch the partial file is removed and ArtifactError raised.
    """
    partial_dir.mkdir(parents=True, exist_ok=True)
    dest.parent.mkdir(parents=True, exist_ok=True)
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        tmp = partial_dir / f"{dest.name}.{uuid.uuid4().hex}"
        h = hashlib.new(algorithm)
        done = 0
        try:
            with urllib.request.urlopen(_request(url), timeout=timeout) as resp:  # noqa: S310
                total = int(resp.headers.get("Content-Length") or 0)
                next_report = 0.0
                with open(tmp, "wb") as fh:
                    while True:
                        chunk = resp.read(DOWNLOAD_CHUNK)
                        if not chunk:
                            break
                        fh.write(chunk)
                        h.update(chunk)
                        done += len(chunk)
                        if progress and total and done / total >= next_report:
                            progress(f"{dest.name}: {done * 100 // total}% of {total // (1024 * 1024)} MiB")
                            next_report += 0.2
                    fh.flush()
                    os.fsync(fh.fileno())
            if total and done != total:
                raise OSError(f"truncated download ({done} of {total} bytes)")
            digest = h.hexdigest()
            if expected is not None and digest != expected:
                tmp.unlink(missing_ok=True)
                raise ArtifactError(
                    "CHECKSUM_MISMATCH",
                    f"{algorithm} of {url} is {digest}, official value is {expected}",
                    "The download is corrupt or the artifact changed upstream. Nothing was extracted. "
                    "Re-run bootstrap; if it persists, check the official release page.",
                )
            os.replace(tmp, dest)
            return digest
        except ArtifactError:
            raise
        except urllib.error.HTTPError as exc:
            tmp.unlink(missing_ok=True)
            last = exc
            if exc.code in (400, 401, 403, 404, 410):
                break
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            tmp.unlink(missing_ok=True)
            last = exc
        if attempt < retries:
            wait = 2 ** attempt
            if progress:
                progress(f"download attempt {attempt} of {url} failed ({last}); retrying in {wait}s")
            time.sleep(wait)
    raise ArtifactError("DOWNLOAD_FAILED", f"download of {url} failed: {last}",
                        "Check network access to the artifact host (see doctor --check-network).")


# --------------------------------------------------------------------------------------------------
# Safe tar extraction
# --------------------------------------------------------------------------------------------------

def _normalize_member_name(name: str) -> str:
    while name.startswith("./"):
        name = name[2:]
    return name.rstrip("/")


def check_tar_members(members: Iterable[tarfile.TarInfo], top_dir: str, *, max_members: int = MAX_MEMBERS,
                      max_total_bytes: int = MAX_TOTAL_BYTES) -> int:
    """Validate every member before extraction. Returns the number of members. Raises ArtifactError."""
    count = 0
    total = 0
    prefix = top_dir + "/"
    for m in members:
        count += 1
        if count > max_members:
            raise ArtifactError("UNSAFE_ARCHIVE", f"archive has more than {max_members} members")
        raw = m.name
        if raw.startswith(("/", "\\")) or re.match(r"^[A-Za-z]:", raw):
            raise ArtifactError("UNSAFE_ARCHIVE", f"absolute member path {raw!r}")
        name = _normalize_member_name(raw)
        parts = PurePosixPath(name).parts
        if ".." in parts or "\\" in name:
            raise ArtifactError("UNSAFE_ARCHIVE", f"member path escapes the archive root: {raw!r}")
        if name != top_dir and not name.startswith(prefix):
            raise ArtifactError("UNSAFE_ARCHIVE", f"member {raw!r} is outside the expected top directory {top_dir!r}")
        if m.ischr() or m.isblk() or m.isfifo() or m.isdev():
            raise ArtifactError("UNSAFE_ARCHIVE", f"device or FIFO member {raw!r}")
        if not (m.isfile() or m.isdir() or m.issym() or m.islnk()):
            raise ArtifactError("UNSAFE_ARCHIVE", f"unsupported member type for {raw!r}")
        if m.issym():
            link = m.linkname
            if link.startswith("/"):
                raise ArtifactError("UNSAFE_ARCHIVE", f"absolute symlink {raw!r} -> {link!r}")
            target = posixpath.normpath(posixpath.join(posixpath.dirname(name), link))
            if target != top_dir and not target.startswith(prefix):
                raise ArtifactError("UNSAFE_ARCHIVE", f"symlink {raw!r} -> {link!r} escapes {top_dir!r}")
        if m.islnk():
            target = posixpath.normpath(_normalize_member_name(m.linkname))
            if target.startswith("/") or (target != top_dir and not target.startswith(prefix)):
                raise ArtifactError("UNSAFE_ARCHIVE", f"hard link {raw!r} -> {m.linkname!r} escapes {top_dir!r}")
        total += max(0, int(m.size))
        if total > max_total_bytes:
            raise ArtifactError("UNSAFE_ARCHIVE", f"archive expands to more than {max_total_bytes} bytes")
    if count == 0:
        raise ArtifactError("UNSAFE_ARCHIVE", "archive is empty")
    return count


def safe_extract(archive: Path, top_dir: str, dest_parent: Path, *, trash_dir: Path,
                 max_members: int = MAX_MEMBERS, max_total_bytes: int = MAX_TOTAL_BYTES) -> Path:
    """Extract ``archive`` (whose single top directory is ``top_dir``) to ``dest_parent/top_dir``.

    Existing destination: moved to ``trash_dir`` first (callers only extract when the marker is invalid).
    """
    dest_parent.mkdir(parents=True, exist_ok=True)
    staging = dest_parent / f".staging-{uuid.uuid4().hex[:12]}"
    staging.mkdir()
    try:
        with tarfile.open(archive, "r:*") as tf:
            members = tf.getmembers()
            check_tar_members(members, top_dir, max_members=max_members, max_total_bytes=max_total_bytes)
            tf.extractall(staging, filter="data")
        extracted = staging / top_dir
        if not extracted.is_dir():
            raise ArtifactError("UNSAFE_ARCHIVE", f"archive did not produce {top_dir}/")
        dest = dest_parent / top_dir
        if dest.exists() or dest.is_symlink():
            move_to_trash(dest, trash_dir)
        os.replace(extracted, dest)
        return dest
    except (tarfile.TarError, OSError) as exc:
        raise ArtifactError("EXTRACT_FAILED", f"extracting {archive.name} failed: {exc}") from exc
    finally:
        _remove_staging(staging)


def _remove_staging(staging: Path) -> None:
    """Remove our own staging directory (only ever contains what this process extracted)."""
    if not staging.exists():
        return
    import shutil

    shutil.rmtree(staging, ignore_errors=True)


def move_to_trash(path: Path, trash_dir: Path) -> Path:
    trash_dir.mkdir(parents=True, exist_ok=True)
    target = trash_dir / f"{path.name}.{time.strftime('%Y%m%dT%H%M%S')}.{uuid.uuid4().hex[:6]}"
    os.replace(path, target)
    log.warning("moved incomplete install %s to %s", path, target)
    return target


# --------------------------------------------------------------------------------------------------
# Artifact installation (download + verify + extract, idempotent)
# --------------------------------------------------------------------------------------------------

@dataclass
class InstallResult:
    id: str
    version: str
    status: str                      # VERIFIED | REUSED_VERIFIED | REUSED_INSTALL
    home: Path
    algorithm: str
    digest: str | None
    official_digest: str | None
    checksum_source: str | None
    source_url: str | None
    archive: str | None
    reused_archive: bool = False
    reused_install: bool = False
    pinned: bool = False
    warnings: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id, "version": self.version, "status": self.status, "home": str(self.home),
            "algorithm": self.algorithm, "observed": self.digest, "official": self.official_digest,
            "checksum_source": self.checksum_source, "source_url": self.source_url, "archive": self.archive,
            "reused_archive": self.reused_archive, "reused_install": self.reused_install, "pinned": self.pinned,
            "warnings": list(self.warnings),
        }


def archive_name(url: str) -> str:
    return urllib.parse.unquote(PurePosixPath(urllib.parse.urlsplit(url).path).name)


def _official_cache_path(paths: WorkPaths, name: str, algorithm: str) -> Path:
    return paths.downloads / f"{name}.{algorithm}.official"


def official_checksum(entry: Mapping[str, Any], url: str, *, fetch: Callable[[str], bytes] = fetch_bytes
                      ) -> tuple[str, str]:
    """Fetch the official digest for ``url`` according to the manifest checksum spec. Returns (digest, source)."""
    ck = entry["checksum"]
    algorithm = ck["algorithm"]
    fmt = str(ck["format"])
    name = archive_name(url)
    if fmt.startswith("adoptium-api"):
        src = ck["official_source"]
        payload = json.loads(fetch(src).decode("utf-8"))
        digest, link = parse_adoptium_checksum(payload, name)
        return digest, src
    template = str(ck["official_source"])
    if template.startswith("<"):
        suffix = template.split(">", 1)[1] if ">" in template else f".{algorithm}"
        src = url + suffix
    else:
        src = template
    text = fetch(src).decode("utf-8", errors="replace")
    return parse_checksum_text(text, algorithm, filename=name), src


PINNED_ONLY_FORMAT = "pinned-release-notes"


def expected_binary_digest(entry: Mapping[str, Any], url: str, *, offline: bool,
                           fetch: Callable[[str], bytes]) -> tuple[str, str]:
    """Expected digest of a single-file artifact: the pinned value (publisher release notes) or the official
    checksum file of the same origin. Returns ``(digest, source)``."""
    ck = entry["checksum"]
    pinned = ck.get("value")
    if str(ck.get("format")) == PINNED_ONLY_FORMAT:
        if not pinned:
            raise ArtifactError("CHECKSUM_UNAVAILABLE", f"{entry['id']}: format {PINNED_ONLY_FORMAT} needs a pinned "
                                "value in deploy/runtime-manifest.json")
        return str(pinned), str(ck.get("official_source"))
    if offline:
        if pinned:
            return str(pinned), "pinned (offline)"
        raise ArtifactError("CHECKSUM_UNAVAILABLE", f"{entry['id']}: no pinned checksum and --offline was given")
    official, source = official_checksum(entry, url, fetch=fetch)
    if pinned and pinned != official:
        raise ArtifactError("CHECKSUM_MISMATCH", f"{entry['id']}: pinned checksum {pinned} differs from the "
                            f"official {official}")
    return official, source


def install_binary(entry: Mapping[str, Any], paths: WorkPaths, *, offline: bool = False, strict: bool = False,
                   fetch: Callable[[str], bytes] = fetch_bytes,
                   progress: Callable[[str], None] | None = None) -> InstallResult:
    """Single-executable artifact (``type: binary``): ``.partial`` -> sha256 verify -> ``os.replace`` to
    ``W/<install_to>`` -> chmod. No extraction. An existing file is re-hashed every run; a file that does not
    match is moved to ``tools/.trash`` and downloaded again. A mismatching download installs nothing."""
    aid = str(entry["id"])
    version = str(entry["version"])
    algorithm = str(entry["checksum"]["algorithm"])
    if strict and not entry["checksum"].get("value"):
        raise ArtifactError("CHECKSUM_NOT_PINNED", f"--strict: artifact {aid} has no pinned checksum")
    rel = Path(str(entry["install_to"]))
    if rel.is_absolute() or ".." in rel.parts:
        raise ArtifactError("MANIFEST_INVALID", f"{aid}: install_to must be relative to the work directory")
    dest = paths.root / rel
    marker = dest.with_name(dest.name + ".tp-installed.json")
    mode = int(str(entry.get("mode", "0755")), 8)
    url = str(entry["urls"][0])
    expected, source = expected_binary_digest(entry, url, offline=offline, fetch=fetch)
    warnings: list[str] = []
    pinned = entry["checksum"].get("value") is not None
    if dest.is_file():
        digest = hash_file(dest, algorithm)
        if digest == expected:
            if not os.access(dest, os.X_OK):
                os.chmod(dest, mode)
            return InstallResult(aid, version, "REUSED_VERIFIED", dest, algorithm, digest, expected, source, url,
                                 None, reused_install=True, pinned=pinned, warnings=warnings)
        warnings.append(f"{dest.name} does not match the expected {algorithm}; reinstalling")
        move_to_trash(dest, paths.tools / ".trash")
        marker.unlink(missing_ok=True)
    if offline:
        raise ArtifactError("OFFLINE_ARTIFACT_MISSING", f"{aid} {version} is not installed and --offline was given",
                            "run bootstrap once with network access")
    errors: list[str] = []
    for candidate in entry["urls"]:
        try:
            digest = download_to(str(candidate), dest, paths.downloads / ".partial", algorithm=algorithm,
                                 expected=expected, progress=progress)
        except ArtifactError as exc:
            if exc.code == "CHECKSUM_MISMATCH":
                raise
            errors.append(f"{candidate}: {exc.message}")
            continue
        os.chmod(dest, mode)
        atomic_write_json(marker, {"id": aid, "version": version, "algorithm": algorithm, "digest": digest,
                                   "expected": expected, "checksum_source": source, "source_url": str(candidate),
                                   "installed_at": iso_now()})
        return InstallResult(aid, version, "VERIFIED", dest, algorithm, digest, expected, source, str(candidate),
                             None, pinned=pinned, warnings=warnings)
    raise ArtifactError("DOWNLOAD_FAILED", f"{aid}: every source failed: " + " | ".join(errors),
                        "check network access to github.com")


def install_artifact(entry: Mapping[str, Any], paths: WorkPaths, *, offline: bool = False, strict: bool = False,
                     fetch: Callable[[str], bytes] = fetch_bytes,
                     progress: Callable[[str], None] | None = None) -> InstallResult:
    """Ensure ``W/<install_to>/<top_dir>`` holds the verified distribution. Idempotent."""
    if str(entry.get("type")) == "binary":
        return install_binary(entry, paths, offline=offline, strict=strict, fetch=fetch, progress=progress)
    aid = str(entry["id"])
    version = str(entry["version"])
    ck = entry["checksum"]
    algorithm = str(ck["algorithm"])
    pinned_value = ck.get("value")
    if strict and pinned_value is None:
        raise ArtifactError("CHECKSUM_NOT_PINNED",
                            f"--strict: artifact {aid} has no pinned checksum in deploy/runtime-manifest.json",
                            "Run prepare_runtime.py --emit-pin-patch once, review and commit the values.")
    top_dir = str(entry["top_dir"])
    install_parent = paths.root / str(entry["install_to"])
    home = install_parent / top_dir
    marker = install_parent / f"{top_dir}.tp-installed.json"
    trash = paths.tools / ".trash"
    urls = list(entry["urls"])
    warnings: list[str] = []

    marker_data = read_json_or_none(marker) if marker.is_file() else None
    marker_ok = (isinstance(marker_data, dict) and marker_data.get("id") == aid
                 and marker_data.get("version") == version and marker_data.get("algorithm") == algorithm
                 and home.is_dir())

    # 1. Locate a cached archive (any URL basename) and re-hash it every time.
    cached: tuple[Path, str] | None = None
    for url in urls:
        cand = paths.downloads / archive_name(url)
        if cand.is_file():
            cached = (cand, url)
            break

    digest: str | None = None
    official: str | None = None
    source: str | None = None
    used_url: str | None = None
    reused_archive = False

    if cached is not None:
        cand, url = cached
        if progress:
            progress(f"{aid}: re-verifying cached archive {cand.name}")
        digest = hash_file(cand, algorithm)
        official, source = _cached_or_fetch_official(entry, paths, url, offline=offline, fetch=fetch)
        if official is not None and digest != official:
            # A corrupt cached archive is never used; it is moved aside and re-downloaded below.
            warnings.append(f"cached archive {cand.name} does not match the official checksum; re-downloading")
            move_to_trash(cand, paths.downloads / ".trash")
            digest = None
            official = None
            source = None
            cached = None
        elif official is None and pinned_value is None:
            if marker_ok and marker_data.get("archive_digest") == digest:
                warnings.append(f"{aid}: official checksum unavailable offline; cached archive matches the "
                                "digest verified at install time")
                official, source = marker_data.get("official_digest") or digest, "install-marker"
                reused_archive = True
                used_url = url
            else:
                raise ArtifactError("CHECKSUM_UNAVAILABLE",
                                    f"{aid}: cannot obtain the official checksum (offline) for the cached archive",
                                    "Run bootstrap once with network access.")
        else:
            reused_archive = True
            used_url = url

    if cached is None:
        if marker_ok and (pinned_value is None or marker_data.get("archive_digest") == pinned_value):
            # Installed and verified earlier; the archive is simply no longer cached (cache cleanup).
            warnings.append(f"{aid}: archive not cached; reusing the install verified on "
                            f"{marker_data.get('installed_at')}")
            return InstallResult(aid, version, "REUSED_INSTALL", home, algorithm,
                                 marker_data.get("archive_digest"), marker_data.get("official_digest"),
                                 marker_data.get("checksum_source"), marker_data.get("source_url"), None,
                                 reused_install=True, pinned=pinned_value is not None, warnings=warnings)
        if offline:
            raise ArtifactError("OFFLINE_ARTIFACT_MISSING", f"{aid} {version} is not cached and --offline was given",
                                "Run bootstrap once with network access.")
        errors: list[str] = []
        for url in urls:
            name = archive_name(url)
            try:
                official, source = official_checksum(entry, url, fetch=fetch)
            except ArtifactError as exc:
                errors.append(f"{url}: checksum: {exc.message}")
                continue
            if pinned_value is not None and pinned_value != official:
                raise ArtifactError("CHECKSUM_MISMATCH",
                                    f"{aid}: pinned checksum {pinned_value} differs from the official {official}")
            try:
                digest = download_to(url, paths.downloads / name, paths.downloads / ".partial",
                                     algorithm=algorithm, expected=official, progress=progress)
            except ArtifactError as exc:
                if exc.code == "CHECKSUM_MISMATCH":
                    raise
                errors.append(f"{url}: {exc.message}")
                continue
            _official_cache_path(paths, name, algorithm).write_text(f"{official}  {name}\n", encoding="utf-8")
            used_url = url
            break
        if used_url is None:
            raise ArtifactError("DOWNLOAD_FAILED", f"{aid}: every source failed: " + " | ".join(errors),
                                "Check network access; Apache releases fall back to archive.apache.org.")

    assert digest is not None
    if pinned_value is not None and digest != pinned_value:
        raise ArtifactError("CHECKSUM_MISMATCH", f"{aid}: archive {algorithm} {digest} != pinned {pinned_value}")

    archive_path = paths.downloads / archive_name(used_url or urls[0])
    if marker_ok and marker_data.get("archive_digest") == digest:
        return InstallResult(aid, version, "REUSED_VERIFIED", home, algorithm, digest, official, source, used_url,
                             str(archive_path), reused_archive=reused_archive, reused_install=True,
                             pinned=pinned_value is not None, warnings=warnings)

    if progress:
        progress(f"{aid}: extracting {archive_path.name}")
    if home.exists() or home.is_symlink():
        move_to_trash(home, trash)
    marker.unlink(missing_ok=True)
    safe_extract(archive_path, top_dir, install_parent, trash_dir=trash)
    atomic_write_json(marker, {
        "id": aid, "version": version, "algorithm": algorithm, "archive_digest": digest,
        "official_digest": official, "checksum_source": source, "source_url": used_url,
        "archive": archive_path.name, "installed_at": iso_now(),
    })
    return InstallResult(aid, version, "VERIFIED", home, algorithm, digest, official, source, used_url,
                         str(archive_path), reused_archive=reused_archive, reused_install=False,
                         pinned=pinned_value is not None, warnings=warnings)


def _cached_or_fetch_official(entry: Mapping[str, Any], paths: WorkPaths, url: str, *, offline: bool,
                              fetch: Callable[[str], bytes]) -> tuple[str | None, str | None]:
    """Official digest for a cached archive: the saved official file, else (online) a fresh fetch."""
    algorithm = entry["checksum"]["algorithm"]
    name = archive_name(url)
    saved = _official_cache_path(paths, name, algorithm)
    if saved.is_file():
        try:
            return parse_checksum_text(saved.read_text(encoding="utf-8"), algorithm, filename=name), \
                f"{saved.name} (saved from the official source)"
        except ArtifactError:
            pass
    if offline:
        return None, None
    digest, src = official_checksum(entry, url, fetch=fetch)
    saved.write_text(f"{digest}  {name}\n", encoding="utf-8")
    return digest, src
