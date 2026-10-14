"""Artifact install: verified download + extract, idempotent rerun, and checksum MISMATCH -> nothing extracted.

The HTTP layer is replaced by an in-memory response (no network); the official checksum file is served by a
fake ``fetch``.
"""

from __future__ import annotations

import hashlib
import io
import tarfile
import urllib.request
from pathlib import Path

import pytest

from transitpulse.runtime.artifacts import ArtifactError, install_artifact
from transitpulse.runtime.paths import WorkPaths

TOP = "kafka_2.13-4.2.2"
NAME = "kafka_2.13-4.2.2.tgz"
URL = f"https://downloads.apache.org/kafka/4.2.2/{NAME}"
ENTRY = {
    "id": "kafka", "version": "4.2.2", "top_dir": TOP, "install_to": "tools/kafka", "urls": [URL],
    "checksum": {"algorithm": "sha512", "value": None, "official_source": "<same-origin url>.sha512",
                 "format": "gpg-print-md"},
    "status": "URL_CONFIRMED",
}


def _archive_bytes() -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, data in ((f"{TOP}/libs/kafka_2.13-4.2.2.jar", b"jar"), (f"{TOP}/bin/kafka-storage.sh", b"#!/bin/sh\n")):
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o755
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _gpg(digest: str) -> bytes:
    groups = " ".join(digest[i : i + 8].upper() for i in range(0, len(digest), 8))
    return f"{NAME}: {groups}\n".encode()


class _Resp:
    def __init__(self, data: bytes) -> None:
        self._buf = io.BytesIO(data)
        self.headers = {"Content-Length": str(len(data))}
        self.status = 200

    def read(self, n: int = -1) -> bytes:
        return self._buf.read(n)

    def __enter__(self) -> _Resp:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


@pytest.fixture()
def served(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    data = _archive_bytes()
    calls = {"downloads": 0, "data": 0}

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _Resp:
        assert req.full_url == URL
        calls["downloads"] += 1
        return _Resp(data)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    calls["data"] = len(data)
    return calls


def _digest() -> str:
    return hashlib.sha512(_archive_bytes()).hexdigest()


@pytest.mark.ct_case("CT07")
def test_checksum_mismatch_fails_and_leaves_nothing(tmp_path: Path, served: dict) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    paths.ensure_base_dirs()
    wrong = hashlib.sha512(b"something else").hexdigest()
    with pytest.raises(ArtifactError) as exc:
        install_artifact(ENTRY, paths, fetch=lambda url: _gpg(wrong))
    assert exc.value.code == "CHECKSUM_MISMATCH"
    assert not (paths.downloads / NAME).exists()                     # never renamed into the cache
    assert list((paths.downloads / ".partial").iterdir()) == []      # partial file removed
    assert not (paths.root / "tools" / "kafka" / TOP).exists()        # nothing extracted
    assert not (paths.root / "tools" / "kafka" / f"{TOP}.tp-installed.json").exists()


@pytest.mark.ct_case("CT07")
def test_pinned_value_disagreeing_with_official_fails_before_download(tmp_path: Path, served: dict) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    paths.ensure_base_dirs()
    entry = {**ENTRY, "checksum": {**ENTRY["checksum"], "value": hashlib.sha512(b"pinned").hexdigest()}}
    with pytest.raises(ArtifactError) as exc:
        install_artifact(entry, paths, fetch=lambda url: _gpg(_digest()))
    assert exc.value.code == "CHECKSUM_MISMATCH"
    assert served["downloads"] == 0


@pytest.mark.ct_case("CT02")
def test_verified_install_then_rerun_reuses_cache(tmp_path: Path, served: dict) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    paths.ensure_base_dirs()
    fetched: list[str] = []

    def fetch(url: str) -> bytes:
        fetched.append(url)
        return _gpg(_digest())

    first = install_artifact(ENTRY, paths, fetch=fetch)
    assert first.status == "VERIFIED" and first.digest == _digest()
    assert fetched == [URL + ".sha512"]
    assert (first.home / "libs" / "kafka_2.13-4.2.2.jar").read_bytes() == b"jar"
    second = install_artifact(ENTRY, paths, fetch=fetch)
    assert second.status == "REUSED_VERIFIED"
    assert served["downloads"] == 1                     # the cached archive was re-hashed, not downloaded
    assert fetched == [URL + ".sha512"]                 # saved official checksum reused


@pytest.mark.ct_case("CT07")
def test_strict_mode_requires_pinned_checksum(tmp_path: Path, served: dict) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    with pytest.raises(ArtifactError) as exc:
        install_artifact(ENTRY, paths, strict=True, fetch=lambda url: _gpg(_digest()))
    assert exc.value.code == "CHECKSUM_NOT_PINNED"


# ---------------------------------------------------------------------------------------- binary artifacts

BIN_URL = "https://github.com/cloudflare/cloudflared/releases/download/2026.10.0/cloudflared-linux-amd64"
BIN_DATA = b"\x7fELF fake cloudflared binary"


def _bin_entry(value: str) -> dict:
    return {"id": "cloudflared", "version": "2026.10.0", "type": "binary", "urls": [BIN_URL],
            "checksum": {"algorithm": "sha256", "value": value, "format": "pinned-release-notes",
                         "official_source": "GitHub release notes"},
            "install_to": "tools/cloudflared/2026.10.0/cloudflared", "mode": "0755", "status": "PINNED"}


@pytest.fixture()
def served_binary(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    calls = {"downloads": 0}

    def fake_urlopen(req: urllib.request.Request, timeout: float = 0) -> _Resp:
        assert req.full_url == BIN_URL
        calls["downloads"] += 1
        return _Resp(BIN_DATA)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    return calls


@pytest.mark.ct_case("CT07")
def test_binary_checksum_mismatch_installs_nothing(tmp_path: Path, served_binary: dict) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    paths.ensure_base_dirs()
    with pytest.raises(ArtifactError) as exc:
        install_artifact(_bin_entry(hashlib.sha256(b"another binary").hexdigest()), paths)
    assert exc.value.code == "CHECKSUM_MISMATCH"
    target = paths.root / "tools" / "cloudflared" / "2026.10.0" / "cloudflared"
    assert not target.exists()
    assert not target.with_name("cloudflared.tp-installed.json").exists()
    assert list((paths.downloads / ".partial").iterdir()) == []


@pytest.mark.ct_case("CT02")
def test_binary_install_is_executable_and_rerun_reuses(tmp_path: Path, served_binary: dict) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    paths.ensure_base_dirs()
    entry = _bin_entry(hashlib.sha256(BIN_DATA).hexdigest())
    first = install_artifact(entry, paths)
    assert first.status == "VERIFIED"
    assert first.home.read_bytes() == BIN_DATA
    assert first.home.stat().st_mode & 0o777 == 0o755
    second = install_artifact(entry, paths)
    assert second.status == "REUSED_VERIFIED"
    assert served_binary["downloads"] == 1


def test_tampered_installed_binary_is_replaced(tmp_path: Path, served_binary: dict) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    paths.ensure_base_dirs()
    entry = _bin_entry(hashlib.sha256(BIN_DATA).hexdigest())
    first = install_artifact(entry, paths)
    first.home.write_bytes(b"tampered")
    again = install_artifact(entry, paths)
    assert again.status == "VERIFIED" and again.home.read_bytes() == BIN_DATA
    assert any(p.name.startswith("cloudflared.") for p in (paths.tools / ".trash").iterdir())


def test_pinned_only_format_without_value_is_rejected(tmp_path: Path, served_binary: dict) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    with pytest.raises(ArtifactError) as exc:
        install_artifact({**_bin_entry("x"), "checksum": {**_bin_entry("x")["checksum"], "value": None}}, paths)
    assert exc.value.code == "CHECKSUM_UNAVAILABLE"
    assert served_binary["downloads"] == 0
