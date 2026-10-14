"""Official checksum file formats (design D §0.5, §1): gpg --print-md, sha256sum, bare hex, Adoptium API."""

from __future__ import annotations

import hashlib

import pytest

from transitpulse.runtime.artifacts import ArtifactError, official_checksum, parse_adoptium_checksum, parse_checksum_text

SHA512 = hashlib.sha512(b"kafka").hexdigest()
SHA256 = hashlib.sha256(b"cassandra").hexdigest()


def _gpg_print_md(name: str, digest: str) -> str:
    """Render a digest the way `gpg --print-md SHA512` does: uppercase, 8-char groups, wrapped lines."""
    groups = [digest[i : i + 8].upper() for i in range(0, len(digest), 8)]
    prefix = f"{name}: "
    lines = [prefix + " ".join(groups[0:6])]
    for start in range(6, len(groups), 6):
        lines.append(" " * len(prefix) + " ".join(groups[start : start + 6]))
    return "\n".join(lines) + "\n"


def test_gpg_print_md_multiline_uppercase() -> None:
    text = _gpg_print_md("kafka_2.13-4.2.2.tgz", SHA512)
    assert "\n" in text.strip()
    assert parse_checksum_text(text, "sha512") == SHA512


def test_sha256sum_format_with_filename() -> None:
    text = f"{SHA256}  apache-cassandra-5.0.9-bin.tar.gz\n"
    assert parse_checksum_text(text, "sha256", filename="apache-cassandra-5.0.9-bin.tar.gz") == SHA256


def test_sha256sum_binary_marker_and_multiple_lines() -> None:
    other = hashlib.sha256(b"other").hexdigest()
    text = f"{other} *other.tar.gz\n{SHA256} *uv-x86_64-unknown-linux-gnu.tar.gz\n"
    assert parse_checksum_text(text, "sha256", filename="uv-x86_64-unknown-linux-gnu.tar.gz") == SHA256


def test_bare_hex_uppercase_and_bom() -> None:
    assert parse_checksum_text("﻿" + SHA256.upper() + "\n", "sha256") == SHA256


@pytest.mark.parametrize("text", ["", "not a checksum", "abc123", SHA256[:-1]])
def test_unparseable_inputs_raise(text: str) -> None:
    with pytest.raises(ArtifactError):
        parse_checksum_text(text, "sha256")


def test_wrong_algorithm_length_rejected() -> None:
    with pytest.raises(ArtifactError):
        parse_checksum_text(SHA256, "sha512")


def test_adoptium_payload_selects_package_by_name() -> None:
    payload = [{"binaries": [
        {"package": {"name": "OpenJDK17U-jre_x64_linux_hotspot_17.0.20.1_1.tar.gz", "checksum": "0" * 64}},
        {"package": {"name": "OpenJDK17U-jdk_x64_linux_hotspot_17.0.20.1_1.tar.gz", "checksum": SHA256.upper(),
                     "checksum_link": "https://example.invalid/x.sha256.txt"}},
    ]}]
    digest, link = parse_adoptium_checksum(payload, "OpenJDK17U-jdk_x64_linux_hotspot_17.0.20.1_1.tar.gz")
    assert digest == SHA256
    assert link.endswith(".sha256.txt")


def test_adoptium_missing_package_raises() -> None:
    with pytest.raises(ArtifactError):
        parse_adoptium_checksum([{"binaries": []}], "missing.tar.gz")
    with pytest.raises(ArtifactError):
        parse_adoptium_checksum({"not": "a list"}, "x")


def test_official_checksum_uses_same_origin_url() -> None:
    fetched: list[str] = []
    entry = {"checksum": {"algorithm": "sha512", "official_source": "<same-origin url>.sha512",
                          "format": "gpg-print-md"}}
    url = "https://archive.apache.org/dist/kafka/4.2.2/kafka_2.13-4.2.2.tgz"

    def fake_fetch(u: str) -> bytes:
        fetched.append(u)
        return _gpg_print_md("kafka_2.13-4.2.2.tgz", SHA512).encode()

    digest, source = official_checksum(entry, url, fetch=fake_fetch)
    assert digest == SHA512
    assert fetched == [url + ".sha512"]
    assert source == url + ".sha512"
