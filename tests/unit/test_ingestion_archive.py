"""Unit tests for the raw archive (ingestion.archive): durability, checksums and path guards."""

from __future__ import annotations

import gzip
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from transitpulse.contracts.enums import FEED_ABBR
from transitpulse.contracts.ids import capture_id, sha256_hex
from transitpulse.ingestion.archive import (
    ERROR_BODY_MAX_BYTES,
    RawArchive,
    RawIntegrityError,
    directory_bytes,
    validate_raw_relpath,
)

RUN = "3f1c2a9e-0000-4000-8000-000000000001"
AT = datetime(2026, 10, 5, 12, 0, 31, 123456, tzinfo=UTC)
BODY = b'{"header":{"gtfs_realtime_version":"2.0","timestamp":1791201630},"entity":[]}'


def _cid(body: bytes = BODY, feed: str = "vehicle_positions", at: datetime = AT) -> str:
    return capture_id(FEED_ABBR[feed], at, sha256_hex(body))


@pytest.fixture()
def archive(tmp_path: Path) -> RawArchive:
    return RawArchive(tmp_path / "raw")


def test_write_then_read_roundtrip_with_checksums(archive: RawArchive, tmp_path: Path) -> None:
    cid = _cid()
    ref = archive.write_capture(RUN, "vehicle_positions", cid, AT, BODY, {"http_status": 200})
    assert ref.raw_path == f"{RUN}/vehicle_positions/2026-10-05/{cid}.json.gz"
    assert ref.raw_sha256 == sha256_hex(BODY) and ref.raw_bytes == len(BODY)
    path = tmp_path / "raw" / ref.raw_path
    stored = path.read_bytes()
    assert sha256_hex(stored) == ref.stored_sha256 and len(stored) == ref.stored_bytes
    assert gzip.decompress(stored) == BODY
    assert archive.read_capture(ref.raw_path) == BODY
    assert archive.read_capture(ref.raw_path, expected_sha256=ref.raw_sha256) == BODY
    meta = archive.read_meta(ref.raw_path)
    assert meta is not None
    assert meta["http_status"] == 200 and meta["capture_id"] == cid and meta["raw_sha256"] == ref.raw_sha256
    assert meta["fetched_at"] == "2026-10-05T12:00:31.123456Z"
    assert (path.parent / f"{cid}.meta.json").is_file()
    assert not list(path.parent.glob(".*.tmp-*"))


def test_gzip_output_is_deterministic(archive: RawArchive) -> None:
    other_run = "3f1c2a9e-0000-4000-8000-000000000002"
    a = archive.write_capture(RUN, "vehicle_positions", _cid(), AT, BODY, {})
    b = archive.write_capture(other_run, "vehicle_positions", _cid(), AT, BODY, {})
    assert a.stored_sha256 == b.stored_sha256
    data = archive.resolve(a.raw_path).read_bytes()
    assert data[:2] == b"\x1f\x8b"
    assert data[3] == 0                      # no FNAME flag: the file name is not embedded
    assert data[4:8] == b"\x00\x00\x00\x00"  # mtime = 0


def test_rewrite_is_idempotent_and_meta_update_protects_identity(archive: RawArchive) -> None:
    cid = _cid()
    first = archive.write_capture(RUN, "vehicle_positions", cid, AT, BODY, {"x": 1})
    again = archive.write_capture(RUN, "vehicle_positions", cid, AT, BODY, {"x": 2})
    assert first == again
    merged = archive.update_meta(first.raw_path, {"snapshot_id": "sn_1", "raw_sha256": "forged"})
    assert merged["snapshot_id"] == "sn_1" and merged["raw_sha256"] == first.raw_sha256


def test_capture_id_must_match_body(archive: RawArchive) -> None:
    with pytest.raises(ValueError):
        archive.write_capture(RUN, "vehicle_positions", _cid(b"other body"), AT, BODY, {})
    with pytest.raises(ValueError):
        archive.write_capture(RUN, "trip_updates", _cid(), AT, BODY, {})   # vp id under trip_updates


def test_sha256_mismatch_is_detected(archive: RawArchive) -> None:
    ref = archive.write_capture(RUN, "alerts", _cid(feed="alerts"), AT, BODY, {})
    path = archive.resolve(ref.raw_path)
    path.write_bytes(gzip.compress(b"tampered"))
    with pytest.raises(RawIntegrityError):
        archive.read_capture(ref.raw_path)
    with pytest.raises(RawIntegrityError):
        archive.read_capture(ref.raw_path, expected_sha256="0" * 64)
    path.write_bytes(b"not gzip at all")
    with pytest.raises(RawIntegrityError):
        archive.read_capture(ref.raw_path, expected_sha256=ref.raw_sha256)


def test_read_without_sidecar_checks_capture_id_prefix(archive: RawArchive) -> None:
    ref = archive.write_capture(RUN, "alerts", _cid(feed="alerts"), AT, BODY, {})
    archive.resolve(ref.raw_path[: -len(".json.gz")] + ".meta.json").unlink()
    assert archive.read_capture(ref.raw_path) == BODY
    archive.resolve(ref.raw_path).write_bytes(gzip.compress(b"tampered"))
    with pytest.raises(RawIntegrityError):
        archive.read_capture(ref.raw_path)


def test_decompression_is_bounded(archive: RawArchive) -> None:
    ref = archive.write_capture(RUN, "alerts", _cid(feed="alerts"), AT, BODY, {})
    with pytest.raises(RawIntegrityError):
        archive.read_capture(ref.raw_path, max_bytes=10)


@pytest.mark.parametrize("bad", [
    "../x.json.gz",
    "/etc/passwd",
    f"{RUN}/../../etc/passwd",
    f"{RUN}/vehicle_positions/2026-10-05/../../../../x.json.gz",
    "capture_index.jsonl",
    f"{RUN}/capture_index.jsonl",
    f"{RUN}/vehicle_positions/2026-10-05/evil.json.gz",
    f"{RUN}/bikes/2026-10-05/" + "vp-20261005T120031123456Z-0123456789ab.json.gz",
    f"{RUN}/vehicle_positions/2026-13-5/" + "vp-20261005T120031123456Z-0123456789ab.json.gz",
    f"{RUN}/trip_updates/2026-10-05/" + "vp-20261005T120031123456Z-0123456789ab.json.gz",
    f"{RUN}/vehicle_positions/2026-10-05/" + "vp-20261005T120031123456Z-0123456789ab.txt",
    f"x/{RUN}/vehicle_positions/2026-10-05/" + "vp-20261005T120031123456Z-0123456789ab.json.gz",
    "",
    "vehicle_positions\\2026-10-05\\x.json.gz",
])
def test_unsafe_or_foreign_paths_are_rejected(archive: RawArchive, bad: str) -> None:
    with pytest.raises(ValueError):
        archive.resolve(bad)
    with pytest.raises(ValueError):
        archive.read_capture(bad if bad.endswith(".json.gz") else bad + ".json.gz")


def test_recording_relative_form_is_accepted(archive: RawArchive, tmp_path: Path) -> None:
    rel = "vehicle_positions/2026-10-05/vp-20261005T120031123456Z-0123456789ab.json.gz"
    assert validate_raw_relpath(rel).parts[0] == "vehicle_positions"
    assert archive.resolve(rel) == (tmp_path / "raw" / rel).resolve()
    run_archive = RawArchive(tmp_path / "raw" / RUN)
    ref = archive.write_capture(RUN, "vehicle_positions", _cid(), AT, BODY, {})
    assert run_archive.read_capture(ref.raw_path[len(RUN) + 1:]) == BODY


def test_symlink_escape_is_rejected(archive: RawArchive, tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    feed_dir = tmp_path / "raw" / RUN / "vehicle_positions"
    feed_dir.mkdir(parents=True)
    os.symlink(outside, feed_dir / "2026-10-05")
    with pytest.raises(ValueError):
        archive.resolve(f"{RUN}/vehicle_positions/2026-10-05/{_cid()}.json.gz")


def test_error_bodies_are_truncated_to_64_kib(archive: RawArchive) -> None:
    body = b"x" * (ERROR_BODY_MAX_BYTES + 5000)
    rel = archive.write_error_body(RUN, "alerts", AT, "al-20261005T120031123456Z-e03", body, {"http_status": 503})
    assert rel == f"{RUN}/alerts/2026-10-05/errors/al-20261005T120031123456Z-e03.body.gz"
    assert len(gzip.decompress(archive.resolve(rel).read_bytes())) == ERROR_BODY_MAX_BYTES
    meta = archive.read_meta(rel)
    assert meta is not None and meta["body_truncated"] is True and meta["http_status"] == 503
    with pytest.raises(ValueError):
        archive.write_error_body(RUN, "alerts", AT, "../evil", body, {})


def test_index_append_and_torn_last_line(archive: RawArchive) -> None:
    archive.append_index(RUN, {"kind": "capture", "capture_id": "c1", "feed_type": "alerts"})
    archive.append_index(RUN, {"kind": "fetch_event", "feed_type": "alerts", "outcome": "timeout"})
    with open(archive.index_path(RUN), "ab") as fh:
        fh.write(b'{"kind": "capt')          # crash in the middle of a line
    records = archive.read_index(RUN)
    assert [r["kind"] for r in records] == ["capture", "fetch_event"]
    assert archive.indexed_capture_ids(RUN) == {"c1"}
    assert archive.read_index("3f1c2a9e-0000-4000-8000-0000000000ff") == []


def test_orphan_temp_files_are_quarantined_not_deleted(archive: RawArchive) -> None:
    ref = archive.write_capture(RUN, "vehicle_positions", _cid(), AT, BODY, {})
    date_dir = archive.resolve(ref.raw_path).parent
    old = date_dir / f".{_cid()}.json.gz.tmp-1-deadbeef"
    fresh = date_dir / f".{_cid()}.json.gz.tmp-2-cafebabe"
    old.write_bytes(b"partial")
    fresh.write_bytes(b"in progress")
    past = time.time() - 7200
    os.utime(old, (past, past))
    moved = archive.quarantine_orphans(RUN, older_than_seconds=3600)
    assert moved == [f"vehicle_positions/2026-10-05/{old.name}"]
    assert not old.exists() and fresh.exists()
    quarantined = list((archive.run_dir(RUN) / "quarantine").iterdir())
    assert len(quarantined) == 1 and quarantined[0].read_bytes() == b"partial"


def test_recent_sidecars_limits_to_newest_dates(archive: RawArchive) -> None:
    for days in (0, 1, 2):
        at = AT + timedelta(days=days)
        body = BODY + str(days).encode()
        archive.write_capture(RUN, "trip_updates", _cid(body, "trip_updates", at), at, body, {})
    sidecars = archive.recent_sidecars(RUN, "trip_updates", n_dates=2)
    assert [p.parent.name for p in sidecars] == ["2026-10-06", "2026-10-07"]
    assert len(archive.recent_sidecars(RUN, "trip_updates", n_dates=10)) == 3
    assert archive.recent_sidecars(RUN, "alerts") == []


def test_run_bytes_excludes_the_hard_linked_static_zip(archive: RawArchive) -> None:
    archive.write_capture(RUN, "alerts", _cid(feed="alerts"), AT, BODY, {})
    before = archive.run_bytes(RUN)
    static = archive.run_dir(RUN) / "static"
    static.mkdir()
    (static / "MBTA_GTFS.zip").write_bytes(b"z" * 100_000)
    assert archive.run_bytes(RUN) == before
    assert directory_bytes(archive.run_dir(RUN)) == before + 100_000
