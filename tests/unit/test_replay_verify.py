"""Recording verification before replay (design E §3.1, CT11): checksums, static version, scope."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta

import pytest

from transitpulse.contracts.ids import sha256_hex
from transitpulse.replay.fileio import manifest_sha256
from transitpulse.replay.recording_verify import RecordingVerificationError, verify_recording
from transitpulse.replay.synthetic.gtfs_builder import deterministic_zip
from transitpulse.replay.synthetic.recording import PlannedItem, RecordingSpec, write_recording
from transitpulse.runtime.paths import WorkPaths

T0 = datetime(2026, 10, 5, 12, 0, 0, tzinfo=UTC)
TS0 = int(T0.timestamp())


def _doc(ts: int) -> dict:
    return {"header": {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET", "timestamp": ts},
            "entity": []}


def _make_recording(root):
    zip_bytes = deterministic_zip({"agency.txt": b"agency_id,agency_name\nSYN,Synthetic\n"})
    items = [
        PlannedItem(kind="capture", at=T0, feed_type="vehicle_positions", doc=_doc(TS0 - 5), label="VP1"),
        PlannedItem(kind="capture", at=T0 + timedelta(seconds=30), feed_type="alerts", doc=_doc(TS0 + 25)),
    ]
    spec = RecordingSpec(
        recording_id="rec-verify-0001", fixture_kind="generated", seed=1, scenario_ids=(),
        provenance="unit test; synthetic, NOT MBTA", attribution="not MBTA", static_zip=zip_bytes,
        static_feed_version="t1", static_source_url="synthetic://test/static", route_ids=("R_A", "R_B"),
        route_type=3, scope_policy_version="scope-v1", poll_interval_seconds=30.0,
        feed_source_urls={"vehicle_positions": "synthetic://test/vp", "alerts": "synthetic://test/al"},
        start_clock=T0, generator={"kind": "test", "inputs_sha256": "0" * 64},
    )
    return write_recording(root, spec, items), zip_bytes


def test_valid_recording_verifies(tmp_path):
    res, zip_bytes = _make_recording(tmp_path / "recordings")
    v = verify_recording(res.manifest_path)
    assert v.recording_id == "rec-verify-0001"
    assert v.gtfs_version == sha256_hex(zip_bytes) == res.gtfs_version
    assert v.route_ids == ("R_A", "R_B")
    assert v.start_clock == T0
    assert v.item_count == 2 and v.capture_count == 2
    assert v.static_zip_path == res.recording_dir / "static" / "MBTA_GTFS.zip"
    assert v.manifest_sha256 == manifest_sha256(json.loads(res.manifest_path.read_text()))
    assert v.to_json()["start_clock"] == "2026-10-05T12:00:00.000Z"


def test_route_ids_must_be_a_subset(tmp_path):
    res, _ = _make_recording(tmp_path / "recordings")
    assert verify_recording(res.manifest_path, route_ids=["R_A"]).route_ids == ("R_A",)
    with pytest.raises(RecordingVerificationError) as info:
        verify_recording(res.manifest_path, route_ids=["R_A", "R_Z"])
    assert info.value.code == "ROUTE_NOT_IN_RECORDING"


@pytest.mark.ct_case("CT11")
def test_tampered_manifest_is_rejected(tmp_path):
    res, _ = _make_recording(tmp_path / "recordings")
    doc = json.loads(res.manifest_path.read_text())
    doc["provenance"] = "edited by hand"
    res.manifest_path.write_text(json.dumps(doc))
    with pytest.raises(RecordingVerificationError) as info:
        verify_recording(res.manifest_path)
    assert info.value.code == "MANIFEST_CHECKSUM_MISMATCH"
    # not under <W>/data/raw/<run_id>/: explicit placeholders for the required arguments
    assert "finalize-recording --workdir <W> --run-id <run_id>" in info.value.hint


@pytest.mark.ct_case("CT11")
def test_finalize_hint_names_the_live_recording_workdir_and_run(tmp_path):
    paths = WorkPaths.from_workdir(tmp_path / "w")
    run_dir = paths.raw_run_dir("run-0001-abcdef")
    run_dir.mkdir(parents=True)
    manifest = run_dir / "recording_manifest.json"
    manifest.write_text(json.dumps({"kind": "transitpulse.recording", "manifest_version": 1,
                                    "manifest_sha256": "0" * 64}), encoding="utf-8")
    expected = f"finalize-recording --workdir {paths.root} --run-id run-0001-abcdef"
    for kwargs in ({}, {"paths": paths}):
        with pytest.raises(RecordingVerificationError) as info:
            verify_recording(manifest, **kwargs)
        assert info.value.code == "MANIFEST_CHECKSUM_MISMATCH"
        assert expected in info.value.hint


@pytest.mark.ct_case("CT11")
def test_missing_static_zip_gives_guidance_and_never_downloads(tmp_path):
    res, _ = _make_recording(tmp_path / "recordings")
    (res.recording_dir / "static" / "MBTA_GTFS.zip").unlink()
    with pytest.raises(RecordingVerificationError) as info:
        verify_recording(res.manifest_path)
    assert info.value.code == "STATIC_ZIP_NOT_FOUND"
    assert "will not be downloaded" in info.value.hint
    assert res.gtfs_version in info.value.message


def test_static_zip_falls_back_to_work_dir_static_store(tmp_path):
    res, zip_bytes = _make_recording(tmp_path / "recordings")
    paths = WorkPaths.from_workdir(tmp_path / "w")
    target = paths.static_version_dir(res.gtfs_version) / "MBTA_GTFS.zip"
    target.parent.mkdir(parents=True)
    shutil.move(str(res.recording_dir / "static" / "MBTA_GTFS.zip"), target)
    v = verify_recording(res.manifest_path, paths=paths)
    assert v.static_zip_path == target


@pytest.mark.ct_case("CT11")
def test_wrong_static_zip_is_a_version_mismatch(tmp_path):
    res, _ = _make_recording(tmp_path / "recordings")
    other = tmp_path / "other.zip"
    other.write_bytes(deterministic_zip({"agency.txt": b"agency_id\nOTHER\n"}))
    with pytest.raises(RecordingVerificationError) as info:
        verify_recording(res.manifest_path, static_zip=other)
    assert info.value.code == "STATIC_VERSION_MISMATCH"


@pytest.mark.ct_case("CT11")
def test_raw_checksum_and_presence_are_checked(tmp_path):
    res, _ = _make_recording(tmp_path / "recordings")
    manifest = json.loads(res.manifest_path.read_text())
    raw = res.recording_dir / manifest["items"][0]["raw_path"]
    raw.write_bytes(b"corrupted")
    with pytest.raises(RecordingVerificationError) as info:
        verify_recording(res.manifest_path)
    assert info.value.code == "RAW_CHECKSUM_MISMATCH"
    raw.unlink()
    with pytest.raises(RecordingVerificationError) as info:
        verify_recording(res.manifest_path)
    assert info.value.code == "RAW_MISSING"


def test_missing_manifest(tmp_path):
    with pytest.raises(RecordingVerificationError) as info:
        verify_recording(tmp_path / "nope" / "recording_manifest.json")
    assert info.value.code == "MANIFEST_NOT_FOUND"
