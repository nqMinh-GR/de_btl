"""Synthetic generator: determinism (same seed -> identical bytes/hashes), MBTA JSON shape, scenarios."""

from __future__ import annotations

import gzip
import io
import json
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from transitpulse.config import load_config
from transitpulse.contracts.ids import sha256_hex
from transitpulse.replay.fileio import sha256_file
from transitpulse.replay.recording_verify import verify_recording
from transitpulse.replay.synthetic.demo import generate_demo
from transitpulse.replay.synthetic.gtfs_builder import build_static_model, deterministic_zip
from transitpulse.replay.synthetic.recording import classify_body
from transitpulse.replay.synthetic.scenarios import SCENARIOS, ScenarioError, normalize_scenarios
from transitpulse.replay.timeline import build_timeline
from transitpulse.runtime.paths import WorkPaths

ROUTES = ("R_A", "R_B", "R_C", "R_D", "R_E")


def _env(tmp_path: Path, name: str):
    cfg = load_config("local-native", workdir=str(tmp_path / name), environ={})
    return cfg, WorkPaths.from_workdir(cfg.workdir)


def _tree(directory: Path) -> dict[str, str]:
    return {p.relative_to(directory).as_posix(): sha256_file(p) for p in sorted(directory.rglob("*")) if p.is_file()}


def _manifest(result) -> dict:
    return json.loads(result.manifest_path.read_text(encoding="utf-8"))


def _body(result, item: dict) -> bytes:
    return gzip.decompress((result.recording_dir / item["raw_path"]).read_bytes())


def test_same_seed_gives_byte_identical_recordings(tmp_path):
    cfg_a, paths_a = _env(tmp_path, "a")
    cfg_b, paths_b = _env(tmp_path, "b")
    a = generate_demo(cfg_a, paths_a, seed=42, scenarios=["invalid_gps", "duplicate_snapshot"], duration_seconds=180)
    b = generate_demo(cfg_b, paths_b, seed=42, scenarios=["duplicate_snapshot", "invalid_gps"], duration_seconds=180)
    assert a.recording_id == b.recording_id
    assert a.manifest_sha256 == b.manifest_sha256
    assert a.gtfs_version == b.gtfs_version == sha256_file(a.static_zip_path)
    assert _tree(a.recording_dir) == _tree(b.recording_dir)
    assert not a.reused and not b.reused


def test_different_seed_gives_a_different_recording(tmp_path):
    cfg_a, paths_a = _env(tmp_path, "a")
    cfg_b, paths_b = _env(tmp_path, "b")
    a = generate_demo(cfg_a, paths_a, seed=42, duration_seconds=120)
    b = generate_demo(cfg_b, paths_b, seed=7, duration_seconds=120)
    assert a.recording_id != b.recording_id
    assert a.gtfs_version != b.gtfs_version
    assert a.manifest_sha256 != b.manifest_sha256


def test_generation_is_idempotent(tmp_path):
    cfg, paths = _env(tmp_path, "w")
    first = generate_demo(cfg, paths, seed=42, duration_seconds=120)
    before = first.manifest_path.stat().st_mtime_ns
    second = generate_demo(cfg, paths, seed=42, duration_seconds=120)
    assert second.reused and second.manifest_sha256 == first.manifest_sha256
    assert second.manifest_path.stat().st_mtime_ns == before
    assert second.recording_dir == first.recording_dir


def test_demo_recording_verifies_and_has_mbta_json_shape(tmp_path):
    cfg, paths = _env(tmp_path, "w")
    res = generate_demo(cfg, paths, seed=42, duration_seconds=120)
    verified = verify_recording(res.manifest_path, paths=paths)
    assert verified.source_type == "synthetic" and verified.fixture_kind == "generated" and verified.seed == 42
    assert verified.route_ids == ROUTES
    assert verified.start_clock.isoformat() == "2026-10-05T12:00:00+00:00"
    manifest = _manifest(res)
    assert "NOT MBTA" in manifest["provenance"]
    feeds_seen = set()
    for item in manifest["items"]:
        assert item["kind"] == "capture" and item["parse_status"] == "ok"
        body = _body(res, item)
        assert sha256_hex(body) == item["raw_sha256"]
        doc = json.loads(body)
        assert doc["header"]["gtfs_realtime_version"] == "2.0"
        assert doc["header"]["incrementality"] == "FULL_DATASET"
        assert isinstance(doc["header"]["timestamp"], int)
        assert doc["header"]["timestamp"] == item["feed_timestamp"]
        feeds_seen.add(item["feed_type"])
        for ent in doc["entity"]:
            trip = (ent.get("vehicle") or ent.get("trip_update") or {}).get("trip")
            if trip is not None:
                assert isinstance(trip["direction_id"], int) and trip["route_id"] in ROUTES
            if "vehicle" in ent:
                veh = ent["vehicle"]
                assert isinstance(veh["current_status"], str) and isinstance(veh["timestamp"], int)
                assert 42.30 <= veh["position"]["latitude"] <= 42.40
                assert -71.15 <= veh["position"]["longitude"] <= -71.00
                assert veh["timestamp"] <= doc["header"]["timestamp"]
    assert feeds_seen == {"vehicle_positions", "trip_updates", "alerts"}
    with zipfile.ZipFile(res.static_zip_path) as zf:
        agency = zf.read("agency.txt").decode("utf-8")
    assert "SYN" in agency and "NOT MBTA" in agency


def test_static_model_edge_cases_and_deterministic_zip():
    model = build_static_model(ROUTES, seed=42, service_date=date(2026, 10, 5))
    for route in model.routes.values():
        assert 8 <= len(route.stop_ids) <= 12
    night = model.trips["R_A_NIGHT"]
    assert night.end_secs == 25 * 3600 + 10 * 60 and night.service_id == "DAILY"
    loop = model.trips["R_B_LOOP"]
    assert loop.stop_ids[0] == loop.stop_ids[-1] and len(set(loop.stop_ids)) == len(loop.stop_ids) - 1
    assert model.service_active("WKDY", date(2026, 10, 5))
    assert not model.service_active("WKDY", date(2026, 10, 12))      # calendar_dates removal
    assert not model.service_active("WKDY", date(2026, 10, 10))      # Saturday
    files = model.files()
    assert b"25:10:00" in files["stop_times.txt"]
    assert b"WKDY,20261012,2" in files["calendar_dates.txt"]
    z1, z2 = model.zip_bytes(), build_static_model(ROUTES, seed=42, service_date=date(2026, 10, 5)).zip_bytes()
    assert z1 == z2
    with zipfile.ZipFile(io.BytesIO(z1)) as zf:
        infos = zf.infolist()
    names = [i.filename for i in infos]
    assert names == sorted(names) and "stop_times.txt" in names
    assert all(i.date_time == (1980, 1, 1, 0, 0, 0) and i.compress_type == zipfile.ZIP_STORED for i in infos)
    assert deterministic_zip({"b.txt": b"2", "a.txt": b"1"}) == deterministic_zip({"a.txt": b"1", "b.txt": b"2"})


def test_all_scenarios_apply_and_produce_a_valid_labelled_recording(tmp_path):
    cfg, paths = _env(tmp_path, "w")
    res = generate_demo(cfg, paths, seed=42, scenarios=list(SCENARIOS), duration_seconds=900)
    verified = verify_recording(res.manifest_path, paths=paths)
    manifest = _manifest(res)
    assert manifest["scenario_ids"] == list(SCENARIOS)
    items = build_timeline(manifest)
    assert len(items) == verified.item_count
    labels = json.loads((res.recording_dir / "labels.json").read_text(encoding="utf-8"))
    labelled = {k.split(":")[0] for k in labels}
    expected_labelled = set(SCENARIOS) - {"paused_feed"}   # paused_feed labels only when a resumed capture is free
    assert expected_labelled <= labelled
    by_label = {it["label"]: it for it in manifest["items"] if it.get("label")}

    gps = json.loads(_body(res, by_label["invalid_gps"]))
    assert any(e["vehicle"]["position"]["latitude"] == 95.0 for e in gps["entity"])
    assert by_label["malformed_response:capture"]["parse_status"] == "INVALID_JSON"
    assert classify_body(_body(res, by_label["malformed_response:capture"]), verified.start_clock)[0] == "INVALID_JSON"
    assert by_label["malformed_response:envelope"]["kind"] == "malformed_envelope"
    assert by_label["timeout"]["kind"] == "fetch_event"
    assert by_label["timeout"]["params"]["outcome"] == "timeout"
    arm = by_label["crash_before_pointer:arm"]
    target = by_label["crash_before_pointer:target"]
    assert arm["kind"] == "arm_fault" and arm["seq"] < target["seq"]
    dup = by_label["duplicate_snapshot:republish"]
    assert dup["params"]["of_seq"] == by_label["duplicate_snapshot:original"]["seq"]
    late = by_label["late_snapshot:new_unseen"]
    assert late["at"] < by_label["late_snapshot:newer"]["at"] and late["seq"] > by_label["late_snapshot:newer"]["seq"]
    assert manifest["gaps"] and manifest["gaps"][0]["feed_type"] == "vehicle_positions"
    empty = json.loads(_body(res, by_label["valid_empty_feed"]))
    assert empty["entity"] == []
    cancelled = json.loads(_body(res, by_label["cancelled_trip"]))
    assert any(e["trip_update"]["trip"].get("schedule_relationship") == "CANCELED" for e in cancelled["entity"])
    partial = json.loads(_body(res, by_label["partial_alerts"]))
    assert any(p.get("start") == "abc" for e in partial["entity"] for p in e["alert"].get("active_period", []))


def test_classify_body_is_the_collector_parser():
    from transitpulse.ingestion.feed_parse import Rejection, parse_feed

    at = datetime(2026, 10, 5, 12, 0, 0, tzinfo=UTC)
    ts = int(at.timestamp())
    bodies = [
        b'{"header": {"gtfs_realtime_version": "2.0", "timestamp": %d}, "entity": null}' % ts,
        b'{"header": {"timestamp": %d}, "entity": []}' % ts,
        b'{"header": {"gtfs_realtime_version": "2.0", "incrementality": 0, "timestamp": %d}}' % ts,
        b'{"header": {"gtfs_realtime_version": "3.0", "timestamp": %d}}' % ts,
        b'{"header": {"gtfs_realtime_version": "2.0", "timestamp": %d}}' % (ts * 1000),
        b'{"header": {"gtfs_realtime_version": "2.0", "timestamp": NaN}}',
        b"not json",
    ]
    for body in bodies:
        expected = parse_feed(body, at)
        if isinstance(expected, Rejection):
            assert classify_body(body, at) == (expected.reason_code, None), body
        else:
            assert classify_body(body, at) == ("ok", expected.feed_timestamp), body
    assert classify_body(bodies[0], at)[0] == "ok"                 # entity null -> []
    assert classify_body(bodies[1], at)[0] == "INVALID_HEADER"     # missing version
    assert classify_body(bodies[2], at)[0] == "ok"                 # incrementality 0 == FULL_DATASET


def test_unknown_scenario_is_rejected():
    with pytest.raises(ScenarioError):
        normalize_scenarios(["duplicate_snapshot", "teleport"])
    assert normalize_scenarios(["timeout", "invalid_gps"]) == ("invalid_gps", "timeout")
