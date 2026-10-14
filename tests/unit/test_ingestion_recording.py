"""Unit tests for the recording manifest (ingestion.recording)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from transitpulse.contracts.ids import canonical_json, sha256_hex
from transitpulse.ingestion.recording import (
    MANIFEST_KIND,
    build_items,
    build_manifest,
    compute_gaps,
    manifest_sha256,
    read_manifest,
    static_info,
    verify_manifest_sha256,
    write_manifest,
)

RUN = "4e5f6a7b-8c9d-4e0f-a1b2-c3d4e5f6a7b8"
URLS = {
    "vehicle_positions": "https://cdn.mbta.com/realtime/VehiclePositions.json",
    "trip_updates": "https://cdn.mbta.com/realtime/TripUpdates.json",
    "alerts": "https://cdn.mbta.com/realtime/Alerts.json",
}
INTERVALS = {"vehicle_positions": 30.0, "trip_updates": 30.0, "alerts": 30.0}


def capture(feed: str, at: str, cid: str, status: str = "ok") -> dict[str, Any]:
    return {"kind": "capture", "feed_type": feed, "at": at, "capture_id": cid,
            "raw_path": f"{RUN}/{feed}/2026-10-05/{cid}.json.gz", "raw_sha256": "a" * 64, "raw_bytes": 10,
            "stored_sha256": "b" * 64, "stored_bytes": 8, "parse_status": status, "http_status": 200,
            "feed_timestamp": 1791201600, "snapshot_id": "sn_x", "outbox_state": "pending"}


def event(feed: str, at: str, outcome: str, status: int | None, reason: str | None) -> dict[str, Any]:
    return {"kind": "fetch_event", "feed_type": feed, "at": at, "outcome": outcome, "http_status": status,
            "reason_code": reason, "attempts": 3}


def make_run(**over: Any) -> SimpleNamespace:
    base: dict[str, Any] = dict(run_id=RUN, scope_id="scope0001", source_type="live", gtfs_version="f" * 64,
                                route_ids=("R1", "R2"), scenario_id=None, recording_id=None, fixture_kind=None,
                                seed=None, static_zip_path=None, created_at=datetime(2026, 10, 5, 11, 59, tzinfo=UTC))
    base.update(over)
    return SimpleNamespace(**base)


INDEX = [
    capture("trip_updates", "2026-10-05T12:00:10.000Z", "tu-20261005T120010000000Z-aaaaaaaaaaaa"),
    capture("vehicle_positions", "2026-10-05T12:00:00.000Z", "vp-20261005T120000000000Z-aaaaaaaaaaaa"),
    capture("vehicle_positions", "2026-10-05T12:00:00.000Z", "vp-20261005T120000000000Z-aaaaaaaaaaaa"),  # dup line
    event("vehicle_positions", "2026-10-05T12:00:30.000Z", "not_modified", 304, None),
    capture("alerts", "2026-10-05T12:00:20.000Z", "al-20261005T120020000000Z-aaaaaaaaaaaa", "INVALID_JSON"),
    event("vehicle_positions", "2026-10-05T12:01:00.000Z", "timeout", None, "SOURCE_FETCH_FAILED"),
    event("vehicle_positions", "2026-10-05T12:01:30.000Z", "http_error", 503, "SOURCE_FETCH_FAILED"),
    capture("vehicle_positions", "2026-10-05T12:02:00.000Z", "vp-20261005T120200000000Z-bbbbbbbbbbbb"),
    capture("trip_updates", "2026-10-05T12:00:10.000Z", "tu-20261005T120010000000Z-bbbbbbbbbbbb", "PAYLOAD_TOO_LARGE"),
]


def test_items_are_sorted_deduplicated_and_relative_to_the_run_dir() -> None:
    items = build_items(INDEX, RUN)
    assert [i["seq"] for i in items] == list(range(1, 9))
    assert [(i["at"][11:19], i["feed_type"]) for i in items] == [
        ("12:00:00", "vehicle_positions"), ("12:00:10", "trip_updates"), ("12:00:10", "trip_updates"),
        ("12:00:20", "alerts"), ("12:00:30", "vehicle_positions"), ("12:01:00", "vehicle_positions"),
        ("12:01:30", "vehicle_positions"), ("12:02:00", "vehicle_positions"),
    ]
    first = items[0]
    assert first["raw_path"] == "vehicle_positions/2026-10-05/vp-20261005T120000000000Z-aaaaaaaaaaaa.json.gz"
    assert not first["raw_path"].startswith(RUN)
    assert first["params"] == {}
    ev = items[4]
    assert ev["kind"] == "fetch_event" and ev["capture_id"] is None and ev["raw_path"] is None
    assert ev["params"] == {"outcome": "not_modified", "http_status": 304, "reason_code": None}
    assert set(first) == {"seq", "kind", "feed_type", "at", "capture_id", "raw_path", "raw_sha256", "raw_bytes",
                          "stored_sha256", "parse_status", "http_status", "feed_timestamp", "params"}


def test_manifest_shape_counts_and_checksum(tmp_path: Path) -> None:
    m = build_manifest(run=make_run(), index_records=INDEX, source_urls=URLS, poll_intervals=INTERVALS,
                       user_agent="UA", route_type=3, scope_policy_version="scope-v1",
                       created_at=datetime(2026, 10, 5, 11, 59, tzinfo=UTC), complete=True,
                       static={"gtfs_version": "f" * 64, "zip_sha256": "f" * 64, "zip_relpath": None,
                               "feed_version": None, "fetched_at": None, "source_url": None},
                       finalized_at=datetime(2026, 10, 5, 12, 5, tzinfo=UTC))
    assert m["manifest_version"] == 1 and m["kind"] == MANIFEST_KIND
    assert m["recording_id"] == RUN and m["source_type"] == "live" and m["complete"] is True
    assert m["scope"] == {"scope_id": "scope0001", "route_ids": ["R1", "R2"], "route_type": 3,
                          "policy_version": "scope-v1"}
    assert m["collector"]["poll_interval_seconds"] == 30.0
    assert m["time_range"] == {"first_fetched_at": "2026-10-05T12:00:00.000Z",
                               "last_fetched_at": "2026-10-05T12:02:00.000Z", "source_duration_seconds": 120.0}
    assert m["feeds"]["vehicle_positions"] == {"source_url": URLS["vehicle_positions"], "capture_count": 2,
                                               "rejected_count": 0, "not_modified_count": 1, "failed_fetch_count": 2}
    assert m["feeds"]["trip_updates"]["capture_count"] == 2 and m["feeds"]["trip_updates"]["rejected_count"] == 1
    assert m["feeds"]["alerts"]["rejected_count"] == 1
    assert m["manifest_sha256"] == sha256_hex(canonical_json({k: v for k, v in m.items() if k != "manifest_sha256"}))
    assert verify_manifest_sha256(m)
    tampered = dict(m, complete=False)
    assert not verify_manifest_sha256(tampered)
    assert manifest_sha256(tampered) != m["manifest_sha256"]
    path = write_manifest(tmp_path, m)
    loaded = read_manifest(tmp_path)
    assert loaded == json.loads(path.read_text(encoding="utf-8")) == m
    assert verify_manifest_sha256(loaded)


def test_gaps_report_periods_without_successful_polls() -> None:
    items = build_items(INDEX, RUN)
    gaps = compute_gaps(items, INTERVALS)
    assert gaps == [{"feed_type": "vehicle_positions", "start": "2026-10-05T12:00:30.000Z",
                     "end": "2026-10-05T12:02:00.000Z", "seconds": 90.0, "failed_or_rejected_polls": 2}]


def test_empty_recording() -> None:
    m = build_manifest(run=make_run(scenario_id="simulated_source_fault:x", source_type="synthetic"),
                       index_records=[], source_urls=URLS, poll_intervals=INTERVALS, user_agent="UA", route_type=3,
                       scope_policy_version="scope-v1", created_at=datetime(2026, 10, 5, tzinfo=UTC), complete=False,
                       static={})
    assert m["items"] == [] and m["gaps"] == [] and m["complete"] is False
    assert m["time_range"] == {"first_fetched_at": None, "last_fetched_at": None, "source_duration_seconds": 0}
    assert m["scenario_ids"] == ["simulated_source_fault:x"]
    assert verify_manifest_sha256(m)


def test_static_info_hard_links_the_pinned_zip(tmp_path: Path) -> None:
    vdir = tmp_path / "static" / ("f" * 64)
    vdir.mkdir(parents=True)
    zip_path = vdir / "MBTA_GTFS.zip"
    zip_path.write_bytes(b"PK\x05\x06" + b"\x00" * 18)
    (vdir / "version.json").write_text(json.dumps({"zip_sha256": "f" * 64, "feed_version": "v-2026",
                                                   "downloaded_at": "2026-10-05T10:00:00.000Z",
                                                   "source_url": "https://cdn.mbta.com/MBTA_GTFS.zip"}))
    raw_run_dir = tmp_path / "raw" / RUN
    info = static_info(make_run(static_zip_path=str(zip_path)), raw_run_dir)
    assert info["zip_relpath"] == "static/MBTA_GTFS.zip"
    linked = raw_run_dir / "static" / "MBTA_GTFS.zip"
    assert linked.read_bytes() == zip_path.read_bytes()
    assert linked.stat().st_ino == zip_path.stat().st_ino          # hard link, no extra disk
    assert info["feed_version"] == "v-2026" and info["fetched_at"] == "2026-10-05T10:00:00.000Z"
    assert static_info(make_run(static_zip_path=str(zip_path)), raw_run_dir)["zip_relpath"] == "static/MBTA_GTFS.zip"


def test_static_info_without_zip_never_invents_values(tmp_path: Path) -> None:
    info = static_info(make_run(), tmp_path / "raw" / RUN)
    assert info == {"gtfs_version": "f" * 64, "zip_sha256": "f" * 64, "zip_relpath": None, "feed_version": None,
                    "fetched_at": None, "source_url": None}
