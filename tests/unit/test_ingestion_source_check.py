"""Unit tests for ``source-check`` with scripted responses (no network; nothing here is MBTA data)."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from transitpulse.config import load_config
from transitpulse.ingestion.source_check import (
    REQUIRED_STATIC_FILES,
    analyze_alerts,
    analyze_trip_updates,
    inspect_static_zip,
    run_source_check,
)
from transitpulse.runtime.paths import WorkPaths

CHECKED_AT = datetime(2026, 10, 7, 12, 0, 0, tzinfo=UTC)

STATIC_FILES = {
    "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\n1,Test Agency,https://example.test,America/New_York\n",
    "routes.txt": "route_id,agency_id,route_short_name,route_long_name,route_type\nR1,1,1,Route One,3\nRED,1,,Red Line,1\n",
    "trips.txt": "route_id,service_id,trip_id\nR1,WK,T1\nRED,WK,TR\n",
    "stops.txt": "stop_id,stop_name,stop_lat,stop_lon,parent_station\nS1,Stop 1,42.3,-71.1,P1\nP1,Station,42.3,-71.1,\n",
    "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,12:00:00,12:00:00,S1,1\n"
                      "TR,12:00:00,12:00:00,S9,1\n",
    "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    "WK,1,1,1,1,1,0,0,20261001,20261031\n",
    "calendar_dates.txt": "service_id,date,exception_type\nWK,20261012,2\n",
    "shapes.txt": "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\nSH1,42.3,-71.1,1\n",
    "feed_info.txt": "feed_publisher_name,feed_publisher_url,feed_lang,feed_version,feed_start_date,feed_end_date\n"
                     "Test,https://example.test,en,TEST-1,20261001,20261031\n",
}


def make_zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, text in files.items():
            zf.writestr(name, text)
    return buf.getvalue()


def feed(entities: list[dict[str, Any]]) -> bytes:
    return json.dumps({"header": {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET",
                                  "timestamp": int(time.time()) - 20}, "entity": entities}).encode()


VP = [
    {"id": "e1", "vehicle": {"trip": {"route_id": "R1", "trip_id": "T1"}, "vehicle": {"id": "y1"},
                             "position": {"latitude": 42.3, "longitude": -71.1}, "timestamp": int(time.time()) - 30}},
    {"id": "e2", "vehicle": {"trip": {"route_id": "RED"}, "vehicle": {"id": "y2"}}},
]
ALERTS = [{"id": "a1", "alert": {"informed_entity": [{"agency_id": "1", "route_id": "R1", "route_type": 3}],
                                 "active_period": [{"start": 1}], "severity_level": "WARNING", "effect": "DETOUR"}}]


def transport(static: bytes, tu_status: int = 503) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        if name == "MBTA_GTFS.zip":
            return httpx.Response(200, content=static, headers={"Content-Type": "application/zip"})
        if name == "VehiclePositions.json":
            return httpx.Response(200, content=feed(VP), headers={"ETag": '"e"', "Content-Type": "application/json"})
        if name == "TripUpdates.json":
            return httpx.Response(tu_status, content=b"maintenance")
        if name == "Alerts.json":
            return httpx.Response(200, content=feed(ALERTS))
        return httpx.Response(404)
    return httpx.MockTransport(handler)


@pytest.fixture()
def ctx(tmp_path: Path) -> tuple[Any, WorkPaths]:
    return load_config("local-native", workdir=tmp_path, environ={}), WorkPaths.from_workdir(tmp_path)


def test_offline_mode_writes_not_checked_entries_and_fabricates_nothing(ctx: Any) -> None:
    cfg, paths = ctx
    report, path = run_source_check(cfg, paths, route_ids=["R1"], offline=True, checked_at=CHECKED_AT)
    assert path == paths.reports / "source_probe_report.json" and path.is_file()
    assert json.loads(path.read_text(encoding="utf-8")) == report
    assert {e["status"] for e in report["sources"].values()} == {"not_checked"}
    assert all(e["http_status"] is None and e["parse_ok"] is None for e in report["sources"].values())
    assert report["summary"] == {"all_ok": False, "verified_sources": [], "failed_sources": [],
                                 "not_checked_sources": ["gtfs_static", "vehicle_positions", "trip_updates", "alerts"]}
    assert not (paths.root / "data" / "source_check").exists()


def test_probe_reports_real_outcomes_per_source(ctx: Any, tmp_path: Path) -> None:
    cfg, paths = ctx
    static = make_zip(STATIC_FILES)
    out = tmp_path / "custom" / "probe.json"
    report, path = run_source_check(cfg, paths, route_ids=["R1"], out=out, transport=transport(static),
                                    checked_at=CHECKED_AT)
    assert path == out and out.is_file()
    summary = report["summary"]
    assert summary["verified_sources"] == ["gtfs_static", "vehicle_positions", "alerts"]
    assert summary["failed_sources"] == ["trip_updates"] and summary["all_ok"] is False

    tu = report["sources"]["trip_updates"]
    assert tu["status"] == "failed" and tu["http_status"] == 503 and tu["error"]["kind"] == "http"
    assert tu["parse_ok"] is False and "evidence" not in tu

    st = report["sources"]["gtfs_static"]
    assert st["status"] == "ok" and st["zip_sha256"] == hashlib.sha256(static).hexdigest()
    assert st["missing"] == [] and st["empty"] == [] and st["feed_version"] == "TEST-1"
    assert st["calendar_range"] == {"start": "20261001", "end": "20261031"}
    assert st["calendar_dates_range"] == {"start": "20261012", "end": "20261012"}
    assert st["agency_timezones"] == ["America/New_York"]
    assert [r["route_id"] for r in st["bus_routes"]] == ["R1"]
    assert st["row_counts"]["stop_times.txt"] == 2 and st["row_counts"]["routes.txt"] == 2
    assert Path(st["evidence"]["path"]).read_bytes() == static
    assert st["realtime_active_bus_routes"] == [{"route_id": "R1", "short_name": "1", "long_name": "Route One",
                                                 "vehicle_count": 1}]

    vp = report["sources"]["vehicle_positions"]
    assert vp["status"] == "ok" and vp["parse_ok"] is True and vp["has_etag"] is True
    assert vp["entity_count"] == 2 and vp["header_age_seconds"] >= 0
    assert vp["gtfs_realtime_version"] == "2.0" and vp["incrementality"] == "FULL_DATASET"
    assert vp["analysis"]["missing_fields"]["vehicle.timestamp"] == 1
    assert vp["route_id_distribution"] == {"R1": 1, "RED": 1}
    evidence = Path(vp["evidence"]["path"])
    assert hashlib.sha256(gzip.decompress(evidence.read_bytes())).hexdigest() == vp["evidence"]["sha256"]

    scope = report["scope"]
    assert scope["static_available"] is True and scope["unknown_route_ids"] == [] and scope["non_bus_route_ids"] == []
    assert scope["in_scope_counts"]["vehicle_positions"]["selected"] == 1
    assert scope["in_scope_counts"]["trip_updates"] is None
    assert scope["in_scope_counts"]["alerts"]["selected"] == 1
    size = scope["estimated_envelope_bytes"]["vehicle_positions"]
    assert size["fits_limit"] is True and 0 < size["bytes"] <= cfg.kafka.max_snapshot_bytes


def test_network_failure_is_reported_never_verified(ctx: Any) -> None:
    cfg, paths = ctx

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("[Errno -3] Temporary failure in name resolution", request=request)

    report, _ = run_source_check(cfg, paths, route_ids=[], transport=httpx.MockTransport(handler),
                                 checked_at=CHECKED_AT)
    assert report["summary"]["verified_sources"] == []
    assert len(report["summary"]["failed_sources"]) == 4
    assert {e["error"]["kind"] for e in report["sources"].values()} == {"dns"}
    assert not list((paths.root / "data" / "source_check" / "static").glob("*.zip"))


def test_incomplete_static_zip_is_not_verified(tmp_path: Path) -> None:
    files = {k: v for k, v in STATIC_FILES.items() if k != "shapes.txt"}
    files["calendar_dates.txt"] = "service_id,date,exception_type\n"
    zpath = tmp_path / "gtfs.zip"
    zpath.write_bytes(make_zip(files))
    probe = inspect_static_zip(zpath, ["R1"])
    assert probe.missing == ["shapes.txt"] and probe.empty == ["calendar_dates.txt"] and not probe.complete
    assert probe.scope_stop_ids == {"S1", "P1"}
    assert set(probe.files_present) == set(REQUIRED_STATIC_FILES) - {"shapes.txt"}


def test_trip_update_uncertainty_absent_is_reported_separately() -> None:
    entities = [{"id": "t", "trip_update": {"trip": {"trip_id": "T1", "schedule_relationship": "SCHEDULED"},
                                            "stop_time_update": [
                                                {"stop_sequence": 1, "arrival": {"time": 1, "uncertainty": 0}},
                                                {"stop_id": "S1", "departure": {"time": 2}},
                                                {"stop_sequence": 3, "arrival": {"time": 3, "uncertainty": 300,
                                                                                 "delay": 60}},
                                            ]}}]
    a = analyze_trip_updates(entities)
    assert a["uncertainty_distribution"]["absent"] == 1
    assert a["uncertainty_distribution"]["values"] == {"0": 1, "300": 1}
    assert a["uncertainty_distribution"]["classes"] == {"realtime": 1, "schedule_only": 1}
    assert a["stop_time_update_total"] == 3 and a["missing_stop_sequence"] == 1 and a["missing_stop_id"] == 2
    assert a["delay_present_count"] == 1 and a["trip_update_timestamp_missing"] == 1
    assert a["schedule_relationship_distribution"] == {"SCHEDULED": 1}


def test_alert_analysis() -> None:
    a = analyze_alerts(ALERTS + [{"id": "a2", "alert": {"informed_entity": [{"stop_id": "S1"}]}}])
    assert a["selector_kind_distribution"] == {"agency_id+route_id+route_type": 1, "stop_id": 1}
    assert a["alerts_with_active_period"] == 1 and a["alerts_without_active_period"] == 1
    assert a["severity_level_distribution"] == {"WARNING": 1, "absent": 1}
