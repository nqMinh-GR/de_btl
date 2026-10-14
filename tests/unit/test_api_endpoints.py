"""Serving API endpoint tests against the in-memory repository (unit: no Cassandra, no network).

These are unit tests of the API contract (envelope, status codes, serving-time freshness, generation
states, KPI nulls, CORS, request ids). They are never counted as integration evidence.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient

from transitpulse.api.app import create_app
from transitpulse.api.memory_repository import InMemoryRepository
from transitpulse.api.repository import RepositoryError
from transitpulse.api.settings import ApiSettings
from transitpulse.config import ProjectConfig, load_config
from transitpulse.contracts.policy import Policy

BASE = "/api/v1"
RUN = "6f1c2a3b-0000-4000-8000-000000000001"
RUN_B = "6f1c2a3b-0000-4000-8000-000000000002"
RUN_C = "6f1c2a3b-0000-4000-8000-000000000003"
GV = "gv1"
VP, TU, AL = "vehicle_positions", "trip_updates", "alerts"

WALL = datetime(2026, 10, 6, 9, 0, 0, tzinfo=UTC)          # wall clock of the API (expiry checks)
LIVE_UNTIL = WALL + timedelta(hours=1)                     # expires_at of the seeded generations
C1 = datetime(2026, 10, 5, 12, 1, 10, tzinfo=UTC)          # application clock (source mode)
C2 = datetime(2026, 10, 5, 12, 3, 10, tzinfo=UTC)          # later application clock


def t(hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(2026, 10, 5, hh, mm, ss, tzinfo=UTC)


BACKLOG = [
    {"topic": "mbta.trip_updates.snapshots.v1", "partition": 0, "end_offset": 40, "latest_offset": 40, "backlog": 0},
    {"topic": "mbta.vehicle_positions.snapshots.v1", "partition": 0, "end_offset": 120, "latest_offset": 122,
     "backlog": 2},
    {"topic": "mbta.alerts.snapshots.v1", "partition": 0, "end_offset": 7, "latest_offset": None, "backlog": None},
]

ISO_LIKE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")
ISO_Z_MS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


# ================================================================================================
# Seed data
# ================================================================================================
def run_row(cfg: ProjectConfig, run_id: str = RUN, **over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "run_id": run_id,
        "project_id": cfg.project.project_id,
        "scope_id": "sc_golden",
        "source_type": "synthetic",
        "clock_mode": "source",
        "gtfs_version": GV,
        "agency_timezone": "America/New_York",
        "route_ids": ["R_A", "R_B"],
        "pipeline_version": "p1",
        "policy_version": "policy-v1",
        "policy_json": Policy.from_config(cfg).to_json(),
        "scenario_id": "golden",
        "created_at": t(11, 59),
        "run_state": "RUNNING",
        "application_clock": C1,
        "clock_sequence": 3,
    }
    row.update(over)
    return row


def vehicle(route_id: str, vehicle_id: str, obs: datetime | None, **over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "route_id": route_id,
        "vehicle_id": vehicle_id,
        "observation_id": f"ob_{vehicle_id}",
        "record_kind": "observation",
        "observation_time": obs,
        "event_time": obs,
        "event_time_origin": "vehicle_timestamp",
        "freshness_known": True,
        "position_valid": True,
        "invalid_reason": None,
        "latitude": 42.35,
        "longitude": -71.06,
        "bearing": 90.0,
        "speed_mps": None,
        "vehicle_label": f"L{vehicle_id}",
        "trip_id": "T1",
        "direction_id": 0,
        "start_date": "20261005",
        "feed_timestamp": t(12, 1),
        "is_inherited": False,
        "quality_flags": [],
    }
    row.update(over)
    return row


def seed(repo: InMemoryRepository, cfg: ProjectConfig) -> None:
    repo.put("run_metadata", run_row(cfg))
    repo.put("schema_migrations", {"migration_id": "0001_initial"})
    for rid, created in ((RUN, t(11, 59)), (RUN_B, t(11, 58)), (RUN_C, t(11, 57))):
        repo.put("runs_by_project", {"project_id": cfg.project.project_id, "created_at": created, "run_id": rid,
                                     "source_type": "synthetic", "gtfs_version": GV})
    repo.put("run_metadata", run_row(cfg, RUN_B, run_state="FINISHED", created_at=t(11, 58)))

    # ---- static catalogue
    repo.put("routes_by_version", {"gtfs_version": GV, "route_id": "R_A", "route_short_name": "A",
                                   "route_long_name": "Route A", "route_type": 3, "route_color": "FFC72C"})
    repo.put("routes_by_version", {"gtfs_version": GV, "route_id": "R_B", "route_short_name": "B",
                                   "route_long_name": "Route B", "route_type": 3})
    for route_id, stops in (("R_A", ("S1", "S2")), ("R_B", ("S2", "S3"))):
        for stop_id in stops:
            repo.put("stops_by_route", {"gtfs_version": GV, "route_id": route_id, "stop_id": stop_id,
                                        "stop_name": f"Stop {stop_id}", "stop_lat": 42.3, "stop_lon": -71.0,
                                        "direction_ids": [0], "pattern_count": 1})
    for seq, stop_id, secs, raw in ((1, "S1", 43200, "12:00:00"), (2, "S2", 43530, "12:05:30"),
                                    (3, "S3", 90600, "25:10:00")):
        repo.put("stop_times_by_trip", {"gtfs_version": GV, "trip_id": "T1", "stop_sequence": seq, "stop_id": stop_id,
                                        "arrival_time_raw": raw, "departure_time_raw": raw, "arrival_seconds": secs,
                                        "departure_seconds": secs, "route_id": "R_A", "direction_id": 0,
                                        "service_id": "WKDY", "shape_id": "SH1", "trip_headsign": "Downtown"})
    repo.put("stop_times_by_trip", {"gtfs_version": GV, "trip_id": "T9", "stop_sequence": 1, "stop_id": "S9",
                                    "arrival_seconds": 40000, "route_id": "R_Z"})
    repo.put("geometries_by_route", {"gtfs_version": GV, "route_id": "R_A", "shape_id": "SH1", "direction_ids": [0],
                                     "trip_count": 4, "point_count": 3, "simplified": False, "is_representative": True,
                                     "points_json": "[[-71.06, 42.35], [-71.05, 42.36], [-71.04, 42.355]]"})

    # ---- vehicle_positions generation (header 12:01:00)
    vehicles = [
        vehicle("R_A", "V1", t(12, 0, 55)),
        vehicle("R_A", "V2", None, record_kind="snapshot_state_sample", event_time=t(12, 1),
                event_time_origin="feed_header", freshness_known=False, observation_id="ss_V2"),
        vehicle("R_A", "V3", t(12, 0, 50), invalid_reason="FUTURE_TIMESTAMP"),
        vehicle("R_B", "V4", t(12, 0, 58)),
        vehicle("R_B", "V5", t(12, 1, 12)),
    ]
    vp_metrics = [
        {"route_id": "R_A", "metrics_as_of": t(12, 1), "fresh_vehicle_count": 1, "stale_vehicle_count": 0,
         "unknown_age_vehicle_count": 1, "invalid_vehicle_count": 1, "snapshot_membership_count": 3,
         "inherited_vehicle_count": 0, "stale_threshold_seconds": 120},
        {"route_id": "R_B", "metrics_as_of": t(12, 1), "fresh_vehicle_count": 2, "stale_vehicle_count": 0,
         "unknown_age_vehicle_count": 0, "invalid_vehicle_count": 0, "snapshot_membership_count": 2,
         "inherited_vehicle_count": 0, "stale_threshold_seconds": 120},
    ]
    vp_quality = [
        {"reason_code": "MISSING_OBSERVATION_TIMESTAMP", "severity": "warning", "unit": "entity", "record_count": 1},
        {"reason_code": "FUTURE_TIMESTAMP", "severity": "error", "unit": "entity", "record_count": 1,
         "sample_refs": ["V3"]},
    ]
    repo.publish_generation(
        run_id=RUN, feed_type=VP, snapshot_id="sn_vp1", feed_timestamp=t(12, 1), metrics_as_of=t(12, 1),
        expires_at=LIVE_UNTIL,
        tables={"vehicles_by_route_snapshot": vehicles, "route_metrics_by_snapshot": vp_metrics,
                "quality_by_snapshot": vp_quality},
        manifest_extra={"source_entity_count": 9, "selected_entity_count": 5, "fully_valid_entity_count": 3,
                        "partially_valid_entity_count": 1, "rejected_entity_count": 1,
                        "accounting_json": json.dumps({"membership": 5, "fresh": 3})},
    )

    # ---- trip_updates generation (header 12:00:50)
    predictions = [
        {"stop_id": "S2", "predicted_at": t(12, 5, 30), "prediction_id": "pr_1", "trip_id": "T1", "route_id": "R_A",
         "direction_id": 0, "event_type": "arrival", "stop_sequence": 2, "scheduled_at": t(11, 58, 50),
         "predicted_delay_seconds": 400, "uncertainty": 60, "prediction_quality": "realtime",
         "schedule_resolution": "resolved", "kpi_eligible": True, "is_target": True},
        {"stop_id": "S2", "predicted_at": t(12, 0, 0), "prediction_id": "pr_2", "trip_id": "T2", "route_id": "R_A",
         "direction_id": 0, "event_type": "arrival", "prediction_quality": "realtime"},
        {"stop_id": "S2", "predicted_at": t(12, 6, 0), "prediction_id": "pr_3", "trip_id": "T3", "route_id": "R_B",
         "direction_id": 1, "event_type": "departure", "uncertainty": 400, "prediction_quality": "invalid"},
    ]
    tu_metrics = [
        {"route_id": "R_A", "metrics_as_of": t(12, 1), "trip_instance_count": 3, "eligible_trip_count": 2,
         "predicted_late_trip_count": 1, "predicted_late_rate": 0.5, "mean_predicted_delay_seconds": 200.0,
         "excluded_trip_counts": {"NO_TARGET_STOP": 1}, "unknown_uncertainty_count": 0,
         "late_threshold_seconds": 300, "target_policy": "first_future_resolved_stop_v1",
         "policy_version": "policy-v1"},
        {"route_id": "R_B", "metrics_as_of": t(12, 1), "trip_instance_count": 1, "eligible_trip_count": 0,
         "predicted_late_trip_count": 0, "predicted_late_rate": 0.0, "mean_predicted_delay_seconds": 0.0,
         "excluded_trip_counts": {"SCHEDULE_ONLY_PREDICTION": 1}, "unknown_uncertainty_count": 0,
         "late_threshold_seconds": 300, "target_policy": "first_future_resolved_stop_v1"},
    ]
    repo.publish_generation(
        run_id=RUN, feed_type=TU, snapshot_id="sn_tu1", feed_timestamp=t(12, 0, 50), metrics_as_of=t(12, 1),
        expires_at=LIVE_UNTIL,
        tables={"predictions_by_stop_snapshot": predictions, "route_metrics_by_snapshot": tu_metrics},
    )

    # ---- alerts generation (header 12:00:40)
    def epoch(dt: datetime) -> int:
        return int(dt.timestamp())

    a1 = {"alert_id": "A1", "effect": "DETOUR", "cause": "CONSTRUCTION", "header_text": "Detour",
          "active_periods_json": json.dumps([{"start": epoch(t(11, 0)), "end": epoch(t(13, 0))}]),
          "confirmation_status": "CONFIRMED", "match_kinds": ["route"]}
    a2 = {"alert_id": "A2", "effect": "STOP_MOVED", "header_text": "Stop moved",
          "active_periods_json": json.dumps([{"start": epoch(t(14, 0))}]), "confirmation_status": "CONFIRMED"}
    repo.publish_generation(
        run_id=RUN, feed_type=AL, snapshot_id="sn_al1", feed_timestamp=t(12, 0, 40), metrics_as_of=t(12, 1),
        expires_at=LIVE_UNTIL,
        tables={"alerts_by_route_snapshot": [{**a1, "route_id": "R_A"}, {**a1, "route_id": "R_B"},
                                             {**a2, "route_id": "R_B"}]},
    )

    # ---- pipeline status
    repo.put("pipeline_status", {"run_id": RUN, "component": "processor", "status": "RUNNING",
                                 "last_heartbeat_at": WALL - timedelta(seconds=3), "last_batch_id": 7,
                                 "last_batch_duration_ms": 1500})
    # Same shape and component as processing.stream_job.ProgressListener (backlog_entries on progress events).
    repo.put("pipeline_status", {"run_id": RUN, "component": "heartbeat", "status": "ACTIVE",
                                 "last_heartbeat_at": WALL - timedelta(seconds=2), "last_batch_id": 7,
                                 "progress_json": json.dumps({"query_id": "q-1", "batch_id": 7, "numInputRows": 3,
                                                              "kafka_backlog": BACKLOG})})
    repo.put("pipeline_status", {"run_id": RUN, "component": "collector:vehicle_positions", "status": "degraded",
                                 "last_error": "HTTP 503"})
    repo.put("pipeline_status", {"run_id": RUN, "component": "collector:alerts", "status": "backoff"})
    repo.put("pipeline_status", {"run_id": RUN, "component": "feed:trip_updates", "status": "ok",
                                 "last_visibility_latency_ms": 2500})


# ================================================================================================
# Fixtures and helpers
# ================================================================================================
@pytest.fixture()
def cfg(tmp_path: Any) -> ProjectConfig:
    return load_config("local-native", workdir=tmp_path, environ={})


@pytest.fixture()
def repo(cfg: ProjectConfig) -> InMemoryRepository:
    r = InMemoryRepository()
    seed(r, cfg)
    return r


@pytest.fixture()
def wall() -> dict[str, datetime]:
    return {"now": WALL}


@pytest.fixture()
def client(cfg: ProjectConfig, repo: InMemoryRepository, wall: dict[str, datetime]) -> Iterator[TestClient]:
    settings = ApiSettings.from_config(
        cfg, token_key=b"k" * 32, configure_logging=False, wall_clock=lambda: wall["now"]
    )
    with TestClient(create_app(settings, repo)) as c:
        yield c


def get(client: TestClient, path: str, **params: Any) -> Any:
    return client.get(BASE + path, params=params)


def strings(obj: Any) -> Iterator[str]:
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from strings(v)


def codes(body: dict[str, Any]) -> list[str]:
    return [w["code"] for w in body["warnings"]]


def assert_error(resp: Any, status: int, code: str) -> dict[str, Any]:
    assert resp.status_code == status, resp.text
    body = resp.json()
    assert set(body) == {"error", "meta"}
    error = body["error"]
    assert set(error) == {"code", "message", "request_id", "retryable", "details", "hint"}
    assert error["code"] == code
    assert error["request_id"] == resp.headers["x-request-id"]
    return body


# ================================================================================================
# Envelope, health, request ids, CORS
# ================================================================================================
def test_envelope_shape_and_iso_z_times(client: TestClient) -> None:
    resp = get(client, "/vehicles", run_id=RUN, route_id="R_A")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == {"data", "meta", "warnings"}
    meta = body["meta"]
    assert meta["run_id"] == RUN
    assert meta["source_type"] == "synthetic"
    assert meta["application_clock"] == "2026-10-05T12:01:10.000Z"
    assert meta["served_at"] == "2026-10-06T09:00:00.000Z"
    assert meta["feeds"]["vehicle_positions"]["snapshot_id"] == "sn_vp1"
    assert meta["feeds"]["vehicle_positions"]["state"] == "available"
    assert meta["feeds"]["vehicle_positions"]["metrics_as_of"] == "2026-10-05T12:01:00.000Z"
    assert meta["coverage"]["route_ids"] == ["R_A"]
    assert meta["freshness"]["vehicle_stale_threshold_seconds"] == 120
    assert meta["request_id"] == resp.headers["x-request-id"]
    assert resp.headers["cache-control"] == "no-store"
    instants = [s for s in strings(body) if ISO_LIKE.match(s)]
    assert instants
    for s in instants:
        assert ISO_Z_MS.match(s), s


def test_health_live_and_ready(client: TestClient) -> None:
    live = get(client, "/health/live")
    assert live.status_code == 200
    assert live.json()["data"] == {"status": "live"}
    ready = get(client, "/health/ready")
    assert ready.status_code == 200, ready.text
    checks = ready.json()["data"]["checks"]
    assert checks["cassandra"] == "ok"
    assert checks["schema_tables"] == "ok"
    assert checks["schema_version"] == "0001_initial"


def test_database_down_live_200_ready_and_data_503(client: TestClient, repo: InMemoryRepository) -> None:
    repo.failure = RepositoryError("database_unavailable", "Cassandra is down")
    assert get(client, "/health/live").status_code == 200
    body = assert_error(get(client, "/health/ready"), 503, "database_unavailable")
    assert body["error"]["retryable"] is True
    resp = get(client, "/vehicles", run_id=RUN, route_id="R_A")
    assert_error(resp, 503, "database_unavailable")
    assert resp.headers["retry-after"]


def test_ready_503_when_schema_missing(client: TestClient, repo: InMemoryRepository) -> None:
    repo.schema_present = False
    assert_error(get(client, "/health/ready"), 503, "schema_not_ready")


def test_request_id_is_echoed_or_generated(client: TestClient) -> None:
    resp = client.get(f"{BASE}/health/live", headers={"X-Request-ID": "client-1.a_b"})
    assert resp.headers["x-request-id"] == "client-1.a_b"
    assert resp.json()["meta"]["request_id"] == "client-1.a_b"
    bad = client.get(f"{BASE}/health/live", headers={"X-Request-ID": "bad id!"})
    assert bad.headers["x-request-id"].startswith("req-")
    err = client.get(f"{BASE}/runs/does-not-exist", headers={"X-Request-ID": "trace-42"})
    assert err.status_code == 404
    assert err.json()["error"]["request_id"] == "trace-42"


def test_cors_allowlist(client: TestClient) -> None:
    allowed = client.get(f"{BASE}/health/live", headers={"Origin": "http://localhost:8501"})
    assert allowed.headers["access-control-allow-origin"] == "http://localhost:8501"
    denied = client.get(f"{BASE}/health/live", headers={"Origin": "http://evil.example"})
    assert "access-control-allow-origin" not in denied.headers
    preflight = client.options(
        f"{BASE}/vehicles",
        headers={"Origin": "http://127.0.0.1:8501", "Access-Control-Request-Method": "GET"},
    )
    assert preflight.status_code == 200
    assert preflight.headers["access-control-allow-origin"] == "http://127.0.0.1:8501"
    bad_origin = client.options(
        f"{BASE}/vehicles", headers={"Origin": "http://evil.example", "Access-Control-Request-Method": "GET"}
    )
    assert bad_origin.status_code == 400
    bad_method = client.options(
        f"{BASE}/vehicles", headers={"Origin": "http://localhost:8501", "Access-Control-Request-Method": "POST"}
    )
    assert bad_method.status_code == 400


def test_unknown_path_uses_error_envelope(client: TestClient) -> None:
    assert_error(client.get(f"{BASE}/no-such-endpoint"), 404, "not_found")


def test_openapi_lists_every_endpoint(client: TestClient) -> None:
    resp = client.get(f"{BASE}/openapi.json")
    assert resp.status_code == 200
    paths = set(resp.json()["paths"])
    expected = {
        "/health/live", "/health/ready", "/runs", "/runs/{run_id}", "/routes", "/routes/{route_id}/geometry",
        "/routes/{route_id}/stops", "/trips/{trip_id}/stops", "/vehicles", "/vehicles/{vehicle_id}/history",
        "/stops/{stop_id}/predictions", "/routes/{route_id}/metrics", "/alerts", "/quality", "/pipeline/status",
        "/metrics",
    }
    assert {BASE + p for p in expected} <= paths


def test_metrics_endpoint_merges_textfiles_without_duplicates(client: TestClient, tmp_path: Any) -> None:
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir(exist_ok=True)
    (metrics_dir / "collector.prom").write_text(
        "# HELP tp_collector_selected_entities x\n# TYPE tp_collector_selected_entities gauge\n"
        "tp_collector_selected_entities 7.0\n",
        encoding="utf-8",
    )
    (metrics_dir / "api.prom").write_text(
        "# HELP tp_api_requests_total stale\n# TYPE tp_api_requests_total counter\n"
        'tp_api_requests_total{route="x",status="200"} 999.0\n',
        encoding="utf-8",
    )
    get(client, "/health/live")
    resp = get(client, "/metrics")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    text = resp.text
    assert "tp_api_requests_total" in text
    assert "tp_collector_selected_entities 7.0" in text
    assert text.count("# TYPE tp_api_requests_total") == 1
    assert "999.0" not in text
    assert 'route="/api/v1/health/live"' in text          # label is the full path template


# ================================================================================================
# Vehicles and freshness
# ================================================================================================
def test_freshness_recomputed_at_later_application_clock(client: TestClient, repo: InMemoryRepository) -> None:
    first = get(client, "/vehicles", run_id=RUN, route_id="R_A").json()
    by_id = {v["vehicle_id"]: v for v in first["data"]["vehicles"]}
    assert by_id["V1"]["freshness_status"] == "fresh"
    assert by_id["V1"]["observation_age_seconds"] == pytest.approx(15.0)
    assert by_id["V2"]["freshness_status"] == "unknown"
    assert by_id["V2"]["observation_age_seconds"] is None
    assert by_id["V3"]["freshness_status"] == "invalid"
    assert by_id["V3"]["invalid_reason"] == "FUTURE_TIMESTAMP"
    assert first["data"]["counts"] == {
        "fresh_vehicle_count": 1, "stale_vehicle_count": 0, "unknown_age_vehicle_count": 1,
        "invalid_vehicle_count": 1, "invalid_without_vehicle_id_count": 0, "snapshot_membership_count": 3,
        "inherited_vehicle_count": 0, "clock_skew_vehicle_count": 0,
    }

    # No new data, only the replay clock advances: V1 becomes 135 s old -> stale.
    repo.put("run_metadata", {"run_id": RUN, "application_clock": C2, "clock_sequence": 4})
    second = get(client, "/vehicles", run_id=RUN, route_id="R_A").json()
    by_id = {v["vehicle_id"]: v for v in second["data"]["vehicles"]}
    assert second["meta"]["application_clock"] == "2026-10-05T12:03:10.000Z"
    assert by_id["V1"]["freshness_status"] == "stale"
    assert by_id["V1"]["observation_age_seconds"] == pytest.approx(135.0)
    assert by_id["V3"]["freshness_status"] == "invalid"  # frozen invalid decision never recomputed
    counts = second["data"]["counts"]
    assert counts["fresh_vehicle_count"] == 0
    assert counts["stale_vehicle_count"] == 1
    assert counts["snapshot_membership_count"] == 3
    # the VP header (12:01:00) is 130 s old at 12:03:10 > 120 s
    assert second["meta"]["feeds"]["vehicle_positions"]["state"] == "stale"
    assert second["meta"]["feeds"]["vehicle_positions"]["stale"] is True
    assert "FEED_STALE" in codes(second)


def test_invalid_entities_without_vehicle_id_are_counted(client: TestClient, repo: InMemoryRepository) -> None:
    # The processor counted 3 invalid entities on R_A; only V3 has a row (2 lacked a vehicle id).
    repo.put("route_metrics_by_snapshot", {"run_id": RUN, "feed_type": VP, "snapshot_id": "sn_vp1",
                                           "route_id": "R_A", "invalid_vehicle_count": 3})
    route_a = get(client, "/vehicles", run_id=RUN, route_id="R_A").json()["data"]
    assert route_a["counts"]["invalid_vehicle_count"] == 3
    assert route_a["counts"]["invalid_without_vehicle_id_count"] == 2
    assert route_a["counts"]["snapshot_membership_count"] == 3      # row-less entities are not members
    assert len(route_a["vehicles"]) == 3
    every = get(client, "/vehicles", run_id=RUN).json()["data"]["counts"]
    assert every["invalid_vehicle_count"] == 3                       # R_B adds no row-less invalid entity
    metrics = get(client, "/routes/R_A/metrics", run_id=RUN).json()["data"]["vehicle_metrics"]
    assert metrics["counts"]["invalid_vehicle_count"] == 3
    assert metrics["counts"]["invalid_without_vehicle_id_count"] == 2
    assert metrics["counts"]["fresh_vehicle_count"] == 1


def test_stored_clock_skew_flag_is_replaced_by_serving_evaluation(client: TestClient,
                                                                  repo: InMemoryRepository) -> None:
    # V4 was skewed when processed; at the request clock it is 12 s old: CLOCK_SKEW must not be shown.
    repo.put("vehicles_by_route_snapshot", {"run_id": RUN, "snapshot_id": "sn_vp1", "route_id": "R_B",
                                            "vehicle_id": "V4", "quality_flags": ["CLOCK_SKEW", "OBSERVATION_TIME_REGRESSION"]})
    by_id = {v["vehicle_id"]: v for v in get(client, "/vehicles", run_id=RUN, route_id="R_B").json()["data"]["vehicles"]}
    assert by_id["V4"]["quality_flags"] == ["OBSERVATION_TIME_REGRESSION"]
    assert "CLOCK_SKEW" in by_id["V5"]["quality_flags"]             # recomputed at serving time


def test_clock_skew_shown_as_zero_age_with_flag(client: TestClient) -> None:
    body = get(client, "/vehicles", run_id=RUN, route_id="R_B").json()
    by_id = {v["vehicle_id"]: v for v in body["data"]["vehicles"]}
    assert by_id["V5"]["freshness_status"] == "fresh"
    assert by_id["V5"]["observation_age_seconds"] == 0.0
    assert "CLOCK_SKEW" in by_id["V5"]["quality_flags"]
    assert by_id["V4"]["observation_age_seconds"] == pytest.approx(12.0)
    assert body["data"]["counts"]["clock_skew_vehicle_count"] == 1
    assert "CLOCK_SKEW" in codes(body)


def test_vehicles_fan_out_over_scope_and_filter(client: TestClient) -> None:
    body = get(client, "/vehicles", run_id=RUN).json()
    assert body["data"]["route_ids"] == ["R_A", "R_B"]
    assert [v["vehicle_id"] for v in body["data"]["vehicles"]] == ["V1", "V2", "V3", "V4", "V5"]
    assert body["data"]["counts"]["snapshot_membership_count"] == 5
    # source problems reach `warnings` only
    assert "SOURCE_FETCH_FAILED" in codes(body)
    stale_only = get(client, "/vehicles", run_id=RUN, status="stale").json()
    assert stale_only["data"]["vehicles"] == []
    assert stale_only["data"]["reason"] == "NO_MATCHING_VEHICLES"
    assert stale_only["data"]["counts"]["fresh_vehicle_count"] == 3  # counts describe all vehicles
    limited = get(client, "/vehicles", run_id=RUN, limit=2).json()
    assert limited["data"]["returned_count"] == 2
    assert limited["data"]["truncated"] is True
    assert "RESULT_TRUNCATED" in codes(limited)


def test_fan_out_without_route_requires_small_scope(client: TestClient, repo: InMemoryRepository,
                                                    cfg: ProjectConfig) -> None:
    repo.put("run_metadata", run_row(cfg, route_ids=["R_A", "R_B", "R_C", "R_D", "R_E", "R_F"]))
    assert_error(get(client, "/vehicles", run_id=RUN), 422, "route_id_required")
    assert_error(get(client, "/alerts", run_id=RUN), 422, "route_id_required")
    assert get(client, "/vehicles", run_id=RUN, route_id="R_A").status_code == 200


# ================================================================================================
# Status codes: 404 / 422 / 503
# ================================================================================================
def test_not_found_codes(client: TestClient) -> None:
    assert_error(get(client, "/vehicles", run_id="no-such-run"), 404, "run_not_found")
    assert_error(get(client, "/vehicles", run_id=RUN, route_id="R_Z"), 404, "route_not_in_scope")
    assert_error(get(client, "/routes/R_Z/stops", run_id=RUN), 404, "route_not_in_scope")
    assert_error(get(client, "/routes/R_Z/metrics", run_id=RUN), 404, "route_not_in_scope")
    assert_error(get(client, "/stops/S9/predictions", run_id=RUN), 404, "stop_not_in_scope")
    assert_error(get(client, "/trips/T9/stops", run_id=RUN), 404, "trip_not_found")
    assert_error(get(client, "/trips/T404/stops", run_id=RUN), 404, "trip_not_found")
    assert_error(get(client, "/runs/no-such-run"), 404, "run_not_found")


def test_validation_errors_use_error_envelope(client: TestClient) -> None:
    body = assert_error(get(client, "/vehicles"), 422, "validation_error")
    assert any("run_id" in d["loc"] for d in body["error"]["details"])
    assert_error(get(client, "/vehicles", run_id=RUN, limit=0), 422, "validation_error")
    assert_error(get(client, "/vehicles", run_id=RUN, status="bogus"), 422, "validation_error")
    assert_error(get(client, "/stops/S2/predictions", run_id=RUN, direction_id=2), 422, "validation_error")
    assert_error(get(client, "/runs", limit=101), 422, "validation_error")


def test_generation_expired_is_503_not_zero_vehicles(client: TestClient, wall: dict[str, datetime]) -> None:
    wall["now"] = LIVE_UNTIL + timedelta(seconds=1)
    body = assert_error(get(client, "/vehicles", run_id=RUN, route_id="R_A"), 503, "generation_expired")
    assert body["error"]["retryable"] is True
    assert body["meta"]["feeds"]["vehicle_positions"]["state"] == "expired"
    assert "data" not in body


def test_valid_empty_snapshot_is_200_with_reason(client: TestClient, repo: InMemoryRepository) -> None:
    repo.publish_generation(
        run_id=RUN, feed_type=VP, snapshot_id="sn_vp_empty", feed_timestamp=t(12, 1, 5), metrics_as_of=t(12, 1, 5),
        expires_at=LIVE_UNTIL, is_empty=True,
        tables={"vehicles_by_route_snapshot": [],
                "route_metrics_by_snapshot": [{"route_id": "R_A", "fresh_vehicle_count": 0},
                                              {"route_id": "R_B", "fresh_vehicle_count": 0}]},
    )
    body = get(client, "/vehicles", run_id=RUN, route_id="R_A").json()
    assert body["data"]["vehicles"] == []
    assert body["data"]["reason"] == "EMPTY_SNAPSHOT"
    assert body["data"]["counts"]["snapshot_membership_count"] == 0
    assert body["meta"]["feeds"]["vehicle_positions"]["state"] == "empty"
    assert body["meta"]["feeds"]["vehicle_positions"]["snapshot_id"] == "sn_vp_empty"
    assert "EMPTY_SNAPSHOT" in codes(body)


def test_lost_rows_are_generation_inconsistent(client: TestClient, repo: InMemoryRepository) -> None:
    # expected_counts says 3 rows for R_A; one row disappeared (e.g. TTL drift) -> never "2 vehicles"
    repo.delete("vehicles_by_route_snapshot", run_id=RUN, snapshot_id="sn_vp1", vehicle_id="V2")
    body = assert_error(get(client, "/vehicles", run_id=RUN, route_id="R_A"), 503, "generation_inconsistent")
    assert body["meta"]["feeds"]["vehicle_positions"]["state"] == "inconsistent"
    assert body["error"]["details"][0]["feed_type"] == "vehicle_positions"


def test_leftover_rows_are_generation_inconsistent(client: TestClient, repo: InMemoryRepository) -> None:
    # a row the manifest does not account for (e.g. left by an aborted build) is never served
    repo.put("vehicles_by_route_snapshot", {**vehicle("R_A", "V9", t(12, 0, 59)), "run_id": RUN,
                                            "snapshot_id": "sn_vp1"})
    body = assert_error(get(client, "/vehicles", run_id=RUN, route_id="R_A"), 503, "generation_inconsistent")
    assert body["meta"]["feeds"]["vehicle_positions"]["state"] == "inconsistent"


def test_leftover_row_detected_when_expected_equals_limit(tmp_path: Any, repo: InMemoryRepository) -> None:
    # max_total_rows = 3 == expected rows of R_A: reads use LIMIT 4, so one leftover row is still detected
    small = load_config("local-native", workdir=tmp_path, environ={}, overrides={"api": {"max_total_rows": 3}})
    settings = ApiSettings.from_config(small, token_key=b"k" * 32, configure_logging=False, wall_clock=lambda: WALL)
    with TestClient(create_app(settings, repo)) as c:
        resp = get(c, "/vehicles", run_id=RUN, route_id="R_A")
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        assert data["returned_count"] == 3                  # never more than the limit
        assert data["truncated"] is False and data["counts_complete"] is True
    repo.put("vehicles_by_route_snapshot", {**vehicle("R_A", "V9", t(12, 0, 59)), "run_id": RUN,
                                            "snapshot_id": "sn_vp1"})
    with TestClient(create_app(settings, repo)) as c:      # fresh app: nothing cached
        assert_error(get(c, "/vehicles", run_id=RUN, route_id="R_A"), 503, "generation_inconsistent")


def test_audit_quality_rows_after_ready_are_not_inconsistent(client: TestClient, repo: InMemoryRepository) -> None:
    # DUPLICATE/LATE/EXPIRED audit rows are added to a READY generation outside its expected counts
    repo.put("quality_by_snapshot", {"run_id": RUN, "feed_type": VP, "snapshot_id": "sn_vp1",
                                     "reason_code": "DUPLICATE_SNAPSHOT_DELIVERY", "record_count": 1})
    body = get(client, "/quality", run_id=RUN).json()
    codes_vp = [r["reason_code"] for r in body["data"]["feeds"][VP]["reasons"]]
    assert "DUPLICATE_SNAPSHOT_DELIVERY" in codes_vp
    assert body["meta"]["feeds"]["vehicle_positions"]["state"] == "available"


def test_manifest_counts_above_rows_are_inconsistent(client: TestClient, repo: InMemoryRepository) -> None:
    repo.publish_generation(
        run_id=RUN, feed_type=AL, snapshot_id="sn_al2", feed_timestamp=t(12, 1), metrics_as_of=t(12, 1),
        expires_at=LIVE_UNTIL,
        tables={"alerts_by_route_snapshot": [{"route_id": "R_A", "alert_id": "A9"}]},
        expected_counts={"alerts_by_route_snapshot": 3, "alerts_by_route_snapshot/R_A": 3},
    )
    assert_error(get(client, "/alerts", run_id=RUN, route_id="R_A"), 503, "generation_inconsistent")


def test_missing_pointer_is_generation_unavailable(client: TestClient, repo: InMemoryRepository,
                                                   cfg: ProjectConfig) -> None:
    assert_error(get(client, "/vehicles", run_id=RUN_B, route_id="R_A"), 503, "generation_unavailable")
    detail = get(client, f"/runs/{RUN_B}").json()
    assert {g["state"] for g in detail["data"]["generations"].values()} == {"unavailable"}
    assert "RUN_FINISHED" in codes(detail)


def test_null_source_clock_is_run_clock_unavailable(client: TestClient, repo: InMemoryRepository,
                                                    cfg: ProjectConfig) -> None:
    repo.put("run_metadata", run_row(cfg, application_clock=None))
    assert_error(get(client, "/vehicles", run_id=RUN, route_id="R_A"), 503, "run_clock_unavailable")
    status = get(client, "/pipeline/status", run_id=RUN)
    assert status.status_code == 200
    assert status.json()["data"]["application_clock"] is None


def test_wall_clock_run_uses_wall_now(client: TestClient, repo: InMemoryRepository, cfg: ProjectConfig) -> None:
    repo.put("run_metadata", run_row(cfg, source_type="live", clock_mode="wall", application_clock=None))
    body = get(client, "/vehicles", run_id=RUN, route_id="R_A").json()
    assert body["meta"]["application_clock"] == body["meta"]["served_at"] == "2026-10-06T09:00:00.000Z"
    assert {v["freshness_status"] for v in body["data"]["vehicles"]} == {"stale", "unknown", "invalid"}


# ================================================================================================
# Metrics / KPI
# ================================================================================================
def test_feed_pointer_read_once_per_feed(client: TestClient, repo: InMemoryRepository) -> None:
    repo.reset_reads()
    assert get(client, "/routes/R_A/metrics", run_id=RUN).status_code == 200
    assert repo.read_count("feed_current") == 2
    assert repo.read_count("run_metadata") == 1
    repo.reset_reads()
    assert get(client, "/quality", run_id=RUN).status_code == 200
    assert repo.read_count("feed_current") == 3


def test_route_metrics_kpi_and_null_denominator(client: TestClient) -> None:
    body = get(client, "/routes/R_A/metrics", run_id=RUN).json()
    pm = body["data"]["prediction_metrics"]
    assert pm["eligible_trip_count"] == 2
    assert pm["predicted_late_trip_count"] == 1
    assert pm["predicted_late_rate"] == 0.5
    assert pm["mean_predicted_delay_seconds"] == 200.0
    assert pm["excluded_trip_count"] == 1
    assert pm["excluded_by_reason"] == {"NO_TARGET_STOP": 1}
    assert pm["denominator_unit"] == "trip_instance"
    assert pm["late_threshold_seconds"] == 300
    vm = body["data"]["vehicle_metrics"]
    assert vm["counts"]["fresh_vehicle_count"] == 1
    assert vm["counts"]["invalid_vehicle_count"] == 1
    assert vm["stored_counts_at_as_of"]["snapshot_membership_count"] == 3
    assert body["meta"]["coverage"]["denominator_unit"] == "trip_instance"

    zero = get(client, "/routes/R_B/metrics", run_id=RUN).json()["data"]["prediction_metrics"]
    assert zero["eligible_trip_count"] == 0
    assert zero["predicted_late_rate"] is None          # never "0 % late"
    assert zero["mean_predicted_delay_seconds"] is None
    assert zero["predicted_late_trip_count"] == 0
    assert zero["excluded_by_reason"] == {"SCHEDULE_ONLY_PREDICTION": 1}


def test_route_metrics_partial_200_when_one_feed_expired(client: TestClient, repo: InMemoryRepository,
                                                         wall: dict[str, datetime]) -> None:
    repo.put("feed_current", {"run_id": RUN, "feed_type": TU, "expires_at": WALL - timedelta(seconds=1)})
    body = get(client, "/routes/R_A/metrics", run_id=RUN).json()
    assert body["data"]["prediction_metrics"] is None
    assert body["data"]["vehicle_metrics"] is not None
    assert body["meta"]["feeds"]["trip_updates"]["state"] == "expired"
    assert any(w["code"] == "GENERATION_EXPIRED" and w["feed_type"] == TU for w in body["warnings"])
    wall["now"] = LIVE_UNTIL + timedelta(seconds=1)
    assert_error(get(client, "/routes/R_A/metrics", run_id=RUN), 503, "generation_expired")


def test_stale_trip_updates_flag_kpi(client: TestClient, repo: InMemoryRepository) -> None:
    repo.put("run_metadata", {"run_id": RUN, "application_clock": C2})
    body = get(client, "/routes/R_A/metrics", run_id=RUN).json()
    pm = body["data"]["prediction_metrics"]
    assert pm["stale"] is True                  # header 12:00:50 is 140 s old > 90 s
    assert pm["feed_age_seconds"] == pytest.approx(140.0)
    assert any(w["code"] == "FEED_STALE" and w["feed_type"] == TU for w in body["warnings"])


# ================================================================================================
# Predictions, alerts, quality, pipeline
# ================================================================================================
def test_stop_predictions_window_and_quality_filter(client: TestClient) -> None:
    body = get(client, "/stops/S2/predictions", run_id=RUN).json()
    data = body["data"]
    assert [p["prediction_id"] for p in data["predictions"]] == ["pr_1"]  # pr_2 before window, pr_3 invalid
    first = data["predictions"][0]
    assert first["stop_id"] == "S2"
    assert first["trip_id"] == "T1"
    assert first["stop_sequence"] == 2
    assert first["predicted_time"] == "2026-10-05T12:05:30.000Z"
    assert first["predicted_delay_seconds"] == 400
    assert first["prediction_quality"] == "realtime"
    assert data["route_ids"] == ["R_A", "R_B"]
    assert data["stop_name"] == "Stop S2"
    assert data["window_start"] == "2026-10-05T12:00:10.000Z"
    every = get(client, "/stops/S2/predictions", run_id=RUN, include="all").json()["data"]
    assert [p["prediction_id"] for p in every["predictions"]] == ["pr_1", "pr_3"]
    by_route = get(client, "/stops/S2/predictions", run_id=RUN, include="all", route_id="R_B").json()["data"]
    assert [p["prediction_id"] for p in by_route["predictions"]] == ["pr_3"]


def test_stop_without_predictions_returns_reason(client: TestClient) -> None:
    body = get(client, "/stops/S1/predictions", run_id=RUN).json()
    assert body["data"]["predictions"] == []
    assert body["data"]["reason"] == "NO_PREDICTION_DATA"
    assert body["data"]["reason_detail"]
    assert "NO_PREDICTION_DATA" in codes(body)


def test_alerts_are_service_alerts_and_pipeline_signals_are_warnings(client: TestClient) -> None:
    body = get(client, "/alerts", run_id=RUN).json()
    alerts = {a["alert_id"]: a for a in body["data"]["alerts"]}
    assert set(alerts) == {"A1", "A2"}
    assert alerts["A1"]["route_ids"] == ["R_A", "R_B"]          # merged across route partitions
    assert {a["category"] for a in alerts.values()} == {"MBTA_SERVICE_ALERT"}
    assert alerts["A1"]["active_state_at_clock"] == "ACTIVE"
    assert alerts["A2"]["active_state_at_clock"] == "UPCOMING"
    assert alerts["A1"]["active_periods"] == [
        {"start": "2026-10-05T11:00:00.000Z", "end": "2026-10-05T13:00:00.000Z"}
    ]
    warning = next(w for w in body["warnings"] if w["code"] == "SOURCE_FETCH_FAILED")
    assert warning["category"] == "PIPELINE_DATA_QUALITY_ALERT"
    assert warning["feed_type"] == "alerts"
    assert all(w["feed_type"] in (None, "alerts") for w in body["warnings"])  # only the feed that was read
    active = get(client, "/alerts", run_id=RUN, active_state="ACTIVE").json()["data"]["alerts"]
    assert [a["alert_id"] for a in active] == ["A1"]
    route_a = get(client, "/alerts", run_id=RUN, route_id="R_A").json()["data"]["alerts"]
    assert [a["alert_id"] for a in route_a] == ["A1"]


def test_quality_reasons_and_accounting(client: TestClient) -> None:
    body = get(client, "/quality", run_id=RUN).json()
    feeds = body["data"]["feeds"]
    assert set(feeds) == {VP, TU, AL}
    vp = feeds[VP]
    assert [r["reason_code"] for r in vp["reasons"]] == ["FUTURE_TIMESTAMP", "MISSING_OBSERVATION_TIMESTAMP"]
    assert vp["reasons"][0]["disposition"] == "excluded_from_metric"
    assert vp["reasons"][0]["unit"] == "entity"
    assert vp["reasons"][0]["count"] == 1
    assert vp["accounting"] == {"membership": 5, "fresh": 3}
    assert vp["rejected_entity_count"] == 1
    assert feeds[TU]["reasons"] == []
    components = [c["component"] for c in body["data"]["source_health"]]
    assert "collector:vehicle_positions" in components
    assert "processor" not in components


def test_pipeline_status_components(client: TestClient) -> None:
    data = get(client, "/pipeline/status", run_id=RUN).json()["data"]
    components = {c["component"]: c for c in data["components"]}
    assert list(components) == sorted(components)
    processor = components["processor"]
    assert processor["heartbeat_age_seconds"] == pytest.approx(3.0)
    assert processor["last_batch_duration_seconds"] == pytest.approx(1.5)
    assert data["kafka_backlog"] == BACKLOG                  # read from the heartbeat row, producer's shape
    assert components["heartbeat"]["heartbeat_age_seconds"] == pytest.approx(2.0)
    assert data["latency"] == {"trip_updates": 2.5}
    assert {g["state"] for g in data["generations"].values()} == {"available"}
    assert data["application_clock"] == "2026-10-05T12:01:10.000Z"


# ================================================================================================
# Runs and static catalogue
# ================================================================================================
def test_runs_list_paging_and_detail(client: TestClient) -> None:
    first = get(client, "/runs", limit=2).json()["data"]
    assert [r["run_id"] for r in first["runs"]] == [RUN, RUN_B]
    assert first["runs"][0]["application_clock"] == "2026-10-05T12:01:10.000Z"
    token = first["next_page_token"]
    assert token
    second = get(client, "/runs", limit=2, page_token=token).json()["data"]
    assert [r["run_id"] for r in second["runs"]] == [RUN_C]
    assert second["runs"][0]["run_state"] is None          # index row without run_metadata
    assert second["next_page_token"] is None
    tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
    assert_error(get(client, "/runs", page_token=tampered), 422, "invalid_page_token")

    detail = get(client, f"/runs/{RUN}").json()
    data = detail["data"]
    assert data["policy_pinned"] is True
    assert data["policy"]["vehicle_stale_seconds"] == 120
    assert data["route_ids"] == ["R_A", "R_B"]
    assert set(data["generations"]) == {VP, TU, AL}
    assert data["generations"][VP]["snapshot_id"] == "sn_vp1"
    assert {c["component"] for c in data["source_health"]} == {
        "collector:alerts", "collector:vehicle_positions", "feed:trip_updates"
    }


def test_static_routes_geometry_and_stops(client: TestClient) -> None:
    routes = get(client, "/routes", run_id=RUN).json()["data"]
    assert [r["route_id"] for r in routes["routes"]] == ["R_A", "R_B"]
    assert routes["routes"][0]["short_name"] == "A"
    assert routes["missing_route_ids"] == []
    geometry = get(client, "/routes/R_A/geometry", run_id=RUN).json()["data"]
    shape = geometry["shapes"][0]
    assert shape["path"] == [[-71.06, 42.35], [-71.05, 42.36], [-71.04, 42.355]]
    assert shape["bbox"] == [-71.06, 42.35, -71.04, 42.36]
    stops = get(client, "/routes/R_A/stops", run_id=RUN).json()["data"]
    assert stops["ordering"] == "none_use_trip_stops"
    assert [s["stop_id"] for s in stops["stops"]] == ["S1", "S2"]


def test_trip_stops_with_service_date(client: TestClient) -> None:
    plain = get(client, "/trips/T1/stops", run_id=RUN).json()["data"]
    assert [s["stop_sequence"] for s in plain["stops"]] == [1, 2, 3]
    assert plain["stops"][2]["arrival_time"] == "25:10:00"
    assert plain["stops"][0]["scheduled_arrival_utc"] is None
    dated = get(client, "/trips/T1/stops", run_id=RUN, service_date="20261005").json()["data"]
    assert dated["service_date"] == "2026-10-05"
    # America/New_York is UTC-4 on 2026-10-05: 12:00:00 local -> 16:00Z; 25:10:00 -> next day 05:10Z
    assert dated["stops"][0]["scheduled_arrival_utc"] == "2026-10-05T16:00:00.000Z"
    assert dated["stops"][2]["scheduled_arrival_utc"] == "2026-10-06T05:10:00.000Z"
    iso = get(client, "/trips/T1/stops", run_id=RUN, service_date="2026-10-05").json()["data"]
    assert iso["stops"][0]["scheduled_arrival_utc"] == "2026-10-05T16:00:00.000Z"
    assert_error(get(client, "/trips/T1/stops", run_id=RUN, service_date="20261345"), 422, "validation_error")
    assert_error(get(client, "/trips/T1/stops", run_id=RUN, service_date="Oct-5"), 422, "validation_error")
