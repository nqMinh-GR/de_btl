"""Unit tests of API building blocks: cache, freshness, request budget, row normalisation, CQL rules,
driver-error mapping and the lazy Cassandra connection (no Cassandra, no network)."""

from __future__ import annotations

import sys
import types
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient

from transitpulse.api.app import create_app
from transitpulse.api.cache import MISSING, ImmutableCache
from transitpulse.api.freshness import (
    count_vehicles,
    evaluate_vehicle,
    invalid_without_vehicle_id,
    merged_flags,
    stored_invalid_row_count,
)
from transitpulse.api.metrics import route_label
from transitpulse.api.repository import (
    CassandraRepository,
    Deadline,
    RepositoryError,
    build_cql,
    map_driver_error,
    normalize_row,
)
from transitpulse.api.settings import ApiSettings
from transitpulse.config import ProjectConfig, load_config
from transitpulse.contracts.enums import FreshnessClass
from transitpulse.contracts.policy import Policy

CLOCK = datetime(2026, 10, 5, 12, 3, 10, tzinfo=UTC)


@pytest.fixture()
def cfg(tmp_path: Any) -> ProjectConfig:
    return load_config("local-native", workdir=tmp_path, environ={})


@pytest.fixture()
def policy(cfg: ProjectConfig) -> Policy:
    return Policy.from_config(cfg)


# ================================================================================================
# Cache
# ================================================================================================
class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_cache_ttl_lru_and_counters() -> None:
    clock = FakeClock()
    hits: list[int] = []
    misses: list[int] = []
    cache = ImmutableCache(2, 30.0, on_hit=lambda: hits.append(1), on_miss=lambda: misses.append(1), clock=clock)
    assert cache.get("a") is MISSING
    assert cache.put("a", 1, 10.0)
    assert cache.get("a") == 1
    clock.now += 10.0
    assert cache.get("a") is MISSING            # expired exactly at its TTL
    assert not cache.put("b", 2, 0.0)            # generation already expired: never stored
    assert not cache.put("b", 2, -5.0)
    cache.put("x", None, 100.0)                  # TTL capped at 30 s; None is a cacheable value
    clock.now += 29.0
    assert cache.get("x") is None
    clock.now += 2.0
    assert cache.get("x") is MISSING
    cache.put("p", 1, 30.0)
    cache.put("q", 2, 30.0)
    cache.get("p")                               # p becomes most recently used
    cache.put("r", 3, 30.0)                      # evicts q
    assert cache.get("q") is MISSING
    assert cache.get("p") == 1 and cache.get("r") == 3
    assert len(cache) == 2
    assert sum(hits) >= 4 and sum(misses) >= 4


# ================================================================================================
# Freshness
# ================================================================================================
def row(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "vehicle_id": "V1", "position_valid": True, "invalid_reason": None, "freshness_known": True,
        "event_time_origin": "vehicle_timestamp", "record_kind": "observation",
        "observation_time": CLOCK - timedelta(seconds=30), "is_inherited": False,
    }
    base.update(over)
    return base


def test_vehicle_freshness_classes(policy: Policy) -> None:
    fresh = evaluate_vehicle(row(), CLOCK, policy)
    assert fresh.status == FreshnessClass.FRESH and fresh.age_seconds == 30.0
    edge = evaluate_vehicle(row(observation_time=CLOCK - timedelta(seconds=120)), CLOCK, policy)
    assert edge.status == FreshnessClass.FRESH
    stale = evaluate_vehicle(row(observation_time=CLOCK - timedelta(seconds=135)), CLOCK, policy)
    assert stale.status == FreshnessClass.STALE and stale.age_seconds == 135.0
    skew = evaluate_vehicle(row(observation_time=CLOCK + timedelta(seconds=3)), CLOCK, policy)
    assert skew.status == FreshnessClass.FRESH and skew.age_seconds == 0.0 and "CLOCK_SKEW" in skew.flags
    future = evaluate_vehicle(row(observation_time=CLOCK + timedelta(seconds=6)), CLOCK, policy)
    assert future.status == FreshnessClass.INVALID and future.invalid_reason == "FUTURE_TIMESTAMP"
    sample = evaluate_vehicle(
        row(observation_time=None, freshness_known=False, event_time_origin="feed_header",
            record_kind="snapshot_state_sample"),
        CLOCK, policy,
    )
    assert sample.status == FreshnessClass.UNKNOWN and sample.age_seconds is None
    header_only = evaluate_vehicle(row(event_time_origin="feed_header"), CLOCK, policy)
    assert header_only.status == FreshnessClass.UNKNOWN   # the header is never a fake observation time
    bad_position = evaluate_vehicle(row(position_valid=False), CLOCK, policy)
    assert bad_position.status == FreshnessClass.INVALID and bad_position.invalid_reason == "INVALID_COORDINATES"
    unknown_validity = evaluate_vehicle(row(position_valid=None), CLOCK, policy)
    assert unknown_validity.status == FreshnessClass.INVALID
    frozen = evaluate_vehicle(row(invalid_reason="INVALID_COORDINATES"), CLOCK, policy)
    assert frozen.status == FreshnessClass.INVALID and frozen.invalid_reason == "INVALID_COORDINATES"


def test_vehicle_counts_units(policy: Policy) -> None:
    rows = [
        row(vehicle_id="V1"),
        row(vehicle_id="V2", observation_time=CLOCK - timedelta(seconds=500), is_inherited=True),
        row(vehicle_id="V3", invalid_reason="INVALID_COORDINATES"),
        row(vehicle_id="V4", observation_time=None, freshness_known=False),
    ]
    counts = count_vehicles((r, evaluate_vehicle(r, CLOCK, policy)) for r in rows)
    assert counts.as_dict() == {
        "fresh_vehicle_count": 1, "stale_vehicle_count": 1, "unknown_age_vehicle_count": 1,
        "invalid_vehicle_count": 1, "invalid_without_vehicle_id_count": 0, "snapshot_membership_count": 4,
        "inherited_vehicle_count": 1, "clock_skew_vehicle_count": 0,
    }
    # stored route metrics counted 4 invalid entities; 1 has a row (V3) -> 3 entities had no vehicle id
    rowless = invalid_without_vehicle_id(4, rows)
    assert rowless == 3
    with_rowless = count_vehicles(((r, evaluate_vehicle(r, CLOCK, policy)) for r in rows), invalid_without_id=rowless)
    assert with_rowless.invalid_vehicle_count == 4
    assert with_rowless.invalid_without_vehicle_id_count == 3
    assert with_rowless.snapshot_membership_count == 4
    assert invalid_without_vehicle_id(None, rows) == 0          # no stored metrics row -> nothing added
    assert invalid_without_vehicle_id(0, rows) == 0             # never negative
    assert stored_invalid_row_count([row(position_valid=None), row()]) == 1


def test_merged_flags_drop_stored_time_relative_flags() -> None:
    assert merged_flags(["CLOCK_SKEW", "OBSERVATION_TIME_REGRESSION"], ()) == ["OBSERVATION_TIME_REGRESSION"]
    assert merged_flags(["FUTURE_TIMESTAMP", "INVALID_COORDINATES"], ("CLOCK_SKEW",)) == [
        "CLOCK_SKEW", "FUTURE_TIMESTAMP", "INVALID_COORDINATES"
    ]
    assert merged_flags(None, ()) == []


def test_route_label_includes_base_path() -> None:
    relative = types.SimpleNamespace(path="/vehicles")
    absolute = types.SimpleNamespace(path="/api/v1/vehicles")
    assert route_label(relative, "/api/v1") == "/api/v1/vehicles"
    assert route_label(absolute, "/api/v1") == "/api/v1/vehicles"
    assert route_label(relative, "") == "/vehicles"
    assert route_label(None, "/api/v1") == "unmatched"


# ================================================================================================
# Repository helpers
# ================================================================================================
def test_deadline_caps_statement_timeout_and_expires() -> None:
    generous = Deadline.start(10.0, 2.0)
    assert generous.statement_timeout() == pytest.approx(2.0, abs=0.01)
    nearly_spent = Deadline.start(0.5, 2.0)
    assert nearly_spent.statement_timeout() <= 0.5
    spent = Deadline.start(0.0, 2.0)
    with pytest.raises(RepositoryError) as info:
        spent.statement_timeout()
    assert info.value.code == "database_timeout"


def test_normalize_row_converts_driver_values() -> None:
    naive = datetime(2026, 10, 5, 12, 0, 30)
    out = normalize_row({
        "t": naive, "s": {"b", "a"}, "m": {"k": 1}, "l": ("x", "y"), "d": date(2026, 10, 5), "n": None,
    })
    assert out["t"] == datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC) and out["t"].tzinfo is not None
    assert out["s"] == ["a", "b"]
    assert out["m"] == {"k": 1}
    assert out["l"] == ["x", "y"]
    assert out["d"] == date(2026, 10, 5)
    assert out["n"] is None


def test_cql_is_bounded_and_never_filters(cfg: ProjectConfig) -> None:
    statements = build_cql(cfg.cassandra.keyspace)
    point_reads = {"run_metadata", "pointer", "manifest", "route_stop", "routes", "system_local", "schema_probe",
                   "migration"}
    for name, cql in statements.items():
        assert "ALLOW FILTERING" not in cql.upper(), name
        assert "SELECT *" not in cql, name
        if name not in point_reads:
            assert cql.rstrip().endswith("LIMIT ?"), name
        if name != "system_local":
            assert f"{cfg.cassandra.keyspace}." in cql, name
    assert "ORDER BY event_time ASC, observation_id DESC" in statements["history_range"]
    with pytest.raises(ValueError):
        build_cql("bad;keyspace")


def test_map_driver_error_classifies_driver_exceptions() -> None:
    assert map_driver_error(ValueError("not a driver error")) is None
    cassandra = pytest.importorskip("cassandra")
    from cassandra.cluster import NoHostAvailable

    unavailable = map_driver_error(NoHostAvailable("no hosts", {}))
    assert unavailable is not None and unavailable.code == "database_unavailable"
    timeout = map_driver_error(cassandra.OperationTimedOut())
    assert timeout is not None and timeout.code == "database_timeout"
    schema = map_driver_error(cassandra.InvalidRequest("unconfigured table run_metadata"))
    assert schema is not None and schema.code == "schema_not_ready"
    assert map_driver_error(cassandra.InvalidRequest("line 1:7 no viable alternative")) is None


# ================================================================================================
# Lazy connection: the app works while Cassandra is down
# ================================================================================================
def _failing_storage_module(calls: list[int]) -> types.ModuleType:
    module = types.ModuleType("transitpulse.storage.cassandra")

    def get_session(cfg: Any, *, keyspace: bool = True) -> Any:
        calls.append(1)
        raise ConnectionRefusedError("connection refused 127.0.0.1:9042")

    def prepare(session: Any, cql: str) -> Any:  # pragma: no cover - never reached without a session
        raise AssertionError("prepare called without a session")

    def shutdown() -> None:
        return None

    module.get_session = get_session  # type: ignore[attr-defined]
    module.prepare = prepare  # type: ignore[attr-defined]
    module.shutdown = shutdown  # type: ignore[attr-defined]
    return module


def test_cassandra_repository_reconnect_is_throttled(cfg: ProjectConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []
    monkeypatch.setitem(sys.modules, "transitpulse.storage.cassandra", _failing_storage_module(calls))
    repo = CassandraRepository(cfg, reconnect_interval_seconds=60.0)
    with pytest.raises(RepositoryError) as first:
        repo.run_metadata("r1")
    assert first.value.code == "database_unavailable"
    with pytest.raises(RepositoryError):
        repo.feed_pointer("r1", "vehicle_positions")
    assert len(calls) == 1                       # second call inside the interval does not reconnect
    repo.warmup()                                # never raises


def test_app_starts_and_reports_not_ready_when_cassandra_is_down(
    cfg: ProjectConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int] = []
    monkeypatch.setitem(sys.modules, "transitpulse.storage.cassandra", _failing_storage_module(calls))
    settings = ApiSettings.from_config(cfg, token_key=b"k" * 32, configure_logging=False)
    with TestClient(create_app(settings)) as client:
        live = client.get("/api/v1/health/live")
        assert live.status_code == 200
        ready = client.get("/api/v1/health/ready")
        assert ready.status_code == 503
        assert ready.json()["error"]["code"] == "database_unavailable"
        assert ready.json()["error"]["retryable"] is True
        data = client.get("/api/v1/vehicles", params={"run_id": "r1", "route_id": "R_A"})
        assert data.status_code == 503
        assert data.json()["error"]["code"] == "database_unavailable"
        metrics = client.get("/api/v1/metrics").text
        assert 'tp_api_db_errors_total{kind="database_unavailable"}' in metrics
