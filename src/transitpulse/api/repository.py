"""Serving repository: every Cassandra read the API performs (design E §4.1/§4.3, spec md §12-13).

Rules enforced here:
* every query restricts a full partition key (single-partition reads), has an explicit ``LIMIT`` (or is a
  primary-key point read) and never uses ``ALLOW FILTERING``;
* fan-out over routes/stops is bounded (``api.max_fanout_routes`` concurrent statements, at most the
  run's scope routes);
* every statement gets ``timeout = min(api.statement_timeout_seconds, remaining request budget)``
  (:class:`Deadline`, D-E18);
* driver failures are mapped to :class:`RepositoryError` (HTTP 503 ``database_unavailable`` /
  ``database_timeout`` / ``schema_not_ready``); anything else is a programming error and propagates;
* the session is created lazily through ``storage.cassandra.get_session``; when Cassandra is down the
  API still starts and reconnects at most every ``reconnect_interval_seconds``.

Rows are returned as plain dicts keyed by CQL column names, timestamps as timezone-aware UTC
``datetime``, ``date`` columns as ``datetime.date``, sets as sorted lists and maps as dicts.
"""

from __future__ import annotations

import importlib.resources
import re
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Protocol

from transitpulse.config import ProjectConfig
from transitpulse.obs.logging import get_logger, log_event

_LOG = get_logger("transitpulse.api.repository")

READINESS_TIMEOUT_SECONDS = 1.0
READINESS_PROBE_RUN_ID = "__transitpulse_readiness_probe__"
MIN_STATEMENT_TIMEOUT_SECONDS = 0.05
RUN_METADATA_CONCURRENCY = 10
STOP_LOOKUP_CONCURRENCY = 5


# ================================================================================================
# Errors and request budget
# ================================================================================================
class RepositoryError(Exception):
    """Serving-store failure; always rendered as HTTP 503 with ``retryable=true``.

    ``code`` is one of ``database_unavailable``, ``database_timeout``, ``schema_not_ready``.
    """

    def __init__(self, code: str, message: str, *, hint: str | None = None) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.hint = hint


@dataclass
class Deadline:
    """Per-request time budget (``api.request_timeout_seconds``) shared by all statements."""

    expires_at_monotonic: float
    statement_timeout_seconds: float

    @classmethod
    def start(cls, budget_seconds: float, statement_timeout_seconds: float) -> Deadline:
        return cls(time.monotonic() + float(budget_seconds), float(statement_timeout_seconds))

    def remaining(self) -> float:
        return self.expires_at_monotonic - time.monotonic()

    def statement_timeout(self) -> float:
        """Timeout for the next statement; raises ``database_timeout`` once the budget is spent."""
        remaining = self.remaining()
        if remaining <= MIN_STATEMENT_TIMEOUT_SECONDS:
            raise RepositoryError(
                "database_timeout",
                "the request time budget is exhausted before all reads completed",
                hint="retry; narrow the request (route_id, shorter history range) if it persists",
            )
        return min(self.statement_timeout_seconds, remaining)


# ================================================================================================
# Protocol
# ================================================================================================
class ServingRepository(Protocol):
    """Read-only access used by the routers. Implementations: :class:`CassandraRepository` and
    :class:`transitpulse.api.memory_repository.InMemoryRepository`."""

    def warmup(self) -> None: ...

    def close(self) -> None: ...

    def ping(self, *, deadline: Deadline | None = None) -> dict[str, Any]: ...

    def run_metadata(self, run_id: str, *, deadline: Deadline | None = None) -> dict[str, Any] | None: ...

    def run_metadata_many(
        self, run_ids: Sequence[str], *, deadline: Deadline | None = None
    ) -> dict[str, dict[str, Any]]: ...

    def runs_page(
        self,
        project_id: str,
        *,
        limit: int,
        after: tuple[datetime, str] | None,
        deadline: Deadline | None = None,
    ) -> list[dict[str, Any]]: ...

    def feed_pointer(
        self, run_id: str, feed_type: str, *, deadline: Deadline | None = None
    ) -> dict[str, Any] | None: ...

    def manifest(
        self, run_id: str, feed_type: str, snapshot_id: str, *, deadline: Deadline | None = None
    ) -> dict[str, Any] | None: ...

    def pipeline_status(
        self, run_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]: ...

    def routes(
        self, gtfs_version: str, route_ids: Sequence[str], *, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]: ...

    def geometries(
        self, gtfs_version: str, route_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]: ...

    def route_stops(
        self, gtfs_version: str, route_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]: ...

    def stop_routes(
        self, gtfs_version: str, route_ids: Sequence[str], stop_id: str, *, deadline: Deadline | None = None
    ) -> dict[str, dict[str, Any]]: ...

    def trip_stop_times(
        self, gtfs_version: str, trip_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]: ...

    def vehicles(
        self,
        run_id: str,
        snapshot_id: str,
        route_ids: Sequence[str],
        *,
        limit_per_route: int,
        deadline: Deadline | None = None,
    ) -> dict[str, list[dict[str, Any]]]: ...

    def vehicle_history(
        self,
        run_id: str,
        vehicle_id: str,
        utc_date: date,
        *,
        start: datetime,
        end: datetime,
        after: tuple[datetime, str] | None,
        limit: int,
        deadline: Deadline | None = None,
    ) -> list[dict[str, Any]]:
        """Rows of one day bucket with ``start <= event_time < end`` ordered by
        ``(event_time ASC, observation_id DESC)``, strictly after ``after`` in that order."""
        ...

    def predictions(
        self, run_id: str, snapshot_id: str, stop_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]: ...

    def route_metrics(
        self, run_id: str, feed_type: str, snapshot_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]: ...

    def alerts(
        self,
        run_id: str,
        snapshot_id: str,
        route_ids: Sequence[str],
        *,
        limit_per_route: int,
        deadline: Deadline | None = None,
    ) -> dict[str, list[dict[str, Any]]]: ...

    def quality(
        self, run_id: str, feed_type: str, snapshot_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]: ...


# ================================================================================================
# Row normalisation
# ================================================================================================
def normalize_value(value: Any) -> Any:
    """Driver value -> API value (aware UTC datetimes, ``date``, sorted lists, plain dicts)."""
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, date):
        return value
    if type(value).__name__ == "Date" and hasattr(value, "date"):  # cassandra.util.Date
        return value.date()
    if isinstance(value, Mapping):
        return {k: normalize_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_value(v) for v in value]
    if isinstance(value, (set, frozenset)) or type(value).__name__ == "SortedSet":
        items = [normalize_value(v) for v in value]
        try:
            return sorted(items)
        except TypeError:
            return items
    return value


def normalize_row(row: Any) -> dict[str, Any]:
    """Normalise one driver row (``dict_factory`` dict, or a named tuple as a fallback)."""
    if isinstance(row, Mapping):
        items = row.items()
    elif hasattr(row, "_asdict"):
        items = row._asdict().items()
    else:  # pragma: no cover - the session always uses dict_factory
        raise TypeError(f"unsupported row type {type(row).__name__}")
    return {str(k): normalize_value(v) for k, v in items}


# ================================================================================================
# Driver error mapping
# ================================================================================================
_SCHEMA_MARKERS = ("unconfigured table", "does not exist", "undefined column", "undefined name")


def _driver_classes(module: Any, *names: str) -> tuple[type, ...]:
    out: list[type] = []
    for name in names:
        cls = getattr(module, name, None)
        if isinstance(cls, type):
            out.append(cls)
    return tuple(out)


def map_driver_error(exc: BaseException) -> RepositoryError | None:
    """Map a cassandra-driver exception to :class:`RepositoryError`; ``None`` for anything else."""
    try:
        import cassandra
    except ImportError:  # pragma: no cover - cassandra-driver is a pinned dependency
        return None
    timeouts = _driver_classes(cassandra, "OperationTimedOut", "Timeout")
    invalid = _driver_classes(cassandra, "InvalidRequest", "ConfigurationException")
    unavailable = list(_driver_classes(cassandra, "Unavailable", "CoordinationFailure", "DriverException"))
    try:
        from cassandra.cluster import NoHostAvailable

        unavailable.append(NoHostAvailable)
    except ImportError:  # pragma: no cover
        pass
    try:
        from cassandra.connection import ConnectionException

        unavailable.append(ConnectionException)
    except ImportError:  # pragma: no cover
        pass
    text = f"{type(exc).__name__}: {str(exc)[:300]}"
    if timeouts and isinstance(exc, timeouts):
        return RepositoryError(
            "database_timeout",
            f"Cassandra did not answer within the statement timeout ({text})",
            hint="retry; check Cassandra load with `scripts/infra.py status`",
        )
    if invalid and isinstance(exc, invalid):
        if any(marker in str(exc).lower() for marker in _SCHEMA_MARKERS):
            return RepositoryError(
                "schema_not_ready",
                f"the serving schema is missing or outdated ({text})",
                hint="apply the CQL migrations with `scripts/infra.py init`",
            )
        return None
    if unavailable and isinstance(exc, tuple(unavailable)):
        return RepositoryError(
            "database_unavailable",
            f"Cassandra is unavailable ({text})",
            hint="start Cassandra with `scripts/infra.py start` and check `scripts/infra.py status`",
        )
    return None


# ================================================================================================
# CQL (fully qualified, explicit columns; column names come from 0001_initial.cql)
# ================================================================================================
_COLUMNS: dict[str, tuple[str, ...]] = {
    "run_metadata": (
        "run_id", "project_id", "scope_id", "source_type", "clock_mode", "gtfs_version", "agency_timezone",
        "route_ids", "pipeline_version", "policy_version", "policy_json", "fixture_kind", "scenario_id", "seed",
        "recording_id", "session_id", "speed", "label", "provenance_json", "created_at", "run_state",
        "application_clock", "clock_sequence", "clock_updated_at", "state_reason",
    ),
    "runs_by_project": (
        "project_id", "created_at", "run_id", "source_type", "scope_id", "gtfs_version", "scenario_id", "label",
    ),
    "feed_current": (
        "run_id", "feed_type", "snapshot_id", "generation_id", "source_order", "feed_timestamp", "metrics_as_of",
        "view_published_at", "expires_at", "is_partial", "is_empty",
    ),
    "snapshot_manifest": (
        "run_id", "feed_type", "snapshot_id", "generation_id", "status", "source_order", "feed_timestamp",
        "original_fetched_at", "published_at", "capture_id", "gtfs_version", "scope_id", "scenario_id",
        "parent_snapshot_id", "built_as_late", "metrics_as_of", "clock_sequence_at_build",
        "first_materialized_at", "expires_at", "history_expires_at", "source_entity_count",
        "selected_entity_count", "fully_valid_entity_count", "partially_valid_entity_count",
        "rejected_entity_count", "is_partial", "is_empty", "ready_at", "expected_counts", "accounting_json",
        "publish_outcome", "first_visible_at", "visibility_latency_ms",
    ),
    "pipeline_status": (
        "run_id", "component", "status", "last_batch_id", "last_success_at", "last_heartbeat_at",
        "last_batch_duration_ms", "input_messages_total", "rejected_messages_total", "snapshots_built_total",
        "snapshots_published_total", "late_deliveries_total", "duplicate_deliveries_total", "last_snapshot_id",
        "last_published_snapshot_id", "last_visibility_latency_ms", "last_rejected_at", "last_rejected_reason",
        "last_rejected_error_id", "progress_json", "last_error",
    ),
    "routes_by_version": (
        "gtfs_version", "route_id", "agency_id", "route_short_name", "route_long_name", "route_type",
        "route_color", "route_text_color", "route_sort_order",
    ),
    "stops_by_route": (
        "gtfs_version", "route_id", "stop_id", "stop_name", "stop_lat", "stop_lon", "direction_ids",
        "pattern_count",
    ),
    "stop_times_by_trip": (
        "gtfs_version", "trip_id", "stop_sequence", "stop_id", "arrival_time_raw", "departure_time_raw",
        "arrival_seconds", "departure_seconds", "route_id", "direction_id", "service_id", "shape_id",
        "trip_headsign", "stop_headsign", "pickup_type", "drop_off_type", "timepoint",
    ),
    "geometries_by_route": (
        "gtfs_version", "route_id", "shape_id", "direction_ids", "trip_count", "point_count", "points_json",
        "simplified", "is_representative",
    ),
    "vehicles_by_route_snapshot": (
        "run_id", "snapshot_id", "route_id", "vehicle_id", "observation_id", "record_kind", "observation_time",
        "event_time", "event_time_origin", "freshness_known", "position_valid", "invalid_reason", "latitude",
        "longitude", "bearing", "speed_mps", "vehicle_label", "trip_id", "direction_id", "start_date",
        "start_time", "trip_schedule_relationship", "stop_id", "current_stop_sequence", "current_status",
        "occupancy_status", "feed_timestamp", "is_inherited", "inherited_from_snapshot_id", "quality_flags",
    ),
    "predictions_by_stop_snapshot": (
        "run_id", "snapshot_id", "stop_id", "predicted_at", "prediction_id", "event_type", "trip_id", "route_id",
        "direction_id", "start_date", "start_time", "service_date", "trip_instance_key", "stop_sequence",
        "trip_schedule_relationship", "stop_schedule_relationship", "vehicle_id", "scheduled_at",
        "predicted_delay_seconds", "schedule_resolution", "resolution_method", "uncertainty",
        "prediction_quality", "kpi_eligible", "is_target", "feed_timestamp", "quality_flags",
    ),
    "route_metrics_by_snapshot": (
        "run_id", "feed_type", "snapshot_id", "route_id", "metrics_as_of", "feed_timestamp", "is_partial",
        "policy_version", "snapshot_membership_count", "fresh_vehicle_count", "stale_vehicle_count",
        "unknown_age_vehicle_count", "invalid_vehicle_count", "inherited_vehicle_count",
        "stale_threshold_seconds", "trip_instance_count", "eligible_trip_count", "predicted_late_trip_count",
        "predicted_late_rate", "mean_predicted_delay_seconds", "excluded_trip_counts",
        "unknown_uncertainty_count", "late_threshold_seconds", "target_policy", "quality_flags",
    ),
    "alerts_by_route_snapshot": (
        "run_id", "snapshot_id", "route_id", "alert_id", "cause", "effect", "severity_level", "header_text",
        "description_text", "url", "translations_json", "active_periods_json", "informed_entities_json",
        "match_kinds", "selector_resolution", "active_state_at_build", "confirmation_status",
        "inherited_from_snapshot_id", "first_seen_snapshot_id", "feed_timestamp", "quality_flags",
    ),
    "vehicle_observations_by_day": (
        "run_id", "vehicle_id", "utc_date", "event_time", "observation_id", "record_kind", "event_time_origin",
        "route_id", "trip_id", "direction_id", "start_date", "latitude", "longitude", "bearing", "speed_mps",
        "stop_id", "current_stop_sequence", "current_status", "quality_flags",
    ),
    "quality_by_snapshot": (
        "run_id", "feed_type", "snapshot_id", "reason_code", "severity", "unit", "record_count",
        "disposition_counts", "sample_refs", "details_json",
    ),
}

_KEYSPACE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,47}$")

# name -> (table, WHERE/ORDER/LIMIT clause). Every clause fixes the complete partition key.
_QUERIES: dict[str, tuple[str, str]] = {
    "run_metadata": ("run_metadata", "WHERE run_id = ?"),
    "runs_first": ("runs_by_project", "WHERE project_id = ? LIMIT ?"),
    "runs_tie": ("runs_by_project", "WHERE project_id = ? AND created_at = ? AND run_id > ? LIMIT ?"),
    "runs_before": ("runs_by_project", "WHERE project_id = ? AND created_at < ? LIMIT ?"),
    "pointer": ("feed_current", "WHERE run_id = ? AND feed_type = ?"),
    "manifest": ("snapshot_manifest", "WHERE run_id = ? AND feed_type = ? AND snapshot_id = ?"),
    "pipeline_status": ("pipeline_status", "WHERE run_id = ? LIMIT ?"),
    "routes": ("routes_by_version", "WHERE gtfs_version = ? AND route_id IN ?"),
    "geometries": ("geometries_by_route", "WHERE gtfs_version = ? AND route_id = ? LIMIT ?"),
    "route_stops": ("stops_by_route", "WHERE gtfs_version = ? AND route_id = ? LIMIT ?"),
    "route_stop": ("stops_by_route", "WHERE gtfs_version = ? AND route_id = ? AND stop_id = ?"),
    "trip_stop_times": ("stop_times_by_trip", "WHERE gtfs_version = ? AND trip_id = ? LIMIT ?"),
    "vehicles": ("vehicles_by_route_snapshot", "WHERE run_id = ? AND snapshot_id = ? AND route_id = ? LIMIT ?"),
    # vehicle_observations_by_day is clustered (event_time DESC, observation_id ASC); the API pages in the
    # exact reverse order (event_time ASC, observation_id DESC), which CQL serves as a reversed slice.
    "history_range": (
        "vehicle_observations_by_day",
        "WHERE run_id = ? AND vehicle_id = ? AND utc_date = ? AND event_time >= ? AND event_time < ? "
        "ORDER BY event_time ASC, observation_id DESC LIMIT ?",
    ),
    "history_after": (
        "vehicle_observations_by_day",
        "WHERE run_id = ? AND vehicle_id = ? AND utc_date = ? AND event_time > ? AND event_time < ? "
        "ORDER BY event_time ASC, observation_id DESC LIMIT ?",
    ),
    "history_tie": (
        "vehicle_observations_by_day",
        "WHERE run_id = ? AND vehicle_id = ? AND utc_date = ? AND event_time = ? AND observation_id < ? "
        "ORDER BY event_time ASC, observation_id DESC LIMIT ?",
    ),
    "predictions": (
        "predictions_by_stop_snapshot",
        "WHERE run_id = ? AND snapshot_id = ? AND stop_id = ? LIMIT ?",
    ),
    "route_metrics": (
        "route_metrics_by_snapshot",
        "WHERE run_id = ? AND feed_type = ? AND snapshot_id = ? LIMIT ?",
    ),
    "alerts": ("alerts_by_route_snapshot", "WHERE run_id = ? AND snapshot_id = ? AND route_id = ? LIMIT ?"),
    "quality": ("quality_by_snapshot", "WHERE run_id = ? AND feed_type = ? AND snapshot_id = ? LIMIT ?"),
}


def build_cql(keyspace: str) -> dict[str, str]:
    """All statements of the API, keyed by name (exposed for review and tests)."""
    if not _KEYSPACE_RE.match(keyspace):
        raise ValueError(f"invalid keyspace name {keyspace!r}")
    out = {
        name: f"SELECT {', '.join(_COLUMNS[table])} FROM {keyspace}.{table} {clause}"
        for name, (table, clause) in _QUERIES.items()
    }
    out["system_local"] = "SELECT release_version FROM system.local WHERE key = 'local'"
    out["schema_probe"] = f"SELECT run_id FROM {keyspace}.run_metadata WHERE run_id = ?"
    out["migration"] = (
        f"SELECT migration_id, checksum_sha256, applied_at FROM {keyspace}.schema_migrations WHERE migration_id = ?"
    )
    return out


def known_migration_ids() -> tuple[str, ...]:
    """Migration ids shipped with the package (file stems of ``storage/migrations/*.cql``)."""
    try:
        root = importlib.resources.files("transitpulse.storage").joinpath("migrations")
        names = sorted(entry.name for entry in root.iterdir() if entry.name.endswith(".cql"))
    except (ModuleNotFoundError, FileNotFoundError, NotADirectoryError, OSError):
        return ()
    return tuple(name[: -len(".cql")] for name in names)


# ================================================================================================
# Cassandra implementation
# ================================================================================================
class CassandraRepository:
    """:class:`ServingRepository` backed by the process-cached session of ``storage.cassandra``."""

    def __init__(self, cfg: ProjectConfig, *, reconnect_interval_seconds: float = 2.0) -> None:
        self._cfg = cfg
        self._cql = build_cql(cfg.cassandra.keyspace)
        self._statement_timeout = float(cfg.api.statement_timeout_seconds)
        self._fanout = max(1, int(cfg.api.max_fanout_routes))
        self._reconnect_interval = float(reconnect_interval_seconds)
        self._lock = threading.Lock()
        self._session: Any | None = None
        self._next_attempt = 0.0
        self._last_error: str | None = None

    # ------------------------------------------------------------------ lifecycle
    def warmup(self) -> None:
        """Try to connect once (called from a background thread at startup); never raises."""
        try:
            self._session_or_raise()
        except RepositoryError as exc:
            log_event(_LOG, "cassandra_connect", "failed", exc.message, reason=exc.code)

    def close(self) -> None:
        with self._lock:
            had_session = self._session is not None
            self._session = None
        if not had_session:
            return
        try:
            from transitpulse.storage.cassandra import shutdown

            shutdown()
        except Exception as exc:  # shutdown is best effort at process exit
            log_event(_LOG, "cassandra_shutdown", "failed", f"{type(exc).__name__}: {exc}")

    def _session_or_raise(self) -> Any:
        session = self._session
        if session is not None:
            return session
        with self._lock:
            if self._session is not None:
                return self._session
            if time.monotonic() < self._next_attempt:
                raise RepositoryError(
                    "database_unavailable",
                    f"Cassandra is unavailable ({self._last_error}); reconnect is attempted at most every "
                    f"{self._reconnect_interval:g} s",
                    hint="start Cassandra with `scripts/infra.py start` and check `scripts/infra.py status`",
                )
            try:
                from transitpulse.storage.cassandra import get_session

                session = get_session(self._cfg, keyspace=False)
            except Exception as exc:  # any connect failure means "unavailable"; retried later
                self._next_attempt = time.monotonic() + self._reconnect_interval
                self._last_error = f"{type(exc).__name__}: {str(exc)[:300]}"
                log_event(_LOG, "cassandra_connect", "failed", self._last_error, reason="database_unavailable")
                raise RepositoryError(
                    "database_unavailable",
                    f"cannot connect to Cassandra ({self._last_error})",
                    hint="start Cassandra with `scripts/infra.py start` and check `scripts/infra.py status`",
                ) from exc
            self._session = session
            self._last_error = None
            log_event(_LOG, "cassandra_connect", "ok", "connected to Cassandra")
            return session

    # ------------------------------------------------------------------ execution helpers
    def _timeout(self, deadline: Deadline | None, cap: float | None = None) -> float:
        timeout = deadline.statement_timeout() if deadline is not None else self._statement_timeout
        return min(timeout, cap) if cap is not None else timeout

    def _rows(
        self, name: str, params: Sequence[Any], deadline: Deadline | None, *, cap: float | None = None
    ) -> list[dict[str, Any]]:
        session = self._session_or_raise()
        timeout = self._timeout(deadline, cap)
        try:
            from transitpulse.storage.cassandra import prepare

            statement = prepare(session, self._cql[name])
            result = session.execute(statement, tuple(params), timeout=timeout)
            return [normalize_row(row) for row in result]
        except RepositoryError:
            raise
        except Exception as exc:
            mapped = map_driver_error(exc)
            if mapped is None:
                raise
            raise mapped from exc

    def _rows_many(
        self, name: str, param_list: Sequence[Sequence[Any]], deadline: Deadline | None, *, concurrency: int
    ) -> list[list[dict[str, Any]]]:
        """Run one prepared statement for several parameter tuples, ``concurrency`` at a time."""
        if not param_list:
            return []
        session = self._session_or_raise()
        out: list[list[dict[str, Any]]] = []
        try:
            from transitpulse.storage.cassandra import prepare

            statement = prepare(session, self._cql[name])
            step = max(1, concurrency)
            for offset in range(0, len(param_list), step):
                timeout = self._timeout(deadline)
                futures = [
                    session.execute_async(statement, tuple(params), timeout=timeout)
                    for params in param_list[offset : offset + step]
                ]
                for future in futures:
                    out.append([normalize_row(row) for row in future.result()])
        except RepositoryError:
            raise
        except Exception as exc:
            mapped = map_driver_error(exc)
            if mapped is None:
                raise
            raise mapped from exc
        return out

    @staticmethod
    def _first(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        return rows[0] if rows else None

    # ------------------------------------------------------------------ health
    def ping(self, *, deadline: Deadline | None = None) -> dict[str, Any]:
        local = self._rows("system_local", (), deadline, cap=READINESS_TIMEOUT_SECONDS)
        release = local[0].get("release_version") if local else None
        result: dict[str, Any] = {
            "cassandra": "ok",
            "release_version": release,
            "schema_tables": "missing",
            "schema_version": None,
            "migrations_recorded": [],
        }
        try:
            self._rows("schema_probe", (READINESS_PROBE_RUN_ID,), deadline, cap=READINESS_TIMEOUT_SECONDS)
        except RepositoryError as exc:
            if exc.code != "schema_not_ready":
                raise
            return result
        result["schema_tables"] = "ok"
        recorded: list[str] = []
        for migration_id in known_migration_ids():
            try:
                rows = self._rows("migration", (migration_id,), deadline, cap=READINESS_TIMEOUT_SECONDS)
            except RepositoryError as exc:
                if exc.code != "schema_not_ready":
                    raise
                break
            if rows:
                recorded.append(migration_id)
        result["migrations_recorded"] = recorded
        result["schema_version"] = recorded[-1] if recorded else None
        return result

    # ------------------------------------------------------------------ runs and publish state
    def run_metadata(self, run_id: str, *, deadline: Deadline | None = None) -> dict[str, Any] | None:
        return self._first(self._rows("run_metadata", (run_id,), deadline))

    def run_metadata_many(
        self, run_ids: Sequence[str], *, deadline: Deadline | None = None
    ) -> dict[str, dict[str, Any]]:
        results = self._rows_many(
            "run_metadata", [(rid,) for rid in run_ids], deadline, concurrency=RUN_METADATA_CONCURRENCY
        )
        return {rid: rows[0] for rid, rows in zip(run_ids, results, strict=True) if rows}

    def runs_page(
        self,
        project_id: str,
        *,
        limit: int,
        after: tuple[datetime, str] | None,
        deadline: Deadline | None = None,
    ) -> list[dict[str, Any]]:
        if after is None:
            return self._rows("runs_first", (project_id, limit), deadline)
        created_at, run_id = after
        ties = self._rows("runs_tie", (project_id, created_at, run_id, limit), deadline)
        if len(ties) >= limit:
            return ties[:limit]
        return ties + self._rows("runs_before", (project_id, created_at, limit - len(ties)), deadline)

    def feed_pointer(
        self, run_id: str, feed_type: str, *, deadline: Deadline | None = None
    ) -> dict[str, Any] | None:
        return self._first(self._rows("pointer", (run_id, feed_type), deadline))

    def manifest(
        self, run_id: str, feed_type: str, snapshot_id: str, *, deadline: Deadline | None = None
    ) -> dict[str, Any] | None:
        return self._first(self._rows("manifest", (run_id, feed_type, snapshot_id), deadline))

    def pipeline_status(
        self, run_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._rows("pipeline_status", (run_id, limit), deadline)

    # ------------------------------------------------------------------ static catalogue
    def routes(
        self, gtfs_version: str, route_ids: Sequence[str], *, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        if not route_ids:
            return []
        return self._rows("routes", (gtfs_version, list(route_ids)), deadline)

    def geometries(
        self, gtfs_version: str, route_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._rows("geometries", (gtfs_version, route_id, limit), deadline)

    def route_stops(
        self, gtfs_version: str, route_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._rows("route_stops", (gtfs_version, route_id, limit), deadline)

    def stop_routes(
        self, gtfs_version: str, route_ids: Sequence[str], stop_id: str, *, deadline: Deadline | None = None
    ) -> dict[str, dict[str, Any]]:
        results = self._rows_many(
            "route_stop",
            [(gtfs_version, route_id, stop_id) for route_id in route_ids],
            deadline,
            concurrency=STOP_LOOKUP_CONCURRENCY,
        )
        return {route_id: rows[0] for route_id, rows in zip(route_ids, results, strict=True) if rows}

    def trip_stop_times(
        self, gtfs_version: str, trip_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._rows("trip_stop_times", (gtfs_version, trip_id, limit), deadline)

    # ------------------------------------------------------------------ serving generations
    def vehicles(
        self,
        run_id: str,
        snapshot_id: str,
        route_ids: Sequence[str],
        *,
        limit_per_route: int,
        deadline: Deadline | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        results = self._rows_many(
            "vehicles",
            [(run_id, snapshot_id, route_id, limit_per_route) for route_id in route_ids],
            deadline,
            concurrency=self._fanout,
        )
        return dict(zip(route_ids, results, strict=True))

    def vehicle_history(
        self,
        run_id: str,
        vehicle_id: str,
        utc_date: date,
        *,
        start: datetime,
        end: datetime,
        after: tuple[datetime, str] | None,
        limit: int,
        deadline: Deadline | None = None,
    ) -> list[dict[str, Any]]:
        if after is None:
            return self._rows("history_range", (run_id, vehicle_id, utc_date, start, end, limit), deadline)
        event_time, observation_id = after
        ties = self._rows("history_tie", (run_id, vehicle_id, utc_date, event_time, observation_id, limit), deadline)
        if len(ties) >= limit:
            return ties[:limit]
        rest = self._rows(
            "history_after", (run_id, vehicle_id, utc_date, event_time, end, limit - len(ties)), deadline
        )
        return ties + rest

    def predictions(
        self, run_id: str, snapshot_id: str, stop_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._rows("predictions", (run_id, snapshot_id, stop_id, limit), deadline)

    def route_metrics(
        self, run_id: str, feed_type: str, snapshot_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._rows("route_metrics", (run_id, feed_type, snapshot_id, limit), deadline)

    def alerts(
        self,
        run_id: str,
        snapshot_id: str,
        route_ids: Sequence[str],
        *,
        limit_per_route: int,
        deadline: Deadline | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        results = self._rows_many(
            "alerts",
            [(run_id, snapshot_id, route_id, limit_per_route) for route_id in route_ids],
            deadline,
            concurrency=self._fanout,
        )
        return dict(zip(route_ids, results, strict=True))

    def quality(
        self, run_id: str, feed_type: str, snapshot_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._rows("quality", (run_id, feed_type, snapshot_id, limit), deadline)
