"""In-memory :class:`~transitpulse.api.repository.ServingRepository` (unit tests, offline UI work).

It stores rows per table keyed by the CQL primary key of ``0001_initial.cql`` and answers the same
bounded queries as :class:`~transitpulse.api.repository.CassandraRepository` with identical ordering
(clustering order, including the reversed history slice) and ``LIMIT`` semantics. Rows are copied on
the way in and out, so callers can never mutate stored state.

Test helpers:
* :meth:`InMemoryRepository.put` upserts one row (missing columns stay ``None``);
* :meth:`InMemoryRepository.publish_generation` writes generation rows, a READY manifest whose
  ``expected_counts`` are derived exactly like the processor does (``"<table>"`` totals and
  ``"<table>/<key>"`` per :data:`~transitpulse.processing.context.PER_PARTITION_COUNT_KEYS`), and the
  feed pointer;
* :attr:`InMemoryRepository.failure` makes every call raise (simulates Cassandra down);
* :attr:`InMemoryRepository.reads` records ``(table, partition key)`` of every read.
"""

from __future__ import annotations

import copy
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from typing import Any

from transitpulse.api.repository import Deadline, RepositoryError
from transitpulse.processing.context import PER_PARTITION_COUNT_KEYS

ASC, DESC = False, True

# table -> (partition key columns, ((clustering column, descending), ...))
TABLE_KEYS: dict[str, tuple[tuple[str, ...], tuple[tuple[str, bool], ...]]] = {
    "run_metadata": (("run_id",), ()),
    "runs_by_project": (("project_id",), (("created_at", DESC), ("run_id", ASC))),
    "feed_current": (("run_id", "feed_type"), ()),
    "snapshot_manifest": (("run_id", "feed_type"), (("snapshot_id", ASC),)),
    "routes_by_version": (("gtfs_version",), (("route_id", ASC),)),
    "stops_by_version": (("gtfs_version",), (("stop_id", ASC),)),
    "stops_by_route": (("gtfs_version", "route_id"), (("stop_id", ASC),)),
    "stop_times_by_trip": (("gtfs_version", "trip_id"), (("stop_sequence", ASC),)),
    "geometries_by_route": (("gtfs_version", "route_id"), (("shape_id", ASC),)),
    "vehicles_by_route_snapshot": (("run_id", "snapshot_id", "route_id"), (("vehicle_id", ASC),)),
    "predictions_by_stop_snapshot": (
        ("run_id", "snapshot_id", "stop_id"),
        (("predicted_at", ASC), ("prediction_id", ASC)),
    ),
    "route_metrics_by_snapshot": (("run_id", "feed_type", "snapshot_id"), (("route_id", ASC),)),
    "alerts_by_route_snapshot": (("run_id", "snapshot_id", "route_id"), (("alert_id", ASC),)),
    "vehicle_observations_by_day": (
        ("run_id", "vehicle_id", "utc_date"),
        (("event_time", DESC), ("observation_id", ASC)),
    ),
    "quality_by_snapshot": (("run_id", "feed_type", "snapshot_id"), (("reason_code", ASC),)),
    "pipeline_status": (("run_id",), (("component", ASC),)),
    "schema_migrations": (("migration_id",), ()),
}

# Generation tables whose partition key contains feed_type (filled in by publish_generation).
_FEED_KEYED_TABLES = frozenset({"route_metrics_by_snapshot", "quality_by_snapshot"})


def _sort_rows(rows: list[dict[str, Any]], clustering: Sequence[tuple[str, bool]]) -> list[dict[str, Any]]:
    """Order rows by clustering columns (stable multi-pass sort, last key first)."""
    out = list(rows)
    for column, descending in reversed(clustering):
        out.sort(key=lambda r, c=column: r[c], reverse=descending)
    return out


class InMemoryRepository:
    """Dictionary-backed serving repository with Cassandra-equivalent query semantics."""

    def __init__(self) -> None:
        self._tables: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = {name: {} for name in TABLE_KEYS}
        self.failure: RepositoryError | None = None
        self.reads: list[tuple[str, tuple[Any, ...]]] = []
        self.release_version: str | None = "5.0.9"
        self.schema_present: bool = True

    # ================================================================== writes (test helpers)
    def put(self, table: str, row: Mapping[str, Any]) -> dict[str, Any]:
        """Upsert one row (Cassandra semantics: provided columns overwrite, others are kept)."""
        partition, clustering = TABLE_KEYS[table]
        key_columns = partition + tuple(c for c, _ in clustering)
        missing = [c for c in key_columns if row.get(c) is None]
        if missing:
            raise ValueError(f"{table}: primary-key columns missing: {missing}")
        key = tuple(row[c] for c in key_columns)
        stored = self._tables[table].setdefault(key, {})
        stored.update(copy.deepcopy(dict(row)))
        return copy.deepcopy(stored)

    def delete(self, table: str, **key: Any) -> int:
        """Delete every row whose columns match ``key`` (simulates TTL expiry of individual rows)."""
        doomed = [k for k, row in self._tables[table].items() if all(row.get(c) == v for c, v in key.items())]
        for k in doomed:
            del self._tables[table][k]
        return len(doomed)

    def rows(self, table: str) -> list[dict[str, Any]]:
        """Every stored row of ``table`` (copies)."""
        return [copy.deepcopy(r) for r in self._tables[table].values()]

    def publish_generation(
        self,
        *,
        run_id: str,
        feed_type: str,
        snapshot_id: str,
        feed_timestamp: datetime,
        metrics_as_of: datetime,
        expires_at: datetime,
        tables: Mapping[str, Iterable[Mapping[str, Any]]] | None = None,
        is_partial: bool = False,
        is_empty: bool = False,
        status: str = "READY",
        expected_counts: Mapping[str, int] | None = None,
        view_published_at: datetime | None = None,
        update_pointer: bool = True,
        manifest_extra: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Write a generation like the processor: rows, manifest (``status``), then the pointer.

        ``expected_counts`` overrides the derived counts (to simulate lost rows / inconsistency).
        """
        derived: dict[str, int] = {}
        for table, table_rows in (tables or {}).items():
            n = 0
            for raw in table_rows:
                row = dict(raw)
                row.setdefault("run_id", run_id)
                if table != "vehicle_observations_by_day":
                    row.setdefault("snapshot_id", snapshot_id)
                if table in _FEED_KEYED_TABLES:
                    row.setdefault("feed_type", feed_type)
                self.put(table, row)
                n += 1
                part_col = PER_PARTITION_COUNT_KEYS.get(table)
                if part_col is not None:
                    pkey = f"{table}/{row[part_col]}"
                    derived[pkey] = derived.get(pkey, 0) + 1
            derived[table] = n
        counts = dict(expected_counts) if expected_counts is not None else derived
        published = view_published_at if view_published_at is not None else metrics_as_of
        manifest = {
            "run_id": run_id,
            "feed_type": feed_type,
            "snapshot_id": snapshot_id,
            "generation_id": f"gn_{snapshot_id}",
            "status": status,
            "source_order": f"{int(feed_timestamp.timestamp()):012d}.{snapshot_id}",
            "feed_timestamp": feed_timestamp,
            "metrics_as_of": metrics_as_of,
            "expires_at": expires_at,
            "is_partial": is_partial,
            "is_empty": is_empty,
            "ready_at": published if status == "READY" else None,
            "expected_counts": counts if status == "READY" else None,
            "publish_outcome": "PUBLISHED" if update_pointer else None,
        }
        if manifest_extra:
            manifest.update(manifest_extra)
        self.put("snapshot_manifest", manifest)
        if update_pointer:
            self.put(
                "feed_current",
                {
                    "run_id": run_id,
                    "feed_type": feed_type,
                    "snapshot_id": snapshot_id,
                    "generation_id": manifest["generation_id"],
                    "source_order": manifest["source_order"],
                    "feed_timestamp": feed_timestamp,
                    "metrics_as_of": metrics_as_of,
                    "view_published_at": published,
                    "expires_at": expires_at,
                    "is_partial": is_partial,
                    "is_empty": is_empty,
                },
            )
        return copy.deepcopy(manifest)

    # ================================================================== read helpers
    def _check(self) -> None:
        if self.failure is not None:
            raise self.failure

    def _partition(self, table: str, *values: Any) -> list[dict[str, Any]]:
        self._check()
        partition, clustering = TABLE_KEYS[table]
        self.reads.append((table, tuple(values)))
        rows = [
            r for r in self._tables[table].values() if all(r.get(c) == v for c, v in zip(partition, values, strict=True))
        ]
        return [copy.deepcopy(r) for r in _sort_rows(rows, clustering)]

    def read_count(self, table: str) -> int:
        """Number of partition reads of ``table`` recorded since the last :meth:`reset_reads`."""
        return sum(1 for t, _ in self.reads if t == table)

    def reset_reads(self) -> None:
        self.reads.clear()

    # ================================================================== protocol
    def warmup(self) -> None:
        return None

    def close(self) -> None:
        return None

    def ping(self, *, deadline: Deadline | None = None) -> dict[str, Any]:
        self._check()
        recorded = [k[0] for k in self._tables["schema_migrations"]] if self.schema_present else []
        return {
            "cassandra": "ok",
            "release_version": self.release_version,
            "schema_tables": "ok" if self.schema_present else "missing",
            "schema_version": sorted(recorded)[-1] if recorded else None,
            "migrations_recorded": sorted(recorded),
        }

    def run_metadata(self, run_id: str, *, deadline: Deadline | None = None) -> dict[str, Any] | None:
        rows = self._partition("run_metadata", run_id)
        return rows[0] if rows else None

    def run_metadata_many(
        self, run_ids: Sequence[str], *, deadline: Deadline | None = None
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for run_id in run_ids:
            row = self.run_metadata(run_id)
            if row is not None:
                out[run_id] = row
        return out

    def runs_page(
        self,
        project_id: str,
        *,
        limit: int,
        after: tuple[datetime, str] | None,
        deadline: Deadline | None = None,
    ) -> list[dict[str, Any]]:
        rows = self._partition("runs_by_project", project_id)
        if after is not None:
            created_at, run_id = after
            rows = [
                r
                for r in rows
                if r["created_at"] < created_at or (r["created_at"] == created_at and r["run_id"] > run_id)
            ]
        return rows[:limit]

    def feed_pointer(
        self, run_id: str, feed_type: str, *, deadline: Deadline | None = None
    ) -> dict[str, Any] | None:
        rows = self._partition("feed_current", run_id, feed_type)
        return rows[0] if rows else None

    def manifest(
        self, run_id: str, feed_type: str, snapshot_id: str, *, deadline: Deadline | None = None
    ) -> dict[str, Any] | None:
        rows = [r for r in self._partition("snapshot_manifest", run_id, feed_type) if r["snapshot_id"] == snapshot_id]
        return rows[0] if rows else None

    def pipeline_status(
        self, run_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._partition("pipeline_status", run_id)[:limit]

    def routes(
        self, gtfs_version: str, route_ids: Sequence[str], *, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        wanted = set(route_ids)
        return [r for r in self._partition("routes_by_version", gtfs_version) if r["route_id"] in wanted]

    def geometries(
        self, gtfs_version: str, route_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._partition("geometries_by_route", gtfs_version, route_id)[:limit]

    def route_stops(
        self, gtfs_version: str, route_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._partition("stops_by_route", gtfs_version, route_id)[:limit]

    def stop_routes(
        self, gtfs_version: str, route_ids: Sequence[str], stop_id: str, *, deadline: Deadline | None = None
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for route_id in route_ids:
            rows = [r for r in self._partition("stops_by_route", gtfs_version, route_id) if r["stop_id"] == stop_id]
            if rows:
                out[route_id] = rows[0]
        return out

    def trip_stop_times(
        self, gtfs_version: str, trip_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._partition("stop_times_by_trip", gtfs_version, trip_id)[:limit]

    def vehicles(
        self,
        run_id: str,
        snapshot_id: str,
        route_ids: Sequence[str],
        *,
        limit_per_route: int,
        deadline: Deadline | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        return {
            route_id: self._partition("vehicles_by_route_snapshot", run_id, snapshot_id, route_id)[:limit_per_route]
            for route_id in route_ids
        }

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
        rows = self._partition("vehicle_observations_by_day", run_id, vehicle_id, utc_date)
        # API page order (event_time ASC, observation_id DESC) == reverse of the clustering order.
        rows = list(reversed(rows))
        if after is None:
            rows = [r for r in rows if start <= r["event_time"] < end]
        else:
            event_time, observation_id = after
            rows = [
                r
                for r in rows
                if r["event_time"] < end
                and (
                    r["event_time"] > event_time
                    or (r["event_time"] == event_time and r["observation_id"] < observation_id)
                )
            ]
        return rows[:limit]

    def predictions(
        self, run_id: str, snapshot_id: str, stop_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._partition("predictions_by_stop_snapshot", run_id, snapshot_id, stop_id)[:limit]

    def route_metrics(
        self, run_id: str, feed_type: str, snapshot_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._partition("route_metrics_by_snapshot", run_id, feed_type, snapshot_id)[:limit]

    def alerts(
        self,
        run_id: str,
        snapshot_id: str,
        route_ids: Sequence[str],
        *,
        limit_per_route: int,
        deadline: Deadline | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        return {
            route_id: self._partition("alerts_by_route_snapshot", run_id, snapshot_id, route_id)[:limit_per_route]
            for route_id in route_ids
        }

    def quality(
        self, run_id: str, feed_type: str, snapshot_id: str, *, limit: int, deadline: Deadline | None = None
    ) -> list[dict[str, Any]]:
        return self._partition("quality_by_snapshot", run_id, feed_type, snapshot_id)[:limit]
