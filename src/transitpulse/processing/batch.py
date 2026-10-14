"""Micro-batch orchestration: ``process_batch(df, batch_id)`` (WP4; design B §4-§6, interfaces §7).

Order inside one ``foreachBatch`` call (every failure raises; nothing is swallowed):

  0. Kafka columns + exact run filter, persisted (every action would otherwise re-read Kafka).
  A. Control first: fold clock events, ONE run-clock LWT, collector/publisher status upserts; then read
     the run at SERIAL (:class:`RunInfo`).
  B. Message-level rejects (DLQ rows + per-feed counts).
  C. Snapshot metadata: canonical deliveries (``delivery_rank = 1``) collected with a HARD bound
     (``spark.max_snapshots_per_batch``; exceeding it raises :class:`BacklogBoundExceeded`, no
     ``maxOffsetsPerTrigger`` is used, D-PUB-04); in-batch duplicates aggregated per snapshot.
  D. Manifests, per feed in ``source_order``: a NEW snapshot gets a BUILDING manifest via
     ``INSERT IF NOT EXISTS`` with FROZEN parameters, parent = max(pointer, previous non-late in-batch
     snapshot of the same feed) (D-PUB-02; vehicles and alerts only; late builds have no parent) and its
     availability (``parent_available``: parent generation not expired) frozen with it,
     ``metrics_as_of`` = run clock (source) or wall now (live), ``expires_at``/``history_expires_at`` from
     the run-pinned policy. An existing manifest is reused as is (its frozen parameters win).
  E. Per snapshot (feed order, then source order):
       BUILDING -> parent rows (bounded reads) -> WP5 builder on a ONE-ROW ``snap`` + frozen BuildParams
       -> stats/accounting check -> curated unit staged -> tables conformed (CQL types, timestamps to
       epoch-ms) -> expected counts + checksum (one small aggregation) -> sink writes in two groups
       (fault point ``after_partial_write`` between them) -> written == expected -> READY
       -> fault point ``after_ready_before_cas``;
       READY -> reused (no rewrite);
       then the pointer CAS loop (D-PUB-03) and the one-shot publish outcome.
  F. Batch-level curated unit (deliveries, message DLQ, publication outcomes), DLQ to Kafka from the
     staged files, curated commit (``_COMMITTED``).
  G. ``pipeline_status`` rows ``processor`` and ``feed:<ft>`` with batch-id-guarded LWTs, metrics, and
     fault point ``after_cas_before_commit`` (just before ``foreachBatch`` returns, i.e. before the
     Spark checkpoint commit).

At-least-once delivery + idempotent writes/publish (D-PUB-05): replaying an uncommitted batch reuses the
frozen manifests, rewrites the same primary keys, skips READY generations and never moves the pointer
backwards.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import reduce
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from transitpulse.config import ProjectConfig
from transitpulse.contracts.enums import FEED_ABBR, FEED_ORDER, QUALITY_REASONS, GenerationStatus, PublishOutcome
from transitpulse.contracts.envelope import iso_utc, utcnow
from transitpulse.contracts.ids import canonical_json, generation_id, sha256_hex
from transitpulse.obs.logging import get_logger, log_event
from transitpulse.processing import control_clock, dlq, parse, sink
from transitpulse.processing.context import (
    PARSED_SNAPSHOT_COLUMNS,
    PER_PARTITION_COUNT_KEYS,
    STATS_COLUMNS,
    TABLE_TTL_CLASS,
    BuildParams,
    GenerationOutput,
    RunInfo,
)
from transitpulse.processing.curated import BATCH_UNIT, CuratedBatch
from transitpulse.processing.faults import (
    AFTER_CAS_BEFORE_COMMIT,
    AFTER_PARTIAL_WRITE,
    AFTER_READY_BEFORE_CAS,
    FaultInjector,
)
from transitpulse.runtime.paths import WorkPaths
from transitpulse.storage.cassandra import epoch_ms, execute, ms_floor, normalize_row, unset_if_none, was_applied
from transitpulse.storage.manifest import insert_building, mark_ready, read_manifest, set_outcome_once
from transitpulse.storage.migrate import TableSchema, table_schemas
from transitpulse.storage.pointer import bootstrap_pointer, cas_pointer, read_pointer

_log = get_logger("transitpulse.processing.batch")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_SL = StorageLevel.MEMORY_AND_DISK

READY = str(GenerationStatus.READY)
BUILDING = str(GenerationStatus.BUILDING)
OUTCOME_PUBLISHED = str(PublishOutcome.PUBLISHED)
OUTCOME_LATE = str(PublishOutcome.LATE_NOT_PUBLISHED)
OUTCOME_EXPIRED = str(PublishOutcome.EXPIRED_NOT_PUBLISHED)

# Feeds whose projection inherits from the parent generation, and the parent table read for them.
PARENT_TABLE: dict[str, str] = {
    "vehicle_positions": "vehicles_by_route_snapshot",
    "alerts": "alerts_by_route_snapshot",
}
# Written AFTER the fault point after_partial_write (entity/history rows first, then metrics + quality).
SECOND_WRITE_GROUP: tuple[str, ...] = ("route_metrics_by_snapshot", "quality_by_snapshot")
QUALITY_TABLE = "quality_by_snapshot"
QUALITY_COLUMNS: tuple[str, ...] = (
    "run_id", "feed_type", "snapshot_id", "reason_code", "severity", "unit", "record_count",
    "disposition_counts", "sample_refs", "details_json",
)
STATUS_TOTALS: tuple[str, ...] = (
    "input_messages_total", "rejected_messages_total", "snapshots_built_total", "snapshots_published_total",
    "late_deliveries_total", "duplicate_deliveries_total",
)
DUPLICATE_REFS_CAP = 200
STATUS_LWT_ATTEMPTS = 5

# result labels of tp_processor_generation_publish_total
RESULT_PUBLISHED = "published"
RESULT_REUSED = "reused"
RESULT_LATE = "late_not_published"
RESULT_EXPIRED = "expired_not_published"


# --------------------------------------------------------------------------------------------------
# Errors (all fatal for the batch: foreachBatch raises, the checkpoint is not committed)
# --------------------------------------------------------------------------------------------------

class BatchError(RuntimeError):
    pass


class BacklogBoundExceeded(BatchError):
    pass


class CountMismatch(BatchError):
    def __init__(self, snapshot_id: str, expected: Mapping[str, int], written: Mapping[str, int]) -> None:
        super().__init__(f"snapshot {snapshot_id}: written rows {dict(written)} != expected {dict(expected)}")
        self.snapshot_id, self.expected, self.written = snapshot_id, dict(expected), dict(written)


class AccountingMismatch(BatchError):
    pass


class ManifestConflict(BatchError):
    pass


class PointerContention(BatchError):
    pass


class StatusContention(BatchError):
    pass


class InvalidGenerationOutput(BatchError):
    pass


class ParentUnavailable(BatchError):
    """A frozen-available parent generation cannot be read (retry; replay into a new run if lost)."""


# Parent read outcomes that are deterministic for an immutable parent (same result on every retry).
DETERMINISTIC_PARENT_REASONS: frozenset[str] = frozenset({
    "parent_read_limit_per_route_exceeded", "parent_read_total_limit_exceeded",
})


# --------------------------------------------------------------------------------------------------
# Pure helpers (unit-tested)
# --------------------------------------------------------------------------------------------------

def ms_to_dt(ms: int | None) -> datetime | None:
    return None if ms is None else _EPOCH + timedelta(milliseconds=int(ms))


def epoch_seconds(dt: datetime) -> float:
    return epoch_ms(dt) / 1000.0


def remaining_s(expires_at: datetime | None, now: datetime | None = None) -> int:
    """Whole seconds left before ``expires_at`` with the sink's TTL rule (``ceil(expires) - floor(now)``)."""
    if expires_at is None:
        return 0
    return sink.remaining_seconds(epoch_seconds(expires_at), None if now is None else epoch_seconds(now))


def latency_ms(visible_at: datetime | None, published_at: datetime | None) -> int | None:
    if visible_at is None or published_at is None:
        return None
    return epoch_ms(visible_at) - epoch_ms(published_at)


def select_parent(pointer: Mapping[str, Any] | None, previous: Mapping[str, Any] | None) -> dict | None:
    """Parent = the candidate with the greatest ``source_order`` among (pointer, previous non-late in-batch
    snapshot of the same feed). Returns ``{snapshot_id, generation_id, source_order, expires_at}`` or None."""
    cands = [c for c in (pointer, previous) if c and c.get("snapshot_id")]
    if not cands:
        return None
    best = max(cands, key=lambda c: str(c.get("source_order") or ""))
    return {"snapshot_id": best["snapshot_id"], "generation_id": best.get("generation_id"),
            "source_order": best.get("source_order"), "expires_at": best.get("expires_at")}


def parent_available_at(parent: Mapping[str, Any] | None, now: datetime) -> bool | None:
    """Frozen availability of the selected parent at first materialization (None = no parent).

    The parent is the current pointer (READY by construction) or an earlier non-late snapshot of the same
    batch (built before its child in this batch); it is available when its generation has not expired
    under the sink TTL rule. Rows that later turn out unreadable are handled at build time.
    """
    if not parent:
        return None
    return remaining_s(parent.get("expires_at"), now) >= 1


def building_row(
    meta: Mapping[str, Any],
    run: RunInfo,
    *,
    batch_id: int,
    now: datetime,
    parent: Mapping[str, Any] | None,
    late: bool,
    pipeline_version: str,
) -> dict[str, Any]:
    """BUILDING manifest row with every frozen generation parameter (ms precision, aware UTC)."""
    now = ms_floor(now)
    if run.clock_mode == "source":
        if run.application_clock is None:
            raise BatchError(f"source-clock run {run.run_id} has no application_clock")
        metrics_as_of = ms_floor(run.application_clock)
    else:
        metrics_as_of = now
    pol = run.policy
    feed, sid = meta["feed_type"], meta["snapshot_id"]
    return {
        "run_id": run.run_id,
        "feed_type": feed,
        "snapshot_id": sid,
        "generation_id": generation_id(run.run_id, feed, sid, pipeline_version),
        "source_order": meta["source_order"],
        "feed_timestamp": _EPOCH + timedelta(seconds=int(meta["feed_timestamp"])),
        "original_fetched_at": ms_to_dt(meta.get("original_fetched_at_ms")),
        "published_at": ms_to_dt(meta.get("published_at_ms")),
        "capture_id": meta.get("capture_id"),
        "kafka_topic": meta.get("kafka_topic"),
        "kafka_partition": None if meta.get("kafka_partition") is None else int(meta["kafka_partition"]),
        "kafka_offset": None if meta.get("kafka_offset") is None else int(meta["kafka_offset"]),
        "first_batch_id": int(batch_id),
        "raw_path": meta.get("raw_path"),
        "raw_sha256": meta.get("raw_sha256"),
        "gtfs_version": meta.get("gtfs_version"),
        "scope_id": meta.get("scope_id"),
        "scenario_id": meta.get("scenario_id"),
        "parent_snapshot_id": None if (late or not parent) else parent["snapshot_id"],
        "parent_generation_id": None if (late or not parent) else parent.get("generation_id"),
        "parent_available": None if (late or not parent) else parent_available_at(parent, now),
        "built_as_late": bool(late),
        "metrics_as_of": metrics_as_of,
        "clock_sequence_at_build": int(run.clock_sequence),
        "first_materialized_at": now,
        "expires_at": now + timedelta(seconds=int(pol.current_generation_ttl_seconds)),
        "history_expires_at": now + timedelta(seconds=int(pol.history_ttl_seconds)),
        "source_entity_count": meta.get("source_entity_count"),
        "selected_entity_count": meta.get("selected_entity_count"),
    }


def frozen_parent_available(man: Mapping[str, Any]) -> bool:
    """Stored ``parent_available`` (NULL = no parent, or a manifest written before the column -> True)."""
    v = man.get("parent_available")
    return True if v is None else bool(v)


def params_from_manifest(man: Mapping[str, Any], *, parent_available: bool | None = None) -> BuildParams:
    """Frozen BuildParams exactly as stored (identical on every retry).

    ``parent_available`` defaults to the frozen manifest value; the caller passes False only when a
    frozen-available parent exceeds the parent read bounds (deterministic for an immutable parent; then
    PARENT_UNAVAILABLE, no inheritance). Missing parent rows raise :class:`ParentUnavailable` instead.
    """
    if parent_available is None:
        parent_available = frozen_parent_available(man)
    return BuildParams(
        run_id=man["run_id"],
        feed_type=man["feed_type"],
        snapshot_id=man["snapshot_id"],
        generation_id=man["generation_id"],
        source_order=man["source_order"],
        feed_timestamp=man["feed_timestamp"],
        original_fetched_at=man.get("original_fetched_at"),
        published_at=man.get("published_at"),
        capture_id=man.get("capture_id"),
        raw_path=man.get("raw_path"),
        raw_sha256=man.get("raw_sha256"),
        kafka_topic=man.get("kafka_topic"),
        kafka_partition=man.get("kafka_partition"),
        kafka_offset=man.get("kafka_offset"),
        first_batch_id=int(man["first_batch_id"]),
        metrics_as_of=man["metrics_as_of"],
        clock_sequence_at_build=int(man.get("clock_sequence_at_build") or 0),
        parent_snapshot_id=man.get("parent_snapshot_id"),
        parent_generation_id=man.get("parent_generation_id"),
        built_as_late=bool(man.get("built_as_late")),
        first_materialized_at=man["first_materialized_at"],
        expires_at=man["expires_at"],
        history_expires_at=man["history_expires_at"],
        gtfs_version=man.get("gtfs_version"),
        scope_id=man.get("scope_id"),
        scenario_id=man.get("scenario_id"),
        parent_available=bool(parent_available),
    )


def pointer_row(man: Mapping[str, Any], view_published_at: datetime) -> dict[str, Any]:
    return {
        "run_id": man["run_id"], "feed_type": man["feed_type"], "snapshot_id": man["snapshot_id"],
        "generation_id": man["generation_id"], "source_order": man["source_order"],
        "feed_timestamp": man.get("feed_timestamp"), "metrics_as_of": man.get("metrics_as_of"),
        "view_published_at": view_published_at, "expires_at": man["expires_at"],
        "is_partial": bool(man.get("is_partial")), "is_empty": bool(man.get("is_empty")),
    }


def quality_row(run_id: str, feed_type: str, snapshot_id: str, reason_code: str, *, record_count: int = 1,
                sample_refs: Sequence[str] = (), details: Mapping[str, Any] | None = None) -> dict[str, Any]:
    info = QUALITY_REASONS[reason_code]
    return {
        "run_id": run_id, "feed_type": feed_type, "snapshot_id": snapshot_id, "reason_code": reason_code,
        "severity": str(info.severity), "unit": info.unit, "record_count": int(record_count),
        "disposition_counts": {str(info.disposition): int(record_count)},
        "sample_refs": list(sample_refs),
        "details_json": json.dumps(dict(details or {}), sort_keys=True, default=str),
    }


_CQL_SCALAR: dict[str, T.DataType] = {
    "text": T.StringType(), "varchar": T.StringType(), "ascii": T.StringType(), "uuid": T.StringType(),
    "timeuuid": T.StringType(), "inet": T.StringType(), "int": T.IntegerType(), "bigint": T.LongType(),
    "counter": T.LongType(), "smallint": T.ShortType(), "tinyint": T.ByteType(), "double": T.DoubleType(),
    "float": T.FloatType(), "boolean": T.BooleanType(), "date": T.DateType(),
}


def _split_type_args(s: str) -> list[str]:
    depth, cur, out = 0, [], []
    for ch in s:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
        if ch == "," and depth == 0:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    out.append("".join(cur))
    return out


def spark_data_type(cql_type: str, *, timestamp_ms: bool) -> T.DataType:
    """Spark type of a CQL type. ``timestamp_ms=True``: timestamps as epoch-ms LongType (sink input);
    False: TimestampType (frames handed to WP5 builders, e.g. parent rows)."""
    t = cql_type.replace(" ", "").lower()
    if t.startswith("frozen<") and t.endswith(">"):
        t = t[len("frozen<"):-1]
    if t == "timestamp":
        return T.LongType() if timestamp_ms else T.TimestampType()
    if t in _CQL_SCALAR:
        return _CQL_SCALAR[t]
    if (t.startswith("set<") or t.startswith("list<")) and t.endswith(">"):
        inner = t[t.index("<") + 1:-1]
        return T.ArrayType(spark_data_type(inner, timestamp_ms=timestamp_ms), True)
    if t.startswith("map<") and t.endswith(">"):
        k, v = _split_type_args(t[4:-1])
        return T.MapType(spark_data_type(k, timestamp_ms=timestamp_ms), spark_data_type(v, timestamp_ms=timestamp_ms),
                         True)
    raise ValueError(f"unsupported CQL type {cql_type!r}")


def conform_table(df: DataFrame, table: str, schema: TableSchema) -> DataFrame:
    """Validate a builder table frame against the CQL contract and cast it for the sink.

    Columns must be a subset of the CQL columns and include the full primary key. TimestampType columns
    become epoch milliseconds (``unix_millis``; sub-ms precision truncated like Cassandra); everything else
    is cast to the Spark type implied by the CQL type. Column order follows the CQL file.
    """
    cols = list(df.columns)
    if len(cols) != len(set(cols)):
        raise InvalidGenerationOutput(f"{table}: duplicate column names {cols}")
    unknown = [c for c in cols if c not in schema.columns]
    if unknown:
        raise InvalidGenerationOutput(f"{table}: columns not in the CQL schema: {unknown}")
    missing_pk = [c for c in schema.primary_key if c not in cols]
    if missing_pk:
        raise InvalidGenerationOutput(f"{table}: primary key columns missing: {missing_pk}")
    dtypes = {f.name: f.dataType for f in df.schema.fields}
    exprs: list[Column] = []
    for c in [c for c in schema.columns if c in cols]:
        cql = schema.columns[c]
        if cql == "timestamp":
            dt = dtypes[c]
            if not isinstance(dt, (T.TimestampType, T.TimestampNTZType, T.DateType, T.NullType)):
                raise InvalidGenerationOutput(
                    f"{table}.{c}: timestamp columns must be TimestampType (got {dt.simpleString()})")
            exprs.append(parse.unix_millis(c).alias(c))
        else:
            exprs.append(F.col(c).cast(spark_data_type(cql, timestamp_ms=True)).alias(c))
    return df.select(*exprs)


def _hash_struct(df: DataFrame) -> Column:
    """Order-insensitive normalization of one row for the content checksum (sets/maps sorted)."""
    parts: list[Column] = []
    for f in sorted(df.schema.fields, key=lambda x: x.name):
        c = F.col(f.name)
        if isinstance(f.dataType, T.ArrayType):
            c = F.array_sort(c)
        elif isinstance(f.dataType, T.MapType):
            c = F.array_sort(F.map_entries(c))
        parts.append(c.alias(f.name))
    return F.struct(*parts)


def aggregate_frame(tables: Mapping[str, DataFrame], schemas: Mapping[str, TableSchema]) -> DataFrame | None:
    """One union frame ``(t, k, pkh, bad, h)`` over all (conformed) tables for a single aggregation job."""
    parts: list[DataFrame] = []
    for t, df in sorted(tables.items()):
        pk = schemas[t].primary_key
        key = PER_PARTITION_COUNT_KEYS.get(t)
        kcol = F.col(key).cast("string") if key else F.lit(None).cast("string")
        bad = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in pk])
        parts.append(df.select(
            F.lit(t).alias("t"),
            kcol.alias("k"),
            F.sha2(F.to_json(F.struct(*[F.col(c) for c in pk])), 256).alias("pkh"),
            F.when(bad, F.lit(1)).otherwise(F.lit(0)).alias("bad"),
            F.abs(F.xxhash64(F.to_json(_hash_struct(df))) % F.lit(2147483647)).alias("h"),
        ))
    if not parts:
        return None
    return reduce(lambda a, b: a.unionByName(b), parts)


@dataclass(frozen=True)
class GenerationCounts:
    expected_counts: dict[str, int]
    content_checksum: str


def expected_counts_from_aggregates(
    rows: Iterable[Mapping[str, Any]],
    tables: Iterable[str],
    route_ids: Iterable[str],
) -> GenerationCounts:
    """``"<table>"`` totals + ``"<table>/<key>"`` per PER_PARTITION_COUNT_KEYS (zero-filled for every scope
    route of route-keyed tables) + an order-insensitive content checksum. Raises on NULL or duplicate PKs."""
    tables = sorted(set(tables))
    expected: dict[str, int] = {t: 0 for t in tables}
    for t in tables:
        if PER_PARTITION_COUNT_KEYS.get(t) == "route_id":
            for r in route_ids:
                expected[f"{t}/{r}"] = 0
    sums: dict[str, int] = {t: 0 for t in tables}
    for row in rows:
        t, k = row["t"], row.get("k")
        n, d = int(row["n"]), int(row["d"])
        if int(row.get("bad") or 0):
            raise InvalidGenerationOutput(f"{t}: {row['bad']} rows with NULL primary key columns")
        if d != n:
            raise InvalidGenerationOutput(f"{t}: {n - d} rows duplicate a primary key (rows must be unique per PK)")
        expected[t] = expected.get(t, 0) + n
        if PER_PARTITION_COUNT_KEYS.get(t):
            if k is None:
                raise InvalidGenerationOutput(f"{t}: NULL partition key")
            expected[f"{t}/{k}"] = expected.get(f"{t}/{k}", 0) + n
        sums[t] = sums.get(t, 0) + int(row.get("hs") or 0)
    checksum = sha256_hex(canonical_json({t: [expected.get(t, 0), sums.get(t, 0)] for t in tables}))
    return GenerationCounts(expected, checksum)


def compute_generation_counts(tables: Mapping[str, DataFrame], schemas: Mapping[str, TableSchema],
                              route_ids: Iterable[str]) -> GenerationCounts:
    """ONE small aggregation job over every table of the generation."""
    agg = aggregate_frame(tables, schemas)
    rows: list[dict] = []
    if agg is not None:
        rows = [r.asDict() for r in agg.groupBy("t", "k").agg(
            F.count(F.lit(1)).alias("n"),
            F.countDistinct("pkh").alias("d"),
            F.sum("bad").alias("bad"),
            F.sum("h").alias("hs"),
        ).collect()]
    return expected_counts_from_aggregates(rows, tables.keys(), route_ids)


def accounting_metrics(accounting_json: str | None) -> dict[str, dict[str, int]]:
    """Opportunistic extraction of record-level counters from a builder's accounting JSON (top-level ints)."""
    out: dict[str, dict[str, int]] = {"candidates": {}, "unique": {}, "duplicate": {}}
    try:
        acc = json.loads(accounting_json) if accounting_json else {}
    except ValueError:
        return out
    if not isinstance(acc, dict):
        return out
    for k, v in acc.items():
        if isinstance(v, bool) or not isinstance(v, int):
            continue
        if k.startswith(("candidate_", "valid_candidate_")):
            out["candidates"][k] = v
        elif k.startswith("unique_"):
            out["unique"][k[len("unique_"):]] = v
        elif k.startswith("duplicate_"):
            out["duplicate"][k[len("duplicate_"):]] = v
    return out


def _ts_lit(dt: datetime | None) -> Column:
    """TimestampType literal from exact epoch microseconds (no py4j datetime conversion, TZ independent)."""
    if dt is None:
        return F.lit(None).cast("timestamp")
    d = dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    delta = d - _EPOCH
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return F.expr(f"timestamp_micros({micros})")


def quality_frame(spark, rows: Sequence[Mapping[str, Any]], schema: TableSchema) -> DataFrame:
    struct = T.StructType([T.StructField(c, spark_data_type(schema.columns[c], timestamp_ms=True), True)
                           for c in QUALITY_COLUMNS])
    return spark.createDataFrame([tuple(r.get(c) for c in QUALITY_COLUMNS) for r in rows], schema=struct)


def rows_to_frame(spark, rows: Sequence[Mapping[str, Any]], schema: TableSchema) -> DataFrame:
    """Cassandra rows (normalized dicts) -> DataFrame with the explicit CQL-derived schema (TimestampType)."""
    cols = list(schema.columns)
    struct = T.StructType([T.StructField(c, spark_data_type(schema.columns[c], timestamp_ms=False), True)
                           for c in cols])
    data = []
    for r in rows:
        vals = []
        for c in cols:
            v = r.get(c)
            if isinstance(v, (set, frozenset)):
                v = sorted(v)
            vals.append(v)
        data.append(tuple(vals))
    return spark.createDataFrame(data, schema=struct)


# --------------------------------------------------------------------------------------------------
# Metrics (names fixed by design E §6)
# --------------------------------------------------------------------------------------------------

_DURATION_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600)


class ProcessorMetrics:
    def __init__(self, cm=None) -> None:
        from transitpulse.obs.metrics import ComponentMetrics

        self.cm = cm or ComponentMetrics("processor")
        c = self.cm
        self.batch_duration = c.histogram("batch_duration_seconds", "Micro-batch wall duration",
                                          buckets=_DURATION_BUCKETS)
        self.input_rows = c.counter("input_rows_total", "Kafka rows of this run per topic", ("topic",))
        self.input_snapshots = c.counter("input_snapshots_total", "Valid snapshot deliveries", ("feed",))
        self.normalized_candidates = c.counter("normalized_candidates_total", "Normalized candidate records",
                                               ("feed", "kind"))
        self.unique_records = c.counter("unique_records_total", "Unique retained records", ("kind",))
        self.duplicate_records = c.counter("duplicate_records_total", "Duplicate records", ("kind",))
        self.invalid_records = c.counter("invalid_records_total", "Rejected messages/entities", ("reason",))
        self.batch_failures = c.counter("batch_failures_total", "Failed micro-batches")
        self.last_batch_id = c.gauge("last_successful_batch_id", "Last successful batch id")
        self.last_batch_ts = c.gauge("last_successful_batch_timestamp_seconds", "Wall time of the last success")
        self.kafka_backlog = c.gauge("kafka_backlog_messages", "latestOffset - endOffset per partition",
                                     ("topic", "partition"))
        self.cassandra_write_seconds = c.histogram("cassandra_write_seconds", "Sink write time per table",
                                                   ("table",), buckets=_DURATION_BUCKETS)
        self.cassandra_write_errors = c.counter("cassandra_write_errors_total", "Failed sink writes", ("table",))
        self.generation_publish = c.counter("generation_publish_total", "Publish decisions", ("feed", "result"))
        self.visibility_latency = c.histogram("visibility_latency_seconds",
                                              "view_published_at - published_at (first publication)", ("feed",),
                                              buckets=_DURATION_BUCKETS)
        self.application_clock = c.gauge("application_clock_timestamp_seconds", "Run application clock")


# --------------------------------------------------------------------------------------------------
# pipeline_status (LWT rows guarded by batch id)
# --------------------------------------------------------------------------------------------------

SELECT_STATUS = "SELECT * FROM pipeline_status WHERE run_id = ? AND component = ?"
BOOTSTRAP_STATUS = (
    "INSERT INTO pipeline_status (run_id, component, status, last_batch_id, last_heartbeat_at, "
    f"{', '.join(STATUS_TOTALS)}) VALUES (?, ?, ?, ?, ?, {', '.join('?' for _ in STATUS_TOTALS)}) IF NOT EXISTS"
)


def bootstrap_status(session, run_id: str, component: str, *, now: datetime) -> bool:
    """Create an LWT status row (``last_batch_id = -1``, totals 0) if it does not exist."""
    params = [run_id, component, "STARTING", -1, now] + [0 for _ in STATUS_TOTALS]
    return was_applied(execute(session, BOOTSTRAP_STATUS, params))


def _stored_query_id(row: Mapping[str, Any]) -> str | None:
    try:
        return (json.loads(row.get("progress_json") or "{}") or {}).get("query_id")
    except (ValueError, AttributeError):
        return None


def status_bind_values(values: Mapping[str, Any], cols: Sequence[str], clear: Iterable[str] = ()) -> list[Any]:
    """Bind values in ``cols`` order: explicit NULL for ``clear`` columns, UNSET for other ``None`` (pure)."""
    clear_set = set(clear)
    out: list[Any] = []
    for c in cols:
        v = values.get(c)
        out.append(None if c in clear_set else unset_if_none([v])[0])
    return out


def update_status_guarded(
    session,
    run_id: str,
    component: str,
    batch_id: int,
    *,
    deltas: Mapping[str, int],
    sets: Mapping[str, Any],
    query_id: str | None,
    now: datetime,
    clear: Iterable[str] = (),
) -> bool:
    """Add ``deltas`` to the cumulative totals once per batch id (``IF last_batch_id = <read>``).

    Skipped (False) when the row already reflects this batch (replay of an uncommitted batch). A different
    stored ``query_id`` (new checkpoint for the run) restarts the guard. ``None`` in ``sets`` leaves a column
    untouched (UNSET); columns named in ``clear`` are bound as explicit NULL (e.g. ``last_error`` once the
    processor is healthy again).
    """
    clear_set = set(clear)
    protected = {"last_batch_id", "run_id", "component"}
    unknown = ((set(deltas) - set(STATUS_TOTALS)) | (protected & set(sets))
               | (clear_set & (protected | set(STATUS_TOTALS))) | (clear_set & set(sets)))
    if unknown:
        raise ValueError(f"invalid status columns {sorted(unknown)}")
    for _ in range(STATUS_LWT_ATTEMPTS):
        row = normalize_row(execute(session, SELECT_STATUS, (run_id, component), serial=True).one())
        if row is None:
            bootstrap_status(session, run_id, component, now=now)
            continue
        last = row.get("last_batch_id")
        same_lineage = query_id is None or _stored_query_id(row) in (None, query_id)
        if last is not None and last >= batch_id and same_lineage:
            return False
        values: dict[str, Any] = {c: int(row.get(c) or 0) + int(deltas.get(c, 0)) for c in STATUS_TOTALS}
        values.update(sets)
        values["last_batch_id"] = int(batch_id)
        for c in clear_set:
            values[c] = None
        cols = sorted(values)
        cond = "IF last_batch_id = ?" if last is not None else "IF last_batch_id = null"
        cql = (f"UPDATE pipeline_status SET {', '.join(f'{c} = ?' for c in cols)} "
               f"WHERE run_id = ? AND component = ? {cond}")
        params = status_bind_values(values, cols, clear_set) + [run_id, component]
        if last is not None:
            params.append(last)
        if was_applied(execute(session, cql, params)):
            return True
    raise StatusContention(f"pipeline_status({component}) LWT contention after {STATUS_LWT_ATTEMPTS} attempts")


def record_processor_failure(session, run_id: str, message: str, *, now: datetime) -> bool:
    """Mark the processor row FAILED (LWT on the LWT-only row; last_batch_id unchanged)."""
    row = normalize_row(execute(session, SELECT_STATUS, (run_id, "processor"), serial=True).one())
    if row is None:
        bootstrap_status(session, run_id, "processor", now=now)
        row = normalize_row(execute(session, SELECT_STATUS, (run_id, "processor"), serial=True).one()) or {}
    last = row.get("last_batch_id")
    cond = "IF last_batch_id = ?" if last is not None else "IF last_batch_id = null"
    cql = ("UPDATE pipeline_status SET status = ?, last_error = ?, last_heartbeat_at = ? "
           f"WHERE run_id = ? AND component = ? {cond}")
    params: list[Any] = ["FAILED", message[:2000], now, run_id, "processor"]
    if last is not None:
        params.append(last)
    return was_applied(execute(session, cql, params))


# --------------------------------------------------------------------------------------------------
# Context and per-snapshot records
# --------------------------------------------------------------------------------------------------

@dataclass
class BatchContext:
    cfg: ProjectConfig
    paths: WorkPaths
    run_id: str
    scope_id: str
    gtfs_version: str
    pipeline_version: str
    clock_mode: str
    dims: Any
    session: Any
    metrics: ProcessorMetrics
    faults: FaultInjector
    query_name: str
    query_id: str | None = None
    schemas: dict[str, TableSchema] = field(default_factory=table_schemas)
    timestamp_ms_threshold: int | None = None     # run-pinned policy value (falls back to config)
    checkpoint_dir: Path | None = None

    def resolve_query_id(self) -> str | None:
        """Streaming query id; read from ``<checkpoint>/metadata`` if a batch runs before start() returned."""
        if self.query_id is None and self.checkpoint_dir is not None:
            meta = Path(self.checkpoint_dir) / "metadata"
            try:
                first = meta.read_text(encoding="utf-8").splitlines()[0]
                self.query_id = json.loads(first).get("id")
            except (OSError, ValueError, IndexError):
                pass
        return self.query_id

    @property
    def parse_ctx(self) -> parse.ParseContext:
        return parse.ParseContext.from_config(
            self.cfg, run_id=self.run_id, scope_id=self.scope_id, gtfs_version=self.gtfs_version,
            clock_mode=self.clock_mode, pipeline_version=self.pipeline_version,
            timestamp_ms_threshold=self.timestamp_ms_threshold,
        )


@dataclass
class SnapshotPlan:
    feed_type: str
    snapshot_id: str
    meta: dict[str, Any]
    manifest: dict[str, Any]
    created: bool
    duplicates_in_batch: int = 0
    duplicate_refs: list[str] = field(default_factory=list)


@dataclass
class SnapshotResult:
    feed_type: str
    snapshot_id: str
    generation_id: str
    action: str                 # built | reused | curated_only | history_only
    result: str                 # published | reused | late_not_published | expired_not_published
    redelivery: bool
    duplicates: int             # duplicate deliveries in this batch (in-batch copies + 1 if re-delivered)
    outcome_applied: bool
    manifest: dict[str, Any]
    in_batch_duplicates: int = 0


def lineage_totals(rs: Sequence[SnapshotResult]) -> dict[str, int]:
    """Per-batch deltas of the cumulative pipeline_status counters (pure).

    Lead decision from the golden fixture (``deliveries`` / ``lineage_totals``, steps S3 and S4):
      * a delivery of a snapshot whose manifest was created in an EARLIER batch, and every in-batch copy
        (``delivery_rank > 1``), is a duplicate delivery;
      * a delivery whose snapshot loses the pointer comparison is a late delivery. A re-delivered old
        snapshot (S4: VP1 after VP2 is current) is BOTH duplicate and late; a re-delivery of the current
        generation (S3: VP2 again) is only a duplicate;
      * built/published count a generation once, in the batch that first materialized it (deterministic
        when an uncommitted batch is replayed, because then ``first_batch_id == batch_id``).
    """
    first = [r for r in rs if not r.redelivery]
    return {
        "snapshots_built_total": sum(1 for r in first if r.manifest.get("status") == READY),
        "snapshots_published_total": sum(1 for r in first if r.manifest.get("publish_outcome") == OUTCOME_PUBLISHED),
        "late_deliveries_total": sum(1 + r.in_batch_duplicates for r in rs if r.result == RESULT_LATE),
        "duplicate_deliveries_total": sum(r.duplicates for r in rs),
    }


# --------------------------------------------------------------------------------------------------
# The processor
# --------------------------------------------------------------------------------------------------

class BatchProcessor:
    """``foreachBatch`` callable bound to one run (driver side; never pickled)."""

    def __init__(self, ctx: BatchContext) -> None:
        self.ctx = ctx
        self.sink_conf = sink.connection_conf(ctx.cfg)
        self._pctx = ctx.parse_ctx

    def __call__(self, df: DataFrame, batch_id: int) -> None:
        self.process_batch(df, batch_id)

    # ---------------------------------------------------------------- entry point
    def process_batch(self, df: DataFrame, batch_id: int) -> None:
        batch_id = int(batch_id)
        t0 = time.monotonic()
        wall = ms_floor(utcnow())
        held: list[DataFrame] = []
        log_event(_log, "batch_start", "ok", batch_id=batch_id)
        try:
            self._process(df, batch_id, wall, t0, held)
        except Exception as exc:
            self.ctx.metrics.batch_failures.inc()
            log_event(_log, "batch_commit", "failed", f"{type(exc).__name__}: {exc}"[:1000], level=logging.ERROR,
                      batch_id=batch_id)
            raise
        finally:
            for d in held:
                try:
                    d.unpersist()
                except Exception:  # noqa: BLE001 - cleanup only; the original outcome propagates
                    log_event(_log, "batch_commit", "failed", "unpersist failed", level=logging.WARNING,
                              batch_id=batch_id)

    # ---------------------------------------------------------------- whole batch
    def _process(self, df: DataFrame, batch_id: int, wall: datetime, t0: float, held: list[DataFrame]) -> None:
        ctx, cfg, pctx = self.ctx, self.ctx.cfg, self._pctx
        spark = df.sparkSession
        session = ctx.session
        ctx.resolve_query_id()

        m = parse.filter_run(parse.kafka_columns(df), ctx.run_id).persist(_SL)
        held.append(m)
        topic_counts = {r["topic"]: int(r["count"]) for r in m.groupBy("topic").count().collect()}
        total = sum(topic_counts.values())
        if total == 0:
            log_event(_log, "batch_commit", "skipped", "no messages of this run in the batch", batch_id=batch_id)
            return
        for topic, n in topic_counts.items():
            ctx.metrics.input_rows.labels(topic=topic).inc(n)
        snap_msgs = sum(topic_counts.get(t, 0) for t in pctx.topics_feed)
        ctl_msgs = topic_counts.get(pctx.control_topic, 0)

        curated = CuratedBatch(ctx.paths, ctx.run_id, ctx.query_name, batch_id)
        committed = curated.begin()

        sp = parse.parse_snapshots(m, pctx).persist(_SL)
        cp = parse.parse_controls(m, pctx).persist(_SL)
        held.extend([sp, cp])

        # ---- A. control first
        if ctl_msgs:
            limit = int(cfg.spark.max_control_events_per_batch)
            clock_rows = parse.clock_event_frame(cp).limit(limit + 1).collect()
            truncated = len(clock_rows) > limit
            status_rows = parse.status_event_frame(cp).collect()
            control_clock.apply_control(
                session, ctx.run_id, clock_mode=ctx.clock_mode, clock_rows=clock_rows[:limit],
                truncated=truncated, status_rows=status_rows, now=wall, batch_id=batch_id,
            )
        run = control_clock.read_run_info(session, ctx.run_id)
        if run.clock_mode == "wall" and run.run_state == control_clock.CREATED and snap_msgs:
            if control_clock.mark_running_on_first_data(session, ctx.run_id, now=wall):
                run = control_clock.read_run_info(session, ctx.run_id)
        if run.application_clock is not None:
            ctx.metrics.application_clock.set(epoch_seconds(run.application_clock))

        # ---- B. message-level rejects
        rejects = parse.message_rejects(sp, cp, pctx, batch_id=batch_id, detected_at_iso=iso_utc(wall))
        reject_counts = [r.asDict() for r in rejects.groupBy("feed_type", "reason_code").count().collect()]
        n_rejected = sum(int(r["count"]) for r in reject_counts)
        last_reject: dict[str, dict] = {}
        if n_rejected:
            for r in reject_counts:
                ctx.metrics.invalid_records.labels(reason=r["reason_code"]).inc(int(r["count"]))
            w = Window.partitionBy("feed_type").orderBy(F.col("source_offset").desc())
            for r in (rejects.select("feed_type", "reason_code", "error_id", "source_offset")
                      .withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1).collect()):
                last_reject[r["feed_type"]] = r.asDict()
            log_event(_log, "validate", "rejected", f"{n_rejected} message(s) rejected", batch_id=batch_id,
                      counts={f"{r['feed_type']}:{r['reason_code']}": int(r["count"]) for r in reject_counts})

        # ---- C. snapshot metadata (hard bound)
        ok: DataFrame | None = None
        plans: list[SnapshotPlan] = []
        n_dup_in_batch = 0
        if snap_msgs:
            ok = parse.ok_snapshots(sp).persist(_SL)
            held.append(ok)
            bound = int(cfg.spark.max_snapshots_per_batch)
            meta_rows = parse.meta_frame(ok).orderBy("feed_type", "source_order").limit(bound + 1).collect()
            if len(meta_rows) > bound:
                raise BacklogBoundExceeded(
                    f"batch {batch_id} holds more than spark.max_snapshots_per_batch={bound} distinct snapshots; "
                    "raise the bound (driver memory permitting) or start a new run")
            dup_rows = parse.duplicate_frame(ok).limit(bound + 1).collect()
            n_dup_in_batch = sum(int(r["duplicates"]) for r in dup_rows)
            plans = self._prepare_manifests([r.asDict() for r in meta_rows], [r.asDict() for r in dup_rows],
                                            run, batch_id, wall)
            for p in plans:
                ctx.metrics.input_snapshots.labels(feed=p.feed_type).inc(1 + p.duplicates_in_batch)

        # ---- E. per snapshot
        results: list[SnapshotResult] = []
        for plan in plans:
            assert ok is not None
            results.append(self._process_snapshot(spark, ok, plan, run, batch_id, curated, committed))

        # ---- F. batch-level curated + DLQ + commit
        rejected_snap = sum(int(r["count"]) for r in reject_counts if r["feed_type"] != parse.CONTROL_FEED)
        rejected_ctl = n_rejected - rejected_snap
        ok_deliveries = len(plans) + n_dup_in_batch
        accounting = {
            "unit": "kafka_message",
            "input_messages": total, "control_messages": ctl_msgs, "snapshot_messages": snap_msgs,
            "control_ok": ctl_msgs - rejected_ctl, "snapshot_ok": ok_deliveries,
            "rejected_messages": n_rejected, "distinct_snapshots": len(plans),
            "duplicate_deliveries_in_batch": n_dup_in_batch,
            "identity": "input_messages == control_ok + snapshot_ok + rejected_messages",
        }
        accounting["identity_ok"] = total == (ctl_msgs - rejected_ctl) + ok_deliveries + n_rejected
        if not accounting["identity_ok"]:
            log_event(_log, "batch_commit", "failed", "batch message accounting identity does not hold",
                      level=logging.ERROR, batch_id=batch_id, accounting=accounting)
        if not committed:
            self._commit_curated(spark, curated, sp, ok, rejects, results, batch_id, accounting,
                                 has_snapshots=bool(snap_msgs), has_rejects=bool(n_rejected))
        else:
            log_event(_log, "parquet_commit", "reused", "curated batch already committed", batch_id=batch_id)

        # ---- G. status, metrics, fault point
        duration_ms = int((time.monotonic() - t0) * 1000)
        self._update_status(results, topic_counts, reject_counts, last_reject, batch_id, wall, duration_ms,
                            n_rejected, total)
        ctx.metrics.batch_duration.observe(time.monotonic() - t0)
        ctx.metrics.last_batch_id.set(batch_id)
        ctx.metrics.last_batch_ts.set(time.time())
        log_event(_log, "batch_commit", "ok", batch_id=batch_id, duration_ms=round((time.monotonic() - t0) * 1000, 3),
                  input_messages=total, snapshots=len(plans), rejected=n_rejected)
        ctx.faults.check(AFTER_CAS_BEFORE_COMMIT, candidates=[(r.feed_type, r.snapshot_id) for r in results])

    # ---------------------------------------------------------------- D. manifests
    def _prepare_manifests(self, metas: list[dict], dups: list[dict], run: RunInfo, batch_id: int,
                           now: datetime) -> list[SnapshotPlan]:
        ctx, session = self.ctx, self.ctx.session
        dup_by_key = {(d["feed_type"], d["snapshot_id"]): d for d in dups}
        by_feed: dict[str, list[dict]] = {}
        for m in metas:
            by_feed.setdefault(m["feed_type"], []).append(m)
        plans: list[SnapshotPlan] = []
        for feed in sorted(by_feed, key=lambda f: (FEED_ORDER.get(f, 99), f)):
            ptr = read_pointer(session, ctx.run_id, feed, serial=True)
            previous: dict | None = None
            for meta in sorted(by_feed[feed], key=lambda x: x["source_order"]):
                sid = meta["snapshot_id"]
                man = read_manifest(session, ctx.run_id, feed, sid, serial=True)
                created = False
                if man is None:
                    late = ptr is not None and str(ptr["source_order"]) > str(meta["source_order"])
                    parent = select_parent(ptr, previous) if (feed in PARENT_TABLE and not late) else None
                    row = building_row(meta, run, batch_id=batch_id, now=now, parent=parent, late=late,
                                       pipeline_version=ctx.pipeline_version)
                    created, man = insert_building(session, row)
                    log_event(_log, "manifest_ready", "ok" if created else "reused",
                              "BUILDING manifest created" if created else "manifest created concurrently; adopted",
                              batch_id=batch_id, feed=feed, snapshot_id=sid, built_as_late=late,
                              parent_snapshot_id=man.get("parent_snapshot_id"))
                if ptr is None or str(man["source_order"]) > str(ptr["source_order"]):
                    previous = {"snapshot_id": man["snapshot_id"], "generation_id": man["generation_id"],
                                "source_order": man["source_order"], "expires_at": man.get("expires_at")}
                d = dup_by_key.get((feed, sid)) or {}
                plans.append(SnapshotPlan(feed, sid, meta, man, created, int(d.get("duplicates") or 0),
                                          list(d.get("refs") or [])))
        return plans

    # ---------------------------------------------------------------- E. one snapshot
    def _process_snapshot(self, spark, ok: DataFrame, plan: SnapshotPlan, run: RunInfo, batch_id: int,
                          curated: CuratedBatch, committed: bool) -> SnapshotResult:
        ctx = self.ctx
        man = plan.manifest
        feed, sid = plan.feed_type, plan.snapshot_id
        key = (ctx.run_id, feed, sid)
        redelivery = man.get("first_batch_id") != batch_id
        unit = CuratedBatch.unit_name(FEED_ABBR[feed], sid)
        need_curated = (not committed) and (not redelivery) and (not curated.has_unit(unit))
        now = ms_floor(utcnow())
        min_ttl = int(run.policy.min_publish_ttl_seconds)

        if man.get("status") != READY:
            if remaining_s(man["expires_at"], now) < min_ttl:
                self._build_and_write(spark, ok, plan, run, batch_id, curated, unit, need_curated, history_only=True)
                applied = set_outcome_once(ctx.session, key, OUTCOME_EXPIRED)
                ctx.metrics.generation_publish.labels(feed=feed, result=RESULT_EXPIRED).inc()
                final = read_manifest(ctx.session, ctx.run_id, feed, sid, serial=True) or man
                self._audit_duplicates(plan, final, redelivery, batch_id)
                return SnapshotResult(feed, sid, man["generation_id"], "history_only", RESULT_EXPIRED, redelivery,
                                      plan.duplicates_in_batch + int(redelivery), applied, final,
                                      in_batch_duplicates=plan.duplicates_in_batch)
            man = self._build_and_write(spark, ok, plan, run, batch_id, curated, unit, need_curated,
                                        history_only=False)
            action = "built"
            ctx.faults.check(AFTER_READY_BEFORE_CAS, feed_type=feed, snapshot_id=sid)
        elif need_curated:
            # READY from an interrupted attempt of THIS batch whose curated unit was not staged
            self._build_curated_only(spark, ok, plan, run, curated, unit)
            action = "curated_only"
        else:
            action = "reused"

        result, applied = self._publish(man, batch_id, min_ttl)
        final = read_manifest(ctx.session, ctx.run_id, feed, sid, serial=True) or man
        self._audit_duplicates(plan, final, redelivery, batch_id)
        return SnapshotResult(feed, sid, man["generation_id"], action, result, redelivery,
                              plan.duplicates_in_batch + int(redelivery), applied, final,
                              in_batch_duplicates=plan.duplicates_in_batch)

    def _snap_frame(self, ok: DataFrame, man: Mapping[str, Any]) -> DataFrame:
        """ONE-row ``snap`` (PARSED_SNAPSHOT_COLUMNS) with lineage columns pinned to the frozen manifest."""
        overrides: dict[str, Column] = {}
        for c in ("capture_id", "raw_path", "raw_sha256", "kafka_topic", "source_order"):
            if man.get(c) is not None:
                overrides[c] = F.lit(str(man[c]))
        for c in ("original_fetched_at", "published_at"):
            if man.get(c) is not None:
                overrides[c] = _ts_lit(man[c])
        if man.get("kafka_partition") is not None:
            overrides["kafka_partition"] = F.lit(int(man["kafka_partition"])).cast("int")
        if man.get("kafka_offset") is not None:
            overrides["kafka_offset"] = F.lit(int(man["kafka_offset"])).cast("long")
        base = ok.where(
            (F.col("feed_type") == F.lit(man["feed_type"])) & (F.col("snapshot_id") == F.lit(man["snapshot_id"]))
            & (F.col("delivery_rank") == 1)
        )
        return base.select(*[overrides[c].alias(c) if c in overrides else F.col(c) for c in PARSED_SNAPSHOT_COLUMNS])

    def _load_parent(self, spark, man: Mapping[str, Any], run: RunInfo) -> tuple[DataFrame | None, bool, str | None]:
        """Parent rows for inheritance (bounded reads). Returns (frame, parent_available, reason).

        Availability is FROZEN in the manifest at first materialization (``parent_available``) and reused on
        every retry, so a retry builds the same generation (D-B-06):
          * frozen unavailable -> never read; PARENT_UNAVAILABLE, no inheritance;
          * frozen available -> rows are read WITHOUT re-checking the wall clock (under the sink TTL rule the
            rows live until ``ceil(expires_at)``). The read-bound overflow is a deterministic property of the
            immutable parent and yields PARENT_UNAVAILABLE identically on every attempt. Genuinely missing
            data (parent manifest not READY, fewer/more rows than its ``expected_counts``) raises
            :class:`ParentUnavailable`: the batch fails and is retried, it is never silently built without
            inheritance.
        """
        ctx = self.ctx
        feed = man["feed_type"]
        psid = man.get("parent_snapshot_id")
        table = PARENT_TABLE.get(feed)
        if not psid or table is None:
            return None, True, None
        if not frozen_parent_available(man):
            return None, False, "frozen_unavailable"
        got = self._read_parent(man, run, table)
        if isinstance(got, str):
            if got in DETERMINISTIC_PARENT_REASONS:
                log_event(_log, "join", "skipped", f"parent rows exceed the read bounds ({got}); building "
                          "without inheritance", level=logging.WARNING, feed=feed, snapshot_id=man["snapshot_id"],
                          parent_snapshot_id=psid)
                return None, False, got
            raise ParentUnavailable(
                f"{feed} snapshot {man['snapshot_id']}: its frozen parent generation {psid} is no longer readable "
                f"({got}). The batch fails and will be retried; if the parent rows are permanently lost "
                "(e.g. its TTL elapsed while the processor was down), replay the recording into a NEW run "
                "(runbook: demo.py start with a fresh run) instead of restarting this one.")
        return rows_to_frame(spark, got, ctx.schemas[table]), True, None

    def _read_parent(self, man: Mapping[str, Any], run: RunInfo, table: str) -> list[dict] | str:
        """Bounded parent row read; returns the rows or a reason string (no wall-clock expiry check)."""
        ctx = self.ctx
        psid = man["parent_snapshot_id"]
        pman = read_manifest(ctx.session, ctx.run_id, man["feed_type"], psid, serial=True)
        if pman is None or pman.get("status") != READY:
            return "parent_not_ready"
        expected = pman.get("expected_counts") or {}
        prefix = f"{table}/"
        routes = sorted(set(run.route_ids) | {k[len(prefix):] for k in expected if k.startswith(prefix)})
        per_route_limit = int(ctx.cfg.cassandra.parent_read_limit_per_route)
        total_limit = int(ctx.cfg.cassandra.parent_read_total_limit)
        cql = f"SELECT * FROM {table} WHERE run_id = ? AND snapshot_id = ? AND route_id = ? LIMIT ?"
        rows: list[dict] = []
        for route in routes:
            got = [normalize_row(r) for r in execute(ctx.session, cql, (ctx.run_id, psid, route, per_route_limit + 1))]
            if len(got) > per_route_limit:
                return "parent_read_limit_per_route_exceeded"
            exp = expected.get(f"{prefix}{route}")
            if exp is not None and len(got) != int(exp):
                return "parent_rows_incomplete"
            rows.extend(got)  # type: ignore[arg-type]
            if len(rows) > total_limit:
                return "parent_read_total_limit_exceeded"
        return rows

    def _call_builder(self, spark, snap: DataFrame, params: BuildParams, run: RunInfo,
                      parent: DataFrame | None) -> GenerationOutput:
        feed = params.feed_type
        dims = self.ctx.dims
        if feed == "vehicle_positions":
            from transitpulse.processing.vehicles import build_vehicle_generation

            out = build_vehicle_generation(spark, snap, params, run, dims, parent)
        elif feed == "trip_updates":
            from transitpulse.processing.predictions import build_trip_update_generation

            out = build_trip_update_generation(spark, snap, params, run, dims)
        elif feed == "alerts":
            from transitpulse.processing.alerts import build_alert_generation

            out = build_alert_generation(spark, snap, params, run, dims, parent)
        else:
            raise InvalidGenerationOutput(f"unknown feed_type {feed!r}")
        if not isinstance(out, GenerationOutput) or out.stats is None:
            raise InvalidGenerationOutput(f"{feed} builder did not return a GenerationOutput with stats")
        unknown = sorted(set(out.tables) - set(TABLE_TTL_CLASS))
        if unknown:
            raise InvalidGenerationOutput(f"{feed} builder returned unknown tables {unknown}")
        return out

    def _build(self, spark, ok: DataFrame, man: Mapping[str, Any], run: RunInfo
               ) -> tuple[GenerationOutput, BuildParams, str | None]:
        parent, available, reason = self._load_parent(spark, man, run)
        if reason:
            log_event(_log, "join", "skipped", f"parent unavailable: {reason}", level=logging.WARNING,
                      feed=man["feed_type"], snapshot_id=man["snapshot_id"],
                      parent_snapshot_id=man.get("parent_snapshot_id"))
        params = params_from_manifest(man, parent_available=available)
        out = self._call_builder(spark, self._snap_frame(ok, man), params, run, parent)
        return out, params, reason

    def _stage_curated(self, curated: CuratedBatch, unit: str, out: GenerationOutput, man: Mapping[str, Any],
                       batch_id: int) -> None:
        cfg = self.ctx.cfg
        datasets: dict[str, DataFrame] = dict(out.curated or {})
        if "dlq" in datasets:
            raise InvalidGenerationOutput("builder curated outputs must not use the reserved name 'dlq'")
        if out.dlq is not None:
            datasets["dlq"] = dlq.conform_dlq(out.dlq, max_excerpt_chars=cfg.kafka.dlq_excerpt_max_chars,
                                              batch_id=batch_id, pipeline_version=self.ctx.pipeline_version)
        curated.write_unit(unit, datasets, info={"feed_type": man["feed_type"], "snapshot_id": man["snapshot_id"],
                                                 "generation_id": man["generation_id"]})
        log_event(_log, "parquet_commit", "ok", "curated unit staged", batch_id=batch_id, feed=man["feed_type"],
                  snapshot_id=man["snapshot_id"], datasets=sorted(datasets))

    def _build_curated_only(self, spark, ok: DataFrame, plan: SnapshotPlan, run: RunInfo, curated: CuratedBatch,
                            unit: str) -> None:
        out, _params, _reason = self._build(spark, ok, plan.manifest, run)
        self._stage_curated(curated, unit, out, plan.manifest, int(plan.manifest["first_batch_id"]))

    def _collect_stats(self, out: GenerationOutput, man: Mapping[str, Any]) -> dict[str, Any]:
        rows = out.stats.collect()
        if len(rows) != 1:
            raise InvalidGenerationOutput(f"stats must have exactly one row, got {len(rows)}")
        st = rows[0].asDict()
        missing = [c for c in STATS_COLUMNS if c not in st]
        if missing:
            raise InvalidGenerationOutput(f"stats is missing {missing}")
        if not st.get("accounting_ok"):
            raise AccountingMismatch(f"snapshot {man['snapshot_id']} ({man['feed_type']}): accounting identities "
                                     f"violated: {st.get('accounting_violations')}")
        env_sel = man.get("selected_entity_count")
        if env_sel is not None and st.get("selected_entity_count") is not None \
                and int(st["selected_entity_count"]) != int(env_sel):
            raise AccountingMismatch(f"snapshot {man['snapshot_id']}: builder selected_entity_count "
                                     f"{st['selected_entity_count']} != envelope {env_sel}")
        return st

    def _generation_tables(self, spark, out: GenerationOutput, params: BuildParams, parent_reason: str | None,
                           *, history_only: bool) -> dict[str, DataFrame]:
        schemas = self.ctx.schemas
        tables = {t: conform_table(df, t, schemas[t]) for t, df in out.tables.items() if df is not None}
        extra: list[dict] = []
        if params.parent_snapshot_id and not params.parent_available:
            extra.append(quality_row(params.run_id, params.feed_type, params.snapshot_id, "PARENT_UNAVAILABLE",
                                     sample_refs=[params.parent_snapshot_id],
                                     details={"parent_snapshot_id": params.parent_snapshot_id,
                                              "reason": parent_reason}))
        if history_only:
            extra.append(quality_row(params.run_id, params.feed_type, params.snapshot_id, "EXPIRED_NOT_PUBLISHED",
                                     details={"expires_at": iso_utc(params.expires_at),
                                              "detail": "generation expired before it could be published; "
                                                        "history rows written only"}))
        if extra:
            q_extra = conform_table(quality_frame(spark, extra, schemas[QUALITY_TABLE]), QUALITY_TABLE,
                                    schemas[QUALITY_TABLE])
            if QUALITY_TABLE in tables:
                builder_q = tables[QUALITY_TABLE]
                q_extra = q_extra.join(builder_q.select("reason_code").distinct(), on="reason_code", how="left_anti")
                tables[QUALITY_TABLE] = builder_q.unionByName(q_extra, allowMissingColumns=True)
            else:
                tables[QUALITY_TABLE] = q_extra
        if history_only:
            tables = {t: df for t, df in tables.items() if TABLE_TTL_CLASS[t] == "history"}
        return tables

    def _write(self, spark, tables: Mapping[str, DataFrame], man: Mapping[str, Any],
               fault: Mapping[str, Any] | None) -> dict[str, int]:
        if not tables:
            return {}
        ctx = self.ctx
        plan: dict[str, dict] = {}
        rdds = []
        for t, df in sorted(tables.items()):
            exp = man["expires_at"] if TABLE_TTL_CLASS[t] == "serving" else man["history_expires_at"]
            plan[t] = sink.table_plan(t, df.columns, ctx.schemas[t].columns, epoch_seconds(exp))
            rdds.append(df.rdd.map(lambda r, _t=t: (_t, tuple(r))))
        rdd = rdds[0] if len(rdds) == 1 else spark.sparkContext.union(rdds)
        writer = sink.make_partition_writer(self.sink_conf, plan, fault)
        try:
            res = rdd.mapPartitions(writer).collect()
        except Exception:
            ctx.metrics.cassandra_write_errors.labels(table=",".join(sorted(tables))).inc()
            raise
        written: dict[str, int] = {}
        secs: dict[str, float] = {}
        for t, n, s in res:
            written[t] = written.get(t, 0) + int(n)
            secs[t] = secs.get(t, 0.0) + float(s)
        for t, s in secs.items():
            ctx.metrics.cassandra_write_seconds.labels(table=t).observe(s)
        return written

    def _build_and_write(self, spark, ok: DataFrame, plan: SnapshotPlan, run: RunInfo, batch_id: int,
                         curated: CuratedBatch, unit: str, need_curated: bool, *, history_only: bool) -> dict:
        ctx = self.ctx
        man = plan.manifest
        feed, sid = plan.feed_type, plan.snapshot_id
        out, params, parent_reason = self._build(spark, ok, man, run)
        stats = self._collect_stats(out, man)
        if need_curated:
            self._stage_curated(curated, unit, out, man, batch_id)
        tables = self._generation_tables(spark, out, params, parent_reason, history_only=history_only)
        if history_only and remaining_s(man["history_expires_at"]) < 1:
            log_event(_log, "cassandra_write", "skipped", "history TTL already elapsed", batch_id=batch_id,
                      feed=feed, snapshot_id=sid)
            return man
        persisted = {t: df.persist(_SL) for t, df in tables.items()}
        try:
            counts = compute_generation_counts(persisted, ctx.schemas, run.route_ids)
            group1 = {t: df for t, df in persisted.items() if t not in SECOND_WRITE_GROUP}
            group2 = {t: df for t, df in persisted.items() if t in SECOND_WRITE_GROUP}
            fault = ctx.faults.take_executor_fault(feed_type=feed, snapshot_id=sid)
            written = self._write(spark, group1, man, fault if group1 else None)
            ctx.faults.check(AFTER_PARTIAL_WRITE, feed_type=feed, snapshot_id=sid)
            for t, n in self._write(spark, group2, man, None if group1 else fault).items():
                written[t] = written.get(t, 0) + n
        finally:
            for df in persisted.values():
                try:
                    df.unpersist()
                except Exception:  # noqa: BLE001 - cleanup only
                    pass
        expected_totals = {t: counts.expected_counts.get(t, 0) for t in tables}
        written_totals = {t: written.get(t, 0) for t in tables}
        if written_totals != expected_totals:
            raise CountMismatch(sid, expected_totals, written_totals)
        log_event(_log, "cassandra_write", "ok", batch_id=batch_id, feed=feed, snapshot_id=sid,
                  rows=written_totals, history_only=history_only)
        acc = accounting_metrics(stats.get("accounting_json"))
        for k, v in acc["candidates"].items():
            ctx.metrics.normalized_candidates.labels(feed=feed, kind=k).inc(v)
        for k, v in acc["unique"].items():
            ctx.metrics.unique_records.labels(kind=k).inc(v)
        for k, v in acc["duplicate"].items():
            ctx.metrics.duplicate_records.labels(kind=k).inc(v)
        if int(stats.get("rejected_entity_count") or 0):
            ctx.metrics.invalid_records.labels(reason="entity_rejected").inc(int(stats["rejected_entity_count"]))
        if history_only:
            return man
        ready_counts = {c: stats.get(c) for c in ("selected_entity_count", "fully_valid_entity_count",
                                                  "partially_valid_entity_count", "rejected_entity_count",
                                                  "is_partial", "is_empty")}
        applied = mark_ready(ctx.session, (ctx.run_id, feed, sid), ready_at=ms_floor(utcnow()),
                             expected_counts=counts.expected_counts, content_checksum=counts.content_checksum,
                             accounting_json=str(stats.get("accounting_json") or "{}"), counts=ready_counts)
        final = read_manifest(ctx.session, ctx.run_id, feed, sid, serial=True)
        if final is None or final.get("status") != READY:
            raise ManifestConflict(f"snapshot {sid}: READY transition not applied and manifest is "
                                   f"{None if final is None else final.get('status')}")
        log_event(_log, "manifest_ready", "ok" if applied else "reused", batch_id=batch_id, feed=feed,
                  snapshot_id=sid, generation_id=final.get("generation_id"))
        return final

    # ---------------------------------------------------------------- publish (CAS loop)
    def _publish(self, man: Mapping[str, Any], batch_id: int, min_ttl: int) -> tuple[str, bool]:
        ctx = self.ctx
        feed, sid = man["feed_type"], man["snapshot_id"]
        key = (ctx.run_id, feed, sid)
        attempts = int(ctx.cfg.storage.pointer_cas_max_attempts)
        for attempt in range(1, attempts + 1):
            ptr = read_pointer(ctx.session, ctx.run_id, feed, serial=True)
            if ptr is not None and ptr["snapshot_id"] == sid:
                vpa = ptr.get("view_published_at")
                lat = latency_ms(vpa, man.get("published_at"))
                applied = set_outcome_once(ctx.session, key, OUTCOME_PUBLISHED, vpa, lat)
                if applied and lat is not None:
                    ctx.metrics.visibility_latency.labels(feed=feed).observe(lat / 1000.0)
                ctx.metrics.generation_publish.labels(feed=feed, result=RESULT_REUSED).inc()
                log_event(_log, "pointer_cas", "reused", "pointer already at this generation", batch_id=batch_id,
                          feed=feed, snapshot_id=sid, outcome_recorded=applied)
                return RESULT_REUSED, applied
            if ptr is not None and str(ptr["source_order"]) > str(man["source_order"]):
                self._audit_quality(man, "LATE_SNAPSHOT_NOT_PUBLISHED", details={
                    "pointer_snapshot_id": ptr["snapshot_id"], "pointer_source_order": ptr["source_order"],
                    "snapshot_source_order": man["source_order"], "batch_id": batch_id,
                    "previously_published": man.get("publish_outcome") == OUTCOME_PUBLISHED,
                    "built_as_late": bool(man.get("built_as_late")),
                })
                applied = set_outcome_once(ctx.session, key, OUTCOME_LATE)
                ctx.metrics.generation_publish.labels(feed=feed, result=RESULT_LATE).inc()
                log_event(_log, "pointer_cas", "skipped", "late snapshot not published", batch_id=batch_id,
                          feed=feed, snapshot_id=sid, pointer_snapshot_id=ptr["snapshot_id"])
                return RESULT_LATE, applied
            now = ms_floor(utcnow())
            if remaining_s(man["expires_at"], now) < min_ttl:
                self._audit_quality(man, "EXPIRED_NOT_PUBLISHED", details={
                    "expires_at": iso_utc(man["expires_at"]), "checked_at": iso_utc(now), "batch_id": batch_id})
                applied = set_outcome_once(ctx.session, key, OUTCOME_EXPIRED)
                ctx.metrics.generation_publish.labels(feed=feed, result=RESULT_EXPIRED).inc()
                return RESULT_EXPIRED, applied
            row = pointer_row(man, now)
            if ptr is None:
                applied_ptr, _existing = bootstrap_pointer(ctx.session, row)
            else:
                applied_ptr = cas_pointer(ctx.session, row, ptr["snapshot_id"])
            if applied_ptr:
                lat = latency_ms(now, man.get("published_at"))
                applied = set_outcome_once(ctx.session, key, OUTCOME_PUBLISHED, now, lat)
                if applied and lat is not None:
                    ctx.metrics.visibility_latency.labels(feed=feed).observe(lat / 1000.0)
                ctx.metrics.generation_publish.labels(feed=feed, result=RESULT_PUBLISHED).inc()
                log_event(_log, "pointer_cas", "ok", "generation published", batch_id=batch_id, feed=feed,
                          snapshot_id=sid, previous_snapshot_id=None if ptr is None else ptr["snapshot_id"],
                          visibility_latency_ms=lat, attempt=attempt)
                return RESULT_PUBLISHED, applied
            log_event(_log, "pointer_cas", "retry", "pointer changed concurrently; re-reading", batch_id=batch_id,
                      feed=feed, snapshot_id=sid, attempt=attempt)
        raise PointerContention(f"pointer CAS for {feed}/{sid} failed {attempts} times")

    # ---------------------------------------------------------------- audit quality rows
    def _quality_ttl(self, man: Mapping[str, Any]) -> int | None:
        hist = man.get("history_expires_at")
        if hist is None:
            return None
        try:
            return sink.ttl_seconds(epoch_seconds(hist))
        except sink.GenerationExpiredError:
            return None

    def _upsert_quality(self, row: Mapping[str, Any], ttl: int) -> None:
        cols = list(QUALITY_COLUMNS)
        cql = (f"INSERT INTO {QUALITY_TABLE} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)}) "
               "USING TTL ?")
        vals = [row.get(c) for c in cols]
        vals[cols.index("disposition_counts")] = vals[cols.index("disposition_counts")] or None
        vals[cols.index("sample_refs")] = vals[cols.index("sample_refs")] or None
        execute(self.ctx.session, cql, list(unset_if_none(vals)) + [ttl], idempotent=True)

    def _audit_quality(self, man: Mapping[str, Any], reason_code: str, *, details: Mapping[str, Any],
                       sample_refs: Sequence[str] = (), record_count: int = 1) -> None:
        """Generation-level audit row (LATE / EXPIRED / DUPLICATE) written outside the generation's counts."""
        ttl = self._quality_ttl(man)
        if ttl is None:
            log_event(_log, "dedup", "skipped", f"{reason_code}: history TTL elapsed; audit row not written",
                      feed=man["feed_type"], snapshot_id=man["snapshot_id"])
            return
        self._upsert_quality(quality_row(man["run_id"], man["feed_type"], man["snapshot_id"], reason_code,
                                         record_count=record_count, sample_refs=sample_refs, details=details), ttl)

    def _audit_duplicates(self, plan: SnapshotPlan, man: Mapping[str, Any], redelivery: bool, batch_id: int) -> None:
        refs = list(plan.duplicate_refs)
        if redelivery:
            m = plan.meta
            refs.append(f"{m.get('kafka_topic')}:{m.get('kafka_partition')}:{m.get('kafka_offset')}")
        if not refs:
            return
        ctx = self.ctx
        existing = normalize_row(execute(
            ctx.session,
            f"SELECT sample_refs, record_count FROM {QUALITY_TABLE} WHERE run_id = ? AND feed_type = ? "
            "AND snapshot_id = ? AND reason_code = ?",
            (ctx.run_id, plan.feed_type, plan.snapshot_id, "DUPLICATE_SNAPSHOT_DELIVERY"),
        ).one()) or {}
        merged = sorted(set(existing.get("sample_refs") or []) | set(refs))
        capped = len(merged) > DUPLICATE_REFS_CAP
        count = max(len(merged), int(existing.get("record_count") or 0)) if capped else len(merged)
        self._audit_quality(man, "DUPLICATE_SNAPSHOT_DELIVERY", record_count=count,
                            sample_refs=merged[:DUPLICATE_REFS_CAP],
                            details={"unit": "kafka_delivery", "last_batch_id": batch_id,
                                     "first_batch_id": man.get("first_batch_id"), "refs_capped": capped})

    # ---------------------------------------------------------------- F. curated batch commit
    def _publications_frame(self, spark, results: Sequence[SnapshotResult], batch_id: int) -> DataFrame:
        schema = T.StructType([
            T.StructField("run_id", T.StringType()), T.StructField("batch_id", T.LongType()),
            T.StructField("feed_type", T.StringType()), T.StructField("snapshot_id", T.StringType()),
            T.StructField("generation_id", T.StringType()), T.StructField("source_order", T.StringType()),
            T.StructField("action", T.StringType()), T.StructField("result", T.StringType()),
            T.StructField("publish_outcome", T.StringType()), T.StructField("status", T.StringType()),
            T.StructField("built_as_late", T.BooleanType()), T.StructField("redelivery", T.BooleanType()),
            T.StructField("duplicate_deliveries", T.IntegerType()), T.StructField("is_partial", T.BooleanType()),
            T.StructField("is_empty", T.BooleanType()), T.StructField("first_batch_id", T.LongType()),
            T.StructField("metrics_as_of", T.TimestampType()), T.StructField("feed_timestamp", T.TimestampType()),
            T.StructField("published_at", T.TimestampType()), T.StructField("first_visible_at", T.TimestampType()),
            T.StructField("visibility_latency_ms", T.LongType()), T.StructField("expires_at", T.TimestampType()),
            T.StructField("expected_counts_json", T.StringType()),
        ])
        rows = []
        for r in results:
            m = r.manifest
            rows.append((
                self.ctx.run_id, int(batch_id), r.feed_type, r.snapshot_id, r.generation_id, m.get("source_order"),
                r.action, r.result, m.get("publish_outcome"), m.get("status"), m.get("built_as_late"),
                r.redelivery, int(r.duplicates), m.get("is_partial"), m.get("is_empty"),
                None if m.get("first_batch_id") is None else int(m["first_batch_id"]),
                m.get("metrics_as_of"), m.get("feed_timestamp"), m.get("published_at"), m.get("first_visible_at"),
                None if m.get("visibility_latency_ms") is None else int(m["visibility_latency_ms"]),
                m.get("expires_at"), json.dumps(m.get("expected_counts") or {}, sort_keys=True),
            ))
        return spark.createDataFrame(rows, schema=schema)

    def _commit_curated(self, spark, curated: CuratedBatch, sp: DataFrame, ok: DataFrame | None,
                        rejects: DataFrame, results: Sequence[SnapshotResult], batch_id: int, accounting: dict,
                        *, has_snapshots: bool, has_rejects: bool) -> None:
        ctx, cfg = self.ctx, self.ctx.cfg
        datasets: dict[str, DataFrame] = {}
        if has_snapshots and ok is not None:
            datasets["snapshots"] = parse.deliveries_frame(sp, ok, batch_id=batch_id, run_id=ctx.run_id)
        if has_rejects:
            datasets["dlq"] = dlq.conform_dlq(rejects, max_excerpt_chars=cfg.kafka.dlq_excerpt_max_chars,
                                              batch_id=batch_id, pipeline_version=ctx.pipeline_version)
        if results:
            datasets["publications"] = self._publications_frame(spark, results, batch_id)
        if datasets:
            curated.write_unit(BATCH_UNIT, datasets, info={"batch_id": batch_id})
        dlq_files = [str(p) for p in curated.unit_files("dlq")]
        n_dlq = dlq.send_staged(spark, dlq_files, bootstrap_servers=cfg.kafka.bootstrap_servers,
                                topic=cfg.kafka.topics.dlq)
        if n_dlq:
            log_event(_log, "batch_commit", "ok", f"{n_dlq} DLQ record(s) sent", batch_id=batch_id)
        row_counts: dict[str, int] = {"dlq": n_dlq}
        for ds in curated.datasets():
            if ds in row_counts:
                continue
            files = [str(p) for p in curated.unit_files(ds)]
            row_counts[ds] = int(spark.read.parquet(*files).count()) if files else 0
        curated.commit(query_id=ctx.query_id, snapshot_ids=[r.snapshot_id for r in results], accounting=accounting,
                       row_counts=row_counts)
        log_event(_log, "parquet_commit", "ok", "curated batch committed", batch_id=batch_id, row_counts=row_counts)

    # ---------------------------------------------------------------- G. status
    def _update_status(self, results: Sequence[SnapshotResult], topic_counts: Mapping[str, int],
                       reject_counts: Sequence[Mapping[str, Any]], last_reject: Mapping[str, Mapping[str, Any]],
                       batch_id: int, now: datetime, duration_ms: int, n_rejected: int, total: int) -> None:
        ctx, pctx = self.ctx, self._pctx
        totals = lineage_totals

        def last_published(rs: Sequence[SnapshotResult]) -> SnapshotResult | None:
            pub = [r for r in rs if r.result in (RESULT_PUBLISHED, RESULT_REUSED)
                   and r.manifest.get("publish_outcome") == OUTCOME_PUBLISHED]
            return pub[-1] if pub else None

        progress_base = {"query_id": ctx.query_id, "batch_id": batch_id, "updated_at": iso_utc(now)}
        rej_by_feed: dict[str, int] = {}
        for r in reject_counts:
            rej_by_feed[r["feed_type"]] = rej_by_feed.get(r["feed_type"], 0) + int(r["count"])

        for feed, topic in sorted(pctx.feed_topics.items()):
            n_in = int(topic_counts.get(topic, 0))
            if not n_in:
                continue
            rs = [r for r in results if r.feed_type == feed]
            deltas = {"input_messages_total": n_in, "rejected_messages_total": rej_by_feed.get(feed, 0), **totals(rs)}
            sets: dict[str, Any] = {"status": "RUNNING", "last_success_at": now, "last_heartbeat_at": now,
                                    "last_batch_duration_ms": duration_ms,
                                    "progress_json": json.dumps({**progress_base, "snapshots": len(rs)})}
            if rs:
                sets["last_snapshot_id"] = rs[-1].snapshot_id
            lp = last_published(rs)
            if lp is not None:
                sets["last_published_snapshot_id"] = lp.snapshot_id
                if lp.manifest.get("visibility_latency_ms") is not None:
                    sets["last_visibility_latency_ms"] = int(lp.manifest["visibility_latency_ms"])
            lr = last_reject.get(feed)
            if lr:
                sets.update(last_rejected_at=now, last_rejected_reason=lr["reason_code"],
                            last_rejected_error_id=lr["error_id"])
            update_status_guarded(ctx.session, ctx.run_id, f"feed:{feed}", batch_id, deltas=deltas, sets=sets,
                                  query_id=ctx.query_id, now=now)

        deltas = {"input_messages_total": total, "rejected_messages_total": n_rejected, **totals(results)}
        sets = {"status": "RUNNING", "last_success_at": now, "last_heartbeat_at": now,
                "last_batch_duration_ms": duration_ms,
                "progress_json": json.dumps({**progress_base, "snapshots": len(results),
                                             "results": {k: sum(1 for r in results if r.result == k) for k in
                                                         (RESULT_PUBLISHED, RESULT_REUSED, RESULT_LATE, RESULT_EXPIRED)}})}
        if results:
            sets["last_snapshot_id"] = results[-1].snapshot_id
        lp = last_published(results)
        if lp is not None:
            sets["last_published_snapshot_id"] = lp.snapshot_id
            if lp.manifest.get("visibility_latency_ms") is not None:
                sets["last_visibility_latency_ms"] = int(lp.manifest["visibility_latency_ms"])
        # a successful batch means the processor is healthy again: clear a FAILED run's last_error
        update_status_guarded(ctx.session, ctx.run_id, "processor", batch_id, deltas=deltas, sets=sets,
                              query_id=ctx.query_id, now=now, clear=("last_error",))


# --------------------------------------------------------------------------------------------------
# Module-level entry point (interfaces §7: process_batch(df, batch_id))
# --------------------------------------------------------------------------------------------------

_PROCESSOR: dict[str, BatchProcessor] = {}


def configure(ctx: BatchContext) -> BatchProcessor:
    proc = BatchProcessor(ctx)
    _PROCESSOR["current"] = proc
    return proc


def process_batch(df: DataFrame, batch_id: int) -> None:
    """``foreachBatch`` function of the configured run (call :func:`configure` first)."""
    proc = _PROCESSOR.get("current")
    if proc is None:
        raise RuntimeError("processing.batch.configure(ctx) must be called before process_batch")
    proc.process_batch(df, batch_id)


def bootstrap_status_rows(session, run_id: str, feeds: Iterable[str], *, now: datetime) -> None:
    """Create the LWT status rows (``processor`` and ``feed:<ft>``) before the stream starts."""
    for comp in ["processor", *[f"feed:{f}" for f in feeds]]:
        bootstrap_status(session, run_id, comp, now=now)
