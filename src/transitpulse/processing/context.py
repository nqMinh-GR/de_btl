"""Frozen data passed between the orchestration layer (batch.py, WP4) and the domain transformations
(vehicles.py / predictions.py / alerts.py, WP5).

Contract summary (see docs/interfaces.md §Processing):
  * WP4 freezes :class:`BuildParams` in snapshot_manifest at the FIRST materialization of a snapshot and
    reuses them on every retry (parent, metrics_as_of, expiry never change).
  * WP5 builders receive ONE snapshot (``snap``: a one-row DataFrame with PARSED_SNAPSHOT columns) and
    return a :class:`GenerationOutput` whose ``tables`` DataFrames have column names identical to the CQL
    columns of the target table (a subset is allowed; unspecified columns stay unset).
  * WP4 converts TimestampType columns to epoch-ms before the Python writer, applies TTL, counts writes and
    publishes the generation. WP5 never writes to Cassandra or Kafka.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from transitpulse.contracts.policy import Policy

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

# Columns of the one-row-per-snapshot DataFrame produced by processing/parse.py (WP4) and consumed by WP5.
PARSED_SNAPSHOT_COLUMNS: tuple[str, ...] = (
    "run_id",                 # string
    "scope_id",               # string
    "source_type",            # string
    "scenario_id",            # string (nullable)
    "feed_type",              # string
    "snapshot_id",            # string
    "capture_id",             # string
    "gtfs_version",           # string
    "feed_timestamp",         # long (epoch seconds, header.timestamp)
    "feed_ts",                # timestamp (= feed_timestamp)
    "original_fetched_at",    # timestamp
    "published_at",           # timestamp (this delivery's enqueue time)
    "raw_path",               # string
    "raw_sha256",             # string
    "source_entity_count",    # int
    "selected_entity_count",  # int
    "header",                 # struct<gtfs_realtime_version, incrementality, timestamp:long>
    "entities",               # array<string>  (raw JSON text of each selected entity, source order)
    "kafka_topic",            # string
    "kafka_partition",        # int
    "kafka_offset",           # long
    "source_order",           # string (contracts.ids.source_order format)
)

# Exactly one row; columns produced by every WP5 builder in GenerationOutput.stats.
STATS_COLUMNS: tuple[str, ...] = (
    "selected_entity_count",         # int
    "fully_valid_entity_count",      # int
    "partially_valid_entity_count",  # int
    "rejected_entity_count",         # int
    "is_partial",                    # boolean (rejected_entity_count > 0)
    "is_empty",                      # boolean (selected_entity_count == 0)
    "accounting_json",               # string: feed-specific accounting (json object, units explicit)
    "accounting_ok",                 # boolean: all feed identities hold
    "accounting_violations",         # string (nullable): human-readable list when accounting_ok is false
)


@dataclass(frozen=True)
class RunInfo:
    """Snapshot of run_metadata read on the driver at the start of each micro-batch (after control)."""

    run_id: str
    project_id: str
    scope_id: str
    source_type: str
    clock_mode: str                 # wall | source
    gtfs_version: str
    agency_timezone: str
    route_ids: tuple[str, ...]
    pipeline_version: str
    policy: Policy
    application_clock: datetime | None   # None for wall-clock runs
    clock_sequence: int
    run_state: str
    scenario_id: str | None = None


@dataclass(frozen=True)
class BuildParams:
    """Generation parameters frozen in snapshot_manifest at first materialization (reused on retry)."""

    run_id: str
    feed_type: str
    snapshot_id: str
    generation_id: str
    source_order: str
    feed_timestamp: datetime
    original_fetched_at: datetime
    published_at: datetime
    capture_id: str
    raw_path: str
    raw_sha256: str
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    first_batch_id: int
    metrics_as_of: datetime          # run clock (source) or wall now (live) at first materialization
    clock_sequence_at_build: int
    parent_snapshot_id: str | None   # pointer (or previous in-batch snapshot) at first materialization
    parent_generation_id: str | None
    built_as_late: bool
    first_materialized_at: datetime
    expires_at: datetime             # first_materialized_at + current_generation_ttl
    history_expires_at: datetime     # first_materialized_at + history_ttl
    gtfs_version: str
    scope_id: str
    scenario_id: str | None = None
    parent_available: bool = True    # False when parent_snapshot_id is set but its rows/manifest are unusable


@dataclass
class GenerationOutput:
    """What a WP5 builder returns for one snapshot (all DataFrames are lazy)."""

    tables: dict[str, DataFrame]                 # CQL table name -> rows (columns == CQL column names)
    stats: DataFrame                             # exactly one row with STATS_COLUMNS
    curated: dict[str, DataFrame] = field(default_factory=dict)  # observations|predictions|alerts|entity_quality
    dlq: DataFrame | None = None                 # entity-level DLQ rows (columns == contracts.dlq.DLQ_FIELDS)
    notes: dict[str, Any] = field(default_factory=dict)


# Which serving tables count per partition in snapshot_manifest.expected_counts ("<table>/<key>").
PER_PARTITION_COUNT_KEYS: dict[str, str] = {
    "vehicles_by_route_snapshot": "route_id",
    "alerts_by_route_snapshot": "route_id",
    "predictions_by_stop_snapshot": "stop_id",
}

# TTL class per table written by the sink: "serving" -> expires_at, "history" -> history_expires_at.
TABLE_TTL_CLASS: dict[str, str] = {
    "vehicles_by_route_snapshot": "serving",
    "predictions_by_stop_snapshot": "serving",
    "route_metrics_by_snapshot": "serving",
    "alerts_by_route_snapshot": "serving",
    "vehicle_observations_by_day": "history",
    "quality_by_snapshot": "history",
}
