"""``snapshot_manifest`` (WP4; interfaces §6, design B §1/§4, decisions D-PUB-01..03).

Lifecycle (LWT-only row, no TTL):
  * ``insert_building``  ``INSERT ... IF NOT EXISTS`` with ``status='BUILDING'`` and the FROZEN generation
    parameters (parent, ``parent_available``, metrics_as_of, expires_at, history_expires_at, source_order,
    lineage). A losing writer adopts the existing row (the winner's frozen parameters).
    ``parent_available`` is NULL when the generation has no parent.
  * ``mark_ready``       ``UPDATE ... SET status='READY', ... IF status = 'BUILDING'``.
  * ``set_outcome_once`` ``UPDATE ... SET publish_outcome=... IF publish_outcome = null`` (one-shot; the
    first visibility time/latency is never rewritten).
All reads that drive a protocol decision use ``serial=True``.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from transitpulse.contracts.enums import GenerationStatus, PublishOutcome
from transitpulse.storage.cassandra import (
    execute,
    lwt_existing_row,
    normalize_row,
    to_utc,
    unset_if_none,
    was_applied,
)

MANIFEST_COLUMNS: tuple[str, ...] = (
    "run_id", "feed_type", "snapshot_id", "generation_id", "status", "source_order", "feed_timestamp",
    "original_fetched_at", "published_at", "capture_id", "kafka_topic", "kafka_partition", "kafka_offset",
    "first_batch_id", "raw_path", "raw_sha256", "gtfs_version", "scope_id", "scenario_id",
    "parent_snapshot_id", "parent_generation_id", "parent_available", "built_as_late", "metrics_as_of",
    "clock_sequence_at_build",
    "first_materialized_at", "expires_at", "history_expires_at", "source_entity_count", "selected_entity_count",
    "fully_valid_entity_count", "partially_valid_entity_count", "rejected_entity_count", "is_partial",
    "is_empty", "ready_at", "expected_counts", "content_checksum", "accounting_json", "publish_outcome",
    "first_visible_at", "visibility_latency_ms",
)
# Columns that may be written by insert_building (everything frozen at first materialization).
BUILDING_COLUMNS: tuple[str, ...] = MANIFEST_COLUMNS[:MANIFEST_COLUMNS.index("fully_valid_entity_count")] + (
    "fully_valid_entity_count", "partially_valid_entity_count", "rejected_entity_count", "is_partial", "is_empty",
)
REQUIRED_BUILDING: tuple[str, ...] = (
    "run_id", "feed_type", "snapshot_id", "generation_id", "source_order", "first_batch_id", "metrics_as_of",
    "first_materialized_at", "expires_at", "history_expires_at",
)
# Columns mark_ready may set from the builder statistics.
READY_COUNT_COLUMNS: tuple[str, ...] = (
    "source_entity_count", "selected_entity_count", "fully_valid_entity_count",
    "partially_valid_entity_count", "rejected_entity_count", "is_partial", "is_empty",
)

SELECT_MANIFEST = "SELECT * FROM snapshot_manifest WHERE run_id = ? AND feed_type = ? AND snapshot_id = ?"
_KEY_WHERE = "WHERE run_id = ? AND feed_type = ? AND snapshot_id = ?"
SET_OUTCOME = (
    "UPDATE snapshot_manifest SET publish_outcome = ?, first_visible_at = ?, visibility_latency_ms = ? "
    f"{_KEY_WHERE} IF publish_outcome = null"
)

Key = tuple[str, str, str]   # (run_id, feed_type, snapshot_id)


def _key(key: Key) -> tuple[str, str, str]:
    if len(key) != 3 or not all(isinstance(k, str) and k for k in key):
        raise ValueError(f"manifest key must be (run_id, feed_type, snapshot_id), got {key!r}")
    return key[0], key[1], key[2]


def read_manifest(session, run_id: str, feed_type: str, snapshot_id: str, *, serial: bool = False) -> dict | None:
    return normalize_row(execute(session, SELECT_MANIFEST, (run_id, feed_type, snapshot_id), serial=serial).one())


def insert_building(session, row: dict) -> tuple[bool, dict]:
    """Create the BUILDING manifest with frozen parameters; ``(applied, effective_row)``.

    When not applied the effective row is the EXISTING manifest (its frozen parameters win).
    """
    unknown = sorted(set(row) - set(BUILDING_COLUMNS))
    if unknown:
        raise ValueError(f"insert_building got columns that are not frozen-at-build: {unknown}")
    missing = [c for c in REQUIRED_BUILDING if row.get(c) is None]
    if missing:
        raise ValueError(f"insert_building requires {missing}")
    data = dict(row)
    data["status"] = str(GenerationStatus.BUILDING)
    cols = [c for c in BUILDING_COLUMNS if c in data]
    cql = (
        f"INSERT INTO snapshot_manifest ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)}) "
        "IF NOT EXISTS"
    )
    rs = execute(session, cql, unset_if_none([data[c] for c in cols]))
    if was_applied(rs):
        return True, normalize_row(data)  # type: ignore[return-value]
    existing = lwt_existing_row(rs)
    if existing is None:  # pragma: no cover - Cassandra always returns the existing row
        raise RuntimeError(f"INSERT IF NOT EXISTS not applied but no existing row returned for {row['snapshot_id']}")
    return False, existing


def mark_ready(
    session,
    key: Key,
    *,
    ready_at: datetime,
    expected_counts: Mapping[str, int],
    content_checksum: str,
    accounting_json: str,
    counts: Mapping[str, Any],
) -> bool:
    """BUILDING -> READY (``IF status = 'BUILDING'``). Returns ``rs.was_applied``."""
    run_id, feed_type, snapshot_id = _key(key)
    unknown = sorted(set(counts) - set(READY_COUNT_COLUMNS))
    if unknown:
        raise ValueError(f"mark_ready counts has unknown columns {unknown}")
    count_cols = [c for c in READY_COUNT_COLUMNS if c in counts]
    sets = [f"status = '{GenerationStatus.READY}'", "ready_at = ?", "expected_counts = ?", "content_checksum = ?",
            "accounting_json = ?"]
    sets += [f"{c} = ?" for c in count_cols]
    cql = f"UPDATE snapshot_manifest SET {', '.join(sets)} {_KEY_WHERE} IF status = '{GenerationStatus.BUILDING}'"
    params: list[Any] = [
        to_utc(ready_at), {str(k): int(v) for k, v in expected_counts.items()}, content_checksum, accounting_json,
    ]
    params += list(unset_if_none([counts[c] for c in count_cols]))
    params += [run_id, feed_type, snapshot_id]
    return was_applied(execute(session, cql, params))


def set_outcome_once(
    session,
    key: Key,
    outcome: str,
    first_visible_at: datetime | None = None,
    visibility_latency_ms: int | None = None,
) -> bool:
    """Record the publish outcome once (``IF publish_outcome = null``)."""
    run_id, feed_type, snapshot_id = _key(key)
    valid = {str(o) for o in PublishOutcome}
    if outcome not in valid:
        raise ValueError(f"unknown publish outcome {outcome!r}; expected one of {sorted(valid)}")
    params = list(unset_if_none([
        outcome, to_utc(first_visible_at), None if visibility_latency_ms is None else int(visibility_latency_ms),
    ])) + [run_id, feed_type, snapshot_id]
    return was_applied(execute(session, SET_OUTCOME, params))
