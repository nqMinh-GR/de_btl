"""Kafka snapshot envelope (schema_version 1): one scope-filtered feed snapshot per Kafka message.

Key: ``f"{run_id}|{feed_type}"``. Headers: ``schema_version``, ``snapshot_id``, ``capture_id``.
Serialized size (UTF-8 compact JSON) must be <= ``kafka.max_snapshot_bytes`` (4 MiB); larger
snapshots are never truncated: raw is kept and the capture is rejected with PAYLOAD_TOO_LARGE.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, TypedDict

SCHEMA_VERSION = 1
MESSAGE_TYPE_SNAPSHOT = "feed_snapshot"
PUBLISHED_AT_PLACEHOLDER = "0000-00-00T00:00:00.000Z"  # same width as a real ISO-ms timestamp


class HttpMeta(TypedDict, total=False):
    status: int | None
    etag: str | None
    last_modified: str | None
    content_type: str | None


class Envelope(TypedDict, total=False):
    schema_version: int
    message_type: str                 # "feed_snapshot"
    run_id: str
    scope_id: str
    source_type: str                  # live | replay | synthetic
    scenario_id: str | None
    feed_type: str                    # vehicle_positions | trip_updates | alerts
    snapshot_id: str
    capture_id: str
    source_url: str
    gtfs_version: str
    feed_timestamp: int               # header.timestamp, UTC epoch seconds
    original_fetched_at: str          # ISO-8601 UTC with ms and Z (kept unchanged by replay)
    published_at: str                 # ISO-8601 UTC, wall clock of THIS enqueue
    raw_sha256: str                   # sha256 of the decoded source body
    raw_path: str                     # POSIX path relative to RAW_ROOT
    raw_bytes: int
    http: HttpMeta
    source_entity_count: int
    selected_entity_count: int
    scope_accounting: dict[str, Any]
    header: dict[str, Any]            # original GTFS-RT header object
    entities: list[dict[str, Any]]    # original selected entity objects, unchanged, source order
    producer: dict[str, str]          # {component, version}


def iso_utc(dt: datetime) -> str:
    """ISO-8601 UTC with millisecond precision and 'Z' (API and envelope convention)."""
    if dt.tzinfo is None:
        raise ValueError("naive datetime is not allowed; use timezone-aware UTC datetimes")
    return dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso_utc(s: str) -> datetime:
    """Parse ISO-8601 (with Z or offset) into an aware UTC datetime. Naive strings are rejected."""
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"naive datetime string {s!r}")
    return dt.astimezone(UTC)


def utcnow() -> datetime:
    return datetime.now(UTC)


def kafka_key(run_id: str, feed_type: str) -> bytes:
    return f"{run_id}|{feed_type}".encode()


def control_key(run_id: str) -> bytes:
    return f"{run_id}|control".encode()


def build_envelope(
    *,
    run_id: str,
    scope_id: str,
    source_type: str,
    scenario_id: str | None,
    feed_type: str,
    snapshot_id: str,
    capture_id: str,
    source_url: str,
    gtfs_version: str,
    header: dict[str, Any],
    entities: list[dict[str, Any]],
    original_fetched_at: datetime,
    raw_sha256: str,
    raw_path: str,
    raw_bytes: int,
    http: HttpMeta | None,
    source_entity_count: int,
    scope_accounting: dict[str, Any],
    producer_component: str,
    producer_version: str,
) -> Envelope:
    """Build an envelope WITHOUT published_at (set at send time by :func:`serialize_envelope`)."""
    ts = header.get("timestamp")
    if not isinstance(ts, int) or isinstance(ts, bool):
        raise ValueError("header.timestamp must be integer epoch seconds")
    return Envelope(
        schema_version=SCHEMA_VERSION,
        message_type=MESSAGE_TYPE_SNAPSHOT,
        run_id=run_id,
        scope_id=scope_id,
        source_type=source_type,
        scenario_id=scenario_id,
        feed_type=feed_type,
        snapshot_id=snapshot_id,
        capture_id=capture_id,
        source_url=source_url,
        gtfs_version=gtfs_version,
        feed_timestamp=ts,
        original_fetched_at=iso_utc(original_fetched_at),
        raw_sha256=raw_sha256,
        raw_path=raw_path,
        raw_bytes=raw_bytes,
        http=http or HttpMeta(),
        source_entity_count=source_entity_count,
        selected_entity_count=len(entities),
        scope_accounting=scope_accounting,
        header=header,
        entities=entities,
        producer={"component": producer_component, "version": producer_version},
    )


def serialize_envelope(env: Envelope, published_at: datetime | str) -> bytes:
    """Compact UTF-8 JSON with published_at set. Field order is not significant."""
    body = dict(env)
    body["published_at"] = published_at if isinstance(published_at, str) else iso_utc(published_at)
    return json.dumps(body, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def envelope_size(env: Envelope) -> int:
    """Serialized size using a fixed-width published_at placeholder (pre-send size check)."""
    return len(serialize_envelope(env, PUBLISHED_AT_PLACEHOLDER))


def kafka_headers(env: Envelope) -> list[tuple[str, bytes]]:
    return [
        ("schema_version", str(env["schema_version"]).encode()),
        ("snapshot_id", env["snapshot_id"].encode()),
        ("capture_id", env["capture_id"].encode()),
    ]
