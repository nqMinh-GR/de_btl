"""Dead-letter records on ``mbta.dlq.v1`` (key ``run_id|feed_type``). Consumers dedupe on error_id.

DLQ never carries an unbounded body: ``excerpt`` is capped (kafka.dlq_excerpt_max_chars) and
``raw_path``/``raw_sha256`` reference the full raw capture.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from transitpulse.contracts.enums import reason as reason_info
from transitpulse.contracts.envelope import iso_utc, utcnow

SCHEMA_VERSION = 1

DLQ_FIELDS: tuple[str, ...] = (
    "schema_version", "error_id", "run_id", "feed_type", "source_topic", "source_partition", "source_offset",
    "value_sha256", "snapshot_id", "capture_id", "entity_ref", "sub_ref", "reason_code", "severity",
    "disposition", "stage", "envelope_schema_version", "raw_path", "raw_sha256", "excerpt",
    "excerpt_truncated", "batch_id", "pipeline_version", "detected_at",
)


def bounded_excerpt(text: str | bytes | None, max_chars: int) -> tuple[str | None, bool]:
    if text is None:
        return None, False
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    if len(text) <= max_chars:
        return text, False
    return text[:max_chars], True


def dlq_record(
    *,
    error_id: str,
    run_id: str,
    feed_type: str | None,
    reason_code: str,
    stage: str,
    max_excerpt_chars: int,
    excerpt_source: str | bytes | None = None,
    source_topic: str | None = None,
    source_partition: int | None = None,
    source_offset: int | None = None,
    value_sha256: str | None = None,
    snapshot_id: str | None = None,
    capture_id: str | None = None,
    entity_ref: str | None = None,
    sub_ref: str | None = None,
    envelope_schema_version: int | None = None,
    raw_path: str | None = None,
    raw_sha256: str | None = None,
    batch_id: int | None = None,
    pipeline_version: str | None = None,
    detected_at: datetime | None = None,
) -> dict[str, Any]:
    info = reason_info(reason_code)
    excerpt, truncated = bounded_excerpt(excerpt_source, max_excerpt_chars)
    return {
        "schema_version": SCHEMA_VERSION,
        "error_id": error_id,
        "run_id": run_id,
        "feed_type": feed_type,
        "source_topic": source_topic,
        "source_partition": source_partition,
        "source_offset": source_offset,
        "value_sha256": value_sha256,
        "snapshot_id": snapshot_id,
        "capture_id": capture_id,
        "entity_ref": entity_ref,
        "sub_ref": sub_ref,
        "reason_code": reason_code,
        "severity": str(info.severity),
        "disposition": str(info.disposition),
        "stage": stage,
        "envelope_schema_version": envelope_schema_version,
        "raw_path": raw_path,
        "raw_sha256": raw_sha256,
        "excerpt": excerpt,
        "excerpt_truncated": truncated,
        "batch_id": batch_id,
        "pipeline_version": pipeline_version,
        "detected_at": iso_utc(detected_at or utcnow()),
    }
