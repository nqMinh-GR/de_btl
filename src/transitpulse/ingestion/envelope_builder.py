"""Build snapshot envelopes and collector-side DLQ records from a capture (design E §1.5).

``envelope_from_capture`` is the single place where a capture becomes a Kafka envelope; it is used by
the collector, by restart reconciliation (rebuilding from raw) and by the replay publisher. The
envelope has no ``published_at`` (set at send time) so the outbox can store and resend it unchanged:

* ``payload_sha`` = sha256 over the canonical header and the canonically sorted selected entities;
* ``snapshot_id`` = ``sn_`` + H(K("snapshot", "v1", run_id, scope_id, feed_type, header.timestamp,
  payload_sha)) — independent of ``published_at``, ``capture_id`` and retries (D-ID-02).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from transitpulse import __version__
from transitpulse.contracts.dlq import dlq_record
from transitpulse.contracts.envelope import SCHEMA_VERSION, Envelope, HttpMeta, build_envelope
from transitpulse.contracts.ids import error_id_message, payload_sha, snapshot_id
from transitpulse.ingestion.archive import RawRef
from transitpulse.ingestion.feed_parse import ParsedFeed
from transitpulse.ingestion.scope_filter import include_unparsed_entities

PRODUCER_VERSION = __version__


class RunLike(Protocol):
    """The attributes of ``transitpulse.runs.RunRecord`` used to build an envelope."""

    run_id: str
    scope_id: str
    source_type: str
    gtfs_version: str


def http_meta(status: int | None, headers: dict[str, str] | None) -> HttpMeta:
    """Envelope ``http`` block from a response status and its lower-cased header subset."""
    h = headers or {}
    return HttpMeta(
        status=status,
        etag=h.get("etag"),
        last_modified=h.get("last-modified"),
        content_type=h.get("content-type"),
    )


def compute_snapshot_id(run_id: str, scope_id: str, feed_type: str, parsed: ParsedFeed,
                        selected: list[dict[str, Any]]) -> str:
    return snapshot_id(run_id, scope_id, feed_type, parsed.feed_timestamp, payload_sha(parsed.header, selected))


def envelope_from_capture(
    *,
    run: RunLike,
    feed_type: str,
    parsed: ParsedFeed,
    selected: list[dict[str, Any]],
    accounting: dict[str, Any],
    raw_ref: RawRef,
    capture_id: str,
    fetched_at: datetime,
    source_url: str,
    http: dict[str, Any] | None,
    scenario_id: str | None,
    component: str,
) -> Envelope:
    """Envelope (without ``published_at``) for one capture of ``run``.

    ``source_entity_count`` counts every entry of the source ``entity`` list (non-object entries that
    the parser could not keep included); ``selected`` entities are copied unchanged in source order.
    A ScopeFilter accounting is reconciled with :func:`include_unparsed_entities` (idempotent), so
    ``scope_accounting.source_entities == source_entity_count`` for every caller.
    """
    sid = compute_snapshot_id(run.run_id, run.scope_id, feed_type, parsed, selected)
    return build_envelope(
        run_id=run.run_id,
        scope_id=run.scope_id,
        source_type=run.source_type,
        scenario_id=scenario_id,
        feed_type=feed_type,
        snapshot_id=sid,
        capture_id=capture_id,
        source_url=source_url,
        gtfs_version=run.gtfs_version,
        header=parsed.header,
        entities=list(selected),
        original_fetched_at=fetched_at,
        raw_sha256=raw_ref.raw_sha256,
        raw_path=raw_ref.raw_path,
        raw_bytes=raw_ref.raw_bytes,
        http=HttpMeta(**http) if http is not None else None,
        source_entity_count=parsed.source_entity_count,
        scope_accounting=include_unparsed_entities(accounting, parsed.source_entity_count),
        producer_component=component,
        producer_version=PRODUCER_VERSION,
    )


def capture_dlq_record(
    *,
    run_id: str,
    feed_type: str,
    topic: str,
    reason_code: str,
    detail: str,
    capture_id: str,
    raw_path: str,
    raw_sha256: str,
    max_excerpt_chars: int,
    pipeline_version: str | None,
    stage: str,
    snapshot_id: str | None = None,
    body: bytes | None = None,
    detected_at: datetime | None = None,
) -> dict[str, Any]:
    """Message-level DLQ record for a capture rejected by the collector.

    ``source_topic`` is the feed's snapshot topic (``topic``), as the replay publisher and Spark record it
    for the same rejection; partition/offset stay null because the capture was never produced.
    ``error_id = error_id_message(run_id, topic, raw_sha256, reason)``: the value hashed is the decoded
    raw body, so the id is stable across restart reconciliation and re-publish.
    The excerpt starts with ``detail=...`` followed by a bounded prefix of the body (if any); the full
    body stays in the raw archive referenced by ``raw_path``/``raw_sha256``.
    """
    excerpt = f"detail={detail}"
    if body:
        excerpt += "\n" + body[: max(0, max_excerpt_chars) * 4].decode("utf-8", errors="replace")
    return dlq_record(
        error_id=error_id_message(run_id, topic, raw_sha256, reason_code),
        run_id=run_id,
        feed_type=feed_type,
        reason_code=reason_code,
        stage=stage,
        max_excerpt_chars=max_excerpt_chars,
        excerpt_source=excerpt,
        source_topic=topic,          # the feed's snapshot topic, as the replay publisher and Spark record it
        value_sha256=raw_sha256,
        snapshot_id=snapshot_id,
        capture_id=capture_id,
        envelope_schema_version=SCHEMA_VERSION if snapshot_id is not None else None,
        raw_path=raw_path,
        raw_sha256=raw_sha256,
        pipeline_version=pipeline_version,
        detected_at=detected_at,
    )
