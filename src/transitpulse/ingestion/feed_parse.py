"""Transport-level validation of a GTFS-Realtime JSON body (design E §1.3, decision D-E06).

The collector only checks what is needed to build a trustworthy snapshot envelope; entity-level
validation (coordinates, timestamps, uncertainty, ...) is Spark's job and is never done here.

=========================================================  ===============================
Check                                                      Rejection reason
=========================================================  ===============================
strict UTF-8, ``json.loads`` rejecting NaN/Infinity        ``INVALID_JSON``
top level object with a ``header`` object; ``entity`` a    ``INVALID_HEADER``
list (a missing ``entity`` is the empty list)
``gtfs_realtime_version`` present and in {"1.0", "2.0"}    ``INVALID_HEADER`` / ``UNSUPPORTED_SCHEMA``
``incrementality`` absent (GTFS-RT default) or FULL_DATASET ``UNSUPPORTED_INCREMENTALITY``
``timestamp`` integer epoch seconds in                     ``INVALID_HEADER``
[2000-01-01, fetched_at + 300 s]; values > 1e11 get
detail ``timestamp_ms_suspected`` and are NOT converted
=========================================================  ===============================

A rejection never raises: the caller keeps the raw body, records the reason in the outbox and DLQ, and
produces no snapshot. Nothing in the parsed objects is normalized: header and entities are returned
exactly as received (optional fields and enums preserved).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

SUPPORTED_GTFS_RT_VERSIONS: frozenset[str] = frozenset({"1.0", "2.0"})
# incrementality: proto3 JSON accepts the enum name ("FULL_DATASET") or its number (0).
MIN_FEED_TIMESTAMP = 946_684_800                             # 2000-01-01T00:00:00Z
FUTURE_TOLERANCE_SECONDS = 300
TIMESTAMP_MS_THRESHOLD = 100_000_000_000                     # mirrors policy.timestamp_ms_threshold (1e11)
DETAIL_MAX_CHARS = 300

INVALID_JSON = "INVALID_JSON"
INVALID_HEADER = "INVALID_HEADER"
UNSUPPORTED_SCHEMA = "UNSUPPORTED_SCHEMA"
UNSUPPORTED_INCREMENTALITY = "UNSUPPORTED_INCREMENTALITY"
PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"
TRANSPORT_REASONS: frozenset[str] = frozenset({
    INVALID_JSON, INVALID_HEADER, UNSUPPORTED_SCHEMA, UNSUPPORTED_INCREMENTALITY, PAYLOAD_TOO_LARGE,
})


@dataclass
class ParsedFeed:
    """A transport-valid feed. ``feed_timestamp`` equals ``header["timestamp"]`` (epoch seconds).

    ``warnings`` counts entity-level observations made while parsing (for the capture sidecar only);
    for example ``entity_not_object`` or ``entity_timestamp_ms_suspected``. Values are never altered.
    """

    header: dict[str, Any]
    entities: list[dict[str, Any]]
    feed_timestamp: int
    warnings: dict[str, int] = field(default_factory=dict)

    @property
    def source_entity_count(self) -> int:
        """Entities in the source document, including non-object entries that were not kept."""
        return len(self.entities) + int(self.warnings.get("entity_not_object", 0))


@dataclass
class Rejection:
    reason_code: str
    detail: str


class _NonFiniteConstant(ValueError):
    pass


def _reject_constant(name: str) -> Any:
    raise _NonFiniteConstant(f"non-finite JSON constant {name}")


def _clip(text: str) -> str:
    return text if len(text) <= DETAIL_MAX_CHARS else text[:DETAIL_MAX_CHARS] + "…"


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def parse_feed(body: bytes, fetched_at: datetime) -> ParsedFeed | Rejection:
    """Validate and decode one GTFS-Realtime JSON body fetched at ``fetched_at`` (aware UTC)."""
    if fetched_at.tzinfo is None:
        raise ValueError("fetched_at must be timezone-aware")
    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        return Rejection(INVALID_JSON, _clip(f"invalid UTF-8 at byte {exc.start}: {exc.reason}"))
    try:
        doc = json.loads(text, parse_constant=_reject_constant)
    except _NonFiniteConstant as exc:
        return Rejection(INVALID_JSON, _clip(str(exc)))
    except json.JSONDecodeError as exc:
        return Rejection(INVALID_JSON, _clip(f"{exc.msg} at line {exc.lineno} column {exc.colno}"))
    except (ValueError, RecursionError) as exc:
        return Rejection(INVALID_JSON, _clip(f"{type(exc).__name__}: {exc}"))

    if not isinstance(doc, dict):
        return Rejection(INVALID_HEADER, f"top-level JSON value is {type(doc).__name__}, expected object")
    header = doc.get("header")
    if not isinstance(header, dict):
        return Rejection(INVALID_HEADER, "missing or non-object 'header'")
    raw_entities = doc.get("entity", [])
    if raw_entities is None:
        raw_entities = []
    if not isinstance(raw_entities, list):
        return Rejection(INVALID_HEADER, f"'entity' is {type(raw_entities).__name__}, expected list")

    version = header.get("gtfs_realtime_version")
    if version is None:
        return Rejection(INVALID_HEADER, "header.gtfs_realtime_version is missing")
    if not isinstance(version, str):
        return Rejection(INVALID_HEADER, "header.gtfs_realtime_version is not a string")
    if version not in SUPPORTED_GTFS_RT_VERSIONS:
        return Rejection(UNSUPPORTED_SCHEMA, _clip(f"gtfs_realtime_version {version!r} not in {{1.0, 2.0}}"))

    if "incrementality" in header:
        inc = header["incrementality"]
        is_full = inc == "FULL_DATASET" or (_is_int(inc) and inc == 0)
        if not is_full:
            return Rejection(UNSUPPORTED_INCREMENTALITY, _clip(f"incrementality {inc!r} is not FULL_DATASET"))

    ts = header.get("timestamp")
    if ts is None:
        return Rejection(INVALID_HEADER, "header.timestamp is missing")
    if not _is_int(ts):
        return Rejection(INVALID_HEADER, _clip(f"header.timestamp {ts!r} is not integer epoch seconds"))
    if ts > TIMESTAMP_MS_THRESHOLD:
        return Rejection(INVALID_HEADER, f"timestamp_ms_suspected: header.timestamp {ts} > 1e11 (not converted)")
    upper = int(fetched_at.timestamp()) + FUTURE_TOLERANCE_SECONDS
    if ts < MIN_FEED_TIMESTAMP or ts > upper:
        return Rejection(INVALID_HEADER,
                         f"timestamp_out_of_range: header.timestamp {ts} not in [{MIN_FEED_TIMESTAMP}, {upper}]")

    entities: list[dict[str, Any]] = []
    warnings: dict[str, int] = {}
    for ent in raw_entities:
        if not isinstance(ent, dict):
            warnings["entity_not_object"] = warnings.get("entity_not_object", 0) + 1
            continue
        entities.append(ent)
        if _entity_ms_timestamp(ent):
            warnings["entity_timestamp_ms_suspected"] = warnings.get("entity_timestamp_ms_suspected", 0) + 1
    return ParsedFeed(header=header, entities=entities, feed_timestamp=ts, warnings=warnings)


def _entity_ms_timestamp(ent: dict[str, Any]) -> bool:
    """True when a vehicle/trip_update timestamp looks like milliseconds (flagged only, never converted)."""
    for key in ("vehicle", "trip_update"):
        obj = ent.get(key)
        if isinstance(obj, dict):
            ts = obj.get("timestamp")
            if _is_int(ts) and ts > TIMESTAMP_MS_THRESHOLD:
                return True
    return False


def header_summary(parsed: ParsedFeed | None) -> dict[str, Any] | None:
    """Small header digest for sidecars, logs and status events."""
    if parsed is None:
        return None
    h = parsed.header
    return {
        "gtfs_realtime_version": h.get("gtfs_realtime_version"),
        "incrementality": h.get("incrementality"),
        "timestamp": parsed.feed_timestamp,
    }
