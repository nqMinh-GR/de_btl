"""Stable identifiers and canonical serialization (design B §2, decisions D-ID-*).

Hash primitive
--------------
``H(s) = sha256(s.encode("utf-8")).hexdigest()[:32]``  (128 bits, lower-case hex)

``K(*parts)`` joins parts with the unit separator ``\\x1f``; ``None`` becomes ``\\x00``.
Parts must be ``str``, ``int`` or ``None`` only (bools and floats are rejected so that Python and
Spark render them identically). Floats are converted first with the fixed-point helpers
``e7`` (lat/lon), ``e2`` (bearing) and ``e3`` (speed).

Spark parity (processing code MUST use :func:`spark_k_expr` style expressions):
  ``sha2(concat_ws('\\u001f', coalesce(cast(p1 as string), '\\u0000'), ...), 256)`` then ``substring(.., 1, 32)``.
  ``cast(int/long as string)`` equals Python ``str(int)``; ``e7(x) = floor(x * 1e7 + 0.5)`` is
  computed in IEEE double in both engines and rendered as a long.
"""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

SEP = "\x1f"
NULL = "\x00"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def canonical_json(obj: Any) -> bytes:
    """Canonical JSON bytes: sorted keys, compact separators, UTF-8, NaN/Infinity rejected."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
        "utf-8"
    )


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def H(s: str) -> str:  # noqa: N802 - name fixed by the design
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:32]


def K(*parts: str | int | None) -> str:  # noqa: N802
    out: list[str] = []
    for p in parts:
        if p is None:
            out.append(NULL)
        elif isinstance(p, bool) or not isinstance(p, (str, int)):
            raise TypeError(f"K() accepts str/int/None only, got {type(p).__name__}: {p!r}")
        else:
            out.append(str(p))
    return SEP.join(out)


def e7(x: float | None) -> int | None:
    """Latitude/longitude fixed point (1e-7 degrees)."""
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else math.floor(float(x) * 1e7 + 0.5)


def e2(x: float | None) -> int | None:
    """Bearing fixed point (1e-2 degrees)."""
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else math.floor(float(x) * 100 + 0.5)


def e3(x: float | None) -> int | None:
    """Speed fixed point (1e-3 m/s)."""
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else math.floor(float(x) * 1000 + 0.5)


# --------------------------------------------------------------------------------------------------
# Run / scope / static identity
# --------------------------------------------------------------------------------------------------

def new_run_id() -> str:
    """uuid4 created once per run (namespace of Kafka keys, checkpoints, raw paths and serving rows)."""
    return str(uuid.uuid4())


def gtfs_version_of_bytes(zip_bytes: bytes) -> str:
    """Internal GTFS version = full sha256 hex of the ZIP bytes (never the file name)."""
    return sha256_hex(zip_bytes)


def scope_id(route_ids: Iterable[str], route_type: int, scope_policy_version: str, gtfs_version: str) -> str:
    payload = {
        "route_ids": sorted(set(route_ids)),
        "route_type": int(route_type),
        "scope_policy_version": scope_policy_version,
        "gtfs_version": gtfs_version,
    }
    return sha256_hex(canonical_json(payload))[:16]


# --------------------------------------------------------------------------------------------------
# Capture / snapshot identity
# --------------------------------------------------------------------------------------------------

def capture_id(feed_abbr: str, fetched_at: datetime, raw_sha256: str) -> str:
    """``<feed abbr>-<YYYYMMDDTHHMMSSffffff>Z-<sha256[:12]>``; fixed at capture time, preserved by replay."""
    ts = fetched_at.astimezone(UTC).strftime("%Y%m%dT%H%M%S%f")
    return f"{feed_abbr}-{ts}Z-{raw_sha256[:12]}"


def payload_sha(header: dict[str, Any], selected_entities: Sequence[dict[str, Any]]) -> str:
    """sha256 over canonical {header, entities sorted by canonical form}; entity order does not matter."""
    ents = sorted((json.loads(canonical_json(e)) for e in selected_entities), key=canonical_json)
    return sha256_hex(canonical_json({"header": header, "entities": ents}))


def snapshot_id(run_id: str, scope_id_: str, feed_type: str, header_timestamp: int | None, payload_sha_: str) -> str:
    """Stable logical snapshot id: independent of published_at, capture_id and retries."""
    return "sn_" + H(K("snapshot", "v1", run_id, scope_id_, feed_type, header_timestamp, payload_sha_))


def generation_id(run_id: str, feed_type: str, snapshot_id_: str, pipeline_version: str) -> str:
    return "gn_" + H(K("gen", "v1", run_id, feed_type, snapshot_id_, pipeline_version))


def source_order(feed_timestamp: int, original_fetched_at: datetime, snapshot_id_: str) -> str:
    """Fixed-width text so that lexicographic order == tuple order (feed ts, capture µs, snapshot id).

    Spark builds the same value from the envelope's ISO ``original_fetched_at`` (millisecond precision).
    Python callers that must match Spark should pass ``parse_iso_utc(env["original_fetched_at"])``,
    not a higher-precision in-memory datetime.
    """
    # Exact integer microseconds (float timestamp()*1e6 can be off by 1 µs vs Spark's unix_micros).
    micros = (original_fetched_at.astimezone(UTC) - _EPOCH) // timedelta(microseconds=1)
    if feed_timestamp < 0 or micros < 0:
        raise ValueError("source_order components must be non-negative")
    return f"{int(feed_timestamp):012d}.{micros:017d}.{snapshot_id_}"


# --------------------------------------------------------------------------------------------------
# Entity-level identity (Spark mirrors these exactly)
# --------------------------------------------------------------------------------------------------

def observation_id(
    *,
    vehicle_id: str | None,
    trip_id: str | None,
    route_id: str | None,
    direction_id: int | None,
    start_date: str | None,
    start_time: str | None,
    trip_schedule_relationship: str | None,
    vehicle_timestamp: int | None,
    latitude: float | None,
    longitude: float | None,
    bearing: float | None,
    speed: float | None,
    stop_id: str | None,
    current_stop_sequence: int | None,
    current_status: str | None,
) -> str:
    """Observation with a source timestamp. Excludes run/snapshot/capture so a re-poll is not new history."""
    return "ob_" + H(K(
        "obs", "v1", vehicle_id, trip_id, route_id, direction_id, start_date, start_time,
        trip_schedule_relationship, vehicle_timestamp, e7(latitude), e7(longitude), e2(bearing), e3(speed),
        stop_id, current_stop_sequence, current_status,
    ))


def state_sample_id(
    *,
    vehicle_id: str | None,
    trip_id: str | None,
    route_id: str | None,
    direction_id: int | None,
    start_date: str | None,
    start_time: str | None,
    trip_schedule_relationship: str | None,
    header_timestamp: int,
    latitude: float | None,
    longitude: float | None,
    bearing: float | None,
    speed: float | None,
    stop_id: str | None,
    current_stop_sequence: int | None,
    current_status: str | None,
) -> str:
    """snapshot_state_sample: vehicle without timestamp; effective event time = feed header timestamp."""
    return "ss_" + H(K(
        "sample", "v1", vehicle_id, trip_id, route_id, direction_id, start_date, start_time,
        trip_schedule_relationship, header_timestamp, e7(latitude), e7(longitude), e2(bearing), e3(speed),
        stop_id, current_stop_sequence, current_status,
    ))


def prediction_id(
    *,
    trip_id: str,
    start_date: str | None,
    start_time: str | None,
    route_id: str | None,
    direction_id: int | None,
    stop_sequence: int | None,
    stop_id: str | None,
    event_type: str,
) -> str:
    """Trip instance + stop occurrence + event type, from SOURCE fields only (never static resolution)."""
    if not trip_id:
        raise ValueError("prediction_id requires trip_id; records without it are quarantined (INVALID_PREDICTION)")
    return "pr_" + H(K("pred", "v1", trip_id, start_date, start_time, route_id, direction_id, stop_sequence,
                       stop_id, event_type))


def error_id_message(run_id: str, topic: str, value_sha256: str, reason: str) -> str:
    """Message-level DLQ id: stable across Kafka redelivery and collector re-publish."""
    return "er_" + H(K("err", "v1", run_id, topic, value_sha256, reason))


def error_id_entity(
    run_id: str, feed_type: str, snapshot_id_: str, entity_ref: str, sub_ref: str | None, reason: str
) -> str:
    """Entity-level DLQ id. entity_ref = entity.id, or 'h:' + sha256(entity_json)[:16] when the id is missing."""
    return "er_" + H(K("err", "v1", run_id, feed_type, snapshot_id_, entity_ref, sub_ref, reason))


def entity_ref(entity: dict[str, Any] | None, entity_json: str | None = None) -> str:
    if entity and isinstance(entity.get("id"), str) and entity["id"]:
        return entity["id"]
    raw = entity_json if entity_json is not None else canonical_json(entity or {}).decode("utf-8")
    return "h:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def event_id() -> str:
    """Control-event id (audit only; never part of a logical key)."""
    return "ev_" + uuid.uuid4().hex
