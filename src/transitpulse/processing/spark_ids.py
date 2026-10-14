"""Spark Column mirrors of :mod:`transitpulse.contracts.ids` (design B §2, decisions D-ID-01..D-ID-04).

Every function here produces exactly the same string as its Python reference for the same inputs;
``tests/spark/test_spark_ids_parity.py`` asserts this on many deterministic pseudo-random inputs.

Rules that make the two engines agree:
  * ``K(*parts)`` joins parts with U+001F; a null part becomes U+0000 (``concat_ws`` would silently DROP
    nulls and create collisions, so every part is coalesced first). The control characters are produced
    with ``chr(31)`` / ``chr(0)`` inside Spark, so no NUL byte ever travels through py4j.
  * Integer parts (IntegerType/LongType columns) are rendered with ``CAST(x AS STRING)`` which equals
    Python ``str(int)``. Python int literals are rendered with ``str()`` on the driver.
  * Floats never enter ``K`` directly: ``e7/e2/e3`` compute ``floor(x * scale + 0.5)`` in IEEE double in
    both engines and render the result as a long. NaN/null -> null (Python ``None``).
  * ``H(s) = sha256(utf8(s))[:32]`` -> ``substring(sha2(s, 256), 1, 32)`` (both lower-case hex).

Argument conventions:
  * :func:`k_cols` - a ``Column`` is used as-is; a Python ``str``/``int`` is a LITERAL part; ``None`` is a
    null part. Booleans, floats and DoubleType columns must not be passed (Python ``K`` rejects them).
  * The ``*_id_col`` helpers take ``Column`` objects or column NAMES (``str``) for every field;
    ``None`` means a null field.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column
from pyspark.sql import functions as F

__all__ = [
    "k_cols",
    "h_col",
    "e7_col",
    "e2_col",
    "e3_col",
    "observation_id_col",
    "state_sample_id_col",
    "prediction_id_col",
    "error_id_entity_col",
    "entity_ref_col",
]


def _sep() -> Column:
    return F.expr("chr(31)")


def _nul() -> Column:
    return F.expr("chr(0)")


def _literal_part(p: Any) -> Column:
    """K() part given as a Python value (literal)."""
    if isinstance(p, Column):
        return p
    if p is None:
        return F.lit(None).cast("string")
    if isinstance(p, bool) or not isinstance(p, (str, int)):
        raise TypeError(f"k_cols() accepts Column/str/int/None only, got {type(p).__name__}: {p!r}")
    return F.lit(str(p))


def _field(x: Any) -> Column:
    """ID-helper field: Column as-is, str = column name, None = null."""
    if isinstance(x, Column):
        return x
    if x is None:
        return F.lit(None).cast("string")
    if isinstance(x, str):
        return F.col(x)
    raise TypeError(f"expected Column, column name or None, got {type(x).__name__}: {x!r}")


def k_cols(*parts: Any) -> Column:
    """Mirror of ``ids.K``: parts joined by U+001F, null parts rendered as U+0000."""
    pieces: list[Column] = []
    for i, p in enumerate(parts):
        if i:
            pieces.append(_sep())
        pieces.append(F.coalesce(_literal_part(p).cast("string"), _nul()))
    if not pieces:
        return F.lit("")
    return F.concat(*pieces)


def h_col(c: Column) -> Column:
    """Mirror of ``ids.H``: first 32 lower-case hex chars of sha256(utf8(c))."""
    return F.substring(F.sha2(c, 256), 1, 32)


def _fixed_point(x: Any, scale: float) -> Column:
    v = _field(x).cast("double")
    return (
        F.when(v.isNull() | F.isnan(v), F.lit(None).cast("long"))
        .otherwise(F.floor(v * F.lit(float(scale)) + F.lit(0.5)).cast("long"))
    )


def e7_col(x: Any) -> Column:
    """Latitude/longitude fixed point (1e-7 degrees), mirror of ``ids.e7``."""
    return _fixed_point(x, 1e7)


def e2_col(x: Any) -> Column:
    """Bearing fixed point (1e-2 degrees), mirror of ``ids.e2``."""
    return _fixed_point(x, 100.0)


def e3_col(x: Any) -> Column:
    """Speed fixed point (1e-3 m/s), mirror of ``ids.e3``."""
    return _fixed_point(x, 1000.0)


def observation_id_col(
    *,
    vehicle_id: Any,
    trip_id: Any,
    route_id: Any,
    direction_id: Any,
    start_date: Any,
    start_time: Any,
    trip_schedule_relationship: Any,
    vehicle_timestamp: Any,
    latitude: Any,
    longitude: Any,
    bearing: Any,
    speed: Any,
    stop_id: Any,
    current_stop_sequence: Any,
    current_status: Any,
) -> Column:
    """Mirror of ``ids.observation_id`` (``ob_`` + H(K('obs','v1', ...)))."""
    key = k_cols(
        "obs", "v1",
        _field(vehicle_id), _field(trip_id), _field(route_id), _field(direction_id),
        _field(start_date), _field(start_time), _field(trip_schedule_relationship), _field(vehicle_timestamp),
        e7_col(latitude), e7_col(longitude), e2_col(bearing), e3_col(speed),
        _field(stop_id), _field(current_stop_sequence), _field(current_status),
    )
    return F.concat(F.lit("ob_"), h_col(key))


def state_sample_id_col(
    *,
    vehicle_id: Any,
    trip_id: Any,
    route_id: Any,
    direction_id: Any,
    start_date: Any,
    start_time: Any,
    trip_schedule_relationship: Any,
    header_timestamp: Any,
    latitude: Any,
    longitude: Any,
    bearing: Any,
    speed: Any,
    stop_id: Any,
    current_stop_sequence: Any,
    current_status: Any,
) -> Column:
    """Mirror of ``ids.state_sample_id`` (``ss_`` + H(K('sample','v1', ...)), header timestamp in the key)."""
    key = k_cols(
        "sample", "v1",
        _field(vehicle_id), _field(trip_id), _field(route_id), _field(direction_id),
        _field(start_date), _field(start_time), _field(trip_schedule_relationship), _field(header_timestamp),
        e7_col(latitude), e7_col(longitude), e2_col(bearing), e3_col(speed),
        _field(stop_id), _field(current_stop_sequence), _field(current_status),
    )
    return F.concat(F.lit("ss_"), h_col(key))


def prediction_id_col(
    *,
    trip_id: Any,
    start_date: Any,
    start_time: Any,
    route_id: Any,
    direction_id: Any,
    stop_sequence: Any,
    stop_id: Any,
    event_type: Any,
) -> Column:
    """Mirror of ``ids.prediction_id``. Python raises for a missing/empty trip_id; Spark returns null
    (such records are quarantined as INVALID_PREDICTION and never get a key)."""
    trip = _field(trip_id)
    key = k_cols(
        "pred", "v1",
        trip, _field(start_date), _field(start_time), _field(route_id), _field(direction_id),
        _field(stop_sequence), _field(stop_id), _field(event_type),
    )
    return F.when(trip.isNotNull() & (F.length(trip) > 0), F.concat(F.lit("pr_"), h_col(key)))


def error_id_entity_col(run_id: Any, feed_type: Any, snapshot_id: Any, entity_ref: Any, sub_ref: Any,
                        reason: Any) -> Column:
    """Mirror of ``ids.error_id_entity`` (``er_`` + H(K('err','v1',run,feed,snapshot,entity_ref,sub_ref,reason)))."""
    key = k_cols(
        "err", "v1",
        _field(run_id), _field(feed_type), _field(snapshot_id), _field(entity_ref), _field(sub_ref),
        _field(reason),
    )
    return F.concat(F.lit("er_"), h_col(key))


def entity_ref_col(entity_id: Any, entity_json: Any) -> Column:
    """Mirror of ``ids.entity_ref(entity, entity_json)``: the entity id when it is a non-empty string,
    else ``'h:' + sha256(entity_json)[:16]``."""
    eid = _field(entity_id)
    js = _field(entity_json)
    return (
        F.when(eid.isNotNull() & (F.length(eid) > 0), eid)
        .otherwise(F.concat(F.lit("h:"), F.substring(F.sha2(js, 256), 1, 16)))
    )
