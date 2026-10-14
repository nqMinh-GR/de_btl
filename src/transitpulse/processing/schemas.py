"""Explicit Spark schemas (no schema inference anywhere in streaming).

Parsing strategy (design B §4):
  1. Kafka ``value`` -> ``from_json(value_str, ENVELOPE_SCHEMA, {"mode": "PERMISSIVE",
     "columnNameOfCorruptRecord": "_corrupt"})``. ``entities`` is ``ARRAY<STRING>``: Spark returns
     the raw JSON text of each entity object, so a single bad entity does not null the envelope.
  2. ``posexplode(entities)`` then ``from_json(entity_json, <FEED>_ENTITY_SCHEMA, same options)``;
     ``_corrupt IS NOT NULL`` => entity rejected (INVALID_ENTITY) and sent to the DLQ.
  3. Control messages use CONTROL_SCHEMA with the same corrupt-record handling.

Field types follow the real MBTA JSON (string enums, integer epoch seconds, double coordinates).
A type mismatch (e.g. ``"start": "abc"`` for a LONG) makes the record corrupt — this is how
malformed entities are detected, never silently converted.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

CORRUPT_COL = "_corrupt"
JSON_OPTIONS = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_COL}


def _s(name: str, dtype=StringType()) -> StructField:  # noqa: B008 - immutable Spark type instance
    return StructField(name, dtype, True)


TRANSLATED_STRING = StructType([
    _s("translation", ArrayType(StructType([_s("text"), _s("language")]))),
])

TRIP_DESCRIPTOR = StructType([
    _s("trip_id"),
    _s("route_id"),
    _s("direction_id", IntegerType()),
    _s("start_time"),
    _s("start_date"),
    _s("schedule_relationship"),
])

VEHICLE_DESCRIPTOR = StructType([_s("id"), _s("label"), _s("license_plate")])

HEADER_SCHEMA = StructType([
    _s("gtfs_realtime_version"),
    _s("incrementality"),
    _s("timestamp", LongType()),
])

HTTP_SCHEMA = StructType([
    _s("status", IntegerType()),
    _s("etag"),
    _s("last_modified"),
    _s("content_type"),
])

ENVELOPE_SCHEMA = StructType([
    _s("schema_version", IntegerType()),
    _s("message_type"),
    _s("run_id"),
    _s("scope_id"),
    _s("source_type"),
    _s("scenario_id"),
    _s("feed_type"),
    _s("snapshot_id"),
    _s("capture_id"),
    _s("source_url"),
    _s("gtfs_version"),
    _s("feed_timestamp", LongType()),
    _s("original_fetched_at"),       # ISO string; converted with to_timestamp in parse.py
    _s("published_at"),
    _s("raw_sha256"),
    _s("raw_path"),
    _s("raw_bytes", LongType()),
    _s("http", HTTP_SCHEMA),
    _s("source_entity_count", IntegerType()),
    _s("selected_entity_count", IntegerType()),
    _s("scope_accounting", MapType(StringType(), StringType())),
    _s("header", HEADER_SCHEMA),
    _s("entities", ArrayType(StringType())),
    _s("producer", MapType(StringType(), StringType())),
    _s(CORRUPT_COL),
])

VP_ENTITY_SCHEMA = StructType([
    _s("id"),
    _s("is_deleted", BooleanType()),
    _s("vehicle", StructType([
        _s("trip", TRIP_DESCRIPTOR),
        _s("vehicle", VEHICLE_DESCRIPTOR),
        _s("position", StructType([
            _s("latitude", DoubleType()),
            _s("longitude", DoubleType()),
            _s("bearing", DoubleType()),
            _s("odometer", DoubleType()),
            _s("speed", DoubleType()),
        ])),
        _s("current_stop_sequence", IntegerType()),
        _s("stop_id"),
        _s("current_status"),
        _s("timestamp", LongType()),
        _s("congestion_level"),
        _s("occupancy_status"),
        _s("occupancy_percentage", IntegerType()),
    ])),
    _s(CORRUPT_COL),
])

STOP_TIME_EVENT = StructType([
    _s("delay", IntegerType()),
    _s("time", LongType()),
    _s("uncertainty", IntegerType()),
])

TU_ENTITY_SCHEMA = StructType([
    _s("id"),
    _s("is_deleted", BooleanType()),
    _s("trip_update", StructType([
        _s("trip", TRIP_DESCRIPTOR),
        _s("vehicle", VEHICLE_DESCRIPTOR),
        _s("timestamp", LongType()),
        _s("delay", IntegerType()),
        _s("stop_time_update", ArrayType(StructType([
            _s("stop_sequence", IntegerType()),
            _s("stop_id"),
            _s("arrival", STOP_TIME_EVENT),
            _s("departure", STOP_TIME_EVENT),
            _s("schedule_relationship"),
        ]))),
    ])),
    _s(CORRUPT_COL),
])

INFORMED_ENTITY = StructType([
    _s("agency_id"),
    _s("route_id"),
    _s("route_type", IntegerType()),
    _s("direction_id", IntegerType()),
    _s("trip", TRIP_DESCRIPTOR),
    _s("stop_id"),
])

ALERT_ENTITY_SCHEMA = StructType([
    _s("id"),
    _s("is_deleted", BooleanType()),
    _s("alert", StructType([
        _s("active_period", ArrayType(StructType([_s("start", LongType()), _s("end", LongType())]))),
        _s("informed_entity", ArrayType(INFORMED_ENTITY)),
        _s("cause"),
        _s("effect"),
        _s("severity_level"),
        _s("url", TRANSLATED_STRING),
        _s("header_text", TRANSLATED_STRING),
        _s("description_text", TRANSLATED_STRING),
        _s("cause_detail", TRANSLATED_STRING),
        _s("effect_detail", TRANSLATED_STRING),
    ])),
    _s(CORRUPT_COL),
])

CONTROL_SCHEMA = StructType([
    _s("schema_version", IntegerType()),
    _s("message_type"),
    _s("event_type"),
    _s("event_id"),
    _s("run_id"),
    _s("source_type"),
    _s("emitted_at"),
    _s("clock_sequence", LongType()),
    _s("source_clock"),
    _s("speed", DoubleType()),
    _s("run_state"),
    _s("reason"),
    # collector_status / heartbeat fields used by pipeline_status
    _s("feed_type"),
    _s("outcome"),
    _s("http_status", IntegerType()),
    _s("reason_code"),
    _s("feed_state"),
    _s("last_success_fetched_at"),
    _s("last_feed_timestamp", LongType()),
    _s("consecutive_failures", IntegerType()),
    _s("outbox_pending", IntegerType()),
    _s(CORRUPT_COL),
])

ENTITY_SCHEMA_BY_FEED = {
    "vehicle_positions": VP_ENTITY_SCHEMA,
    "trip_updates": TU_ENTITY_SCHEMA,
    "alerts": ALERT_ENTITY_SCHEMA,
}
