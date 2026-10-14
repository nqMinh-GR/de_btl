"""Spark parsing of Kafka rows (WP4): run filter, reason precedence, source_order, duplicates, DLQ rows.

Uses the session-scoped ``spark`` fixture of tests/spark/conftest.py (local, UTC, no Kafka jars).
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from transitpulse.contracts.dlq import DLQ_FIELDS
from transitpulse.contracts.enums import QUALITY_REASONS
from transitpulse.contracts.ids import H, K, error_id_message
from transitpulse.processing import parse
from transitpulse.processing.context import PARSED_SNAPSHOT_COLUMNS
from transitpulse.processing.parse import ParseContext

RUN = "run-1"
SCOPE = "scope0123456789a"
GTFS = "g" * 64
VP, TU, AL, CTL = (
    "mbta.vehicle_positions.snapshots.v1", "mbta.trip_updates.snapshots.v1", "mbta.alerts.snapshots.v1",
    "mbta.control.v1",
)
CTX = ParseContext(
    run_id=RUN, scope_id=SCOPE, gtfs_version=GTFS, clock_mode="source", pipeline_version="p1",
    feed_topics={"vehicle_positions": VP, "trip_updates": TU, "alerts": AL}, control_topic=CTL,
    timestamp_ms_threshold=100_000_000_000, excerpt_max_chars=40,
)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
FETCHED = "2026-10-05T12:00:30.123Z"
FETCHED_MICROS = (datetime(2026, 10, 5, 12, 0, 30, 123000, tzinfo=UTC) - EPOCH) // timedelta(microseconds=1)

KAFKA_SCHEMA = T.StructType([
    T.StructField("key", T.BinaryType()), T.StructField("value", T.BinaryType()),
    T.StructField("topic", T.StringType()), T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()), T.StructField("timestamp", T.TimestampType()),
])


def envelope(feed: str = "vehicle_positions", ts: int = 1791201620, sid: str | None = None, **over) -> dict:
    ents = [{"id": "e1", "vehicle": {"vehicle": {"id": "V1"}, "timestamp": ts - 10}}, {"id": "e2"}]
    env = {
        "schema_version": 1, "message_type": "feed_snapshot", "run_id": RUN, "scope_id": SCOPE,
        "source_type": "synthetic", "scenario_id": None, "feed_type": feed,
        "snapshot_id": sid or ("sn_" + H(K("t", feed, ts))), "capture_id": f"vp-{ts}Z-abcdefabcdef",
        "source_url": "https://example.invalid/x", "gtfs_version": GTFS, "feed_timestamp": ts,
        "original_fetched_at": FETCHED, "published_at": "2026-10-05T12:00:31.000Z",
        "raw_sha256": "r" * 64, "raw_path": f"{RUN}/{feed}/2026-10-05/cap.json.gz", "raw_bytes": 100,
        "http": {"status": 200}, "source_entity_count": 5, "selected_entity_count": 2, "scope_accounting": {},
        "header": {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET", "timestamp": ts},
        "entities": ents, "producer": {"component": "test", "version": "0"},
    }
    env.update(over)
    return env


def msg(value, *, topic: str = VP, key: str | None = None, offset: int = 0, partition: int = 0,
        nokey: bool = False):
    if isinstance(value, dict):
        value = json.dumps(value)
    if key is None and not nokey:
        feed = {VP: "vehicle_positions", TU: "trip_updates", AL: "alerts", CTL: "control"}[topic]
        key = f"{RUN}|{feed}"
    return (key.encode() if key is not None else None, value.encode() if value is not None else None, topic,
            partition, offset, datetime(2026, 10, 5, 12, 0, 31, tzinfo=UTC))


def frame(spark, rows):
    return parse.filter_run(parse.kafka_columns(spark.createDataFrame(rows, KAFKA_SCHEMA)), RUN)


def reasons(spark, rows) -> list:
    sp = parse.parse_snapshots(frame(spark, rows), CTX)
    return [r["message_reason"] for r in sp.orderBy("kafka_offset").select("message_reason").collect()]


# ------------------------------------------------------------------------------------------ tests
def test_valid_envelope_projects_parsed_snapshot_columns(spark):
    env = envelope()
    sp = parse.parse_snapshots(frame(spark, [msg(env, offset=3)]), CTX)
    ok = parse.ok_snapshots(sp)
    assert set(PARSED_SNAPSHOT_COLUMNS) <= set(ok.columns)
    row = ok.collect()[0]
    assert row["snapshot_id"] == env["snapshot_id"] and row["feed_type"] == "vehicle_positions"
    assert row["source_order"] == f"{1791201620:012d}.{FETCHED_MICROS:017d}.{env['snapshot_id']}"
    assert row["original_fetched_at_ms"] == FETCHED_MICROS // 1000
    assert row["kafka_topic"] == VP and row["kafka_offset"] == 3 and row["delivery_rank"] == 1
    assert len(row["entities"]) == 2 and json.loads(row["entities"][0])["id"] == "e1"
    assert row["header"]["timestamp"] == 1791201620
    assert row["value_sha256"] == hashlib.sha256(json.dumps(env).encode()).hexdigest()
    snap = parse.snapshot_frame(ok)
    assert tuple(snap.columns) == PARSED_SNAPSHOT_COLUMNS


def test_run_filter_is_exact_key_match(spark):
    rows = [
        msg(envelope(), offset=0),
        msg(envelope(run_id="run-10"), key="run-10|vehicle_positions", offset=1),
        msg(envelope(), key="run-1x|vehicle_positions", offset=2),
        msg(envelope(), nokey=True, offset=3),
    ]
    assert [r["kafka_offset"] for r in frame(spark, rows).collect()] == [0]


def test_corrupt_json_is_invalid_json_with_bounded_excerpt(spark):
    bad = '{"schema_version": 1, "run_id": "run-1", ' + "x" * 100
    sp = parse.parse_snapshots(frame(spark, [msg(bad, offset=1), msg("", offset=2)]), CTX)
    rows = sp.orderBy("kafka_offset").collect()
    assert [r["message_reason"] for r in rows] == ["INVALID_JSON", "INVALID_JSON"]
    assert len(rows[0]["excerpt"]) == 40 and rows[0]["excerpt_truncated"] is True


@pytest.mark.parametrize("over,key,expected", [
    ({"schema_version": 2}, None, "UNSUPPORTED_SCHEMA"),
    ({"message_type": "other"}, None, "UNSUPPORTED_SCHEMA"),
    ({"selected_entity_count": 3}, None, "UNSUPPORTED_SCHEMA"),
    ({}, f"{RUN}|alerts", "INVALID_HEADER"),                         # key feed != topic feed
    ({"feed_type": "alerts"}, None, "INVALID_HEADER"),
    ({"scope_id": "other"}, None, "INVALID_HEADER"),
    ({"run_id": "run-2"}, None, "INVALID_HEADER"),
    ({"feed_timestamp": 1791201621}, None, "INVALID_HEADER"),
    ({"original_fetched_at": "not-a-time"}, None, "INVALID_HEADER"),
    ({"snapshot_id": "bad-id"}, None, "INVALID_HEADER"),
    ({"gtfs_version": "h" * 64}, None, "STATIC_VERSION_MISMATCH"),
    ({"schema_version": 2, "gtfs_version": "h" * 64}, None, "UNSUPPORTED_SCHEMA"),   # precedence
])
def test_message_reason_precedence(spark, over, key, expected):
    assert reasons(spark, [msg(envelope(**over), key=key)]) == [expected]


def test_header_timestamp_checks(spark):
    ms_ts = 1791201620000
    env_ms = envelope(ts=ms_ts)
    env_zero = envelope(ts=0)
    assert reasons(spark, [msg(env_ms, offset=0), msg(env_zero, offset=1)]) == ["INVALID_HEADER", "INVALID_HEADER"]


def test_incrementality(spark):
    h = {"gtfs_realtime_version": "2.0", "timestamp": 1791201620}
    rows = [
        msg(envelope(header={**h, "incrementality": "DIFFERENTIAL"}), offset=0),
        msg(envelope(header=h), offset=1),                                   # absent = FULL_DATASET
        msg(envelope(header={**h, "incrementality": 0}), offset=2),          # proto enum 0
    ]
    assert reasons(spark, rows) == ["UNSUPPORTED_INCREMENTALITY", None, None]


def test_duplicate_delivery_ranks_and_source_order(spark):
    s1 = envelope(ts=1791201620)
    s2 = envelope(ts=1791201660)
    rows = [msg(s2, offset=1), msg(s1, offset=5), msg(s1, offset=9)]
    ok = parse.ok_snapshots(parse.parse_snapshots(frame(spark, rows), CTX))
    got = {(r["snapshot_id"], r["kafka_offset"]): r["delivery_rank"] for r in ok.collect()}
    assert got == {(s2["snapshot_id"], 1): 1, (s1["snapshot_id"], 5): 1, (s1["snapshot_id"], 9): 2}
    metas = parse.meta_frame(ok).orderBy("feed_type", "source_order").collect()
    assert [m["snapshot_id"] for m in metas] == [s1["snapshot_id"], s2["snapshot_id"]]   # ordered by header ts
    dups = parse.duplicate_frame(ok).collect()
    assert len(dups) == 1 and dups[0]["duplicates"] == 1 and dups[0]["refs"] == [f"{VP}:0:9"]
    assert {c for c in parse.META_COLUMNS} == set(metas[0].asDict())


def test_source_order_is_fixed_width_lexicographic():
    a = f"{999:012d}.{5:017d}.sn_b"
    b = f"{1000:012d}.{1:017d}.sn_a"
    assert a < b


def test_message_rejects_are_dlq_rows_with_stable_error_id(spark):
    bad = '{"broken": '
    rows = [msg(bad, offset=7), msg(envelope(gtfs_version="h" * 64), offset=8)]
    sp = parse.parse_snapshots(frame(spark, rows), CTX)
    cp = parse.parse_controls(frame(spark, rows), CTX)
    rej = parse.message_rejects(sp, cp, CTX, batch_id=12, detected_at_iso="2026-10-05T12:00:32.000Z")
    assert tuple(rej.columns) == DLQ_FIELDS
    out = {r["source_offset"]: r for r in rej.collect()}
    r7 = out[7]
    sha = hashlib.sha256(bad.encode()).hexdigest()
    assert r7["error_id"] == error_id_message(RUN, VP, sha, "INVALID_JSON")
    assert r7["reason_code"] == "INVALID_JSON" and r7["feed_type"] == "vehicle_positions"
    assert r7["severity"] == str(QUALITY_REASONS["INVALID_JSON"].severity)
    assert r7["disposition"] == str(QUALITY_REASONS["INVALID_JSON"].disposition)
    assert r7["batch_id"] == 12 and r7["stage"] == "parse" and r7["excerpt"] == bad
    assert out[8]["reason_code"] == "STATIC_VERSION_MISMATCH" and out[8]["snapshot_id"].startswith("sn_")
    assert out[8]["raw_path"].startswith(RUN)


def control(event_type: str, seq: int | None, clock: str | None = "2026-10-05T12:00:30.000Z", **over) -> dict:
    ev = {"schema_version": 1, "message_type": "control", "event_type": event_type, "event_id": "ev_1",
          "run_id": RUN, "source_type": "synthetic", "emitted_at": "2026-10-05T12:00:30.500Z",
          "clock_sequence": seq, "source_clock": clock, "speed": 1.0, "run_state": "RUNNING", "reason": None,
          "producer": {"component": "publisher", "version": "0"}}
    ev.update(over)
    return ev


def test_control_parsing_and_clock_events(spark):
    rows = [
        msg(control("clock_tick", 3), topic=CTL, offset=0),
        msg(control("clock_tick", 4, "2026-10-05T12:01:00.000Z"), topic=CTL, offset=1),
        msg(control("clock_tick", 5), topic=CTL, key=f"{RUN}|vehicle_positions", offset=2),   # wrong key
        msg(control("mystery_event", 6), topic=CTL, offset=3),
        msg(control("clock_tick", None), topic=CTL, offset=4),
        msg(control("collector_status", None, None, feed_type="alerts", outcome="ok", feed_state="healthy"),
            topic=CTL, offset=5),
    ]
    cp = parse.parse_controls(frame(spark, rows), CTX)
    got = [r["message_reason"] for r in cp.orderBy("kafka_offset").collect()]
    assert got == [None, None, "INVALID_HEADER", "UNSUPPORTED_SCHEMA", "INVALID_HEADER", None]
    ev = parse.clock_event_frame(cp).collect()
    assert [e["clock_sequence"] for e in ev] == [4, 3]          # newest first
    assert ev[0]["source_clock_ms"] == 1791201660000
    st = parse.status_event_frame(cp).collect()
    assert len(st) == 1 and st[0]["feed_type"] == "alerts" and st[0]["feed_state"] == "healthy"
    assert st[0]["progress_text"].startswith("{")


def test_h_k_mirror_matches_python(spark):
    row = spark.range(1).select(
        parse.h_k(F.lit("err"), F.lit("v1"), F.lit(None).cast("string"), F.lit(7)).alias("h")).collect()[0]
    assert row["h"] == H(K("err", "v1", None, 7))


def test_deliveries_frame_has_one_row_per_delivery(spark):
    s1 = envelope()
    rows = [msg(s1, offset=0), msg(s1, offset=1), msg("{bad", offset=2)]
    sp = parse.parse_snapshots(frame(spark, rows), CTX)
    ok = parse.ok_snapshots(sp)
    d = parse.deliveries_frame(sp, ok, batch_id=4, run_id=RUN).orderBy("kafka_offset").collect()
    assert [(r["delivery_rank"], r["is_duplicate_delivery"], r["message_reason"]) for r in d] == [
        (1, False, None), (2, True, None), (None, False, "INVALID_JSON")]
