"""Kafka rows -> parsed snapshot envelopes, control events and message-level rejects (design B §3-§4).

All parsing is Spark DataFrame work with explicit schemas (``processing/schemas.py``); nothing is
inferred and a malformed message never disappears as a null row:

1. :func:`kafka_columns` casts the binary key/value and keeps ``sha2(value)`` (``value_sha256``), topic,
   partition, offset.
2. :func:`filter_run` keeps only this run's messages: ``substring_index(key, '|', 1) == RUN_ID`` (an exact
   match on the first key component, never a prefix test).
3. :func:`parse_snapshots` / :func:`parse_controls` run ``from_json`` (PERMISSIVE + ``_corrupt``) and set
   ``message_reason`` with this precedence (first match wins):

   ``INVALID_JSON`` (unparseable / not an object)
   > ``UNSUPPORTED_SCHEMA`` (schema_version != 1, wrong message_type, ``size(entities) !=
   selected_entity_count``; unknown control event type)
   > ``INVALID_HEADER`` (key != ``run_id|feed_type`` of the topic, envelope run/feed/scope mismatch,
   missing snapshot/capture id, header timestamp null / <= 0 / > ``policy.timestamp_ms_threshold``,
   ``feed_timestamp != header.timestamp``, unparseable ``original_fetched_at`` / ``published_at``)
   > ``STATIC_VERSION_MISMATCH`` (envelope gtfs_version != the run's)
   > ``UNSUPPORTED_INCREMENTALITY`` (``header.incrementality`` present and not ``FULL_DATASET``/``0``;
   absent means FULL_DATASET, the GTFS-RT proto default).

4. :func:`ok_snapshots` projects valid envelopes to ``context.PARSED_SNAPSHOT_COLUMNS`` plus
   ``source_order = format_string('%012d.%017d.%s', feed_timestamp, unix_micros(original_fetched_at),
   snapshot_id)`` and ``delivery_rank = row_number() OVER (PARTITION BY feed_type, snapshot_id ORDER BY
   source_order, kafka_partition, kafka_offset)``; ``delivery_rank > 1`` is an in-batch duplicate delivery.

Timestamps are never collected to Python as ``TimestampType`` (PySpark would produce naive LOCAL
datetimes); driver-side metadata uses epoch milliseconds (``*_ms`` columns).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from transitpulse.contracts.control import ALL_EVENTS, CLOCK_EVENTS
from transitpulse.contracts.control import MESSAGE_TYPE_CONTROL as CONTROL_MESSAGE_TYPE
from transitpulse.contracts.control import SCHEMA_VERSION as CONTROL_SCHEMA_VERSION
from transitpulse.contracts.dlq import DLQ_FIELDS
from transitpulse.contracts.dlq import SCHEMA_VERSION as DLQ_SCHEMA_VERSION
from transitpulse.contracts.enums import QUALITY_REASONS
from transitpulse.contracts.envelope import MESSAGE_TYPE_SNAPSHOT
from transitpulse.contracts.envelope import SCHEMA_VERSION as ENVELOPE_SCHEMA_VERSION
from transitpulse.processing.context import PARSED_SNAPSHOT_COLUMNS
from transitpulse.processing.schemas import CONTROL_SCHEMA, CORRUPT_COL, ENVELOPE_SCHEMA, JSON_OPTIONS

MESSAGE_REASONS: tuple[str, ...] = (
    "INVALID_JSON", "UNSUPPORTED_SCHEMA", "INVALID_HEADER", "STATIC_VERSION_MISMATCH",
    "UNSUPPORTED_INCREMENTALITY",
)
SUPPORTED_INCREMENTALITY: tuple[str, ...] = ("FULL_DATASET", "0")
CONTROL_FEED = "control"
STATUS_EVENTS: tuple[str, ...] = ("collector_status", "collector_heartbeat", "publisher_heartbeat")
PROGRESS_TEXT_MAX_CHARS = 8000

# Columns of one snapshot delivery kept on the driver (small; no entities).
META_COLUMNS: tuple[str, ...] = (
    "feed_type", "snapshot_id", "capture_id", "source_order", "feed_timestamp", "original_fetched_at_ms",
    "published_at_ms", "kafka_topic", "kafka_partition", "kafka_offset", "raw_path", "raw_sha256",
    "gtfs_version", "scope_id", "scenario_id", "source_type", "source_entity_count", "selected_entity_count",
    "value_sha256",
)


@dataclass(frozen=True)
class ParseContext:
    run_id: str
    scope_id: str
    gtfs_version: str
    clock_mode: str                      # wall | source
    pipeline_version: str
    feed_topics: Mapping[str, str]       # feed_type -> topic
    control_topic: str
    timestamp_ms_threshold: int
    excerpt_max_chars: int
    topics_feed: dict[str, str] = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "topics_feed", {t: f for f, t in self.feed_topics.items()})

    @classmethod
    def from_config(cls, cfg, *, run_id: str, scope_id: str, gtfs_version: str, clock_mode: str,
                    pipeline_version: str, timestamp_ms_threshold: int | None = None) -> ParseContext:
        t = cfg.kafka.topics
        return cls(
            run_id=run_id,
            scope_id=scope_id,
            gtfs_version=gtfs_version,
            clock_mode=clock_mode,
            pipeline_version=pipeline_version,
            feed_topics={"vehicle_positions": t.vehicle_positions, "trip_updates": t.trip_updates,
                         "alerts": t.alerts},
            control_topic=t.control,
            timestamp_ms_threshold=int(timestamp_ms_threshold or cfg.policy.timestamp_ms_threshold),
            excerpt_max_chars=int(cfg.kafka.dlq_excerpt_max_chars),
        )


# --------------------------------------------------------------------------------------------------
# Small column helpers
# --------------------------------------------------------------------------------------------------

def _true(c: Column) -> Column:
    """NULL-safe boolean: NULL -> False."""
    return F.coalesce(c, F.lit(False))


def _neq(a: Column, b: Column) -> Column:
    """NULL-safe inequality (NULL vs value is 'different'; NULL vs NULL is 'equal')."""
    return ~a.eqNullSafe(b)


def _blank_value() -> Column:
    """NULL or whitespace-only Kafka value (INVALID_JSON whatever from_json returns for empty input)."""
    return F.col("v").isNull() | _true(F.length(F.trim(F.col("v"))) == F.lit(0))


def _quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def unix_micros(name: str) -> Column:
    """Exact epoch microseconds of a TIMESTAMP column (SQL ``unix_micros``, Spark >= 3.1)."""
    fn = getattr(F, "unix_micros", None)
    if fn is not None:
        return fn(F.col(name).cast("timestamp"))
    return F.expr(f"unix_micros(CAST({_quote(name)} AS TIMESTAMP))")


def unix_millis(name: str) -> Column:
    """Epoch milliseconds of a TIMESTAMP column, truncating sub-ms precision (SQL ``unix_millis``)."""
    fn = getattr(F, "unix_millis", None)
    if fn is not None:
        return fn(F.col(name).cast("timestamp"))
    return F.expr(f"unix_millis(CAST({_quote(name)} AS TIMESTAMP))")


def h_k(*parts: Column) -> Column:
    """Spark mirror of ``contracts.ids.H(K(*parts))`` (\\x1f separator, NULL -> \\x00, sha256[:32])."""
    joined = F.concat_ws("\u001f", *[F.coalesce(p.cast("string"), F.lit("\u0000")) for p in parts])
    return F.substring(F.sha2(joined, 256), 1, 32)


def message_error_id(run_id: str) -> Column:
    """``contracts.ids.error_id_message(run_id, topic, value_sha256, reason)`` over columns."""
    return F.concat(
        F.lit("er_"),
        h_k(F.lit("err"), F.lit("v1"), F.lit(run_id), F.col("topic"), F.col("value_sha256"), F.col("message_reason")),
    )


def _reason_lookup(attr: str) -> Column:
    """severity / disposition / unit of ``message_reason`` from contracts.enums.QUALITY_REASONS."""
    expr: Column | None = None
    for code in MESSAGE_REASONS:
        val = str(getattr(QUALITY_REASONS[code], attr))
        cond = F.col("message_reason") == F.lit(code)
        expr = F.when(cond, F.lit(val)) if expr is None else expr.when(cond, F.lit(val))
    assert expr is not None
    return expr


def _excerpt_cols(reason: Column, max_chars: int) -> list[Column]:
    v = F.col("v")
    return [
        F.when(reason.isNotNull(), F.substring(v, 1, max_chars)).alias("excerpt"),
        F.when(reason.isNotNull(), _true(F.length(v) > F.lit(max_chars))).alias("excerpt_truncated"),
    ]


# --------------------------------------------------------------------------------------------------
# Stage 1-2: Kafka columns + run filter
# --------------------------------------------------------------------------------------------------

def kafka_columns(df: DataFrame) -> DataFrame:
    """Kafka source rows -> key/value strings, value hash and coordinates."""
    return df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("v"),
        F.sha2(F.col("value"), 256).alias("value_sha256"),
        F.col("topic").cast("string").alias("topic"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )


def filter_run(df: DataFrame, run_id: str) -> DataFrame:
    """Exact run match on the first key component (``run_id|feed_type`` / ``run_id|control``)."""
    return df.where(F.substring_index(F.col("key"), "|", 1) == F.lit(run_id))


def topic_feed_col(ctx: ParseContext) -> Column:
    expr: Column | None = None
    for topic, feed in sorted(ctx.topics_feed.items()):
        cond = F.col("topic") == F.lit(topic)
        expr = F.when(cond, F.lit(feed)) if expr is None else expr.when(cond, F.lit(feed))
    assert expr is not None
    return expr


# --------------------------------------------------------------------------------------------------
# Stage 3a: snapshot envelopes
# --------------------------------------------------------------------------------------------------

def snapshot_reason(ctx: ParseContext) -> Column:
    env = F.col("env")
    hdr = env.getField("header")
    hts = hdr.getField("timestamp")
    entities = env.getField("entities")
    sel = env.getField("selected_entity_count")
    invalid_json = _blank_value() | env.isNull() | env.getField(CORRUPT_COL).isNotNull()
    unsupported_schema = (
        _neq(env.getField("schema_version"), F.lit(ENVELOPE_SCHEMA_VERSION))
        | _neq(env.getField("message_type"), F.lit(MESSAGE_TYPE_SNAPSHOT))
        | entities.isNull()
        | sel.isNull()
        | _true(F.size(entities) != sel)
    )
    invalid_header = (
        _neq(F.col("key"), F.concat(F.lit(ctx.run_id + "|"), F.col("topic_feed")))
        | _neq(env.getField("run_id"), F.lit(ctx.run_id))
        | _neq(env.getField("feed_type"), F.col("topic_feed"))
        | _neq(env.getField("scope_id"), F.lit(ctx.scope_id))
        | ~_true(env.getField("snapshot_id").startswith("sn_"))
        | env.getField("capture_id").isNull()
        | hdr.isNull()
        | hts.isNull()
        | _true(hts <= F.lit(0))
        | _true(hts > F.lit(int(ctx.timestamp_ms_threshold)))
        | _neq(env.getField("feed_timestamp"), hts)
        | F.col("original_fetched_at_ts").isNull()
        | F.col("published_at_ts").isNull()
        | _true(unix_micros("original_fetched_at_ts") < F.lit(0))
    )
    static_mismatch = _neq(env.getField("gtfs_version"), F.lit(ctx.gtfs_version))
    incr = hdr.getField("incrementality")
    unsupported_incr = incr.isNotNull() & ~_true(incr.isin(*SUPPORTED_INCREMENTALITY))
    return (
        F.when(invalid_json, F.lit("INVALID_JSON"))
        .when(unsupported_schema, F.lit("UNSUPPORTED_SCHEMA"))
        .when(invalid_header, F.lit("INVALID_HEADER"))
        .when(static_mismatch, F.lit("STATIC_VERSION_MISMATCH"))
        .when(unsupported_incr, F.lit("UNSUPPORTED_INCREMENTALITY"))
        .otherwise(F.lit(None).cast("string"))
    )


def parse_snapshots(m: DataFrame, ctx: ParseContext) -> DataFrame:
    """Snapshot-topic messages with ``env`` (parsed struct) and ``message_reason`` (NULL = valid).

    The raw value text is dropped after the bounded DLQ excerpt is taken (only for rejected messages).
    """
    s = m.where(F.col("topic").isin(*sorted(ctx.topics_feed)))
    s = s.withColumn("topic_feed", topic_feed_col(ctx))
    s = s.withColumn("env", F.from_json(F.col("v"), ENVELOPE_SCHEMA, JSON_OPTIONS))
    s = s.withColumn("original_fetched_at_ts", F.col("env").getField("original_fetched_at").cast("timestamp"))
    s = s.withColumn("published_at_ts", F.col("env").getField("published_at").cast("timestamp"))
    s = s.withColumn("message_reason", snapshot_reason(ctx))
    s = s.select("*", *_excerpt_cols(F.col("message_reason"), ctx.excerpt_max_chars))
    return s.drop("v")


def ok_snapshots(parsed: DataFrame) -> DataFrame:
    """Valid snapshot deliveries: PARSED_SNAPSHOT_COLUMNS + ``value_sha256``, ``kafka_timestamp``,
    ``original_fetched_at_ms``, ``published_at_ms``, ``source_order`` and ``delivery_rank``."""
    env = F.col("env")
    base = parsed.where(F.col("message_reason").isNull()).select(
        env.getField("run_id").alias("run_id"),
        env.getField("scope_id").alias("scope_id"),
        env.getField("source_type").alias("source_type"),
        env.getField("scenario_id").alias("scenario_id"),
        env.getField("feed_type").alias("feed_type"),
        env.getField("snapshot_id").alias("snapshot_id"),
        env.getField("capture_id").alias("capture_id"),
        env.getField("gtfs_version").alias("gtfs_version"),
        env.getField("feed_timestamp").cast("long").alias("feed_timestamp"),
        F.timestamp_seconds(env.getField("feed_timestamp")).alias("feed_ts"),
        F.col("original_fetched_at_ts").alias("original_fetched_at"),
        F.col("published_at_ts").alias("published_at"),
        env.getField("raw_path").alias("raw_path"),
        env.getField("raw_sha256").alias("raw_sha256"),
        env.getField("source_entity_count").alias("source_entity_count"),
        env.getField("selected_entity_count").alias("selected_entity_count"),
        env.getField("header").alias("header"),
        env.getField("entities").alias("entities"),
        F.col("topic").alias("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("value_sha256"),
        F.col("kafka_timestamp"),
    )
    base = base.withColumn(
        "source_order",
        F.format_string("%012d.%017d.%s", F.col("feed_timestamp"), unix_micros("original_fetched_at"),
                        F.col("snapshot_id")),
    )
    base = base.withColumn("original_fetched_at_ms", unix_millis("original_fetched_at"))
    base = base.withColumn("published_at_ms", unix_millis("published_at"))
    w = Window.partitionBy("feed_type", "snapshot_id").orderBy("source_order", "kafka_partition", "kafka_offset")
    return base.withColumn("delivery_rank", F.row_number().over(w))


def snapshot_frame(ok: DataFrame) -> DataFrame:
    """Only the PARSED_SNAPSHOT_COLUMNS (the ``snap`` contract of the WP5 builders)."""
    return ok.select(*PARSED_SNAPSHOT_COLUMNS)


def meta_frame(ok: DataFrame) -> DataFrame:
    """Canonical (``delivery_rank = 1``) deliveries as small driver-side metadata."""
    return ok.where(F.col("delivery_rank") == 1).select(*META_COLUMNS)


def duplicate_frame(ok: DataFrame, max_refs: int = 50) -> DataFrame:
    """In-batch duplicate deliveries (``delivery_rank > 1``) per snapshot with bounded Kafka refs."""
    ref = F.concat_ws(":", F.col("kafka_topic"), F.col("kafka_partition").cast("string"),
                      F.col("kafka_offset").cast("string"))
    return (
        ok.where(F.col("delivery_rank") > 1)
        .groupBy("feed_type", "snapshot_id")
        .agg(F.count(F.lit(1)).alias("duplicates"), F.slice(F.sort_array(F.collect_list(ref)), 1, max_refs).alias("refs"))
    )


def deliveries_frame(parsed: DataFrame, ok: DataFrame, *, batch_id: int, run_id: str) -> DataFrame:
    """Curated ``snapshots`` dataset: one row per snapshot-topic delivery (valid ones incl. duplicates and
    rejected ones with ``message_reason``)."""
    good = ok.select(
        F.lit(run_id).alias("run_id"), F.lit(int(batch_id)).cast("long").alias("batch_id"),
        "feed_type", "snapshot_id", "capture_id", "kafka_topic", "kafka_partition", "kafka_offset",
        "kafka_timestamp", "value_sha256", "source_order", "feed_timestamp", "original_fetched_at",
        "published_at", "source_entity_count", "selected_entity_count",
        F.lit(None).cast("string").alias("message_reason"),
        F.col("delivery_rank").cast("int").alias("delivery_rank"),
        (F.col("delivery_rank") > 1).alias("is_duplicate_delivery"),
    )
    env = F.col("env")
    bad = parsed.where(F.col("message_reason").isNotNull()).select(
        F.lit(run_id).alias("run_id"), F.lit(int(batch_id)).cast("long").alias("batch_id"),
        F.col("topic_feed").alias("feed_type"),
        env.getField("snapshot_id").alias("snapshot_id"),
        env.getField("capture_id").alias("capture_id"),
        F.col("topic").alias("kafka_topic"), "kafka_partition", "kafka_offset", "kafka_timestamp",
        "value_sha256",
        F.lit(None).cast("string").alias("source_order"),
        env.getField("feed_timestamp").cast("long").alias("feed_timestamp"),
        F.col("original_fetched_at_ts").alias("original_fetched_at"),
        F.col("published_at_ts").alias("published_at"),
        env.getField("source_entity_count").alias("source_entity_count"),
        env.getField("selected_entity_count").alias("selected_entity_count"),
        F.col("message_reason"),
        F.lit(None).cast("int").alias("delivery_rank"),
        F.lit(False).alias("is_duplicate_delivery"),
    )
    return good.unionByName(bad)


# --------------------------------------------------------------------------------------------------
# Stage 3b: control events
# --------------------------------------------------------------------------------------------------

def control_reason(ctx: ParseContext) -> Column:
    ctl = F.col("ctl")
    et = ctl.getField("event_type")
    seq = ctl.getField("clock_sequence")
    invalid_json = _blank_value() | ctl.isNull() | ctl.getField(CORRUPT_COL).isNotNull()
    unsupported = (
        _neq(ctl.getField("schema_version"), F.lit(CONTROL_SCHEMA_VERSION))
        | _neq(ctl.getField("message_type"), F.lit(CONTROL_MESSAGE_TYPE))
        | et.isNull()
        | ~_true(et.isin(*sorted(ALL_EVENTS)))
    )
    is_clock = _true(et.isin(*sorted(CLOCK_EVENTS)))
    invalid_header = (
        _neq(F.col("key"), F.lit(f"{ctx.run_id}|{CONTROL_FEED}"))
        | _neq(ctl.getField("run_id"), F.lit(ctx.run_id))
        | (is_clock & (seq.isNull() | _true(seq < F.lit(0))))
    )
    if ctx.clock_mode == "source":
        invalid_header = invalid_header | (is_clock & F.col("source_clock_ts").isNull())
    return (
        F.when(invalid_json, F.lit("INVALID_JSON"))
        .when(unsupported, F.lit("UNSUPPORTED_SCHEMA"))
        .when(invalid_header, F.lit("INVALID_HEADER"))
        .otherwise(F.lit(None).cast("string"))
    )


def parse_controls(m: DataFrame, ctx: ParseContext) -> DataFrame:
    c = m.where(F.col("topic") == F.lit(ctx.control_topic))
    c = c.withColumn("ctl", F.from_json(F.col("v"), CONTROL_SCHEMA, JSON_OPTIONS))
    ctl = F.col("ctl")
    c = c.withColumn("source_clock_ts", ctl.getField("source_clock").cast("timestamp"))
    c = c.withColumn("emitted_at_ts", ctl.getField("emitted_at").cast("timestamp"))
    c = c.withColumn("message_reason", control_reason(ctx))
    progress = F.when(
        F.col("message_reason").isNull() & _true(ctl.getField("event_type").isin(*STATUS_EVENTS)),
        F.substring(F.col("v"), 1, PROGRESS_TEXT_MAX_CHARS),
    )
    c = c.select("*", progress.alias("progress_text"), *_excerpt_cols(F.col("message_reason"), ctx.excerpt_max_chars))
    return c.drop("v")


def clock_event_frame(ctl_parsed: DataFrame) -> DataFrame:
    """Valid clock events (epoch-ms clock) ordered newest first (callers bound with ``limit``)."""
    ctl = F.col("ctl")
    return (
        ctl_parsed.where(F.col("message_reason").isNull() & ctl.getField("event_type").isin(*sorted(CLOCK_EVENTS)))
        .select(
            ctl.getField("event_type").alias("event_type"),
            ctl.getField("event_id").alias("event_id"),
            ctl.getField("clock_sequence").alias("clock_sequence"),
            unix_millis("source_clock_ts").alias("source_clock_ms"),
            ctl.getField("run_state").alias("run_state"),
            ctl.getField("reason").alias("reason"),
            F.col("kafka_offset"),
        )
        .orderBy(F.col("clock_sequence").desc(), F.col("kafka_offset").desc())
    )


def status_event_frame(ctl_parsed: DataFrame) -> DataFrame:
    """Latest valid status/heartbeat event per (event_type, feed_type) — at most a handful of rows."""
    ctl = F.col("ctl")
    feed_key = F.coalesce(ctl.getField("feed_type"), F.lit(""))
    s = ctl_parsed.where(
        F.col("message_reason").isNull() & ctl.getField("event_type").isin(*STATUS_EVENTS)
    ).select(
        ctl.getField("event_type").alias("event_type"),
        ctl.getField("feed_type").alias("feed_type"),
        feed_key.alias("feed_key"),
        ctl.getField("outcome").alias("outcome"),
        ctl.getField("http_status").alias("http_status"),
        ctl.getField("reason_code").alias("reason_code"),
        ctl.getField("feed_state").alias("feed_state"),
        ctl.getField("run_state").alias("run_state"),
        ctl.getField("clock_sequence").alias("clock_sequence"),
        unix_millis("emitted_at_ts").alias("emitted_at_ms"),
        F.expr("unix_millis(CAST(ctl.last_success_fetched_at AS TIMESTAMP))").alias("last_success_fetched_at_ms"),
        F.col("progress_text"),
        F.col("kafka_offset"),
    )
    w = Window.partitionBy("event_type", "feed_key").orderBy(
        F.col("emitted_at_ms").desc_nulls_last(), F.col("kafka_offset").desc()
    )
    return s.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1).drop("rn")


# --------------------------------------------------------------------------------------------------
# Stage 3c: message-level rejects -> DLQ rows
# --------------------------------------------------------------------------------------------------

def message_rejects(
    snap_parsed: DataFrame,
    ctl_parsed: DataFrame,
    ctx: ParseContext,
    *,
    batch_id: int,
    detected_at_iso: str,
) -> DataFrame:
    """Rejected messages as DLQ rows (columns ``contracts.dlq.DLQ_FIELDS``)."""
    env = F.col("env")
    s = snap_parsed.where(F.col("message_reason").isNotNull()).select(
        F.col("topic_feed").alias("feed_type"), "topic", "kafka_partition", "kafka_offset", "value_sha256",
        "message_reason", "excerpt", "excerpt_truncated",
        env.getField("snapshot_id").alias("snapshot_id"),
        env.getField("capture_id").alias("capture_id"),
        env.getField("schema_version").alias("envelope_schema_version"),
        env.getField("raw_path").alias("raw_path"),
        env.getField("raw_sha256").alias("raw_sha256"),
    )
    ctl = F.col("ctl")
    c = ctl_parsed.where(F.col("message_reason").isNotNull()).select(
        F.lit(CONTROL_FEED).alias("feed_type"), "topic", "kafka_partition", "kafka_offset", "value_sha256",
        "message_reason", "excerpt", "excerpt_truncated",
        F.lit(None).cast("string").alias("snapshot_id"),
        F.lit(None).cast("string").alias("capture_id"),
        ctl.getField("schema_version").alias("envelope_schema_version"),
        F.lit(None).cast("string").alias("raw_path"),
        F.lit(None).cast("string").alias("raw_sha256"),
    )
    u = s.unionByName(c)
    cols = {
        "schema_version": F.lit(DLQ_SCHEMA_VERSION).cast("int"),
        "error_id": message_error_id(ctx.run_id),
        "run_id": F.lit(ctx.run_id),
        "feed_type": F.col("feed_type"),
        "source_topic": F.col("topic"),
        "source_partition": F.col("kafka_partition").cast("int"),
        "source_offset": F.col("kafka_offset").cast("long"),
        "value_sha256": F.col("value_sha256"),
        "snapshot_id": F.col("snapshot_id"),
        "capture_id": F.col("capture_id"),
        "entity_ref": F.lit(None).cast("string"),
        "sub_ref": F.lit(None).cast("string"),
        "reason_code": F.col("message_reason"),
        "severity": _reason_lookup("severity"),
        "disposition": _reason_lookup("disposition"),
        "stage": F.lit("parse"),
        "envelope_schema_version": F.col("envelope_schema_version").cast("int"),
        "raw_path": F.col("raw_path"),
        "raw_sha256": F.col("raw_sha256"),
        "excerpt": F.col("excerpt"),
        "excerpt_truncated": F.coalesce(F.col("excerpt_truncated"), F.lit(False)),
        "batch_id": F.lit(int(batch_id)).cast("long"),
        "pipeline_version": F.lit(ctx.pipeline_version),
        "detected_at": F.lit(detected_at_iso),
    }
    missing = [f for f in DLQ_FIELDS if f not in cols]
    if missing:  # pragma: no cover - guarded by tests
        raise AssertionError(f"DLQ fields not mapped: {missing}")
    return u.select(*[cols[f].alias(f) for f in DLQ_FIELDS])


def union_all(frames: list[DataFrame]) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
