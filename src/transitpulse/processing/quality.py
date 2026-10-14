"""Quality accounting helpers shared by the WP5 builders (vehicles / predictions / alerts).

Outputs built here:
  * ``quality_by_snapshot`` rows: one row per reason code with ``record_count``, ``disposition_counts``,
    ``sample_refs`` (<= 10, sorted) and ``details_json``; severity/unit come from
    ``contracts.enums.QUALITY_REASONS``.
  * ``entity_quality`` curated rows: one row per (record, reason).
  * entity-level DLQ rows with columns ``contracts.dlq.DLQ_FIELDS``.
  * the one-row ``stats`` DataFrame with ``context.STATS_COLUMNS`` and accounting identities.

Plus small Column helpers used by every builder (timestamp literals, blank->null, flag arrays).

Issue DataFrames ("issues") passed to :func:`quality_rows` / :func:`entity_quality_rows` have at least:
  ``reason_code`` (string), ``ref`` (string sample reference), ``disposition`` (string, nullable -> the
  registry default of the reason code).
Nothing here calls ``current_timestamp()``: every instant comes from the frozen ``BuildParams``.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from transitpulse.contracts.dlq import DLQ_FIELDS
from transitpulse.contracts.dlq import SCHEMA_VERSION as DLQ_SCHEMA_VERSION
from transitpulse.contracts.enums import QUALITY_REASONS
from transitpulse.contracts.envelope import iso_utc
from transitpulse.processing import spark_ids as S

if TYPE_CHECKING:  # pragma: no cover
    from transitpulse.processing.context import BuildParams, RunInfo

DEFAULT_DLQ_EXCERPT_MAX_CHARS = 2048          # config kafka.dlq_excerpt_max_chars (base.yaml)
MAX_SAMPLE_REFS = 10
ENVELOPE_SCHEMA_VERSION = 1                   # parse.py rejects every other schema_version before WP5

QUALITY_COLUMNS: tuple[str, ...] = (
    "run_id", "feed_type", "snapshot_id", "reason_code", "severity", "unit", "record_count",
    "disposition_counts", "sample_refs", "details_json",
)

ENTITY_QUALITY_COLUMNS: tuple[str, ...] = (
    "run_id", "snapshot_id", "feed_type", "entity_ref", "entity_pos", "sub_ref", "record_id", "route_id",
    "event_time", "reason_code", "severity", "disposition", "unit", "category", "entity_status",
    "metrics_as_of",
)

STRING_ARRAY = T.ArrayType(T.StringType())
INT_MAP = T.MapType(T.StringType(), T.IntegerType())

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


# --------------------------------------------------------------------------------------------------
# Time / literal helpers
# --------------------------------------------------------------------------------------------------

def epoch_micros(dt: datetime) -> int:
    """Exact integer microseconds since the Unix epoch of an AWARE datetime."""
    if dt.tzinfo is None:
        raise ValueError("naive datetime is not allowed; BuildParams instants must be timezone-aware UTC")
    d = dt - _EPOCH
    return d.days * 86_400_000_000 + d.seconds * 1_000_000 + d.microseconds


def epoch_seconds_floor(dt: datetime) -> int:
    return epoch_micros(dt) // 1_000_000


def ts_lit(dt: datetime | None) -> Column:
    """TimestampType literal built from exact epoch microseconds (no py4j datetime conversion, no TZ)."""
    if dt is None:
        return F.lit(None).cast("timestamp")
    return F.expr(f"timestamp_micros({epoch_micros(dt)})")


def str_lit(s: str | None) -> Column:
    return F.lit(s).cast("string") if s is not None else F.lit(None).cast("string")


def blank_to_null(c: Column) -> Column:
    """Empty / whitespace-only strings become null (IDs stay strings; missing never becomes '')."""
    return F.when(F.length(F.trim(c)) > 0, c)


def empty_string_array() -> Column:
    return F.array().cast(STRING_ARRAY)


def empty_int_map() -> Column:
    return F.create_map().cast(INT_MAP)


def flags_array(*codes: Column) -> Column:
    """Sorted distinct array of the non-null codes (deterministic set<text> value)."""
    if not codes:
        return empty_string_array()
    return F.array_sort(F.array_distinct(F.filter(F.array(*codes), lambda x: x.isNotNull())))


def merge_flags(arr: Column, *codes: Column) -> Column:
    """``arr`` plus the non-null ``codes``, sorted and distinct."""
    if not codes:
        return F.array_sort(F.array_distinct(F.coalesce(arr, empty_string_array())))
    extra = F.array(*codes)
    merged = F.concat(F.coalesce(arr, empty_string_array()), extra)
    return F.array_sort(F.array_distinct(F.filter(merged, lambda x: x.isNotNull())))


def sorted_int_map(key: Column, value: Column) -> Column:
    """Aggregate expression: deterministic map<string,int> from (key, value) pairs of a group."""
    entries = F.array_sort(F.collect_list(F.struct(key.cast("string").alias("k"), value.cast("int").alias("v"))))
    return F.map_from_entries(entries)


# --------------------------------------------------------------------------------------------------
# Reason registry lookups (contracts.enums.QUALITY_REASONS as literal maps)
# --------------------------------------------------------------------------------------------------

def _registry_map(attr: str) -> Column:
    pairs: list[Column] = []
    for code, info in sorted(QUALITY_REASONS.items()):
        pairs.append(F.lit(code))
        pairs.append(F.lit(str(getattr(info, attr))))
    return F.create_map(*pairs)


def severity_of(code: Column) -> Column:
    return _registry_map("severity")[code]


def disposition_of(code: Column) -> Column:
    return _registry_map("disposition")[code]


def unit_of(code: Column) -> Column:
    return _registry_map("unit")[code]


def category_of(code: Column) -> Column:
    return _registry_map("category")[code]


def is_warning_or_error(code: Column) -> Column:
    return F.coalesce(severity_of(code).isin("error", "warning"), F.lit(False))


def check_registered(codes: Sequence[str]) -> None:
    """Fail fast (driver side) when a builder uses an unregistered reason code."""
    missing = [c for c in codes if c not in QUALITY_REASONS]
    if missing:
        raise KeyError(f"unregistered quality reason(s): {missing}")


# --------------------------------------------------------------------------------------------------
# quality_by_snapshot
# --------------------------------------------------------------------------------------------------

def issue_frame(df: DataFrame, *, reason_code: Column, ref: Column, disposition: Column | None = None,
                extra: Sequence[Column] = ()) -> DataFrame:
    """Project ``df`` to the issue schema (reason_code, ref, disposition, *extra); null reason rows dropped."""
    disp = disposition if disposition is not None else F.lit(None).cast("string")
    return (
        df.select(reason_code.cast("string").alias("reason_code"), ref.cast("string").alias("ref"),
                  disp.cast("string").alias("disposition"), *extra)
        .where(F.col("reason_code").isNotNull())
    )


def snapshot_issue(spark, reason_code: str, ref: str | None, disposition: str | None = None) -> DataFrame:
    """A single snapshot-level issue row (e.g. PARENT_UNAVAILABLE)."""
    check_registered([reason_code])
    schema = T.StructType([
        T.StructField("reason_code", T.StringType(), True),
        T.StructField("ref", T.StringType(), True),
        T.StructField("disposition", T.StringType(), True),
    ])
    return spark.createDataFrame([(reason_code, ref, disposition)], schema)


def empty_issues(spark) -> DataFrame:
    schema = T.StructType([
        T.StructField("reason_code", T.StringType(), True),
        T.StructField("ref", T.StringType(), True),
        T.StructField("disposition", T.StringType(), True),
    ])
    return spark.createDataFrame([], schema)


def union_issues(frames: Sequence[DataFrame]) -> DataFrame:
    base = frames[0].select("reason_code", "ref", "disposition")
    for f in frames[1:]:
        base = base.unionByName(f.select("reason_code", "ref", "disposition"))
    return base


def quality_rows(issues: DataFrame, *, run_id: str, feed_type: str, snapshot_id: str,
                 policy_version: str | None = None) -> DataFrame:
    """Aggregate issue rows into ``quality_by_snapshot`` rows (one per reason code; PK unique)."""
    iss = issues.select(
        F.col("reason_code"),
        F.col("ref"),
        F.coalesce(F.col("disposition"), disposition_of(F.col("reason_code")), F.lit("retained")).alias("disposition"),
    )
    by_disp = iss.groupBy("reason_code", "disposition").agg(
        F.count(F.lit(1)).alias("_n"),
        F.collect_set("ref").alias("_refs"),
    )
    agg = by_disp.groupBy("reason_code").agg(
        F.sum("_n").alias("_total"),
        sorted_int_map(F.col("disposition"), F.col("_n")).alias("disposition_counts"),
        F.array_sort(F.array_distinct(F.flatten(F.collect_list("_refs")))).alias("_all_refs"),
    )
    code = F.col("reason_code")
    details = F.to_json(F.struct(
        category_of(code).alias("category"),
        disposition_of(code).alias("default_disposition"),
        F.size(F.col("_all_refs")).alias("distinct_refs"),
        F.lit(policy_version).cast("string").alias("policy_version"),
    ))
    return agg.select(
        str_lit(run_id).alias("run_id"),
        str_lit(feed_type).alias("feed_type"),
        str_lit(snapshot_id).alias("snapshot_id"),
        code.alias("reason_code"),
        severity_of(code).alias("severity"),
        unit_of(code).alias("unit"),
        F.col("_total").cast("int").alias("record_count"),
        F.col("disposition_counts"),
        F.slice(F.col("_all_refs"), 1, MAX_SAMPLE_REFS).alias("sample_refs"),
        details.alias("details_json"),
    )


# --------------------------------------------------------------------------------------------------
# entity_quality (curated)
# --------------------------------------------------------------------------------------------------

def entity_quality_rows(issues: DataFrame, *, params: BuildParams, feed_type: str) -> DataFrame:
    """Curated per-record issue rows. ``issues`` needs reason_code, disposition and may carry entity_ref,
    entity_pos, sub_ref, record_id, route_id, event_time, entity_status (missing ones become null)."""
    cols = set(issues.columns)

    def opt(name: str, dtype: str) -> Column:
        return F.col(name).cast(dtype) if name in cols else F.lit(None).cast(dtype)

    code = F.col("reason_code")
    return issues.select(
        str_lit(params.run_id).alias("run_id"),
        str_lit(params.snapshot_id).alias("snapshot_id"),
        str_lit(feed_type).alias("feed_type"),
        opt("entity_ref", "string").alias("entity_ref"),
        opt("entity_pos", "int").alias("entity_pos"),
        opt("sub_ref", "string").alias("sub_ref"),
        opt("record_id", "string").alias("record_id"),
        opt("route_id", "string").alias("route_id"),
        opt("event_time", "timestamp").alias("event_time"),
        code.alias("reason_code"),
        severity_of(code).alias("severity"),
        F.coalesce(opt("disposition", "string"), disposition_of(code)).alias("disposition"),
        unit_of(code).alias("unit"),
        category_of(code).alias("category"),
        opt("entity_status", "string").alias("entity_status"),
        ts_lit(params.metrics_as_of).alias("metrics_as_of"),
    )


# --------------------------------------------------------------------------------------------------
# DLQ rows (entity level)
# --------------------------------------------------------------------------------------------------

def entity_dlq_rows(rejects: DataFrame, *, params: BuildParams, run: RunInfo, feed_type: str, stage: str,
                    max_excerpt_chars: int = DEFAULT_DLQ_EXCERPT_MAX_CHARS) -> DataFrame:
    """DLQ rows for rejected entities. ``rejects`` columns: entity_ref, sub_ref (nullable), reason_code,
    entity_json. ``error_id`` mirrors ``ids.error_id_entity``; ``disposition`` defaults to the registry value
    unless ``rejects`` carries a ``disposition`` column. ``detected_at`` = first materialization (stable)."""
    if max_excerpt_chars < 1:
        raise ValueError("max_excerpt_chars must be >= 1")
    code = F.col("reason_code")
    js = F.col("entity_json")
    disp = (F.coalesce(F.col("disposition"), disposition_of(code)) if "disposition" in rejects.columns
            else disposition_of(code))
    too_long = F.coalesce(F.length(js) > F.lit(max_excerpt_chars), F.lit(False))
    out = rejects.select(
        F.lit(DLQ_SCHEMA_VERSION).cast("int").alias("schema_version"),
        S.error_id_entity_col(str_lit(params.run_id), str_lit(feed_type), str_lit(params.snapshot_id),
                              F.col("entity_ref"), F.col("sub_ref"), code).alias("error_id"),
        str_lit(params.run_id).alias("run_id"),
        str_lit(feed_type).alias("feed_type"),
        str_lit(params.kafka_topic).alias("source_topic"),
        F.lit(int(params.kafka_partition)).cast("int").alias("source_partition"),
        F.lit(int(params.kafka_offset)).cast("long").alias("source_offset"),
        F.lit(None).cast("string").alias("value_sha256"),
        str_lit(params.snapshot_id).alias("snapshot_id"),
        str_lit(params.capture_id).alias("capture_id"),
        F.col("entity_ref").cast("string").alias("entity_ref"),
        F.col("sub_ref").cast("string").alias("sub_ref"),
        code.alias("reason_code"),
        severity_of(code).alias("severity"),
        disp.alias("disposition"),
        F.lit(stage).alias("stage"),
        F.lit(ENVELOPE_SCHEMA_VERSION).cast("int").alias("envelope_schema_version"),
        str_lit(params.raw_path).alias("raw_path"),
        str_lit(params.raw_sha256).alias("raw_sha256"),
        F.when(too_long, F.substring(js, 1, max_excerpt_chars)).otherwise(js).alias("excerpt"),
        too_long.alias("excerpt_truncated"),
        F.lit(int(params.first_batch_id)).cast("long").alias("batch_id"),
        str_lit(run.pipeline_version).alias("pipeline_version"),
        F.lit(iso_utc(params.first_materialized_at)).alias("detected_at"),
    )
    return out.select(*DLQ_FIELDS)


# --------------------------------------------------------------------------------------------------
# stats (one row, context.STATS_COLUMNS)
# --------------------------------------------------------------------------------------------------

def build_stats(base: DataFrame, *, accounting: Column, identities: Sequence[tuple[str, Column]]) -> DataFrame:
    """``base`` is ONE row with selected/fully_valid/partially_valid/rejected entity counts plus whatever
    columns ``accounting`` and ``identities`` reference. Every identity is a boolean Column; a false or null
    identity is reported in ``accounting_violations`` and makes ``accounting_ok`` false."""
    entity_identity = (
        "selected_entity_count == fully_valid + partially_valid + rejected",
        F.col("selected_entity_count")
        == F.col("fully_valid_entity_count") + F.col("partially_valid_entity_count") + F.col("rejected_entity_count"),
    )
    all_ids = [entity_identity, *identities]
    viol = F.filter(
        F.array(*[F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in all_ids]),
        lambda x: x.isNotNull(),
    )
    return base.select(
        F.col("selected_entity_count").cast("int").alias("selected_entity_count"),
        F.col("fully_valid_entity_count").cast("int").alias("fully_valid_entity_count"),
        F.col("partially_valid_entity_count").cast("int").alias("partially_valid_entity_count"),
        F.col("rejected_entity_count").cast("int").alias("rejected_entity_count"),
        (F.col("rejected_entity_count") > 0).alias("is_partial"),
        (F.col("selected_entity_count") == 0).alias("is_empty"),
        F.to_json(accounting).alias("accounting_json"),
        (F.size(viol) == 0).alias("accounting_ok"),
        F.when(F.size(viol) > 0, F.concat_ws("; ", viol)).alias("accounting_violations"),
    )


def entity_status_counts(ents: DataFrame, status_col: str = "entity_status") -> DataFrame:
    """One row: selected/fully/partially/rejected entity counts (zeros on an empty snapshot)."""
    st = F.col(status_col)
    return ents.agg(
        F.count(F.lit(1)).alias("selected_entity_count"),
        F.coalesce(F.sum(F.when(st == "fully_valid", 1).otherwise(0)), F.lit(0)).alias("fully_valid_entity_count"),
        F.coalesce(F.sum(F.when(st == "partially_valid", 1).otherwise(0)), F.lit(0))
        .alias("partially_valid_entity_count"),
        F.coalesce(F.sum(F.when(st == "rejected", 1).otherwise(0)), F.lit(0)).alias("rejected_entity_count"),
    )


def scope_routes_frame(spark, route_ids: Sequence[str]) -> DataFrame:
    """One row per distinct scope route (sorted) - the left side of every route_metrics join."""
    rows = [(r,) for r in sorted(set(route_ids))]
    return spark.createDataFrame(rows, T.StructType([T.StructField("route_id", T.StringType(), False)]))


def ensure_columns(df: DataFrame, spec: dict[str, str]) -> DataFrame:
    """Select exactly ``spec`` columns (name -> Spark SQL type), casting present ones, null for missing."""
    present = set(df.columns)
    return df.select(*[
        (F.col(n).cast(t) if n in present else F.lit(None).cast(t)).alias(n) for n, t in spec.items()
    ])
