"""Dead-letter output to Kafka ``mbta.dlq.v1`` (design B §6, spec §16.1, contracts/dlq.py).

* Rows have exactly ``contracts.dlq.DLQ_FIELDS`` (canonical types in :data:`DLQ_SPARK_TYPES`).
* Key ``run_id|feed_type`` (``control`` for control-topic rejects), value = compact JSON with every field
  (nulls kept). Consumers deduplicate on ``error_id`` (stable across redelivery/replay: at-least-once).
* The excerpt is re-bounded here to ``kafka.dlq_excerpt_max_chars`` whatever the producer of the rows
  did; ``raw_path``/``raw_sha256`` reference the full raw capture.
* Message-level rows come from ``parse.message_rejects``; entity-level rows from the WP5 builders. Both
  are staged in the curated batch (``dlq`` dataset) and sent ONCE per batch from the staged files, before
  the curated commit, so a committed batch implies its DLQ records were sent.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from transitpulse.contracts.dlq import DLQ_FIELDS

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

DLQ_SPARK_TYPES: dict[str, str] = {
    "schema_version": "int", "error_id": "string", "run_id": "string", "feed_type": "string",
    "source_topic": "string", "source_partition": "int", "source_offset": "bigint", "value_sha256": "string",
    "snapshot_id": "string", "capture_id": "string", "entity_ref": "string", "sub_ref": "string",
    "reason_code": "string", "severity": "string", "disposition": "string", "stage": "string",
    "envelope_schema_version": "int", "raw_path": "string", "raw_sha256": "string", "excerpt": "string",
    "excerpt_truncated": "boolean", "batch_id": "bigint", "pipeline_version": "string", "detected_at": "string",
}
_SPARK_TYPE = {"int": T.IntegerType(), "bigint": T.LongType(), "string": T.StringType(), "boolean": T.BooleanType()}
DLQ_SPARK_SCHEMA = T.StructType([T.StructField(f, _SPARK_TYPE[DLQ_SPARK_TYPES[f]], True) for f in DLQ_FIELDS])

if set(DLQ_SPARK_TYPES) != set(DLQ_FIELDS):  # pragma: no cover - import-time contract guard
    raise AssertionError("DLQ_SPARK_TYPES must cover exactly contracts.dlq.DLQ_FIELDS")


def conform_dlq(df: DataFrame, *, max_excerpt_chars: int, batch_id: int | None = None,
                pipeline_version: str | None = None) -> DataFrame:
    """Select DLQ_FIELDS with canonical types; fill batch_id/pipeline_version when absent; bound excerpt."""
    cols = []
    for f in DLQ_FIELDS:
        t = DLQ_SPARK_TYPES[f]
        c = F.col(f).cast(t) if f in df.columns else F.lit(None).cast(t)
        if f == "batch_id" and batch_id is not None:
            c = F.coalesce(c, F.lit(int(batch_id)).cast("bigint"))
        elif f == "pipeline_version" and pipeline_version is not None:
            c = F.coalesce(c, F.lit(pipeline_version))
        elif f == "schema_version":
            c = F.coalesce(c, F.lit(1))
        cols.append(c.alias(f))
    out = df.select(*cols)
    long_excerpt = F.coalesce(F.length("excerpt") > F.lit(int(max_excerpt_chars)), F.lit(False))
    return (
        out.withColumn("excerpt_truncated", F.coalesce(F.col("excerpt_truncated"), F.lit(False)) | long_excerpt)
        .withColumn("excerpt", F.substring(F.col("excerpt"), 1, int(max_excerpt_chars)))
        .select(*DLQ_FIELDS)
    )


def kafka_records(df: DataFrame) -> DataFrame:
    """DLQ rows -> Kafka ``key``/``value`` strings."""
    key = F.concat(F.coalesce(F.col("run_id"), F.lit("")), F.lit("|"), F.coalesce(F.col("feed_type"), F.lit("unknown")))
    value = F.to_json(F.struct(*[F.col(f) for f in DLQ_FIELDS]), {"ignoreNullFields": "false"})
    return df.select(key.alias("key"), value.alias("value"))


def write_to_kafka(df: DataFrame, *, bootstrap_servers: str, topic: str) -> None:
    """Batch write to Kafka (raises on failure: the micro-batch then fails and is replayed)."""
    (kafka_records(df).write.format("kafka")
     .option("kafka.bootstrap.servers", bootstrap_servers)
     .option("topic", topic)
     .save())


def send_staged(spark: SparkSession, files: list[str], *, bootstrap_servers: str, topic: str) -> int:
    """Send the DLQ rows of the staged parquet files; returns the number of rows sent (0 = no Kafka job)."""
    if not files:
        return 0
    df = spark.read.schema(DLQ_SPARK_SCHEMA).parquet(*files)
    n = df.count()
    if n:
        write_to_kafka(df, bootstrap_servers=bootstrap_servers, topic=topic)
    return int(n)
