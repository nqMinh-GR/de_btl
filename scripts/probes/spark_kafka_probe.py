"""Spark Kafka connector probe, run by ``spark-submit`` during bootstrap (steps B7 and B10).

It never connects to a broker: it loads the connector classes through the driver's context class loader
(where ``--packages``/``--jars`` put them), builds a streaming Kafka source DataFrame (which exercises the
``kafka`` data source lookup and option validation without any network I/O) and reports the Spark,
Scala and Java versions. The result is printed as one line ``TP_PROBE_RESULT=<json>`` on stdout.
"""

from __future__ import annotations

import json
import sys

CLASSES = (
    "org.apache.spark.sql.kafka010.KafkaSourceProvider",
    "org.apache.spark.kafka010.KafkaTokenUtil",
    "org.apache.kafka.clients.consumer.KafkaConsumer",
    "org.apache.kafka.clients.producer.KafkaProducer",
    "org.apache.commons.pool2.impl.GenericKeyedObjectPool",
)


def main() -> int:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("transitpulse-connector-probe").getOrCreate()
    result: dict[str, object]
    try:
        jvm = spark.sparkContext._jvm
        loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
        if loader is None:
            loader = jvm.java.lang.ClassLoader.getSystemClassLoader()
        classes: dict[str, object] = {}
        for name in CLASSES:
            try:
                jvm.java.lang.Class.forName(name, False, loader)
                classes[name] = True
            except Exception as exc:  # noqa: BLE001 - reported in the probe result
                classes[name] = f"missing: {str(exc).splitlines()[0][:300]}"
        df = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", "127.0.0.1:1")
              .option("subscribe", "transitpulse-probe-unused")
              .option("startingOffsets", "earliest")
              .load())
        schema = [f.name for f in df.schema.fields]
        system = jvm.java.lang.System
        result = {
            "ok": all(v is True for v in classes.values()) and "value" in schema and "offset" in schema,
            "spark_version": spark.version,
            "scala_version": jvm.scala.util.Properties.versionNumberString(),
            "java_version": system.getProperty("java.version"),
            "java_vendor": system.getProperty("java.vendor"),
            "java_runtime": system.getProperty("java.runtime.version"),
            "java_home": system.getProperty("java.home"),
            "python": sys.version.split()[0],
            "python_executable": sys.executable,
            "classes": classes,
            "kafka_source_schema": schema,
        }
    except Exception as exc:  # noqa: BLE001 - the probe must always print a result line
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:2000]}
    finally:
        spark.stop()
    sys.stdout.write("TP_PROBE_RESULT=" + json.dumps(result, sort_keys=True) + "\n")
    sys.stdout.flush()
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
