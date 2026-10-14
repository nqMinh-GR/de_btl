"""In-process SparkSession factory (WP4; interfaces §7, design B §3, design D B7).

The processor, the static loader and the report job create Spark from Python (``pyspark`` launches the
JVM through ``spark-submit`` with every ``spark.*`` option given to the builder, so driver memory and
``spark.jars`` take effect). No ``spark.jars.packages`` / Ivy resolution ever happens at run time: the
Kafka connector closure is the list of local jars recorded in ``runtime.json`` by the bootstrap.

``runtime=None`` or ``with_kafka=False`` means no connector jars (static loader, Spark unit tests).
"""

from __future__ import annotations

import os
import sys
import time
from typing import TYPE_CHECKING, Any

from transitpulse.config import ProjectConfig
from transitpulse.obs.logging import get_logger, log_event
from transitpulse.runtime.paths import WorkPaths

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

_log = get_logger("transitpulse.processing.spark_session")


def _force_utc() -> None:
    """Python-side UTC (PySpark converts TimestampType to naive LOCAL datetimes) before the JVM starts."""
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()


def session_conf(
    cfg: ProjectConfig,
    runtime: Any | None = None,
    *,
    with_kafka: bool = True,
    extra_conf: dict[str, str] | None = None,
) -> dict[str, str]:
    """The exact Spark configuration used by :func:`build_session` (pure; unit-testable)."""
    conf: dict[str, str] = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": str(cfg.spark.shuffle_partitions),
        "spark.ui.enabled": "true" if cfg.spark.ui_enabled else "false",
        "spark.driver.host": "127.0.0.1",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.memory": cfg.spark.driver_memory,
        "spark.python.worker.reuse": "true",
        "spark.sql.ansi.enabled": "false",          # parse.py relies on NULL (not errors) for bad casts
        "spark.sql.codegen.wholeStage": "true" if cfg.spark.whole_stage_codegen else "false",
        "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
        "spark.executor.extraJavaOptions": "-Duser.timezone=UTC",
        "spark.sql.streaming.metricsEnabled": "false",
    }
    try:
        local_dir = WorkPaths.from_workdir(cfg.workdir).spark_local
        local_dir.mkdir(parents=True, exist_ok=True)
        conf["spark.local.dir"] = str(local_dir)
    except ValueError:
        pass  # no work directory configured (pure Spark unit tests): keep Spark's default temp dir
    if with_kafka and runtime is not None:
        jars = [str(j) for j in getattr(runtime, "connector_jars", ()) or ()]
        if not jars:
            raise RuntimeError("runtime.json lists no Spark Kafka connector jars; re-run the bootstrap")
        missing = [j for j in jars if not os.path.isfile(j)]
        if missing:
            raise FileNotFoundError(f"Spark Kafka connector jars are missing: {missing}; re-run the bootstrap")
        conf["spark.jars"] = ",".join(jars)
    if extra_conf:
        conf.update({str(k): str(v) for k, v in extra_conf.items()})
    return conf


def build_session(
    cfg: ProjectConfig,
    runtime: Any | None = None,
    app_name: str = "transitpulse",
    *,
    with_kafka: bool = True,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Create (or return the existing) SparkSession with UTC everywhere and the pinned connector jars."""
    _force_utc()
    # Workers must run the same Python 3.11 venv (cassandra-driver needs asyncore; Colab's system python is 3.12).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    if runtime is not None:
        env = getattr(runtime, "env", None) or {}
        for k in ("JAVA_HOME",):
            if env.get(k):
                os.environ.setdefault(k, env[k])

    from pyspark.sql import SparkSession

    conf = session_conf(cfg, runtime, with_kafka=with_kafka, extra_conf=extra_conf)
    builder = SparkSession.builder.master(cfg.spark.master).appName(app_name)
    for k, v in conf.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log_event(_log, "spark_session", "ok", "SparkSession ready", master=cfg.spark.master, app_name=app_name,
              spark_version=spark.version, jars=conf.get("spark.jars"))
    return spark
