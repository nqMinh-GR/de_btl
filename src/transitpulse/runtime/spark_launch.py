"""Launch helpers for the Spark processor and other Spark-using core commands.

The processor (``python -m transitpulse.processing.stream_job``) creates its SparkSession in-process and
reads the connector jars from runtime.json itself; the launcher only guarantees the interpreter, the JDK
and an environment free of the caller's PYTHONPATH/SPARK_HOME/PYSPARK_* (a Colab kernel ships its own
pyspark and Java).
"""

from __future__ import annotations

from transitpulse.runtime.manifest import RuntimeInfo, subprocess_env
from transitpulse.runtime.paths import WorkPaths


def processor_argv(runtime: RuntimeInfo, paths: WorkPaths, run_id: str, *, profile: str,
                   fault_injection: bool = False) -> list[str]:
    argv = [str(runtime.core_python), "-m", "transitpulse.processing.stream_job", "--workdir", str(paths.root),
            "--run-id", run_id, "--profile", profile]
    if fault_injection:
        argv.append("--enable-fault-injection")
    return argv


def spark_env(runtime: RuntimeInfo, paths: WorkPaths | None = None) -> dict[str, str]:
    """Environment for any process that starts a Spark JVM (processor, static loader, report job)."""
    core = str(runtime.core_python)
    extra = {
        "JAVA_HOME": str(runtime.java_home),
        "SPARK_HOME": str(runtime.spark_home),
        "PYSPARK_PYTHON": core,
        "PYSPARK_DRIVER_PYTHON": core,
        "SPARK_LOCAL_IP": "127.0.0.1",
        "TZ": "UTC",
        "PYTHONUNBUFFERED": "1",
    }
    if paths is not None:
        extra["TMPDIR"] = str(paths.tmp)
    return subprocess_env(runtime, extra)


def processor_env(runtime: RuntimeInfo) -> dict[str, str]:
    paths = WorkPaths.from_workdir(runtime.raw["workdir"]) if runtime.raw.get("workdir") else None
    return spark_env(runtime, paths)
