"""Write ``<W>/runtime/runtime.json`` for the local-docker profile (run by the Compose ``runtime-init`` job).

The processor reads ``runtime.json`` (``runtime.manifest.load_runtime``) for the Spark connector jars and the core
interpreter. In the container there is no native bootstrap: Java comes from the ``eclipse-temurin`` base image,
the venv from ``uv sync`` and the connector jars were pre-fetched (sha1-verified against Maven Central) into
``/opt/spark-jars`` at image build time. Kafka and Cassandra are Compose services, so their ``home`` is empty and
``managed_by`` says so. Stdlib only; writes atomically; idempotent.

Profile local-docker is NOT_RUN in this repository's verification.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import platform
import sys
from pathlib import Path


def _java_release(java_home: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        for line in (java_home / "release").read_text(encoding="utf-8").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip('"')
    except OSError:
        pass
    return out


def _sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def build(workdir: Path, repo: Path, jars_dir: Path) -> dict:
    java_home = Path(os.environ.get("JAVA_HOME", "/opt/java/openjdk"))
    rel = _java_release(java_home)
    spec = importlib.util.find_spec("pyspark")
    if spec is None or not spec.origin:
        raise SystemExit("pyspark is not installed in this interpreter (/opt/venv)")
    spark_home = Path(spec.origin).parent
    version_file = spark_home / "version.py"
    spark_version = None
    if version_file.is_file():
        for line in version_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("__version__"):
                spark_version = line.split("=", 1)[1].strip().strip("\"'")
    jars = sorted(str(p) for p in jars_dir.glob("*.jar"))
    if not jars:
        raise SystemExit(f"no connector jars in {jars_dir}; rebuild the image")
    core_python = Path(sys.executable)
    path_env = os.pathsep.join([str(core_python.parent), str(java_home / "bin"), "/usr/local/sbin", "/usr/local/bin",
                                "/usr/sbin", "/usr/bin", "/sbin", "/bin"])
    return {
        "schema_version": 1,
        "profile": "local-docker",
        "repo_dir": str(repo),
        "repo_commit": os.environ.get("TP_REPO_COMMIT") or None,
        "workdir": str(workdir),
        "python": {"core_python": str(core_python), "version": platform.python_version(), "uv": "/usr/local/bin/uv",
                   "lock_sha256": _sha256(repo / "uv.lock"), "lock_origin": "committed"},
        "java": {"java_home": str(java_home), "version": rel.get("JAVA_VERSION"),
                 "vendor": rel.get("IMPLEMENTOR")},
        "kafka": {"home": "", "config": "", "log_dir": "", "bootstrap": "kafka:29092", "managed_by": "docker-compose"},
        "cassandra": {"home": "", "conf_dir": "", "host": "cassandra", "port": 9042, "managed_by": "docker-compose"},
        "spark": {"spark_home": str(spark_home), "version": spark_version, "scala": "2.12", "connector_jars": jars},
        "api": {"base_url": "http://api:8000/api/v1"},
        "paths": {"workdir": str(workdir), "raw_root": str(workdir / "data" / "raw"),
                  "checkpoints": str(workdir / "checkpoints"), "curated_root": str(workdir / "data" / "curated")},
        "hashes": {"lock": _sha256(repo / "uv.lock"), "connector_jars": hashlib.sha256(
            "\n".join(f"{j}:{_sha256(Path(j))}" for j in jars).encode()).hexdigest()},
        "env": {
            "JAVA_HOME": str(java_home), "PATH": path_env, "PYSPARK_PYTHON": str(core_python),
            "PYSPARK_DRIVER_PYTHON": str(core_python), "TZ": "UTC", "TP_WORKDIR": str(workdir),
            "TP_REPO_DIR": str(repo), "TP_PROFILE": "local-docker", "HOME": os.environ.get("HOME", str(workdir)),
            "LANG": "C.UTF-8",
        },
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="render runtime.json for the local-docker profile")
    ap.add_argument("--workdir", default=os.environ.get("TP_WORKDIR", "/work"))
    ap.add_argument("--repo-dir", default=os.environ.get("TP_REPO_DIR", "/app"))
    ap.add_argument("--jars-dir", default=os.environ.get("TP_SPARK_JARS_DIR", "/opt/spark-jars"))
    args = ap.parse_args(argv)
    workdir = Path(args.workdir).resolve()
    doc = build(workdir, Path(args.repo_dir).resolve(), Path(args.jars_dir))
    out = workdir / "runtime" / "runtime.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(f".runtime.json.tmp-{os.getpid()}")
    tmp.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, out)
    sys.stdout.write(json.dumps({"ok": True, "action": "render-runtime", "path": str(out),
                                 "connector_jars": doc["spark"]["connector_jars"], "error": None}) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
