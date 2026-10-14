"""Spark Kafka connector closure (bootstrap step B7, design D §2).

1. ``spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7`` with an Ivy settings file
   naming Maven Central as the only resolver runs ``scripts/probes/spark_kafka_probe.py`` (class probe,
   no broker connection).
2. Every jar Ivy retrieved into ``W/cache/ivy/jars`` is identified through the Ivy cache layout
   (org/module/revision), checked against Maven Central's ``.sha1`` and hashed with sha256.
3. Fail on any ``_2.13`` artifact, any Spark artifact that is not the pinned Spark version, or more
   than one ``kafka-clients``. Artifacts already shipped in ``$SPARK_HOME/jars`` are dropped
   (``provided_by_spark``) so no duplicate classes reach the classpath.
4. The remaining jars are copied to ``W/cache/jars/spark-kafka-3.5.7/`` with ``connector-lock.json``.
5. The probe runs again with ``--jars`` and without ``--packages``: a cached launch needs no network.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from transitpulse.runtime.artifacts import ArtifactError, fetch_bytes
from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.util import atomic_write_json, hash_file, iso_now, read_json_or_none, tail_lines

log = logging.getLogger("transitpulse.runtime.connector")

PROBE_MARK = "TP_PROBE_RESULT="
MAVEN_CENTRAL = "https://repo1.maven.org/maven2"
LOCK_NAME = "connector-lock.json"


def lock_path(paths: WorkPaths) -> Path:
    return paths.connector_jars / LOCK_NAME


def parse_probe_output(stdout: str) -> dict[str, Any] | None:
    for line in reversed(stdout.splitlines()):
        if line.startswith(PROBE_MARK):
            try:
                doc = json.loads(line[len(PROBE_MARK):])
            except ValueError:
                return None
            return doc if isinstance(doc, dict) else None
    return None


def spark_submit_argv(spark_home: Path, paths: WorkPaths, probe_script: Path, *, packages: str | None = None,
                      jars: Sequence[str] = (), ivy_settings: Path | None = None) -> list[str]:
    argv = [str(spark_home / "bin" / "spark-submit"), "--master", "local[1]", "--driver-memory", "768m",
            "--conf", "spark.ui.enabled=false", "--conf", "spark.driver.host=127.0.0.1",
            "--conf", "spark.driver.bindAddress=127.0.0.1", "--conf", f"spark.local.dir={paths.spark_local}",
            "--conf", "spark.sql.session.timeZone=UTC"]
    if packages:
        argv += ["--packages", packages, "--conf", f"spark.jars.ivy={paths.ivy_cache}"]
        if ivy_settings is not None:
            argv += ["--conf", f"spark.jars.ivySettings={ivy_settings}"]
    if jars:
        argv += ["--jars", ",".join(jars)]
    argv.append(str(probe_script))
    return argv


def run_probe(argv: Sequence[str], env: Mapping[str, str], cwd: Path, log_file: Path, *, timeout: float
              ) -> dict[str, Any]:
    """Run spark-submit with the probe; full output goes to ``log_file``; returns the probe JSON."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    with open(log_file, "ab") as lf:
        lf.write(f"\n===== {iso_now()} {' '.join(argv)}\n".encode())
        lf.flush()
        try:
            r = subprocess.run(list(argv), env=dict(env), cwd=str(cwd), stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=lf, timeout=timeout, check=False)
        except subprocess.TimeoutExpired as exc:
            raise ArtifactError("CONNECTOR_PROBE_TIMEOUT", f"spark-submit probe exceeded {int(timeout)} s",
                                f"see {log_file}") from exc
        out = r.stdout.decode("utf-8", errors="replace")
        lf.write(out.encode())
    result = parse_probe_output(out)
    if r.returncode != 0 or result is None or not result.get("ok"):
        raise ArtifactError("CONNECTOR_PROBE_FAILED",
                            f"Spark Kafka connector probe failed (exit {r.returncode}): "
                            f"{(result or {}).get('error') or (result or {}).get('classes') or 'no probe result'}",
                            f"see {log_file}:\n{tail_lines(log_file, 40, 4000)}")
    result["seconds"] = round(time.monotonic() - t0, 1)
    return result


def index_ivy_cache(ivy_dir: Path) -> dict[str, tuple[str, str, str]]:
    """Map ``<org>_<filename>`` (Spark's retrieve name) -> (org, module, revision) via the Ivy cache layout.

    Ivy stores artifacts under ``cache/<org>/<module>/<type>s/<artifact>-<revision>.<ext>``. The type
    directory is ``jars`` for ``<packaging>jar`` but ``bundles`` for OSGi ``<packaging>bundle`` artifacts
    (e.g. org.xerial.snappy:snappy-java pulled by kafka-clients), so every ``*s`` type directory is indexed.
    Spark retrieves all of them as ``[organization]_[artifact]-[revision].jar``.
    """
    out: dict[str, tuple[str, str, str]] = {}
    cache = ivy_dir / "cache"
    if not cache.is_dir():
        return out
    for jar in sorted(cache.glob("*/*/*/*.jar")):
        type_dir = jar.parent.name
        if not type_dir.endswith("s") or type_dir in ("sources", "javadocs"):
            continue
        module = jar.parent.parent.name
        org = jar.parent.parent.parent.name
        name = jar.name
        if not name.startswith(module + "-"):
            continue
        rev = name[len(module) + 1 : -len(".jar")]
        key = f"{org}_{name}"
        # A plain "jars" entry wins over any other type directory holding the same file name.
        if key not in out or type_dir == "jars":
            out[key] = (org, module, rev)
    return out


def classify_jars(retrieved: Sequence[str], index: Mapping[str, tuple[str, str, str]], spark_jar_names: set[str],
                  *, spark_version: str) -> dict[str, Any]:
    """Pure classification of the retrieved jars (keep / provided_by_spark) with hard failures."""
    keep: list[dict[str, str]] = []
    provided: list[dict[str, str]] = []
    errors: list[str] = []
    warnings: list[str] = []
    kafka_clients = 0
    for fname in sorted(retrieved):
        ident = index.get(fname)
        if ident is None:
            errors.append(f"cannot identify {fname} in the Ivy cache")
            continue
        org, module, rev = ident
        rec = {"org": org, "module": module, "rev": rev, "file": fname}
        if "_2.13" in module:
            errors.append(f"Scala 2.13 artifact on a Scala 2.12 Spark classpath: {org}:{module}:{rev}")
        if org == "org.apache.spark" and rev != spark_version:
            errors.append(f"Spark artifact {module}:{rev} does not match Spark {spark_version}")
        if module == "kafka-clients":
            kafka_clients += 1
        same = f"{module}-{rev}.jar"
        if same in spark_jar_names:
            provided.append({**rec, "spark_jar": same})
            continue
        other = sorted(n for n in spark_jar_names if n.startswith(module + "-") and n[len(module) + 1 : len(module) + 2].isdigit())
        if other:
            provided.append({**rec, "spark_jar": other[0], "version_differs": True})
            warnings.append(f"{org}:{module}:{rev} dropped: Spark ships {other[0]} (Spark's version is used)")
            continue
        keep.append(rec)
    if kafka_clients > 1:
        errors.append("more than one kafka-clients jar resolved")
    if not any(r["module"].startswith("spark-sql-kafka-0-10_") for r in keep):
        errors.append("spark-sql-kafka-0-10 jar missing from the resolved closure")
    if not any(r["module"] == "kafka-clients" for r in keep):
        errors.append("kafka-clients jar missing from the resolved closure")
    return {"keep": keep, "provided_by_spark": provided, "errors": errors, "warnings": warnings}


def maven_sha1_url(org: str, module: str, rev: str) -> str:
    return f"{MAVEN_CENTRAL}/{org.replace('.', '/')}/{module}/{rev}/{module}-{rev}.jar.sha1"


def valid_lock(paths: WorkPaths, coordinate: str) -> dict[str, Any] | None:
    lock = read_json_or_none(lock_path(paths))
    if not isinstance(lock, dict) or lock.get("coordinate") != coordinate or not lock.get("jars"):
        return None
    for jar in lock["jars"]:
        p = Path(jar.get("path", ""))
        if not p.is_file() or hash_file(p, "sha256") != jar.get("sha256"):
            return None
    return lock


def resolve_closure(*, spark_home: Path, env: Mapping[str, str], paths: WorkPaths, repo_dir: Path, coordinate: str,
                    spark_version: str, offline: bool, expected_direct: Sequence[str] = (),
                    fetch: Callable[[str], bytes] = fetch_bytes) -> dict[str, Any]:
    """Return ``{"status": REUSED|RESOLVED, "lock": {...}, "probe": {...}|None, "warnings": [...]}``."""
    probe_script = repo_dir / "scripts" / "probes" / "spark_kafka_probe.py"
    probe_log = paths.root / "logs" / "bootstrap" / "spark_probe.log"
    existing = valid_lock(paths, coordinate)
    if existing is not None:
        return {"status": "REUSED", "lock": existing, "probe": None, "warnings": []}
    if offline:
        raise ArtifactError("OFFLINE_ARTIFACT_MISSING", "Spark Kafka connector jars are not cached and --offline "
                            "was given", "run bootstrap once with network access")
    ivy_settings = paths.conf / "spark" / "ivysettings.xml"
    if not ivy_settings.is_file():
        raise ArtifactError("CONNECTOR_SETTINGS_MISSING", f"{ivy_settings} not found (configs not rendered)")
    retrieve_dir = paths.ivy_cache / "jars"
    if retrieve_dir.exists():
        shutil.rmtree(retrieve_dir)  # cache-only directory: rebuilt by this resolution
    log.info("resolving %s from Maven Central (spark-submit --packages, probe)", coordinate)
    probe1 = run_probe(spark_submit_argv(spark_home, paths, probe_script, packages=coordinate,
                                         ivy_settings=ivy_settings), env, paths.root, probe_log, timeout=1200)
    retrieved = sorted(p.name for p in retrieve_dir.glob("*.jar")) if retrieve_dir.is_dir() else []
    if not retrieved:
        raise ArtifactError("CONNECTOR_RESOLVE_FAILED", f"no jars retrieved into {retrieve_dir}", f"see {probe_log}")
    spark_jar_names = {p.name for p in (spark_home / "jars").glob("*.jar")}
    cls = classify_jars(retrieved, index_ivy_cache(paths.ivy_cache), spark_jar_names, spark_version=spark_version)
    if cls["errors"]:
        raise ArtifactError("CONNECTOR_CLOSURE_INVALID", "; ".join(cls["errors"]),
                            "the resolved dependency closure violates the Spark 3.5.7 / Scala 2.12 contract")
    warnings = list(cls["warnings"])
    jars: list[dict[str, Any]] = []
    for rec in cls["keep"]:
        src = retrieve_dir / rec["file"]
        sha1 = hash_file(src, "sha1")
        url = maven_sha1_url(rec["org"], rec["module"], rec["rev"])
        tokens = fetch(url).decode("utf-8", errors="replace").split()
        official = tokens[0].lower() if tokens else ""
        if official != sha1:
            raise ArtifactError("CHECKSUM_MISMATCH", f"{rec['file']}: sha1 {sha1} != Maven Central {official}",
                                "the Ivy cache may be corrupt; remove W/cache/ivy and re-run bootstrap")
        jars.append({**rec, "sha1": sha1, "sha1_source": url, "sha256": hash_file(src, "sha256"),
                     "bytes": src.stat().st_size})
    for exp in expected_direct:
        org, module, rev = exp.split(":")
        match = [j for j in jars if j["org"] == org and j["module"] == module]
        if not match:
            warnings.append(f"expected direct dependency {exp} not in the kept closure")
        elif match[0]["rev"] != rev:
            warnings.append(f"{org}:{module} resolved to {match[0]['rev']} (manifest expects {rev})")

    staging = paths.connector_jars.parent / f".staging-{uuid.uuid4().hex[:8]}"
    staging.mkdir(parents=True)
    for j in jars:
        shutil.copy2(retrieve_dir / j["file"], staging / j["file"])
        j["path"] = str(paths.connector_jars / j["file"])
    lock = {
        "schema_version": 1, "coordinate": coordinate, "resolver": "maven-central", "spark_version": spark_version,
        "scala": "2.12", "resolved_at": iso_now(), "jars": jars, "provided_by_spark": cls["provided_by_spark"],
        "warnings": warnings, "probe_with_packages": probe1,
        "closure_sha256": hashlib.sha256("\n".join(sorted(j["sha256"] for j in jars)).encode()).hexdigest(),
    }
    atomic_write_json(staging / LOCK_NAME, lock)
    if paths.connector_jars.exists():
        trash = paths.connector_jars.parent / ".trash"
        trash.mkdir(parents=True, exist_ok=True)
        os.replace(paths.connector_jars, trash / f"{paths.connector_jars.name}.{time.strftime('%Y%m%dT%H%M%S')}")
    os.replace(staging, paths.connector_jars)
    log.info("connector closure: %d jar(s) kept, %d provided by Spark", len(jars), len(cls["provided_by_spark"]))
    return {"status": "RESOLVED", "lock": lock, "probe": probe1, "warnings": warnings}


def cached_launch_probe(*, spark_home: Path, env: Mapping[str, str], paths: WorkPaths, repo_dir: Path,
                        jars: Sequence[str]) -> dict[str, Any]:
    """Probe with ``--jars`` only (no --packages, no resolver): proves restarts need no network."""
    probe_script = repo_dir / "scripts" / "probes" / "spark_kafka_probe.py"
    probe_log = paths.root / "logs" / "bootstrap" / "spark_probe.log"
    result = run_probe(spark_submit_argv(spark_home, paths, probe_script, jars=jars), env, paths.root, probe_log,
                       timeout=600)
    result["mode"] = "cached --jars, no --packages"
    return result
