"""Work-directory layout. All runtime paths derive from one configured work root (never /content in core).

W/
  tools/{uv, python, jdk, kafka, cassandra, .trash}         pinned binaries (bootstrap)
  cache/{downloads/.partial, uv, ivy, jars/spark-kafka-3.5.7}
  conf/{kafka/server.properties, cassandra/, spark/ivysettings.xml, rendered.json}
  runtime/{runtime.json, runtime.env, bootstrap_report.json, bootstrap.lock}
  infra/kafka/{data, cluster.json}  infra/cassandra/{data, commitlog, saved_caches, hints, cdc_raw}
  data/static/<gtfs_version>/{MBTA_GTFS.zip, version.json, <scope_id>/dim_*/, scope_index.json, static_quality_report.json}
  data/raw/<run_id>/<feed_type>/<YYYY-MM-DD>/<capture_id>.json.gz (+ .meta.json), capture_index.jsonl, recording_manifest.json
  data/recordings/<recording_id>/ (generated synthetic / golden recordings, same layout as data/raw/<run_id>)
  data/curated/run_id=<run_id>/query=<query>/batch_id=<n>/<dataset>/
  data/inputs/ (imported bundles)
  state/outbox/<run_id>.sqlite (+ .lock), state/collector/<run_id>/poll_state.json
  runs/<run_id>/run.json
  checkpoints/<pipeline_version>/<run_id>/<query_name>/
  processes/{registry.json, registry.lock, exits/<component>.json}
  sessions/<session_id>/{session.json, publisher_control.json, publisher_state.json, publish_log.jsonl}
  faults/<run_id>/<point>.json (arm files; renamed fired-<fault_id>.json when fired)
  metrics/<component>.prom
  logs/<component>/..., logs/runs/<run_id>/query_progress.jsonl
  reports/, artifacts/, tmp/, spark-local/
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def safe_component(value: str, what: str = "id") -> str:
    """Validate a single path component built from an identifier (no traversal, no separators)."""
    if not isinstance(value, str) or not _SAFE_ID.match(value) or value in {".", ".."}:
        raise ValueError(f"unsafe {what} for a path component: {value!r}")
    return value


@dataclass(frozen=True)
class WorkPaths:
    root: Path

    @classmethod
    def from_workdir(cls, workdir: str | os.PathLike[str]) -> WorkPaths:
        return cls(Path(workdir).expanduser().resolve())

    # ---- setup / tooling
    @property
    def tools(self) -> Path:
        return self.root / "tools"

    @property
    def cache(self) -> Path:
        return self.root / "cache"

    @property
    def downloads(self) -> Path:
        return self.cache / "downloads"

    @property
    def ivy_cache(self) -> Path:
        return self.cache / "ivy"

    @property
    def connector_jars(self) -> Path:
        return self.cache / "jars" / "spark-kafka-3.5.7"

    @property
    def conf(self) -> Path:
        return self.root / "conf"

    @property
    def runtime_dir(self) -> Path:
        return self.root / "runtime"

    @property
    def runtime_json(self) -> Path:
        return self.runtime_dir / "runtime.json"

    @property
    def runtime_env(self) -> Path:
        return self.runtime_dir / "runtime.env"

    @property
    def bootstrap_report(self) -> Path:
        return self.runtime_dir / "bootstrap_report.json"

    # ---- infra data
    @property
    def kafka_data(self) -> Path:
        return self.root / "infra" / "kafka" / "data"

    @property
    def kafka_cluster_json(self) -> Path:
        return self.root / "infra" / "kafka" / "cluster.json"

    @property
    def cassandra_root(self) -> Path:
        return self.root / "infra" / "cassandra"

    # ---- data
    @property
    def static_root(self) -> Path:
        return self.root / "data" / "static"

    def static_version_dir(self, gtfs_version: str) -> Path:
        return self.static_root / safe_component(gtfs_version, "gtfs_version")

    def static_scope_dir(self, gtfs_version: str, scope_id: str) -> Path:
        return self.static_version_dir(gtfs_version) / safe_component(scope_id, "scope_id")

    @property
    def raw_root(self) -> Path:
        return self.root / "data" / "raw"

    def raw_run_dir(self, run_id: str) -> Path:
        return self.raw_root / safe_component(run_id, "run_id")

    @property
    def recordings_root(self) -> Path:
        return self.root / "data" / "recordings"

    @property
    def curated_root(self) -> Path:
        return self.root / "data" / "curated"

    def curated_run_dir(self, run_id: str) -> Path:
        return self.curated_root / f"run_id={safe_component(run_id, 'run_id')}"

    @property
    def inputs_root(self) -> Path:
        return self.root / "data" / "inputs"

    # ---- state
    def outbox_db(self, run_id: str) -> Path:
        return self.root / "state" / "outbox" / f"{safe_component(run_id, 'run_id')}.sqlite"

    def collector_state_dir(self, run_id: str) -> Path:
        return self.root / "state" / "collector" / safe_component(run_id, "run_id")

    def run_dir(self, run_id: str) -> Path:
        return self.root / "runs" / safe_component(run_id, "run_id")

    def run_json(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "run.json"

    def checkpoint_dir(self, pipeline_version: str, run_id: str, query_name: str) -> Path:
        return (
            self.root
            / "checkpoints"
            / safe_component(pipeline_version, "pipeline_version")
            / safe_component(run_id, "run_id")
            / safe_component(query_name, "query_name")
        )

    # ---- processes / sessions / faults
    @property
    def processes(self) -> Path:
        return self.root / "processes"

    @property
    def registry_json(self) -> Path:
        return self.processes / "registry.json"

    @property
    def registry_lock(self) -> Path:
        return self.processes / "registry.lock"

    def exit_file(self, component: str) -> Path:
        return self.processes / "exits" / f"{safe_component(component, 'component')}.json"

    def session_dir(self, session_id: str) -> Path:
        return self.root / "sessions" / safe_component(session_id, "session_id")

    def fault_dir(self, run_id: str) -> Path:
        return self.root / "faults" / safe_component(run_id, "run_id")

    # ---- observability / outputs
    @property
    def metrics_dir(self) -> Path:
        return self.root / "metrics"

    def log_dir(self, component: str) -> Path:
        return self.root / "logs" / safe_component(component, "component")

    def query_progress_log(self, run_id: str) -> Path:
        return self.root / "logs" / "runs" / safe_component(run_id, "run_id") / "query_progress.jsonl"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

    @property
    def artifacts(self) -> Path:
        return self.root / "artifacts"

    @property
    def tmp(self) -> Path:
        return self.root / "tmp"

    @property
    def spark_local(self) -> Path:
        return self.root / "spark-local"

    def ensure_base_dirs(self) -> None:
        """Create the directory skeleton (idempotent; never deletes anything)."""
        for p in (
            self.tools, self.downloads / ".partial", self.ivy_cache, self.conf, self.runtime_dir,
            self.kafka_data, self.cassandra_root, self.static_root, self.raw_root, self.recordings_root,
            self.curated_root, self.inputs_root, self.root / "state" / "outbox", self.root / "runs",
            self.root / "checkpoints", self.processes / "exits", self.root / "sessions", self.root / "faults",
            self.metrics_dir, self.root / "logs", self.reports, self.artifacts, self.tmp, self.spark_local,
        ):
            p.mkdir(parents=True, exist_ok=True)


def resolve_under(root: Path, relative: str) -> Path:
    """Resolve a stored relative path (e.g. raw_path) under ``root``; reject absolute paths and traversal."""
    rel = Path(relative)
    if rel.is_absolute() or any(part in ("..", "") for part in rel.parts):
        raise ValueError(f"unsafe relative path {relative!r}")
    full = (root / rel).resolve()
    if not full.is_relative_to(root.resolve()):
        raise ValueError(f"path {relative!r} escapes {root}")
    return full
