"""Layered project configuration.

Merge order (later wins):
  1. config/base.yaml
  2. config/<profile file>.yaml   (local-native -> local.yaml, colab-native -> colab.yaml, local-docker -> docker.yaml)
  3. environment variables (TP_WORKDIR, TP_ROUTE_IDS, TP_KAFKA_BOOTSTRAP, TP_CASSANDRA_HOSTS, TP_API_URL,
     and the generic form TP__<section>__<key>=<yaml scalar>)
  4. explicit ``overrides`` passed by the caller (nested dict)

Every model forbids unknown keys so that a typo in YAML fails loudly instead of being ignored.
The effective policy of a run is pinned into ``run_metadata.policy_json`` when the run is created;
changing YAML later never changes an existing run.
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

PROFILES: dict[str, str] = {
    "local-native": "local.yaml",
    "colab-native": "colab.yaml",
    "local-docker": "docker.yaml",
}
DEFAULT_PROFILE = "local-native"


def repo_root() -> Path:
    """Repository root (directory that contains pyproject.toml and config/)."""
    env = os.environ.get("TP_REPO_DIR")
    if env:
        return Path(env).resolve()
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").is_file() and (parent / "config" / "base.yaml").is_file():
            return parent
    raise RuntimeError("cannot locate repository root (pyproject.toml + config/base.yaml); set TP_REPO_DIR")


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ProjectSection(_Model):
    project_id: str
    pipeline_version: str
    policy_version: str
    agency_timezone_fallback: str | None = None


class SourcesSection(_Model):
    gtfs_static_url: str
    vehicle_positions_url: str
    trip_updates_url: str
    alerts_url: str
    user_agent: str

    def realtime_url(self, feed_type: str) -> str:
        return {
            "vehicle_positions": self.vehicle_positions_url,
            "trip_updates": self.trip_updates_url,
            "alerts": self.alerts_url,
        }[feed_type]


class ScopeSection(_Model):
    route_ids: list[str] = Field(default_factory=list)
    route_type: int = 3
    max_routes: int = 10
    scope_policy_version: str = "scope-v1"


class HttpSection(_Model):
    connect_timeout_seconds: float
    read_timeout_seconds: float
    write_timeout_seconds: float
    pool_timeout_seconds: float
    max_attempts: int
    backoff_base_seconds: float
    backoff_cap_seconds: float
    retry_after_max_seconds: float
    max_response_bytes: int


class OutboxSection(_Model):
    max_in_flight: int
    keep_published_payload: bool
    retry_backoff_cap_seconds: float


class CollectorSection(_Model):
    poll_interval_seconds: dict[str, float]
    http: HttpSection
    raw_disk_budget_bytes: int
    min_free_bytes: int
    disk_warning_fraction: float
    recording_seconds: int
    outbox: OutboxSection
    heartbeat_seconds: float


class KafkaTopics(_Model):
    vehicle_positions: str
    trip_updates: str
    alerts: str
    control: str
    dlq: str

    def for_feed(self, feed_type: str) -> str:
        return getattr(self, feed_type)


class KafkaProducerSection(_Model):
    enable_idempotence: bool
    acks: str
    max_in_flight_requests_per_connection: int
    message_timeout_ms: int
    linger_ms: int
    compression_type: str
    message_max_bytes: int


class KafkaSection(_Model):
    bootstrap_servers: str
    topics: KafkaTopics
    partitions: int
    replication_factor: int
    retention_hours: float
    snapshot_retention_bytes: int
    max_snapshot_bytes: int
    topic_max_message_bytes: int
    control_max_message_bytes: int
    dlq_excerpt_max_chars: int
    producer: KafkaProducerSection


class SparkSection(_Model):
    master: str
    app_name: str
    trigger_seconds: int
    driver_memory: str
    shuffle_partitions: int
    max_partition_fetch_bytes: int
    fetch_max_bytes: int
    max_snapshots_per_batch: int
    max_control_events_per_batch: int
    query_name: str
    connector_coordinate: str
    ui_enabled: bool


class CassandraSection(_Model):
    hosts: list[str]
    port: int
    keyspace: str
    local_dc: str
    consistency: str
    serial_consistency: str
    connect_timeout_seconds: float
    request_timeout_seconds: float
    write_concurrency: int
    write_buffer_rows: int
    parent_read_limit_per_route: int
    parent_read_total_limit: int


class PolicySection(_Model):
    vehicle_stale_seconds: int
    prediction_stale_seconds: int
    alert_stale_seconds: int
    predicted_late_seconds: int
    future_tolerance_seconds: int
    service_date_inference_window_seconds: int
    uncertainty_policy: str
    target_policy: str
    timestamp_ms_threshold: int


class StorageSection(_Model):
    current_generation_ttl_seconds: int
    history_ttl_seconds: int
    min_publish_ttl_seconds: int
    pointer_cas_max_attempts: int
    raw_deletion_enabled: bool
    geometry_max_points: int


class ApiSection(_Model):
    host: str
    port: int
    base_path: str
    cors_allow_origins: list[str]
    history_max_hours: int
    history_default_hours: int
    page_size_default: int
    page_size_max: int
    max_total_rows: int
    max_fanout_routes: int
    predictions_limit_default: int
    predictions_limit_max: int
    request_timeout_seconds: float
    statement_timeout_seconds: float
    cache_entries: int
    cache_max_ttl_seconds: float


class UiSection(_Model):
    refresh_seconds: int
    api_base_url: str
    http_timeout_seconds: float


class ReplaySection(_Model):
    tick_interval_seconds: float
    max_speed: float
    heartbeat_seconds: float
    control_poll_seconds: float


class SyntheticSection(_Model):
    default_seed: int
    route_ids: list[str]
    service_date: str
    start_clock: str


class PathsSection(_Model):
    workdir: str | None = None


class RuntimeSection(_Model):
    kafka_heap: str
    cassandra_max_heap: str
    cassandra_max_direct_memory: str
    allow_root_in_demo_vm: bool
    ready_timeout_seconds: int
    min_mem_available_bytes: int
    min_disk_free_bytes: int
    min_cpus: int


class ObservabilitySection(_Model):
    metrics_export_seconds: float
    log_excerpt_chars: int


class ProjectConfig(_Model):
    profile: str = DEFAULT_PROFILE
    project: ProjectSection
    sources: SourcesSection
    scope: ScopeSection
    collector: CollectorSection
    kafka: KafkaSection
    spark: SparkSection
    cassandra: CassandraSection
    policy: PolicySection
    storage: StorageSection
    api: ApiSection
    ui: UiSection
    replay: ReplaySection
    synthetic: SyntheticSection
    paths: PathsSection
    runtime: RuntimeSection
    observability: ObservabilitySection

    @property
    def workdir(self) -> Path:
        """Absolute work directory (raw data, checkpoints, infra data, logs). Never inside src/."""
        if not self.paths.workdir:
            raise ValueError("work directory is not configured: pass --workdir or set TP_WORKDIR")
        p = Path(self.paths.workdir)
        if not p.is_absolute():
            p = repo_root() / p
        return p.resolve()

    def config_hash(self) -> str:
        import hashlib

        return hashlib.sha256(json.dumps(self.model_dump(), sort_keys=True).encode()).hexdigest()


def _deep_merge(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _env_overrides(environ: dict[str, str]) -> dict[str, Any]:
    over: dict[str, Any] = {}

    def put(path: list[str], value: Any) -> None:
        node = over
        for key in path[:-1]:
            node = node.setdefault(key, {})
        node[path[-1]] = value

    if environ.get("TP_WORKDIR"):
        put(["paths", "workdir"], environ["TP_WORKDIR"])
    if environ.get("TP_ROUTE_IDS"):
        ids = [r.strip() for r in environ["TP_ROUTE_IDS"].split(",") if r.strip()]
        put(["scope", "route_ids"], ids)
    if environ.get("TP_KAFKA_BOOTSTRAP"):
        put(["kafka", "bootstrap_servers"], environ["TP_KAFKA_BOOTSTRAP"])
    if environ.get("TP_CASSANDRA_HOSTS"):
        put(["cassandra", "hosts"], [h.strip() for h in environ["TP_CASSANDRA_HOSTS"].split(",") if h.strip()])
    if environ.get("TP_API_URL"):
        put(["ui", "api_base_url"], environ["TP_API_URL"])
    for key, raw in environ.items():
        if key.startswith("TP__"):
            path = [p.lower() for p in key[4:].split("__") if p]
            if path:
                put(path, yaml.safe_load(raw))
    return over


def load_config(
    profile: str | None = None,
    *,
    workdir: str | os.PathLike[str] | None = None,
    overrides: dict[str, Any] | None = None,
    environ: dict[str, str] | None = None,
    config_dir: str | os.PathLike[str] | None = None,
) -> ProjectConfig:
    """Load and validate the merged configuration for ``profile``."""
    env = dict(os.environ if environ is None else environ)
    profile = profile or env.get("TP_PROFILE") or DEFAULT_PROFILE
    if profile not in PROFILES:
        raise ValueError(f"unknown profile {profile!r}; expected one of {sorted(PROFILES)}")
    cdir = Path(config_dir) if config_dir else repo_root() / "config"
    with open(cdir / "base.yaml", encoding="utf-8") as fh:
        data: dict[str, Any] = yaml.safe_load(fh) or {}
    with open(cdir / PROFILES[profile], encoding="utf-8") as fh:
        data = _deep_merge(data, yaml.safe_load(fh) or {})
    data = _deep_merge(data, _env_overrides(env))
    if overrides:
        data = _deep_merge(data, overrides)
    if workdir is not None:
        data = _deep_merge(data, {"paths": {"workdir": str(workdir)}})
    data["profile"] = profile
    return ProjectConfig.model_validate(data)
