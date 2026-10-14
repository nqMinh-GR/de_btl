"""Kafka 4.2.2 KRaft single node, native (design D §4).

* Format once: ``kafka-storage.sh format --standalone`` runs only when ``log.dirs`` is empty. An existing
  ``meta.properties`` must carry ``node.id=1`` and the cluster id recorded in ``infra/kafka/cluster.json``;
  a mismatch fails with ``KAFKA_METADATA_MISMATCH`` and is never "fixed" by reformatting.
* Readiness: confluent-kafka AdminClient metadata (broker 1 present) + ``describe_cluster`` — an open
  port is not readiness.
* Topics: created idempotently from ``contracts.topics.desired_topic_configs``; existing topics are
  compared (configs and partition count) and reported ``EXISTS_MATCH`` / ``EXISTS_DRIFT`` with a diff.
  Partitions are never changed and topics are never deleted.
"""

from __future__ import annotations

import logging
import re
import subprocess
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from transitpulse.config import ProjectConfig
from transitpulse.runtime.configs import KAFKA_CONTROLLER_PORT, KAFKA_NODE_ID, kafka_listener, kafka_server_properties_path
from transitpulse.runtime.manifest import RuntimeInfo, subprocess_env
from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.supervisor import OWNER_PLACEHOLDER
from transitpulse.runtime.util import atomic_write_json, iso_now, read_json_or_none, sha256_file

log = logging.getLogger("transitpulse.runtime.kafka")

RECONCILABLE_PREFIXES = ("retention.", "segment.")
RECONCILABLE_KEYS = frozenset({"max.message.bytes"})
_KAFKA_UUID = re.compile(r"^[A-Za-z0-9_-]{22}$")


class KafkaError(Exception):
    def __init__(self, code: str, message: str, hint: str | None = None, *, exit_code: int = 1,
                 log_excerpt: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint
        self.exit_code = exit_code
        self.log_excerpt = log_excerpt


@dataclass(frozen=True)
class FormatDecision:
    action: str               # FORMAT | REUSE | ADOPT
    cluster_id: str | None
    reason: str


def read_properties(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "!")):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def decide_format(data_dir: Path, cluster_json: Path, *, node_id: int = KAFKA_NODE_ID) -> FormatDecision:
    """Pure format-once decision. Raises KafkaError for states that need a human decision."""
    meta = data_dir / "meta.properties"
    recorded = read_json_or_none(cluster_json) if cluster_json.is_file() else None
    recorded_id = recorded.get("cluster_id") if isinstance(recorded, dict) else None
    workdir = data_dir.parents[2] if len(data_dir.parents) > 2 else data_dir  # W/infra/kafka/data -> W
    reset_hint = ("Kafka data belongs to another cluster. To start over (DESTROYS Kafka data of this work "
                  f"directory): `scripts/demo.py reset --targets kafka --workdir {workdir}`, then the same command "
                  "with --confirm <token>")
    if meta.is_file():
        props = read_properties(meta)
        if props.get("node.id") != str(node_id):
            raise KafkaError("KAFKA_METADATA_MISMATCH",
                             f"{meta}: node.id={props.get('node.id')!r}, expected {node_id}", reset_hint, exit_code=4)
        cid = props.get("cluster.id")
        if not cid:
            raise KafkaError("KAFKA_METADATA_MISMATCH", f"{meta} has no cluster.id", reset_hint, exit_code=4)
        if recorded_id is None:
            return FormatDecision("ADOPT", cid, "meta.properties present but cluster.json missing: adopting its id")
        if recorded_id != cid:
            raise KafkaError("KAFKA_METADATA_MISMATCH",
                             f"{meta} cluster.id={cid} differs from cluster.json cluster_id={recorded_id}",
                             reset_hint, exit_code=4)
        return FormatDecision("REUSE", cid, "already formatted with the recorded cluster id")
    if data_dir.is_dir() and any(data_dir.iterdir()):
        raise KafkaError("KAFKA_DIR_UNKNOWN_CONTENT",
                         f"{data_dir} is not empty but has no meta.properties; refusing to format over it",
                         "inspect the directory; reset with "
                         f"`scripts/demo.py reset --targets kafka --workdir {workdir}`", exit_code=4)
    return FormatDecision("FORMAT", recorded_id, "empty log directory")


def kafka_env(runtime: RuntimeInfo, paths: WorkPaths, cfg: ProjectConfig, *, heap: str | None = None,
              with_owner: bool = True) -> dict[str, str]:
    heap = heap or cfg.runtime.kafka_heap
    opts = f"-Djava.io.tmpdir={paths.tmp}"
    if with_owner:
        opts += f" -Dtransitpulse.owner={OWNER_PLACEHOLDER}"
    return subprocess_env(runtime, {
        "KAFKA_HEAP_OPTS": f"-Xms{heap} -Xmx{heap}",
        "LOG_DIR": str(paths.log_dir("kafka")),
        "KAFKA_OPTS": opts,
    })


def kafka_argv(runtime: RuntimeInfo, paths: WorkPaths) -> list[str]:
    return [str(runtime.kafka_home / "bin" / "kafka-server-start.sh"), str(kafka_server_properties_path(paths))]


def kafka_ports(cfg: ProjectConfig) -> tuple[int, ...]:
    _, port = kafka_listener(cfg)
    return (port, KAFKA_CONTROLLER_PORT)


def fingerprint_extra(paths: WorkPaths) -> str:
    conf = kafka_server_properties_path(paths)
    return f"server.properties={sha256_file(conf)}" if conf.is_file() else "server.properties=missing"


def _run_tool(argv: list[str], env: Mapping[str, str], cwd: Path, timeout: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, env=dict(env), cwd=str(cwd), capture_output=True, text=True, timeout=timeout,
                          stdin=subprocess.DEVNULL, check=False)


def random_uuid(runtime: RuntimeInfo, paths: WorkPaths, cfg: ProjectConfig) -> str:
    env = kafka_env(runtime, paths, cfg, heap="256m", with_owner=False)
    r = _run_tool([str(runtime.kafka_home / "bin" / "kafka-storage.sh"), "random-uuid"], env, paths.root, 120)
    for line in reversed(r.stdout.splitlines()):
        if _KAFKA_UUID.match(line.strip()):
            return line.strip()
    raise KafkaError("KAFKA_TOOL_FAILED", f"kafka-storage.sh random-uuid failed (exit {r.returncode})",
                     "check JAVA_HOME in runtime.json", log_excerpt=(r.stderr or r.stdout)[-2000:])


def ensure_formatted(runtime: RuntimeInfo, paths: WorkPaths, cfg: ProjectConfig) -> dict[str, Any]:
    """Format the KRaft log directory exactly once. Returns ``{action, cluster_id, warnings}``."""
    conf = kafka_server_properties_path(paths)
    if not conf.is_file():
        raise KafkaError("KAFKA_CONFIG_MISSING", f"{conf} not found", "re-run bootstrap (step B8 renders it)",
                         exit_code=3)
    paths.kafka_data.mkdir(parents=True, exist_ok=True)
    decision = decide_format(paths.kafka_data, paths.kafka_cluster_json)
    warnings: list[str] = []
    if decision.action == "REUSE":
        return {"action": "REUSE", "cluster_id": decision.cluster_id, "warnings": warnings}
    if decision.action == "ADOPT":
        warnings.append(decision.reason)
        log.warning("kafka: %s", decision.reason)
        atomic_write_json(paths.kafka_cluster_json, {"cluster_id": decision.cluster_id, "node_id": KAFKA_NODE_ID,
                                                     "created_at": iso_now(), "formatted_at": None,
                                                     "adopted_at": iso_now()})
        return {"action": "ADOPT", "cluster_id": decision.cluster_id, "warnings": warnings}
    cluster_id = decision.cluster_id or random_uuid(runtime, paths, cfg)
    existing = read_json_or_none(paths.kafka_cluster_json) or {}
    atomic_write_json(paths.kafka_cluster_json, {"cluster_id": cluster_id, "node_id": KAFKA_NODE_ID,
                                                 "created_at": existing.get("created_at") or iso_now(),
                                                 "formatted_at": None})
    log.info("kafka: formatting %s (cluster id %s, --standalone)", paths.kafka_data, cluster_id)
    env = kafka_env(runtime, paths, cfg, heap="256m", with_owner=False)
    r = _run_tool([str(runtime.kafka_home / "bin" / "kafka-storage.sh"), "format", "--standalone", "-t", cluster_id,
                   "-c", str(conf)], env, paths.root, 180)
    if r.returncode != 0 or not (paths.kafka_data / "meta.properties").is_file():
        raise KafkaError("KAFKA_FORMAT_FAILED", f"kafka-storage.sh format failed (exit {r.returncode})",
                         "see the excerpt; the cluster id is kept in cluster.json for the next attempt",
                         log_excerpt=((r.stdout or "") + (r.stderr or ""))[-3000:])
    atomic_write_json(paths.kafka_cluster_json, {"cluster_id": cluster_id, "node_id": KAFKA_NODE_ID,
                                                 "created_at": existing.get("created_at") or iso_now(),
                                                 "formatted_at": iso_now()})
    return {"action": "FORMATTED", "cluster_id": cluster_id, "warnings": warnings}


# --------------------------------------------------------------------------------------------------
# Readiness, topics, offsets (confluent-kafka imported lazily: unit tests do not need it)
# --------------------------------------------------------------------------------------------------

def _admin(cfg: ProjectConfig):
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": cfg.kafka.bootstrap_servers, "socket.timeout.ms": 5000,
                        "client.id": "transitpulse-infra", "log_level": 2})


class KafkaReadyProbe:
    """Callable readiness probe; ``info`` keeps the last observation for reporting."""

    def __init__(self, cfg: ProjectConfig, expected_cluster_id: str | None) -> None:
        self.cfg = cfg
        self.expected_cluster_id = expected_cluster_id
        self.info: dict[str, Any] = {}
        self._client = None

    def __call__(self) -> bool:
        if self._client is None:
            self._client = _admin(self.cfg)
        md = self._client.list_topics(timeout=5)
        brokers = sorted(int(b) for b in (md.brokers or {}))
        if KAFKA_NODE_ID not in brokers:
            self.info = {"brokers": brokers}
            return False
        res = self._client.describe_cluster(request_timeout=5).result(timeout=10)
        controller = getattr(res, "controller", None)
        cluster_id = getattr(res, "cluster_id", None)
        self.info = {"brokers": brokers, "cluster_id": cluster_id,
                     "controller_id": getattr(controller, "id", None) if controller is not None else None}
        if self.expected_cluster_id and cluster_id and cluster_id != self.expected_cluster_id:
            # Never "ready": the broker on this port is not the one formatted for this work directory.
            self.info["mismatch"] = f"broker reports cluster id {cluster_id}, expected {self.expected_cluster_id}"
            return False
        return controller is not None and bool(cluster_id)


def topic_status(cfg: ProjectConfig, *, reconcile: bool = False) -> list[dict[str, Any]]:
    """Create missing topics; compare existing ones; optionally reconcile retention/segment/max.message.bytes."""
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AlterConfigOpType, ConfigEntry, ConfigResource, NewTopic, ResourceType

    from transitpulse.contracts.topics import ALL_TOPICS, desired_topic_configs

    admin = _admin(cfg)
    desired = desired_topic_configs(cfg)
    md = admin.list_topics(timeout=15)
    existing = dict(md.topics or {})
    results: list[dict[str, Any]] = []
    missing = [t for t in ALL_TOPICS if t not in existing]
    if missing:
        new = [NewTopic(t, num_partitions=cfg.kafka.partitions, replication_factor=cfg.kafka.replication_factor,
                        config=dict(desired[t])) for t in missing]
        futures = admin.create_topics(new, request_timeout=30)
        for topic, fut in futures.items():
            try:
                fut.result(timeout=60)
                results.append({"topic": topic, "status": "CREATED", "diff": {}})
            except KafkaException as exc:
                err = exc.args[0] if exc.args else None
                if getattr(err, "name", lambda: "")() == "TOPIC_ALREADY_EXISTS":
                    existing[topic] = None
                else:
                    raise KafkaError("KAFKA_TOPIC_CREATE_FAILED", f"create_topics({topic}) failed: {exc}",
                                     "check the broker log (infra.py logs --component kafka --file server)") from exc
        md = admin.list_topics(timeout=15)
        existing = dict(md.topics or {})
    created = {r["topic"] for r in results}
    for topic in ALL_TOPICS:
        if topic in created:
            continue
        tmeta = existing.get(topic)
        resource = ConfigResource(ResourceType.TOPIC, topic)
        described = admin.describe_configs([resource], request_timeout=30)
        entries = list(described.values())[0].result(timeout=60)
        diff: dict[str, dict[str, Any]] = {}
        for key, value in desired[topic].items():
            actual = entries.get(key).value if entries.get(key) is not None else None
            if actual != value:
                diff[key] = {"desired": value, "actual": actual}
        partitions = len(tmeta.partitions) if tmeta is not None and tmeta.partitions else None
        if partitions is not None and partitions != cfg.kafka.partitions:
            diff["partitions"] = {"desired": cfg.kafka.partitions, "actual": partitions, "reconcilable": False}
        status = "EXISTS_MATCH" if not diff else "EXISTS_DRIFT"
        applied: dict[str, str] = {}
        if diff and reconcile:
            fixes = {k: v["desired"] for k, v in diff.items()
                     if k != "partitions" and (k.startswith(RECONCILABLE_PREFIXES) or k in RECONCILABLE_KEYS)}
            if fixes:
                res = ConfigResource(ResourceType.TOPIC, topic, incremental_configs=[
                    ConfigEntry(k, str(v), incremental_operation=AlterConfigOpType.SET) for k, v in fixes.items()])
                for fut in admin.incremental_alter_configs([res], request_timeout=30).values():
                    fut.result(timeout=60)
                applied = {k: str(v) for k, v in fixes.items()}
                for k in fixes:
                    diff.pop(k, None)
                status = "RECONCILED" if not diff else "EXISTS_DRIFT"
        results.append({"topic": topic, "status": status, "diff": diff, "reconciled": applied,
                        "partitions": partitions})
    order = {t: i for i, t in enumerate(ALL_TOPICS)}
    return sorted(results, key=lambda r: order.get(r["topic"], 99))


def end_offsets(bootstrap: str, topics: list[str], *, timeout: float = 10.0) -> dict[str, dict[int, int]]:
    """High watermarks per topic/partition (uncached)."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"tp-offsets-{uuid.uuid4().hex[:8]}",
                         "enable.auto.commit": False, "log_level": 2})
    try:
        md = consumer.list_topics(timeout=timeout)
        out: dict[str, dict[int, int]] = {}
        for topic in topics:
            tmeta = (md.topics or {}).get(topic)
            if tmeta is None or tmeta.error is not None:
                continue
            for pid in sorted(tmeta.partitions):
                _low, high = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=timeout, cached=False)
                out.setdefault(topic, {})[int(pid)] = int(high)
        return out
    finally:
        consumer.close()


def make_ready_probe(cfg: ProjectConfig, expected_cluster_id: str | None) -> Callable[[], bool]:
    return KafkaReadyProbe(cfg, expected_cluster_id)
