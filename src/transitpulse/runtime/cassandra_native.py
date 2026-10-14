"""Cassandra 5.0.9 single node, native (design D §5).

* ``bin/cassandra -f`` (foreground; the launcher ``exec``s java), ``-R`` only when uid or gid is 0 AND
  ``runtime.allow_root_in_demo_vm`` is true (the 5.0 launcher refuses root otherwise).
* Memory through the environment: ``MAX_HEAP_SIZE`` (+ ``MAX_DIRECT_MEMORY_SIZE``); ``HEAP_NEWSIZE`` stays
  unset because ``jvm17-server.options`` selects G1.
* Every directory (data, commitlog, saved caches, hints, cdc, logs, heap dumps, tmp) is under W.
* Readiness = python driver from the core .venv: ``SELECT release_version FROM system.local``.
* Migrations: ``transitpulse.storage.migrate.apply_migrations`` (WP4). Never TRUNCATE/DROP.
* Stop: ``nodetool drain`` (bounded) before the supervisor's graceful SIGTERM.
"""

from __future__ import annotations

import logging
import os
import subprocess
from collections.abc import Callable
from typing import Any

from transitpulse.config import ProjectConfig
from transitpulse.runtime.configs import cassandra_conf_dir
from transitpulse.runtime.manifest import RuntimeInfo, subprocess_env
from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.supervisor import OWNER_PLACEHOLDER
from transitpulse.runtime.util import iso_now, sha256_file

log = logging.getLogger("transitpulse.runtime.cassandra")

EXPECTED_RELEASE = "5.0.9"
STORAGE_PORT = 7000
JMX_PORT = 7199
DATA_SUBDIRS = ("data", "commitlog", "saved_caches", "hints", "cdc_raw")
PROBE_RUN_ID = "_infra_probe"


class CassandraError(Exception):
    def __init__(self, code: str, message: str, hint: str | None = None, *, exit_code: int = 1,
                 log_excerpt: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint
        self.exit_code = exit_code
        self.log_excerpt = log_excerpt


def running_as_root() -> bool:
    return os.getuid() == 0 or os.getgid() == 0


def root_flag_needed(cfg: ProjectConfig, *, is_root: bool | None = None) -> bool:
    """True when ``-R`` must be passed. Raises when running as root without the explicit config opt-in."""
    root = running_as_root() if is_root is None else is_root
    if not root:
        return False
    if not cfg.runtime.allow_root_in_demo_vm:
        raise CassandraError(
            "CASSANDRA_ROOT_REFUSED",
            "running as uid/gid 0 and Cassandra 5.0 refuses root without -R",
            "on a disposable demo VM (Google Colab) use --profile colab-native "
            "(runtime.allow_root_in_demo_vm: true); otherwise run as a normal user",
            exit_code=3,
        )
    return True


def ensure_dirs(paths: WorkPaths) -> None:
    for sub in DATA_SUBDIRS:
        (paths.cassandra_root / sub).mkdir(parents=True, exist_ok=True)
    (paths.log_dir("cassandra") / "heapdump").mkdir(parents=True, exist_ok=True)
    paths.tmp.mkdir(parents=True, exist_ok=True)


def cassandra_env(runtime: RuntimeInfo, paths: WorkPaths, cfg: ProjectConfig, *, with_owner: bool = True
                  ) -> dict[str, str]:
    extra_opts = f"-Dcassandra.skip_wait_for_gossip_to_settle=0 -Djava.io.tmpdir={paths.tmp}"
    if with_owner:
        extra_opts += f" -Dtransitpulse.owner={OWNER_PLACEHOLDER}"
    return subprocess_env(runtime, {
        "CASSANDRA_HOME": str(runtime.cassandra_home),
        "CASSANDRA_CONF": str(cassandra_conf_dir(paths)),
        "CASSANDRA_LOG_DIR": str(paths.log_dir("cassandra")),
        "CASSANDRA_HEAPDUMP_DIR": str(paths.log_dir("cassandra") / "heapdump"),
        "MAX_HEAP_SIZE": cfg.runtime.cassandra_max_heap,
        "MAX_DIRECT_MEMORY_SIZE": cfg.runtime.cassandra_max_direct_memory,
        "LOCAL_JMX": "yes",
        "JMX_PORT": str(JMX_PORT),
        "JVM_EXTRA_OPTS": extra_opts,
    })


def cassandra_argv(runtime: RuntimeInfo, cfg: ProjectConfig, *, is_root: bool | None = None) -> list[str]:
    argv = [str(runtime.cassandra_home / "bin" / "cassandra"), "-f"]
    if root_flag_needed(cfg, is_root=is_root):
        argv.append("-R")
    return argv


def cassandra_ports(cfg: ProjectConfig) -> tuple[int, ...]:
    return (int(cfg.cassandra.port), STORAGE_PORT, JMX_PORT)


def fingerprint_extra(paths: WorkPaths) -> str:
    yaml_path = cassandra_conf_dir(paths) / "cassandra.yaml"
    return f"cassandra.yaml={sha256_file(yaml_path)}" if yaml_path.is_file() else "cassandra.yaml=missing"


def check_conf(paths: WorkPaths) -> None:
    conf = cassandra_conf_dir(paths)
    for name in ("cassandra.yaml", "cassandra-env.sh", "jvm17-server.options", "logback.xml"):
        if not (conf / name).is_file():
            raise CassandraError("CASSANDRA_CONFIG_MISSING", f"{conf / name} not found",
                                 "re-run bootstrap (step B8 materializes the Cassandra conf dir)", exit_code=3)


def _cluster(cfg: ProjectConfig):
    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy

    return Cluster(contact_points=list(cfg.cassandra.hosts), port=int(cfg.cassandra.port),
                   connect_timeout=float(cfg.cassandra.connect_timeout_seconds),
                   load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=cfg.cassandra.local_dc))


class CassandraReadyProbe:
    """Readiness probe: ``system.local.release_version`` must equal the pinned release."""

    def __init__(self, cfg: ProjectConfig, expected: str = EXPECTED_RELEASE) -> None:
        self.cfg = cfg
        self.expected = expected
        self.info: dict[str, Any] = {}

    def __call__(self) -> bool:
        logging.getLogger("cassandra").setLevel(logging.CRITICAL)
        cluster = _cluster(self.cfg)
        try:
            session = cluster.connect()
            row = session.execute("SELECT release_version, host_id, data_center FROM system.local", timeout=5).one()
        finally:
            cluster.shutdown()
        if row is None:
            return False
        release = row[0]
        self.info = {"release_version": release, "host_id": str(row[1]) if row[1] else None, "data_center": row[2]}
        if release != self.expected:
            self.info["mismatch"] = f"release_version {release} != pinned {self.expected}"
            return False
        return True


def make_ready_probe(cfg: ProjectConfig) -> Callable[[], bool]:
    return CassandraReadyProbe(cfg)


def drain(runtime: RuntimeInfo, paths: WorkPaths, cfg: ProjectConfig, *, timeout: float = 60.0) -> dict[str, Any]:
    """``nodetool -h 127.0.0.1 -p 7199 drain`` (flush memtables, stop accepting writes) with a bound."""
    env = cassandra_env(runtime, paths, cfg, with_owner=False)
    # nodetool is a small JMX client: never give it the server's heap/direct-memory budget.
    for key in ("JVM_EXTRA_OPTS", "MAX_HEAP_SIZE", "MAX_DIRECT_MEMORY_SIZE"):
        env.pop(key, None)
    argv = [str(runtime.cassandra_home / "bin" / "nodetool"), "-h", "127.0.0.1", "-p", str(JMX_PORT), "drain"]
    try:
        r = subprocess.run(argv, env=env, cwd=str(paths.root), capture_output=True, text=True, timeout=timeout,
                           stdin=subprocess.DEVNULL, check=False)
    except subprocess.TimeoutExpired:
        return {"status": "TIMEOUT", "seconds": timeout}
    except OSError as exc:
        return {"status": "ERROR", "detail": str(exc)[:300]}
    return {"status": "OK" if r.returncode == 0 else "FAILED", "exit_code": r.returncode,
            "output": ((r.stdout or "") + (r.stderr or ""))[-800:]}


def apply_schema(cfg: ProjectConfig) -> list[dict[str, Any]]:
    """Keyspace + tables through WP4's migration runner (idempotent, checksum-checked)."""
    from transitpulse.storage.migrate import apply_migrations

    return apply_migrations(cfg)


def probe_read_write(cfg: ProjectConfig) -> dict[str, Any]:
    """Write then read back a short-lived row (TTL 600 s) in pipeline_status under a reserved run id."""
    from transitpulse.storage.cassandra import get_session

    session = get_session(cfg)
    ks = cfg.cassandra.keyspace
    marker = iso_now()
    session.execute(
        f"INSERT INTO {ks}.pipeline_status (run_id, component, status, last_error) VALUES (%s, %s, %s, %s) USING TTL 600",
        (PROBE_RUN_ID, "infra_probe", "OK", marker),
    )
    row = session.execute(
        f"SELECT status, last_error FROM {ks}.pipeline_status WHERE run_id = %s AND component = %s",
        (PROBE_RUN_ID, "infra_probe"),
    ).one()
    status = row["status"] if isinstance(row, dict) else (row[0] if row else None)
    value = row["last_error"] if isinstance(row, dict) else (row[1] if row else None)
    local = session.execute("SELECT release_version, host_id FROM system.local").one()
    release = local["release_version"] if isinstance(local, dict) else (local[0] if local else None)
    host_id = local["host_id"] if isinstance(local, dict) else (local[1] if local else None)
    ok = status == "OK" and value == marker
    return {"status": "PASS" if ok else "FAIL", "ttl_seconds": 600, "release_version": release,
            "host_id": str(host_id) if host_id else None}


def current_host_id(cfg: ProjectConfig) -> str | None:
    """``system.local.host_id`` (changes when Cassandra data is wiped) or None when unreachable."""
    try:
        from transitpulse.storage.cassandra import get_session

        row = get_session(cfg, keyspace=False).execute("SELECT host_id FROM system.local").one()
    except Exception as exc:  # noqa: BLE001 - informational
        log.debug("host_id query failed: %s", exc)
        return None
    if row is None:
        return None
    value = row["host_id"] if isinstance(row, dict) else row[0]
    return str(value) if value else None
