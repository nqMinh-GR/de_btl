"""Integration fixtures (design E §7.2): real Kafka + Cassandra + Spark started by the native supervisor.

* ``runtime``: ``<W>/runtime/runtime.json``; missing or unusable -> every test is NOT_RUN with the reason.
* ``native_stack``: ``scripts/infra.py start`` + ``init`` (idempotent), then Kafka metadata and Cassandra
  ``system.local`` are checked; teardown leaves infra running (other sessions may use it).
* ``guard_no_active_session``: a demo session's processor/publisher/collector/api is never touched; the tests are
  NOT_RUN ("stop demo session first"). Components left by an earlier pytest session are stopped (owned by tests).
* ``api_server``: real uvicorn with the core ``.venv`` interpreter on a free loopback port.
* ``kafka_admin``, ``cass``: real clients. ``harness`` (function scope) / ``module_harness`` wrap the shared
  :class:`transitpulse.verify.stack.StackHarness`.

The report plugin (``tests/plugins/test_report.py``) is registered here in ``pytest_configure`` (a
``pytest_plugins`` variable in a non-root conftest is rejected by pytest when the directory is collected from the
repository root, which would break ``pytest -m unit``). Disruptive cases (IT12, IT16) are moved to the end.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

_TESTS_DIR = Path(__file__).resolve().parents[1]
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))

from integration.cases import ADDITIONAL_TESTS, CT_CASES, DISRUPTIVE_ORDER, IT_CASES  # noqa: E402
from plugins.test_report import PLUGIN_NAME, ITReportPlugin  # noqa: E402


def pytest_configure(config: pytest.Config) -> None:
    if config.pluginmanager.get_plugin(PLUGIN_NAME) is None:
        config.pluginmanager.register(ITReportPlugin(config, IT_CASES, ADDITIONAL_TESTS, CT_CASES), PLUGIN_NAME)


def _case_of(item: pytest.Item) -> str | None:
    m = item.get_closest_marker("it_case")
    return str(m.args[0]) if m and m.args else None


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Stable reorder: every disruptive case (IT12 stops Cassandra, IT16 may restart infra) runs last."""
    rank = {cid: i for i, cid in enumerate(DISRUPTIVE_ORDER)}
    normal = [it for it in items if _case_of(it) not in rank]
    last = sorted((it for it in items if _case_of(it) in rank), key=lambda it: rank[_case_of(it) or ""])
    items[:] = normal + last


# --------------------------------------------------------------------------------------------------
# session fixtures
# --------------------------------------------------------------------------------------------------

@pytest.fixture(scope="session")
def tp_paths() -> Any:
    from transitpulse.config import load_config
    from transitpulse.runtime.paths import WorkPaths

    workdir = os.environ.get("TP_WORKDIR")
    if not workdir:
        try:
            workdir = str(load_config().workdir)
        except Exception as exc:  # noqa: BLE001 - reported as NOT_RUN
            pytest.skip(f"NOT_RUN: no work directory (set TP_WORKDIR): {exc}")
    return WorkPaths.from_workdir(workdir)


@pytest.fixture(scope="session")
def runtime(tp_paths: Any) -> Any:
    from transitpulse.runtime.manifest import load_runtime

    try:
        return load_runtime(tp_paths)
    except FileNotFoundError as exc:
        pytest.skip(f"NOT_RUN: {exc}")
    except ValueError as exc:
        pytest.skip(f"NOT_RUN: runtime.json is not usable for {tp_paths.root}: {exc}")


@pytest.fixture(scope="session")
def tp_profile(runtime: Any) -> str:
    return os.environ.get("TP_PROFILE") or runtime.profile


@pytest.fixture(scope="session")
def tp_cfg(tp_paths: Any, tp_profile: str) -> Any:
    from transitpulse.config import load_config

    return load_config(tp_profile, workdir=tp_paths.root)


@pytest.fixture(scope="session")
def core_env(runtime: Any) -> dict[str, str]:
    from transitpulse.runtime.manifest import subprocess_env

    return subprocess_env(runtime)


@pytest.fixture(scope="session")
def native_stack(runtime: Any, tp_paths: Any, tp_profile: str, tp_cfg: Any, repo_dir: Path,
                 core_env: dict[str, str]) -> dict[str, Any]:
    from confluent_kafka.admin import AdminClient

    from transitpulse.storage.cassandra import get_session
    from transitpulse.verify.fixture import run_json_cli

    def infra(*args: str, timeout: float) -> dict[str, Any]:
        argv = [str(runtime.core_python), str(repo_dir / "scripts" / "infra.py"), *args, "--profile", tp_profile,
                "--workdir", str(tp_paths.root)]
        return run_json_cli(argv, env=core_env, cwd=repo_dir, timeout=timeout)

    started = infra("start", timeout=900)
    inited = infra("init", timeout=600)
    admin = AdminClient({"bootstrap.servers": tp_cfg.kafka.bootstrap_servers})
    md = admin.list_topics(timeout=30)
    if not md.brokers:
        pytest.fail(f"Kafka at {tp_cfg.kafka.bootstrap_servers} reports no broker after infra start")
    rows = list(get_session(tp_cfg).execute("SELECT release_version FROM system.local"))
    if not rows:
        pytest.fail("Cassandra system.local returned no row after infra start")
    return {"start": started, "init": inited, "brokers": sorted(md.brokers), "topics": sorted(md.topics)}


@pytest.fixture(scope="session")
def session_id() -> str:
    from transitpulse.verify.stack import new_session_id

    return new_session_id("pytest")


@pytest.fixture(scope="session")
def guard_no_active_session(native_stack: dict[str, Any], tp_paths: Any, session_id: str) -> None:
    from transitpulse.runtime.supervisor import Supervisor

    sup = Supervisor(tp_paths)
    for comp in ("processor", "publisher", "collector", "api", "dashboard"):
        entry = sup.entry(comp)
        if not entry:
            continue
        try:
            alive = sup.is_alive(comp)
        except Exception:  # noqa: BLE001 - an unreadable entry is treated as not alive
            alive = False
        if not alive:
            continue
        owner = str(entry.get("session_id") or "")
        if owner.startswith("pytest-") and owner != session_id and comp != "api":
            sup.stop(comp)          # left behind by an earlier (crashed) pytest session: owned by the tests
            continue
        pytest.skip(f"NOT_RUN: a demo session is active ({comp} of session {owner or 'unknown'}); "
                    "stop it first with scripts/demo.py stop — tests never stop a demo session")


@pytest.fixture(scope="session")
def kafka_admin(tp_cfg: Any, native_stack: dict[str, Any]) -> Any:
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": tp_cfg.kafka.bootstrap_servers})


@pytest.fixture(scope="session")
def cass(tp_cfg: Any, native_stack: dict[str, Any]) -> Any:
    from transitpulse.storage.cassandra import get_session

    return get_session(tp_cfg)


@pytest.fixture(scope="session")
def evidence_root(tp_paths: Any, session_id: str) -> Path:
    p = tp_paths.reports / "integration" / session_id
    p.mkdir(parents=True, exist_ok=True)
    return p


@pytest.fixture(scope="session")
def api_server(runtime: Any, tp_paths: Any, tp_cfg: Any, tp_profile: str, repo_dir: Path,
               guard_no_active_session: None, evidence_root: Path) -> Iterator[str]:
    from transitpulse.verify.stack import ApiServer

    server = ApiServer(runtime=runtime, paths=tp_paths, cfg=tp_cfg, profile=tp_profile, repo_dir=repo_dir,
                       log_path=evidence_root / "api.log")
    base = server.start()
    try:
        yield base
    finally:
        server.stop()


@pytest.fixture(scope="session")
def stack_ctx(tp_cfg: Any, tp_paths: Any, runtime: Any, tp_profile: str, repo_dir: Path, golden_dir: Path,
              core_env: dict[str, str], native_stack: dict[str, Any]) -> Any:
    from transitpulse.verify.flows import StackContext

    return StackContext(cfg=tp_cfg, paths=tp_paths, runtime=runtime, profile=tp_profile, repo_dir=repo_dir,
                        golden_dir=golden_dir, env=dict(core_env))


def _make_harness(request: pytest.FixtureRequest, tp_cfg: Any, tp_paths: Any, runtime: Any, tp_profile: str,
                  session_id: str, repo_dir: Path, api_server: str, evidence_root: Path) -> Any:
    from integration.harness import IntegrationHarness

    safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in request.node.name)[:80]
    return IntegrationHarness(cfg=tp_cfg, paths=tp_paths, runtime=runtime, profile=tp_profile, session_id=session_id,
                              repo_dir=repo_dir, api_base=api_server, evidence_dir=evidence_root / safe)


@pytest.fixture
def harness(request: pytest.FixtureRequest, tp_cfg: Any, tp_paths: Any, runtime: Any, tp_profile: str,
            session_id: str, repo_dir: Path, api_server: str, evidence_root: Path, cass: Any, kafka_admin: Any,
            guard_no_active_session: None) -> Iterator[Any]:
    h = _make_harness(request, tp_cfg, tp_paths, runtime, tp_profile, session_id, repo_dir, api_server, evidence_root)
    try:
        yield h
    finally:
        try:
            h.cleanup_session_components()
        finally:
            h.close()


@pytest.fixture(scope="module")
def module_harness(request: pytest.FixtureRequest, tp_cfg: Any, tp_paths: Any, runtime: Any, tp_profile: str,
                   session_id: str, repo_dir: Path, api_server: str, evidence_root: Path, cass: Any,
                   kafka_admin: Any, guard_no_active_session: None) -> Iterator[Any]:
    h = _make_harness(request, tp_cfg, tp_paths, runtime, tp_profile, session_id, repo_dir, api_server, evidence_root)
    try:
        yield h
    finally:
        try:
            h.cleanup_session_components()
        finally:
            h.close()


@pytest.fixture
def case_record(request: pytest.FixtureRequest) -> Any:
    plugin = request.config.pluginmanager.get_plugin(PLUGIN_NAME)
    if plugin is None:
        pytest.fail("test report plugin is not registered")
    return plugin.record_for(request.node.nodeid)
