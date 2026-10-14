"""Bootstrap phase B (``scripts/prepare_runtime.py``), steps B1-B10 of design D §2.

Runs inside the core ``.venv`` created by phase A (``scripts/bootstrap_colab.sh``). Each step records
``{status, seconds, detail}`` into ``W/runtime/bootstrap_report.json`` (written also on failure). Order of
execution: B1 preflight, B2 manifest, B3 venv, B4 JDK, B5 Kafka, B6 Cassandra, B8 configs (the Ivy
settings used by B7 are a rendered config), B7 connector closure, B9 runtime.json/runtime.env, B10 probes.

Idempotent: verified artifacts and the connector lock are reused (cached archives are re-hashed every
run); configs are rewritten only when their content changes; nothing under W/data or W/infra is touched.
"""

from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import importlib
import logging
import os
import platform
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from transitpulse.runtime.artifacts import ArtifactError, head_ok, install_artifact
from transitpulse.runtime.cli_common import (
    EXIT_ARTIFACT,
    EXIT_CONFLICT,
    EXIT_FAILURE,
    EXIT_PREFLIGHT,
    CliError,
    JsonArgumentParser,
    Result,
    run_main,
)
from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.proc import GIB, ProcFS, cgroup_memory_limit, cpu_count, disk_info
from transitpulse.runtime.util import (
    atomic_write_json,
    atomic_write_text,
    check_safe_path,
    git_info,
    iso_now,
    read_json_or_none,
    sha256_bytes,
    sha256_file,
)

log = logging.getLogger("transitpulse.runtime.bootstrap")

SYSTEM_PATH = ("/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin")
NETWORK_HOSTS = ("https://github.com/", "https://api.adoptium.net/", "https://downloads.apache.org/",
                 "https://archive.apache.org/", "https://repo1.maven.org/maven2/", "https://nodejs.org/")
REPORT_PORTS = (9092, 9093, 9042, 7000, 7199, 8000, 3000)
DASHBOARD_MODULES = ("streamlit", "pydeck")
CORE_MODULES = ("pyspark", "py4j", "confluent_kafka", "cassandra", "fastapi", "uvicorn", "pydantic", "httpx",
                "yaml", "psutil", "prometheus_client", "transitpulse")


class StepFailed(Exception):
    def __init__(self, code: str, message: str, *, hint: str | None = None, exit_code: int = EXIT_FAILURE,
                 detail: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint
        self.exit_code = exit_code
        self.detail = detail


class Report:
    def __init__(self, paths: WorkPaths, repo_dir: Path, args: argparse.Namespace) -> None:
        self.paths = paths
        self.data: dict[str, Any] = {
            "schema_version": 1, "overall": "RUNNING", "started_at": iso_now(), "finished_at": None,
            "workdir": str(paths.root), "repo_dir": str(repo_dir), "profile": args.profile, "strict": args.strict,
            "offline": args.offline, "steps": {}, "artifacts": [], "versions": {}, "warnings": [],
            "connector": None, "probes": {}, "ran_as_root": os.getuid() == 0 or os.getgid() == 0,
            "lock_origin": args.lock_origin, "error": None, "tunnel": None, "no_tunnel": bool(args.no_tunnel),
        }

    def warn(self, msg: str) -> None:
        log.warning(msg)
        self.data["warnings"].append(msg)

    def step(self, sid: str, name: str, fn: Callable[[], Any]) -> Any:
        log.info("[%s] %s", sid, name)
        t0 = time.monotonic()
        try:
            detail = fn()
        except (StepFailed, ArtifactError, CliError) as exc:
            self.data["steps"][sid] = {"name": name, "status": "FAIL", "seconds": round(time.monotonic() - t0, 2),
                                       "detail": {"code": exc.code, "message": exc.message}}
            raise
        except Exception as exc:
            self.data["steps"][sid] = {"name": name, "status": "FAIL", "seconds": round(time.monotonic() - t0, 2),
                                       "detail": {"code": "INTERNAL_ERROR", "message": f"{type(exc).__name__}: {exc}"}}
            raise
        self.data["steps"][sid] = {"name": name, "status": "PASS", "seconds": round(time.monotonic() - t0, 2),
                                   "detail": detail}
        return detail

    def skip(self, sid: str, name: str, reason: str) -> None:
        log.info("[%s] %s: skipped (%s)", sid, name, reason)
        self.data["steps"][sid] = {"name": name, "status": "SKIPPED", "seconds": 0.0, "detail": {"reason": reason}}

    def write(self) -> None:
        self.data["finished_at"] = iso_now()
        atomic_write_json(self.paths.bootstrap_report, self.data)


@contextlib.contextmanager
def bootstrap_lock(paths: WorkPaths, *, held_by_parent: bool) -> Iterator[None]:
    if held_by_parent:
        yield
        return
    paths.runtime_dir.mkdir(parents=True, exist_ok=True)
    fd = os.open(paths.runtime_dir / "bootstrap.lock", os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EAGAIN, errno.EACCES):
                raise CliError("BOOTSTRAP_LOCKED", "another bootstrap is running for this work directory",
                               hint="wait for it to finish", exit_code=EXIT_CONFLICT) from exc
            raise
        yield
    finally:
        os.close(fd)


def canonical_env(*, java_home: Path, venv_bin: Path, spark_home: Path, core_python: Path, paths: WorkPaths,
                  repo_dir: Path, profile: str) -> dict[str, str]:
    path = ":".join([str(java_home / "bin"), str(venv_bin)] + [p for p in SYSTEM_PATH if Path(p).is_dir()])
    return {
        "JAVA_HOME": str(java_home),
        "PATH": path,
        "SPARK_HOME": str(spark_home),
        "PYSPARK_PYTHON": str(core_python),
        "PYSPARK_DRIVER_PYTHON": str(core_python),
        "SPARK_LOCAL_IP": "127.0.0.1",
        "TZ": "UTC",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "TMPDIR": str(paths.tmp),
        "PYTHONUNBUFFERED": "1",
        "PYTHONNOUSERSITE": "1",
        "TP_WORKDIR": str(paths.root),
        "TP_REPO_DIR": str(repo_dir),
        "TP_PROFILE": profile,
    }


def _run(argv: list[str], env: dict[str, str], cwd: Path, timeout: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, env=env, cwd=str(cwd), capture_output=True, text=True, timeout=timeout,
                          stdin=subprocess.DEVNULL, check=False)


# --------------------------------------------------------------------------------------------------
# Steps
# --------------------------------------------------------------------------------------------------

def step_preflight(paths: WorkPaths, repo_dir: Path, cfg: Any, args: argparse.Namespace, report: Report) -> dict:
    try:
        check_safe_path(paths.root, "work directory")
        check_safe_path(repo_dir, "repository directory")
    except ValueError as exc:
        raise StepFailed("UNSAFE_PATH", str(exc), exit_code=EXIT_PREFLIGHT) from exc
    machine = platform.machine()
    if platform.system() != "Linux" or machine not in ("x86_64", "amd64"):
        raise StepFailed("UNSUPPORTED_PLATFORM", f"{platform.system()} {machine} is not supported",
                         hint="the native runner targets Linux x86_64 (Google Colab CPU VM)", exit_code=EXIT_PREFLIGHT)
    proc = ProcFS()
    mem = proc.meminfo()
    disk = disk_info(paths.root)
    detail: dict[str, Any] = {
        "uid": os.getuid(), "gid": os.getgid(), "arch": machine, "kernel": platform.release(),
        "cpus": cpu_count(), "mem_total_bytes": mem.get("MemTotal"), "mem_available_bytes": mem.get("MemAvailable"),
        "cgroup_memory_limit_bytes": cgroup_memory_limit(), "disk_free_bytes": disk.free_bytes,
        "inodes_free": disk.inodes_free, "ports": {}, "network": {},
    }
    if disk.free_bytes < cfg.runtime.min_disk_free_bytes:
        raise StepFailed("DISK_TOO_SMALL", f"only {disk.free_bytes / GIB:.1f} GiB free on {disk.path}; "
                         f"at least {cfg.runtime.min_disk_free_bytes / GIB:.0f} GiB required",
                         hint="free disk space or choose another --workdir", exit_code=EXIT_PREFLIGHT)
    if disk.free_bytes < 10 * GIB:
        report.warn(f"disk free {disk.free_bytes / GIB:.1f} GiB < 10 GiB recommended")
    if detail["uid"] == 0 or detail["gid"] == 0:
        if cfg.runtime.allow_root_in_demo_vm:
            report.warn("running as root: Cassandra will be started with -R (runtime.allow_root_in_demo_vm=true)")
        else:
            report.warn("running as root but runtime.allow_root_in_demo_vm=false: Cassandra will refuse to start")
    listeners = proc.tcp_listeners()
    for port in REPORT_PORTS:
        detail["ports"][str(port)] = "LISTENING" if port in listeners else "FREE"
    if not args.offline:
        reachable = 0
        for url in NETWORK_HOSTS:
            ok, info = head_ok(url, timeout=10)
            detail["network"][url] = info
            reachable += int(ok)
            if not ok:
                report.warn(f"cannot reach {url}: {info}")
        if reachable == 0:
            raise StepFailed("NETWORK_UNAVAILABLE", "none of the artifact hosts is reachable",
                             hint="first bootstrap needs Internet access (or use --offline with a warm cache)",
                             exit_code=EXIT_PREFLIGHT)
    return detail


def step_manifest(repo_dir: Path, paths: WorkPaths, args: argparse.Namespace) -> dict:
    from transitpulse.runtime.manifest import ManifestError, artifact_entry, load_artifact_manifest, unpinned_artifacts

    try:
        manifest = load_artifact_manifest(repo_dir)
    except ManifestError as exc:
        raise StepFailed("MANIFEST_INVALID", str(exc), exit_code=EXIT_ARTIFACT) from exc
    unpinned = unpinned_artifacts(manifest)
    if args.strict and unpinned:
        raise StepFailed("CHECKSUM_NOT_PINNED", f"--strict: no pinned checksum for {', '.join(unpinned)}",
                         hint=f"run `scripts/prepare_runtime.py --emit-pin-patch --workdir {paths.root}` after a "
                              "verified bootstrap and commit the values",
                         exit_code=EXIT_ARTIFACT)
    uv = artifact_entry(manifest, "uv")
    uv_bin = paths.root / uv["install_to"] / "uv"
    uv_version = None
    if uv_bin.is_file():
        r = _run([str(uv_bin), "--version"], {"PATH": ":".join(SYSTEM_PATH)}, paths.root, 30)
        uv_version = r.stdout.strip()
    if not uv_version or not uv_version.startswith(str(uv.get("expect", f"uv {uv['version']}"))):
        raise StepFailed("UV_MISMATCH", f"{uv_bin} reports {uv_version!r}, manifest expects {uv.get('expect')!r}",
                         hint=f"run `bash scripts/bootstrap_colab.sh --workdir {paths.root}` (phase A installs the "
                              "pinned uv)",
                         exit_code=EXIT_ARTIFACT)
    return {"manifest": manifest, "sha256": sha256_file(repo_dir / "deploy" / "runtime-manifest.json"),
            "unpinned": unpinned, "uv": {"path": str(uv_bin), "version": uv_version}}


def step_venv(repo_dir: Path, manifest: dict, report: Report, *, require_dashboard: bool = True) -> dict:
    venv = repo_dir / ".venv"
    if Path(sys.prefix).resolve() != venv.resolve():
        raise StepFailed("WRONG_INTERPRETER", f"prepare_runtime.py runs in {sys.prefix}, expected {venv}",
                         hint=f"run `{venv}/bin/python scripts/prepare_runtime.py --workdir {report.paths.root}` "
                              "(bootstrap_colab.sh does this)",
                         exit_code=EXIT_PREFLIGHT)
    pin = (repo_dir / ".python-version").read_text(encoding="utf-8").strip()
    actual = ".".join(str(x) for x in sys.version_info[:3])
    if actual != pin:
        raise StepFailed("PYTHON_VERSION_MISMATCH", f".venv python is {actual}, .python-version pins {pin}",
                         hint="delete .venv and re-run bootstrap", exit_code=EXIT_PREFLIGHT)
    versions: dict[str, Any] = {}
    for name in CORE_MODULES:
        try:
            mod = importlib.import_module(name)
        except Exception as exc:  # noqa: BLE001 - report every broken dependency
            raise StepFailed("CORE_IMPORT_FAILED", f"cannot import {name} in the core .venv: {exc}",
                             hint="re-run bootstrap (uv sync --locked)", exit_code=EXIT_PREFLIGHT) from exc
        versions[name] = getattr(mod, "__version__", None)
    for name in DASHBOARD_MODULES:
        try:
            mod = importlib.import_module(name)
            versions[name] = getattr(mod, "__version__", None)
        except Exception as exc:  # noqa: BLE001
            msg = f"cannot import {name} (dashboard extra) in the core .venv: {exc}"
            if require_dashboard:
                raise StepFailed("DASHBOARD_IMPORT_FAILED", msg,
                                 hint="bootstrap syncs with `uv sync --locked --extra dashboard`; re-run it",
                                 exit_code=EXIT_PREFLIGHT) from exc
            report.warn(msg)
    import confluent_kafka

    versions["confluent_kafka"] = confluent_kafka.version()[0]
    versions["librdkafka"] = confluent_kafka.libversion()[0]
    try:
        importlib.import_module("cassandra.cluster")
    except Exception as exc:  # noqa: BLE001
        raise StepFailed("CORE_IMPORT_FAILED", f"cassandra.cluster import failed: {exc}",
                         hint="cassandra-driver needs an event loop (asyncore on 3.11 or libev)",
                         exit_code=EXIT_PREFLIGHT) from exc
    import pyspark

    want = manifest["pyspark"]["version"]
    if pyspark.__version__ != want:
        raise StepFailed("PYSPARK_VERSION_MISMATCH", f"pyspark {pyspark.__version__} != pinned {want}",
                         exit_code=EXIT_PREFLIGHT)
    spark_home = Path(pyspark.__file__).resolve().parent
    fixed: list[str] = []
    for sub in ("bin", "sbin"):
        d = spark_home / sub
        if not d.is_dir():
            continue
        for f in d.iterdir():
            if f.is_file() and not f.name.endswith(".cmd") and not os.access(f, os.X_OK):
                f.chmod(f.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                fixed.append(f.name)
    if fixed:
        report.warn(f"restored the executable bit on {len(fixed)} Spark launcher script(s) in {spark_home}")
    if not (spark_home / "bin" / "spark-submit").is_file():
        raise StepFailed("SPARK_LAUNCHER_MISSING", f"{spark_home}/bin/spark-submit not found", exit_code=EXIT_PREFLIGHT)
    lock = repo_dir / "uv.lock"
    if not lock.is_file():
        raise StepFailed("LOCK_MISSING", "uv.lock not found",
                         hint=f"run `bash scripts/bootstrap_colab.sh --workdir {report.paths.root}`",
                         exit_code=EXIT_PREFLIGHT)
    return {"python": actual, "python_executable": str(venv / "bin" / "python"), "modules": versions,
            "spark_home": str(spark_home), "lock_sha256": sha256_file(lock)}


def step_artifact(entry: dict, paths: WorkPaths, args: argparse.Namespace, report: Report,
                  check: Callable[[Path], dict]) -> dict:
    res = install_artifact(entry, paths, offline=args.offline, strict=args.strict, progress=log.info)
    for w in res.warnings:
        report.warn(w)
    detail = res.to_json()
    detail.update(check(res.home))
    report.data["artifacts"].append(detail)
    return detail


def _check_jdk(entry: dict) -> Callable[[Path], dict]:
    def check(home: Path) -> dict:
        java = home / "bin" / "java"
        r = _run([str(java), "-version"], {"PATH": ":".join(SYSTEM_PATH)}, home, 60)
        text = (r.stderr or "") + (r.stdout or "")
        expect = str(entry.get("expect"))
        vendor = str(entry.get("expect_vendor", "Temurin"))
        if r.returncode != 0 or f'"{expect}"' not in text or vendor not in text:
            raise StepFailed("JAVA_VERSION_MISMATCH", f"{java} -version does not report {expect} {vendor}: "
                             f"{text.strip()[:300]}", exit_code=EXIT_ARTIFACT)
        return {"java_version_output": text.strip().splitlines()[:3]}
    return check


def _check_file(entry: dict) -> Callable[[Path], dict]:
    def check(home: Path) -> dict:
        rel = entry.get("expect_file")
        if rel and not (home / rel).is_file():
            raise StepFailed("ARTIFACT_INCOMPLETE", f"{home / rel} missing after install", exit_code=EXIT_ARTIFACT)
        return {"expect_file": rel}
    return check


def step_tunnel(entry: dict, paths: WorkPaths, args: argparse.Namespace, report: Report) -> dict:
    """cloudflared single binary: .partial -> sha256 (pinned from the release notes) -> os.replace -> chmod."""
    res = install_artifact(entry, paths, offline=args.offline, strict=args.strict, progress=log.info)
    for w in res.warnings:
        report.warn(w)
    r = _run([str(res.home), "--version"], {"PATH": ":".join(SYSTEM_PATH), "HOME": str(paths.tmp)}, paths.root, 60)
    text = ((r.stdout or "") + (r.stderr or "")).strip()
    expect = str(entry.get("expect") or entry["version"])
    if r.returncode != 0 or expect not in text:
        raise StepFailed("CLOUDFLARED_PROBE_FAILED", f"{res.home} --version did not report {expect}: {text[:300]}",
                         hint=f"re-run `bash scripts/bootstrap_colab.sh --workdir {paths.root}` (add --no-tunnel to "
                              "skip the public tunnel)",
                         exit_code=EXIT_ARTIFACT)
    detail = res.to_json()
    detail["version_output"] = text.splitlines()[0] if text else None
    report.data["artifacts"].append(detail)
    report.data["tunnel"] = {"cloudflared": str(res.home), "version": entry["version"], "sha256": res.digest,
                             "status": res.status}
    return detail


def step_node(entry: dict, paths: WorkPaths, args: argparse.Namespace, report: Report) -> dict:
    """Pinned Node.js distribution used to build and preview the React console."""
    res = install_artifact(entry, paths, offline=args.offline, strict=args.strict, progress=log.info)
    for warning in res.warnings:
        report.warn(warning)
    node = res.home / "bin" / "node"
    npm = res.home / "bin" / "npm"
    if not node.is_file() or not npm.is_file():
        raise StepFailed("NODE_INCOMPLETE", f"{node} or {npm} missing after install", exit_code=EXIT_ARTIFACT)
    env = {"PATH": f"{res.home / 'bin'}:/usr/bin:/bin", "HOME": str(paths.tmp), "LANG": "C.UTF-8"}
    probed = _run([str(node), "--version"], env, paths.root, 60)
    text = ((probed.stdout or "") + (probed.stderr or "")).strip()
    expect = str(entry.get("expect") or f"v{entry['version']}")
    if probed.returncode != 0 or expect not in text:
        raise StepFailed("NODE_PROBE_FAILED", f"{node} --version did not report {expect}: {text[:300]}",
                         exit_code=EXIT_ARTIFACT)
    detail = res.to_json()
    detail["version_output"] = text.splitlines()[0] if text else None
    report.data["artifacts"].append(detail)
    return {"node": str(node), "npm": str(npm), "version": entry["version"], "home": str(res.home)}


def step_frontend(repo_dir: Path, paths: WorkPaths, node_info: dict, args: argparse.Namespace, report: Report) -> dict:
    """``npm ci`` and ``vite build`` with a relative API base. Skips when dist still matches the lockfile."""
    frontend = repo_dir / "frontend"
    lock = frontend / "package-lock.json"
    if not lock.is_file():
        raise StepFailed("FRONTEND_MISSING", f"{lock} not found", exit_code=EXIT_ARTIFACT)
    lock_sha = sha256_file(lock)
    dist = frontend / "dist"
    index = dist / "index.html"
    stamp_path = dist / ".tp-build.json"
    previous = read_json_or_none(stamp_path)
    if (index.is_file() and isinstance(previous, dict) and previous.get("lock_sha256") == lock_sha
            and previous.get("vite_api_base") == "/api/v1"):
        return {"status": "REUSED", "dist": str(dist), "lock_sha256": lock_sha}
    if args.offline:
        raise StepFailed("FRONTEND_OFFLINE", "frontend/dist is missing or stale and --offline was given",
                         hint="run bootstrap once with network access so npm ci can install frontend dependencies",
                         exit_code=EXIT_ARTIFACT)
    cache = paths.root / "cache" / "npm"
    cache.mkdir(parents=True, exist_ok=True)
    env = {
        "PATH": f"{Path(node_info['home']) / 'bin'}:/usr/bin:/bin",
        "HOME": str(paths.tmp),
        "LANG": "C.UTF-8",
        "CI": "1",
        "npm_config_cache": str(cache),
        "npm_config_update_notifier": "false",
    }
    installed = _run([node_info["npm"], "ci", "--no-audit", "--no-fund"], env, frontend, 900)
    if installed.returncode != 0:
        tail = ((installed.stderr or "") + (installed.stdout or ""))[-800:]
        raise StepFailed("FRONTEND_INSTALL_FAILED", f"npm ci failed: {tail}", exit_code=EXIT_ARTIFACT)
    built = _run([node_info["npm"], "run", "build"], {**env, "VITE_API_BASE": "/api/v1"}, frontend, 900)
    if built.returncode != 0 or not index.is_file():
        tail = ((built.stderr or "") + (built.stdout or ""))[-800:]
        raise StepFailed("FRONTEND_BUILD_FAILED", f"vite build failed: {tail}", exit_code=EXIT_ARTIFACT)
    atomic_write_json(stamp_path, {"lock_sha256": lock_sha, "vite_api_base": "/api/v1", "built_at": iso_now()})
    return {"status": "BUILT", "dist": str(dist), "lock_sha256": lock_sha}


def step_configs(repo_dir: Path, paths: WorkPaths, cfg: Any, cassandra_home: Path) -> dict:
    from transitpulse.runtime.configs import ConfigRenderError, materialize

    try:
        record = materialize(repo_dir, paths, cfg, cassandra_home=cassandra_home)
    except (ConfigRenderError, OSError) as exc:
        raise StepFailed("CONFIG_RENDER_FAILED", str(exc)) from exc
    return {"hash": record["hash"], "changed": record["changed"], "files": len(record["files"])}


def step_connector(repo_dir: Path, paths: WorkPaths, manifest: dict, spark_home: Path, env: dict,
                   args: argparse.Namespace, report: Report) -> dict:
    from transitpulse.runtime.connector import resolve_closure

    conn = manifest["spark_connector"]
    res = resolve_closure(spark_home=spark_home, env=env, paths=paths, repo_dir=repo_dir,
                          coordinate=conn["coordinate"], spark_version=manifest["pyspark"]["version"],
                          offline=args.offline, expected_direct=conn.get("expected_direct", []))
    for w in res["warnings"]:
        report.warn(w)
    lock = res["lock"]
    report.data["connector"] = {
        "status": res["status"], "coordinate": lock["coordinate"], "closure_sha256": lock.get("closure_sha256"),
        "jars": [{"file": j["file"], "sha256": j["sha256"], "sha1_verified": True} for j in lock["jars"]],
        "provided_by_spark": [f"{p['org']}:{p['module']}:{p['rev']}" for p in lock.get("provided_by_spark", [])],
    }
    return {"status": res["status"], "jars": [j["path"] for j in lock["jars"]], "probe": res.get("probe")}


def step_probes(repo_dir: Path, paths: WorkPaths, env: dict, jars: list[str], spark_home: Path, kafka_home: Path,
                cassandra_home: Path, cfg: Any, report: Report) -> dict:
    from transitpulse.runtime.connector import cached_launch_probe

    out: dict[str, Any] = {}
    r = _run([str(Path(env["JAVA_HOME"]) / "bin" / "java"), "-version"], env, paths.root, 60)
    out["java"] = ((r.stderr or "") + (r.stdout or "")).strip().splitlines()[:3]
    probe = cached_launch_probe(spark_home=spark_home, env=env, paths=paths, repo_dir=repo_dir, jars=jars)
    out["spark_cached_launch"] = probe
    if probe.get("spark_version") != "3.5.7" or not str(probe.get("scala_version", "")).startswith("2.12"):
        raise StepFailed("SPARK_VERSION_MISMATCH", f"probe reports Spark {probe.get('spark_version')} / Scala "
                         f"{probe.get('scala_version')}", exit_code=EXIT_ARTIFACT)
    kenv = dict(env, KAFKA_HEAP_OPTS="-Xmx256m", LOG_DIR=str(paths.log_dir("kafka")))
    r = _run([str(kafka_home / "bin" / "kafka-storage.sh"), "random-uuid"], kenv, paths.root, 120)
    if r.returncode != 0:
        raise StepFailed("KAFKA_PROBE_FAILED", f"kafka-storage.sh random-uuid failed: {(r.stderr or r.stdout)[-500:]}",
                         exit_code=EXIT_ARTIFACT)
    out["kafka_storage_tool"] = "OK"
    cenv = dict(env, CASSANDRA_CONF=str(paths.conf / "cassandra"), CASSANDRA_LOG_DIR=str(paths.log_dir("cassandra")),
                MAX_HEAP_SIZE=cfg.runtime.cassandra_max_heap,
                MAX_DIRECT_MEMORY_SIZE=cfg.runtime.cassandra_max_direct_memory)
    paths.log_dir("cassandra").mkdir(parents=True, exist_ok=True)
    argv = [str(cassandra_home / "bin" / "cassandra"), "-v"]
    if os.getuid() == 0 or os.getgid() == 0:
        argv.append("-R")
    r = _run(argv, cenv, paths.root, 120)
    text = ((r.stdout or "") + (r.stderr or "")).strip()
    if r.returncode != 0 or "5.0.9" not in text:
        raise StepFailed("CASSANDRA_PROBE_FAILED", f"bin/cassandra -v did not report 5.0.9: {text[-500:]}",
                         exit_code=EXIT_ARTIFACT)
    out["cassandra_version"] = text.splitlines()[-1].strip()
    return out


# --------------------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------------------

def build_parser() -> JsonArgumentParser:
    p = JsonArgumentParser(prog="prepare_runtime.py",
                           description="Bootstrap phase B: verify/install JDK, Kafka, Cassandra and the Spark Kafka "
                                       "connector; render configs; write runtime.json and bootstrap_report.json.")
    p.add_argument("--workdir", required=True)
    p.add_argument("--repo-dir", default=None, help="repository root (default: inferred from this package)")
    p.add_argument("--profile", default=None, choices=["colab-native", "local-native"],
                   help="default: $TP_PROFILE, then the profile of an existing runtime.json, then colab-native")
    p.add_argument("--strict", action="store_true", help="fail when any artifact checksum is not pinned")
    p.add_argument("--offline", action="store_true", help="use caches only; never download")
    p.add_argument("--lock-origin", default=None, choices=["committed", "generated_on_colab", "generated_locally"],
                   help="set by bootstrap_colab.sh")
    p.add_argument("--emit-pin-patch", action="store_true",
                   help="include the observed artifact checksums (to review and commit) in the JSON output")
    p.add_argument("--no-tunnel", action="store_true",
                   help="do not install cloudflared (no public quick tunnel for the dashboard)")
    p.add_argument("--lock-held", action="store_true", help=argparse.SUPPRESS)
    return p


def prepare(args: argparse.Namespace) -> Result:
    from transitpulse.config import load_config, repo_root
    from transitpulse.runtime.manifest import artifact_entry, render_runtime_env_file

    from transitpulse.runtime.cli_common import resolve_profile

    args.profile = resolve_profile(args.workdir, args.profile)
    if args.profile not in ("colab-native", "local-native"):
        raise CliError("USAGE", f"bootstrap supports the native profiles only, got {args.profile!r}",
                       hint="use --profile colab-native or local-native", exit_code=2)
    repo_dir = Path(args.repo_dir).resolve() if args.repo_dir else repo_root()
    os.environ["TP_REPO_DIR"] = str(repo_dir)
    paths = WorkPaths.from_workdir(args.workdir)
    paths.ensure_base_dirs()
    for sub in ("bootstrap", "kafka", "cassandra"):
        paths.log_dir(sub).mkdir(parents=True, exist_ok=True)
    if args.lock_origin is None:
        prev = read_json_or_none(paths.runtime_dir / "lock_origin.json") or {}
        args.lock_origin = prev.get("lock_origin") or "committed"
    report = Report(paths, repo_dir, args)
    with bootstrap_lock(paths, held_by_parent=args.lock_held):
        try:
            cfg = load_config(args.profile, workdir=paths.root)
            report.step("B1", "preflight", lambda: step_preflight(paths, repo_dir, cfg, args, report))
            m = report.step("B2", "runtime manifest", lambda: step_manifest(repo_dir, paths, args))
            manifest = m["manifest"]
            report.data["steps"]["B2"]["detail"] = {k: v for k, v in m.items() if k != "manifest"}
            venv = report.step("B3", "core .venv",
                               lambda: step_venv(repo_dir, manifest, report, require_dashboard=not args.no_tunnel))
            jdk_e = artifact_entry(manifest, "jdk")
            jdk = report.step("B4", "JDK", lambda: step_artifact(jdk_e, paths, args, report, _check_jdk(jdk_e)))
            kaf_e = artifact_entry(manifest, "kafka")
            kafka = report.step("B5", "Kafka", lambda: step_artifact(kaf_e, paths, args, report, _check_file(kaf_e)))
            cas_e = artifact_entry(manifest, "cassandra")
            cass = report.step("B6", "Cassandra",
                               lambda: step_artifact(cas_e, paths, args, report, _check_file(cas_e)))
            if args.no_tunnel:
                report.skip("B6T", "cloudflared (public quick tunnel)", "--no-tunnel")
            else:
                cf_e = artifact_entry(manifest, "cloudflared")
                try:
                    report.step("B6T", "cloudflared (public quick tunnel)",
                                lambda: step_tunnel(cf_e, paths, args, report))
                except (StepFailed, ArtifactError) as exc:
                    # Optional component: a failure never stops the bootstrap (unless --strict), but it is
                    # recorded loudly: step FAIL, artifact status MISMATCH/FAILED, a warning, tunnel=None.
                    if args.strict:
                        raise
                    mismatch = exc.code == "CHECKSUM_MISMATCH"
                    report.data["artifacts"].append({
                        "id": "cloudflared", "version": cf_e["version"], "status": "MISMATCH" if mismatch else "FAILED",
                        "algorithm": cf_e["checksum"]["algorithm"], "observed": None,
                        "official": cf_e["checksum"].get("value"), "checksum_source": cf_e["checksum"].get(
                            "official_source"), "error": {"code": exc.code, "message": exc.message}})
                    report.data["tunnel"] = None
                    report.warn(f"cloudflared {'CHECKSUM MISMATCH' if mismatch else 'install failed'} "
                                f"({exc.code}: {exc.message}); the public tunnel (demo.py expose) is disabled. "
                                "Nothing unverified was installed.")
            node_e = artifact_entry(manifest, "node")
            try:
                node_info = report.step("B6N", "Node.js (React console)",
                                        lambda: step_node(node_e, paths, args, report))
            except (StepFailed, ArtifactError) as exc:
                if args.strict:
                    raise
                report.data["frontend"] = None
                report.warn(f"Node.js install failed ({exc.code}: {exc.message}); the React console will not build. "
                            "Kafka, Cassandra and the API are unaffected.")
                node_info = None
            if node_info is not None:
                try:
                    built = report.step("B6F", "React console (npm ci + vite build)",
                                        lambda: step_frontend(repo_dir, paths, node_info, args, report))
                    report.data["frontend"] = {
                        **node_info, **built, "local_url": "http://127.0.0.1:3000", "built": True,
                    }
                except (StepFailed, ArtifactError) as exc:
                    if args.strict:
                        raise
                    report.data["frontend"] = {**node_info, "local_url": "http://127.0.0.1:3000", "built": False,
                                               "dist": str(repo_dir / "frontend" / "dist")}
                    report.warn(f"React build failed ({exc.code}: {exc.message}); demo.py expose --target react "
                                "stays unavailable until bootstrap succeeds at step B6F.")
            java_home = Path(jdk["home"])
            spark_home = Path(venv["spark_home"])
            core_python = repo_dir / ".venv" / "bin" / "python"
            env = canonical_env(java_home=java_home, venv_bin=core_python.parent, spark_home=spark_home,
                                core_python=core_python, paths=paths, repo_dir=repo_dir, profile=args.profile)
            from transitpulse.runtime.manifest import clean_base_env

            run_env = {**clean_base_env(), **env}
            conf = report.step("B8", "native configs",
                               lambda: step_configs(repo_dir, paths, cfg, Path(cass["home"])))
            conn = report.step("B7", "Spark Kafka connector closure",
                               lambda: step_connector(repo_dir, paths, manifest, spark_home, run_env, args, report))
            git = git_info(repo_dir)
            runtime_doc = {
                "schema_version": 1, "profile": args.profile, "repo_dir": str(repo_dir),
                "repo_commit": git.get("commit"), "repo_dirty": git.get("dirty"), "workdir": str(paths.root),
                "generated_at": iso_now(),
                "python": {"core_python": str(core_python), "version": venv["python"], "uv": m["uv"]["path"],
                           "uv_version": m["uv"]["version"], "lock_sha256": venv["lock_sha256"],
                           "lock_origin": args.lock_origin},
                "java": {"java_home": str(java_home), "version": str(jdk_e.get("expect")),
                         "vendor": jdk_e.get("vendor", "Eclipse Temurin"), "release": jdk_e["version"]},
                "kafka": {"home": kafka["home"], "version": kaf_e["version"],
                          "config": str(paths.conf / "kafka" / "server.properties"), "log_dir": str(paths.kafka_data),
                          "bootstrap": cfg.kafka.bootstrap_servers},
                "cassandra": {"home": cass["home"], "version": cas_e["version"],
                              "conf_dir": str(paths.conf / "cassandra"), "host": cfg.cassandra.hosts[0],
                              "port": cfg.cassandra.port},
                "spark": {"spark_home": str(spark_home), "version": manifest["pyspark"]["version"],
                          "scala": manifest["pyspark"].get("scala", "2.12"), "connector_jars": conn["jars"],
                          "connector_coordinate": manifest["spark_connector"]["coordinate"]},
                "api": {"base_url": f"http://127.0.0.1:{cfg.api.port}{cfg.api.base_path}"},
                "dashboard": {"local_url": "http://127.0.0.1:8501", "app": str(repo_dir / "app" / "streamlit_app.py")},
                "frontend": report.data.get("frontend"),
                "tunnel": ({"cloudflared": report.data["tunnel"]["cloudflared"],
                            "version": report.data["tunnel"]["version"]} if report.data.get("tunnel") else None),
                "paths": {"workdir": str(paths.root), "conf": str(paths.conf), "kafka_data": str(paths.kafka_data),
                          "cassandra_root": str(paths.cassandra_root), "logs": str(paths.root / "logs"),
                          "reports": str(paths.reports), "checkpoints": str(paths.root / "checkpoints"),
                          "registry": str(paths.registry_json), "spark_local": str(paths.spark_local),
                          "tmp": str(paths.tmp), "connector_jars": str(paths.connector_jars),
                          "ivy_cache": str(paths.ivy_cache)},
                "hashes": {"manifest": m["sha256"], "config": cfg.config_hash(), "lock": venv["lock_sha256"],
                           "rendered_conf": conf["hash"]},
                "env": env,
            }

            def _write_runtime() -> dict:
                content_hash = sha256_bytes(repr(sorted((k, repr(v)) for k, v in runtime_doc.items()
                                                        if k != "generated_at")).encode())
                runtime_doc["content_sha256"] = content_hash
                atomic_write_json(paths.runtime_json, runtime_doc)
                atomic_write_text(paths.runtime_env, render_runtime_env_file(env))
                return {"runtime_json": str(paths.runtime_json), "runtime_env": str(paths.runtime_env),
                        "content_sha256": content_hash}

            report.step("B9", "runtime.json / runtime.env", _write_runtime)
            probes = report.step("B10", "compatibility probes",
                                 lambda: step_probes(repo_dir, paths, run_env, conn["jars"], spark_home,
                                                     Path(kafka["home"]), Path(cass["home"]), cfg, report))
            report.data["probes"] = probes
            report.data["versions"] = {
                "python": venv["python"], "uv": m["uv"]["version"], "java": jdk_e.get("expect"),
                "kafka": kaf_e["version"], "cassandra": cas_e["version"], "spark": manifest["pyspark"]["version"],
                "scala": (probes.get("spark_cached_launch") or {}).get("scala_version"),
                "cloudflared": (report.data.get("tunnel") or {}).get("version"),
                "node": (report.data.get("frontend") or {}).get("version"),
                "streamlit": venv["modules"].get("streamlit"),
                "pyspark_module": venv["modules"].get("pyspark"),
                "confluent_kafka": venv["modules"].get("confluent_kafka"),
                "cassandra_driver": venv["modules"].get("cassandra"),
            }
            report.data["repo_commit"] = git.get("commit")
            report.data["overall"] = "PASS"
        except (StepFailed, ArtifactError, CliError) as exc:
            report.data["overall"] = "FAIL"
            report.data["error"] = {"code": exc.code, "message": exc.message, "hint": exc.hint}
            report.write()
            exit_code = exc.exit_code if isinstance(exc, (StepFailed, CliError)) else EXIT_ARTIFACT
            raise CliError(exc.code, exc.message, hint=exc.hint, exit_code=exit_code,
                           details={"report_path": str(paths.bootstrap_report),
                                    "steps": {k: v["status"] for k, v in report.data["steps"].items()}}) from exc
        except Exception as exc:
            report.data["overall"] = "FAIL"
            report.data["error"] = {"code": "INTERNAL_ERROR", "message": f"{type(exc).__name__}: {exc}", "hint": None}
            report.write()
            raise
        report.write()
    pin_patch = {a["id"]: {"algorithm": a["algorithm"], "value": a["observed"], "official": a["official"],
                           "source": a["checksum_source"]} for a in report.data["artifacts"] if a.get("observed")}
    atomic_write_json(paths.runtime_dir / "manifest_pin_patch.json", pin_patch)
    payload: dict[str, Any] = {
        "overall": report.data["overall"],
        "report_path": str(paths.bootstrap_report),
        "runtime_json": str(paths.runtime_json),
        "repo_commit": report.data.get("repo_commit"),
        "lock_origin": args.lock_origin,
        "versions": report.data["versions"],
        "artifacts": [{"id": a["id"], "version": a["version"], "status": a["status"],
                       "algorithm": a["algorithm"], "observed": a["observed"]} for a in report.data["artifacts"]],
        "connector": {k: report.data["connector"][k] for k in ("status", "coordinate", "closure_sha256")}
        if report.data.get("connector") else None,
        "steps": {k: {"status": v["status"], "seconds": v["seconds"]} for k, v in report.data["steps"].items()},
        "warnings": report.data["warnings"],
        "ran_as_root": report.data["ran_as_root"],
        "tunnel": report.data.get("tunnel"),
    }
    if args.emit_pin_patch:
        payload["pin_patch"] = pin_patch
    return Result(payload)


def main(argv: list[str] | None = None) -> int:
    return run_main(lambda _a: "bootstrap", build_parser(), prepare, argv)


__all__ = ["main", "prepare", "canonical_env"]


if __name__ == "__main__":  # python -m transitpulse.runtime.bootstrap ... (same as scripts/prepare_runtime.py)
    sys.exit(main())
