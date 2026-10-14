"""Resource / version / port / configuration preflight (``scripts/doctor.py``).

Every check is ``{id, status: PASS|WARN|FAIL|INFO, observed, threshold, hint}`` (INFO = reported, not
evaluated, e.g. the kernel Python version or Docker which is not required). Exit code 3 when any check
FAILs. Docker is never required by the native profiles. Thresholds are project sizing targets
(design D §9), not guarantees.
"""

from __future__ import annotations

import argparse
import os
import platform
import resource
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from transitpulse.runtime.cli_common import (
    EXIT_OK,
    EXIT_PREFLIGHT,
    JsonArgumentParser,
    Result,
    add_common_args,
    load_ctx,
    run_main,
)
from transitpulse.runtime.proc import GIB, ProcFS, cgroup_memory_limit, cpu_count, disk_info
from transitpulse.runtime.util import atomic_write_json, git_info, iso_now, read_json_or_none, sha256_file

PORTS: dict[int, str] = {9092: "kafka", 9093: "kafka-controller", 9042: "cassandra-cql", 7000: "cassandra-storage",
                         7199: "cassandra-jmx", 8000: "api", 8501: "dashboard"}
MEM_TOTAL_FAIL = 10 * GIB
MEM_AVAILABLE_FAIL = 8 * GIB
DISK_FAIL = 6 * GIB
DISK_WARN = 10 * GIB
MAX_MAP_COUNT_WARN = 1048575
NOFILE_WARN = 100000


def check(cid: str, status: str, observed: Any, threshold: Any = None, hint: str | None = None) -> dict[str, Any]:
    return {"id": cid, "status": status, "observed": observed, "threshold": threshold, "hint": hint}


def evaluate_min(observed: float | int | None, *, fail_below: float | None = None,
                 warn_below: float | None = None) -> str:
    """PASS/WARN/FAIL for "at least" thresholds; an unknown observation is a WARN."""
    if observed is None:
        return "WARN"
    if fail_below is not None and observed < fail_below:
        return "FAIL"
    if warn_below is not None and observed < warn_below:
        return "WARN"
    return "PASS"


def classify_port_status(state: str, component: str | None, workdir: Any = None) -> tuple[str, str | None]:
    """Map supervisor.classify_port states to a doctor status + hint."""
    if state == "FREE":
        return "PASS", None
    if state == "OWNED":
        return "PASS", None
    if state == "OURS_UNREGISTERED":
        return "WARN", ("a process of this work directory holds the port but is not registered: run "
                        f"`scripts/infra.py adopt --workdir {workdir or 'W'}`")
    if state == "UNKNOWN_OWNER":
        return "WARN", "port in use by a process this user cannot inspect"
    return "FAIL", "port in use by a foreign process; free it (this tool never kills foreign processes)"


def summarize(checks: list[Mapping[str, Any]]) -> dict[str, int]:
    out = {"PASS": 0, "WARN": 0, "FAIL": 0, "INFO": 0}
    for c in checks:
        out[c["status"]] = out.get(c["status"], 0) + 1
    return out


def _java_version(java_home: Path) -> str | None:
    java = java_home / "bin" / "java"
    if not java.is_file():
        return None
    try:
        r = subprocess.run([str(java), "-version"], capture_output=True, text=True, timeout=60,
                           stdin=subprocess.DEVNULL, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return ((r.stderr or "") + (r.stdout or "")).strip()


def run_doctor(args: argparse.Namespace) -> Result:
    from transitpulse.runtime.configs import rendered_hash, verify_rendered
    from transitpulse.runtime.supervisor import Supervisor, classify_port

    ctx = load_ctx(args, need_runtime=False)
    paths, cfg = ctx.paths, ctx.cfg
    boot = f"`bash scripts/bootstrap_colab.sh --workdir {paths.root}`"
    proc = ProcFS()
    checks: list[dict[str, Any]] = []

    machine = platform.machine()
    checks.append(check("arch", "PASS" if machine in ("x86_64", "amd64") else "FAIL", machine, "x86_64",
                        None if machine in ("x86_64", "amd64") else "pinned binaries are x86_64 only"))
    checks.append(check("os", "PASS" if platform.system() == "Linux" else "FAIL", platform.system(), "Linux"))
    uid, gid = os.getuid(), os.getgid()
    root = uid == 0 or gid == 0
    if root and not cfg.runtime.allow_root_in_demo_vm:
        checks.append(check("uid_gid", "FAIL", {"uid": uid, "gid": gid}, "non-root, or allow_root_in_demo_vm",
                            "Cassandra 5.0 refuses root; use --profile colab-native on a disposable VM or a normal user"))
    else:
        checks.append(check("uid_gid", "PASS", {"uid": uid, "gid": gid, "cassandra_flag": "-R" if root else None},
                            None, "running as root: Cassandra will be started with -R" if root else None))

    sup = Supervisor(paths, proc=proc)
    st = sup.status() if paths.registry_json.is_file() else {"components": {}}
    owned_running = [n for n, e in st["components"].items() if e.get("alive")]

    cpus = cpu_count()
    checks.append(check("cpu", evaluate_min(cpus, fail_below=cfg.runtime.min_cpus), cpus,
                        f">= {cfg.runtime.min_cpus}", "Spark local[2] needs 2 CPUs"))
    mem = proc.meminfo()
    total = mem.get("MemTotal")
    avail = mem.get("MemAvailable")
    checks.append(check("mem_total", evaluate_min(total, fail_below=MEM_TOTAL_FAIL), total, f">= {MEM_TOTAL_FAIL}",
                        "use a VM with about 12 GiB RAM (standard Colab CPU runtime)"))
    if owned_running:
        rss = sum(int(e.get("rss_bytes") or 0) for e in st["components"].values() if e.get("alive"))
        effective = (avail or 0) + rss
        checks.append(check("mem_available", evaluate_min(effective, warn_below=MEM_AVAILABLE_FAIL),
                            {"mem_available": avail, "owned_rss": rss, "owned_running": owned_running},
                            f">= {MEM_AVAILABLE_FAIL} incl. owned services",
                            "owned services are running; the FAIL threshold applies only to an idle VM"))
    else:
        checks.append(check("mem_available", evaluate_min(avail, fail_below=cfg.runtime.min_mem_available_bytes),
                            avail, f">= {cfg.runtime.min_mem_available_bytes}",
                            "close other notebooks/processes or restart the runtime"))
    limit = cgroup_memory_limit()
    checks.append(check("cgroup_memory_limit",
                        "PASS" if limit is None else evaluate_min(limit, warn_below=MEM_TOTAL_FAIL),
                        limit, f">= {MEM_TOTAL_FAIL} or unlimited"))
    disk = disk_info(paths.root)
    checks.append(check("disk_free", evaluate_min(disk.free_bytes, fail_below=DISK_FAIL, warn_below=DISK_WARN),
                        {"path": disk.path, "free_bytes": disk.free_bytes}, f">= {DISK_FAIL} (WARN < {DISK_WARN})",
                        "free disk space; Kafka/Cassandra/raw data live in the work directory"))
    checks.append(check("inodes_free", evaluate_min(disk.inodes_free, warn_below=100000), disk.inodes_free, ">= 100000"))

    tokens = {n: e.get("owner_token", "") for n, e in st["components"].items() if e.get("alive")}
    for port, name in PORTS.items():
        info = classify_port(port, proc, wtag=sup.wtag, registered_tokens=tokens)
        status, hint = classify_port_status(info["state"], info.get("component"), paths.root)
        checks.append(check(f"port_{port}", status,
                            {"service": name, "state": info["state"], "owner": info.get("component"),
                             "pid": info.get("pid"), "cmdline": info.get("cmdline")}, "free or owned", hint))

    runtime = ctx.runtime
    if runtime is None:
        checks.append(check("runtime_json", "FAIL", None, "present",
                            f"run: bash scripts/bootstrap_colab.sh --workdir {paths.root}"))
    else:
        raw = runtime.raw
        checks.append(check("runtime_json", "PASS", str(paths.runtime_json)))
        hashes = raw.get("hashes", {})
        current: dict[str, str | None] = {"config": cfg.config_hash(), "rendered_conf": rendered_hash(paths)}
        man = ctx.repo_dir / "deploy" / "runtime-manifest.json"
        current["manifest"] = sha256_file(man) if man.is_file() else None
        lock = ctx.repo_dir / "uv.lock"
        current["lock"] = sha256_file(lock) if lock.is_file() else None
        stale = {k: {"bootstrap": hashes.get(k), "now": v} for k, v in current.items() if hashes.get(k) != v}
        checks.append(check("runtime_hashes", "PASS" if not stale else "FAIL", stale or "all match",
                            "manifest/config/lock/rendered conf unchanged since bootstrap",
                            f"stale bootstrap: re-run {boot}" if stale else None))
        edited = verify_rendered(paths)
        checks.append(check("rendered_configs", "PASS" if not edited else "WARN", edited or "unchanged", None,
                            f"W/conf files were edited by hand; re-run {boot} to re-render" if edited else None))
        jv = _java_version(runtime.java_home)
        want = str(raw.get("java", {}).get("version"))
        ok = bool(jv) and f'"{want}"' in jv and "Temurin" in jv
        checks.append(check("java", "PASS" if ok else "FAIL", (jv or "missing").splitlines()[:2],
                            f"{want} Temurin from runtime.json JAVA_HOME", None if ok else f"re-run {boot}"))
        core_v = ".".join(str(x) for x in sys.version_info[:3])
        want_py = raw.get("python", {}).get("version")
        in_venv = Path(sys.prefix).resolve() == (ctx.repo_dir / ".venv").resolve()
        checks.append(check("core_python", "PASS" if core_v == want_py and in_venv else "FAIL",
                            {"version": core_v, "prefix": sys.prefix}, f"{want_py} in {ctx.repo_dir / '.venv'}",
                            None if core_v == want_py and in_venv else "run doctor with <repo>/.venv/bin/python"))
        jars = list(runtime.connector_jars)
        missing = [j for j in jars if not Path(j).is_file()]
        checks.append(check("connector_jars", "PASS" if jars and not missing else "FAIL",
                            {"count": len(jars), "missing": missing}, "all present",
                            None if jars and not missing else f"re-run {boot} (step B7)"))
        origin = raw.get("python", {}).get("lock_origin")
        checks.append(check("lock_origin", "PASS" if origin == "committed" else "WARN", origin, "committed",
                            None if origin == "committed" else
                            "uv.lock was generated by bootstrap: export it with "
                            f"`scripts/export_artifacts.py --workdir {paths.root}` and commit it"))
        tunnel = raw.get("tunnel")
        if not tunnel:
            checks.append(check("cloudflared", "INFO",
                                "tunnel disabled (bootstrap --no-tunnel, or cloudflared failed: see bootstrap_report)",
                                None,
                                f"demo.py expose is unavailable; re-run {boot} without --no-tunnel to enable it"))
        else:
            cf = Path(str(tunnel.get("cloudflared")))
            out = None
            if cf.is_file():
                try:
                    r = subprocess.run([str(cf), "--version"], capture_output=True, text=True, timeout=30,
                                       stdin=subprocess.DEVNULL, check=False, env={"PATH": "/usr/bin:/bin"})
                    out = ((r.stdout or "") + (r.stderr or "")).strip()
                except (OSError, subprocess.TimeoutExpired):
                    out = None
            ok = bool(out) and str(tunnel.get("version")) in str(out)
            checks.append(check("cloudflared", "PASS" if ok else "FAIL",
                                {"path": str(cf), "version_output": (out or "missing").splitlines()[0]},
                                f"cloudflared {tunnel.get('version')}",
                                None if ok else f"re-run {boot} (step B6T), or add --no-tunnel to skip the tunnel"))
        report = read_json_or_none(paths.bootstrap_report) or {}
        checks.append(check("bootstrap_report", "PASS" if report.get("overall") == "PASS" else "FAIL",
                            report.get("overall"), "PASS"))
    git = git_info(ctx.repo_dir)
    checks.append(check("git", "PASS" if git.get("commit") and not git.get("dirty") else "WARN",
                        {"commit": git.get("commit"), "dirty": git.get("dirty"), "reason": git.get("reason")}, None,
                        None if git.get("commit") and not git.get("dirty") else
                        "reports will record the commit as unknown or dirty"))
    if args.kernel_python:
        checks.append(check("kernel_python", "INFO", args.kernel_python, None,
                            "reported separately: the notebook kernel only renders UI; core runs in .venv"))
    mmc = proc.vm_max_map_count()
    checks.append(check("vm_max_map_count", evaluate_min(mmc, warn_below=MAX_MAP_COUNT_WARN), mmc,
                        f">= {MAX_MAP_COUNT_WARN}", "Cassandra only warns; cannot be changed without root sysctl"))
    soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    nofile = None if soft == resource.RLIM_INFINITY else int(soft)
    checks.append(check("ulimit_nofile", "PASS" if nofile is None else evaluate_min(nofile, warn_below=NOFILE_WARN),
                        nofile, f">= {NOFILE_WARN}", "Cassandra recommends a high open-files limit (warning only)"))
    checks.append(check("docker", "INFO", "not checked: Docker is not required by the native profiles", None))
    if args.check_network:
        from transitpulse.runtime.artifacts import head_ok

        for url in ("https://cdn.mbta.com/realtime/VehiclePositions.json", "https://repo1.maven.org/maven2/",
                    "https://github.com/"):
            ok, info = head_ok(url, timeout=10)
            checks.append(check(f"network:{url}", "PASS" if ok else "WARN", info, "reachable",
                                None if ok else "live mode and first-time setup need this host"))

    summary = summarize(checks)
    doc = {"profile": ctx.profile, "workdir": str(paths.root), "checked_at": iso_now(), "checks": checks,
           "summary": summary, "kernel_python": args.kernel_python,
           "core_python": ".".join(str(x) for x in sys.version_info[:3]), "owned_running": owned_running}
    try:
        paths.reports.mkdir(parents=True, exist_ok=True)
        atomic_write_json(paths.reports / "doctor.json", doc)
        doc["report_path"] = str(paths.reports / "doctor.json")
    except OSError:
        doc["report_path"] = None
    exit_code = EXIT_PREFLIGHT if summary["FAIL"] else EXIT_OK
    error = None
    if summary["FAIL"]:
        failed = [c["id"] for c in checks if c["status"] == "FAIL"]
        error = {"code": "PREFLIGHT_FAILED", "message": f"failed checks: {', '.join(failed)}",
                 "hint": "see the hint of each failed check", "log_excerpt": None}
    return Result(doc, exit_code=exit_code, error=error)


def build_parser() -> JsonArgumentParser:
    p = JsonArgumentParser(prog="doctor.py", description="Native runtime preflight (resources, versions, ports, "
                                                         "configuration and lock hashes).")
    add_common_args(p)
    p.add_argument("--kernel-python", default=None, help="notebook kernel Python version (reported separately)")
    p.add_argument("--check-network", action="store_true", help="also probe MBTA / Maven Central / GitHub")
    return p


def main(argv: list[str] | None = None) -> int:
    return run_main(lambda _a: "doctor", build_parser(), run_doctor, argv)


if __name__ == "__main__":  # python -m transitpulse.runtime.doctor ...
    sys.exit(main())
