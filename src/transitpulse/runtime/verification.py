"""Environment verification report (spec §27.3) built ONLY from evidence files that exist.

``scripts/verify_environment.py`` writes ``W/reports/environment_verification.json``::

    {"generated_at", "commit", "commit_reason", "dirty", "versions", "environment", "statuses": {
        "unit_tests": {status, command, scenario, timestamp, commit, versions, evidence: [paths], detail}, ...}}

A status is never PASS/VERIFIED/YES without an evidence file that says so; missing evidence gives NOT_RUN
(or NOT_VERIFIED for the managed Colab Run All, NO for benchmarks).
"""

from __future__ import annotations

import argparse
import os
import platform
import xml.etree.ElementTree as ET
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.util import atomic_write_json, git_info, iso_now, read_json_or_none

STATUS_KEYS = (
    "unit_tests", "native_full_stack_linux", "docker_full_stack_local", "clean_clone", "notebook_schema_validation",
    "managed_colab_run_all", "live_mbta_capture", "replay_actual_recording", "synthetic_golden_smoke",
    "offline_test", "benchmark_measured",
)
LABELS = {
    "unit_tests": "Unit tests", "native_full_stack_linux": "Native full-stack Linux",
    "docker_full_stack_local": "Docker full-stack local", "clean_clone": "Clean clone",
    "notebook_schema_validation": "Notebook schema validation",
    "managed_colab_run_all": "Managed Google Colab Run All", "live_mbta_capture": "Live MBTA capture",
    "replay_actual_recording": "Replay actual recording", "synthetic_golden_smoke": "Synthetic golden smoke",
    "offline_test": "Offline test", "benchmark_measured": "Benchmark measured",
}


def _mtime_iso(path: Path) -> str | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    except OSError:
        return None


def entry(status: str, *, command: str | None = None, scenario: str | None = None, evidence: list[str] | None = None,
          timestamp: str | None = None, commit: str | None = None, detail: Any = None) -> dict[str, Any]:
    return {"status": status, "command": command, "scenario": scenario, "timestamp": timestamp, "commit": commit,
            "evidence": evidence or [], "detail": detail}


def junit_counts(path: Path) -> dict[str, int] | None:
    try:
        root = ET.parse(path).getroot()
    except (OSError, ET.ParseError):
        return None
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    for s in suites:
        for k in totals:
            try:
                totals[k] += int(s.get(k, 0) or 0)
            except ValueError:
                continue
    return totals


def junit_status(path: Path, *, command: str) -> dict[str, Any]:
    counts = junit_counts(path) if path.is_file() else None
    if counts is None:
        return entry("NOT_RUN", command=command, detail="no junit report found" if not path.exists() else "unparseable")
    executed = counts["tests"] - counts["skipped"]
    if executed <= 0:
        status = "NOT_RUN"
    elif counts["failures"] or counts["errors"]:
        status = "FAIL"
    else:
        status = "PASS"
    return entry(status, command=command, evidence=[str(path)], timestamp=_mtime_iso(path), detail=counts)


def junit_cases_matching(path: Path, needle: str) -> dict[str, int] | None:
    """Counts of testcases whose classname/name contains ``needle`` (e.g. 'notebook')."""
    try:
        root = ET.parse(path).getroot()
    except (OSError, ET.ParseError):
        return None
    out = {"tests": 0, "failed": 0, "skipped": 0}
    for case in root.iter("testcase"):
        ident = f"{case.get('classname', '')}::{case.get('name', '')}".lower()
        if needle not in ident:
            continue
        out["tests"] += 1
        if case.find("failure") is not None or case.find("error") is not None:
            out["failed"] += 1
        elif case.find("skipped") is not None:
            out["skipped"] += 1
    return out


def _cases(report: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(report, Mapping):
        return []
    cases = report.get("cases")
    return [c for c in cases if isinstance(c, dict)] if isinstance(cases, list) else []


def integration_status(report_path: Path) -> dict[str, Any]:
    report = read_json_or_none(report_path)
    cases = _cases(report)
    if not cases:
        return entry("NOT_RUN", command="pytest tests/integration -m integration",
                     detail="no integration test_report.json with cases")
    statuses = [str(c.get("status")) for c in cases]
    executed = [s for s in statuses if s in ("PASS", "FAIL")]
    if not executed:
        status = "NOT_RUN"
    elif "FAIL" in executed:
        status = "FAIL"
    elif len(executed) == len(statuses):
        status = "PASS"
    else:
        # Some cases passed, others were not run: never reported as a full PASS.
        status = "PARTIAL"
    return entry(status, command=str((report or {}).get("command") or "pytest tests/integration"),
                 evidence=[str(report_path)], timestamp=(report or {}).get("generated_at"),
                 commit=(report or {}).get("commit"),
                 detail={"cases": len(statuses), "pass": statuses.count("PASS"), "fail": statuses.count("FAIL"),
                         "not_run": statuses.count("NOT_RUN")})


def case_status(report_path: Path, case_id: str, *, command: str) -> dict[str, Any]:
    report = read_json_or_none(report_path)
    for c in _cases(report):
        if c.get("id") == case_id:
            st = str(c.get("status"))
            return entry(st if st in ("PASS", "FAIL") else "NOT_RUN", command=command, scenario=case_id,
                         evidence=[str(report_path), *[str(e) for e in c.get("evidence", []) or []]],
                         timestamp=c.get("finished_at"), commit=(report or {}).get("commit"),
                         detail={"reason": c.get("reason")})
    return entry("NOT_RUN", command=command, scenario=case_id, detail=f"{case_id} not present in a test report")


def golden_status(paths: WorkPaths) -> dict[str, Any]:
    d = paths.reports / "golden_smoke"
    files = sorted(d.glob("*.json"), key=lambda p: p.stat().st_mtime) if d.is_dir() else []
    if not files:
        return entry("NOT_RUN", command="scripts/demo.py golden-smoke")
    data = read_json_or_none(files[-1]) or {}
    st = str(data.get("status"))
    return entry(st if st in ("PASS", "FAIL") else "NOT_RUN", command="scripts/demo.py golden-smoke",
                 scenario="golden-v1", evidence=[str(files[-1])], timestamp=data.get("finished_at"),
                 commit=data.get("repo_commit"), detail={"run_id": data.get("run_id"), "steps": [
                     {"step": s.get("step"), "passed": s.get("passed")} for s in data.get("steps", [])]})


def benchmark_status(paths: WorkPaths) -> dict[str, Any]:
    d = paths.reports / "benchmarks"
    measured: list[str] = []
    if d.is_dir():
        for f in sorted(d.glob("*/benchmark_results.json")):
            data = read_json_or_none(f)
            rows = data.get("rows") if isinstance(data, dict) else data
            if isinstance(rows, list) and any(isinstance(r, dict) and r.get("status") in ("MEASURED", "PARTIAL")
                                              for r in rows):
                measured.append(str(f))
    if measured:
        return entry("YES", command="scripts/demo.py benchmark", evidence=measured, timestamp=_mtime_iso(Path(measured[-1])))
    return entry("NO", command="scripts/demo.py benchmark", detail="no benchmark_results.json with measured rows")


def live_capture_status(paths: WorkPaths) -> dict[str, Any]:
    root = paths.raw_root
    verified: list[str] = []
    failed: list[str] = []
    if root.is_dir():
        for man in sorted(root.glob("*/recording_manifest.json")):
            data = read_json_or_none(man) or {}
            if data.get("source_type") != "live":
                continue
            feeds = data.get("feeds") or {}
            captures = sum(int((f or {}).get("capture_count") or 0) for f in feeds.values())
            (verified if captures > 0 else failed).append(str(man))
    sessions_failed = [s for s in _sessions(paths) if s.get("mode") == "live" and s.get("state") == "FAILED"]
    if verified:
        return entry("VERIFIED", command="scripts/demo.py start --mode live", evidence=verified,
                     timestamp=_mtime_iso(Path(verified[-1])))
    if failed or sessions_failed:
        return entry("FAILED", command="scripts/demo.py start --mode live",
                     evidence=failed + [str(paths.session_dir(s["session_id"]) / "session.json") for s in sessions_failed])
    return entry("NOT_RUN", command="scripts/demo.py start --mode live")


def _sessions(paths: WorkPaths) -> list[dict[str, Any]]:
    root = paths.root / "sessions"
    out: list[dict[str, Any]] = []
    if root.is_dir():
        for f in sorted(root.glob("*/session.json")):
            data = read_json_or_none(f)
            if isinstance(data, dict) and data.get("session_id"):
                out.append(data)
    return out


def replay_status(paths: WorkPaths) -> dict[str, Any]:
    ok: list[str] = []
    bad: list[str] = []
    for s in _sessions(paths):
        if s.get("mode") != "replay" or s.get("source_type") != "replay":
            continue
        f = str(paths.session_dir(s["session_id"]) / "session.json")
        if s.get("state") == "FAILED":
            bad.append(f)
        elif s.get("first_ready_generation_at"):
            ok.append(f)
    if bad and not ok:
        return entry("FAIL", command="scripts/demo.py start --mode replay", evidence=bad)
    if ok:
        return entry("PASS", command="scripts/demo.py start --mode replay + wait-ready", evidence=ok,
                     detail="a replay of a real (live) recording served a READY generation through the API")
    return entry("NOT_RUN", command="scripts/demo.py start --mode replay --recording-manifest <bundle>")


def file_status(path: Path, *, command: str, ok_values: tuple[str, ...] = ("PASS",)) -> dict[str, Any] | None:
    data = read_json_or_none(path) if path.is_file() else None
    if not isinstance(data, dict) or "status" not in data:
        return None
    st = str(data["status"])
    return entry(st if st in ("PASS", "FAIL", *ok_values) else "NOT_RUN", command=data.get("command") or command,
                 evidence=[str(path)], timestamp=data.get("timestamp") or _mtime_iso(path), commit=data.get("commit"),
                 detail=data.get("detail"))


def build_report(paths: WorkPaths, repo_dir: Path) -> dict[str, Any]:
    git = git_info(repo_dir)
    runtime = read_json_or_none(paths.runtime_json) or {}
    reports = paths.reports
    test_report = reports / "test_report.json"
    unit_junit = reports / "unit" / "junit.xml"
    st: dict[str, Any] = {}
    st["unit_tests"] = junit_status(unit_junit, command="pytest tests/unit -q --junitxml W/reports/unit/junit.xml")
    st["native_full_stack_linux"] = integration_status(test_report)
    st["docker_full_stack_local"] = file_status(reports / "docker" / "fullstack_status.json",
                                                command="make test-integration (local-docker)") or \
        entry("NOT_RUN", command="docker compose up + make test-integration",
              detail="no Docker full-stack evidence in this work directory")
    st["clean_clone"] = file_status(reports / "clean_clone.json", command="clean clone check") or \
        entry("NOT_RUN", command="fresh clone + bootstrap + golden smoke", detail="no clean-clone evidence")
    nb = junit_cases_matching(unit_junit, "notebook") if unit_junit.is_file() else None
    if nb and nb["tests"] - nb["skipped"] > 0:
        st["notebook_schema_validation"] = entry("FAIL" if nb["failed"] else "PASS",
                                                 command="pytest tests/unit (notebook validation tests)",
                                                 evidence=[str(unit_junit)], timestamp=_mtime_iso(unit_junit), detail=nb)
    else:
        st["notebook_schema_validation"] = file_status(reports / "notebook_validation.json",
                                                       command="nbformat validation") or \
            entry("NOT_RUN", command="pytest -k notebook", detail="no notebook validation evidence")
    colab = file_status(reports / "colab_run_all.json", command="Colab Runtime > Run all")
    st["managed_colab_run_all"] = colab or entry(
        "NOT_VERIFIED", command="Colab Runtime > Run all",
        detail="no colab_run_all.json evidence; a valid notebook JSON is not a Run All result")
    st["live_mbta_capture"] = live_capture_status(paths)
    st["replay_actual_recording"] = replay_status(paths)
    st["synthetic_golden_smoke"] = golden_status(paths)
    st["offline_test"] = case_status(test_report, "IT16", command="pytest tests/integration -k IT16")
    st["benchmark_measured"] = benchmark_status(paths)
    for value in st.values():
        if value.get("commit") is None:
            value["commit"] = git.get("commit")
    versions = {
        "python_core": (runtime.get("python") or {}).get("version"), "java": (runtime.get("java") or {}).get("version"),
        "kafka": (runtime.get("kafka") or {}).get("version"), "cassandra": (runtime.get("cassandra") or {}).get("version"),
        "spark": (runtime.get("spark") or {}).get("version"), "scala": (runtime.get("spark") or {}).get("scala"),
        "lock_sha256": (runtime.get("python") or {}).get("lock_sha256"),
        "lock_origin": (runtime.get("python") or {}).get("lock_origin"),
    }
    return {
        "schema_version": 1, "generated_at": iso_now(), "workdir": str(paths.root), "repo_dir": str(repo_dir),
        "commit": git.get("commit"), "commit_reason": git.get("reason"), "dirty": git.get("dirty"),
        "versions": versions,
        "environment": {"os": platform.platform(), "machine": platform.machine(), "cpus": os.cpu_count(),
                        "colab": bool(os.environ.get("COLAB_RELEASE_TAG") or os.environ.get("COLAB_GPU") is not None),
                        "profile": runtime.get("profile")},
        "statuses": {k: {"label": LABELS[k], **st[k]} for k in STATUS_KEYS},
        "rule": "statuses are derived only from evidence files that exist; nothing is assumed to pass",
    }


def build_parser() -> Any:
    from transitpulse.runtime.cli_common import JsonArgumentParser, add_common_args

    p = JsonArgumentParser(prog="verify_environment.py",
                           description="Write reports/environment_verification.json from existing evidence only.")
    add_common_args(p)
    p.add_argument("--out", default=None, help="output path (default W/reports/environment_verification.json)")
    return p


def main(argv: list[str] | None = None) -> int:
    from transitpulse.runtime.cli_common import Result, run_main

    return run_main(lambda _a: "verify-environment", build_parser(), lambda a: Result(run(a)), argv)


def run(args: argparse.Namespace) -> dict[str, Any]:
    from transitpulse.config import repo_root

    from transitpulse.runtime.cli_common import resolve_profile

    workdir = args.workdir or os.environ.get("TP_WORKDIR")
    if not workdir:
        from transitpulse.config import load_config

        workdir = str(load_config(resolve_profile(None, args.profile)).workdir)
    paths = WorkPaths.from_workdir(workdir)
    runtime = read_json_or_none(paths.runtime_json) or {}
    repo_dir = Path(runtime["repo_dir"]) if runtime.get("repo_dir") else repo_root()
    report = build_report(paths, repo_dir)
    out = Path(args.out).expanduser().resolve() if args.out else paths.reports / "environment_verification.json"
    atomic_write_json(out, report)
    return {"report_path": str(out), "statuses": {k: v["status"] for k, v in report["statuses"].items()},
            "commit": report["commit"], "commit_reason": report["commit_reason"]}


if __name__ == "__main__":  # python -m transitpulse.runtime.verification ...
    import sys

    sys.exit(main())
