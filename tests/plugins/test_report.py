"""pytest plugin: ``<TP_WORKDIR>/reports/test_report.json`` (IT01-IT16 + CT01-CT15) and ``ct_report.json``.

Registration: ``tests/integration/conftest.py`` registers it in ``pytest_configure`` under ``tp_test_report``; runs
that do not collect tests/integration (``make test``: tests/unit + tests/runtime) load it with
``-p plugins.test_report`` (``PYTHONPATH=tests``), whose module-level ``pytest_configure`` registers the same object.
A module named ``test_*.py`` would otherwise be collected, hence ``__test__ = False``.

Scope rules (never fabricate integration evidence):
* an IT case is counted ONLY from items whose path is under ``tests/integration/`` (an ``it_case`` mark elsewhere,
  e.g. a Spark test, is ignored for the IT table);
* ``test_report.json`` is written only when at least one ``tests/integration`` item was selected (or an
  integration module failed to collect);
* CT cases are counted from any item marked ``ct_case``; when such items ran, ``ct_report.json`` is (re)written
  merging per-CT entries with the previous file; CT entries in ``test_report.json`` come from this session, else from
  ``ct_report.json`` (with its timestamp), else ``NOT_RUN`` with the reason ``manual/Colab``.

Outcome mapping per test: passed -> PASS; failed or error (setup/call/teardown) -> FAIL; skipped -> NOT_RUN with the
skip reason. Per case: any FAIL -> FAIL, all PASS -> PASS, otherwise NOT_RUN.
"""

from __future__ import annotations

import json
import os
import platform
import re
import shlex
import subprocess
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

__test__ = False

REPORT_VERSION = 2
PLUGIN_NAME = "tp_test_report"
_INTEGRATION_SEGMENT = "/tests/integration/"


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _jsonable(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set, frozenset)):
        return [_jsonable(x) for x in v]
    if isinstance(v, Path):
        return str(v)
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    return str(v)


class CaseRecorder:
    """Per-test evidence collected through the ``case_record`` fixture."""

    def __init__(self, nodeid: str) -> None:
        self.nodeid = nodeid
        self.run_id: str | None = None
        self._expected: dict[str, Any] = {}
        self._actual: dict[str, Any] = {}
        self._evidence: list[str] = []
        self._notes: list[str] = []

    def expected(self, value: Any = None, **items: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            self._expected.update(value)
        elif value is not None:
            self._expected["value"] = value
        self._expected.update(items)
        return self._expected

    def actual(self, value: Any = None, **items: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            self._actual.update(value)
        elif value is not None:
            self._actual["value"] = value
        self._actual.update(items)
        return self._actual

    def evidence(self, path: str | os.PathLike[str]) -> None:
        p = str(path)
        if p not in self._evidence:
            self._evidence.append(p)

    def note(self, text: str) -> None:
        self._notes.append(text)

    def as_dict(self) -> dict[str, Any]:
        return {"run_id": self.run_id, "expected": _jsonable(self._expected), "actual": _jsonable(self._actual),
                "evidence": list(self._evidence), "notes": list(self._notes)}


@dataclass
class _TestOutcome:
    nodeid: str
    integration: bool
    it_case: str | None
    ct_cases: tuple[str, ...] = ()
    phases: dict[str, str] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)
    start: float | None = None
    stop: float | None = None
    duration: float = 0.0

    @property
    def status(self) -> str:
        outcomes = self.phases.values()
        if "failed" in outcomes:
            return "FAIL"
        if "skipped" in outcomes:
            return "NOT_RUN"
        if self.phases.get("call") == "passed":
            return "PASS"
        return "NOT_RUN"


def _aggregate(tests: list[_TestOutcome]) -> str:
    statuses = [t.status for t in tests]
    if "FAIL" in statuses:
        return "FAIL"
    if statuses and all(s == "PASS" for s in statuses):
        return "PASS"
    return "NOT_RUN"


def _skip_reason(report: pytest.TestReport) -> str:
    lr = report.longrepr
    if isinstance(lr, tuple) and len(lr) == 3:
        text = str(lr[2])
    else:
        text = str(lr)
    return re.sub(r"^Skipped:\s*", "", text).strip()


def _failure_reason(report: pytest.TestReport) -> str:
    text = report.longreprtext or str(report.longrepr)
    lines = [ln for ln in text.splitlines() if ln.strip()]
    errors = [ln for ln in lines if ln.lstrip().startswith("E ")]
    head = "\n".join(errors[-25:]) if errors else "\n".join(lines[-25:])
    return f"{report.when}: {head}"[-3000:]


def _git(repo: Path, *args: str) -> str | None:
    try:
        proc = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, timeout=10, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return proc.stdout.strip() if proc.returncode == 0 else None


def _meminfo_gib() -> float | None:
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return round(int(line.split()[1]) * 1024 / 2**30, 2)
    except (OSError, ValueError, IndexError):
        return None
    return None


def _cpu_model() -> str | None:
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace").splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        return None
    return platform.processor() or None


def _version_from_path(text: str | None, pattern: str) -> str | None:
    if not text:
        return None
    m = re.search(pattern, text)
    return m.group(1) if m else None


def resolve_workdir() -> Path:
    env = os.environ.get("TP_WORKDIR")
    if env:
        return Path(env).expanduser().resolve()
    try:
        from transitpulse.config import load_config

        return load_config().workdir
    except Exception:  # noqa: BLE001 - report must still be written somewhere predictable
        return Path(__file__).resolve().parents[2] / ".runtime"


def environment(workdir: Path) -> dict[str, Any]:
    env: dict[str, Any] = {"os": platform.platform(), "cpu": {"model": _cpu_model(), "count": os.cpu_count()},
                           "ram_gib": _meminfo_gib(), "pytest_python": platform.python_version()}
    rt_path = workdir / "runtime" / "runtime.json"
    try:
        raw = json.loads(rt_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        env.update(profile=os.environ.get("TP_PROFILE"), runtime_json=None,
                   runtime_reason=f"{rt_path}: {type(exc).__name__}: {exc}"[:300])
        return env
    env.update(
        profile=raw.get("profile"),
        runtime_json=str(rt_path),
        python=(raw.get("python") or {}).get("version"),
        java=" ".join(str(x) for x in ((raw.get("java") or {}).get("vendor"), (raw.get("java") or {}).get("version"))
                      if x) or None,
        kafka=(raw.get("kafka") or {}).get("version")
        or _version_from_path((raw.get("kafka") or {}).get("home"), r"kafka_[\d.]+-(\d+\.\d+\.\d+)"),
        cassandra=(raw.get("cassandra") or {}).get("version")
        or _version_from_path((raw.get("cassandra") or {}).get("home"), r"cassandra-(\d+\.\d+\.\d+)"),
        spark=(raw.get("spark") or {}).get("version"),
        lock_origin=(raw.get("python") or {}).get("lock_origin"),
        runtime_repo_commit=raw.get("repo_commit"),
    )
    return env


def _under_integration(path_or_nodeid: str) -> bool:
    text = "/" + str(path_or_nodeid).replace(os.sep, "/").lstrip("/")
    return _INTEGRATION_SEGMENT in text


def _write_json(path: Path, doc: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    tmp.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


class ITReportPlugin:
    """Collects outcomes and writes ``test_report.json`` / ``ct_report.json`` at session end."""

    def __init__(self, config: pytest.Config, cases: dict[str, Any], additional: dict[str, str] | None = None,
                 ct_cases: dict[str, Any] | None = None) -> None:
        self.config = config
        self.cases = cases
        self.ct_cases = dict(ct_cases or {})
        self.additional = dict(additional or {})
        self.results: dict[str, _TestOutcome] = {}
        self.records: dict[str, CaseRecorder] = {}
        self.integration_selected: list[str] = []
        self.ct_selected: list[str] = []
        self.deselected = 0
        self.collection_errors: list[dict[str, str]] = []
        self.started = time.time()

    # ---- fixture support
    def record_for(self, nodeid: str) -> CaseRecorder:
        if nodeid not in self.records:
            self.records[nodeid] = CaseRecorder(nodeid)
        return self.records[nodeid]

    @staticmethod
    def _is_integration(item: pytest.Item) -> bool:
        path = getattr(item, "path", None)
        return _under_integration(str(path) if path is not None else item.nodeid)

    # ---- hooks
    def pytest_collectreport(self, report: pytest.CollectReport) -> None:
        if report.failed and _under_integration(report.nodeid):
            self.collection_errors.append({"nodeid": report.nodeid, "error": str(report.longrepr)[-3000:]})

    def pytest_deselected(self, items: list[pytest.Item]) -> None:
        self.deselected += len(items)

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        for item in session.items:
            integration = self._is_integration(item)
            marker = item.get_closest_marker("it_case") if integration else None
            case = str(marker.args[0]) if marker and marker.args else None
            cts = tuple(sorted({str(m.args[0]) for m in item.iter_markers("ct_case") if m.args}))
            if not integration and not cts:
                continue
            self.results[item.nodeid] = _TestOutcome(item.nodeid, integration, case, cts)
            if integration:
                self.integration_selected.append(item.nodeid)
            if cts:
                self.ct_selected.append(item.nodeid)

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        res = self.results.get(report.nodeid)
        if res is None:
            return
        res.phases[report.when] = report.outcome
        res.duration += float(report.duration or 0.0)
        start, stop = getattr(report, "start", None), getattr(report, "stop", None)
        if start is not None and (res.start is None or start < res.start):
            res.start = start
        if stop is not None and (res.stop is None or stop > res.stop):
            res.stop = stop
        if report.outcome == "skipped":
            res.reasons.append(_skip_reason(report))
        elif report.outcome == "failed":
            res.reasons.append(_failure_reason(report))

    @pytest.hookimpl(trylast=True)
    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        tr = session.config.pluginmanager.get_plugin("terminalreporter")
        workdir = resolve_workdir()
        written = []
        try:
            if self.ct_selected:
                ct_path = workdir / "reports" / "ct_report.json"
                _write_json(ct_path, self.build_ct_report(ct_path))
                written.append(str(ct_path))
            if self.integration_selected or self.collection_errors:
                doc = self.build(workdir, exitstatus)
                out = workdir / "reports" / "test_report.json"
                _write_json(out, doc)
                stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
                _write_json(out.parent / "test_reports" / f"test_report_{stamp}.json", doc)
                counts = doc["suite"]["case_status"]
                written.append(f"{out} PASS={counts['PASS']} FAIL={counts['FAIL']} NOT_RUN={counts['NOT_RUN']}")
        except OSError as exc:
            if tr is not None:
                tr.write_line(f"[test_report] could not write reports under {workdir}: {exc}", red=True)
            return
        if tr is not None:
            for line in written:
                tr.write_line(f"[test_report] {line}")

    # ---- entries
    def _evidence_fields(self, tests: list[_TestOutcome]) -> dict[str, Any]:
        recorded = [(t.nodeid, self.records[t.nodeid].as_dict()) for t in tests if t.nodeid in self.records]
        records = [r for _, r in recorded]
        reasons = [f"{t.nodeid}: {r}" for t in tests for r in t.reasons]
        starts = [t.start for t in tests if t.start is not None]
        stops = [t.stop for t in tests if t.stop is not None]
        return {
            "nodeid": tests[0].nodeid if len(tests) == 1 else [t.nodeid for t in tests],
            "sub_results": [{"nodeid": t.nodeid, "status": t.status, "phases": t.phases} for t in tests],
            "run_id": next((r["run_id"] for r in records if r["run_id"]), None),
            "run_ids": sorted({r["run_id"] for r in records if r["run_id"]}),
            "started_at": _iso(min(starts)) if starts else None,
            "finished_at": _iso(max(stops)) if stops else None,
            "duration_s": round(sum(t.duration for t in tests), 3),
            "expected": {nid: r["expected"] for nid, r in recorded} or None,
            "actual": {nid: r["actual"] for nid, r in recorded} or None,
            "reason": "\n".join(reasons)[-6000:] if reasons else None,
            "evidence": sorted({e for r in records for e in r["evidence"]}),
            "notes": [n for r in records for n in r["notes"]],
        }

    def _case_entry(self, case_id: str, meta: Any) -> dict[str, Any]:
        module = getattr(meta, "module", None)
        base = {"id": case_id, "title": getattr(meta, "title", str(meta)), "proves": getattr(meta, "proves", None),
                "module": module}
        tests = [r for r in self.results.values() if r.integration and r.it_case == case_id]
        errors = [e for e in self.collection_errors if module and module in e["nodeid"]]
        empty = {"nodeid": None, "run_id": None, "started_at": None, "finished_at": None, "duration_s": None,
                 "expected": None, "actual": None, "evidence": []}
        if not tests:
            if errors:
                return {**base, **empty, "status": "FAIL",
                        "reason": f"collection error in {module}: {errors[0]['error'][-1500:]}"}
            return {**base, **empty, "status": "NOT_RUN",
                    "reason": "not collected or not selected in this pytest session (e.g. -m/-k filter)"}
        return {**base, "status": _aggregate(tests), **self._evidence_fields(tests)}

    def _ct_session_entry(self, ct_id: str, meta: Any) -> dict[str, Any] | None:
        tests = [r for r in self.results.values() if ct_id in r.ct_cases]
        if not tests:
            return None
        return {"id": ct_id, "title": getattr(meta, "title", str(meta)), "expected": getattr(meta, "expected", None),
                "status": _aggregate(tests), "source": "this pytest session", "recorded_at": _iso(time.time()),
                "command": self._command(), "tests": [{"nodeid": t.nodeid, "status": t.status} for t in tests],
                "reason": "\n".join(f"{t.nodeid}: {r}" for t in tests for r in t.reasons)[-3000:] or None}

    def _ct_entries(self, previous: dict[str, Any]) -> list[dict[str, Any]]:
        out = []
        for ct_id, meta in sorted(self.ct_cases.items()):
            entry = self._ct_session_entry(ct_id, meta)
            if entry is None and isinstance(previous.get(ct_id), dict):
                entry = dict(previous[ct_id])
                entry["source"] = f"ct_report.json (recorded_at {entry.get('recorded_at')})"
            if entry is None:
                entry = {"id": ct_id, "title": getattr(meta, "title", str(meta)),
                         "expected": getattr(meta, "expected", None), "status": "NOT_RUN", "source": None,
                         "recorded_at": None, "tests": [], "reason": "manual/Colab"}
            out.append(entry)
        return out

    @staticmethod
    def _previous_ct(path: Path) -> dict[str, Any]:
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        rows = doc.get("ct_cases") if isinstance(doc, dict) else None
        return {r["id"]: r for r in rows or [] if isinstance(r, dict) and r.get("id")}

    def _command(self) -> str:
        return "python -m pytest " + shlex.join(list(self.config.invocation_params.args))

    def build_ct_report(self, path: Path) -> dict[str, Any]:
        entries = self._ct_entries(self._previous_ct(path))
        return {"report_version": REPORT_VERSION, "generated_at": _iso(time.time()),
                "ct_cases": entries,
                "ct_status": {s: sum(1 for e in entries if e["status"] == s) for s in ("PASS", "FAIL", "NOT_RUN")}}

    def build(self, workdir: Path, exitstatus: int) -> dict[str, Any]:
        from_root = Path(str(self.config.rootpath))
        commit = _git(from_root, "rev-parse", "HEAD")
        dirty_text = _git(from_root, "status", "--porcelain", "--untracked-files=no") if commit else None
        cases = [self._case_entry(cid, meta) for cid, meta in sorted(self.cases.items())]
        status_counts = {s: sum(1 for c in cases if c["status"] == s) for s in ("PASS", "FAIL", "NOT_RUN")}
        integration = [r for r in self.results.values() if r.integration]
        test_counts = {"passed": 0, "failed": 0, "skipped": 0}
        for r in integration:
            test_counts[{"PASS": "passed", "FAIL": "failed", "NOT_RUN": "skipped"}[r.status]] += 1
        additional = []
        for r in integration:
            if r.it_case is None:
                rec = self.records.get(r.nodeid)
                additional.append({"nodeid": r.nodeid, "status": r.status,
                                   "title": next((v for k, v in self.additional.items() if k in r.nodeid), None),
                                   "reason": "\n".join(r.reasons)[-3000:] or None,
                                   "duration_s": round(r.duration, 3), **(rec.as_dict() if rec else {})})
        ct_entries = self._ct_entries(self._previous_ct(workdir / "reports" / "ct_report.json"))
        return {
            "report_version": REPORT_VERSION,
            "generated_at": _iso(time.time()),
            "session_started_at": _iso(self.started),
            "command": self._command(),
            "commit": commit,
            "dirty": (bool(dirty_text) if dirty_text is not None else None) if commit else None,
            "workdir": str(workdir),
            "environment": environment(workdir),
            "pytest_exitstatus": int(exitstatus),
            "suite": {"selected_integration_tests": len(self.integration_selected), "deselected": self.deselected,
                      "collection_errors": len(self.collection_errors), **test_counts,
                      "case_status": status_counts,
                      "ct_status": {s: sum(1 for e in ct_entries if e["status"] == s)
                                    for s in ("PASS", "FAIL", "NOT_RUN")}},
            "cases": cases,
            "ct_cases": ct_entries,
            "additional_tests": additional,
            "collection_errors": self.collection_errors,
            "profiles": {
                "native": "this report (Kafka/Cassandra/Spark started by the native supervisor)",
                "local-docker": {"status": "NOT_RUN", "reason": "Docker Compose profile is not exercised by this "
                                 "test suite; the build environment had no Docker daemon access"},
            },
            "method_notes": [
                "IT cases count only tests under tests/integration/; it_case marks elsewhere are ignored here.",
                "Three-way check: API == expected.json, Cassandra == expected.json and oracle(fixture) == "
                "expected.json are asserted by the IT01-IT09 tests.",
                "NOT_RUN is never converted to PASS; skipped tests keep their skip reason; CT entries without an "
                "executed test are NOT_RUN 'manual/Colab'.",
            ],
        }


def pytest_configure(config: pytest.Config) -> None:
    """Self-registration when loaded with ``-p plugins.test_report`` (runs that skip tests/integration)."""
    if config.pluginmanager.get_plugin(PLUGIN_NAME) is None:
        from integration.cases import ADDITIONAL_TESTS, CT_CASES, IT_CASES

        config.pluginmanager.register(ITReportPlugin(config, IT_CASES, ADDITIONAL_TESTS, CT_CASES), PLUGIN_NAME)
