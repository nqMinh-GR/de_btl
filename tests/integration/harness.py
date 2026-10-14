"""Integration-test layer over the shared verification harness (:mod:`transitpulse.verify`).

``StackHarness`` (processor lifecycle through the Supervisor, EnvelopeProducer publishing, barriers on
``query_progress.jsonl`` / pointer / ``run_metadata``, fault arm files, owned-infra control, API calls) and
``FixtureRun`` (timeline.json runs applied step by step, compared with expected.json through the API and Cassandra)
live in ``src/transitpulse/verify`` so that the benchmark harness uses exactly the same code. This module adds the
pytest-facing helpers: assertion with evidence, outbox/raw/DLQ readers and component cleanup.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pytest

from transitpulse.contracts.topics import DLQ, SNAPSHOT_TOPICS
from transitpulse.runtime.paths import WorkPaths
from transitpulse.verify.fixture import StaticScope, classify_golden_steps
from transitpulse.verify.flows import FixtureRun, StackContext, StepOutcome
from transitpulse.verify.golden import cassandra_check_step, check_step, summarize
from transitpulse.verify.stack import (
    ApiServer,
    Delivery,
    HarnessError,
    ProcessorExited,
    StackHarness,
    free_port,
    new_session_id,
    tail_file,
    wait_until,
)

__all__ = [
    "ApiServer", "Delivery", "FixtureRun", "HarnessError", "IntegrationHarness", "ProcessorExited", "StackContext",
    "StaticScope", "StepOutcome", "assert_outcome", "cassandra_check_step", "check_step", "classify_golden_steps",
    "dlq_records", "free_port", "new_session_id", "outbox_rows", "raw_capture_shas", "scope_stop_ids", "summarize",
    "tail_file", "wait_for_outbox", "wait_until", "api_json", "snapshot_topic_highs", "oracle_check",
    "golden_static", "create_fault_run", "start_collector", "capture_file_count", "sleep_until",
]

FAULT_EXIT_CODE = 86          # processing.faults.FAULT_EXIT_CODE (D-PUB-06)
SESSION_OWNED = ("publisher", "collector", "processor")


class IntegrationHarness(StackHarness):
    """StackHarness that also stops every session component it started (never a demo session's)."""

    def cleanup_session_components(self) -> list[str]:
        stopped = []
        for comp in SESSION_OWNED:
            entry = self.sup.entry(comp) or {}
            if entry.get("session_id") == self.session_id and self.component_alive(comp):
                try:
                    self.stop_component(comp)
                    stopped.append(comp)
                except Exception:  # noqa: BLE001 - teardown continues with the next component
                    continue
        self.processor_run_id = None
        return stopped


def assert_outcome(outcome: StepOutcome, case_record: Any, *, label: str | None = None,
                   require_expectation: bool = True) -> None:
    """Record expected/actual of a step and fail with a readable diff when API or Cassandra disagree."""
    name = label or outcome.step
    if outcome.expected is None:
        if require_expectation:
            pytest.fail(f"expected.json has no entry for step {name}; the case cannot be verified")
        case_record.actual(**{name: {"note": "step applied, no expectation for it in expected.json"}})
        return
    case_record.expected(**{name: outcome.expected.get("expect", outcome.expected)})
    api = outcome.api or {}
    cass = outcome.cassandra or {}
    lin = outcome.lineage or {}
    case_record.actual(**{name: {
        "api_passed": api.get("passed"), "cassandra_passed": cass.get("passed"), "lineage_passed": lin.get("passed"),
        "api_failed_checks": [c for c in api.get("checks", []) if not c.get("passed")],
        "cassandra_failed_checks": [c for c in cass.get("checks", []) if not c.get("passed")],
        "lineage_checks": lin.get("checks", []),
        "api_checks": len(api.get("checks", [])), "cassandra_checks": len(cass.get("checks", [])),
        "unsupported": api.get("unsupported", []) + cass.get("unsupported", []),
        "skipped": api.get("skipped", []) + cass.get("skipped", []) + lin.get("skipped", []),
    }})
    problems = []
    if not api.get("passed"):
        problems.append("API: " + summarize(api))
    if cass and not cass.get("passed"):
        problems.append("Cassandra: " + summarize(cass))
    if lin and not lin.get("passed"):
        problems.append("lineage: " + summarize(lin))
    if problems:
        pytest.fail(f"step {name}: " + " | ".join(problems))


def oracle_check(golden_dir: Path, run_key: str, expected: Mapping[str, Any], case_record: Any) -> None:
    """Third leg of the three-way check: oracle(fixture) == expected.json for this run (stdlib-only oracle)."""
    from oracle.scenario import run_scenario
    from transitpulse.verify.golden import diff_paths, expected_steps

    result = run_scenario(golden_dir, run_key)
    want = {s["step"]: s for s in expected_steps(expected, run_key)}
    got = {s["step"]: s for s in result.get("steps", [])}
    diffs = diff_paths({k: v.get("expect") for k, v in want.items()}, {k: v.get("expect") for k, v in got.items()},
                       f"oracle[{run_key}]")
    case_record.actual(oracle_equals_expected=not diffs, oracle_diffs=diffs[:40])
    if diffs:
        pytest.fail(f"oracle(fixture) != expected.json for run {run_key}: " + "; ".join(diffs[:10]))


def api_json(h: StackHarness, path: str, **params: Any) -> tuple[int, dict[str, Any]]:
    r = h.api(path, **params)
    try:
        body = r.json()
    except ValueError:
        body = {"raw": r.text[:500]}
    return r.status_code, body if isinstance(body, dict) else {"raw": body}


def outbox_rows(paths: WorkPaths, run_id: str, *, feed: str | None = None, kind: str | None = None
                ) -> list[dict[str, Any]]:
    """Rows of the collector's SQLite outbox (SELECT only; WAL readers never block the writer).

    The connection is opened normally (not ``mode=ro``) because a read-only WAL reader needs the ``-shm`` file,
    which the collector removes on a clean checkpoint; nothing is written.
    """
    db = paths.outbox_db(run_id)
    if not db.is_file():
        return []
    con = sqlite3.connect(str(db), timeout=10)
    try:
        con.row_factory = sqlite3.Row
        cols = {r[1] for r in con.execute("PRAGMA table_info(outbox)")}
        where, args = [], []
        if feed is not None and "feed_type" in cols:
            where.append("feed_type = ?")
            args.append(feed)
        if kind is not None and "kind" in cols:
            where.append("kind = ?")
            args.append(kind)
        sql = "SELECT * FROM outbox" + (" WHERE " + " AND ".join(where) if where else "")
        rows = [dict(r) for r in con.execute(sql, args)]
    finally:
        con.close()
    for r in rows:
        r.pop("payload", None)
    return rows


def wait_for_outbox(paths: WorkPaths, run_id: str, pred: Any, desc: str, *, timeout: float = 120.0,
                    feed: str | None = None) -> list[dict[str, Any]]:
    def check() -> tuple[bool, Any]:
        try:
            rows = outbox_rows(paths, run_id, feed=feed)
        except sqlite3.Error as exc:
            return False, f"sqlite: {exc}"
        return bool(pred(rows)), [(r.get("feed_type"), r.get("kind"), r.get("state"), r.get("reason"))
                                  for r in rows][-12:]

    wait_until(check, timeout, desc)
    return outbox_rows(paths, run_id, feed=feed)


def raw_capture_shas(paths: WorkPaths, run_id: str, feed: str) -> dict[str, Path]:
    """sha256 of the decoded body -> file, for every archived capture of a run/feed (error bodies excluded)."""
    out: dict[str, Path] = {}
    base = paths.raw_run_dir(run_id) / feed
    if not base.is_dir():
        return out
    for p in base.rglob("*.json.gz"):
        if "errors" in p.parts or ".tmp-" in p.name:
            continue
        try:
            out[hashlib.sha256(gzip.decompress(p.read_bytes())).hexdigest()] = p
        except (OSError, EOFError, gzip.BadGzipFile):
            continue
    return out


def snapshot_topic_highs(h: StackHarness, topics: Iterable[str] = SNAPSHOT_TOPICS) -> dict[tuple[str, int], int]:
    return h.high_watermarks(topics)


def dlq_records(h: StackHarness, run_id: str, start: Mapping[tuple[str, int], int], *, timeout: float = 60.0
                ) -> list[dict[str, Any]]:
    """DLQ records of ``run_id`` produced after the ``start`` watermarks (key ``run_id|feed``)."""
    out: list[dict[str, Any]] = []
    for part in h.partitions(DLQ):
        begin = start.get((DLQ, part), 0)
        end = h.watermarks(DLQ, part)[1]
        for m in h.consume_range(DLQ, part, begin, end, timeout=timeout, key_prefix=f"{run_id}|".encode()):
            try:
                rec = json.loads(m["value"])
            except ValueError:
                continue
            if isinstance(rec, dict):
                rec["_offset"] = m["offset"]
                out.append(rec)
    return out


def scope_stop_ids(static: StaticScope, route_ids: Iterable[str]) -> list[str]:
    """Stops of the scope routes from the static ``scope_index.json`` (no Spark)."""
    with open(Path(static.scope_dir) / "scope_index.json", encoding="utf-8") as fh:
        index = json.load(fh)
    by_route = index.get("stops_by_route") or {}
    stops: list[str] = []
    for r in route_ids:
        for s in by_route.get(r, []):
            if s not in stops:
                stops.append(s)
    return stops


def golden_static(stack_ctx: StackContext) -> StaticScope:
    """Static scope of the golden fixture (materialized recording ZIP, scope R_A/R_B), loaded once per session."""
    rec = stack_ctx.materialize("golden")
    routes = tuple(stack_ctx.timeline.get("scope_route_ids") or rec.route_ids)
    return stack_ctx.static_for(rec.static_zip_path, routes)


def create_fault_run(harness: StackHarness, stack_ctx: StackContext, case: str) -> Any:
    """Synthetic run fed by the fake source: ``scenario_id=simulated_source_fault:<case>`` (D-E28)."""
    from transitpulse.contracts.envelope import utcnow

    return harness.create_run(static=golden_static(stack_ctx), source_type="synthetic", initial_clock=utcnow(),
                              scenario_id=f"simulated_source_fault:{case}", fixture_kind="simulated_source_fault",
                              label=f"{harness.session_id}:{case}")


def start_collector(harness: StackHarness, run: Any, fake: Any, *, extra_env: Mapping[str, str] | None = None,
                    duration_seconds: int = 1800) -> dict[str, Any]:
    """The real collector (``python -m transitpulse.ingestion collect``) pointed at the fake source."""
    env = dict(harness.env)
    env.update(fake.collector_env())
    env.update(extra_env or {})
    argv = [str(harness.runtime.core_python), "-m", "transitpulse.ingestion", "collect", "--workdir",
            str(harness.paths.root), "--run-id", run.run_id, "--profile", harness.profile,
            "--duration-seconds", str(duration_seconds)]
    outbox = harness.paths.outbox_db(run.run_id)
    return harness.start_component("collector", argv, run_id=run.run_id, env=env, ready=outbox.exists,
                                   ready_timeout=120)


def capture_file_count(paths: WorkPaths, run_id: str, feed: str) -> int:
    base = paths.raw_run_dir(run_id) / feed
    if not base.is_dir():
        return 0
    return sum(1 for p in base.rglob("*.json.gz") if "errors" not in p.parts and ".tmp-" not in p.name)


def sleep_until(deadline_epoch: float) -> None:
    delay = deadline_epoch - time.time()
    if delay > 0:
        time.sleep(delay)
