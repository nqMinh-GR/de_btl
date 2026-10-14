"""``demo.py golden-smoke``: the golden fixture through the REAL native stack, step by step.

Raw/fixture input -> publisher (``--step-mode``) -> Kafka -> Spark processor -> Cassandra -> FastAPI, then
``transitpulse.verify.golden.check_step`` (WP9) compares the API with ``tests/fixtures/golden/expected.json``
after each barrier. Uses its own run and session, the single processor slot (refuses while a demo processor
runs) and stops its processor and publisher at the end. Report: ``W/reports/golden_smoke/<run_id>.json``.
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from transitpulse.runtime.cli_common import (
    EXIT_CONFLICT,
    EXIT_FAILURE,
    EXIT_PREFLIGHT,
    EXIT_READINESS,
    CliError,
    Ctx,
    Result,
)
from transitpulse.runtime.paths import resolve_under
from transitpulse.runtime.session import (
    extract_end_offsets,
    last_query_progress,
    new_session_id,
    offsets_caught_up,
    write_session,
)
from transitpulse.runtime.util import atomic_write_json, iso, iso_now, parse_iso, read_json_or_none, sha256_file

log = logging.getLogger("transitpulse.runtime.golden")

STEP_TIMEOUT = 240.0
GOLDEN_DEADLINE_GRACE = 120.0


def compute_label_snapshots(ctx: Ctx, recording_dir: Path, manifest: dict[str, Any], labels: dict[str, Any],
                            *, run_id: str, scope_id: str, scope_dir: Path) -> tuple[dict[str, Any], list[str]]:
    """``{label: {capture_id, raw_sha256, feed_type, snapshot_id}}`` for this run (contracts.ids, WP2 parse/scope)."""
    from transitpulse.contracts.ids import payload_sha, snapshot_id
    from transitpulse.ingestion.feed_parse import ParsedFeed, parse_feed
    from transitpulse.ingestion.scope_filter import ScopeFilter
    from transitpulse.static.index import load_scope_index

    warnings: list[str] = []
    by_capture = {it.get("capture_id"): it for it in manifest.get("items", []) if it.get("kind") == "capture"}
    scope = ScopeFilter(load_scope_index(scope_dir))
    out: dict[str, Any] = {}
    for label, info in labels.items():
        entry = dict(info) if isinstance(info, dict) else {"capture_id": info}
        item = by_capture.get(entry.get("capture_id"))
        if item is None:
            warnings.append(f"label {label}: capture {entry.get('capture_id')} not in the recording manifest")
            out[label] = entry
            continue
        entry["feed_type"] = item.get("feed_type")
        try:
            raw_file = resolve_under(recording_dir, str(item["raw_path"]))
            data = raw_file.read_bytes()
            body = gzip.decompress(data) if raw_file.name.endswith(".gz") else data
            if item.get("raw_sha256") and hashlib.sha256(body).hexdigest() != item["raw_sha256"]:
                raise ValueError("raw_sha256 mismatch")
            parsed = parse_feed(body, parse_iso(str(item["at"])))
            if not isinstance(parsed, ParsedFeed):
                entry["snapshot_id"] = None
                entry["parse_rejection"] = getattr(parsed, "reason_code", "REJECTED")
            else:
                selected, _acct = scope.select(str(item["feed_type"]), parsed.entities)
                entry["snapshot_id"] = snapshot_id(run_id, scope_id, str(item["feed_type"]), parsed.feed_timestamp,
                                                   payload_sha(parsed.header, selected))
        except Exception as exc:  # noqa: BLE001 - labels without snapshot ids are still usable by check_step
            warnings.append(f"label {label}: snapshot id not computed ({type(exc).__name__}: {exc})")
            entry.setdefault("snapshot_id", None)
        out[label] = entry
    return out, warnings


def _read_pointer_ids(cfg: Any, run_id: str) -> dict[str, str | None]:
    from transitpulse.storage.cassandra import get_session
    from transitpulse.storage.pointer import read_pointer

    session = get_session(cfg)
    out: dict[str, str | None] = {}
    for feed in ("vehicle_positions", "trip_updates", "alerts"):
        row = read_pointer(session, run_id, feed)
        out[feed] = (row or {}).get("snapshot_id") if row else None
    return out


def wait_processed(ctx: Ctx, run_id: str, *, timeout: float) -> bool:
    """Barrier: the processor has consumed every offset that existed when the wait started.

    The watermark is read once. The publisher keeps writing control heartbeats while it sits at a
    barrier, so re-reading the high watermark each poll makes the target move forever and every golden
    step burns the whole step timeout. Only the topics the streaming query consumes (snapshots + control)
    are compared; the DLQ is written by the pipeline and never read by it.
    """
    from transitpulse.contracts.topics import CONTROL, SNAPSHOT_TOPICS
    from transitpulse.runtime.kafka_native import end_offsets

    topics = {*SNAPSHOT_TOPICS, CONTROL}
    t0 = time.monotonic()
    target: dict[str, dict[int, int]] | None = None
    while time.monotonic() - t0 < timeout:
        if target is None:
            try:
                target = end_offsets(ctx.cfg.kafka.bootstrap_servers, sorted(topics))
            except Exception as exc:  # noqa: BLE001
                log.debug("end offsets unavailable: %s", exc)
                target = None
        progress = last_query_progress(ctx.paths, run_id)
        if target is not None and progress is not None:
            if offsets_caught_up(extract_end_offsets(progress, topics), target):
                return True
        time.sleep(2.0)
    return False


def run_golden_smoke(ctx: Ctx, args: Any) -> Result:
    from transitpulse.runtime import cassandra_native as cn
    from transitpulse.runtime.cli_demo import ensure_api, start_processor, start_publisher, stop_components
    from transitpulse.runtime.cli_infra import require_infra, supervisor_for
    from transitpulse.runtime.inputs import split_routes, static_load, synth_generate

    rt = ctx.require_runtime()
    sup = supervisor_for(ctx)
    require_infra(ctx, sup)
    for comp in ("processor", "publisher", "collector"):
        if sup.is_alive(comp):
            e = sup.entry(comp) or {}
            raise CliError("PROCESSOR_SLOT_BUSY", f"{comp} of session {e.get('session_id')} is running",
                           hint="golden smoke uses the single processor slot: run "
                                f"`scripts/demo.py stop --workdir {ctx.paths.root}` first",
                           exit_code=EXIT_CONFLICT)
    expected_path = ctx.repo_dir / "tests" / "fixtures" / "golden" / "expected.json"
    expected = read_json_or_none(expected_path)
    if not isinstance(expected, dict):
        raise CliError("GOLDEN_EXPECTED_MISSING", f"{expected_path} not found or invalid", exit_code=EXIT_PREFLIGHT)
    steps = (((expected.get("runs") or {}).get("golden") or {}).get("steps")) or []
    if not steps:
        raise CliError("GOLDEN_EXPECTED_MISSING", "expected.json has no runs.golden.steps", exit_code=EXIT_PREFLIGHT)
    from transitpulse.verify.golden import check_step

    t_start = time.monotonic()
    started_at = iso_now()
    rec = synth_generate(ctx, kind="golden", seed=int(ctx.cfg.synthetic.default_seed))
    routes = split_routes(rec.get("route_ids"))
    static = static_load(ctx, route_ids=routes, zip_path=rec["static_zip_path"])
    host_id = cn.current_host_id(ctx.cfg)
    from transitpulse.runs import create_run
    from transitpulse.storage.cassandra import get_session

    session_id = new_session_id("golden")
    run = create_run(
        ctx.cfg, get_session(ctx.cfg), source_type="synthetic", route_ids=split_routes(static["verified_route_ids"]),
        gtfs_version=static["gtfs_version"], scope_id=static["scope_id"], agency_timezone=static["agency_timezone"],
        static_zip_path=static["static_zip_path"], static_scope_dir=static["scope_dir"],
        initial_clock=parse_iso(str(rec["start_clock"])), fixture_kind=rec.get("fixture_kind") or "golden",
        seed=None, recording_id=rec.get("recording_id"), session_id=session_id, speed=1.0, label="golden-smoke",
        provenance={"fixture": "tests/fixtures/golden", "expected_sha256": sha256_file(expected_path)},
    )
    run_id = run.run_id
    session = {"session_id": session_id, "run_id": run_id, "mode": "golden-smoke", "source_type": "synthetic",
               "started_at": started_at, "deadline_at": None, "duration_seconds": None,
               "recording_manifest": rec.get("manifest_path"), "speed": 1.0, "route_ids": routes, "state": "RUNNING",
               "cassandra_host_id": host_id}
    write_session(ctx.paths, session)
    recording_dir = Path(rec["recording_dir"])
    labels_raw = read_json_or_none(recording_dir / "labels.json") or {}
    manifest = read_json_or_none(Path(rec["manifest_path"])) or {}
    labels, label_warnings = compute_label_snapshots(ctx, recording_dir, manifest, labels_raw, run_id=run_id,
                                                     scope_id=static["scope_id"], scope_dir=Path(static["scope_dir"]))
    report: dict[str, Any] = {
        "schema_version": 1, "kind": "golden_smoke", "run_id": run_id, "session_id": session_id,
        "recording_id": rec.get("recording_id"), "expected_path": str(expected_path),
        "expected_sha256": sha256_file(expected_path), "started_at": started_at, "finished_at": None,
        "status": "RUNNING", "repo_commit": rt.repo_commit, "versions": {
            "spark": rt.raw.get("spark", {}).get("version"), "kafka": rt.raw.get("kafka", {}).get("version"),
            "cassandra": rt.raw.get("cassandra", {}).get("version"), "java": rt.raw.get("java", {}).get("version"),
            "python": rt.raw.get("python", {}).get("version")},
        "labels": labels, "warnings": label_warnings, "steps": [], "api_base": ctx.api_base,
        "evidence": {"processor_log": str(ctx.paths.log_dir("processor") / "stdout.log"),
                     "publisher_log": str(ctx.paths.log_dir("publisher") / "stdout.log"),
                     "query_progress": str(ctx.paths.query_progress_log(run_id))},
    }
    report_path = ctx.paths.reports / "golden_smoke" / f"{run_id}.json"
    components: dict[str, Any] = {}
    failure: CliError | None = None
    # Backstop: even if this CLI is SIGKILLed, the runners stop the golden processor/publisher after the budget,
    # so the single processor slot is never held forever.
    deadline = time.time() + float(args.timeout_seconds) + GOLDEN_DEADLINE_GRACE
    session["deadline_at"] = iso(datetime.fromtimestamp(deadline, UTC))
    write_session(ctx.paths, session)
    try:
        components["processor"] = start_processor(ctx, sup, run_id=run_id, session_id=session_id, deadline=deadline,
                                                  fault_injection=False, ready_timeout=float(args.ready_timeout))
        components["api"] = ensure_api(ctx, sup)
        components["publisher"] = start_publisher(ctx, sup, run_id=run_id, session_id=session_id,
                                                  manifest=str(rec["manifest_path"]), speed=1.0, step_mode=True,
                                                  duration_seconds=None, deadline=deadline)
        all_passed = True
        for step in steps:
            name = str(step.get("step"))
            res = _run_step(ctx, sup, run_id, session_id, step, labels, steps, check_step)
            report["steps"].append(res)
            all_passed = all_passed and res["passed"]
            atomic_write_json(report_path, report)
            if time.monotonic() - t_start > float(args.timeout_seconds):
                raise CliError("GOLDEN_TIMEOUT", f"golden smoke exceeded {args.timeout_seconds} s at step {name}",
                               exit_code=EXIT_READINESS)
        report["status"] = "PASS" if all_passed else "FAIL"
    except CliError as exc:
        failure = exc
        report["status"] = "FAIL"
        report["error"] = exc.error_json()
    except Exception as exc:  # noqa: BLE001 - always write the report and stop the processor
        failure = CliError("GOLDEN_ERROR", f"{type(exc).__name__}: {exc}", exit_code=EXIT_FAILURE)
        report["status"] = "FAIL"
        report["error"] = failure.error_json()
    finally:
        if report["status"] == "RUNNING":
            # Only reachable through an interrupt (Ctrl-C / SIGTERM = cli_common.Terminated, not an Exception):
            # never leave a RUNNING report behind a STOPPED session.
            report["status"] = "FAIL"
            report["error"] = {"code": "TERMINATED", "message": "golden smoke interrupted (signal or Ctrl-C) before "
                               "all steps were checked",
                               "hint": f"re-run `scripts/demo.py golden-smoke --workdir {ctx.paths.root}`",
                               "log_excerpt": None}
        stopped = stop_components(ctx, sup, ("publisher", "processor"), session_id=session_id)
        report["stopped"] = stopped
        report["finished_at"] = iso_now()
        report["duration_s"] = round(time.monotonic() - t_start, 1)
        atomic_write_json(report_path, report)
        session["state"] = "STOPPED"
        session["result"] = report["status"]
        write_session(ctx.paths, session)
    summary = [{"step": s["step"], "passed": s["passed"], "failed_checks": [c.get("name") for c in s.get("checks", [])
                                                                            if not c.get("passed")][:10]}
               for s in report["steps"]]
    payload = {"run_id": run_id, "session_id": session_id, "status": report["status"], "report_path": str(report_path),
               "steps": summary, "components": {k: {"state": v.get("state"), "pid": v.get("pid")}
                                                for k, v in components.items()}}
    if failure is not None:
        raise CliError(failure.code, failure.message, hint=failure.hint or f"see {report_path}",
                       exit_code=failure.exit_code, log_excerpt=failure.log_excerpt, details=payload)
    if report["status"] != "PASS":
        return Result(payload, exit_code=EXIT_FAILURE, error={
            "code": "GOLDEN_MISMATCH", "message": "API results differ from expected.json",
            "hint": f"see {report_path}", "log_excerpt": None})
    return Result(payload)


def _run_step(ctx: Ctx, sup: Any, run_id: str, session_id: str, step: dict[str, Any], labels: dict[str, Any],
              steps: list[dict[str, Any]], check_step: Any) -> dict[str, Any]:
    from transitpulse.replay.control import read_publisher_state, send_command

    name = str(step.get("step"))
    order = [str(s.get("step")) for s in steps]
    session_dir = ctx.paths.session_dir(session_id)
    t0 = time.monotonic()
    log.info("golden step %s: waiting for the publisher barrier", name)
    reached = False
    while time.monotonic() - t0 < STEP_TIMEOUT:
        st = read_publisher_state(session_dir) or {}
        state = str(st.get("state") or "")
        barrier = st.get("last_barrier")
        if state == "FAILED":
            raise CliError("PUBLISHER_FAILED", f"publisher failed before step {name}",
                           log_excerpt=sup.log_tail("publisher"), exit_code=EXIT_READINESS)
        if state in ("STEP_WAIT", "FINISHED") and barrier == name:
            reached = True
            break
        if state == "FINISHED" and st.get("items_total") is not None and st.get("items_published") == st.get("items_total"):
            reached = True  # last step without a trailing barrier
            break
        if state == "STEP_WAIT":
            if barrier in order and order.index(barrier) > order.index(name):
                raise CliError("BARRIER_ORDER", f"publisher is at barrier {barrier} after step {name} was expected",
                               exit_code=EXIT_FAILURE)
            before = (barrier, st.get("items_published"), st.get("next_index"))
            send_command(session_dir, "step", {}, timeout=15.0)
            # Never send a second step before the first one visibly moved the publisher.
            moved_by = time.monotonic() + 30.0
            while time.monotonic() < moved_by:
                cur = read_publisher_state(session_dir) or {}
                if (str(cur.get("state")) != "STEP_WAIT"
                        or (cur.get("last_barrier"), cur.get("items_published"), cur.get("next_index")) != before):
                    break
                time.sleep(0.25)
            continue
        if not sup.is_alive("publisher") and state != "FINISHED":
            raise CliError("PUBLISHER_EXITED", f"publisher exited before step {name}",
                           log_excerpt=sup.log_tail("publisher"), exit_code=EXIT_READINESS)
        time.sleep(0.5)
    if not reached:
        raise CliError("STEP_TIMEOUT", f"publisher did not reach barrier {name} within {int(STEP_TIMEOUT)} s",
                       log_excerpt=sup.log_tail("publisher"), exit_code=EXIT_READINESS)
    log.info("golden step %s: barrier reached after %.0fs; waiting for Spark", name, time.monotonic() - t0)
    if not sup.is_alive("processor"):
        raise CliError("PROCESSOR_EXITED", f"processor exited during step {name}",
                       log_excerpt=sup.log_tail("processor"), exit_code=EXIT_READINESS)
    processed = wait_processed(ctx, run_id, timeout=STEP_TIMEOUT)
    expect = step.get("expect") or {}
    pointer_expect = expect.get("pointer") or {}
    pointer_ok = True
    pointers: dict[str, Any] = {}
    deadline = time.monotonic() + 60.0
    while True:
        try:
            pointers = _read_pointer_ids(ctx.cfg, run_id)
        except Exception as exc:  # noqa: BLE001
            pointers = {"error": str(exc)}
        pointer_ok = all(pointers.get(feed) is not None and
                         ((labels.get(lbl) or {}).get("snapshot_id") in (None, pointers.get(feed)))
                         for feed, lbl in pointer_expect.items())
        if pointer_ok or time.monotonic() > deadline:
            break
        time.sleep(2.0)
    result: dict[str, Any] = {"passed": False, "checks": []}
    for attempt in range(3):
        # The whole step (step, clock, inputs, expect) so that the application clock is asserted too.
        result = check_step(ctx.api_base, run_id, step, labels)
        if result.get("passed"):
            break
        time.sleep(3.0 * (attempt + 1))
    seconds = round(time.monotonic() - t0, 1)
    passed = bool(result.get("passed"))
    log.info("golden step %s: %s in %.0fs (processor caught up=%s)", name, "PASS" if passed else "FAIL",
             seconds, processed)
    return {"step": name, "clock": step.get("clock"), "inputs": step.get("inputs"), "passed": passed,
            "checks": result.get("checks", []), "processed_barrier": processed, "pointers": pointers,
            "pointer_wait_ok": pointer_ok, "seconds": seconds, "checked_at": iso_now()}


def golden_report_summary(paths: Any) -> dict[str, Any] | None:
    """Latest golden smoke report (used by export / verify_environment)."""
    d = paths.reports / "golden_smoke"
    if not d.is_dir():
        return None
    files = sorted(d.glob("*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        return None
    data = read_json_or_none(files[-1]) or {}
    return {"path": str(files[-1]), "status": data.get("status"), "run_id": data.get("run_id"),
            "finished_at": data.get("finished_at"), "repo_commit": data.get("repo_commit")}


__all__ = ["run_golden_smoke", "compute_label_snapshots", "golden_report_summary"]
