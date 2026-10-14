"""CLI of the ingestion package (docs/interfaces.md §4).

    python -m transitpulse.ingestion collect --workdir W --run-id R [--profile P] [--duration-seconds N] [--no-publish]
    python -m transitpulse.ingestion record  --workdir W --run-id R [--profile P] [--duration-seconds N]
    python -m transitpulse.ingestion source-check --workdir W [--route-ids A,B] [--offline] [--out PATH]
    python -m transitpulse.ingestion finalize-recording --workdir W --run-id R
    python -m transitpulse.ingestion outbox-status --workdir W --run-id R
    python -m transitpulse.ingestion disk-check --workdir W [--run-id R]

Every command prints exactly ONE JSON document on stdout
(``{"ok": bool, "action": str, ..., "error": {code, message, hint, log_excerpt} | null}``); structured
logs go to stderr. Exit codes: 0 ok, 1 other failure, 2 usage, 3 preflight (missing run/static, disk
budget, unverified sources), 4 conflict (collector already running for the run), 6 unsupported mode.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import traceback
from pathlib import Path
from typing import Any

from transitpulse.config import ProjectConfig, load_config
from transitpulse.ingestion.archive import STATIC_LINK_DIR, directory_bytes
from transitpulse.ingestion.collector import (
    COMPONENT,
    EXIT_CONFLICT,
    EXIT_FAILURE,
    EXIT_OK,
    EXIT_PREFLIGHT,
    EXIT_USAGE,
    CollectorError,
    finalize_recording,
    load_run_record,
    run_collector,
)
from transitpulse.ingestion.disk_budget import existing_ancestor
from transitpulse.ingestion.outbox import Outbox, OutboxLocked
from transitpulse.ingestion.source_check import run_source_check
from transitpulse.obs.logging import configure
from transitpulse.runtime.paths import WorkPaths, safe_component

log = logging.getLogger("transitpulse.ingestion.cli")


class _UsageError(Exception):
    pass


class _Parser(argparse.ArgumentParser):
    """argparse that never writes usage errors/help to stdout (stdout carries exactly one JSON doc)."""

    def error(self, message: str) -> None:  # type: ignore[override]
        raise _UsageError(message)

    def print_help(self, file: Any = None) -> None:
        super().print_help(file or sys.stderr)

    def print_usage(self, file: Any = None) -> None:
        super().print_usage(file or sys.stderr)


def build_parser() -> _Parser:
    p = _Parser(prog="python -m transitpulse.ingestion",
                description="TransitPulse MBTA realtime ingestion (collector, recording, source check)")
    sub = p.add_subparsers(dest="action", required=True, metavar="ACTION")

    def common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--workdir", required=True, help="work directory (W)")
        sp.add_argument("--profile", default=None, help="config profile (default: TP_PROFILE or local-native)")

    for name, help_text in (("collect", "poll MBTA feeds, archive raw, publish envelopes to Kafka"),
                            ("record", "collect without Kafka (same as collect --no-publish)")):
        sp = sub.add_parser(name, help=help_text)
        common(sp)
        sp.add_argument("--run-id", required=True)
        sp.add_argument("--duration-seconds", type=float, default=None, help="stop after N seconds")
        if name == "collect":
            sp.add_argument("--no-publish", action="store_true", help="do not publish to Kafka (record mode)")

    sp = sub.add_parser("source-check", help="probe the four MBTA sources and write source_probe_report.json")
    common(sp)
    sp.add_argument("--route-ids", default=None, help="comma-separated bus route IDs for the scope dry run")
    sp.add_argument("--offline", action="store_true", help="make no request; write not_checked entries")
    sp.add_argument("--out", default=None, help="report path (default W/reports/source_probe_report.json)")

    sp = sub.add_parser("finalize-recording", help="rebuild recording_manifest.json after a crash (complete=false)")
    common(sp)
    sp.add_argument("--run-id", required=True)

    sp = sub.add_parser("outbox-status", help="summarize the run's SQLite outbox")
    common(sp)
    sp.add_argument("--run-id", required=True)

    sp = sub.add_parser("disk-check", help="raw disk usage against the budget")
    common(sp)
    sp.add_argument("--run-id", default=None)
    return p


def _emit(doc: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(doc, ensure_ascii=False, default=str) + "\n")
    sys.stdout.flush()


def _configure_logging(run_id: str | None = None) -> None:
    """JSON logs (obs.logging) redirected to stderr: stdout is reserved for the single JSON result."""
    configure(COMPONENT, run_id=run_id)
    for handler in logging.getLogger().handlers:
        if getattr(handler, "_transitpulse", False) and isinstance(handler, logging.StreamHandler):
            handler.setStream(sys.stderr)


def _load(args: argparse.Namespace) -> tuple[ProjectConfig, WorkPaths]:
    try:
        # A relative --workdir is relative to the caller's current directory (not to the repository).
        cfg = load_config(args.profile, workdir=str(Path(args.workdir).expanduser().resolve()))
        paths = WorkPaths.from_workdir(cfg.workdir)
    except Exception as exc:
        raise CollectorError("CONFIG_INVALID", f"cannot load configuration: {type(exc).__name__}: {exc}"[:800],
                             hint="check --profile, --workdir and the TP_* environment variables",
                             exit_code=EXIT_PREFLIGHT) from exc
    return cfg, paths


def _run_id(args: argparse.Namespace) -> str:
    try:
        return safe_component(args.run_id, "run_id")
    except ValueError as exc:
        raise CollectorError("USAGE", str(exc), exit_code=EXIT_USAGE) from exc


# ------------------------------------------------------------------------------------------------ commands
def cmd_collect(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    run_id = _run_id(args)
    _configure_logging(run_id)
    if args.duration_seconds is not None and args.duration_seconds <= 0:
        raise CollectorError("USAGE", "--duration-seconds must be positive", exit_code=EXIT_USAGE)
    cfg, paths = _load(args)
    publish = args.action == "collect" and not getattr(args, "no_publish", False)
    result = run_collector(cfg, paths, run_id, publish=publish, duration_seconds=args.duration_seconds)
    return result.to_json(args.action), result.exit_code


def cmd_source_check(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    _configure_logging()
    cfg, paths = _load(args)
    route_ids = [r.strip() for r in args.route_ids.split(",") if r.strip()] if args.route_ids else None
    report, path = run_source_check(cfg, paths, route_ids=route_ids, offline=args.offline,
                                    out=Path(args.out).expanduser().resolve() if args.out else None)
    summary = report["summary"]
    ok = bool(summary["all_ok"])
    error = None
    if not ok:
        if args.offline:
            error = {"code": "SOURCES_NOT_CHECKED", "message": "offline mode: no source was contacted",
                     "hint": "run source-check without --offline when Internet is available; use labelled "
                             "synthetic/replay fixtures meanwhile", "log_excerpt": None}
        else:
            error = {"code": "SOURCES_NOT_VERIFIED",
                     "message": f"failed sources: {summary['failed_sources']}",
                     "hint": "see the per-source 'error' entries in the report", "log_excerpt": None}
    doc = {
        "ok": ok,
        "action": "source-check",
        "report_path": str(path),
        "checked_at": report["checked_at"],
        "offline": report["offline"],
        "summary": summary,
        "sources": {name: {"status": e.get("status"), "http_status": e.get("http_status"), "error": e.get("error")}
                    for name, e in report["sources"].items()},
        "error": error,
    }
    return doc, EXIT_OK if ok else EXIT_PREFLIGHT


def cmd_finalize_recording(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    run_id = _run_id(args)
    _configure_logging(run_id)
    cfg, paths = _load(args)
    run = load_run_record(paths, run_id)
    db = paths.outbox_db(run_id)
    lock_path = db.with_name(f"{run_id}.lock")
    outbox: Outbox | None = None
    try:
        if db.is_file():
            outbox = Outbox(db, run_id=run_id, create=False)          # takes the run lock
        elif Outbox.is_locked(lock_path):
            raise OutboxLocked(f"run {run_id} is locked by a running collector")
    except OutboxLocked as exc:
        raise CollectorError("COLLECTOR_RUNNING", str(exc), hint="stop the collector first; it finalizes the "
                             "recording itself on exit", exit_code=EXIT_CONFLICT) from exc
    try:
        path, manifest = finalize_recording(cfg, paths, run, complete=False, include_sidecars=True, outbox=outbox)
    finally:
        if outbox is not None:
            outbox.close()
    return {
        "ok": True,
        "action": "finalize-recording",
        "run_id": run_id,
        "manifest_path": str(path),
        "complete": manifest["complete"],
        "item_count": len(manifest["items"]),
        "manifest_sha256": manifest["manifest_sha256"],
        "feeds": manifest["feeds"],
        "time_range": manifest["time_range"],
        "error": None,
    }, EXIT_OK


def cmd_outbox_status(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    run_id = _run_id(args)
    _configure_logging(run_id)
    _cfg, paths = _load(args)
    db = paths.outbox_db(run_id)
    lock_path = db.with_name(f"{run_id}.lock")
    if not db.is_file():
        return {"ok": False, "action": "outbox-status", "run_id": run_id, "exists": False, "db_path": str(db),
                "error": {"code": "OUTBOX_NOT_FOUND", "message": f"no outbox for run {run_id}",
                          "hint": "the collector has not run for this run_id", "log_excerpt": None}}, EXIT_PREFLIGHT
    outbox = Outbox(db, run_id=run_id, lock=False, create=False)   # read-only usage, never takes the lock
    try:
        status = outbox.status()
    finally:
        outbox.close()
    return {"ok": True, "action": "outbox-status", "run_id": run_id, "exists": True,
            "collector_running": Outbox.is_locked(lock_path), **status, "error": None}, EXIT_OK


def cmd_disk_check(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    run_filter = _run_id(args) if args.run_id else None
    _configure_logging(run_filter)
    cfg, paths = _load(args)
    budget = int(cfg.collector.raw_disk_budget_bytes)
    min_free = int(cfg.collector.min_free_bytes)
    fraction = float(cfg.collector.disk_warning_fraction)
    raw_root = paths.raw_root
    usage = shutil.disk_usage(str(existing_ancestor(raw_root)))
    runs: list[dict[str, Any]] = []
    if raw_root.is_dir():
        for d in sorted(raw_root.iterdir()):
            if not d.is_dir() or (run_filter is not None and d.name != run_filter):
                continue
            used = directory_bytes(d, exclude_top=(STATIC_LINK_DIR,))
            share = used / budget
            state = "exceeded" if used > budget else ("warning" if share >= fraction else "ok")
            runs.append({"run_id": d.name, "raw_bytes": used, "used_fraction": round(share, 6), "state": state})
    free_ok = usage.free >= min_free
    exceeded = [r["run_id"] for r in runs if r["state"] == "exceeded"]
    ok = free_ok and not exceeded
    error = None
    if not ok:
        error = {"code": "DISK_BUDGET_EXCEEDED",
                 "message": f"free={usage.free} (min {min_free}); runs over budget: {exceeded}",
                 "hint": "free disk space or start a new run; raw deletion is disabled by default",
                 "log_excerpt": None}
    return {
        "ok": ok,
        "action": "disk-check",
        "raw_root": str(raw_root),
        "filesystem": {"total_bytes": usage.total, "used_bytes": usage.used, "free_bytes": usage.free},
        "min_free_bytes": min_free,
        "budget_bytes_per_run": budget,
        "warning_fraction": fraction,
        "total_raw_bytes": sum(r["raw_bytes"] for r in runs),
        "runs": runs,
        "status": "ok" if ok else "exceeded",
        "error": error,
    }, EXIT_OK if ok else EXIT_PREFLIGHT


COMMANDS = {
    "collect": cmd_collect,
    "record": cmd_collect,
    "source-check": cmd_source_check,
    "finalize-recording": cmd_finalize_recording,
    "outbox-status": cmd_outbox_status,
    "disk-check": cmd_disk_check,
}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    raw = list(sys.argv[1:] if argv is None else argv)
    guessed_action = next((a for a in raw if a in COMMANDS), "usage")
    try:
        args = parser.parse_args(raw)
    except _UsageError as exc:
        _emit({"ok": False, "action": guessed_action, "error": {
            "code": "USAGE", "message": str(exc), "hint": "python -m transitpulse.ingestion --help",
            "log_excerpt": None}})
        return EXIT_USAGE
    except SystemExit as exc:  # --help printed to stderr
        code = exc.code if isinstance(exc.code, int) else 0
        _emit({"ok": code == 0, "action": "help", "error": None})
        return code
    try:
        doc, code = COMMANDS[args.action](args)
    except CollectorError as exc:
        doc, code = {"ok": False, "action": args.action, "error": exc.to_json()}, exc.exit_code
    except Exception as exc:
        log.exception("%s failed", args.action)
        doc = {"ok": False, "action": args.action, "error": {
            "code": "INTERNAL_ERROR", "message": f"{type(exc).__name__}: {exc}"[:800],
            "hint": "see the JSON logs on stderr", "log_excerpt": traceback.format_exc()[-2000:]}}
        code = EXIT_FAILURE
    _emit(doc)
    return code


if __name__ == "__main__":
    sys.exit(main())
