"""CLI ``python -m transitpulse.static {load,list-routes,inspect}`` (interfaces §3).

Exactly ONE JSON document is printed on stdout; structured logs/progress go to stderr.
Exit codes: 0 ok, 2 usage, 7 artifact/checksum (bad or missing ZIP), 1 other failure (FAILED validation,
download failure, Cassandra write failure).
"""

from __future__ import annotations

import argparse
import json
import logging
import shlex
import sys
import traceback
from collections.abc import Sequence
from pathlib import Path
from typing import Any

__all__ = ["main"]


class _ArgError(Exception):
    pass


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # type: ignore[override]
        raise _ArgError(f"{self.prog}: {message}")


def _build_parser() -> argparse.ArgumentParser:
    p = _Parser(prog="python -m transitpulse.static", description="TransitPulse static GTFS loader")
    sub = p.add_subparsers(dest="command", required=True, parser_class=_Parser)

    load = sub.add_parser("load", help="ingest a GTFS ZIP, validate, scope-filter, publish dims + Cassandra")
    load.add_argument("--workdir", required=True)
    load.add_argument("--profile", default=None)
    src = load.add_mutually_exclusive_group(required=True)
    src.add_argument("--zip", dest="zip_path", default=None)
    src.add_argument("--download", action="store_true")
    load.add_argument("--route-ids", default=None, help="comma-separated verified bus route ids "
                      "(default: scope.route_ids / TP_ROUTE_IDS)")
    load.add_argument("--no-cassandra", action="store_true", help="skip the Cassandra static catalog")
    load.add_argument("--rewrite-cassandra", action="store_true",
                      help="rewrite the Cassandra catalog even when the report says WRITTEN")

    lr = sub.add_parser("list-routes", help="bus routes of a GTFS ZIP with trip counts")
    lr.add_argument("--workdir", required=True)
    lr.add_argument("--profile", default=None)
    src2 = lr.add_mutually_exclusive_group(required=True)
    src2.add_argument("--zip", dest="zip_path", default=None)
    src2.add_argument("--download", action="store_true")
    lr.add_argument("--check-realtime", action="store_true",
                    help="fetch VehiclePositions once and count vehicles per bus route")

    ins = sub.add_parser("inspect", help="files, row counts, feed_version and date range of a GTFS ZIP")
    ins.add_argument("--zip", dest="zip_path", required=True)
    return p


def _route_logs_to_stderr() -> None:
    from transitpulse.obs.logging import configure

    configure("static")
    for handler in logging.getLogger().handlers:
        if getattr(handler, "_transitpulse", False) and isinstance(handler, logging.StreamHandler):
            handler.setStream(sys.stderr)


def _emit(doc: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(doc, ensure_ascii=False, default=str, sort_keys=False) + "\n")
    sys.stdout.flush()


def _error(code: str, message: str, hint: str | None = None, log_excerpt: str | None = None) -> dict[str, Any]:
    return {"code": code, "message": message, "hint": hint, "log_excerpt": log_excerpt}


def _ingest(cfg: Any, paths: Any, args: argparse.Namespace) -> Any:
    from transitpulse.static.ingest import ingest_zip

    return ingest_zip(
        paths,
        Path(args.zip_path) if args.zip_path else None,
        download_url=cfg.sources.gtfs_static_url if args.download else None,
        user_agent=cfg.sources.user_agent,
        connect_timeout=cfg.collector.http.connect_timeout_seconds,
        read_timeout=max(120.0, cfg.collector.http.read_timeout_seconds),
    )


def _setup(args: argparse.Namespace) -> tuple[Any, Any]:
    from transitpulse.config import load_config
    from transitpulse.runtime.paths import WorkPaths

    cfg = load_config(args.profile, workdir=str(Path(args.workdir).expanduser().resolve()))
    paths = WorkPaths.from_workdir(cfg.workdir)
    for d in (paths.static_root, paths.tmp, paths.reports, paths.downloads / ".partial"):
        d.mkdir(parents=True, exist_ok=True)
    return cfg, paths


def _cmd_load(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from transitpulse.static.ingest import UsageError
    from transitpulse.static.loader import load_static, normalize_route_ids
    from transitpulse.static.validation import STATUS_FAILED

    cfg, paths = _setup(args)
    route_ids = normalize_route_ids(args.route_ids) or tuple(cfg.scope.route_ids)
    if not route_ids:
        source = f"--zip {shlex.quote(str(args.zip_path))}" if args.zip_path else "--download"
        raise UsageError(
            "USAGE", "no route ids: pass --route-ids A,B or set TP_ROUTE_IDS",
            f"run `python -m transitpulse.static list-routes --workdir {shlex.quote(str(paths.root))} {source} "
            "--check-realtime` to pick verified bus routes",
        )
    version = _ingest(cfg, paths, args)
    res = load_static(cfg, paths, version.gtfs_version, route_ids, write_cassandra=not args.no_cassandra,
                      rewrite_cassandra=args.rewrite_cassandra)
    failed = res.status == STATUS_FAILED
    doc: dict[str, Any] = {
        "ok": not failed,
        "action": "static-load",
        "gtfs_version": res.gtfs_version,
        "scope_id": res.scope_id,
        "status": res.status,
        "agency_timezone": res.agency_timezone,
        "verified_route_ids": list(res.verified_route_ids),
        "rejected_route_ids": list(res.rejected_route_ids),
        "rejected": [dict(r) for r in res.rejected],
        "static_zip_path": str(version.zip_path),
        "feed_version": version.feed_version,
        "scope_dir": None if failed else str(res.scope_dir),
        "report_path": str(res.report_path),
        "reused": res.reused,
        "row_counts": res.row_counts,
        "cassandra": res.cassandra,
        "error": None,
    }
    if failed:
        doc["error"] = _error("STATIC_VALIDATION_FAILED", "static validation FAILED; nothing was published",
                              f"see {res.report_path}", _failed_checks_excerpt(res.report_path))
        return doc, 1
    return doc, 0


def _failed_checks_excerpt(report_path: Path) -> str | None:
    try:
        with open(report_path, encoding="utf-8") as fh:
            report = json.load(fh)
    except (OSError, ValueError):
        return None
    errors = [f"{c['code']}@{c['table']} x{c['count']} {c['sample_keys'][:3]}"
              for c in report.get("checks", []) if c.get("severity") == "error"]
    return "; ".join(errors)[:512] or None


def _cmd_list_routes(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from datetime import UTC, datetime

    from transitpulse.contracts.envelope import iso_utc
    from transitpulse.static.routes import bus_routes_from_zip, fetch_realtime_vehicle_counts

    cfg, paths = _setup(args)
    version = _ingest(cfg, paths, args)
    routes = bus_routes_from_zip(version.zip_path, cfg.scope.route_type)
    counts: dict[str, int] | None = None
    meta: dict[str, Any] | None = None
    if args.check_realtime:
        counts, meta = fetch_realtime_vehicle_counts(cfg)
    for r in routes:
        r["realtime_vehicle_count"] = counts.get(r["route_id"], 0) if counts is not None else None
    return {
        "ok": True,
        "action": "static-list-routes",
        "gtfs_version": version.gtfs_version,
        "feed_version": version.feed_version,
        "static_zip_path": str(version.zip_path),
        "route_type": cfg.scope.route_type,
        "bus_routes": routes,
        "realtime_check": meta,
        "checked_at": iso_utc(datetime.now(UTC)),
        "error": None,
    }, 0


def _cmd_inspect(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from transitpulse.static.ingest import inspect_zip

    summary = inspect_zip(Path(args.zip_path))
    missing_required = [f["name"] for f in summary["files"] if f["required"] and not f["present"]]
    return {"ok": True, "action": "static-inspect", **summary, "required_files_missing": missing_required,
            "error": None}, 0


_COMMANDS = {"load": _cmd_load, "list-routes": _cmd_list_routes, "inspect": _cmd_inspect}
_ACTIONS = {"load": "static-load", "list-routes": "static-list-routes", "inspect": "static-inspect"}


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI; returns the process exit code."""
    parser = _build_parser()
    try:
        args = parser.parse_args(list(argv) if argv is not None else None)
    except _ArgError as exc:
        _emit({"ok": False, "action": "static", "error": _error("USAGE", str(exc), parser.format_usage().strip())})
        return 2
    action = _ACTIONS.get(args.command, "static")
    _route_logs_to_stderr()
    try:
        from transitpulse.static.ingest import StaticError

        doc, code = _COMMANDS[args.command](args)
    except StaticError as exc:
        _emit({"ok": False, "action": action, "error": _error(exc.code, exc.message, exc.hint)})
        return exc.exit_code
    except ValueError as exc:
        _emit({"ok": False, "action": action, "error": _error("USAGE", str(exc), None)})
        return 2
    except Exception as exc:  # noqa: BLE001 - CLI boundary: always one JSON document
        tb = traceback.format_exc()
        _emit({"ok": False, "action": action,
               "error": _error("INTERNAL_ERROR", f"{type(exc).__name__}: {exc}"[:1000], None, tb[-512:])})
        return 1
    _emit(doc)
    return code


if __name__ == "__main__":
    sys.exit(main())
