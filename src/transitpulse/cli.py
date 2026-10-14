"""``transitpulse`` console entry point: one command name for every project CLI.

Each subcommand execs the real entry point with the current interpreter (so signals reach the long-running process
directly and its single JSON document is the only stdout):

==================  =================================================================================
source-check        python -m transitpulse.ingestion source-check
list-routes         python -m transitpulse.static list-routes
static-load         python -m transitpulse.static load
collect / record    python -m transitpulse.ingestion collect|record
replay              python -m transitpulse.replay <verify|publish|control|synth-generate> ...
synth-generate      python -m transitpulse.replay synth-generate
process             python -m transitpulse.processing.stream_job
api                 uvicorn transitpulse.api.app:create_app --factory --workers 1
report              python -m transitpulse.processing.report_job
lineage             python -m transitpulse.obs.lineage
benchmark           benchmarks/run_benchmark.py
doctor / infra /    scripts/doctor.py, scripts/infra.py, scripts/demo.py, scripts/export_artifacts.py
demo / export
run-create          create a run from a loaded static scope (used by the local-docker Compose profile)
==================  =================================================================================

``--workdir`` (default ``TP_WORKDIR`` or the profile default) and ``--profile`` (explicit > ``TP_PROFILE`` >
the work directory's runtime.json > ``colab-native``, as in scripts/*.py) are appended to the target's arguments when the target accepts them and the caller did not pass
them already. Everything after the subcommand is passed through unchanged.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

EXIT_USAGE = 2
EXIT_PREFLIGHT = 3


@dataclass(frozen=True)
class Target:
    kind: str                        # module | script | uvicorn | internal
    entry: str                       # module name or repo-relative script path
    prefix: tuple[str, ...] = ()     # fixed leading arguments (e.g. the module's own subcommand)
    workdir: bool = True             # target accepts --workdir
    profile: bool = False            # target accepts --profile
    help: str = ""


TARGETS: dict[str, Target] = {
    "source-check": Target("module", "transitpulse.ingestion", ("source-check",),
                           help="probe the four MBTA sources -> reports/source_probe_report.json"),
    "list-routes": Target("module", "transitpulse.static", ("list-routes",), profile=True,
                          help="list bus routes of a GTFS ZIP (--zip PATH | --download) [--check-realtime]"),
    "static-load": Target("module", "transitpulse.static", ("load",), profile=True,
                          help="load a static scope (--zip PATH | --download) --route-ids A,B"),
    "collect": Target("module", "transitpulse.ingestion", ("collect",), profile=True,
                      help="live collector for a run (--run-id R [--duration-seconds N])"),
    "record": Target("module", "transitpulse.ingestion", ("record",), profile=True,
                     help="collect without Kafka (recording only)"),
    "replay": Target("module", "transitpulse.replay", (), workdir=False,
                     help="replay CLI passthrough: verify|publish|control|synth-generate ..."),
    "synth-generate": Target("module", "transitpulse.replay", ("synth-generate",),
                             help="materialize a synthetic recording (--kind demo|golden|stress|scenario --seed N)"),
    "process": Target("module", "transitpulse.processing.stream_job", (), profile=True,
                      help="Spark structured-streaming processor for a run (--run-id R)"),
    "api": Target("uvicorn", "transitpulse.api.app:create_app", (), workdir=False,
                  help="FastAPI server (one uvicorn worker) on cfg.api.host:cfg.api.port"),
    "report": Target("module", "transitpulse.processing.report_job", (), profile=True,
                     help="FR09 batch report over committed curated batches (--run-id R)"),
    "lineage": Target("module", "transitpulse.obs.lineage", (), profile=True,
                      help="trace one snapshot raw -> Kafka -> batch -> Cassandra -> API (--run-id R)"),
    "benchmark": Target("script", "benchmarks/run_benchmark.py", (), profile=True,
                        help="benchmark harness (--scenario NAME ...)"),
    "doctor": Target("script", "scripts/doctor.py", (), profile=True, help="environment preflight"),
    "infra": Target("script", "scripts/infra.py", (), profile=True,
                    help="native Kafka/Cassandra: start|init|status|logs|stop|adopt"),
    "demo": Target("script", "scripts/demo.py", (), profile=True,
                   help="demo session: prepare|start|wait-ready|status|logs|pause|resume|stop|reset|..."),
    "export": Target("script", "scripts/export_artifacts.py", (), help="evidence ZIP export"),
    "run-create": Target("internal", "run-create", (), profile=True,
                         help="create a run from a loaded static scope (Compose profile helper)"),
}


def _emit(doc: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(doc, default=str) + "\n")
    sys.stdout.flush()


def _error(action: str, code: str, message: str, hint: str | None, exit_code: int) -> int:
    _emit({"ok": False, "action": action, "error": {"code": code, "message": message, "hint": hint,
                                                    "log_excerpt": None}})
    return exit_code


def _has_flag(args: list[str], flag: str) -> bool:
    return any(a == flag or a.startswith(flag + "=") for a in args)


DEFAULT_PROFILE = "colab-native"   # same default as scripts/*.py (runtime/cli_common.DEFAULT_PROFILE)


def _runtime_json_profile(workdir: str | None) -> str | None:
    if not workdir:
        return None
    try:
        raw = json.loads((Path(workdir).expanduser() / "runtime" / "runtime.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    profile = raw.get("profile") if isinstance(raw, dict) else None
    return profile if isinstance(profile, str) and profile else None


def resolve_profile(workdir: str | None, explicit: str | None) -> str:
    """explicit > TP_PROFILE > runtime.json of the work directory > colab-native (same order as the scripts).

    Uses ``transitpulse.runtime.cli_common.resolve_profile`` when it exists so both entry points agree.
    """
    try:
        from transitpulse.runtime.cli_common import resolve_profile as runtime_resolve
    except ImportError:
        runtime_resolve = None
    if runtime_resolve is not None:
        try:
            return str(runtime_resolve(workdir, explicit))
        except TypeError:
            pass
    return explicit or os.environ.get("TP_PROFILE") or _runtime_json_profile(workdir) or DEFAULT_PROFILE


def _resolve(args: argparse.Namespace) -> tuple[str, str]:
    from transitpulse.config import PROFILES, load_config

    workdir = args.workdir or os.environ.get("TP_WORKDIR")
    profile = resolve_profile(workdir, args.profile)
    if profile not in PROFILES:
        raise ValueError(f"unknown profile {profile!r}; expected one of {sorted(PROFILES)}")
    if not workdir:
        workdir = str(load_config(profile).workdir)
    return profile, str(Path(workdir).expanduser().resolve())


def build_argv(name: str, target: Target, rest: list[str], *, workdir: str, profile: str,
               repo_dir: Path, python: str) -> tuple[list[str], dict[str, str]]:
    """argv + extra environment for ``target``; pure function (unit-testable)."""
    extra_env = {"TP_WORKDIR": workdir, "TP_PROFILE": profile}
    if target.kind == "uvicorn":
        from transitpulse.config import load_config

        cfg = load_config(profile, workdir=workdir)
        argv = [python, "-m", "uvicorn", target.entry, "--factory", "--host", cfg.api.host,
                "--port", str(cfg.api.port), "--workers", "1", *rest]
        return argv, extra_env
    if target.kind == "module":
        argv = [python, "-m", target.entry, *target.prefix]
    else:
        argv = [python, str(repo_dir / target.entry), *target.prefix]
    args = list(rest)
    if name == "replay":
        # sub-subcommands of the replay CLI all accept --workdir
        if args and not args[0].startswith("-") and not _has_flag(args, "--workdir"):
            args += ["--workdir", workdir]
    else:
        if target.workdir and not _has_flag(args, "--workdir"):
            args += ["--workdir", workdir]
        if target.profile and not _has_flag(args, "--profile"):
            args += ["--profile", profile]
    return argv + args, extra_env


# --------------------------------------------------------------------------------------------------
# run-create (Compose profile helper; native mode uses scripts/demo.py which also creates runs)
# --------------------------------------------------------------------------------------------------

def _run_create(rest: list[str], *, workdir: str, profile: str) -> int:
    from datetime import UTC, datetime

    ap = argparse.ArgumentParser(prog="transitpulse run-create")
    ap.add_argument("--source-type", required=True, choices=("live", "replay", "synthetic"))
    ap.add_argument("--route-ids", default="")
    ap.add_argument("--gtfs-version", default=None, help="static version (default: the only/most recent loaded)")
    ap.add_argument("--recording-manifest", default=None)
    ap.add_argument("--speed", type=float, default=None)
    ap.add_argument("--initial-clock", default=None)
    ap.add_argument("--session-id", default=None)
    ap.add_argument("--label", default=None)
    ap.add_argument("--workdir", default=None)
    ap.add_argument("--profile", default=None)
    try:
        a = ap.parse_args(rest)
    except SystemExit as exc:
        if exc.code in (0, None):
            return 0
        return _error("run-create", "usage", "invalid arguments", "see transitpulse run-create --help", EXIT_USAGE)
    if a.workdir:
        workdir = str(Path(a.workdir).expanduser().resolve())
    if a.profile:
        profile = a.profile
    from transitpulse.config import load_config
    from transitpulse.contracts.envelope import iso_utc, parse_iso_utc
    from transitpulse.contracts.ids import scope_id as make_scope_id
    from transitpulse.runs import create_run
    from transitpulse.runtime.paths import WorkPaths
    from transitpulse.static.dims import read_scope_manifest, scope_dir_is_complete
    from transitpulse.storage.cassandra import get_session

    cfg = load_config(profile, workdir=workdir)
    paths = WorkPaths.from_workdir(workdir)
    routes = [r.strip() for r in a.route_ids.split(",") if r.strip()]
    recording: dict[str, Any] = {}
    if a.recording_manifest:
        with open(a.recording_manifest, encoding="utf-8") as fh:
            manifest = json.load(fh)
        recording = {"recording_id": manifest.get("recording_id"),
                     "gtfs_version": (manifest.get("static") or {}).get("gtfs_version"),
                     "route_ids": (manifest.get("scope") or {}).get("route_ids") or [],
                     "start": min((str(i["at"]) for i in manifest.get("items", []) if i.get("at")), default=None)}
        routes = routes or list(recording["route_ids"])
    if not routes:
        return _error("run-create", "route_ids_required", "no route ids given",
                      "pass --route-ids with verified bus route ids (make list-routes)", EXIT_USAGE)
    version = a.gtfs_version or recording.get("gtfs_version")
    if not version:
        versions = sorted((p for p in paths.static_root.glob("*") if p.is_dir()), key=lambda p: p.stat().st_mtime)
        if not versions:
            return _error("run-create", "static_missing", f"no static version under {paths.static_root}",
                          "run the static loader first (make static-load ROUTE_IDS=...)", EXIT_PREFLIGHT)
        version = versions[-1].name
    sid = make_scope_id(routes, cfg.scope.route_type, cfg.scope.scope_policy_version, version)
    scope_dir = paths.static_scope_dir(version, sid)
    if not scope_dir_is_complete(scope_dir):
        return _error("run-create", "static_scope_missing", f"static scope {scope_dir} is not loaded",
                      f"load it first: transitpulse static-load --zip <ZIP of {version[:12]}> --route-ids "
                      f"{','.join(routes)}", EXIT_PREFLIGHT)
    scope = read_scope_manifest(scope_dir)
    initial = None
    if a.source_type != "live":
        start = a.initial_clock or recording.get("start")
        if not start:
            return _error("run-create", "initial_clock_required", "replay/synthetic runs need a source clock",
                          "pass --initial-clock or --recording-manifest", EXIT_USAGE)
        initial = parse_iso_utc(start)
    session_id = a.session_id or f"s-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-docker"
    run = create_run(cfg, get_session(cfg), source_type=a.source_type, route_ids=routes, gtfs_version=version,
                     scope_id=sid, agency_timezone=str(scope["agency_timezone"]),
                     static_zip_path=str(paths.static_version_dir(version) / "MBTA_GTFS.zip"),
                     static_scope_dir=str(scope_dir), initial_clock=initial,
                     recording_id=recording.get("recording_id"), session_id=session_id, speed=a.speed,
                     label=a.label, provenance={"created_by": "transitpulse run-create", "profile": profile,
                                                "recording_manifest": a.recording_manifest})
    _emit({"ok": True, "action": "run-create", "run_id": run.run_id, "session_id": session_id,
           "source_type": run.source_type, "clock_mode": run.clock_mode, "route_ids": list(run.route_ids),
           "gtfs_version": version, "scope_id": sid,
           "initial_clock": iso_utc(initial) if initial else None, "error": None})
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(
        prog="transitpulse",
        description="TransitPulse MBTA command dispatcher (each command prints one JSON document on stdout).",
        epilog="\n".join(f"  {k:<15} {t.help}" for k, t in TARGETS.items()),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--workdir", default=None, help="work directory (default: TP_WORKDIR or profile default)")
    parser.add_argument("--profile", default=None, help="local-native | colab-native | local-docker")
    parser.add_argument("command", choices=sorted(TARGETS), help="subcommand (see list below)")
    parser.add_argument("args", nargs=argparse.REMAINDER, help="arguments passed to the subcommand")
    try:
        ns = parser.parse_args(argv)
    except SystemExit as exc:
        if exc.code in (0, None):
            return 0
        return _error("transitpulse", "usage", "invalid arguments (details on stderr)", "transitpulse --help",
                      EXIT_USAGE)
    target = TARGETS[ns.command]
    try:
        profile, workdir = _resolve(ns)
    except (ValueError, RuntimeError) as exc:
        return _error(ns.command, "config", str(exc), "set TP_WORKDIR/TP_PROFILE or pass --workdir/--profile",
                      EXIT_USAGE)
    rest = list(ns.args)
    if target.kind == "internal":
        return _run_create(rest, workdir=workdir, profile=profile)
    from transitpulse.config import repo_root

    repo = repo_root()
    if target.kind == "script" and not (repo / target.entry).is_file():
        return _error(ns.command, "entry_missing", f"{repo / target.entry} not found",
                      "run from a full repository checkout", EXIT_PREFLIGHT)
    full, extra_env = build_argv(ns.command, target, rest, workdir=workdir, profile=profile, repo_dir=repo,
                                 python=sys.executable)
    env = dict(os.environ)
    for k, v in extra_env.items():
        env.setdefault(k, v)
    sys.stdout.flush()
    sys.stderr.flush()
    os.chdir(repo)
    os.execve(full[0], full, env)
    return 1  # pragma: no cover - execve does not return


if __name__ == "__main__":
    raise SystemExit(main())
