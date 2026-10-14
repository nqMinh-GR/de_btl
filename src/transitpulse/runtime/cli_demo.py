"""``scripts/demo.py``: application session lifecycle on the native stack.

    demo.py prepare  --mode synthetic|replay|live [--seed N] [--recording-manifest P] [--static-zip Z]
                     [--route-ids A,B] [--speed X] [--duration-seconds N]
    demo.py start    (same flags) [--detach] [--ready-timeout 300] [--enable-fault-injection]
    demo.py wait-ready [--session-id S] [--timeout-seconds 300]
    demo.py status   [--session-id S]
    demo.py logs     --component processor|publisher|collector|api|kafka|cassandra [--file F] [--lines N]
    demo.py pause|resume|step [--session-id S]
    demo.py advance-clock --to ISO   |   demo.py inject --scenario duplicate_snapshot|late_snapshot|invalid_gps
    demo.py restart-processor [--session-id S]
    demo.py stop     [--all] [--drain-timeout 60] [--no-drain]
    demo.py reset    --targets session|checkpoints|kafka|cassandra|data|all [--include-caches] [--confirm TOKEN]
    demo.py golden-smoke [--timeout-seconds 900]
    demo.py lineage  [--run-id R] [--feed vehicle_positions] [--snapshot-id S]
    demo.py benchmark --scenario S [--max-seconds N] [--recording-manifest P] [--repetitions N]
    demo.py expose   start|status|stop [--target dashboard|api|react|both|all] [--ready-timeout 120]
                     (Streamlit dashboard + Cloudflare quick tunnel; see runtime/access.py)

Start order (spec §24.6): infra must be READY and initialized -> inputs (synth-generate / verify / static load)
-> run record (``transitpulse.runs.create_run``) -> processor (wait ``processor_ready.json``) -> publisher or
collector -> API (``/health/ready`` 200). One active session and one processor at a time; a rerun with the
same parameters returns the existing session with ``reused=true``. ``stop`` drains (publisher first) and
never deletes data; ``reset`` is a separate, confirmed command.
"""

from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import inspect
import json
import logging
import os
import time
from collections.abc import Iterator, Sequence
from datetime import timedelta
from pathlib import Path
from typing import Any

from transitpulse.runtime.cli_common import (
    EXIT_CONFLICT,
    EXIT_FAILURE,
    EXIT_PREFLIGHT,
    EXIT_READINESS,
    EXIT_UNSUPPORTED,
    EXIT_USAGE,
    CliError,
    Ctx,
    JsonArgumentParser,
    Result,
    add_common_args,
    http_get,
    load_ctx,
    run_json_cli,
    run_main,
    script_cmd,
    wcmd,
)
from transitpulse.runtime.cli_infra import entry_view, require_infra, supervisor_for
from transitpulse.runtime.inputs import prepare_inputs, split_routes
from transitpulse.runtime.manifest import subprocess_env
from transitpulse.runtime.proc import GIB, ProcFS, disk_info
from transitpulse.runtime.session import (
    MODES,
    derive_state,
    last_query_progress,
    list_sessions,
    new_session_id,
    params_key,
    progress_batch_id,
    read_log,
    read_session,
    update_session,
    write_session,
)
from transitpulse.runtime.spark_launch import processor_argv, processor_env
from transitpulse.runtime.supervisor import ACCESS_COMPONENTS, Supervisor, spawn_gate_bytes
from transitpulse.runtime.util import atomic_write_json, git_info, iso, iso_now, parse_iso, read_json_or_none, utc_now

log = logging.getLogger("transitpulse.runtime.demo")

MIN_DURATION = 60
MAX_DURATION = 7200
PROCESSOR_DRAIN_GRACE = 90.0
INPUT_DEADLINE_BACKSTOP = 60.0
CONTROL_COMMANDS = ("pause", "resume", "step", "advance-clock", "inject", "finish")
INJECT_SCENARIOS = ("duplicate_snapshot", "late_snapshot", "invalid_gps", "restart_processor")
LOG_COMPONENTS = ("processor", "publisher", "collector", "api", "kafka", "cassandra", "dashboard", "react",
                  "tunnel", "tunnel-api", "tunnel-react", "bootstrap")
FEEDS = ("vehicle_positions", "trip_updates", "alerts")


# --------------------------------------------------------------------------------------------------
# Component helpers (also used by golden_smoke)
# --------------------------------------------------------------------------------------------------

def app_fingerprint(ctx: Ctx) -> str:
    rt = ctx.require_runtime()
    commit = git_info(ctx.repo_dir).get("commit") or rt.repo_commit
    return f"commit={commit};lock={rt.raw.get('hashes', {}).get('lock')};config={ctx.cfg.config_hash()}"


def core_env(ctx: Ctx) -> dict[str, str]:
    return subprocess_env(ctx.require_runtime(), {"TP_WORKDIR": str(ctx.paths.root), "TP_PROFILE": ctx.profile,
                                                  "TP_REPO_DIR": str(ctx.repo_dir), "PYTHONUNBUFFERED": "1"})


def _fresh_since_spawn(sup: Supervisor, component: str, path: Path) -> bool:
    e = sup.entry(component) or {}
    started = float(e.get("started_epoch") or 0.0)
    try:
        return path.stat().st_mtime >= started - 1.0
    except OSError:
        return False


def start_processor(ctx: Ctx, sup: Supervisor, *, run_id: str, session_id: str | None, deadline: float | None,
                    fault_injection: bool, ready_timeout: float) -> dict[str, Any]:
    rt = ctx.require_runtime()
    ready_file = ctx.paths.run_dir(run_id) / "processor_ready.json"

    def ready() -> bool:
        e = sup.entry("processor") or {}
        return e.get("run_id") == run_id and _fresh_since_spawn(sup, "processor", ready_file)

    env = {**processor_env(rt), "TP_WORKDIR": str(ctx.paths.root), "TP_PROFILE": ctx.profile,
           "TP_REPO_DIR": str(ctx.repo_dir)}
    entry = sup.start("processor", processor_argv(rt, ctx.paths, run_id, profile=ctx.profile,
                                                  fault_injection=fault_injection),
                      env=env, scope="session", session_id=session_id, run_id=run_id, ready=ready,
                      ready_timeout=ready_timeout, fingerprint_extra=app_fingerprint(ctx),
                      min_mem_available_bytes=spawn_gate_bytes("processor"),
                      deadline_epoch=(deadline + PROCESSOR_DRAIN_GRACE) if deadline else None,
                      graceful_timeout=90.0)
    return {**entry_view(sup, entry), "ready_file": str(ready_file)}


def publisher_argv(ctx: Any, *, run_id: str, session_id: str, manifest: str, speed: float, step_mode: bool,
                   duration_seconds: int | None) -> list[str]:
    """Deterministic publisher argv (part of the supervisor fingerprint: no wall-clock values in it)."""
    argv = [ctx.python, "-m", "transitpulse.replay", "publish", "--workdir", str(ctx.paths.root), "--run-id", run_id,
            "--manifest", str(manifest), "--session-id", session_id, "--speed", f"{float(speed):g}"]
    if step_mode:
        argv.append("--step-mode")
    if duration_seconds:
        argv += ["--duration-seconds", str(int(duration_seconds))]
    return argv


def start_publisher(ctx: Ctx, sup: Supervisor, *, run_id: str, session_id: str, manifest: str, speed: float,
                    step_mode: bool, duration_seconds: int | None, deadline: float | None) -> dict[str, Any]:
    state_file = ctx.paths.session_dir(session_id) / "publisher_state.json"
    ctx.paths.session_dir(session_id).mkdir(parents=True, exist_ok=True)
    argv = publisher_argv(ctx, run_id=run_id, session_id=session_id, manifest=manifest, speed=speed,
                          step_mode=step_mode, duration_seconds=duration_seconds)

    def ready() -> bool:
        if not _fresh_since_spawn(sup, "publisher", state_file):
            return False
        data = read_json_or_none(state_file) or {}
        return str(data.get("state")) in ("RUNNING", "PAUSED", "STEP_WAIT", "FINISHED")

    entry = sup.start("publisher", argv, env=core_env(ctx), scope="session", session_id=session_id, run_id=run_id,
                      ready=ready, ready_timeout=120.0, fingerprint_extra=app_fingerprint(ctx),
                      min_mem_available_bytes=spawn_gate_bytes("publisher"),
                      deadline_epoch=(deadline + INPUT_DEADLINE_BACKSTOP) if deadline else None, graceful_timeout=30.0)
    return {**entry_view(sup, entry), "state_file": str(state_file)}


def start_collector(ctx: Ctx, sup: Supervisor, *, run_id: str, session_id: str, duration_seconds: int,
                    deadline: float | None) -> dict[str, Any]:
    poll_state = ctx.paths.collector_state_dir(run_id) / "poll_state.json"
    argv = [ctx.python, "-m", "transitpulse.ingestion", "collect", "--workdir", str(ctx.paths.root), "--run-id",
            run_id, "--profile", ctx.profile, "--duration-seconds", str(int(duration_seconds))]
    t_first: dict[str, float] = {}

    def ready() -> bool:
        if _fresh_since_spawn(sup, "collector", poll_state):
            return True
        now = time.monotonic()
        t_first.setdefault("t", now)
        return now - t_first["t"] >= 10.0 and sup.is_alive("collector")

    entry = sup.start("collector", argv, env=core_env(ctx), scope="session", session_id=session_id, run_id=run_id,
                      ready=ready, ready_timeout=120.0, fingerprint_extra=app_fingerprint(ctx),
                      min_mem_available_bytes=spawn_gate_bytes("collector"),
                      deadline_epoch=(deadline + INPUT_DEADLINE_BACKSTOP) if deadline else None, graceful_timeout=30.0)
    return {**entry_view(sup, entry), "poll_state": str(poll_state)}


def ensure_api(ctx: Ctx, sup: Supervisor, *, ready_timeout: float = 120.0) -> dict[str, Any]:
    cfg = ctx.cfg
    argv = [ctx.python, "-m", "uvicorn", "transitpulse.api.app:create_app", "--factory", "--host", cfg.api.host,
            "--port", str(cfg.api.port), "--workers", "1"]

    def ready() -> bool:
        status, _ = http_get(f"{ctx.api_base}/health/ready", timeout=3.0)
        return status == 200

    entry = sup.start("api", argv, env=core_env(ctx), scope="app", ports=(int(cfg.api.port),), ready=ready,
                      ready_timeout=ready_timeout, fingerprint_extra=app_fingerprint(ctx),
                      min_mem_available_bytes=spawn_gate_bytes("api"), graceful_timeout=10.0)
    return {**entry_view(sup, entry), "base_url": ctx.api_base}


def stop_components(ctx: Ctx, sup: Supervisor, components: Sequence[str], *, session_id: str | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for comp in components:
        e = sup.entry(comp)
        if e is None:
            continue
        if session_id is not None and comp != "api" and e.get("session_id") not in (session_id, None):
            out[comp] = {"stopped": False, "reason": f"belongs to session {e.get('session_id')}"}
            continue
        try:
            out[comp] = sup.stop(comp)
        except Exception as exc:  # noqa: BLE001 - keep stopping the others
            out[comp] = {"stopped": False, "error": f"{type(exc).__name__}: {exc}"}
    return out


@contextlib.contextmanager
def start_lock(ctx: Ctx) -> Iterator[None]:
    d = ctx.paths.root / "sessions"
    d.mkdir(parents=True, exist_ok=True)
    fd = os.open(d / ".start.lock", os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EAGAIN, errno.EACCES):
                raise CliError("START_IN_PROGRESS", "another demo start/stop is running for this work directory",
                               hint="wait for it to finish", exit_code=EXIT_CONFLICT) from exc
            raise
        yield
    finally:
        os.close(fd)


# --------------------------------------------------------------------------------------------------
# Sessions
# --------------------------------------------------------------------------------------------------

def input_component(session: dict[str, Any]) -> str:
    return "collector" if session.get("mode") == "live" else "publisher"


def _alive_for(sup: Supervisor, comp: str, session_id: str) -> bool:
    e = sup.entry(comp)
    return bool(e) and e.get("session_id") == session_id and sup.is_alive(comp)


def session_status(ctx: Ctx, sup: Supervisor, session: dict[str, Any]) -> str:
    sid = str(session["session_id"])
    pstate = read_json_or_none(ctx.paths.session_dir(sid) / "publisher_state.json")
    return derive_state(session, processor_alive=_alive_for(sup, "processor", sid),
                        input_alive=_alive_for(sup, input_component(session), sid), publisher_state=pstate)


def resolve_session(ctx: Ctx, sup: Supervisor, session_id: str | None, *, required: bool = True
                    ) -> dict[str, Any] | None:
    sid = session_id or sup.current_session()
    if sid is None:
        sessions = [s for s in list_sessions(ctx.paths) if s.get("mode") != "golden-smoke"]
        sid = sessions[-1]["session_id"] if sessions else None
    session = read_session(ctx.paths, sid)
    if session is None and required:
        raise CliError("NO_SESSION", "no demo session found" + (f" with id {session_id}" if session_id else ""),
                       hint=f"start one with {wcmd(ctx.paths.root, 'scripts/demo.py start --mode synthetic --detach')}",
                       exit_code=EXIT_PREFLIGHT)
    return session


def _validate_start_args(ctx: Ctx, args: argparse.Namespace) -> tuple[list[str], float | None]:
    if args.mode not in MODES:
        raise CliError("UNSUPPORTED_MODE", f"unknown mode {args.mode!r}", exit_code=EXIT_UNSUPPORTED)
    if not (MIN_DURATION <= int(args.duration_seconds) <= MAX_DURATION):
        raise CliError("USAGE", f"--duration-seconds must be within [{MIN_DURATION}, {MAX_DURATION}]",
                       exit_code=EXIT_USAGE)
    speed = args.speed
    if speed is None:
        speed = {"synthetic": 1.0, "replay": 10.0}.get(args.mode)
    if speed is not None and not (0 < float(speed) <= float(ctx.cfg.replay.max_speed)):
        raise CliError("USAGE", f"--speed must be in (0, {ctx.cfg.replay.max_speed}]", exit_code=EXIT_USAGE)
    if args.mode == "live" and args.speed is not None:
        raise CliError("USAGE", "--speed is not used in live mode (wall clock)", exit_code=EXIT_USAGE)
    routes = split_routes(args.route_ids)
    if args.mode == "live" and not routes:
        raise CliError("ROUTES_REQUIRED", "--route-ids is required for --mode live",
                       hint=f"choose verified bus route ids: `{ctx.python} -m transitpulse.static list-routes "
                            f"--workdir {ctx.paths.root} --download --check-realtime`",
                       exit_code=EXIT_USAGE)
    if args.mode == "replay" and not args.recording_manifest:
        raise CliError("RECORDING_REQUIRED", "--recording-manifest is required for --mode replay",
                       hint="pass the recording_manifest.json of an imported input bundle", exit_code=EXIT_USAGE)
    return routes, (float(speed) if speed is not None else None)


def _start_params(args: argparse.Namespace, routes: list[str], speed: float | None) -> dict[str, Any]:
    manifest = str(Path(args.recording_manifest).expanduser().resolve()) if args.recording_manifest else None
    return {"mode": args.mode, "seed": args.seed if args.mode == "synthetic" else None, "recording_manifest": manifest,
            "static_zip": args.static_zip, "speed": speed, "route_ids": sorted(routes),
            "fault_injection": bool(getattr(args, "enable_fault_injection", False))}


def cmd_prepare(ctx: Ctx, args: argparse.Namespace) -> Result:
    from transitpulse.runtime.cassandra_native import current_host_id

    routes, speed = _validate_start_args(ctx, args)
    sup = supervisor_for(ctx)
    require_infra(ctx, sup)
    prepared = prepare_inputs(ctx, mode=args.mode, seed=args.seed, recording_manifest=args.recording_manifest,
                              static_zip=args.static_zip, route_ids=routes, speed=speed,
                              duration_seconds=int(args.duration_seconds), reuse=True,
                              cassandra_host_id=current_host_id(ctx.cfg))
    return Result(_prepared_view(ctx, prepared))


def _prepared_view(ctx: Ctx, p: dict[str, Any]) -> dict[str, Any]:
    return {
        "mode": p["mode"], "source_type": p["source_type"], "reused": p.get("reused", False),
        "scope": {"route_ids": p["route_ids"], "rejected_route_ids": p.get("rejected_route_ids", []),
                  "verified_mbta": p["verified_mbta"], "gtfs_version": p["gtfs_version"], "scope_id": p["scope_id"],
                  "agency_timezone": p.get("agency_timezone")},
        "static": {"zip_path": p.get("static_zip_path"), "scope_dir": p.get("static_scope_dir"),
                   "status": p.get("static_status"), "report_path": p.get("static_report")},
        "recording": {"recording_id": p.get("recording_id"), "manifest": p.get("recording_manifest"),
                      "start_clock": p.get("start_clock"), "fixture_kind": p.get("fixture_kind"),
                      "item_count": p.get("item_count")},
        "input_hashes": p.get("input_hashes"), "provenance": p.get("provenance"), "prepared_at": p.get("prepared_at"),
        "prepared_record": str(ctx.paths.inputs_root / "prepared" / f"{p['mode']}.json"),
    }


def cmd_start(ctx: Ctx, args: argparse.Namespace) -> Result:
    from transitpulse.runtime.cassandra_native import current_host_id

    rt = ctx.require_runtime()
    routes, speed = _validate_start_args(ctx, args)
    sup = supervisor_for(ctx)
    require_infra(ctx, sup)
    params = _start_params(args, routes, speed)
    key = params_key(params)
    with start_lock(ctx):
        current = read_session(ctx.paths, sup.current_session())
        if current is not None:
            cstate = session_status(ctx, sup, current)
            same = current.get("params_key") == key
            if cstate in ("RUNNING", "PAUSED") and same:
                comps = _start_sequence(ctx, sup, current, args, resume=True)
                return _start_result(ctx, current, comps, reused=all(c.get("reused") for c in comps.values()))
            if cstate == "STARTING" and same and current.get("run_id"):
                log.info("resuming the interrupted start of session %s", current["session_id"])
                comps = _start_sequence(ctx, sup, current, args, resume=True)
                current = update_session(ctx.paths, current["session_id"], state="RUNNING", ready_at=iso_now())
                return _start_result(ctx, current, comps, reused=False)
            if cstate in ("RUNNING", "PAUSED", "STARTING"):
                raise CliError("SESSION_ACTIVE",
                               f"session {current['session_id']} (mode {current.get('mode')}) is {cstate} with other "
                               "parameters",
                               hint=f"run {wcmd(ctx.paths.root, 'scripts/demo.py stop')} first (data is kept), then "
                                    "start again",
                               exit_code=EXIT_CONFLICT, details={"session_id": current["session_id"]})
            if cstate == "FAILED":
                proc_exit = (sup.entry("processor") or {}).get("exit")
                raise CliError("SESSION_FAILED",
                               f"session {current['session_id']} has no running processor (exit {proc_exit})",
                               hint=f"recover with {wcmd(ctx.paths.root, 'scripts/demo.py restart-processor')}, or "
                                    f"end it with {wcmd(ctx.paths.root, 'scripts/demo.py stop')} and start a new "
                                    "session",
                               exit_code=EXIT_CONFLICT, details={"session_id": current["session_id"]})
            # STOPPED or BUDGET_EXHAUSTED: close it (stop leftovers of that session) and start a new one.
            stop_components(ctx, sup, ("publisher", "collector", "processor"), session_id=current["session_id"])
            update_session(ctx.paths, current["session_id"], state="STOPPED",
                           stopped_at=current.get("stopped_at") or iso_now(), final_state=cstate)
            sup.set_current_session(None)

        for comp in ("processor", "publisher", "collector"):
            if sup.is_alive(comp):
                e = sup.entry(comp) or {}
                raise CliError("PROCESSOR_SLOT_BUSY",
                               f"{comp} of session {e.get('session_id')} / run {e.get('run_id')} is still running",
                               hint=f"run {wcmd(ctx.paths.root, 'scripts/demo.py stop')} first",
                               exit_code=EXIT_CONFLICT)

        prepared = prepare_inputs(ctx, mode=args.mode, seed=args.seed, recording_manifest=args.recording_manifest,
                                  static_zip=args.static_zip, route_ids=routes, speed=speed,
                                  duration_seconds=int(args.duration_seconds), reuse=True,
                                  cassandra_host_id=current_host_id(ctx.cfg))
        sid = new_session_id()
        started = utc_now()
        deadline = started + timedelta(seconds=int(args.duration_seconds))
        session: dict[str, Any] = {
            "session_id": sid, "run_id": None, "mode": args.mode, "source_type": prepared["source_type"],
            "started_at": iso(started), "deadline_at": iso(deadline), "duration_seconds": int(args.duration_seconds),
            "recording_manifest": prepared.get("recording_manifest"),
            "speed": speed if prepared["source_type"] != "live" else None,
            "route_ids": prepared["route_ids"], "state": "STARTING", "params": params, "params_key": key,
            "scope": {"route_ids": prepared["route_ids"], "verified_mbta": prepared["verified_mbta"],
                      "gtfs_version": prepared["gtfs_version"], "scope_id": prepared["scope_id"]},
            "prepared": prepared, "repo_commit": git_info(ctx.repo_dir).get("commit") or rt.repo_commit,
            "starter_pid": os.getpid(), "profile": ctx.profile,
        }
        write_session(ctx.paths, session)
        sup.set_current_session(sid)
        try:
            run = _create_run(ctx, session, prepared)
            session["run_id"] = run.run_id
            session["checkpoint_dir"] = str(ctx.paths.checkpoint_dir(ctx.cfg.project.pipeline_version, run.run_id,
                                                                      ctx.cfg.spark.query_name))
            write_session(ctx.paths, session)
            comps = _start_sequence(ctx, sup, session, args, resume=False)
        except BaseException as exc:
            log.error("start failed: stopping the components started for session %s", sid)
            stopped = stop_components(ctx, sup, ("publisher", "collector", "processor"), session_id=sid)
            err = exc.error_json() if isinstance(exc, CliError) else {"code": type(exc).__name__, "message": str(exc)}
            update_session(ctx.paths, sid, state="FAILED", failed_at=iso_now(), error=err, cleanup=stopped)
            sup.set_current_session(None)
            raise
        session = update_session(ctx.paths, sid, state="RUNNING", ready_at=iso_now(),
                                 components={k: {"state": v.get("state"), "pid": v.get("pid")} for k, v in comps.items()})
    result = _start_result(ctx, session, comps, reused=False)
    if not args.detach:
        result = _run_foreground(ctx, sup, session, result)
    return result


def _create_run(ctx: Ctx, session: dict[str, Any], prepared: dict[str, Any]) -> Any:
    from transitpulse.runs import create_run
    from transitpulse.storage.cassandra import get_session

    source = prepared["source_type"]
    initial = parse_iso(str(prepared["start_clock"])) if source != "live" and prepared.get("start_clock") else None
    if source != "live" and initial is None:
        raise CliError("START_CLOCK_MISSING", "the recording has no start clock", exit_code=EXIT_PREFLIGHT)
    return create_run(
        ctx.cfg, get_session(ctx.cfg), source_type=source, route_ids=prepared["route_ids"],
        gtfs_version=prepared["gtfs_version"], scope_id=prepared["scope_id"],
        agency_timezone=prepared["agency_timezone"], static_zip_path=prepared["static_zip_path"],
        static_scope_dir=prepared["static_scope_dir"], initial_clock=initial, scenario_id=None,
        fixture_kind=prepared.get("fixture_kind"), seed=prepared.get("seed"), recording_id=prepared.get("recording_id"),
        session_id=session["session_id"], speed=session.get("speed") if source != "live" else None,
        label=f"demo-{session['mode']}",
        provenance={"mode": session["mode"], "recording_manifest": prepared.get("recording_manifest"),
                    "recording_dir": prepared.get("recording_dir"), "input_hashes": prepared.get("input_hashes"),
                    "statement": prepared.get("provenance"), "verified_mbta": prepared.get("verified_mbta")},
    )


def _start_sequence(ctx: Ctx, sup: Supervisor, session: dict[str, Any], args: argparse.Namespace, *,
                    resume: bool) -> dict[str, Any]:
    sid = str(session["session_id"])
    run_id = str(session["run_id"])
    deadline = parse_iso(str(session["deadline_at"])).timestamp()
    params = session.get("params") or {}
    prepared = session.get("prepared") or {}
    comps: dict[str, Any] = {}

    def existing_finished(comp: str) -> dict[str, Any] | None:
        e = sup.entry(comp)
        if resume and e and e.get("session_id") == sid and not sup.is_alive(comp):
            return {**entry_view(sup, e), "reused": True, "exit": e.get("exit")}
        return None

    # Resume of the same session: an alive owned component of THIS session is reused as is (never re-started,
    # never fingerprint-compared), so a second Run All cannot end in CONFLICT.
    reused_proc = reuse_alive(sup, "processor", sid) if resume else None
    comps["processor"] = reused_proc or start_processor(
        ctx, sup, run_id=run_id, session_id=sid, deadline=deadline,
        fault_injection=bool(params.get("fault_injection")), ready_timeout=float(getattr(args, "ready_timeout", 300.0)))
    inp = input_component(session)
    reused_inp = reuse_alive(sup, inp, sid) if resume else None
    done = existing_finished(inp)
    if reused_inp is not None:
        comps[inp] = reused_inp
    elif done is not None:
        comps[inp] = done
    elif inp == "collector":
        comps["collector"] = start_collector(ctx, sup, run_id=run_id, session_id=sid,
                                             duration_seconds=input_duration(ctx, session), deadline=deadline)
    else:
        comps["publisher"] = start_publisher(ctx, sup, run_id=run_id, session_id=sid,
                                             manifest=str(prepared["recording_manifest"]),
                                             speed=float(session.get("speed") or prepared.get("speed") or 1.0),
                                             step_mode=False, duration_seconds=input_duration(ctx, session),
                                             deadline=deadline)
    comps["api"] = ensure_api(ctx, sup)
    return comps


def reuse_alive(sup: Supervisor, component: str, session_id: str) -> dict[str, Any] | None:
    """Entry view (reused=True) of an alive, owned component that belongs to ``session_id``; else None."""
    e = sup.entry(component)
    if e and e.get("session_id") == session_id and sup.is_alive(component):
        return {**entry_view(sup, e), "reused": True}
    return None


def input_duration(ctx: Ctx, session: dict[str, Any]) -> int:
    """``--duration-seconds`` of the publisher/collector, computed ONCE per session (budget remaining when the
    input was first started) and stored in session.json, so the argv and its fingerprint stay identical when
    the start is resumed. The runner deadline (session deadline + backstop) still bounds the process."""
    stored = session.get("input_duration_seconds")
    if stored:
        return int(stored)
    deadline = parse_iso(str(session["deadline_at"])).timestamp()
    value = max(1, int(deadline - time.time()))
    session["input_duration_seconds"] = value
    update_session(ctx.paths, str(session["session_id"]), input_duration_seconds=value)
    return value


def _start_result(ctx: Ctx, session: dict[str, Any], comps: dict[str, Any], *, reused: bool) -> Result:
    sid = session["session_id"]
    scope = session.get("scope") or {}
    payload = {
        "reused": bool(reused), "session_id": sid, "run_id": session.get("run_id"),
        "source_type": session.get("source_type"), "mode": session.get("mode"),
        "scope": {"route_ids": scope.get("route_ids"), "verified_mbta": scope.get("verified_mbta"),
                  "gtfs_version": scope.get("gtfs_version"), "scope_id": scope.get("scope_id")},
        "api": {"base_url": ctx.api_base}, "checkpoint_dir": session.get("checkpoint_dir"),
        "deadline_at": session.get("deadline_at"), "started_at": session.get("started_at"),
        "duration_seconds": session.get("duration_seconds"), "speed": session.get("speed"),
        "recording_manifest": session.get("recording_manifest"),
        "components": {k: {"state": v.get("state"), "pid": v.get("pid"), "wrapper_pid": v.get("wrapper_pid"),
                           "log": v.get("log"), "reused": v.get("reused")} for k, v in comps.items()},
        "repo_commit": session.get("repo_commit"),
        "session_file": str(ctx.paths.session_dir(sid) / "session.json"),
        "commands": {
            "status": script_cmd(ctx, "demo.py", "status", "--session-id", sid),
            "logs": script_cmd(ctx, "demo.py", "logs", "--component", "processor"),
            "stop": script_cmd(ctx, "demo.py", "stop"),
        },
    }
    with contextlib.suppress(OSError):
        atomic_write_json(ctx.paths.processes / "last_start.json", {**payload, "ok": True, "action": "start"})
    return Result(payload)


def _run_foreground(ctx: Ctx, sup: Supervisor, session: dict[str, Any], result: Result) -> Result:
    """Without --detach: run until the duration budget ends, then stop the session (data kept)."""
    deadline = parse_iso(str(session["deadline_at"])).timestamp()
    log.info("running in the foreground until %s (use --detach to return immediately)", session["deadline_at"])
    interrupted: KeyboardInterrupt | None = None
    try:
        while time.time() < deadline + 5:
            state = session_status(ctx, sup, read_session(ctx.paths, session["session_id"]) or session)
            if state in ("FAILED", "STOPPED"):
                break
            log.info("session %s: %s, %d s left", session["session_id"], state, max(0, int(deadline - time.time())))
            time.sleep(min(30.0, max(1.0, deadline + 5 - time.time())))
    except KeyboardInterrupt as exc:  # Ctrl-C or SIGTERM (cli_common.Terminated)
        interrupted = exc
        log.warning("interrupted: stopping the session (data is kept)")
    stopped = stop_session(ctx, sup, read_session(ctx.paths, session["session_id"]) or session, drain_timeout=60.0,
                           include_infra=False, no_drain=False)
    if interrupted is not None:
        # run_main reports INTERRUPTED/TERMINATED with exit 1; the session was stopped above.
        raise interrupted
    final_state = (read_session(ctx.paths, session["session_id"]) or {}).get("final_state")
    result.payload["stopped"] = stopped
    result.payload["final_state"] = final_state
    if final_state == "FAILED":
        proc_exit = (sup.entry("processor") or {}).get("exit")
        result.exit_code = EXIT_READINESS
        result.error = {"code": "SESSION_FAILED",
                        "message": f"session {session['session_id']} ended FAILED (processor exit {proc_exit})",
                        "hint": f"see {wcmd(ctx.paths.root, 'scripts/demo.py logs --component processor')}",
                        "log_excerpt":
                            sup.log_tail("processor", 40, 4000) or None}
    return result


# --------------------------------------------------------------------------------------------------
# wait-ready / status / logs
# --------------------------------------------------------------------------------------------------

def _pointers(ctx: Ctx, run_id: str) -> dict[str, Any]:
    from transitpulse.storage.cassandra import get_session
    from transitpulse.storage.pointer import read_pointer

    session = get_session(ctx.cfg)
    out: dict[str, Any] = {}
    for feed in FEEDS:
        row = read_pointer(session, run_id, feed)
        out[feed] = None if not row else {"snapshot_id": row.get("snapshot_id"),
                                          "view_published_at": str(row.get("view_published_at")),
                                          "expires_at": str(row.get("expires_at"))}
    return out


def cmd_wait_ready(ctx: Ctx, args: argparse.Namespace) -> Result:
    sup = supervisor_for(ctx)
    session = resolve_session(ctx, sup, args.session_id)
    run_id = str(session["run_id"])
    sid = str(session["session_id"])
    ready_file = ctx.paths.run_dir(run_id) / "processor_ready.json"
    t0 = time.monotonic()
    checks: dict[str, Any] = {}
    last_log = t0
    while True:
        proc_alive = _alive_for(sup, "processor", sid)
        checks["processor"] = {"alive": proc_alive, "ready_file": ready_file.is_file()}
        prog = last_query_progress(ctx.paths, run_id)
        checks["query_progress"] = {"batch_id": progress_batch_id(prog),
                                    "timestamp": (prog or {}).get("timestamp")}
        try:
            ptrs = _pointers(ctx, run_id)
        except Exception as exc:  # noqa: BLE001 - Cassandra may be briefly busy
            ptrs = {"error": f"{type(exc).__name__}: {exc}"}
        checks["generation"] = ptrs
        status, _body = http_get(f"{ctx.api_base}/health/ready", timeout=3.0)
        checks["api"] = {"health_ready_status": status}
        has_generation = any(isinstance(v, dict) and v.get("snapshot_id") for v in ptrs.values())
        ok = proc_alive and ready_file.is_file() and checks["query_progress"]["batch_id"] is not None \
            and has_generation and status == 200
        if ok:
            waited = round(time.monotonic() - t0, 1)
            update_session(ctx.paths, sid, first_ready_generation_at=session.get("first_ready_generation_at") or iso_now())
            return Result({"ready": True, "waited_s": waited, "session_id": sid, "run_id": run_id, "checks": checks,
                           "api": {"base_url": ctx.api_base}})
        if not proc_alive:
            raise CliError("PROCESSOR_NOT_RUNNING", "the session's processor is not running",
                           hint=f"see {wcmd(ctx.paths.root, 'scripts/demo.py logs --component processor')}; recover "
                                f"with {wcmd(ctx.paths.root, 'scripts/demo.py restart-processor --session-id ' + sid)}",
                           exit_code=EXIT_READINESS, log_excerpt=sup.log_tail("processor"),
                           details={"checks": checks, "session_id": sid, "run_id": run_id})
        if time.monotonic() - t0 > float(args.timeout_seconds):
            raise CliError("READINESS_TIMEOUT", f"no READY generation served within {args.timeout_seconds} s",
                           hint="live mode needs the first poll (30 s) and a processed batch; check "
                                f"{wcmd(ctx.paths.root, 'scripts/demo.py status --session-id ' + sid)}",
                           exit_code=EXIT_READINESS, log_excerpt=sup.log_tail("processor", 40, 4000),
                           details={"checks": checks, "session_id": sid, "run_id": run_id})
        if time.monotonic() - last_log >= 10:
            log.info("waiting for a READY generation (%.0f/%s s): %s", time.monotonic() - t0, args.timeout_seconds,
                     json.dumps({k: v for k, v in checks.items() if k != "generation"}, default=str))
            last_log = time.monotonic()
        time.sleep(2.0)


def cmd_status(ctx: Ctx, args: argparse.Namespace) -> Result:
    sup = supervisor_for(ctx)
    st = sup.status()
    session = resolve_session(ctx, sup, args.session_id, required=False)
    proc = ProcFS()
    mem = proc.meminfo()
    disk = disk_info(ctx.paths.root)
    warnings: list[str] = []
    avail = mem.get("MemAvailable")
    if avail is not None and avail < 1.5 * GIB:
        warnings.append(f"MemAvailable {avail / GIB:.2f} GiB < 1.5 GiB")
    if disk.free_bytes < 4 * GIB:
        warnings.append(f"disk free {disk.free_bytes / GIB:.2f} GiB < 4 GiB")
    comps: dict[str, Any] = {}
    for name, e in st["components"].items():
        ex = e.get("exit") or {}
        if not e.get("alive") and ex.get("note"):
            warnings.append(f"{name}: {ex['note']}")
        comps[name] = {"state": e.get("state"), "alive": e.get("alive"), "pid": e.get("pid"),
                       "wrapper_pid": e.get("wrapper_pid"), "session_id": e.get("session_id"), "run_id": e.get("run_id"),
                       "rss_bytes": e.get("rss_bytes"), "started_at": e.get("started_at"), "ready_at": e.get("ready_at"),
                       "exit": e.get("exit"), "log": e.get("stdout_log")}
    payload: dict[str, Any] = {"components": comps, "current_session": st.get("current_session"),
                               "resources": {"mem_total_bytes": mem.get("MemTotal"), "mem_available_bytes": avail,
                                             "disk_free_bytes": disk.free_bytes},
                               "warnings": warnings, "session": None}
    try:
        from transitpulse.runtime.access import read_public_urls

        payload["access"] = read_public_urls(ctx, sup)
    except Exception as exc:  # noqa: BLE001 - status must never fail because of the access section
        payload["access"] = {"error": f"{type(exc).__name__}: {exc}"}
    if session is not None:
        sid = str(session["session_id"])
        pstate = read_json_or_none(ctx.paths.session_dir(sid) / "publisher_state.json")
        derived = session_status(ctx, sup, session)
        # Persist terminal outcomes so that readers of session.json (e.g. the notebook clone guard) see them.
        if derived == "BUDGET_EXHAUSTED" and not _alive_for(sup, "processor", sid):
            session = update_session(ctx.paths, sid, state="STOPPED", final_state="BUDGET_EXHAUSTED",
                                     stopped_at=session.get("stopped_at") or iso_now())
            if sup.current_session() == sid:
                sup.set_current_session(None)
        elif derived == "FAILED" and session.get("state") == "RUNNING":
            session = update_session(ctx.paths, sid, state="FAILED", failed_at=iso_now(),
                                     error={"code": "PROCESSOR_EXITED",
                                            "message": f"processor exit {(sup.entry('processor') or {}).get('exit')}"})
        prog = last_query_progress(ctx.paths, str(session["run_id"])) if session.get("run_id") else None
        remaining = None
        if session.get("deadline_at"):
            remaining = max(0, int(parse_iso(str(session["deadline_at"])).timestamp() - time.time()))
        payload["session"] = {
            "session_id": sid, "run_id": session.get("run_id"), "mode": session.get("mode"),
            "source_type": session.get("source_type"), "state": derived, "recorded_state": session.get("state"),
            "started_at": session.get("started_at"), "deadline_at": session.get("deadline_at"),
            "remaining_s": remaining, "scope": session.get("scope"), "speed": session.get("speed"),
            "publisher_state": pstate,
            "query_progress": {"batch_id": progress_batch_id(prog), "timestamp": (prog or {}).get("timestamp"),
                               "num_input_rows": (prog or {}).get("numInputRows")},
        }
    return Result(payload)


def cmd_logs(ctx: Ctx, args: argparse.Namespace) -> Result:
    try:
        return Result(read_log(ctx.paths, args.component, args.file, args.lines))
    except ValueError as exc:
        raise CliError("USAGE", str(exc), exit_code=EXIT_USAGE) from exc


# --------------------------------------------------------------------------------------------------
# Publisher control
# --------------------------------------------------------------------------------------------------

def _control_session(ctx: Ctx, sup: Supervisor, args: argparse.Namespace, command: str) -> dict[str, Any]:
    session = resolve_session(ctx, sup, args.session_id)
    if session.get("source_type") == "live" or session.get("mode") == "live":
        raise CliError("UNSUPPORTED_MODE", f"{command} is only valid for replay/synthetic sessions (live uses the wall "
                       "clock)", exit_code=EXIT_UNSUPPORTED)
    if not _alive_for(sup, "publisher", str(session["session_id"])):
        raise CliError("PUBLISHER_NOT_RUNNING", f"the publisher of session {session['session_id']} is not running",
                       hint="the recording may have finished or the budget ended; see "
                            f"{wcmd(ctx.paths.root, 'scripts/demo.py status')}",
                       exit_code=EXIT_READINESS)
    return session


def send(ctx: Ctx, session: dict[str, Any], command: str, cmd_args: dict[str, Any], timeout: float) -> dict[str, Any]:
    from transitpulse.replay.control import send_command

    try:
        state = send_command(ctx.paths.session_dir(str(session["session_id"])), command, cmd_args, timeout=timeout)
    except TimeoutError as exc:
        raise CliError("NO_ACK", f"publisher did not acknowledge {command} within {timeout:.0f} s",
                       hint=f"check {wcmd(ctx.paths.root, 'scripts/demo.py logs --component publisher')}",
                       exit_code=EXIT_READINESS) from exc
    if isinstance(state, dict) and (state.get("error") or state.get("rejected")):
        raise CliError("COMMAND_REJECTED", f"publisher rejected {command}: {state.get('error') or state.get('rejected')}",
                       exit_code=EXIT_CONFLICT, details={"publisher_state": state})
    return state


def cmd_control(ctx: Ctx, args: argparse.Namespace) -> Result:
    command = args.command
    sup = supervisor_for(ctx)
    session = _control_session(ctx, sup, args, command)
    cmd_args: dict[str, Any] = {}
    if command == "advance-clock":
        try:
            target = parse_iso(args.to)
        except ValueError as exc:
            raise CliError("USAGE", f"--to must be an ISO-8601 instant with a timezone: {exc}",
                           exit_code=EXIT_USAGE) from exc
        cmd_args["to"] = iso(target)
    state = send(ctx, session, command, cmd_args, float(args.timeout))
    return Result({"session_id": session["session_id"], "run_id": session.get("run_id"), "command": command,
                   "args": cmd_args, "publisher_state": state})


def cmd_inject(ctx: Ctx, args: argparse.Namespace) -> Result:
    scenario = args.scenario.replace("-", "_")
    if scenario not in INJECT_SCENARIOS:
        raise CliError("USAGE", f"unknown scenario {args.scenario!r}", hint=f"one of {', '.join(INJECT_SCENARIOS)}",
                       exit_code=EXIT_USAGE)
    sup = supervisor_for(ctx)
    if scenario == "restart_processor":
        res = cmd_restart_processor(ctx, args)
        res.payload["scenario"] = scenario
        _record_injection(ctx, res.payload.get("run_id"), {"scenario": scenario, "result": res.payload})
        return res
    session = _control_session(ctx, sup, args, "inject")
    run_id = str(session["run_id"])
    injected_at = iso_now()
    before = _safe_pointers(ctx, run_id)
    state = send(ctx, session, "inject", {"scenario": scenario}, float(args.timeout))
    from transitpulse.runtime.golden_smoke import wait_processed

    processed = wait_processed(ctx, run_id, timeout=float(args.wait_seconds))
    after = _safe_pointers(ctx, run_id)
    expected = {
        "duplicate_snapshot": "same snapshot_id re-delivered: pointer and history unchanged, duplicate audit +1",
        "late_snapshot": "older snapshot delivered late: pointer does not roll back; LATE_SNAPSHOT_NOT_PUBLISHED",
        "invalid_gps": "invalid coordinates flagged INVALID_COORDINATES; excluded from fresh/stale counts",
    }[scenario]
    record = {"scenario": scenario, "injected_at": injected_at, "expected": expected,
              "actual": {"pointer_before": before, "pointer_after": after, "processed": processed},
              "publisher_state": state, "log_reference": str(ctx.paths.log_dir("publisher") / "stdout.log")}
    path = _record_injection(ctx, run_id, record)
    return Result({"session_id": session["session_id"], "run_id": run_id, **record, "record_path": path})


def _safe_pointers(ctx: Ctx, run_id: str) -> dict[str, Any]:
    try:
        return _pointers(ctx, run_id)
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}


def _record_injection(ctx: Ctx, run_id: str | None, record: dict[str, Any]) -> str | None:
    if not run_id:
        return None
    path = ctx.paths.reports / "fault_demo" / f"{run_id}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"recorded_at": iso_now(), **record}, default=str) + "\n")
    return str(path)


# --------------------------------------------------------------------------------------------------
# restart / stop / reset
# --------------------------------------------------------------------------------------------------

def cmd_restart_processor(ctx: Ctx, args: argparse.Namespace) -> Result:
    sup = supervisor_for(ctx)
    require_infra(ctx, sup)
    session = resolve_session(ctx, sup, args.session_id)
    sid = str(session["session_id"])
    run_id = str(session["run_id"])
    if session.get("state") in ("STOPPED",):
        raise CliError("SESSION_STOPPED", f"session {sid} is stopped",
                       hint=f"start a new session: {wcmd(ctx.paths.root, 'scripts/demo.py start --mode <m> --detach')}",
                       exit_code=EXIT_CONFLICT)
    deadline = parse_iso(str(session["deadline_at"])).timestamp()
    if time.time() >= deadline:
        raise CliError("BUDGET_EXHAUSTED", f"session {sid} reached its duration budget",
                       hint="start a new session (a VM-level rebuild creates a new run, spec §26.4)",
                       exit_code=EXIT_CONFLICT)
    entry = sup.entry("processor")
    if entry and entry.get("run_id") not in (None, run_id) and sup.is_alive("processor"):
        raise CliError("PROCESSOR_SLOT_BUSY", f"the running processor belongs to run {entry.get('run_id')}",
                       exit_code=EXIT_CONFLICT)
    prev = progress_batch_id(last_query_progress(ctx.paths, run_id))
    t_stop = time.time()
    stop_res = sup.stop("processor") if sup.is_alive("processor") else {"stopped": False, "already_stopped": True}
    params = session.get("params") or {}
    started = start_processor(ctx, sup, run_id=run_id, session_id=sid, deadline=deadline,
                              fault_injection=bool(params.get("fault_injection")),
                              ready_timeout=float(getattr(args, "ready_timeout", 300.0)))
    t_ready = time.time()
    resumed = None
    end = time.monotonic() + float(getattr(args, "timeout_seconds", 180.0))
    while time.monotonic() < end:
        prog = last_query_progress(ctx.paths, run_id)
        bid = progress_batch_id(prog)
        if bid is not None and ctx.paths.query_progress_log(run_id).stat().st_mtime >= t_stop and \
                (prev is None or bid > prev):
            resumed = bid
            break
        if not sup.is_alive("processor"):
            raise CliError("PROCESSOR_EXITED", "the restarted processor exited", exit_code=EXIT_READINESS,
                           log_excerpt=sup.log_tail("processor"))
        time.sleep(2.0)
    update_session(ctx.paths, sid, state="RUNNING", last_processor_restart_at=iso_now())
    return Result({"session_id": sid, "run_id": run_id, "prev_batch_id": prev, "resumed_batch_id": resumed,
                   "downtime_s": round(t_ready - t_stop, 1), "checkpoint_dir": session.get("checkpoint_dir"),
                   "stop": stop_res, "processor": started,
                   "note": None if resumed is not None else "no new micro-batch within the wait window "
                                                            "(no new input yet); the query is running"})


def stop_session(ctx: Ctx, sup: Supervisor, session: dict[str, Any] | None, *, drain_timeout: float,
                 include_infra: bool, no_drain: bool) -> dict[str, Any]:
    from transitpulse.runtime import cassandra_native as cn
    from transitpulse.runtime.golden_smoke import wait_processed

    out: dict[str, Any] = {}
    sid = str(session["session_id"]) if session else None
    run_id = str(session["run_id"]) if session and session.get("run_id") else None
    final_state = session_status(ctx, sup, session) if session else None
    # 1. inputs first (the outbox/publisher flush on SIGTERM)
    if session and session.get("mode") != "live" and _alive_for(sup, "publisher", sid):
        with contextlib.suppress(Exception):
            send(ctx, session, "finish", {}, 10.0)
    out.update(stop_components(ctx, sup, ("publisher", "collector"), session_id=sid))
    # 2. drain: processor offsets reach the Kafka end offsets (bounded)
    drained = None
    if run_id and sup.is_alive("processor") and not no_drain:
        drained = wait_processed(ctx, run_id, timeout=drain_timeout)
    # 3. processor, then API. The access components (dashboard + public tunnels) outlive sessions so the public
    #    URL stays stable; while they run, the API keeps serving the finished run's READY data to them.
    out.update(stop_components(ctx, sup, ("processor",), session_id=sid))
    access_alive = [c for c in ACCESS_COMPONENTS if sup.is_alive(c)]
    if include_infra or not access_alive:
        if include_infra:
            from transitpulse.runtime.access import stop_access

            out.update(stop_access(sup))
        out.update(stop_components(ctx, sup, ("api",), session_id=None))
    else:
        out["api"] = {"stopped": False, "reason": f"kept for the exposed dashboard ({', '.join(access_alive)}); "
                                                  "use stop --all or expose stop"}
    # 4. infra on --all
    if include_infra:
        out["kafka"] = sup.stop("kafka")
        drain = None
        if sup.is_alive("cassandra") and ctx.runtime is not None:
            drain = cn.drain(ctx.runtime, ctx.paths, ctx.cfg, timeout=60.0)
        out["cassandra"] = {**sup.stop("cassandra"), "drain": drain}
    if sid:
        update_session(ctx.paths, sid, state="STOPPED", stopped_at=iso_now(), drained=drained,
                       final_state=final_state)
        if sup.current_session() == sid:
            sup.set_current_session(None)
    return {"components": out, "drained": drained, "session_id": sid, "run_id": run_id, "final_state": final_state}


def cmd_stop(ctx: Ctx, args: argparse.Namespace) -> Result:
    sup = supervisor_for(ctx)
    session = resolve_session(ctx, sup, args.session_id, required=False)
    if session is not None and session.get("state") == "STOPPED" and not args.session_id:
        session = None if session["session_id"] != sup.current_session() else session
    with start_lock(ctx):
        res = stop_session(ctx, sup, session, drain_timeout=float(args.drain_timeout), include_infra=args.all,
                           no_drain=args.no_drain)
    return Result({**res, "data_preserved": True, "all": bool(args.all)})


def cmd_reset(ctx: Ctx, args: argparse.Namespace) -> Result:
    from transitpulse.runtime.reset import TARGETS, execute_reset, plan_reset

    targets = [t.strip() for t in args.targets.split(",") if t.strip()]
    bad = [t for t in targets if t not in TARGETS]
    if bad or not targets:
        raise CliError("USAGE", f"unknown reset target(s) {bad}", hint=f"choose from {', '.join(TARGETS)}",
                       exit_code=EXIT_USAGE)
    sup = supervisor_for(ctx)
    running = [n for n, e in sup.status()["components"].items() if e.get("alive")]
    plan = plan_reset(ctx.paths, targets, include_caches=args.include_caches)
    payload = {**plan, "running": running, "dry_run": args.confirm is None}
    if args.confirm is None:
        payload["hint"] = (f"re-run with --confirm {plan['confirm_token']} to DELETE the listed paths"
                           + (" after stopping: " + ", ".join(running) if running else ""))
        return Result(payload)
    if running:
        raise CliError("PROCESSES_RUNNING", f"owned processes are running: {', '.join(running)}",
                       hint=f"run {wcmd(ctx.paths.root, 'scripts/demo.py stop --all')} first", exit_code=EXIT_CONFLICT,
                       details=payload)
    if args.confirm != plan["confirm_token"]:
        raise CliError("CONFIRM_TOKEN_MISMATCH", "the confirm token does not match the current plan",
                       hint="run the dry run again and copy its confirm_token", exit_code=EXIT_CONFLICT,
                       details=payload)
    deleted = execute_reset(plan)
    ctx.paths.ensure_base_dirs()
    return Result({**payload, "dry_run": False, "deleted": deleted})


# --------------------------------------------------------------------------------------------------
# golden-smoke / lineage / benchmark
# --------------------------------------------------------------------------------------------------

def cmd_golden_smoke(ctx: Ctx, args: argparse.Namespace) -> Result:
    from transitpulse.runtime.golden_smoke import run_golden_smoke

    with start_lock(ctx):
        return run_golden_smoke(ctx, args)


def cmd_lineage(ctx: Ctx, args: argparse.Namespace) -> Result:
    sup = supervisor_for(ctx)
    run_id = args.run_id
    if not run_id:
        session = resolve_session(ctx, sup, None)
        run_id = session.get("run_id")
    if not run_id:
        raise CliError("NO_RUN", "no run id given and no session found", exit_code=EXIT_USAGE)
    try:
        import transitpulse.obs.lineage as lineage_mod  # WP9

        trace = getattr(lineage_mod, "trace", None)
    except ImportError:
        trace = None
    if callable(trace):
        available = {"cfg": ctx.cfg, "config": ctx.cfg, "paths": ctx.paths, "workdir": str(ctx.paths.root),
                     "run_id": run_id, "feed": args.feed, "feed_type": args.feed, "snapshot_id": args.snapshot_id,
                     "api_base": ctx.api_base}
        sig = inspect.signature(trace)
        kwargs: dict[str, Any] = {}
        satisfiable = True
        for name, param in sig.parameters.items():
            if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                continue
            if name in available:
                kwargs[name] = available[name]
            elif param.default is param.empty:
                satisfiable = False
        if satisfiable:
            result = trace(**kwargs)
            if not isinstance(result, dict):
                result = {"result": result}
            return Result({"run_id": run_id, "feed": args.feed, "source": "transitpulse.obs.lineage.trace",
                           "trace": json.loads(json.dumps(result, default=str))})
    return Result(builtin_trace(ctx, run_id, args.feed, args.snapshot_id))


def builtin_trace(ctx: Ctx, run_id: str, feed: str, snapshot_id: str | None) -> dict[str, Any]:
    """Explanatory lineage using contracted APIs only: pointer -> manifest -> raw -> Kafka -> batch -> API."""
    import gzip
    import hashlib

    from transitpulse.runtime.paths import resolve_under
    from transitpulse.storage.cassandra import get_session
    from transitpulse.storage.manifest import read_manifest
    from transitpulse.storage.pointer import read_pointer

    steps: list[dict[str, Any]] = []
    session = get_session(ctx.cfg)
    if snapshot_id is None:
        ptr = read_pointer(session, run_id, feed)
        snapshot_id = (ptr or {}).get("snapshot_id")
        steps.append({"name": "pointer", "status": "OK" if snapshot_id else "UNAVAILABLE", "detail": ptr})
    if not snapshot_id:
        return {"run_id": run_id, "feed": feed, "snapshot_id": None, "source": "builtin", "steps": steps}
    man = read_manifest(session, run_id, feed, snapshot_id) or {}
    steps.append({"name": "manifest", "status": "OK" if man.get("status") == "READY" else str(man.get("status")),
                  "detail": {k: man.get(k) for k in ("status", "capture_id", "raw_path", "raw_sha256", "kafka_topic",
                                                     "kafka_partition", "kafka_offset", "first_batch_id", "ready_at",
                                                     "publish_outcome", "source_order")}})
    raw_status: dict[str, Any] = {"name": "raw", "status": "UNAVAILABLE", "detail": None}
    run_json = read_json_or_none(ctx.paths.run_json(run_id)) or {}
    roots = [ctx.paths.raw_run_dir(run_id)]
    prov = run_json.get("provenance") or {}
    if prov.get("recording_dir"):
        roots.append(Path(prov["recording_dir"]))
    if run_json.get("recording_id"):
        with contextlib.suppress(ValueError):
            roots.append(ctx.paths.recordings_root / str(run_json["recording_id"]))
    if man.get("raw_path"):
        for root in roots:
            try:
                f = resolve_under(root, str(man["raw_path"]))
            except ValueError:
                continue
            if f.is_file():
                data = f.read_bytes()
                body = gzip.decompress(data) if f.name.endswith(".gz") else data
                ok = hashlib.sha256(body).hexdigest() == man.get("raw_sha256")
                raw_status = {"name": "raw", "status": "OK" if ok else "MISMATCH",
                              "detail": {"path": str(f), "sha256_verified": ok, "bytes": len(body)}}
                break
    steps.append(raw_status)
    kafka_step: dict[str, Any] = {"name": "kafka", "status": "UNAVAILABLE", "detail": None}
    if man.get("kafka_topic") is not None and man.get("kafka_offset") is not None:
        try:
            from confluent_kafka import Consumer, TopicPartition

            c = Consumer({"bootstrap.servers": ctx.cfg.kafka.bootstrap_servers, "group.id": "tp-lineage",
                          "enable.auto.commit": False, "log_level": 2})
            try:
                c.assign([TopicPartition(man["kafka_topic"], int(man.get("kafka_partition") or 0),
                                         int(man["kafka_offset"]))])
                msg = c.poll(10.0)
            finally:
                c.close()
            if msg is None or msg.error():
                kafka_step = {"name": "kafka", "status": "UNAVAILABLE",
                              "detail": "message not readable (retention may have removed it)"}
            else:
                value = json.loads(msg.value().decode("utf-8"))
                match = value.get("snapshot_id") == snapshot_id
                kafka_step = {"name": "kafka", "status": "OK" if match else "MISMATCH",
                              "detail": {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
                                         "snapshot_id": value.get("snapshot_id")}}
        except Exception as exc:  # noqa: BLE001
            kafka_step = {"name": "kafka", "status": "UNAVAILABLE", "detail": f"{type(exc).__name__}: {exc}"}
    steps.append(kafka_step)
    steps.append({"name": "batch", "status": "OK" if man.get("first_batch_id") is not None else "UNAVAILABLE",
                  "detail": {"first_batch_id": man.get("first_batch_id")}})
    endpoint = {"vehicle_positions": "/vehicles", "trip_updates": "/quality", "alerts": "/alerts"}[feed]
    status, body = http_get(f"{ctx.api_base}{endpoint}?run_id={run_id}&limit=1", timeout=5.0)
    found = _find_snapshot_ids(body.get("meta") if isinstance(body, dict) else None)
    steps.append({"name": "api", "status": "OK" if snapshot_id in found else ("UNAVAILABLE" if status != 200
                                                                                else "DIFFERENT_GENERATION"),
                  "detail": {"http_status": status, "endpoint": endpoint, "meta_snapshot_ids": sorted(found)}})
    return {"run_id": run_id, "feed": feed, "snapshot_id": snapshot_id, "source": "builtin", "steps": steps}


def _find_snapshot_ids(node: Any) -> set[str]:
    out: set[str] = set()
    if isinstance(node, dict):
        for k, v in node.items():
            if k == "snapshot_id" and isinstance(v, str):
                out.add(v)
            else:
                out |= _find_snapshot_ids(v)
    elif isinstance(node, list):
        for v in node:
            out |= _find_snapshot_ids(v)
    return out


def cmd_expose(ctx: Ctx, args: argparse.Namespace) -> Result:
    from transitpulse.runtime.access import cmd_expose as expose

    return expose(ctx, args)


def cmd_benchmark(ctx: Ctx, args: argparse.Namespace) -> Result:
    script = ctx.repo_dir / "benchmarks" / "run_benchmark.py"
    if not script.is_file():
        return Result({"status": "NOT_RUN", "reason": f"{script} not present"}, exit_code=EXIT_FAILURE,
                      error={"code": "BENCHMARK_UNAVAILABLE", "message": "benchmark harness not found",
                             "hint": "benchmarks/ is provided by the packaging work package", "log_excerpt": None})
    sup = supervisor_for(ctx)
    require_infra(ctx, sup)
    for comp in ("processor", "publisher", "collector"):
        if sup.is_alive(comp):
            raise CliError("SESSION_ACTIVE", f"{comp} is running: benchmarks use separate runs",
                           hint=f"run {wcmd(ctx.paths.root, 'scripts/demo.py stop')} first", exit_code=EXIT_CONFLICT,
                           details={"status": "NOT_RUN"})
    argv = [ctx.python, str(script), "--scenario", args.scenario, "--workdir", str(ctx.paths.root),
            "--profile", ctx.profile]
    if args.max_seconds:
        argv += ["--max-seconds", str(int(args.max_seconds))]
    if args.recording_manifest:
        argv += ["--recording-manifest", args.recording_manifest]
    if args.repetitions:
        argv += ["--repetitions", str(int(args.repetitions))]
    budget = float(args.max_seconds or 1800) + 900.0
    doc = run_json_cli(argv, env=core_env(ctx), cwd=ctx.repo_dir, timeout=budget, what="benchmark")
    return Result({**{k: v for k, v in doc.items() if k not in ("ok", "action", "error")},
                   "delegated_to": str(script)})


# --------------------------------------------------------------------------------------------------
# Parser
# --------------------------------------------------------------------------------------------------

def _add_input_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--mode", required=True, choices=list(MODES))
    p.add_argument("--seed", type=int, default=None, help="synthetic seed (default: config synthetic.default_seed)")
    p.add_argument("--recording-manifest", default=None, help="replay: recording_manifest.json of the input bundle")
    p.add_argument("--static-zip", default=None, help="replay: the recording's GTFS ZIP if not inside the bundle")
    p.add_argument("--route-ids", default=None, help="comma-separated route ids (required for live)")
    p.add_argument("--speed", type=float, default=None, help="replay/synthetic speed (default 10 replay, 1 synthetic)")
    p.add_argument("--duration-seconds", type=int, default=600, help=f"wall-time budget [{MIN_DURATION}, {MAX_DURATION}]")


def build_parser() -> JsonArgumentParser:
    p = JsonArgumentParser(prog="demo.py", description="TransitPulse demo session lifecycle (native profiles).")
    sub = p.add_subparsers(dest="command", required=True, parser_class=JsonArgumentParser)

    def new(name: str, help_text: str) -> argparse.ArgumentParser:
        s = sub.add_parser(name, help=help_text)
        add_common_args(s)
        return s

    s = new("prepare", "prepare inputs for a mode (static version, scope, input checksums)")
    _add_input_args(s)
    s = new("start", "start (or reuse) a demo session")
    _add_input_args(s)
    s.add_argument("--detach", action="store_true", help="return once ready (the notebook uses this)")
    s.add_argument("--ready-timeout", type=float, default=300.0)
    s.add_argument("--enable-fault-injection", action="store_true", help="processor honours fault arm files")
    s = new("wait-ready", "wait for query progress, a READY generation and API readiness")
    s.add_argument("--session-id", default=None)
    s.add_argument("--timeout-seconds", type=float, default=300.0)
    s = new("status", "session, components, resources")
    s.add_argument("--session-id", default=None)
    s = new("logs", "tail a component log")
    s.add_argument("--component", required=True, choices=list(LOG_COMPONENTS))
    s.add_argument("--file", default="stdout")
    s.add_argument("--lines", type=int, default=200)
    for name in ("pause", "resume", "step"):
        s = new(name, f"{name} the replay/synthetic publisher")
        s.add_argument("--session-id", default=None)
        s.add_argument("--timeout", type=float, default=10.0)
    s = new("advance-clock", "advance the source clock (synthetic/replay)")
    s.add_argument("--session-id", default=None)
    s.add_argument("--to", required=True, help="ISO-8601 instant, e.g. 2026-10-05T12:03:10Z")
    s.add_argument("--timeout", type=float, default=10.0)
    s = new("inject", "interactive scenario (duplicate/late/invalid GPS or processor restart)")
    s.add_argument("--session-id", default=None)
    s.add_argument("--scenario", required=True)
    s.add_argument("--timeout", type=float, default=10.0)
    s.add_argument("--wait-seconds", type=float, default=30.0)
    s.add_argument("--ready-timeout", type=float, default=300.0)
    s.add_argument("--timeout-seconds", type=float, default=180.0)
    s = new("restart-processor", "graceful stop + start of the processor (same run and checkpoint)")
    s.add_argument("--session-id", default=None)
    s.add_argument("--ready-timeout", type=float, default=300.0)
    s.add_argument("--timeout-seconds", type=float, default=180.0)
    s = new("stop", "stop the session (publisher first, drain, processor, API); data is preserved")
    s.add_argument("--session-id", default=None)
    s.add_argument("--all", action="store_true", help="also stop Kafka and Cassandra")
    s.add_argument("--drain-timeout", type=float, default=60.0)
    s.add_argument("--no-drain", action="store_true")
    s = new("reset", "DELETE selected work-directory areas (dry run unless --confirm)")
    s.add_argument("--targets", required=True, help="comma list of session,checkpoints,kafka,cassandra,data,all")
    s.add_argument("--include-caches", action="store_true")
    s.add_argument("--confirm", default=None)
    s = new("golden-smoke", "golden fixture through Kafka -> Spark -> Cassandra -> API vs expected.json")
    s.add_argument("--timeout-seconds", type=float, default=900.0)
    s.add_argument("--ready-timeout", type=float, default=300.0)
    s = new("lineage", "trace one snapshot: pointer -> manifest -> raw -> Kafka -> batch -> API")
    s.add_argument("--run-id", default=None)
    s.add_argument("--feed", default="vehicle_positions", choices=list(FEEDS))
    s.add_argument("--snapshot-id", default=None)
    s = new("expose", "public access: Streamlit, React console, Cloudflare quick tunnel (start|status|stop)")
    s.add_argument("op", choices=["start", "status", "stop"])
    s.add_argument("--target", default="dashboard", choices=["dashboard", "api", "react", "both", "all"],
                   help="dashboard tunnels :8501; react tunnels the Vite preview on :3000; api tunnels :8000; "
                        "both = dashboard+api; all = dashboard+api+react")
    s.add_argument("--ready-timeout", type=float, default=120.0)
    s = new("benchmark", "run a bounded benchmark scenario on separate runs (delegates to benchmarks/)")
    s.add_argument("--scenario", required=True)
    s.add_argument("--max-seconds", type=int, default=None)
    s.add_argument("--recording-manifest", default=None)
    s.add_argument("--repetitions", type=int, default=None)
    return p


HANDLERS = {
    "prepare": cmd_prepare, "start": cmd_start, "wait-ready": cmd_wait_ready, "status": cmd_status, "logs": cmd_logs,
    "pause": cmd_control, "resume": cmd_control, "step": cmd_control, "advance-clock": cmd_control,
    "inject": cmd_inject, "restart-processor": cmd_restart_processor, "stop": cmd_stop, "reset": cmd_reset,
    "golden-smoke": cmd_golden_smoke, "lineage": cmd_lineage, "benchmark": cmd_benchmark, "expose": cmd_expose,
}
NO_RUNTIME_NEEDED = ("status", "logs", "reset", "stop")


def handle(args: argparse.Namespace) -> Result:
    ctx = load_ctx(args, need_runtime=args.command not in NO_RUNTIME_NEEDED)
    try:
        return HANDLERS[args.command](ctx, args)
    finally:
        with contextlib.suppress(Exception):
            from transitpulse.storage.cassandra import shutdown

            shutdown()


def main(argv: list[str] | None = None) -> int:
    return run_main(lambda a: str(a.command), build_parser(), handle, argv)


__all__ = ["main", "start_processor", "start_publisher", "start_collector", "ensure_api", "stop_components",
           "stop_session"]


if __name__ == "__main__":  # python -m transitpulse.runtime.cli_demo ...
    import sys

    sys.exit(main())
