"""``scripts/infra.py``: Kafka + Cassandra lifecycle for the native profiles.

    infra.py start  [--components kafka,cassandra] [--ready-timeout 240]
    infra.py init   [--reconcile-topic-configs]
    infra.py status
    infra.py logs   --component kafka|cassandra [--file stdout|server|controller|system|debug|gc] [--lines 200]
    infra.py stop   [--components kafka,cassandra] [--no-drain]
    infra.py adopt  [--components kafka,cassandra]

Common flags: ``--profile colab-native --workdir W``. One JSON document on stdout.
"""

from __future__ import annotations

import argparse
from typing import Any

from transitpulse.runtime.cli_common import (
    EXIT_CONFLICT,
    EXIT_PREFLIGHT,
    EXIT_READINESS,
    EXIT_USAGE,
    CliError,
    Ctx,
    JsonArgumentParser,
    Result,
    add_common_args,
    load_ctx,
    run_main,
    wait_until,
    wcmd,
)
from transitpulse.runtime.proc import ProcFS, disk_info
from transitpulse.runtime.supervisor import APP_COMPONENTS, INFRA_COMPONENTS, Supervisor, spawn_gate_bytes
from transitpulse.runtime.util import atomic_write_json, iso_now, read_json_or_none


def supervisor_for(ctx: Ctx) -> Supervisor:
    rt = ctx.runtime
    return Supervisor(ctx.paths, python=str(rt.core_python) if rt is not None else None)


def _components(text: str | None) -> list[str]:
    items = [c.strip() for c in (text or "kafka,cassandra").split(",") if c.strip()]
    bad = [c for c in items if c not in INFRA_COMPONENTS]
    if bad or not items:
        raise CliError("USAGE", f"unknown infra component(s) {bad}", hint="use kafka,cassandra", exit_code=EXIT_USAGE)
    return items


def _wrap_native(exc: Exception) -> CliError:
    code = getattr(exc, "code", "INFRA_ERROR")
    return CliError(code, getattr(exc, "message", str(exc)), hint=getattr(exc, "hint", None),
                    exit_code=getattr(exc, "exit_code", 1), log_excerpt=getattr(exc, "log_excerpt", None))


def entry_view(sup: Supervisor, entry: dict[str, Any] | None) -> dict[str, Any]:
    if not entry:
        return {"state": "NOT_REGISTERED"}
    return {"state": entry.get("state"), "reused": entry.get("reused"), "pid": entry.get("pid"),
            "wrapper_pid": entry.get("wrapper_pid"), "started_at": entry.get("started_at"),
            "ready_at": entry.get("ready_at"), "ready_seconds": entry.get("ready_seconds"),
            "log": entry.get("stdout_log"), "session_id": entry.get("session_id"), "run_id": entry.get("run_id")}


# --------------------------------------------------------------------------------------------------
# start-or-reuse helpers (also used by demo.py)
# --------------------------------------------------------------------------------------------------

def start_kafka(ctx: Ctx, sup: Supervisor, *, ready_timeout: float) -> dict[str, Any]:
    from transitpulse.runtime import kafka_native as kn

    rt = ctx.require_runtime()
    try:
        fmt = kn.ensure_formatted(rt, ctx.paths, ctx.cfg)
    except kn.KafkaError as exc:
        raise _wrap_native(exc) from exc
    probe = kn.KafkaReadyProbe(ctx.cfg, fmt["cluster_id"])
    entry = sup.start("kafka", kn.kafka_argv(rt, ctx.paths), env=kn.kafka_env(rt, ctx.paths, ctx.cfg), scope="infra",
                      ports=kn.kafka_ports(ctx.cfg), ready=probe, ready_timeout=ready_timeout,
                      fingerprint_extra=kn.fingerprint_extra(ctx.paths),
                      min_mem_available_bytes=spawn_gate_bytes("kafka"))
    return {**entry_view(sup, entry), "cluster_id": fmt["cluster_id"], "format": fmt["action"],
            "format_warnings": fmt["warnings"], "broker": probe.info, "bootstrap": ctx.cfg.kafka.bootstrap_servers}


def start_cassandra(ctx: Ctx, sup: Supervisor, *, ready_timeout: float) -> dict[str, Any]:
    from transitpulse.runtime import cassandra_native as cn

    rt = ctx.require_runtime()
    try:
        cn.check_conf(ctx.paths)
        cn.ensure_dirs(ctx.paths)
        argv = cn.cassandra_argv(rt, ctx.cfg)
    except cn.CassandraError as exc:
        raise _wrap_native(exc) from exc
    probe = cn.CassandraReadyProbe(ctx.cfg)
    entry = sup.start("cassandra", argv, env=cn.cassandra_env(rt, ctx.paths, ctx.cfg), scope="infra",
                      ports=cn.cassandra_ports(ctx.cfg), ready=probe, ready_timeout=ready_timeout,
                      fingerprint_extra=cn.fingerprint_extra(ctx.paths),
                      min_mem_available_bytes=spawn_gate_bytes("cassandra"))
    return {**entry_view(sup, entry), "release_version": probe.info.get("release_version"),
            "host_id": probe.info.get("host_id"), "ran_as_root": cn.running_as_root(), "root_flag": "-R" in argv,
            "contact_point": f"{ctx.cfg.cassandra.hosts[0]}:{ctx.cfg.cassandra.port}"}


def require_infra(ctx: Ctx, sup: Supervisor, *, need_init: bool = True) -> None:
    down = [c for c in INFRA_COMPONENTS if not sup.is_alive(c)]
    if down:
        raise CliError("INFRA_NOT_RUNNING", f"{', '.join(down)} not running",
                       hint=f"run {wcmd(ctx.paths.root, 'scripts/infra.py start')} and "
                            f"{wcmd(ctx.paths.root, 'scripts/infra.py init')} first", exit_code=EXIT_PREFLIGHT)
    if need_init and not (ctx.paths.runtime_dir / "infra_init.json").is_file():
        raise CliError("INFRA_NOT_INITIALIZED", "topics/CQL schema were never initialized for this work directory",
                       hint=f"run {wcmd(ctx.paths.root, 'scripts/infra.py init')}", exit_code=EXIT_PREFLIGHT)


# --------------------------------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------------------------------

def cmd_start(ctx: Ctx, args: argparse.Namespace) -> Result:
    ctx.require_runtime()
    ctx.paths.ensure_base_dirs()
    sup = supervisor_for(ctx)
    out: dict[str, Any] = {}
    comps = _components(args.components)
    if "kafka" in comps:
        out["kafka"] = start_kafka(ctx, sup, ready_timeout=min(float(args.ready_timeout), 600.0))
    if "cassandra" in comps:
        out["cassandra"] = start_cassandra(ctx, sup, ready_timeout=float(args.ready_timeout))
    return Result({"components": out, "workdir": str(ctx.paths.root),
                   "reused": all(bool(v.get("reused")) for v in out.values())})


def cmd_init(ctx: Ctx, args: argparse.Namespace) -> Result:
    from transitpulse.runtime import cassandra_native as cn
    from transitpulse.runtime import kafka_native as kn

    external = bool(getattr(args, "external_infra", False)) or ctx.profile == "local-docker"
    external_cluster_id = None
    if external:
        # Kafka/Cassandra are managed elsewhere (docker compose): not in our registry. Real readiness probes
        # (AdminClient metadata + describe_cluster; driver query of system.local) instead of the registry.
        kprobe = kn.KafkaReadyProbe(ctx.cfg, None)
        cprobe = cn.CassandraReadyProbe(ctx.cfg)
        timeout = float(getattr(args, "ready_timeout", 240.0))
        if not wait_until(kprobe, timeout=timeout, interval=2.0, what="external Kafka"):
            raise CliError("INFRA_NOT_READY", f"Kafka at {ctx.cfg.kafka.bootstrap_servers} not ready within "
                           f"{int(timeout)} s", hint="check the Kafka container/service", exit_code=EXIT_READINESS,
                           details={"kafka": kprobe.info})
        if not wait_until(cprobe, timeout=timeout, interval=3.0, what="external Cassandra"):
            raise CliError("INFRA_NOT_READY", f"Cassandra at {ctx.cfg.cassandra.hosts} not ready within "
                           f"{int(timeout)} s", hint="check the Cassandra container/service", exit_code=EXIT_READINESS,
                           details={"cassandra": cprobe.info})
        external_cluster_id = kprobe.info.get("cluster_id")
    else:
        require_infra(ctx, supervisor_for(ctx), need_init=False)
    try:
        topics = kn.topic_status(ctx.cfg, reconcile=args.reconcile_topic_configs)
    except kn.KafkaError as exc:
        raise _wrap_native(exc) from exc
    try:
        migrations = cn.apply_schema(ctx.cfg)
    except Exception as exc:
        if type(exc).__name__ == "MigrationChecksumMismatch":
            raise CliError("MIGRATION_CHECKSUM_MISMATCH", str(exc),
                           hint="a migration file changed after it was applied; add a new migration instead",
                           exit_code=EXIT_CONFLICT) from exc
        raise
    probe = cn.probe_read_write(ctx.cfg)
    try:
        from transitpulse.storage.cassandra import shutdown

        shutdown()
    except Exception:  # noqa: BLE001 - closing the driver is best effort
        pass
    cluster = read_json_or_none(ctx.paths.kafka_cluster_json) or {}
    if external:
        cluster = {"cluster_id": external_cluster_id, "external": True}
    drift = [t for t in topics if t["status"] == "EXISTS_DRIFT"]
    record = {"initialized_at": iso_now(), "kafka_cluster_id": cluster.get("cluster_id"),
              "cassandra_host_id": probe.get("host_id"), "release_version": probe.get("release_version"),
              "topics": topics, "migrations": migrations, "probe": probe, "topic_drift": bool(drift)}
    atomic_write_json(ctx.paths.runtime_dir / "infra_init.json", record)
    payload = {"topics": topics, "migrations": migrations, "probe": probe, "cluster_id": cluster.get("cluster_id"),
               "release_version": probe.get("release_version"), "marker": str(ctx.paths.runtime_dir / "infra_init.json")}
    if drift:
        return Result(payload, exit_code=EXIT_CONFLICT, error={
            "code": "TOPIC_CONFIG_DRIFT", "message": f"{len(drift)} topic(s) differ from the desired configuration",
            "hint": "review the diff; --reconcile-topic-configs fixes retention/segment/max.message.bytes "
                    "(partitions are never changed, topics never deleted)", "log_excerpt": None})
    if probe.get("status") != "PASS":
        return Result(payload, exit_code=1, error={"code": "CASSANDRA_PROBE_FAILED",
                                                   "message": "write/read probe did not round-trip", "hint": None,
                                                   "log_excerpt": None})
    return Result(payload)


def cmd_status(ctx: Ctx, args: argparse.Namespace) -> Result:
    sup = supervisor_for(ctx)
    st = sup.status()
    proc = ProcFS()
    mem = proc.meminfo()
    disk = disk_info(ctx.paths.root)
    comps = {c: {**entry_view(sup, st["components"].get(c)), "alive": bool(st["components"].get(c, {}).get("alive")),
                 "rss_bytes": st["components"].get(c, {}).get("rss_bytes"),
                 "exit": st["components"].get(c, {}).get("exit")} for c in INFRA_COMPONENTS}
    init = read_json_or_none(ctx.paths.runtime_dir / "infra_init.json") or {}
    return Result({"components": comps, "initialized_at": init.get("initialized_at"),
                   "kafka_cluster_id": (read_json_or_none(ctx.paths.kafka_cluster_json) or {}).get("cluster_id"),
                   "resources": {"mem_total_bytes": mem.get("MemTotal"), "mem_available_bytes": mem.get("MemAvailable"),
                                 "disk_free_bytes": disk.free_bytes},
                   "current_session": st.get("current_session")})


def cmd_logs(ctx: Ctx, args: argparse.Namespace) -> Result:
    from transitpulse.runtime.session import read_log

    if args.component not in INFRA_COMPONENTS:
        raise CliError("USAGE", "infra.py logs supports kafka|cassandra",
                       hint=f"for app components use {wcmd(ctx.paths.root, 'scripts/demo.py logs --component <c>')}",
                       exit_code=EXIT_USAGE)
    try:
        return Result(read_log(ctx.paths, args.component, args.file, args.lines))
    except ValueError as exc:
        raise CliError("USAGE", str(exc), exit_code=EXIT_USAGE) from exc


def cmd_stop(ctx: Ctx, args: argparse.Namespace) -> Result:
    from transitpulse.runtime import cassandra_native as cn

    sup = supervisor_for(ctx)
    running_app = [c for c in APP_COMPONENTS if sup.is_alive(c)]
    if running_app:
        raise CliError("APP_RUNNING", f"application components still running: {', '.join(running_app)}",
                       hint=f"run {wcmd(ctx.paths.root, 'scripts/demo.py stop --all')} first "
                            "(it also stops the API, dashboard and tunnels)", exit_code=EXIT_CONFLICT)
    comps = _components(args.components)
    out: dict[str, Any] = {}
    if "kafka" in comps:
        out["kafka"] = sup.stop("kafka")
    if "cassandra" in comps:
        drain = None
        if sup.is_alive("cassandra") and not args.no_drain and ctx.runtime is not None:
            drain = cn.drain(ctx.runtime, ctx.paths, ctx.cfg, timeout=60.0)
        out["cassandra"] = {**sup.stop("cassandra"), "drain": drain}
    return Result({"components": out, "data_preserved": True})


def cmd_adopt(ctx: Ctx, args: argparse.Namespace) -> Result:
    from transitpulse.runtime import cassandra_native as cn
    from transitpulse.runtime import kafka_native as kn
    from transitpulse.runtime.supervisor import SupervisorError

    rt = ctx.require_runtime()
    sup = supervisor_for(ctx)
    out: dict[str, Any] = {}
    for comp in _components(args.components):
        if sup.is_alive(comp):
            out[comp] = {"status": "ALREADY_REGISTERED", **entry_view(sup, sup.entry(comp))}
            continue
        if comp == "kafka":
            argv, env, extra, ports = (kn.kafka_argv(rt, ctx.paths), kn.kafka_env(rt, ctx.paths, ctx.cfg),
                                       kn.fingerprint_extra(ctx.paths), kn.kafka_ports(ctx.cfg))
            probe: Any = kn.KafkaReadyProbe(ctx.cfg, (read_json_or_none(ctx.paths.kafka_cluster_json) or {}).get("cluster_id"))
        else:
            try:
                argv = cn.cassandra_argv(rt, ctx.cfg)
            except cn.CassandraError as exc:
                raise _wrap_native(exc) from exc
            env, extra, ports = cn.cassandra_env(rt, ctx.paths, ctx.cfg), cn.fingerprint_extra(ctx.paths), \
                cn.cassandra_ports(ctx.cfg)
            probe = cn.CassandraReadyProbe(ctx.cfg)
        try:
            entry = sup.adopt(comp, ports=ports, fingerprint_argv=argv, fingerprint_env=env, fingerprint_extra=extra)
        except SupervisorError as exc:
            out[comp] = {"status": "NOT_FOUND", "message": exc.message, "hint": exc.hint}
            continue
        try:
            ready = bool(probe())
        except Exception:  # noqa: BLE001
            ready = False
        out[comp] = {"status": "ADOPTED", "ready": ready, "wrapper_pid": entry["wrapper_pid"], "pid": entry["pid"]}
    return Result({"components": out})


def build_parser() -> JsonArgumentParser:
    p = JsonArgumentParser(prog="infra.py", description="Kafka + Cassandra native lifecycle (no Docker/systemd).")
    sub = p.add_subparsers(dest="command", required=True, parser_class=JsonArgumentParser)
    s = sub.add_parser("start", help="start (or reuse) Kafka and Cassandra and wait for real readiness")
    add_common_args(s)
    s.add_argument("--components", default="kafka,cassandra")
    s.add_argument("--ready-timeout", type=float, default=240.0)
    s = sub.add_parser("init", help="create topics (idempotent, diff on drift) and apply CQL migrations")
    add_common_args(s)
    s.add_argument("--reconcile-topic-configs", action="store_true",
                   help="apply retention.*/segment.*/max.message.bytes differences (never partitions)")
    s.add_argument("--external-infra", action="store_true",
                   help="Kafka/Cassandra are managed outside this supervisor (docker compose); implied by "
                        "--profile local-docker. Readiness is probed with AdminClient / the Cassandra driver")
    s.add_argument("--ready-timeout", type=float, default=240.0,
                   help="external infra only: how long to wait for Kafka and Cassandra readiness")
    s = sub.add_parser("status", help="state of the infra components")
    add_common_args(s)
    s = sub.add_parser("logs", help="tail a Kafka/Cassandra log")
    add_common_args(s)
    s.add_argument("--component", required=True, choices=list(INFRA_COMPONENTS))
    s.add_argument("--file", default="stdout")
    s.add_argument("--lines", type=int, default=200)
    s = sub.add_parser("stop", help="graceful stop (Cassandra: nodetool drain first); data is preserved")
    add_common_args(s)
    s.add_argument("--components", default="kafka,cassandra")
    s.add_argument("--no-drain", action="store_true")
    s = sub.add_parser("adopt", help="re-register running Kafka/Cassandra processes of THIS work directory")
    add_common_args(s)
    s.add_argument("--components", default="kafka,cassandra")
    return p


HANDLERS = {"start": cmd_start, "init": cmd_init, "status": cmd_status, "logs": cmd_logs, "stop": cmd_stop,
            "adopt": cmd_adopt}


def handle(args: argparse.Namespace) -> Result:
    # init needs no runtime.json (docker compose runs it without a native bootstrap).
    need_runtime = args.command not in ("status", "logs", "stop", "init")
    ctx = load_ctx(args, need_runtime=need_runtime)
    return HANDLERS[args.command](ctx, args)


def main(argv: list[str] | None = None) -> int:
    return run_main(lambda a: str(a.command), build_parser(), handle, argv)


if __name__ == "__main__":  # python -m transitpulse.runtime.cli_infra ... (docker-compose init job, Make)
    import sys

    sys.exit(main())
