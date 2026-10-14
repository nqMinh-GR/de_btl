"""``python -m transitpulse.replay {verify|publish|synth-generate|control}`` (interfaces §5).

Every command prints exactly ONE JSON document on stdout::

    {"ok": bool, "action": str, ..., "error": {"code", "message", "hint", "log_excerpt"} | null}

Structured logs and progress go to stderr. Exit codes: 0 ok, 2 usage / recording verification failure
(with guidance), 3 preflight, 4 conflict, 5 readiness (publisher not running / no ACK), 6 unsupported
mode (e.g. pause on a live run), 7 artifact/checksum, 1 other failure.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import traceback
from pathlib import Path
from typing import Any

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_PREFLIGHT = 3
EXIT_CONFLICT = 4
EXIT_READINESS = 5
EXIT_UNSUPPORTED = 6
EXIT_ARTIFACT = 7

_COMMAND_ERROR_EXIT = {
    "unsupported_mode": EXIT_UNSUPPORTED,
    "invalid_argument": EXIT_USAGE,
    "invalid_command": EXIT_USAGE,
    "clock_regression": EXIT_CONFLICT,
    "not_ready": EXIT_READINESS,
}

_log = logging.getLogger("transitpulse.replay.cli")


class CliError(Exception):
    def __init__(self, exit_code: int, code: str, message: str, hint: str | None = None,
                 log_excerpt: str | None = None, payload: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.exit_code = exit_code
        self.code = code
        self.message = message
        self.hint = hint
        self.log_excerpt = log_excerpt
        self.payload = payload or {}

    def error(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "hint": self.hint, "log_excerpt": self.log_excerpt}


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Any:  # type: ignore[override]
        raise CliError(EXIT_USAGE, "usage", message, hint=self.format_usage().strip())


def _emit(action: str, ok: bool, payload: dict[str, Any], error: dict[str, Any] | None) -> None:
    doc = {"ok": ok, "action": action, **payload, "error": error}
    sys.stdout.write(json.dumps(doc, ensure_ascii=False, default=str) + "\n")
    sys.stdout.flush()


def _setup_logging(run_id: str | None = None, session_id: str | None = None) -> None:
    from transitpulse.obs.logging import configure

    configure("publisher", run_id=run_id, session_id=session_id)
    for handler in logging.getLogger().handlers:
        if getattr(handler, "_transitpulse", False) and isinstance(handler, logging.StreamHandler):
            handler.setStream(sys.stderr)  # stdout carries exactly one JSON document


def _split_ids(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    out: list[str] = []
    for v in values:
        out.extend(p.strip() for p in v.split(",") if p.strip())
    return out or None


def _load(args: argparse.Namespace):
    from transitpulse.config import load_config
    from transitpulse.runtime.paths import WorkPaths

    try:
        cfg = load_config(args.profile, workdir=args.workdir)
    except Exception as exc:  # noqa: BLE001 - surfaced as a preflight error
        raise CliError(EXIT_PREFLIGHT, "config_invalid", f"cannot load configuration: {exc}",
                       "Check --profile/--workdir and config/*.yaml.") from exc
    return cfg, WorkPaths.from_workdir(cfg.workdir)


def _safe_id(value: str, what: str) -> str:
    from transitpulse.runtime.paths import safe_component

    try:
        return safe_component(value, what)
    except ValueError as exc:
        raise CliError(EXIT_USAGE, "invalid_argument", str(exc)) from exc


# --------------------------------------------------------------------------------------------------
# verify
# --------------------------------------------------------------------------------------------------

def cmd_verify(args: argparse.Namespace) -> dict[str, Any]:
    from transitpulse.replay.recording_verify import RecordingVerificationError, verify_recording

    _, paths = _load(args)
    _setup_logging()
    try:
        verified = verify_recording(
            Path(args.manifest),
            static_zip=Path(args.static_zip) if args.static_zip else None,
            route_ids=_split_ids(args.route_ids),
            paths=paths,
        )
    except RecordingVerificationError as exc:
        raise CliError(EXIT_USAGE, exc.code, exc.message, exc.hint, payload={"details": exc.details}) from exc
    return verified.to_json()


# --------------------------------------------------------------------------------------------------
# publish
# --------------------------------------------------------------------------------------------------

def cmd_publish(args: argparse.Namespace) -> dict[str, Any]:
    from transitpulse.contracts.enums import FEED_ORDER
    from transitpulse.obs.logging import log_event
    from transitpulse.obs.metrics import ComponentMetrics
    from transitpulse.replay.envelopes import CaptureEnveloper
    from transitpulse.replay.publisher import Publisher, PublisherError, PublisherOptions
    from transitpulse.replay.recording_verify import RecordingVerificationError, verify_recording
    from transitpulse.replay.synthetic.materialize import write_run_labels
    from transitpulse.runs import load_run

    run_id = _safe_id(args.run_id, "run_id")
    session_id = _safe_id(args.session_id, "session_id")
    cfg, paths = _load(args)
    _setup_logging(run_id, session_id)
    try:
        run = load_run(paths, run_id)
    except FileNotFoundError as exc:
        raise CliError(EXIT_PREFLIGHT, "run_not_found", f"run {run_id} has no {paths.run_json(run_id)}",
                       "Create the run first (scripts/demo.py start or transitpulse.runs.create_run).") from exc
    if run.source_type == "live":
        raise CliError(EXIT_UNSUPPORTED, "unsupported_mode",
                       f"run {run_id} is live (wall clock); the replay publisher only drives replay/synthetic runs",
                       "Live runs are fed by `python -m transitpulse.ingestion collect`.")
    try:
        verified = verify_recording(Path(args.manifest), static_zip=Path(run.static_zip_path),
                                    route_ids=run.route_ids, paths=paths)
    except RecordingVerificationError as exc:
        raise CliError(EXIT_USAGE, exc.code, exc.message, exc.hint, payload={"details": exc.details}) from exc
    if verified.gtfs_version != run.gtfs_version:
        raise CliError(EXIT_USAGE, "STATIC_VERSION_MISMATCH",
                       f"run pins GTFS {run.gtfs_version} but the recording requires {verified.gtfs_version}",
                       "Create a new run from this recording's static version; never join a recording with "
                       "another ZIP.")
    if (verified.source_type == "synthetic") != (run.source_type == "synthetic"):
        raise CliError(EXIT_USAGE, "SOURCE_TYPE_MISMATCH",
                       f"recording source_type={verified.source_type} cannot be published as a "
                       f"{run.source_type} run",
                       "Synthetic recordings need source_type=synthetic runs; real recordings need "
                       "source_type=replay runs.")
    speed = float(args.speed) if args.speed is not None else float(run.speed or 1.0)
    try:
        options = PublisherOptions.from_config(cfg, speed=speed, step_mode=bool(args.step_mode),
                                               duration_seconds=args.duration_seconds)
    except ValueError as exc:
        raise CliError(EXIT_USAGE, "invalid_argument", str(exc)) from exc

    session_dir = paths.session_dir(session_id)
    session_dir.mkdir(parents=True, exist_ok=True)
    metrics = ComponentMetrics("publisher")
    metrics.start_exporter(paths.metrics_dir, cfg.observability.metrics_export_seconds)
    from transitpulse.ingestion.kafka_producer import EnvelopeProducer

    producer = EnvelopeProducer(cfg, "publisher", run.run_id)
    try:
        enveloper = CaptureEnveloper(run=run, recording_dir=verified.recording_dir, raw_root=paths.raw_root,
                                     max_snapshot_bytes=cfg.kafka.max_snapshot_bytes,
                                     scope_dir=Path(run.static_scope_dir))
        labels_path = None
        if verified.source_type == "synthetic":
            try:
                labels_path = write_run_labels(paths.run_dir(run.run_id), run, verified, enveloper)
            except Exception as exc:  # noqa: BLE001 - labels are an aid for tests, never fatal
                log_event(_log, "timeline", "failed", f"cannot write run labels: {exc}", level=logging.WARNING)
        publisher = Publisher(
            run=run,
            items=verified.items,
            start_clock=run.initial_clock or verified.start_clock,
            session_dir=session_dir,
            producer=producer,
            enveloper=enveloper,
            options=options,
            fault_dir=paths.fault_dir(run.run_id),
            run_dir=paths.run_dir(run.run_id),
            session_id=session_id,
            recording_id=verified.recording_id,
            manifest_sha256=verified.manifest_sha256,
            topics={feed: cfg.kafka.topics.for_feed(feed) for feed in FEED_ORDER},
            control_topic=cfg.kafka.topics.control,
            dlq_topic=cfg.kafka.topics.dlq,
            metrics=metrics,
        )

        def _on_signal(signum: int, _frame: Any) -> None:
            publisher.request_stop(signal.Signals(signum).name)

        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)
        try:
            result = publisher.run()
        except PublisherError as exc:
            raise CliError(exc.exit_code, exc.code, exc.message, exc.hint) from exc
        except CliError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise CliError(EXIT_FAILURE, "publisher_failed", str(exc),
                           "See the publisher log; restarting the publisher resumes from publisher_state.json.",
                           log_excerpt=traceback.format_exc()[-2000:]) from exc
    finally:
        try:
            producer.flush(10.0)
            producer.close()
        except Exception as exc:  # noqa: BLE001
            log_event(_log, "publish", "failed", f"producer close failed: {exc}", level=logging.WARNING)
        metrics.stop(paths.metrics_dir)
    return {
        "run_id": run.run_id,
        "session_id": session_id,
        "recording_id": verified.recording_id,
        "source_type": run.source_type,
        "speed": options.speed,
        "step_mode": options.step_mode,
        "labels_path": str(labels_path) if labels_path else None,
        **result.to_json(),
    }


# --------------------------------------------------------------------------------------------------
# synth-generate
# --------------------------------------------------------------------------------------------------

def cmd_synth_generate(args: argparse.Namespace) -> dict[str, Any]:
    from transitpulse.replay.synthetic.demo import DEFAULT_DEMO_DURATION_SECONDS, generate_demo
    from transitpulse.replay.synthetic.materialize import (
        GOLDEN_RUN,
        IT_SCENARIO_RUNS,
        FixtureError,
        materialize_fixture,
    )
    from transitpulse.replay.synthetic.recording import RecordingConflict
    from transitpulse.replay.synthetic.scenarios import ScenarioError
    from transitpulse.replay.synthetic.stress import DEFAULT_STRESS_DURATION_SECONDS, generate_stress

    cfg, paths = _load(args)
    _setup_logging()
    paths.recordings_root.mkdir(parents=True, exist_ok=True)
    seed = int(args.seed) if args.seed is not None else int(cfg.synthetic.default_seed)
    fixture_dir = Path(args.fixture_dir) if args.fixture_dir else None
    try:
        if args.kind == "demo":
            result = generate_demo(cfg, paths, seed=seed, scenarios=_split_ids(args.scenarios) or (),
                                   duration_seconds=args.duration_seconds or DEFAULT_DEMO_DURATION_SECONDS)
        elif args.kind == "stress":
            if args.rate is None:
                raise CliError(EXIT_USAGE, "invalid_argument", "--kind stress requires --rate 100|500|1000")
            result = generate_stress(cfg, paths, rate=int(args.rate), seed=seed,
                                     duration_seconds=args.duration_seconds or DEFAULT_STRESS_DURATION_SECONDS)
        elif args.kind == "golden":
            result = materialize_fixture(cfg, paths, run_name=GOLDEN_RUN, fixture_dir=fixture_dir)
        else:
            if args.scenario_name not in IT_SCENARIO_RUNS:
                raise CliError(EXIT_USAGE, "invalid_argument",
                               f"--kind scenario requires --scenario-name one of {list(IT_SCENARIO_RUNS)}")
            result = materialize_fixture(cfg, paths, run_name=args.scenario_name, fixture_dir=fixture_dir)
    except RecordingConflict as exc:
        raise CliError(EXIT_CONFLICT, "recording_conflict", str(exc),
                       "Move the existing directory aside; recordings are never overwritten.") from exc
    except (ScenarioError, FixtureError, ValueError) as exc:
        raise CliError(EXIT_USAGE, "invalid_argument", str(exc)) from exc
    payload = result.to_json()
    payload["kind"] = args.kind
    payload["seed"] = seed if args.kind in ("demo", "stress") else None
    return payload


# --------------------------------------------------------------------------------------------------
# control
# --------------------------------------------------------------------------------------------------

def cmd_control(args: argparse.Namespace) -> dict[str, Any]:
    from transitpulse.contracts.envelope import parse_iso_utc
    from transitpulse.replay import control as pc
    from transitpulse.runtime.paths import WorkPaths

    session_id = _safe_id(args.session_id, "session_id")
    paths = WorkPaths.from_workdir(args.workdir)
    _setup_logging(session_id=session_id)
    session_dir = paths.session_dir(session_id)
    command = args.command_name
    source_type = None
    session_file = session_dir / "session.json"
    if session_file.is_file():
        try:
            source_type = json.loads(session_file.read_text(encoding="utf-8")).get("source_type")
        except (OSError, ValueError):
            source_type = None
    state = pc.read_publisher_state(session_dir)
    if source_type is None and state is not None:
        source_type = state.get("source_type")
    if source_type == "live" and command in pc.SOURCE_CLOCK_COMMANDS:
        raise CliError(EXIT_UNSUPPORTED, "unsupported_mode",
                       f"{command} is not supported for a live (wall-clock) session",
                       "pause/resume/step/advance-clock/inject apply to replay or synthetic sessions only.")
    if command == "inject" and source_type not in (None, "synthetic"):
        raise CliError(EXIT_UNSUPPORTED, "unsupported_mode", "inject is only available for synthetic sessions")

    payload: dict[str, Any] = {}
    if command == "advance-clock":
        if not args.to:
            raise CliError(EXIT_USAGE, "invalid_argument", "advance-clock requires --to <ISO-8601 UTC>")
        try:
            parse_iso_utc(args.to)
        except ValueError as exc:
            raise CliError(EXIT_USAGE, "invalid_argument", f"invalid --to {args.to!r}: {exc}") from exc
        payload["to"] = args.to
    if command == "inject":
        if args.scenario not in pc.INJECT_SCENARIOS:
            raise CliError(EXIT_USAGE, "invalid_argument",
                           f"inject requires --scenario one of {list(pc.INJECT_SCENARIOS)}")
        payload["scenario"] = args.scenario
    if state is not None and state.get("state") in pc.TERMINAL_STATES:
        raise CliError(EXIT_READINESS, "publisher_not_running",
                       f"the publisher of session {session_id} is {state.get('state')}",
                       "Start a new session (scripts/demo.py start) or restart the publisher.",
                       payload={"publisher_state": state})
    try:
        ack = pc.send_command(session_dir, command, payload, timeout=float(args.timeout))
    except pc.ControlTimeout as exc:
        raise CliError(EXIT_READINESS, "no_ack",
                       str(exc), "Is the publisher running? Check `scripts/demo.py status` and the publisher log.",
                       payload={"request_id": exc.request_id, "publisher_state": exc.last_state}) from exc
    last = ack.get("last_command") or {}
    request_id = ack.get("applied_request_id")
    if last.get("request_id") == request_id and last.get("ok") is False:
        err = last.get("error") or {}
        code = str(err.get("code") or "command_rejected")
        raise CliError(_COMMAND_ERROR_EXIT.get(code, EXIT_FAILURE), code, str(err.get("message") or code),
                       payload={"request_id": request_id, "publisher_state": ack})
    return {
        "session_id": session_id,
        "command": command,
        "request_id": request_id,
        "state": ack.get("state"),
        "clock_sequence": ack.get("clock_sequence"),
        "source_clock": ack.get("source_clock"),
        "items_published": ack.get("items_published"),
        "items_total": ack.get("items_total"),
        "last_barrier": ack.get("last_barrier"),
        "result": last.get("result") if last.get("request_id") == request_id else None,
    }


# --------------------------------------------------------------------------------------------------
# parser / main
# --------------------------------------------------------------------------------------------------

ACTIONS = {"verify": "verify", "publish": "publish", "synth-generate": "synth-generate", "control": "control"}


def build_parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="python -m transitpulse.replay", description="Replay publisher and synthetic generator")
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--workdir", required=True, help="work directory (W)")
        p.add_argument("--profile", default=None, help="config profile (local-native, colab-native, local-docker)")

    p = sub.add_parser("verify", help="verify a recording (checksums, static version, scope)")
    common(p)
    p.add_argument("--manifest", required=True)
    p.add_argument("--static-zip", default=None)
    p.add_argument("--route-ids", nargs="*", default=None, help="comma or space separated route ids")
    p.set_defaults(func=cmd_verify)

    p = sub.add_parser("publish", help="long-running replay/synthetic publisher")
    common(p)
    p.add_argument("--run-id", required=True)
    p.add_argument("--manifest", required=True)
    p.add_argument("--session-id", required=True)
    p.add_argument("--speed", type=float, default=None)
    p.add_argument("--step-mode", action="store_true")
    p.add_argument("--duration-seconds", type=float, default=None)
    p.set_defaults(func=cmd_publish)

    p = sub.add_parser("synth-generate", help="generate a deterministic synthetic recording")
    common(p)
    p.add_argument("--kind", required=True, choices=("demo", "golden", "stress", "scenario"))
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--scenarios", nargs="*", default=None, help="comma or space separated scenario ids")
    p.add_argument("--rate", type=int, default=None, help="stress rate: 100, 500 or 1000 candidates/s")
    p.add_argument("--duration-seconds", type=int, default=None)
    p.add_argument("--scenario-name", default=None, help="IT06|IT07|IT08|IT09 for --kind scenario")
    p.add_argument("--fixture-dir", default=None, help="golden fixture directory (default tests/fixtures/golden)")
    p.set_defaults(func=cmd_synth_generate)

    p = sub.add_parser("control", help="send a control command to a running publisher")
    common(p)
    p.add_argument("--session-id", required=True)
    p.add_argument("--command", dest="command_name", required=True,
                   choices=("pause", "resume", "finish", "step", "advance-clock", "inject"))
    p.add_argument("--to", default=None, help="advance-clock target (ISO-8601 UTC)")
    p.add_argument("--scenario", default=None, choices=("duplicate_snapshot", "late_snapshot", "invalid_gps"))
    p.add_argument("--timeout", type=float, default=5.0)
    p.set_defaults(func=cmd_control)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    action = "replay"
    try:
        args = parser.parse_args(argv)
        action = ACTIONS.get(args.command, args.command)
        payload = args.func(args)
    except CliError as exc:
        _emit(action, False, exc.payload, exc.error())
        return exc.exit_code
    except Exception as exc:  # noqa: BLE001 - always answer with one JSON document
        _emit(action, False, {}, {"code": "internal_error", "message": str(exc), "hint": None,
                                  "log_excerpt": traceback.format_exc()[-2000:]})
        return EXIT_FAILURE
    _emit(action, True, payload, None)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
