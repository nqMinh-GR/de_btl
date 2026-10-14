"""Streaming processor entry point (WP4; interfaces §7, design B §3).

    python -m transitpulse.processing.stream_job --workdir W --run-id R [--profile P]
           [--enable-fault-injection] [--no-runtime] [--duration-seconds N]

One processor instance serves ONE run. It reads ``runtime.json`` (connector jars), ``W/runs/<R>/run.json``
and the run's static scope, creates the SparkSession in-process and starts the query ``realtime_main``:

* Kafka source: subscribe the three snapshot topics + control, ``startingOffsets=earliest`` (only used for
  a fresh checkpoint; restarts resume from the checkpoint offsets), ``failOnDataLoss=true``,
  ``kafka.max.partition.fetch.bytes`` / ``kafka.fetch.max.bytes`` from config, a ``groupIdPrefix`` (never a
  fixed ``kafka.group.id``) and NO ``maxOffsetsPerTrigger`` (D-PUB-04). Other runs' messages in the
  retention are read and dropped by the exact key filter (D-PUB-08).
* Checkpoint ``W/checkpoints/<pipeline_version>/<run_id>/<query_name>`` (stable across restarts).
* Trigger ``processingTime = spark.trigger_seconds``; sink ``foreachBatch(processing.batch.process_batch)``.
* A StreamingQueryListener appends ``W/logs/runs/<R>/query_progress.jsonl`` (one JSON per progress event:
  batchId, numInputRows, durationMs, endOffsets/latestOffsets per topic/partition, timestamp), updates
  the Kafka backlog gauges and upserts ``pipeline_status('heartbeat')`` (also on idle events).
* ``W/runs/<R>/processor_ready.json`` (``{query_id, started_at, checkpoint, ...}``) is written once the
  query is active; a stale file from a previous process is removed first.
* SIGTERM/SIGINT: wait (bounded) for the running trigger to finish, then ``query.stop()``; exit 0.
  A failed query propagates: the processor row is marked FAILED and the process exits 1. A fired fault
  point exits with 86 from inside the batch (``processing/faults.py``).

Stdout carries exactly one JSON document at exit; logs go to stderr.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql.streaming import StreamingQueryListener

from transitpulse.contracts.envelope import iso_utc, utcnow
from transitpulse.contracts.enums import FEED_ORDER
from transitpulse.obs.logging import configure, excerpt, get_logger, log_event
from transitpulse.runtime.paths import WorkPaths, safe_component

_log = get_logger("transitpulse.processing.stream_job")
ACTION = "stream"
GRACEFUL_STOP_WAIT_SECONDS = 60.0
HEARTBEAT_COLUMNS = ("run_id", "component", "status", "last_heartbeat_at", "last_batch_id", "progress_json",
                     "last_error")


class Preflight(Exception):
    def __init__(self, code: str, message: str, hint: str | None = None, exit_code: int = 3) -> None:
        super().__init__(message)
        self.code, self.message, self.hint, self.exit_code = code, message, hint, exit_code


def emit(doc: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(doc, default=str, sort_keys=True) + "\n")
    sys.stdout.flush()


def _error_doc(code: str, message: str, hint: str | None = None, **extra: Any) -> dict[str, Any]:
    return {"ok": False, "action": ACTION, **extra,
            "error": {"code": code, "message": message, "hint": hint, "log_excerpt": None}}


class _JsonArgParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # type: ignore[override]
        emit(_error_doc("usage", message, "python -m transitpulse.processing.stream_job --workdir W --run-id R"))
        sys.exit(2)


def _logs_to_stderr() -> None:
    for h in logging.getLogger().handlers:
        if getattr(h, "_transitpulse", False) and isinstance(h, logging.StreamHandler):
            h.setStream(sys.stderr)


# --------------------------------------------------------------------------------------------------
# Query progress listener
# --------------------------------------------------------------------------------------------------

def progress_dict(progress: Any) -> dict[str, Any]:
    """StreamingQueryProgress -> plain dict (its JSON form; offsets as nested objects)."""
    raw = getattr(progress, "json", None)
    if callable(raw):
        raw = raw()
    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(progress, dict):
        return progress
    out: dict[str, Any] = {}
    for k in ("id", "runId", "name", "timestamp", "batchId", "numInputRows", "durationMs"):
        v = getattr(progress, k, None)
        out[k] = dict(v) if k == "durationMs" and v is not None else (str(v) if k in ("id", "runId") else v)
    sources = []
    for s in getattr(progress, "sources", None) or []:
        sd: dict[str, Any] = {"description": getattr(s, "description", None)}
        for k in ("startOffset", "endOffset", "latestOffset"):
            v = getattr(s, k, None)
            try:
                sd[k] = json.loads(v) if isinstance(v, str) else v
            except ValueError:
                sd[k] = v
        sources.append(sd)
    out["sources"] = sources
    return out


def _offsets(node: Any) -> dict[str, dict[str, int]]:
    if isinstance(node, str):
        try:
            node = json.loads(node)
        except ValueError:
            return {}
    out: dict[str, dict[str, int]] = {}
    if isinstance(node, dict):
        for topic, parts in node.items():
            if isinstance(parts, dict):
                for p, off in parts.items():
                    try:
                        out.setdefault(str(topic), {})[str(p)] = int(off)
                    except (TypeError, ValueError):
                        continue
    return out


def backlog_entries(rec: dict[str, Any]) -> list[dict[str, Any]]:
    """``[{topic, partition, end_offset, latest_offset, backlog}]`` from a progress record (Spark offsets:
    ``endOffset`` = next offset the query will read, ``latestOffset`` = broker end offset at planning)."""
    out: list[dict[str, Any]] = []
    end_all = rec.get("endOffsets") or {}
    latest_all = rec.get("latestOffsets") or {}
    for topic in sorted(set(end_all) | set(latest_all)):
        end_t, latest_t = end_all.get(topic, {}), latest_all.get(topic, {})
        for part in sorted(set(end_t) | set(latest_t), key=lambda x: (len(str(x)), str(x))):
            end, latest = end_t.get(part), latest_t.get(part)
            out.append({"topic": topic, "partition": int(part), "end_offset": end, "latest_offset": latest,
                        "backlog": max(0, latest - end) if end is not None and latest is not None else None})
    return out


PROGRESS_JSON_MAX_CHARS = 20000
# Dropped in this order when the heartbeat document is too large (the result is always valid JSON).
_PROGRESS_DROP_ORDER: tuple[str, ...] = ("endOffsets", "latestOffsets", "durationMs", "kafka_backlog")
_PROGRESS_KEEP: frozenset[str] = frozenset({"query_id", "batch_id", "numInputRows"})


def bounded_progress_json(doc: dict[str, Any], limit: int = PROGRESS_JSON_MAX_CHARS) -> str:
    """Compact JSON of ``doc`` no longer than ``limit``: bulky keys are DROPPED (never string-sliced)."""
    d = dict(doc)
    text = json.dumps(d, default=str, sort_keys=True)
    dropped: list[str] = []
    order = [k for k in _PROGRESS_DROP_ORDER if k in d] + sorted(k for k in d
                                                                 if k not in _PROGRESS_DROP_ORDER
                                                                 and k not in _PROGRESS_KEEP)
    for key in order:
        if len(text) <= limit:
            break
        d.pop(key, None)
        dropped.append(key)
        d["truncated_keys"] = dropped
        text = json.dumps(d, default=str, sort_keys=True)
    if len(text) > limit:   # pathological: keep only identifiers
        text = json.dumps({k: d.get(k) for k in sorted(_PROGRESS_KEEP)} | {"truncated_keys": dropped},
                          default=str, sort_keys=True)
    return text


def progress_record(p: dict[str, Any]) -> dict[str, Any]:
    end: dict[str, dict[str, int]] = {}
    latest: dict[str, dict[str, int]] = {}
    for s in p.get("sources") or []:
        for topic, parts in _offsets(s.get("endOffset")).items():
            end.setdefault(topic, {}).update(parts)
        for topic, parts in _offsets(s.get("latestOffset")).items():
            latest.setdefault(topic, {}).update(parts)
    return {
        "batchId": p.get("batchId"), "numInputRows": p.get("numInputRows"), "durationMs": p.get("durationMs"),
        "endOffsets": end, "latestOffsets": latest, "timestamp": p.get("timestamp"), "id": p.get("id"),
        "runId": p.get("runId"), "recorded_at": iso_utc(utcnow()),
    }


class ProgressListener(StreamingQueryListener):
    """query_progress.jsonl + backlog gauges + heartbeat row (never raises into Spark)."""

    def __init__(self, *, run_id: str, progress_path: Path, session: Any, metrics: Any) -> None:
        super().__init__()
        self.run_id = run_id
        self.progress_path = progress_path
        self.session = session
        self.metrics = metrics
        self.last_batch_id: int | None = None
        self.last_backlog: list[dict[str, Any]] = []
        self.query_id: str | None = None
        self._lock = threading.Lock()

    def _heartbeat(self, status: str, progress: dict[str, Any] | None = None, error: str | None = None) -> None:
        """Plain upsert of ``pipeline_status('heartbeat')``.

        ``last_error`` is always bound explicitly: the error text for a FAILED heartbeat, otherwise NULL so a
        previous failure does not survive a restart. Every heartbeat carries the last known Kafka backlog.
        """
        from transitpulse.storage.cassandra import execute, unset_if_none

        cql = (f"INSERT INTO pipeline_status ({', '.join(HEARTBEAT_COLUMNS)}) "
               f"VALUES ({', '.join('?' for _ in HEARTBEAT_COLUMNS)})")
        doc = {"query_id": self.query_id, "batch_id": self.last_batch_id, "kafka_backlog": self.last_backlog,
               **(progress or {})}
        row = {"run_id": self.run_id, "component": "heartbeat", "status": status, "last_heartbeat_at": utcnow(),
               "last_batch_id": self.last_batch_id, "progress_json": bounded_progress_json(doc),
               "last_error": error}
        params = [row[c] if c == "last_error" else unset_if_none([row[c]])[0] for c in HEARTBEAT_COLUMNS]
        execute(self.session, cql, params, idempotent=True)

    def _safe(self, what: str, fn, *args) -> None:
        try:
            fn(*args)
        except Exception as exc:  # noqa: BLE001 - observability must never fail the query
            log_event(_log, "heartbeat", "failed", f"listener {what}: {exc!r}"[:500], level=logging.WARNING)

    def onQueryStarted(self, event) -> None:  # noqa: N802 - Spark API name
        self.query_id = str(event.id)
        self._safe("started", self._heartbeat, "STARTED")

    def _on_progress(self, event) -> None:
        rec = progress_record(progress_dict(event.progress))
        if isinstance(rec.get("batchId"), int):
            self.last_batch_id = rec["batchId"]
        self.progress_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, open(self.progress_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, default=str, sort_keys=True) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        backlog = backlog_entries(rec)
        self.last_backlog = backlog
        if self.metrics is not None:
            for b in backlog:
                if b["backlog"] is not None:
                    self.metrics.kafka_backlog.labels(topic=b["topic"], partition=str(b["partition"])).set(
                        b["backlog"])
        self._heartbeat("ACTIVE", {"batch_id": rec["batchId"], "numInputRows": rec["numInputRows"],
                                   "endOffsets": rec["endOffsets"], "latestOffsets": rec["latestOffsets"],
                                   "kafka_backlog": backlog})

    def onQueryProgress(self, event) -> None:  # noqa: N802
        self._safe("progress", self._on_progress, event)

    def onQueryIdle(self, event) -> None:  # noqa: N802
        self._safe("idle", self._heartbeat, "IDLE")

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        exc = getattr(event, "exception", None)
        self._safe("terminated", self._heartbeat, "FAILED" if exc else "TERMINATED", None,
                   None if not exc else str(exc)[:2000])


# --------------------------------------------------------------------------------------------------
# Query
# --------------------------------------------------------------------------------------------------

def kafka_source_options(cfg, run_id: str) -> dict[str, str]:
    t = cfg.kafka.topics
    return {
        "kafka.bootstrap.servers": cfg.kafka.bootstrap_servers,
        "subscribe": ",".join([t.vehicle_positions, t.trip_updates, t.alerts, t.control]),
        "startingOffsets": "earliest",
        "failOnDataLoss": "true",
        "kafka.max.partition.fetch.bytes": str(cfg.spark.max_partition_fetch_bytes),
        "kafka.fetch.max.bytes": str(cfg.spark.fetch_max_bytes),
        "groupIdPrefix": f"tp-{run_id[:8]}",
        "includeHeaders": "false",
    }


def _graceful_stop(query) -> None:
    deadline = time.monotonic() + GRACEFUL_STOP_WAIT_SECONDS
    while time.monotonic() < deadline:
        try:
            if not (query.status or {}).get("isTriggerActive"):
                break
        except Exception:  # noqa: BLE001 - query already gone
            break
        time.sleep(0.2)
    query.stop()


def _write_ready(path: Path, doc: dict[str, Any]) -> None:
    from transitpulse.processing.curated import atomic_write_json

    atomic_write_json(path, doc)


def run(args: argparse.Namespace) -> int:
    from transitpulse.config import load_config
    from transitpulse.processing import batch as batch_mod
    from transitpulse.processing.faults import FaultInjector
    from transitpulse.processing.spark_session import build_session
    from transitpulse.static.dims import load_dims, scope_dir_is_complete
    from transitpulse.storage.cassandra import get_session

    try:
        cfg = load_config(args.profile, workdir=args.workdir)
        paths = WorkPaths.from_workdir(cfg.workdir)
        run_id = safe_component(args.run_id, "run_id")
    except (ValueError, OSError) as exc:
        emit(_error_doc("config", str(exc), "check --workdir/--profile/--run-id"))
        return 2
    configure("processor", run_id=run_id)
    _logs_to_stderr()

    metrics = None
    spark = None
    session = None
    query = None
    stopped_by = "terminated"
    try:
        # ---- preflight
        from transitpulse.runs import load_run

        try:
            run_rec = load_run(paths, run_id)
        except FileNotFoundError as exc:
            raise Preflight("run_not_found", f"run.json not found: {exc}", "create the run first (demo.py start)")
        runtime = None
        try:
            from transitpulse.runtime.manifest import load_runtime

            runtime = load_runtime(paths)
        except FileNotFoundError as exc:
            if not args.no_runtime:
                raise Preflight("runtime_missing", str(exc), "run the bootstrap (scripts/bootstrap_colab.sh)")
        scope_dir = Path(run_rec.static_scope_dir)
        if not scope_dir_is_complete(scope_dir):
            raise Preflight("static_incomplete", f"static scope directory is incomplete: {scope_dir}",
                            "run the static loader for this run's gtfs_version/scope")
        try:
            session = get_session(cfg)
        except Exception as exc:  # noqa: BLE001 - reported as a preflight failure
            raise Preflight("cassandra_unavailable", f"cannot connect to Cassandra: {exc}",
                            "start infra (scripts/infra.py start) and apply migrations") from exc
        from transitpulse.processing.control_clock import RunNotFound, read_run_info

        try:
            info = read_run_info(session, run_id)
        except RunNotFound as exc:
            raise Preflight("run_metadata_missing", str(exc), "create the run (writes run_metadata) first") from exc
        for attr in ("gtfs_version", "scope_id", "clock_mode"):
            if getattr(info, attr) != getattr(run_rec, attr):
                raise Preflight("run_mismatch", f"run.json {attr}={getattr(run_rec, attr)!r} but run_metadata has "
                                f"{getattr(info, attr)!r}", "the run record and Cassandra disagree; create a new run")

        # ---- spark + context
        spark = build_session(cfg, runtime, app_name=cfg.spark.app_name, with_kafka=True,
                              extra_conf={"spark.sql.streaming.noDataProgressEventInterval": "5000"})
        dims = load_dims(spark, scope_dir)
        metrics = batch_mod.ProcessorMetrics()
        metrics.cm.start_exporter(paths.metrics_dir, cfg.observability.metrics_export_seconds)
        query_name = cfg.spark.query_name
        ckpt = paths.checkpoint_dir(run_rec.pipeline_version, run_id, query_name)
        ckpt.parent.mkdir(parents=True, exist_ok=True)
        ctx = batch_mod.BatchContext(
            cfg=cfg, paths=paths, run_id=run_id, scope_id=run_rec.scope_id, gtfs_version=run_rec.gtfs_version,
            pipeline_version=run_rec.pipeline_version, clock_mode=run_rec.clock_mode, dims=dims, session=session,
            metrics=metrics, faults=FaultInjector(paths, run_id, enabled=bool(args.enable_fault_injection)),
            query_name=query_name, timestamp_ms_threshold=int(run_rec.policy.timestamp_ms_threshold),
            checkpoint_dir=ckpt,
        )
        processor = batch_mod.configure(ctx)
        batch_mod.bootstrap_status_rows(session, run_id, sorted(FEED_ORDER, key=FEED_ORDER.get), now=utcnow())

        listener = ProgressListener(run_id=run_id, progress_path=paths.query_progress_log(run_id),
                                    session=session, metrics=metrics)
        spark.streams.addListener(listener)
        ready_path = paths.run_dir(run_id) / "processor_ready.json"
        if ready_path.exists():
            ready_path.unlink()   # stale readiness marker of a previous process (not data)

        reader = spark.readStream.format("kafka")
        for k, v in kafka_source_options(cfg, run_id).items():
            reader = reader.option(k, v)
        source = reader.load()
        query = (source.writeStream.queryName(query_name)
                 .foreachBatch(processor)
                 .option("checkpointLocation", str(ckpt))
                 .trigger(processingTime=f"{int(cfg.spark.trigger_seconds)} seconds")
                 .start())
        ctx.query_id = str(query.id)
        started_at: datetime = utcnow()
        deadline = time.monotonic() + 120
        while not query.isActive and time.monotonic() < deadline:
            time.sleep(0.2)
        if not query.isActive:
            query.awaitTermination(1)   # raises the query's exception if it failed to start
            raise RuntimeError("streaming query did not become active")
        _write_ready(ready_path, {"query_id": str(query.id), "query_run_id": str(query.runId),
                                  "query_name": query_name, "run_id": run_id, "started_at": iso_utc(started_at),
                                  "checkpoint": str(ckpt), "pid": os.getpid(),
                                  "pipeline_version": run_rec.pipeline_version,
                                  "recording_id": getattr(run_rec, "recording_id", None),
                                  "fault_injection": bool(args.enable_fault_injection)})
        log_event(_log, "batch_start", "ok", "processor ready", query_id=str(query.id), checkpoint=str(ckpt))

        # ---- run until terminated / signalled / duration
        stop = threading.Event()

        def _on_signal(signum, _frame) -> None:
            log_event(_log, "batch_commit", "ok", f"signal {signum} received; stopping gracefully")
            stop.set()

        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)
        t_end = time.monotonic() + args.duration_seconds if args.duration_seconds else None
        while True:
            if query.awaitTermination(1):
                stopped_by = "terminated"
                exc = query.exception()
                if exc is not None:
                    raise RuntimeError(f"streaming query terminated with error: {exc}")
                break
            if stop.is_set():
                stopped_by = "signal"
                _graceful_stop(query)
                break
            if t_end is not None and time.monotonic() >= t_end:
                stopped_by = "duration"
                _graceful_stop(query)
                break
        last = query.lastProgress or {}
        emit({"ok": True, "action": ACTION, "run_id": run_id, "query_id": str(query.id), "checkpoint": str(ckpt),
              "stopped_by": stopped_by, "last_batch_id": last.get("batchId") if isinstance(last, dict) else None,
              "error": None})
        return 0
    except Preflight as pf:
        log_event(_log, "batch_start", "failed", pf.message, level=logging.ERROR, code=pf.code)
        emit(_error_doc(pf.code, pf.message, pf.hint, run_id=run_id))
        return pf.exit_code
    except Exception as exc:  # noqa: BLE001 - reported as one JSON document, non-zero exit
        msg = f"{type(exc).__name__}: {exc}"
        log_event(_log, "batch_commit", "failed", msg[:2000], level=logging.ERROR)
        if session is not None:
            try:
                from transitpulse.processing.batch import record_processor_failure

                record_processor_failure(session, run_id, msg, now=utcnow())
            except Exception as rec_exc:  # noqa: BLE001 - Cassandra may be the failure itself
                log_event(_log, "batch_commit", "failed", f"cannot record failure: {rec_exc!r}"[:500],
                          level=logging.WARNING)
        doc = _error_doc("processor_failed", msg[:2000], "inspect the processor log; restart with "
                         "demo.py restart-processor (same run and checkpoint)", run_id=run_id)
        doc["error"]["log_excerpt"] = excerpt(msg, 2000)
        emit(doc)
        return 1
    finally:
        if metrics is not None:
            metrics.cm.stop(paths.metrics_dir)
        if spark is not None:
            try:
                spark.stop()
            except Exception:  # noqa: BLE001 - shutting down
                pass


def build_parser() -> argparse.ArgumentParser:
    p = _JsonArgParser(prog="python -m transitpulse.processing.stream_job",
                       description="TransitPulse Spark Structured Streaming processor (one run).")
    p.add_argument("--workdir", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--profile", default=None)
    p.add_argument("--enable-fault-injection", action="store_true",
                   help="honour arm files under W/faults/<run_id>/ (tests and demo only)")
    p.add_argument("--no-runtime", action="store_true",
                   help="allow a missing runtime.json (no connector jars; tests only)")
    p.add_argument("--duration-seconds", type=float, default=None,
                   help="stop gracefully after this many seconds (tests)")
    return p


def main(argv: list[str] | None = None) -> int:
    return run(build_parser().parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
