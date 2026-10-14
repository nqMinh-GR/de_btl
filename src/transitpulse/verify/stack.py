"""Real-stack harness shared by the integration tests (tests/integration) and the benchmarks (benchmarks/).

It drives ONLY project entry points and frozen interfaces (docs/interfaces.md):

* processes are started/stopped through :class:`transitpulse.runtime.supervisor.Supervisor` (owned process groups,
  never ``pkill``); the processor argv/env come from ``runtime.spark_launch``;
* runs are created with :func:`transitpulse.runs.create_run` (the path the app uses);
* envelopes are produced with :class:`transitpulse.ingestion.kafka_producer.EnvelopeProducer` and clock events with
  :func:`transitpulse.contracts.control.clock_event`;
* processing barriers use the processor's own evidence: ``logs/runs/<run_id>/query_progress.jsonl`` end offsets
  compared with the Kafka high watermarks, ``run_metadata.clock_sequence`` and the ``feed_current`` pointer.

Nothing here deletes data. Fault injection writes the documented one-shot arm files.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from transitpulse import __version__
from transitpulse.config import ProjectConfig, load_config
from transitpulse.contracts import control as ctl
from transitpulse.contracts.envelope import iso_utc, utcnow
from transitpulse.contracts.topics import CONTROL, SNAPSHOT_TOPICS
from transitpulse.runtime.paths import WorkPaths
from transitpulse.verify.fixture import StaticScope, run_json_cli
from transitpulse.verify.progress import latest_end_offsets, read_prom_file

HARNESS_COMPONENT = "pytest-harness"
SESSION_COMPONENTS = ("processor", "publisher", "collector", "api", "dashboard")


class HarnessError(RuntimeError):
    """A wait or an orchestration step of the harness failed (message carries the evidence)."""


class ProcessorExited(HarnessError):
    def __init__(self, run_id: str, exit_code: int | None, log_tail: str) -> None:
        super().__init__(f"processor of run {run_id} exited (code {exit_code}); log tail:\n{log_tail[-4000:]}")
        self.run_id = run_id
        self.exit_code = exit_code
        self.log_tail = log_tail


@dataclass(frozen=True)
class Delivery:
    topic: str
    partition: int
    offset: int
    published_at: datetime | None


def new_session_id(prefix: str = "pytest") -> str:
    return f"{prefix}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:4]}"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def tail_file(path: Path | None, lines: int = 80, max_bytes: int = 16384) -> str:
    if path is None:
        return ""
    try:
        with open(path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - max_bytes))
            data = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return ""
    return "\n".join(data.splitlines()[-lines:])


def wait_until(pred: Callable[[], tuple[bool, Any]], timeout: float, desc: str, *, interval: float = 1.0) -> Any:
    """Poll ``pred`` (returns ``(done, info)``) until done; raise HarnessError with the last info on timeout."""
    deadline = time.monotonic() + timeout
    last: Any = None
    while True:
        done, last = pred()
        if done:
            return last
        if time.monotonic() >= deadline:
            raise HarnessError(f"timeout after {timeout:.0f}s waiting for {desc}; last observation: {last!r}"[:4000])
        time.sleep(interval)


def _aware(dt: Any) -> Any:
    if isinstance(dt, datetime) and dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def _proc_start_ticks(pid: int) -> int | None:
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    fields = text[text.rfind(")") + 2:].split()
    try:
        return int(fields[19])
    except (IndexError, ValueError):
        return None


def _proc_cmdline(pid: int) -> str:
    try:
        return Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="replace")
    except OSError:
        return ""


class ApiServer:
    """Real uvicorn (one worker) on a free loopback port, started with the core ``.venv`` interpreter."""

    def __init__(self, *, runtime: Any, paths: WorkPaths, cfg: ProjectConfig, profile: str, repo_dir: Path,
                 log_path: Path, port: int | None = None, extra_env: Mapping[str, str] | None = None) -> None:
        self.runtime = runtime
        self.paths = paths
        self.cfg = cfg
        self.profile = profile
        self.repo_dir = repo_dir
        self.log_path = log_path
        self.port = port or free_port()
        self.extra_env = dict(extra_env or {})
        self.proc: subprocess.Popen[bytes] | None = None

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}{self.cfg.api.base_path}"

    def start(self, timeout: float = 120.0) -> str:
        from transitpulse.runtime.manifest import subprocess_env

        env = subprocess_env(self.runtime, {"TP_WORKDIR": str(self.paths.root), "TP_PROFILE": self.profile,
                                            **self.extra_env})
        argv = [str(self.runtime.core_python), "-m", "uvicorn", "transitpulse.api.app:create_app", "--factory",
                "--host", "127.0.0.1", "--port", str(self.port), "--workers", "1"]
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        log = open(self.log_path, "ab")  # noqa: SIM115 - handed to the child process
        try:
            self.proc = subprocess.Popen(argv, env=env, cwd=str(self.repo_dir), stdin=subprocess.DEVNULL,
                                         stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        finally:
            log.close()

        def live() -> tuple[bool, Any]:
            if self.proc is not None and self.proc.poll() is not None:
                raise HarnessError(f"API exited with {self.proc.returncode}: {tail_file(self.log_path)}")
            try:
                r = httpx.get(f"{self.base_url}/health/live", timeout=2.0, trust_env=False)
                return r.status_code == 200, r.status_code
            except httpx.HTTPError as exc:
                return False, str(exc)

        wait_until(live, timeout, f"API /health/live on port {self.port}")
        return self.base_url

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def stop(self, timeout: float = 15.0) -> int | None:
        if self.proc is None:
            return None
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout)
        return self.proc.returncode


class StackHarness:
    """Processor lifecycle, publishing, processing barriers, Cassandra reads, faults and infra control."""

    def __init__(self, *, cfg: ProjectConfig, paths: WorkPaths, runtime: Any, profile: str, session_id: str,
                 repo_dir: Path, api_base: str | None = None, evidence_dir: Path | None = None) -> None:
        from transitpulse.runtime.manifest import subprocess_env
        from transitpulse.runtime.supervisor import Supervisor

        self.cfg = cfg
        self.paths = paths
        self.runtime = runtime
        self.profile = profile
        self.session_id = session_id
        self.repo_dir = repo_dir
        self.api_base = api_base
        self.evidence_dir = evidence_dir or (paths.reports / "integration" / session_id)
        self.sup = Supervisor(paths)
        self.env = subprocess_env(runtime)
        self._producers: dict[str, Any] = {}
        self._raw_producer: Any = None
        self._consumer: Any = None
        self._clock_seq: dict[str, int] = {}
        self._clock: dict[str, datetime] = {}
        self.processor_run_id: str | None = None
        self._http = httpx.Client(timeout=15.0, trust_env=False)

    # ------------------------------------------------------------------ lifecycle
    def close(self) -> None:
        for p in self._producers.values():
            try:
                p.close()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
        self._producers.clear()
        if self._raw_producer is not None:
            self._raw_producer.flush(10)
            self._raw_producer = None
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
        self._http.close()

    # ------------------------------------------------------------------ Cassandra
    @property
    def session(self) -> Any:
        from transitpulse.storage.cassandra import get_session

        return get_session(self.cfg)

    def query(self, cql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        rows = []
        for row in self.session.execute(cql, tuple(params)):
            d = row if isinstance(row, dict) else row._asdict()
            rows.append({k: _aware(v) for k, v in d.items()})
        return rows

    def run_row(self, run_id: str) -> dict[str, Any] | None:
        rows = self.query("SELECT * FROM run_metadata WHERE run_id = %s", (run_id,))
        return rows[0] if rows else None

    def pointer(self, run_id: str, feed: str) -> dict[str, Any] | None:
        from transitpulse.storage.pointer import read_pointer

        row = read_pointer(self.session, run_id, feed, serial=True)
        return {k: _aware(v) for k, v in row.items()} if row else None

    def manifest(self, run_id: str, feed: str, snapshot_id: str) -> dict[str, Any] | None:
        from transitpulse.storage.manifest import read_manifest

        row = read_manifest(self.session, run_id, feed, snapshot_id, serial=True)
        return {k: _aware(v) for k, v in row.items()} if row else None

    def manifests(self, run_id: str, feed: str) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM snapshot_manifest WHERE run_id = %s AND feed_type = %s", (run_id, feed))

    def quality_rows(self, run_id: str, feed: str, snapshot_id: str) -> dict[str, dict[str, Any]]:
        rows = self.query("SELECT * FROM quality_by_snapshot WHERE run_id = %s AND feed_type = %s AND snapshot_id = %s",
                          (run_id, feed, snapshot_id))
        return {r["reason_code"]: r for r in rows}

    def vehicle_rows(self, run_id: str, snapshot_id: str, route_id: str) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM vehicles_by_route_snapshot WHERE run_id = %s AND snapshot_id = %s "
                          "AND route_id = %s", (run_id, snapshot_id, route_id))

    def history_rows(self, run_id: str, vehicle_id: str, utc_dates: Iterable[Any]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for d in utc_dates:
            out.extend(self.query("SELECT * FROM vehicle_observations_by_day WHERE run_id = %s AND vehicle_id = %s "
                                  "AND utc_date = %s", (run_id, vehicle_id, d)))
        return out

    # ------------------------------------------------------------------ runs
    def config_with(self, overrides: Mapping[str, Any] | None) -> ProjectConfig:
        if not overrides:
            return self.cfg
        return load_config(self.profile, workdir=self.paths.root, overrides=dict(overrides))

    def create_run(self, *, static: StaticScope, source_type: str, initial_clock: datetime | None,
                   scenario_id: str | None = None, fixture_kind: str | None = None, seed: int | None = None,
                   recording_id: str | None = None, speed: float | None = None, label: str | None = None,
                   provenance: dict[str, Any] | None = None, overrides: Mapping[str, Any] | None = None,
                   route_ids: Sequence[str] | None = None) -> Any:
        from transitpulse.runs import create_run

        cfg = self.config_with(overrides)
        run = create_run(
            cfg, self.session, source_type=source_type, route_ids=list(route_ids or static.route_ids),
            gtfs_version=static.gtfs_version, scope_id=static.scope_id, agency_timezone=static.agency_timezone,
            static_zip_path=str(static.static_zip_path), static_scope_dir=str(static.scope_dir),
            initial_clock=initial_clock, scenario_id=scenario_id, fixture_kind=fixture_kind, seed=seed,
            recording_id=recording_id, session_id=self.session_id, speed=speed, label=label,
            provenance=provenance or {"created_by": HARNESS_COMPONENT, "session_id": self.session_id},
        )
        self._clock_seq[run.run_id] = 0
        if initial_clock is not None:
            self._clock[run.run_id] = initial_clock
        return run

    # ------------------------------------------------------------------ generic components
    def component_alive(self, component: str) -> bool:
        try:
            return bool(self.sup.is_alive(component))
        except Exception:  # noqa: BLE001 - an unknown component is simply not alive
            return False

    def component_log(self, component: str) -> Path | None:
        entry = self.sup.entry(component) or {}
        log = entry.get("stdout_log")
        return Path(log) if log else None

    def component_log_tail(self, component: str, lines: int = 120) -> str:
        return tail_file(self.component_log(component), lines)

    def start_component(self, component: str, argv: Sequence[str], *, run_id: str | None, env: Mapping[str, str],
                        ready: Callable[[], bool] | None, ready_timeout: float = 240.0) -> dict[str, Any]:
        from transitpulse.runtime.supervisor import SupervisorError

        try:
            return self.sup.start(component, list(argv), env=dict(env), scope="session", session_id=self.session_id,
                                  run_id=run_id, ready=ready, ready_timeout=ready_timeout)
        except SupervisorError as exc:
            detail = getattr(exc, "log_excerpt", None) or self.component_log_tail(component)
            raise HarnessError(f"{component} failed to start: {getattr(exc, 'code', '')} {exc}; "
                               f"hint: {getattr(exc, 'hint', None)}; log:\n{detail}") from exc

    def stop_component(self, component: str, timeout: float | None = None) -> dict[str, Any] | None:
        if self.sup.entry(component) is None:
            return None
        return self.sup.stop(component, timeout=timeout)

    def kill_component_hard(self, component: str, expect_in_cmdline: str) -> int:
        """SIGKILL the owned child process of ``component`` after verifying its identity (no pkill)."""
        entry = self.sup.entry(component) or {}
        pid = entry.get("pid")
        if not isinstance(pid, int) or pid <= 1:
            raise HarnessError(f"{component} has no recorded child pid: {entry}")
        ticks = entry.get("child_start_ticks")
        if isinstance(ticks, int) and _proc_start_ticks(pid) != ticks:
            raise HarnessError(f"pid {pid} of {component} is no longer the recorded process (start time differs)")
        if expect_in_cmdline not in _proc_cmdline(pid):
            raise HarnessError(f"pid {pid} of {component} does not run {expect_in_cmdline!r}; refusing to kill")
        os.kill(pid, signal.SIGKILL)
        return pid

    # ------------------------------------------------------------------ processor
    def _ready_file(self, run_id: str) -> Path:
        return self.paths.run_dir(run_id) / "processor_ready.json"

    def start_processor(self, run_id: str, *, fault_injection: bool = False, extra_env: Mapping[str, str] | None = None,
                        ready_timeout: float = 420.0) -> dict[str, Any]:
        from transitpulse.runtime.spark_launch import processor_argv, processor_env

        if self.processor_run_id is not None and self.component_alive("processor"):
            self.stop_processor()
        argv = processor_argv(self.runtime, self.paths, run_id, profile=self.profile, fault_injection=fault_injection)
        env = dict(processor_env(self.runtime))
        env.update(extra_env or {})
        ready_file = self._ready_file(run_id)
        before = ready_file.stat().st_mtime_ns if ready_file.exists() else None

        def ready() -> bool:
            try:
                return ready_file.stat().st_mtime_ns != before
            except FileNotFoundError:
                return False

        entry = self.start_component("processor", argv, run_id=run_id, env=env, ready=ready,
                                     ready_timeout=ready_timeout)
        self.processor_run_id = run_id
        return entry

    def processor_alive(self) -> bool:
        return self.component_alive("processor")

    def stop_processor(self, timeout: float | None = None) -> dict[str, Any] | None:
        result = self.stop_component("processor", timeout=timeout)
        self.processor_run_id = None
        return result

    def restart_processor(self, run_id: str, *, fault_injection: bool = False,
                          extra_env: Mapping[str, str] | None = None) -> dict[str, Any]:
        if self.component_alive("processor"):
            self.stop_processor()
        return self.start_processor(run_id, fault_injection=fault_injection, extra_env=extra_env)

    def wait_processor_exit(self, timeout: float) -> int | None:
        return self.sup.wait_exit("processor", timeout)

    def processor_log_tail(self, lines: int = 120) -> str:
        return self.component_log_tail("processor", lines)

    def _assert_processor_alive(self, run_id: str) -> None:
        if self.processor_run_id == run_id and not self.component_alive("processor"):
            code = self.sup.wait_exit("processor", 0.1)
            self.processor_run_id = None
            raise ProcessorExited(run_id, code, self.processor_log_tail())

    # ------------------------------------------------------------------ Kafka
    def producer(self, run_id: str) -> Any:
        from transitpulse.ingestion.kafka_producer import EnvelopeProducer

        if run_id not in self._producers:
            self._producers[run_id] = EnvelopeProducer(self.cfg, HARNESS_COMPONENT, run_id)
        return self._producers[run_id]

    def _await_ack(self, send: Callable[[Callable[..., None]], None], producer: Any, what: str,
                   timeout: float) -> Delivery:
        acks: list[Delivery] = []

        def on_ack(topic: str, partition: int, offset: int, published_at: datetime) -> None:
            acks.append(Delivery(topic, partition, offset, published_at))

        send(on_ack)
        remaining = producer.flush(timeout)
        if remaining or not acks:
            raise HarnessError(f"no broker ACK for {what} within {timeout}s (still queued: {remaining})")
        return acks[0]

    def publish_envelope(self, env: Mapping[str, Any], *, timeout: float = 60.0) -> Delivery:
        prod = self.producer(str(env["run_id"]))
        return self._await_ack(lambda cb: prod.send_envelope(dict(env), on_ack=cb), prod,
                               f"snapshot {env.get('snapshot_id')}", timeout)

    def publish_control(self, run_id: str, event: Mapping[str, Any], *, timeout: float = 30.0) -> Delivery:
        prod = self.producer(run_id)
        return self._await_ack(lambda cb: prod.send_control(dict(event), on_ack=cb), prod,
                               f"control {event.get('event_type')}", timeout)

    def current_clock(self, run_id: str) -> datetime | None:
        return self._clock.get(run_id)

    def publish_clock(self, run: Any, clock: datetime, *, event_type: str = ctl.CLOCK_TICK,
                      wait_applied: bool = True, timeout: float = 120.0) -> int:
        """Clock event with the next ``clock_sequence``; ACKed, then (by default) waited until applied."""
        seq = self._clock_seq.get(run.run_id, 0) + 1
        extra: dict[str, Any] = {}
        if event_type == ctl.RUN_STARTED:
            extra = {"clock_mode": run.clock_mode, "timeline_start": iso_utc(clock), "recording_id": run.recording_id,
                     "scenario_id": run.scenario_id, "scope_id": run.scope_id, "gtfs_version": run.gtfs_version}
        event = ctl.clock_event(event_type, run_id=run.run_id, source_type=run.source_type, clock_sequence=seq,
                                source_clock=clock, component=HARNESS_COMPONENT, version=__version__,
                                speed=run.speed, run_state=None if event_type != ctl.CLOCK_TICK else "RUNNING",
                                extra=extra or None)
        self.publish_control(run.run_id, event)
        self._clock_seq[run.run_id] = seq
        prev = self._clock.get(run.run_id)
        self._clock[run.run_id] = clock if prev is None or clock > prev else prev
        if wait_applied:
            self.wait_clock_applied(run.run_id, seq, timeout=timeout)
        return seq

    def begin_run(self, run: Any, clock: datetime | None = None) -> int:
        start = clock or run.initial_clock or utcnow()
        return self.publish_clock(run, start, event_type=ctl.RUN_STARTED)

    def wait_clock_applied(self, run_id: str, seq: int, *, timeout: float = 120.0) -> dict[str, Any]:
        def applied() -> tuple[bool, Any]:
            self._assert_processor_alive(run_id)
            row = self.run_row(run_id) or {}
            cur = row.get("clock_sequence")
            return isinstance(cur, int) and cur >= seq, {"clock_sequence": cur, "want": seq,
                                                          "application_clock": row.get("application_clock")}

        return wait_until(applied, timeout, f"clock_sequence >= {seq} applied for run {run_id}", interval=0.5)

    def publish_raw_bytes(self, topic: str, key: bytes, value: bytes, *, timeout: float = 60.0) -> Delivery:
        """Arbitrary bytes on a topic (fault scenarios); producer limits as configured for the project."""
        from confluent_kafka import Producer

        if self._raw_producer is None:
            p = self.cfg.kafka.producer
            self._raw_producer = Producer({
                "bootstrap.servers": self.cfg.kafka.bootstrap_servers, "client.id": f"tp-harness-raw-{uuid.uuid4().hex[:6]}",
                "enable.idempotence": True, "acks": "all", "compression.type": "none",
                "message.max.bytes": int(p.message_max_bytes), "linger.ms": 5,
            })
        result: dict[str, Any] = {}

        def cb(err: Any, msg: Any) -> None:
            result["err"] = err
            if err is None:
                result["delivery"] = Delivery(msg.topic(), msg.partition(), msg.offset(), utcnow())

        self._raw_producer.produce(topic, value=value, key=key, on_delivery=cb)
        self._raw_producer.flush(timeout)
        if result.get("err") is not None or "delivery" not in result:
            raise HarnessError(f"raw produce to {topic} failed: {result.get('err')}")
        return result["delivery"]

    def _wm_consumer(self) -> Any:
        from confluent_kafka import Consumer

        if self._consumer is None:
            self._consumer = Consumer({"bootstrap.servers": self.cfg.kafka.bootstrap_servers,
                                       "group.id": f"tp-harness-wm-{uuid.uuid4().hex[:8]}",
                                       "enable.auto.commit": False})
        return self._consumer

    def partitions(self, topic: str) -> list[int]:
        md = self._wm_consumer().list_topics(topic, timeout=15)
        tmd = md.topics.get(topic)
        if tmd is None or tmd.error is not None:
            raise HarnessError(f"topic {topic} is not available: {getattr(tmd, 'error', 'missing')}")
        return sorted(tmd.partitions)

    def watermarks(self, topic: str, partition: int) -> tuple[int, int]:
        from confluent_kafka import TopicPartition

        res = self._wm_consumer().get_watermark_offsets(TopicPartition(topic, partition), timeout=15, cached=False)
        if res is None:
            raise HarnessError(f"watermark request for {topic}[{partition}] timed out")
        return int(res[0]), int(res[1])

    def high_watermarks(self, topics: Iterable[str]) -> dict[tuple[str, int], int]:
        out: dict[tuple[str, int], int] = {}
        for t in topics:
            for p in self.partitions(t):
                out[(t, p)] = self.watermarks(t, p)[1]
        return out

    def wait_caught_up(self, run_id: str, *, topics: Iterable[str] | None = None, timeout: float = 240.0) -> dict:
        """Wait until the processor's committed progress covers every message produced so far."""
        topics = tuple(topics or (*SNAPSHOT_TOPICS, CONTROL))
        targets = {tp: high for tp, high in self.high_watermarks(topics).items() if high > 0}
        progress = self.paths.query_progress_log(run_id)

        def caught_up() -> tuple[bool, Any]:
            self._assert_processor_alive(run_id)
            ends = latest_end_offsets(progress)
            lag = {f"{t}[{p}]": high - ends.get((t, p), 0) for (t, p), high in targets.items()
                   if ends.get((t, p), 0) < high}
            return not lag, lag

        wait_until(caught_up, timeout, f"processor progress of run {run_id} to reach {targets}")
        return {f"{t}[{p}]": h for (t, p), h in targets.items()}

    def wait_offset_processed(self, run_id: str, delivery: Delivery, *, timeout: float = 240.0) -> None:
        progress = self.paths.query_progress_log(run_id)
        key = (delivery.topic, delivery.partition)

        def done() -> tuple[bool, Any]:
            self._assert_processor_alive(run_id)
            end = latest_end_offsets(progress).get(key)
            return end is not None and end > delivery.offset, end

        wait_until(done, timeout, f"{delivery.topic}[{delivery.partition}]@{delivery.offset} processed")

    def consume_range(self, topic: str, partition: int, start: int, end: int, *, timeout: float = 120.0,
                      key_prefix: bytes | None = None) -> list[dict[str, Any]]:
        """Messages with ``start <= offset < end`` (optionally filtered by key prefix)."""
        from confluent_kafka import Consumer, KafkaError, TopicPartition

        out: list[dict[str, Any]] = []
        if end <= start:
            return out
        c = Consumer({"bootstrap.servers": self.cfg.kafka.bootstrap_servers,
                      "group.id": f"tp-harness-scan-{uuid.uuid4().hex[:8]}", "enable.auto.commit": False,
                      "auto.offset.reset": "earliest", "max.partition.fetch.bytes": 8388608})
        try:
            c.assign([TopicPartition(topic, partition, start)])
            deadline = time.monotonic() + timeout
            last = start - 1
            while last < end - 1 and time.monotonic() < deadline:
                msg = c.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise HarnessError(f"consume {topic}[{partition}] failed: {msg.error()}")
                last = msg.offset()
                if last >= end:
                    break
                key = msg.key() or b""
                if key_prefix is not None and not key.startswith(key_prefix):
                    continue
                out.append({"offset": last, "key": key, "value": msg.value() or b"",
                            "headers": dict(msg.headers() or []),
                            "timestamp_ms": msg.timestamp()[1]})
            if last < end - 1:
                raise HarnessError(f"consume {topic}[{partition}] stopped at {last}, wanted up to {end - 1}")
        finally:
            c.close()
        return out

    def run_messages(self, run_id: str, feed_or_topic: str, start: int, end: int,
                     partition: int = 0) -> list[dict[str, Any]]:
        from transitpulse.contracts.topics import TOPIC_BY_FEED

        topic = TOPIC_BY_FEED.get(feed_or_topic, feed_or_topic)
        msgs = self.consume_range(topic, partition, start, end, key_prefix=f"{run_id}|".encode())
        for m in msgs:
            try:
                m["json"] = json.loads(m["value"])
            except ValueError:
                m["json"] = None
        return msgs

    # ------------------------------------------------------------------ waits on Cassandra
    def wait_pointer(self, run_id: str, feed: str, snapshot_id: str, *, timeout: float = 240.0) -> dict[str, Any]:
        def at() -> tuple[bool, Any]:
            self._assert_processor_alive(run_id)
            row = self.pointer(run_id, feed)
            return bool(row and row.get("snapshot_id") == snapshot_id), (row or {}).get("snapshot_id")

        wait_until(at, timeout, f"pointer {run_id}/{feed} == {snapshot_id}")
        return self.pointer(run_id, feed) or {}

    def wait_manifest(self, run_id: str, feed: str, snapshot_id: str, pred: Callable[[dict[str, Any]], bool],
                      desc: str, *, timeout: float = 240.0) -> dict[str, Any]:
        def ok() -> tuple[bool, Any]:
            self._assert_processor_alive(run_id)
            row = self.manifest(run_id, feed, snapshot_id)
            return bool(row and pred(row)), {k: (row or {}).get(k) for k in ("status", "publish_outcome")}

        wait_until(ok, timeout, f"manifest {feed}/{snapshot_id} {desc}")
        return self.manifest(run_id, feed, snapshot_id) or {}

    # ------------------------------------------------------------------ faults
    def arm_fault(self, run_id: str, point: str, match: Mapping[str, Any]) -> Path:
        from transitpulse.processing.faults import arm_fault

        return arm_fault(self.paths, run_id, point, match=dict(match), fault_id=f"f-{uuid.uuid4().hex[:10]}")

    def fired_faults(self, run_id: str) -> list[Path]:
        d = self.paths.fault_dir(run_id)
        return sorted(d.glob("fired-*.json")) if d.is_dir() else []

    # ------------------------------------------------------------------ infra (owned processes only)
    def infra(self, *args: str, timeout: float = 900.0, check: bool = True) -> dict[str, Any]:
        argv = [str(self.runtime.core_python), str(self.repo_dir / "scripts" / "infra.py"), *args,
                "--profile", self.profile, "--workdir", str(self.paths.root)]
        return run_json_cli(argv, env=self.env, cwd=self.repo_dir, timeout=timeout, check=check)

    def stop_cassandra(self, timeout: float = 180.0) -> dict[str, Any] | None:
        return self.sup.stop("cassandra", timeout=timeout)

    def start_cassandra(self, timeout: float = 600.0) -> dict[str, Any]:
        return self.infra("start", "--components", "cassandra", timeout=timeout)

    # ------------------------------------------------------------------ API
    def api(self, path: str, **params: Any) -> httpx.Response:
        if not self.api_base:
            raise HarnessError("no API server configured for this harness")
        q = {k: v for k, v in params.items() if v is not None}
        return self._http.get(f"{self.api_base}/{path.lstrip('/')}", params=q)

    # ------------------------------------------------------------------ metrics / evidence
    def processor_metric(self, name: str, **labels: str) -> float | None:
        from transitpulse.verify.progress import prom_sum

        return prom_sum(read_prom_file(self.paths.metrics_dir / "processor.prom"), name, **labels)

    def write_evidence(self, name: str, doc: Any) -> Path:
        from transitpulse.verify.fixture import atomic_write_json

        path = self.evidence_dir / name
        atomic_write_json(path, doc)
        return path


def now_iso() -> str:
    return iso_utc(utcnow())
