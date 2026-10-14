"""Replay / synthetic publisher (design E §3.2-3.3, D-E12..D-E15, D-PUB-06, spec §15.1).

One timeline drives every feed. The source clock is advanced ONLY by control events on
``mbta.control.v1`` (``run_started``, ``clock_tick``, ``run_paused``, ``run_resumed``, ``run_finished``),
each with a strictly increasing ``clock_sequence``:

* a ``clock_tick`` is emitted before each item whose time is later than the clock (at the item's time)
  and every ``replay.tick_interval_seconds`` of wall time while waiting for the next item, so the clock
  keeps moving through gaps (staleness becomes observable);
* every clock event is flushed (broker ACK) before the snapshot that follows it is produced;
* late items never move the clock backwards; the clock is never derived from prediction times;
* pause freezes the source clock while wall-clock heartbeats continue; finish keeps the final clock;
* ``publisher_state.json`` is persisted after every item and clock event (the next ``clock_sequence`` is
  reserved on disk BEFORE the event is sent), so a restarted publisher continues the sequence and resumes
  at ``next_index`` (at-least-once: an item in flight during a crash is re-sent; downstream is
  idempotent on ``snapshot_id``).

Step mode (``--step-mode``): nothing is published until a ``step`` command; each ``step`` publishes items
up to and including the next ``barrier`` (no wall pacing; ticks at item times only), then the publisher
waits in ``STEP_WAIT`` with a frozen clock. On a recording without barriers each ``step`` publishes one
item. A ``step`` after the last item finishes the run.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from transitpulse import __version__
from transitpulse.config import ProjectConfig
from transitpulse.contracts import control as ctl
from transitpulse.contracts.dlq import dlq_record
from transitpulse.contracts.enums import FEED_ABBR, QUALITY_REASONS, SourceType
from transitpulse.contracts.envelope import envelope_size, iso_utc, kafka_key, parse_iso_utc, utcnow
from transitpulse.contracts.ids import H, K, error_id_message, sha256_hex
from transitpulse.contracts.ids import capture_id as make_capture_id
from transitpulse.contracts.topics import CONTROL, DLQ, TOPIC_BY_FEED
from transitpulse.obs.logging import get_logger, log_event
from transitpulse.obs.metrics import ComponentMetrics
from transitpulse.replay import control as pc
from transitpulse.replay.envelopes import PAYLOAD_TOO_LARGE, EnvelopeOutcome, Enveloper, RawLocation
from transitpulse.replay.fileio import append_jsonl, atomic_write_json
from transitpulse.replay.timeline import (
    ARM_FAULT,
    BARRIER,
    CAPTURE,
    FETCH_EVENT,
    MALFORMED_ENVELOPE,
    REPUBLISH,
    TICK,
    TimelineItem,
)
from transitpulse.runs import RunRecord
from transitpulse.runtime.paths import safe_component

COMPONENT = "publisher"
PUBLISHER_CLOCK_FILE = "publisher_clock.json"
VEHICLE_POSITIONS = "vehicle_positions"
SOURCE_CLOCK_TYPES = frozenset({SourceType.REPLAY.value, SourceType.SYNTHETIC.value})

_log = get_logger("transitpulse.replay.publisher")

# Local send failures that justify another attempt (queue full, transient client errors). The collector's
# PayloadTooLarge is NOT one of them and propagates unchanged.
_RETRYABLE_SEND_ERRORS: tuple[type[BaseException], ...] = (BufferError,)
try:  # pragma: no cover - confluent-kafka is a pinned dependency; the guard keeps fakes usable without it
    from confluent_kafka import KafkaException as _KafkaException

    _RETRYABLE_SEND_ERRORS = (BufferError, _KafkaException)
except ImportError:  # pragma: no cover
    pass


# --------------------------------------------------------------------------------------------------
# Collaborator protocols
# --------------------------------------------------------------------------------------------------

AckCallback = Callable[[str, int, int, datetime], None]
ErrorCallback = Callable[[Any], None]


class Producer(Protocol):
    """Subset of ``ingestion.kafka_producer.EnvelopeProducer`` used by the publisher.

    ``on_ack`` is called only for a broker ACK; ``on_error`` for a failed delivery report.
    """

    def send_envelope(self, env: Any, *, on_ack: AckCallback | None = None,
                      on_error: ErrorCallback | None = None) -> None: ...

    def send_control(self, event: dict, *, on_ack: AckCallback | None = None,
                     on_error: ErrorCallback | None = None) -> None: ...

    def send_dlq(self, record: dict) -> None: ...

    def send_raw(self, topic: str, key: bytes, value: bytes) -> None: ...

    def flush(self, timeout: float = 30.0) -> int: ...


class ControlSource(Protocol):
    def poll(self) -> dict | None: ...


# --------------------------------------------------------------------------------------------------
# Options, errors, results
# --------------------------------------------------------------------------------------------------

class PublisherError(RuntimeError):
    """Fatal publisher error; ``exit_code`` follows the CLI convention (interfaces global rules)."""

    def __init__(self, code: str, message: str, hint: str | None = None, exit_code: int = 1) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint
        self.exit_code = exit_code


class CommandRejected(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class PublisherOptions:
    speed: float = 1.0
    step_mode: bool = False
    duration_seconds: float | None = None
    tick_interval_seconds: float = 1.0
    heartbeat_seconds: float = 5.0
    control_poll_seconds: float = 0.2
    flush_timeout_seconds: float = 30.0
    max_send_attempts: int = 3
    retry_backoff_seconds: float = 1.0
    dlq_excerpt_max_chars: int = 2048
    pipeline_version: str = "p1"
    max_speed: float = 100.0

    def __post_init__(self) -> None:
        if not (math.isfinite(self.speed) and 0 < self.speed <= self.max_speed):
            raise ValueError(f"speed must be in (0, {self.max_speed}], got {self.speed}")
        if self.tick_interval_seconds <= 0 or self.heartbeat_seconds <= 0 or self.control_poll_seconds <= 0:
            raise ValueError("tick, heartbeat and control poll intervals must be > 0")
        if self.duration_seconds is not None and self.duration_seconds <= 0:
            raise ValueError("duration_seconds must be > 0")
        if self.max_send_attempts < 1:
            raise ValueError("max_send_attempts must be >= 1")

    @classmethod
    def from_config(
        cls,
        cfg: ProjectConfig,
        *,
        speed: float = 1.0,
        step_mode: bool = False,
        duration_seconds: float | None = None,
    ) -> PublisherOptions:
        return cls(
            speed=float(speed),
            step_mode=step_mode,
            duration_seconds=duration_seconds,
            tick_interval_seconds=cfg.replay.tick_interval_seconds,
            heartbeat_seconds=cfg.replay.heartbeat_seconds,
            control_poll_seconds=cfg.replay.control_poll_seconds,
            dlq_excerpt_max_chars=cfg.kafka.dlq_excerpt_max_chars,
            pipeline_version=cfg.project.pipeline_version,
            max_speed=cfg.replay.max_speed,
        )


@dataclass(frozen=True)
class PublisherResult:
    state: str
    finished_reason: str | None
    clock_sequence: int
    source_clock: datetime
    items_published: int
    items_total: int
    messages_published: int
    restarted: bool
    already_finished: bool

    def to_json(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "finished_reason": self.finished_reason,
            "clock_sequence": self.clock_sequence,
            "source_clock": iso_utc(self.source_clock),
            "items_published": self.items_published,
            "items_total": self.items_total,
            "messages_published": self.messages_published,
            "restarted": self.restarted,
            "already_finished": self.already_finished,
        }


@dataclass
class _FeedStats:
    ok: int = 0
    not_modified: int = 0
    failed: int = 0
    rejected_by_reason: dict[str, int] = field(default_factory=dict)
    consecutive_failures: int = 0
    last_success_fetched_at: str | None = None
    last_feed_timestamp: int | None = None


@dataclass(frozen=True)
class _Published:
    feed_type: str
    seq: int | None                    # timeline seq; None for injected snapshots
    item: TimelineItem | None          # timeline capture item (re-readable from the recording)
    body: bytes | None                 # kept only for injected captures
    at: datetime                       # original_fetched_at
    outcome: EnvelopeOutcome
    capture_id: str
    source_url: str | None

    @property
    def snapshot_id(self) -> str:
        return str((self.outcome.envelope or {}).get("snapshot_id"))


def _floor_ms(dt: datetime) -> datetime:
    dt = dt.astimezone(UTC)
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)


# --------------------------------------------------------------------------------------------------
# Publisher
# --------------------------------------------------------------------------------------------------

class Publisher:
    def __init__(
        self,
        *,
        run: RunRecord,
        items: Sequence[TimelineItem],
        start_clock: datetime,
        session_dir: Path,
        producer: Producer,
        enveloper: Enveloper,
        options: PublisherOptions,
        fault_dir: Path,
        run_dir: Path | None = None,
        session_id: str | None = None,
        recording_id: str | None = None,
        manifest_sha256: str | None = None,
        topics: Mapping[str, str] | None = None,
        control_topic: str = CONTROL,
        dlq_topic: str = DLQ,
        metrics: ComponentMetrics | None = None,
        control: ControlSource | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        wall_now: Callable[[], datetime] = utcnow,
    ) -> None:
        if run.source_type not in SOURCE_CLOCK_TYPES:
            raise PublisherError(
                "unsupported_mode",
                f"run {run.run_id} has source_type={run.source_type}; the publisher only drives replay/synthetic runs",
                "Live runs are fed by the collector (python -m transitpulse.ingestion collect).",
                exit_code=6,
            )
        if not run.is_source_clock:
            raise PublisherError("unsupported_mode", f"run {run.run_id} is not a source-clock run", exit_code=6)
        self.run_record = run
        self.items: list[TimelineItem] = list(items)
        self.options = options
        self.session_dir = Path(session_dir)
        self.session_id = session_id or self.session_dir.name
        self.fault_dir = Path(fault_dir)
        self.run_dir = Path(run_dir) if run_dir is not None else None
        self.recording_id = recording_id
        self.manifest_sha256 = manifest_sha256
        self._producer = producer
        self._enveloper = enveloper
        self._topics = dict(topics or TOPIC_BY_FEED)
        self._control_topic = control_topic
        self._dlq_topic = dlq_topic
        self._control = control
        self._mono = monotonic
        self._sleep = sleep
        self._wall = wall_now
        self._start_clock = _floor_ms(start_clock)
        self._by_seq = {it.seq: it for it in self.items}
        self._has_barriers = any(it.kind == BARRIER for it in self.items)
        self._lock = threading.Lock()

        # clock / progress state
        self._clock = self._start_clock
        self._seq = 0
        self._next_index = 0
        self._items_published = 0
        self._messages_published = 0
        self._last_barrier: str | None = None
        self._state = pc.STATE_RUNNING
        self._paused = False
        self._step_credit = 0
        self._stop_reason: str | None = None
        self._stop_extra: dict[str, Any] = {}
        self._signal_reason: str | None = None
        self._finished_reason: str | None = None
        self._applied_request_id: str | None = None
        self._last_command: dict[str, Any] | None = None
        self._restarted = False
        self._already_finished = False
        self._error: dict[str, Any] | None = None
        self._lag = 0.0

        # timing
        self._anchor_mono = 0.0
        self._anchor_clock = self._clock
        self._last_tick_mono = 0.0
        self._last_heartbeat_mono = -math.inf
        self._last_heartbeat_wall: datetime | None = None
        self._last_control_poll = -math.inf
        self._deadline: float | None = None

        self._feed_stats: dict[str, _FeedStats] = {f: _FeedStats() for f in TOPIC_BY_FEED}
        self._history: dict[str, deque[_Published]] = {f: deque(maxlen=8) for f in TOPIC_BY_FEED}
        self._history_rebuilt: set[str] = set()
        # timeline seqs of captures whose snapshot was actually published (ACKed); persisted for restarts
        self._published_seqs: set[int] = set()
        self._published_seqs_known = True

        m = metrics or ComponentMetrics(COMPONENT)
        self.metrics = m
        self._m_messages = m.counter("messages_total", "Kafka messages produced by the publisher", ("topic",))
        self._m_bytes = m.counter("bytes_total", "Serialized bytes produced by the publisher", ("topic",))
        self._m_candidates = m.counter("candidates_total", "Normalized candidate records published", ("feed",))
        self._m_items = m.counter("items_total", "Timeline items processed", ("kind",))
        self._m_clock_seq = m.gauge("clock_sequence", "Last emitted clock_sequence")
        self._m_source_clock = m.gauge("source_clock_timestamp_seconds", "Current source clock (epoch seconds)")
        self._m_paused = m.gauge("paused", "1 while the publisher is paused")
        self._m_lag = m.gauge("schedule_lag_seconds", "Wall seconds the publisher is behind its schedule")

    # ------------------------------------------------------------------ public API
    @property
    def state(self) -> str:
        return self._state

    @property
    def clock(self) -> datetime:
        return self._clock

    @property
    def clock_sequence(self) -> int:
        return self._seq

    def request_stop(self, reason: str = "signal") -> None:
        """Ask the main loop to finish (safe to call from a signal handler)."""
        self._signal_reason = reason

    def run(self) -> PublisherResult:
        self._restore()
        if self._already_finished:
            log_event(_log, "timeline", "skipped", "publisher session already FINISHED; nothing to do",
                      finished_reason=self._finished_reason)
            return self._result()
        now = self._mono()
        if self.options.duration_seconds is not None:
            self._deadline = now + float(self.options.duration_seconds)
        try:
            self._emit_start()
            while self._stop_reason is None:
                item = self.items[self._next_index] if self._next_index < len(self.items) else None
                if item is None and not self.options.step_mode:
                    self._stop_reason = "completed"
                    break
                if not self._wait_for(item):
                    break
                if item is None:
                    self._stop_reason = "completed"
                    break
                self._process(item)
                self._next_index += 1
                self._items_published += 1
                if self.options.step_mode and not self._has_barriers and self._step_credit > 0:
                    self._step_credit -= 1          # no barriers: one step == one item
                self._persist()
            self._finish(self._stop_reason or "completed")
        except BaseException as exc:
            self._fail(exc)
            raise
        return self._result()

    # ------------------------------------------------------------------ restore / persist
    def _restore(self) -> None:
        prev = pc.read_publisher_state(self.session_dir)
        if prev is not None:
            prev_run = prev.get("run_id")
            if prev_run is not None and prev_run != self.run_record.run_id:
                raise PublisherError(
                    "session_conflict",
                    f"session {self.session_id} belongs to run {prev_run}, not {self.run_record.run_id}",
                    "Use a new session id for a new run.",
                    exit_code=4,
                )
            prev_manifest = prev.get("manifest_sha256")
            if self.manifest_sha256 and prev_manifest and prev_manifest != self.manifest_sha256:
                raise PublisherError(
                    "session_conflict",
                    f"session {self.session_id} was started with a different recording ({prev_manifest})",
                    "A run replays exactly one recording; create a new run for another recording.",
                    exit_code=4,
                )
            self._restarted = True
            self._seq = int(prev.get("clock_sequence") or 0)
            if prev.get("source_clock"):
                self._clock = _floor_ms(parse_iso_utc(prev["source_clock"]))
            self._next_index = max(0, min(int(prev.get("next_index") or 0), len(self.items)))
            self._items_published = int(prev.get("items_published") or self._next_index)
            self._messages_published = int(prev.get("messages_published") or 0)
            self._last_barrier = prev.get("last_barrier")
            self._applied_request_id = prev.get("applied_request_id")
            self._last_command = prev.get("last_command")
            self._paused = prev.get("state") == pc.STATE_PAUSED
            seqs = prev.get("published_capture_seqs")
            if isinstance(seqs, list):
                self._published_seqs = {int(s) for s in seqs}
            else:  # state written without the list: fall back to the re-parse result
                self._published_seqs_known = False
            if prev.get("state") == pc.STATE_FINISHED:
                self._already_finished = True
                self._state = pc.STATE_FINISHED
                self._finished_reason = prev.get("finished_reason")
        hwm = self._read_run_clock()
        if hwm is not None:
            self._seq = max(self._seq, int(hwm.get("clock_sequence") or 0))
            if hwm.get("source_clock"):
                hwm_clock = _floor_ms(parse_iso_utc(hwm["source_clock"]))
                if hwm_clock > self._clock:
                    self._clock = hwm_clock
        if self._control is None:
            self._control = pc.FileControlChannel(self.session_dir, ignore_existing=True,
                                                  last_applied=self._applied_request_id)
        if self._restarted:
            log_event(_log, "timeline", "reused", "publisher restarted from persisted state",
                      next_index=self._next_index, clock_sequence=self._seq, source_clock=iso_utc(self._clock))

    def _read_run_clock(self) -> dict | None:
        if self.run_dir is None:
            return None
        path = self.run_dir / PUBLISHER_CLOCK_FILE
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        except (FileNotFoundError, OSError, ValueError):
            return None
        if not isinstance(data, dict) or data.get("run_id") != self.run_record.run_id:
            return None
        return data

    def _state_doc(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "applied_request_id": self._applied_request_id,
            "clock_sequence": self._seq,
            "source_clock": iso_utc(self._clock),
            "wall_heartbeat_at": iso_utc(self._last_heartbeat_wall) if self._last_heartbeat_wall else None,
            "items_published": self._items_published,
            "items_total": len(self.items),
            "next_index": self._next_index,
            "last_barrier": self._last_barrier,
            "run_id": self.run_record.run_id,
            "session_id": self.session_id,
            "source_type": self.run_record.source_type,
            "recording_id": self.recording_id,
            "manifest_sha256": self.manifest_sha256,
            "speed": self.options.speed,
            "step_mode": self.options.step_mode,
            "step_credit": self._step_credit,
            "paused": self._paused,
            "finished_reason": self._finished_reason,
            "messages_published": self._messages_published,
            "published_capture_seqs": sorted(self._published_seqs),
            "schedule_lag_seconds": round(self._lag, 3),
            "last_command": self._last_command,
            "error": self._error,
            "updated_at": iso_utc(self._wall()),
            "pid": os.getpid(),
        }

    def _persist(self, *, fsync: bool = True) -> None:
        pc.write_publisher_state(self.session_dir, self._state_doc(), fsync=fsync)

    def _persist_run_clock(self) -> None:
        if self.run_dir is None:
            return
        atomic_write_json(self.run_dir / PUBLISHER_CLOCK_FILE, {
            "run_id": self.run_record.run_id,
            "clock_sequence": self._seq,
            "source_clock": iso_utc(self._clock),
            "session_id": self.session_id,
            "updated_at": iso_utc(self._wall()),
        })

    def _set_state(self, state: str) -> None:
        if state != self._state:
            previous = self._state
            self._state = state
            self._persist(fsync=False)
            log_event(_log, "timeline", "ok", f"publisher state {previous} -> {state}",
                      previous_state=previous, publisher_state=state, next_index=self._next_index)

    # ------------------------------------------------------------------ result / failure
    def _result(self) -> PublisherResult:
        return PublisherResult(
            state=self._state,
            finished_reason=self._finished_reason,
            clock_sequence=self._seq,
            source_clock=self._clock,
            items_published=self._items_published,
            items_total=len(self.items),
            messages_published=self._messages_published,
            restarted=self._restarted,
            already_finished=self._already_finished,
        )

    def _fail(self, exc: BaseException) -> None:
        code = exc.code if isinstance(exc, PublisherError) else type(exc).__name__
        self._error = {"code": code, "message": str(exc)[:1000], "at": iso_utc(self._wall())}
        self._state = pc.STATE_FAILED
        try:
            self._persist()
        except OSError:
            pass
        log_event(_log, "publish", "failed", f"publisher failed: {exc}", level=logging.ERROR,
                  reason=code, next_index=self._next_index, clock_sequence=self._seq)

    # ------------------------------------------------------------------ Kafka helpers
    def _log_message(self, line: dict[str, Any]) -> None:
        with self._lock:
            append_jsonl(self.session_dir / pc.PUBLISH_LOG_FILE, line)

    def _flush(self, what: str) -> None:
        remaining = self._producer.flush(self.options.flush_timeout_seconds)
        if remaining:
            raise PublisherError(
                "kafka_flush_timeout",
                f"{remaining} message(s) still queued after {self.options.flush_timeout_seconds}s ({what})",
                "Check the Kafka broker (scripts/infra.py status); the publisher can be restarted and resumes.",
            )

    def _produce_acked(self, send: Callable[..., None], what: str) -> tuple[dict[str, Any], int]:
        """Produce via ``send(on_ack=..., on_error=...)`` until the broker ACKs it; (ack, attempts).

        A delivery report with an error, no report after ``flush``, or a retryable local send failure
        (``BufferError``/``KafkaException``) counts as a failed attempt: the message is re-sent (idempotent
        downstream: same snapshot_id / same clock_sequence) up to ``max_send_attempts``, then
        :class:`PublisherError` is raised so nothing that depends on it is produced.

        Every copy carries the same payload, so an ACK of ANY attempt's copy is a delivery (a copy still queued
        after a flush timeout may be ACKed later). Errors are collected per attempt: a late error of an earlier
        copy never vetoes a copy that was ACKed. ``PayloadTooLarge`` and other errors propagate unchanged.
        """
        ack: dict[str, Any] = {}               # shared by all attempts: first ACK of any copy wins
        attempts = 0
        last_error: str | None = None
        for attempts in range(1, self.options.max_send_attempts + 1):
            if ack:                            # an earlier copy was ACKed while backing off
                return dict(ack), attempts - 1
            errors: list[Any] = []             # this attempt's copy only

            def on_ack(t: str, partition: int, offset: int, published_at: datetime, _ack: dict = ack) -> None:
                if not _ack:
                    _ack.update(topic=t, partition=partition, offset=offset, published_at=published_at)

            def on_error(err: Any, _errors: list = errors) -> None:
                _errors.append(err)

            remaining = 0
            try:
                send(on_ack=on_ack, on_error=on_error)
                remaining = self._producer.flush(self.options.flush_timeout_seconds)
            except _RETRYABLE_SEND_ERRORS as exc:
                last_error = f"{type(exc).__name__}: {exc}"[:300]
                remaining = -1
            if remaining == 0:
                grace = 0
                while not ack and not errors and grace < 20:
                    # a delivery report served by another thread may land just after flush() returns
                    self._sleep(0.01)
                    grace += 1
            if ack:
                return dict(ack), attempts
            if errors:
                last_error = str(errors[0])[:300]
            log_event(_log, "publish", "retry", f"{what} not acknowledged by the broker", level=logging.WARNING,
                      attempt=attempts, queued=remaining, reason=last_error or "no_report")
            if attempts < self.options.max_send_attempts:
                self._sleep(self.options.retry_backoff_seconds * attempts)
        if ack:
            return dict(ack), attempts
        raise PublisherError(
            "kafka_delivery_failed",
            f"{what} was not acknowledged after {attempts} attempt(s)"
            + (f" (last error: {last_error})" if last_error else ""),
            "Check the Kafka broker (scripts/infra.py status); restart the publisher to resume.",
        )

    def _send_control(self, event: dict[str, Any], *, acked: bool) -> None:
        """Clock events (``acked=True``) must be ACKed before anything that follows them is produced;
        status/heartbeat events are fire-and-forget (cumulative, repaired by the next one)."""
        ack: dict[str, Any] = {}
        attempts = 1
        if acked:
            what = f"control event {event.get('event_type')} seq={event.get('clock_sequence')}"
            ack, attempts = self._produce_acked(lambda **kw: self._producer.send_control(event, **kw), what)
        else:
            self._producer.send_control(event)
        size = len(json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        self._messages_published += 1
        self._m_messages.labels(topic=self._control_topic).inc()
        self._m_bytes.labels(topic=self._control_topic).inc(size)
        published_at = ack.get("published_at")
        self._log_message({
            "published_at": iso_utc(published_at) if isinstance(published_at, datetime) else event.get("emitted_at"),
            "kind": "control",
            "topic": self._control_topic,
            "partition": ack.get("partition"),
            "offset": ack.get("offset"),
            "event_type": event.get("event_type"),
            "clock_sequence": event.get("clock_sequence"),
            "source_clock": event.get("source_clock"),
            "bytes": size,
            "candidates": 0,
            "acked": bool(ack),
            "attempts": attempts,
        })

    def _processor_run_state(self) -> str:
        if self._state == pc.STATE_FINISHED:
            return "FINISHED"
        if self._state == pc.STATE_FAILED:
            return "FAILED"
        return "PAUSED" if self._paused else "RUNNING"

    def _emit_clock(self, event_type: str, clock: datetime, *, reason: str | None = None,
                    extra: dict[str, Any] | None = None, run_state: str | None = None) -> None:
        clock = _floor_ms(clock)
        if clock < self._clock:
            raise AssertionError(f"clock regression {iso_utc(clock)} < {iso_utc(self._clock)}")
        self._seq += 1
        self._clock = clock
        # Reserve the sequence on disk first: a crash after this point never reuses it.
        self._persist()
        self._persist_run_clock()
        event = ctl.clock_event(
            event_type,
            run_id=self.run_record.run_id,
            source_type=self.run_record.source_type,
            clock_sequence=self._seq,
            source_clock=clock,
            component=COMPONENT,
            version=__version__,
            speed=self.options.speed,
            run_state=run_state,
            reason=reason,
            extra=extra,
        )
        self._send_control(event, acked=True)  # broker ACK before the next snapshot is produced (D-E13)
        self._m_clock_seq.set(self._seq)
        self._m_source_clock.set(clock.timestamp())
        log_event(_log, "tick", "ok", f"{event_type} seq={self._seq} clock={iso_utc(clock)}",
                  level=logging.DEBUG if (extra or {}).get("tick_reason") == "interval" else logging.INFO,
                  event_type=event_type, clock_sequence=self._seq, source_clock=iso_utc(clock), reason=reason)

    def _tick(self, clock: datetime, why: str) -> None:
        clock = _floor_ms(clock)
        if clock < self._clock:
            log_event(_log, "tick", "skipped", "tick earlier than the current clock ignored (clock never goes back)",
                      requested=iso_utc(clock), source_clock=iso_utc(self._clock))
            return
        self._emit_clock(ctl.CLOCK_TICK, clock, extra={"tick_reason": why},
                         run_state="PAUSED" if self._paused else "RUNNING")
        self._last_tick_mono = self._mono()

    def _heartbeat(self) -> None:
        now_wall = self._wall()
        self._last_heartbeat_mono = self._mono()
        self._last_heartbeat_wall = now_wall
        event = ctl.status_event(
            ctl.PUBLISHER_HEARTBEAT,
            run_id=self.run_record.run_id,
            source_type=self.run_record.source_type,
            component=COMPONENT,
            version=__version__,
            fields={
                "run_state": self._processor_run_state(),
                "publisher_state": self._state,
                "clock_sequence": self._seq,
                "source_clock": iso_utc(self._clock),
                "items_published": self._items_published,
                "items_total": len(self.items),
                "schedule_lag_seconds": round(self._lag, 3),
                "wall_heartbeat_at": iso_utc(now_wall),
                "session_id": self.session_id,
                "speed": self.options.speed,
                "step_mode": self.options.step_mode,
                "last_barrier": self._last_barrier,
            },
        )
        self._send_control(event, acked=False)
        self._persist(fsync=False)
        log_event(_log, "heartbeat", "ok", level=logging.DEBUG, clock_sequence=self._seq,
                  source_clock=iso_utc(self._clock), publisher_state=self._state)

    def _send_snapshot(self, outcome: EnvelopeOutcome, feed_type: str, *, seq: int | None, origin: str,
                       at: datetime, capture_id: str) -> None:
        env = outcome.envelope
        if env is None:
            raise ValueError("cannot send a rejected outcome")
        topic = self._topics[feed_type]
        size = outcome.size_bytes or envelope_size(env)  # type: ignore[arg-type]
        ack, attempts = self._produce_acked(
            lambda **kw: self._producer.send_envelope(env, **kw),
            f"snapshot {env.get('snapshot_id')} ({feed_type}, capture {capture_id})",
        )
        published_at = ack.get("published_at")
        published_iso = iso_utc(published_at) if isinstance(published_at, datetime) else published_at
        self._messages_published += 1
        self._m_messages.labels(topic=topic).inc()
        self._m_bytes.labels(topic=topic).inc(size)
        self._m_candidates.labels(feed=feed_type).inc(outcome.candidates)
        self._log_message({
            "published_at": published_iso,
            "kind": "snapshot",
            "origin": origin,
            "topic": ack.get("topic", topic),
            "partition": ack.get("partition"),
            "offset": ack.get("offset"),
            "feed_type": feed_type,
            "snapshot_id": env.get("snapshot_id"),
            "capture_id": capture_id,
            "seq": seq,
            "original_fetched_at": iso_utc(at),
            "feed_timestamp": outcome.feed_timestamp,
            "bytes": size,
            "candidates": outcome.candidates,
            "selected_entities": outcome.selected_count,
            "clock_sequence": self._seq,
            "source_clock": iso_utc(self._clock),
            "attempts": attempts,
        })
        log_event(_log, "publish", "ok", feed=feed_type, snapshot_id=env.get("snapshot_id"), capture_id=capture_id,
                  origin=origin, bytes=size, candidates=outcome.candidates, offset=ack.get("offset"))

    # ------------------------------------------------------------------ main loop
    def _emit_start(self) -> None:
        extra = {
            "clock_mode": "source",
            "timeline_start": iso_utc(self._start_clock),
            "recording_id": self.recording_id,
            "scenario_id": self.run_record.scenario_id,
            "scope_id": self.run_record.scope_id,
            "gtfs_version": self.run_record.gtfs_version,
            "session_id": self.session_id,
            "step_mode": self.options.step_mode,
            "items_total": len(self.items),
            "manifest_sha256": self.manifest_sha256,
            "restarted": self._restarted,
            "resume_index": self._next_index,
        }
        self._state = pc.STATE_PAUSED if self._paused else pc.STATE_RUNNING
        self._emit_clock(ctl.RUN_STARTED, self._clock, extra=extra, run_state="RUNNING")
        if self._paused:
            self._emit_clock(ctl.RUN_PAUSED, self._clock, extra={"restarted": True})
        self._m_paused.set(1 if self._paused else 0)
        self._anchor()
        self._heartbeat()
        log_event(_log, "timeline", "ok", "publisher started", items_total=len(self.items),
                  next_index=self._next_index, speed=self.options.speed, step_mode=self.options.step_mode,
                  source_clock=iso_utc(self._clock), clock_sequence=self._seq)

    def _anchor(self) -> None:
        now = self._mono()
        self._anchor_mono = now
        self._anchor_clock = self._clock
        self._last_tick_mono = now

    def _virtual_clock(self, now: float) -> datetime:
        return self._anchor_clock + timedelta(seconds=(now - self._anchor_mono) * self.options.speed)

    def _due(self, item: TimelineItem) -> float:
        target = max(item.at, self._clock)
        return self._anchor_mono + (target - self._anchor_clock).total_seconds() / self.options.speed

    def _next_service_deadline(self) -> float:
        candidates = [
            self._last_control_poll + self.options.control_poll_seconds,
            self._last_heartbeat_mono + self.options.heartbeat_seconds,
        ]
        if self._deadline is not None:
            candidates.append(self._deadline)
        return min(candidates)

    def _sleep_until(self, deadline: float) -> None:
        dt = deadline - self._mono()
        dt = min(max(dt, 0.001), self.options.control_poll_seconds)
        self._sleep(dt)

    def _service(self) -> None:
        if self._signal_reason is not None and self._stop_reason is None:
            self._stop_reason = "finish_requested"
            self._stop_extra = {"signal": self._signal_reason}
        now = self._mono()
        if self._deadline is not None and now >= self._deadline and self._stop_reason is None:
            self._stop_reason = "duration_budget"
        if now - self._last_control_poll >= self.options.control_poll_seconds:
            self._last_control_poll = now
            req = self._control.poll() if self._control is not None else None
            if req is not None:
                self._apply_command(req)
        if self._mono() - self._last_heartbeat_mono >= self.options.heartbeat_seconds:
            self._heartbeat()

    def _wait_for(self, item: TimelineItem | None) -> bool:
        """Block until ``item`` is due (``None``: past the end in step mode). False when the run must stop."""
        while True:
            self._service()
            if self._stop_reason is not None:
                return False
            if self._paused:
                self._set_state(pc.STATE_PAUSED)
                self._sleep_until(self._next_service_deadline())
                continue
            if self.options.step_mode:
                if self._step_credit > 0:
                    self._set_state(pc.STATE_RUNNING)
                    self._lag = 0.0
                    return True
                self._set_state(pc.STATE_STEP_WAIT)
                self._sleep_until(self._next_service_deadline())
                continue
            self._set_state(pc.STATE_RUNNING)
            if item is None:
                return True
            now = self._mono()
            due = self._due(item)
            if now >= due:
                self._lag = max(0.0, now - due)
                self._m_lag.set(self._lag)
                return True
            if now - self._last_tick_mono >= self.options.tick_interval_seconds:
                self._last_tick_mono = now
                target = _floor_ms(min(item.at, self._virtual_clock(now)))
                if target > self._clock:
                    self._tick(target, "interval")
            wake = min(due, self._last_tick_mono + self.options.tick_interval_seconds, self._next_service_deadline())
            self._sleep_until(wake)

    def _process(self, item: TimelineItem) -> None:
        if item.kind == TICK:
            if _floor_ms(item.at) >= self._clock:
                self._tick(item.at, "timeline")
            else:
                log_event(_log, "tick", "skipped", "timeline tick earlier than the clock ignored",
                          seq=item.seq, at=iso_utc(item.at), source_clock=iso_utc(self._clock))
        else:
            if _floor_ms(item.at) > self._clock:
                self._tick(item.at, "item")
            elif item.at < self._clock and item.kind in (CAPTURE, REPUBLISH):
                log_event(_log, "publish", "ok", "late item: clock is not moved backwards", seq=item.seq,
                          at=iso_utc(item.at), source_clock=iso_utc(self._clock), kind=item.kind)
            handler = {
                CAPTURE: self._handle_capture,
                FETCH_EVENT: self._handle_fetch_event,
                REPUBLISH: self._handle_republish,
                ARM_FAULT: self._handle_arm_fault,
                MALFORMED_ENVELOPE: self._handle_malformed,
                BARRIER: self._handle_barrier,
            }[item.kind]
            handler(item)
        self._m_items.labels(kind=item.kind).inc()

    def _finish(self, reason: str) -> None:
        if self._state == pc.STATE_FINISHED:
            return
        extra = {"items_published": self._items_published, "items_total": len(self.items), **self._stop_extra}
        self._paused = False
        self._emit_clock(ctl.RUN_FINISHED, self._clock, reason=reason, extra=extra, run_state="FINISHED")
        self._state = pc.STATE_FINISHED
        self._finished_reason = reason
        self._m_paused.set(0)
        self._heartbeat()
        self._persist()
        self._flush("run_finished")
        log_event(_log, "timeline", "ok", f"run finished ({reason}); final clock kept", reason=reason,
                  source_clock=iso_utc(self._clock), clock_sequence=self._seq, items_published=self._items_published)

    # ------------------------------------------------------------------ item handlers
    def _feed(self, feed_type: str | None) -> str:
        if feed_type not in self._topics:
            raise PublisherError("invalid_item", f"unknown feed_type {feed_type!r}")
        return feed_type  # type: ignore[return-value]

    def _handle_capture(self, item: TimelineItem, origin: str = "timeline") -> None:
        cap = item.capture
        feed = self._feed(item.feed_type)
        if cap is None:
            raise PublisherError("invalid_item", f"capture item seq={item.seq} has no capture reference")
        # Every capture with a raw file is re-read, re-parsed and re-scoped for THIS run (interfaces §5);
        # the recorded parse_status is used only when a rejected capture has no readable raw file.
        if cap.is_ok:
            body = self._enveloper.read_capture(cap)    # integrity failure of an accepted capture is fatal
        else:
            maybe_body = self._try_read(cap)
            if maybe_body is None:
                self._replay_rejection(feed, cap.capture_id, cap.raw_path, cap.raw_sha256, None, cap.parse_status,
                                       "recorded rejection (raw file missing or unreadable)", item.at,
                                       cap.http_status)
                return
            body = maybe_body
        raw = self._enveloper.raw_location(cap)
        outcome = self._enveloper.build(feed_type=feed, body=body, fetched_at=item.at, raw=raw,
                                        capture_id=cap.capture_id, source_url=cap.source_url,
                                        http_status=cap.http_status)
        if not outcome.ok:
            reason = outcome.rejection_code or "INVALID_JSON"
            if cap.is_ok or reason != cap.parse_status:
                log_event(_log, "publish", "rejected", "re-parse result differs from the recorded parse_status",
                          level=logging.WARNING, feed=feed, capture_id=cap.capture_id, reason=reason,
                          recorded=cap.parse_status, detail=outcome.rejection_detail)
            self._replay_rejection(feed, cap.capture_id, cap.raw_path, cap.raw_sha256, body, reason,
                                   outcome.rejection_detail, item.at, cap.http_status)
            return
        if not cap.is_ok:
            log_event(_log, "publish", "ok", "capture recorded as rejected is valid for the current parser; published",
                      level=logging.WARNING, feed=feed, capture_id=cap.capture_id, recorded=cap.parse_status)
        self._send_snapshot(outcome, feed, seq=item.seq, origin=origin, at=item.at, capture_id=cap.capture_id)
        self._published_seqs.add(item.seq)
        self._note_success(feed, item.at, outcome.feed_timestamp)
        self._history[feed].append(_Published(feed, item.seq, item, None, item.at, outcome, cap.capture_id,
                                              cap.source_url))

    def _try_read(self, cap: Any) -> bytes | None:
        if cap.raw_path is None:
            return None
        try:
            return self._enveloper.read_capture(cap)
        except Exception as exc:  # noqa: BLE001 - caller falls back (recorded reason / not publishable)
            log_event(_log, "publish", "failed", f"cannot read raw capture: {exc}",
                      level=logging.WARNING, capture_id=cap.capture_id)
            return None

    def _rebuild(self, it: TimelineItem) -> EnvelopeOutcome | None:
        """Re-read, re-parse and re-scope a timeline capture for THIS run (None when its raw is unreadable)."""
        cap = it.capture
        if it.kind != CAPTURE or cap is None or cap.raw_path is None or it.feed_type is None:
            return None
        body = self._try_read(cap)
        if body is None:
            return None
        return self._enveloper.build(feed_type=it.feed_type, body=body, fetched_at=it.at,
                                     raw=self._enveloper.raw_location(cap), capture_id=cap.capture_id,
                                     source_url=cap.source_url, http_status=cap.http_status)

    def _replay_rejection(self, feed: str, capture_id: str, raw_path: str | None, raw_sha256: str | None,
                          body: bytes | None, reason: str, detail: str | None, at: datetime,
                          http_status: int | None) -> None:
        topic = self._topics[feed]
        value_sha = raw_sha256 or (sha256_hex(body) if body is not None else None)
        if reason in QUALITY_REASONS:
            record = dlq_record(
                error_id=error_id_message(self.run_record.run_id, topic, value_sha or capture_id, reason),
                run_id=self.run_record.run_id,
                feed_type=feed,
                reason_code=reason,
                stage="replay_capture",
                max_excerpt_chars=self.options.dlq_excerpt_max_chars,
                excerpt_source=None if reason == PAYLOAD_TOO_LARGE else body,
                source_topic=topic,
                value_sha256=value_sha,
                capture_id=capture_id,
                raw_path=raw_path,
                raw_sha256=raw_sha256,
                pipeline_version=self.options.pipeline_version,
                detected_at=self._wall(),
            )
            self._producer.send_dlq(record)
            self._messages_published += 1
            self._m_messages.labels(topic=self._dlq_topic).inc()
            self._log_message({
                "published_at": record["detected_at"], "kind": "dlq", "topic": self._dlq_topic,
                "feed_type": feed, "capture_id": capture_id, "reason_code": reason, "error_id": record["error_id"],
                "bytes": len(json.dumps(record, separators=(",", ":")).encode("utf-8")), "candidates": 0,
            })
        else:
            log_event(_log, "publish", "skipped", "unregistered rejection reason: no DLQ record",
                      level=logging.WARNING, feed=feed, capture_id=capture_id, reason=reason)
        stats = self._feed_stats[feed]
        stats.rejected_by_reason[reason] = stats.rejected_by_reason.get(reason, 0) + 1
        stats.consecutive_failures += 1
        self._emit_collector_status(feed, outcome="rejected", http_status=http_status, reason_code=reason, at=at)
        self._flush("rejection")
        log_event(_log, "publish", "rejected", detail or reason, feed=feed, capture_id=capture_id, reason=reason)

    def _emit_collector_status(self, feed: str, *, outcome: str, http_status: int | None,
                               reason_code: str | None, at: datetime) -> None:
        st = self._feed_stats[feed]
        event = ctl.status_event(
            ctl.COLLECTOR_STATUS,
            run_id=self.run_record.run_id,
            source_type=self.run_record.source_type,
            component=COMPONENT,
            version=__version__,
            fields={
                "feed_type": feed,
                "outcome": outcome,
                "http_status": http_status,
                "reason_code": reason_code,
                "attempts": None,
                "last_success_fetched_at": st.last_success_fetched_at,
                "last_feed_timestamp": st.last_feed_timestamp,
                "consecutive_failures": st.consecutive_failures,
                "feed_state": "healthy" if st.consecutive_failures == 0 else "degraded",
                "counters": {
                    "ok": st.ok,
                    "not_modified": st.not_modified,
                    "failed": st.failed,
                    "rejected_by_reason": dict(st.rejected_by_reason),
                },
                "outbox_pending": None,
                "replayed": True,
                "historical_at": iso_utc(at),
            },
        )
        self._send_control(event, acked=False)

    def _note_success(self, feed: str, at: datetime, feed_timestamp: int | None) -> None:
        st = self._feed_stats[feed]
        recovering = st.consecutive_failures > 0
        st.ok += 1
        st.consecutive_failures = 0
        st.last_success_fetched_at = iso_utc(at)
        st.last_feed_timestamp = feed_timestamp
        if recovering:
            self._emit_collector_status(feed, outcome="ok", http_status=200, reason_code=None, at=at)

    def _handle_fetch_event(self, item: TimelineItem) -> None:
        feed = self._feed(item.feed_type)
        params = item.params
        outcome = str(params.get("outcome") or "failed")
        http_status = params.get("http_status")
        st = self._feed_stats[feed]
        if outcome == "not_modified":
            st.not_modified += 1
            reason = params.get("reason_code")
        else:
            st.failed += 1
            st.consecutive_failures += 1
            reason = params.get("reason_code") or "SOURCE_FETCH_FAILED"
        self._emit_collector_status(feed, outcome=outcome,
                                    http_status=http_status if isinstance(http_status, int) else None,
                                    reason_code=reason, at=item.at)
        self._flush("fetch_event")
        log_event(_log, "publish", "ok", "fetch event replayed as collector_status", feed=feed, outcome=outcome,
                  reason=reason, seq=item.seq)

    def _handle_republish(self, item: TimelineItem) -> None:
        of_seq = int(item.params["of_seq"])
        target = self._by_seq.get(of_seq)
        if target is None or target.kind != CAPTURE:
            raise PublisherError("invalid_item", f"republish seq={item.seq}: of_seq={of_seq} is not a capture")
        log_event(_log, "publish", "ok", "republishing an earlier capture", seq=item.seq, of_seq=of_seq,
                  capture_id=target.capture.capture_id if target.capture else None)
        self._handle_capture(target, origin=f"republish:{item.seq}")

    def _resolve_fault_target(self, feed: str, ordinal: int) -> tuple[TimelineItem, EnvelopeOutcome] | None:
        """The ``ordinal``-th later capture of ``feed`` that will be published as a snapshot (decided by the
        re-parse for this run, never by the recorded parse_status)."""
        count = 0
        for it in self.items[self._next_index + 1:]:
            if it.kind != CAPTURE or it.feed_type != feed:
                continue
            outcome = self._rebuild(it)
            if outcome is None or not outcome.ok:
                continue
            count += 1
            if count == ordinal:
                return it, outcome
        return None

    def _handle_arm_fault(self, item: TimelineItem) -> None:
        params = item.params
        point = safe_component(str(params["point"]), "fault point")
        match = dict(params.get("match") or {})
        feed = self._feed(match.get("feed_type"))
        ordinal = int(match.get("snapshot_ordinal") or 1)
        resolved: dict[str, Any] = {"snapshot_id": None, "capture_id": None, "timeline_seq": None}
        target = self._resolve_fault_target(feed, ordinal)
        if target is not None:
            target_item, outcome = target
            resolved = {"snapshot_id": (outcome.envelope or {}).get("snapshot_id"),
                        "capture_id": target_item.capture.capture_id if target_item.capture else None,
                        "timeline_seq": target_item.seq}
        fault_id = "ft_" + H(K("fault", "v1", self.run_record.run_id, item.seq, point))
        self.fault_dir.mkdir(parents=True, exist_ok=True)
        if (self.fault_dir / f"fired-{fault_id}.json").exists():
            log_event(_log, "publish", "skipped", "fault already fired once; not re-armed (one-shot)",
                      fault_id=fault_id, point=point)
            return
        doc = {
            "fault_id": fault_id,
            "point": point,
            "run_id": self.run_record.run_id,
            "match": {**match, **resolved},
            "fraction": params.get("fraction"),
            "armed_at": iso_utc(self._wall()),
            "armed_by": COMPONENT,
            "session_id": self.session_id,
            "source_clock": iso_utc(self._clock),
            "timeline_seq": item.seq,
        }
        atomic_write_json(self.fault_dir / f"{point}.json", doc)
        event = ctl.status_event(
            ctl.FAULT_ARMED,
            run_id=self.run_record.run_id,
            source_type=self.run_record.source_type,
            component=COMPONENT,
            version=__version__,
            fields={"fault_point": point, "fault_id": fault_id, "match": doc["match"]},
        )
        self._send_control(event, acked=True)
        log_event(_log, "publish", "ok", "fault armed", fault_id=fault_id, point=point, match=doc["match"])

    def _handle_malformed(self, item: TimelineItem) -> None:
        feed = self._feed(item.feed_type)
        topic = self._topics[feed]
        value = str(item.params["value_text"]).encode("utf-8")
        self._producer.send_raw(topic, kafka_key(self.run_record.run_id, feed), value)
        self._flush("malformed_envelope")
        self._messages_published += 1
        self._m_messages.labels(topic=topic).inc()
        self._m_bytes.labels(topic=topic).inc(len(value))
        self._log_message({
            "published_at": iso_utc(self._wall()), "kind": "raw", "topic": topic, "feed_type": feed,
            "seq": item.seq, "bytes": len(value), "candidates": 0, "value_sha256": sha256_hex(value),
        })
        log_event(_log, "publish", "ok", "malformed envelope bytes produced (fault scenario)", feed=feed,
                  seq=item.seq, bytes=len(value))

    def _handle_barrier(self, item: TimelineItem) -> None:
        label = item.label or str(item.params.get("label"))
        self._last_barrier = label
        if self.options.step_mode and self._step_credit > 0:
            self._step_credit -= 1
        log_event(_log, "timeline", "ok", f"barrier {label}", seq=item.seq, label=label,
                  step_credit=self._step_credit, source_clock=iso_utc(self._clock))

    # ------------------------------------------------------------------ control commands
    def _apply_command(self, req: dict[str, Any]) -> None:
        request_id = str(req.get("request_id"))
        command = str(req.get("command"))
        args = req.get("args") if isinstance(req.get("args"), dict) else {}
        log_event(_log, "control_cmd", "ok", f"command {command} received", request_id=request_id,
                  command=command, args=args)
        result: dict[str, Any] | None = None
        error: dict[str, Any] | None = None
        try:
            if command == "pause":
                result = self._cmd_pause()
            elif command == "resume":
                result = self._cmd_resume()
            elif command == "finish":
                self._stop_reason = self._stop_reason or "finish_requested"
                result = {"finishing": True}
            elif command == "step":
                result = self._cmd_step()
            elif command == "advance-clock":
                result = self._cmd_advance_clock(args, request_id)
            elif command == "inject":
                result = self._cmd_inject(args, request_id)
            else:
                raise CommandRejected("invalid_command", f"unknown command {command!r}")
        except CommandRejected as exc:
            error = {"code": exc.code, "message": str(exc)}
            log_event(_log, "control_cmd", "rejected", str(exc), level=logging.WARNING, request_id=request_id,
                      command=command, reason=exc.code)
        self._applied_request_id = request_id
        self._last_command = {
            "request_id": request_id,
            "command": command,
            "args": args,
            "ok": error is None,
            "error": error,
            "result": result,
            "applied_at": iso_utc(self._wall()),
        }
        self._persist()

    def _record_injection(self, request_id: str | None, kind: str, result: dict[str, Any]) -> None:
        append_jsonl(self.session_dir / pc.INJECTIONS_FILE, {
            "request_id": request_id,
            "kind": kind,
            "run_id": self.run_record.run_id,
            "wall_at": iso_utc(self._wall()),
            "source_clock": iso_utc(self._clock),
            "clock_sequence": self._seq,
            "result": result,
        })

    def _cmd_pause(self) -> dict[str, Any]:
        if self._paused:
            return {"noop": True, "source_clock": iso_utc(self._clock)}
        freeze = self._clock
        nxt = self.items[self._next_index] if self._next_index < len(self.items) else None
        if not self.options.step_mode and self._state == pc.STATE_RUNNING and nxt is not None:
            # freeze at the current source time (never beyond the next item, never backwards)
            freeze = max(self._clock, _floor_ms(min(nxt.at, self._virtual_clock(self._mono()))))
        self._paused = True
        self._emit_clock(ctl.RUN_PAUSED, freeze, run_state="PAUSED")
        self._set_state(pc.STATE_PAUSED)
        self._m_paused.set(1)
        return {"source_clock": iso_utc(self._clock), "clock_sequence": self._seq}

    def _cmd_resume(self) -> dict[str, Any]:
        if not self._paused:
            return {"noop": True, "source_clock": iso_utc(self._clock)}
        self._paused = False
        self._emit_clock(ctl.RUN_RESUMED, self._clock, run_state="RUNNING")
        self._anchor()
        self._m_paused.set(0)
        self._set_state(pc.STATE_STEP_WAIT if self.options.step_mode and self._step_credit == 0
                        else pc.STATE_RUNNING)
        return {"source_clock": iso_utc(self._clock), "clock_sequence": self._seq}

    def _cmd_step(self) -> dict[str, Any]:
        if not self.options.step_mode:
            raise CommandRejected("unsupported_mode", "step requires a publisher started with --step-mode")
        self._step_credit += 1
        return {"step_credit": self._step_credit, "next_index": self._next_index, "paused": self._paused,
                "last_barrier": self._last_barrier}

    def _cmd_advance_clock(self, args: dict[str, Any], request_id: str) -> dict[str, Any]:
        raw = args.get("to")
        if not isinstance(raw, str) or not raw:
            raise CommandRejected("invalid_argument", "advance-clock requires args.to (ISO-8601 UTC)")
        try:
            target = _floor_ms(parse_iso_utc(raw))
        except ValueError as exc:
            raise CommandRejected("invalid_argument", f"invalid advance-clock target {raw!r}: {exc}") from exc
        if target < self._clock:
            raise CommandRejected(
                "clock_regression",
                f"the source clock never moves backwards (current {iso_utc(self._clock)}, requested {iso_utc(target)})",
            )
        previous = self._clock
        if target > self._clock:
            self._tick(target, "advance_clock")
            self._anchor()
        result = {"from": iso_utc(previous), "to": iso_utc(self._clock), "clock_sequence": self._seq,
                  "noop": target == previous}
        self._record_injection(request_id, "advance_clock", result)
        return result

    def _cmd_inject(self, args: dict[str, Any], request_id: str) -> dict[str, Any]:
        if self.run_record.source_type != SourceType.SYNTHETIC.value:
            raise CommandRejected("unsupported_mode", "inject is only available for synthetic runs")
        scenario = args.get("scenario")
        if scenario not in pc.INJECT_SCENARIOS:
            raise CommandRejected("invalid_argument",
                                  f"inject scenario must be one of {list(pc.INJECT_SCENARIOS)}, got {scenario!r}")
        if scenario == "duplicate_snapshot":
            result = self._inject_duplicate()
        elif scenario == "late_snapshot":
            result = self._inject_late()
        else:
            result = self._inject_invalid_gps(request_id)
        result["scenario"] = scenario
        self._record_injection(request_id, f"inject:{scenario}", result)
        return result

    def _published(self, feed: str) -> list[_Published]:
        """Published snapshots of ``feed`` (oldest first); rebuilt from the timeline after a restart."""
        hist = self._history[feed]
        if not hist and feed not in self._history_rebuilt and self._next_index > 0:
            self._history_rebuilt.add(feed)
            rebuilt: list[_Published] = []
            for it in reversed(self.items[:self._next_index]):
                if it.kind != CAPTURE or it.feed_type != feed or it.capture is None:
                    continue
                # "published" = the persisted publish outcome (the recorded parse_status is never used)
                if self._published_seqs_known and it.seq not in self._published_seqs:
                    continue
                outcome = self._rebuild(it)
                if outcome is None or not outcome.ok:
                    continue
                rebuilt.append(_Published(feed, it.seq, it, None, it.at, outcome, it.capture.capture_id,
                                          it.capture.source_url))
                if len(rebuilt) >= hist.maxlen:  # type: ignore[operator]
                    break
            for rec in reversed(rebuilt):
                hist.append(rec)
        return list(hist)

    def _latest(self, prefer: str = VEHICLE_POSITIONS) -> _Published:
        recs = self._published(prefer)
        if recs:
            return recs[-1]
        best: _Published | None = None
        for feed in self._topics:
            for rec in self._published(feed)[-1:]:
                if best is None or rec.at > best.at:
                    best = rec
        if best is None:
            raise CommandRejected("not_ready", "no snapshot has been published yet")
        return best

    def _resend(self, rec: _Published, origin: str) -> dict[str, Any]:
        self._send_snapshot(rec.outcome, rec.feed_type, seq=rec.seq, origin=origin, at=rec.at,
                            capture_id=rec.capture_id)
        return {"feed_type": rec.feed_type, "snapshot_id": rec.snapshot_id, "capture_id": rec.capture_id,
                "original_fetched_at": iso_utc(rec.at)}

    def _inject_duplicate(self) -> dict[str, Any]:
        return self._resend(self._latest(), "inject:duplicate_snapshot")

    def _inject_late(self) -> dict[str, Any]:
        recs = self._published(VEHICLE_POSITIONS)
        if not recs:
            raise CommandRejected("not_ready", "late_snapshot needs published vehicle_positions snapshots")
        latest = recs[-1]
        older = [r for r in recs[:-1]
                 if r.snapshot_id != latest.snapshot_id
                 and (r.outcome.feed_timestamp or 0) < (latest.outcome.feed_timestamp or 0)]
        if not older:
            raise CommandRejected(
                "not_ready", "late_snapshot needs at least two distinct published vehicle_positions snapshots"
            )
        result = self._resend(older[-1], "inject:late_snapshot")
        result["current_snapshot_id"] = latest.snapshot_id
        return result

    def _inject_invalid_gps(self, request_id: str) -> dict[str, Any]:
        recs = self._published(VEHICLE_POSITIONS)
        if not recs:
            raise CommandRejected("not_ready", "invalid_gps needs a published vehicle_positions snapshot")
        base = recs[-1]
        if base.body is not None:
            body = base.body
        elif base.item is not None and base.item.capture is not None:
            body = self._enveloper.read_capture(base.item.capture)
        else:
            raise CommandRejected("not_ready", "the last vehicle_positions snapshot cannot be re-read")
        doc = json.loads(body.decode("utf-8"))
        entities = doc.get("entity") or []
        selected_ids = {e.get("id") for e in (base.outcome.envelope or {}).get("entities") or []
                        if isinstance(e, dict)}
        target = None
        for ent in entities:
            veh = ent.get("vehicle") if isinstance(ent, dict) else None
            if isinstance(veh, dict) and isinstance(veh.get("position"), dict) and ent.get("id") in selected_ids:
                target = ent
                break
        if target is None:
            raise CommandRejected("not_ready", "the last vehicle_positions snapshot has no in-scope vehicle position")
        target["vehicle"]["position"]["latitude"] = 95.0
        header = doc.setdefault("header", {})
        last_ts = base.outcome.feed_timestamp or int(header.get("timestamp") or 0)
        new_ts = max(int(last_ts) + 1, int(self._clock.timestamp()))
        header["timestamp"] = new_ts
        fetched_at = max(self._clock, datetime.fromtimestamp(new_ts, UTC))
        if _floor_ms(fetched_at) > self._clock:
            self._tick(fetched_at, "inject")
        new_body = json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        raw_sha = sha256_hex(new_body)
        cap_id = make_capture_id(FEED_ABBR[VEHICLE_POSITIONS], fetched_at, raw_sha)
        raw: RawLocation = self._enveloper.archive_injected(
            feed_type=VEHICLE_POSITIONS, capture_id=cap_id, fetched_at=fetched_at, body=new_body,
            meta={"injected": True, "scenario": "invalid_gps", "request_id": request_id,
                  "derived_from_snapshot_id": base.snapshot_id, "run_id": self.run_record.run_id,
                  "source_type": self.run_record.source_type, "note": "synthetic injection; not MBTA data"},
        )
        outcome = self._enveloper.build(feed_type=VEHICLE_POSITIONS, body=new_body, fetched_at=fetched_at, raw=raw,
                                        capture_id=cap_id, source_url=base.source_url, http_status=200)
        if not outcome.ok:
            raise CommandRejected("inject_failed",
                                  f"injected snapshot rejected: {outcome.rejection_code} {outcome.rejection_detail}")
        self._send_snapshot(outcome, VEHICLE_POSITIONS, seq=None, origin="inject:invalid_gps", at=fetched_at,
                            capture_id=cap_id)
        self._history[VEHICLE_POSITIONS].append(
            _Published(VEHICLE_POSITIONS, None, None, new_body, fetched_at, outcome, cap_id, base.source_url)
        )
        return {"feed_type": VEHICLE_POSITIONS, "snapshot_id": (outcome.envelope or {}).get("snapshot_id"),
                "capture_id": cap_id, "raw_path": raw.raw_path, "modified_entity_id": target.get("id"),
                "latitude": 95.0, "feed_timestamp": new_ts, "derived_from_snapshot_id": base.snapshot_id}
