"""Kafka producer shared by the collector, the replay publisher and the test harness (design E §1.6, D-E08).

Configuration comes from ``cfg.kafka.producer``: idempotence on, ``acks=all``, ``compression.type=none``
(so the 4 MiB snapshot check equals the wire size) and ``message.max.bytes`` with headroom over the
4 MiB envelope limit. Messages are produced asynchronously; delivery reports are served by
:meth:`EnvelopeProducer.poll` / :meth:`EnvelopeProducer.flush` (and a ``poll(0)`` after every produce)
in the calling thread.

* ``on_ack(topic, partition, offset, published_at)`` is invoked only for a broker ACK (``err is None``);
  this is the ONLY place where callers may mark an outbox row as published.
* ``on_error(err)`` (optional extension) is invoked for a failed delivery report.
* A fatal producer error (``KafkaError.fatal()``, e.g. an idempotence violation) makes the wrapper
  purge and recreate the underlying ``confluent_kafka.Producer`` before the next send; ``generation``
  is incremented so dispatchers know that unacknowledged messages must be re-sent from the outbox.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable
from datetime import datetime
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer

from transitpulse.config import ProjectConfig
from transitpulse.contracts.envelope import Envelope, control_key, kafka_headers, kafka_key, serialize_envelope, utcnow
from transitpulse.obs.logging import log_event

log = logging.getLogger(__name__)

AckCallback = Callable[[str, int, int, datetime], None]
ErrorCallback = Callable[[Any], None]

_SIZE_ERROR_NAMES = ("MSG_SIZE_TOO_LARGE", "RECORD_LIST_TOO_LARGE")


class PayloadTooLarge(Exception):
    """A serialized message exceeds the configured limit; it was NOT produced."""

    def __init__(self, size: int, limit: int, what: str = "envelope") -> None:
        super().__init__(f"{what} of {size} bytes exceeds the limit of {limit} bytes")
        self.size = size
        self.limit = limit


def size_error_codes() -> frozenset[int]:
    """librdkafka error codes meaning "message too large" (resolved defensively by name)."""
    codes = set()
    for name in _SIZE_ERROR_NAMES:
        code = getattr(KafkaError, name, None)
        if isinstance(code, int):
            codes.add(code)
    return frozenset(codes)


def is_size_error(err: Any) -> bool:
    try:
        return err is not None and err.code() in size_error_codes()
    except Exception:
        return False


def is_fatal(err: Any) -> bool:
    try:
        return bool(err is not None and err.fatal())
    except Exception:
        return False


def producer_config(cfg: ProjectConfig, component: str, run_id: str) -> dict[str, Any]:
    """librdkafka configuration from ``cfg.kafka`` (callbacks are added by :class:`EnvelopeProducer`)."""
    p = cfg.kafka.producer
    return {
        "bootstrap.servers": cfg.kafka.bootstrap_servers,
        "client.id": f"tp-{component}-{run_id[:8]}",
        "enable.idempotence": bool(p.enable_idempotence),
        "acks": str(p.acks),
        "max.in.flight.requests.per.connection": int(p.max_in_flight_requests_per_connection),
        "message.timeout.ms": int(p.message_timeout_ms),
        "linger.ms": int(p.linger_ms),
        "compression.type": str(p.compression_type),
        "message.max.bytes": int(p.message_max_bytes),
    }


def _json_bytes(obj: dict[str, Any]) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


class EnvelopeProducer:
    """Thin, thread-safe wrapper over ``confluent_kafka.Producer`` with TransitPulse framing."""

    def __init__(
        self,
        cfg: ProjectConfig,
        component: str,
        run_id: str,
        *,
        producer_factory: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        self.cfg = cfg
        self.component = component
        self.run_id = run_id
        self.max_snapshot_bytes = int(cfg.kafka.max_snapshot_bytes)
        self.max_small_bytes = int(cfg.kafka.control_max_message_bytes)
        self.topics = cfg.kafka.topics
        self._conf = producer_config(cfg, component, run_id)
        self._factory = producer_factory or Producer
        self._lock = threading.RLock()
        self._fatal: Any = None
        self._closed = False
        self.generation = 0
        self.stats: dict[str, int] = {"produced": 0, "acked": 0, "failed": 0, "recreated": 0}
        self._producer = self._new_producer(self.generation)

    # ------------------------------------------------------------------ callbacks / lifecycle
    def _new_producer(self, generation: int) -> Any:
        conf = dict(self._conf)
        conf["error_cb"] = self._error_cb_for(generation)
        return self._factory(conf)

    def _error_cb_for(self, generation: int) -> Callable[[Any], None]:
        def error_cb(err: Any) -> None:
            fatal = is_fatal(err)
            log_event(log, "kafka_send", "failed" if fatal else "retry", f"producer error: {err}"[:500],
                      level=logging.ERROR if fatal else logging.WARNING,
                      reason="kafka_fatal" if fatal else "kafka_error")
            if fatal and generation == self.generation:
                self._fatal = err
        return error_cb

    @property
    def fatal_error(self) -> Any:
        return self._fatal

    def _ensure_usable(self) -> Any:
        with self._lock:
            if self._closed:
                raise RuntimeError("producer is closed")
            if self._fatal is not None:
                self._recreate_locked()
            return self._producer

    def _recreate_locked(self) -> None:
        old = self._producer
        reason = self._fatal
        # Swap first so that callbacks served while draining the old instance never touch it again.
        self.generation += 1
        self._producer = self._new_producer(self.generation)
        self._fatal = None
        self.stats["recreated"] += 1
        try:
            old.purge(in_queue=True, in_flight=True, blocking=False)
        except Exception as exc:  # purge is best effort on a broken instance
            log.warning("purge of failed producer raised: %s", exc)
        try:
            old.flush(0)  # serves the purge delivery reports (callers see errors, rows stay pending)
        except Exception as exc:
            log.warning("flush of failed producer raised: %s", exc)
        log_event(log, "kafka_send", "retry", f"producer recreated after fatal error: {reason}"[:500],
                  reason="kafka_fatal", generation=self.generation)

    def _callback(self, on_ack: AckCallback | None, on_error: ErrorCallback | None,
                  published_at: datetime, generation: int) -> Callable[[Any, Any], None]:
        def deliver(err: Any, msg: Any) -> None:
            try:
                if err is None:
                    self.stats["acked"] += 1
                    if on_ack is not None:
                        on_ack(msg.topic(), msg.partition(), msg.offset(), published_at)
                else:
                    self.stats["failed"] += 1
                    if is_fatal(err) and generation == self.generation:
                        self._fatal = err
                    if on_error is not None:
                        on_error(err)
            except Exception:  # an exception here would propagate out of poll()/flush()
                log.exception("delivery callback failed")
        return deliver

    def _produce(self, topic: str, key: bytes, value: bytes, headers: list[tuple[str, bytes]],
                 on_ack: AckCallback | None, on_error: ErrorCallback | None, published_at: datetime) -> None:
        with self._lock:
            producer = self._ensure_usable()
            generation = self.generation
        try:
            producer.produce(topic, value=value, key=key, headers=headers,
                             on_delivery=self._callback(on_ack, on_error, published_at, generation))
        except KafkaException as exc:
            err = exc.args[0] if exc.args else None
            if is_fatal(err) and generation == self.generation:
                self._fatal = err
            if is_size_error(err):
                raise PayloadTooLarge(len(value), int(self._conf["message.max.bytes"]), "kafka message") from exc
            raise
        self.stats["produced"] += 1
        producer.poll(0)

    # ------------------------------------------------------------------ public API
    def send_envelope(self, env: Envelope, *, on_ack: AckCallback | None = None,
                      on_error: ErrorCallback | None = None) -> None:
        """Set ``published_at=now``, re-check the size on the final bytes, produce asynchronously."""
        published_at = utcnow()
        value = serialize_envelope(env, published_at)
        if len(value) > self.max_snapshot_bytes:
            raise PayloadTooLarge(len(value), self.max_snapshot_bytes)
        topic = self.topics.for_feed(env["feed_type"])
        self._produce(topic, kafka_key(env["run_id"], env["feed_type"]), value, kafka_headers(env),
                      on_ack, on_error, published_at)

    def send_control(self, event: dict[str, Any], *, on_ack: AckCallback | None = None,
                     on_error: ErrorCallback | None = None) -> None:
        """Control event on ``mbta.control.v1`` with key ``run_id|control``."""
        value = _json_bytes(event)
        if len(value) > self.max_small_bytes:
            raise PayloadTooLarge(len(value), self.max_small_bytes, "control event")
        headers = [("schema_version", str(event.get("schema_version", 1)).encode()),
                   ("event_type", str(event.get("event_type", "")).encode())]
        self._produce(self.topics.control, control_key(str(event.get("run_id") or self.run_id)), value, headers,
                      on_ack, on_error or self._log_failure("control"), utcnow())

    def send_dlq(self, record: dict[str, Any], *, on_ack: AckCallback | None = None,
                 on_error: ErrorCallback | None = None) -> None:
        """DLQ record on ``mbta.dlq.v1`` with key ``run_id|feed_type``."""
        value = _json_bytes(record)
        if len(value) > self.max_small_bytes:
            raise PayloadTooLarge(len(value), self.max_small_bytes, "dlq record")
        key = kafka_key(str(record.get("run_id") or self.run_id), str(record.get("feed_type") or "unknown"))
        headers = [("schema_version", str(record.get("schema_version", 1)).encode()),
                   ("error_id", str(record.get("error_id", "")).encode())]
        self._produce(self.topics.dlq, key, value, headers, on_ack, on_error or self._log_failure("dlq"), utcnow())

    def send_raw(self, topic: str, key: bytes, value: bytes, *, on_ack: AckCallback | None = None,
                 on_error: ErrorCallback | None = None) -> None:
        """Produce arbitrary bytes (tests and fault scenarios only; no framing, no size check)."""
        self._produce(topic, key, value, [], on_ack, on_error or self._log_failure("raw"), utcnow())

    def poll(self, timeout: float = 0.0) -> int:
        return int(self._ensure_usable().poll(timeout))

    def flush(self, timeout: float = 30.0) -> int:
        """Wait for outstanding deliveries; returns the number of messages still queued."""
        with self._lock:
            producer = self._producer
        return int(producer.flush(timeout))

    def pending(self) -> int:
        with self._lock:
            return len(self._producer)

    def close(self) -> None:
        """Flush (bounded) and release the producer. Unacknowledged messages stay pending upstream."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            producer = self._producer
        try:
            remaining = int(producer.flush(10.0))
        except Exception as exc:
            log.warning("flush on close raised: %s", exc)
            remaining = -1
        if remaining:
            log_event(log, "kafka_send", "failed", f"{remaining} message(s) not acknowledged at close",
                      level=logging.WARNING, reason="unacked_at_close")

    def _log_failure(self, what: str) -> ErrorCallback:
        def on_error(err: Any) -> None:
            log_event(log, "kafka_ack", "failed", f"{what} delivery failed: {err}"[:500], level=logging.WARNING,
                      reason="kafka_delivery_failed")
        return on_error
