"""Durable outbox (SQLite WAL) and its Kafka dispatcher (design E §1.6, decision D-E07).

One database per run at ``W/state/outbox/<run_id>.sqlite``; an exclusive ``fcntl.flock`` on
``<run_id>.lock`` prevents a second collector for the same run. Pragmas: ``journal_mode=WAL``,
``synchronous=FULL`` (every commit is fsynced), ``busy_timeout=5000``.

State machine (enforced by ``WHERE state='pending'`` guards)::

    insert -> pending ---(delivery callback, err is None)---> published
    insert -> rejected                                        (transport rejection at capture time)
              pending ---(send-time size re-check / broker size error)---> rejected

* Snapshot rows (``kind='snapshot'``, ``message_id = capture_id``) store the envelope WITHOUT
  ``published_at`` (zlib-compressed JSON), so a resend after a restart carries the same ``capture_id``
  and ``snapshot_id``; only ``published_at`` is new.
* DLQ rows (``kind='dlq'``, ``message_id = 'dlq:' + capture_id``) make collector-side DLQ records as
  durable as snapshots.
* ``published`` is written ONLY from the producer delivery callback with the broker partition/offset.
  A crash between the broker ACK and the local update causes a re-delivery that downstream tolerates.
"""

from __future__ import annotations

import errno
import fcntl
import json
import logging
import os
import sqlite3
import threading
import time
import zlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from transitpulse.contracts.envelope import Envelope, envelope_size, iso_utc, utcnow
from transitpulse.ingestion.kafka_producer import PayloadTooLarge, is_size_error
from transitpulse.obs.logging import log_event

log = logging.getLogger(__name__)

SCHEMA_VERSION = 1
STATES = ("pending", "published", "rejected")
KINDS = ("snapshot", "dlq")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
  message_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('snapshot','dlq')),
  run_id TEXT NOT NULL,
  feed_type TEXT NOT NULL,
  snapshot_id TEXT,
  capture_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  kafka_key TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending','published','rejected')),
  reason TEXT,
  payload BLOB,
  payload_bytes INTEGER,
  raw_path TEXT NOT NULL,
  raw_sha256 TEXT NOT NULL,
  original_fetched_at TEXT NOT NULL,
  feed_timestamp INTEGER,
  created_at TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_attempt_at TEXT,
  last_error TEXT,
  acked_published_at TEXT,
  kafka_partition INTEGER,
  kafka_offset INTEGER,
  seq INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(state, seq);
CREATE INDEX IF NOT EXISTS outbox_capture ON outbox(capture_id);
"""

_META_COLUMNS = (
    "message_id, kind, run_id, feed_type, snapshot_id, capture_id, topic, kafka_key, state, reason, "
    "payload_bytes, raw_path, raw_sha256, original_fetched_at, feed_timestamp, created_at, attempts, "
    "last_attempt_at, last_error, acked_published_at, kafka_partition, kafka_offset, seq"
)

_INSERT = (
    "INSERT INTO outbox (message_id, kind, run_id, feed_type, snapshot_id, capture_id, topic, kafka_key, state, "
    "reason, payload, payload_bytes, raw_path, raw_sha256, original_fetched_at, feed_timestamp, created_at, seq) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(seq), 0) + 1 FROM outbox)) "
    "ON CONFLICT(message_id) DO NOTHING"
)


class OutboxLocked(RuntimeError):
    """Another process (collector) holds the run's outbox lock."""


@dataclass(frozen=True)
class OutboxRow:
    message_id: str
    kind: str
    run_id: str
    feed_type: str
    snapshot_id: str | None
    capture_id: str
    topic: str
    kafka_key: str
    state: str
    reason: str | None
    payload_bytes: int | None
    raw_path: str
    raw_sha256: str
    original_fetched_at: str
    feed_timestamp: int | None
    created_at: str
    attempts: int
    last_attempt_at: str | None
    last_error: str | None
    acked_published_at: str | None
    kafka_partition: int | None
    kafka_offset: int | None
    seq: int


@dataclass(frozen=True)
class DlqMessage:
    """A DLQ record to enqueue durably next to the capture it refers to."""

    record: dict[str, Any]
    topic: str
    kafka_key: str

    @property
    def message_id(self) -> str:
        return f"dlq:{self.record['capture_id']}"


def encode_payload(obj: dict[str, Any]) -> tuple[bytes, int]:
    raw = json.dumps(obj, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    return zlib.compress(raw, 6), len(raw)


def decode_payload(blob: bytes) -> dict[str, Any]:
    return json.loads(zlib.decompress(blob).decode("utf-8"))


class Outbox:
    """SQLite outbox of one run. All methods are thread-safe (one connection guarded by an RLock)."""

    def __init__(self, db_path: Path, *, run_id: str, keep_published_payload: bool = False,
                 lock: bool = True, create: bool = True) -> None:
        self.db_path = Path(db_path)
        self.run_id = run_id
        self.keep_published_payload = keep_published_payload
        self.lock_path = self.db_path.with_name(f"{run_id}.lock")
        self._lock = threading.RLock()
        self._lock_fd: int | None = None
        if create:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        elif not self.db_path.is_file():
            raise FileNotFoundError(f"outbox database {self.db_path} does not exist")
        if lock:
            self._acquire_flock()
        try:
            # as_uri() percent-encodes spaces, '?', '#' and '%' in the work directory path.
            uri = f"{self.db_path.resolve().as_uri()}?mode={'rwc' if create else 'rw'}"
            self._conn = sqlite3.connect(uri, uri=True, timeout=5.0, isolation_level=None, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            mode = self._conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                raise RuntimeError(f"cannot enable WAL journal mode on {self.db_path} (got {mode!r})")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            if create:
                self._conn.executescript(_SCHEMA)
                self._conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        except BaseException:
            self._release_flock()
            raise

    # ------------------------------------------------------------------ locking
    def _acquire_flock(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.lock_path), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
                raise OutboxLocked(f"outbox of run {self.run_id} is locked by another process "
                                   f"({self.lock_path})") from exc
            raise
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())
        self._lock_fd = fd

    def _release_flock(self) -> None:
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(self._lock_fd)
                self._lock_fd = None

    @staticmethod
    def is_locked(lock_path: Path) -> bool:
        """True when some process currently holds the flock (probe without keeping it)."""
        if not lock_path.exists():
            return False
        fd = os.open(str(lock_path), os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return True
        else:
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
        finally:
            os.close(fd)

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            finally:
                self._release_flock()

    def __enter__(self) -> Outbox:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ inserts
    def insert_snapshot(self, env: Envelope, *, topic: str, kafka_key: str,
                        created_at: datetime | None = None) -> bool:
        """Insert a ``pending`` snapshot row; returns False if the capture already has a row."""
        payload, _ = encode_payload(dict(env))
        params = (
            env["capture_id"], "snapshot", env["run_id"], env["feed_type"], env["snapshot_id"], env["capture_id"],
            topic, kafka_key, "pending", None, payload, envelope_size(env), env["raw_path"], env["raw_sha256"],
            env["original_fetched_at"], env["feed_timestamp"], iso_utc(created_at or utcnow()),
        )
        with self._lock:
            cur = self._conn.execute(_INSERT, params)
            return cur.rowcount == 1

    def insert_rejected(
        self,
        *,
        capture_id: str,
        feed_type: str,
        reason: str,
        topic: str,
        kafka_key: str,
        raw_path: str,
        raw_sha256: str,
        original_fetched_at: str,
        feed_timestamp: int | None = None,
        snapshot_id: str | None = None,
        payload_bytes: int | None = None,
        dlq: DlqMessage | None = None,
        created_at: datetime | None = None,
    ) -> bool:
        """Insert a ``rejected`` snapshot row (no payload) and, atomically, its pending DLQ row."""
        created = iso_utc(created_at or utcnow())
        snap = (
            capture_id, "snapshot", self.run_id, feed_type, snapshot_id, capture_id, topic, kafka_key, "rejected",
            reason, None, payload_bytes, raw_path, raw_sha256, original_fetched_at, feed_timestamp, created,
        )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(_INSERT, snap)
                inserted = cur.rowcount == 1
                if inserted and dlq is not None:
                    self._insert_dlq_locked(dlq, feed_type=feed_type, capture_id=capture_id, raw_path=raw_path,
                                            raw_sha256=raw_sha256, original_fetched_at=original_fetched_at,
                                            feed_timestamp=feed_timestamp, snapshot_id=snapshot_id,
                                            created=created)
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
        return inserted

    def _insert_dlq_locked(self, dlq: DlqMessage, *, feed_type: str, capture_id: str, raw_path: str,
                           raw_sha256: str, original_fetched_at: str, feed_timestamp: int | None,
                           snapshot_id: str | None, created: str) -> None:
        payload, nbytes = encode_payload(dlq.record)
        self._conn.execute(_INSERT, (
            dlq.message_id, "dlq", self.run_id, feed_type, snapshot_id, capture_id, dlq.topic, dlq.kafka_key,
            "pending", dlq.record.get("reason_code"), payload, nbytes, raw_path, raw_sha256, original_fetched_at,
            feed_timestamp, created,
        ))

    # ------------------------------------------------------------------ dispatcher queries
    def pending_rows(self, limit: int, exclude: Iterable[str] = ()) -> list[OutboxRow]:
        """Oldest pending rows (by ``seq``), metadata only, skipping ``exclude`` (in-flight ids)."""
        excl = list(exclude)
        sql = f"SELECT {_META_COLUMNS} FROM outbox WHERE state = 'pending'"
        if excl:
            sql += f" AND message_id NOT IN ({','.join('?' for _ in excl)})"
        sql += " ORDER BY seq LIMIT ?"
        with self._lock:
            rows = self._conn.execute(sql, (*excl, int(limit))).fetchall()
        return [OutboxRow(**dict(r)) for r in rows]

    def load_payload(self, message_id: str) -> dict[str, Any] | None:
        """Decoded payload of a still-pending row (None when the row is not pending)."""
        with self._lock:
            row = self._conn.execute("SELECT payload FROM outbox WHERE message_id = ? AND state = 'pending'",
                                     (message_id,)).fetchone()
        if row is None or row["payload"] is None:
            return None
        return decode_payload(row["payload"])

    def get(self, message_id: str) -> OutboxRow | None:
        with self._lock:
            row = self._conn.execute(f"SELECT {_META_COLUMNS} FROM outbox WHERE message_id = ?",
                                     (message_id,)).fetchone()
        return OutboxRow(**dict(row)) if row is not None else None

    def has_capture(self, capture_id: str) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT 1 FROM outbox WHERE message_id = ?", (capture_id,)).fetchone()
        return row is not None

    def mark_attempt(self, message_id: str, at: datetime | None = None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE outbox SET attempts = attempts + 1, last_attempt_at = ? WHERE message_id = ? AND state = 'pending'",
                (iso_utc(at or utcnow()), message_id),
            )

    def mark_published(self, message_id: str, *, published_at: datetime, partition: int | None,
                       offset: int | None) -> bool:
        """pending -> published. Call ONLY from a producer delivery callback with ``err is None``."""
        payload_sql = "" if self.keep_published_payload else ", payload = NULL"
        with self._lock:
            cur = self._conn.execute(
                f"UPDATE outbox SET state = 'published', acked_published_at = ?, kafka_partition = ?, "
                f"kafka_offset = ?, last_error = NULL{payload_sql} WHERE message_id = ? AND state = 'pending'",
                (iso_utc(published_at), partition, offset, message_id),
            )
            return cur.rowcount == 1

    def mark_failed(self, message_id: str, error: str) -> None:
        with self._lock:
            self._conn.execute("UPDATE outbox SET last_error = ? WHERE message_id = ? AND state = 'pending'",
                               (error[:1000], message_id))

    def reject_pending(self, message_id: str, reason: str, *, dlq: DlqMessage | None = None) -> bool:
        """pending -> rejected (payload dropped, raw kept) plus an optional DLQ row, atomically."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(f"SELECT {_META_COLUMNS} FROM outbox WHERE message_id = ?",
                                         (message_id,)).fetchone()
                cur = self._conn.execute(
                    "UPDATE outbox SET state = 'rejected', reason = ?, payload = NULL "
                    "WHERE message_id = ? AND state = 'pending'",
                    (reason, message_id),
                )
                changed = cur.rowcount == 1
                if changed and dlq is not None and row is not None:
                    self._insert_dlq_locked(dlq, feed_type=row["feed_type"], capture_id=row["capture_id"],
                                            raw_path=row["raw_path"], raw_sha256=row["raw_sha256"],
                                            original_fetched_at=row["original_fetched_at"],
                                            feed_timestamp=row["feed_timestamp"], snapshot_id=row["snapshot_id"],
                                            created=iso_utc(utcnow()))
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
        return changed

    # ------------------------------------------------------------------ status
    def counts(self) -> dict[str, dict[str, int]]:
        out = {k: {s: 0 for s in STATES} for k in KINDS}
        with self._lock:
            for r in self._conn.execute("SELECT kind, state, COUNT(*) AS n FROM outbox GROUP BY kind, state"):
                out.setdefault(r["kind"], {})[r["state"]] = int(r["n"])
        return out

    def pending_count(self) -> int:
        with self._lock:
            return int(self._conn.execute("SELECT COUNT(*) FROM outbox WHERE state = 'pending'").fetchone()[0])

    def status(self) -> dict[str, Any]:
        """Summary used by ``outbox-status``, heartbeats and metrics."""
        with self._lock:
            pending_by_feed = {r["feed_type"]: int(r["n"]) for r in self._conn.execute(
                "SELECT feed_type, COUNT(*) AS n FROM outbox WHERE state = 'pending' GROUP BY feed_type")}
            rejected_by_reason = {str(r["reason"]): int(r["n"]) for r in self._conn.execute(
                "SELECT reason, COUNT(*) AS n FROM outbox WHERE kind = 'snapshot' AND state = 'rejected' "
                "GROUP BY reason")}
            oldest = self._conn.execute(
                "SELECT MIN(created_at) FROM outbox WHERE state = 'pending'").fetchone()[0]
            last_pub = self._conn.execute(
                "SELECT MAX(acked_published_at) FROM outbox WHERE state = 'published'").fetchone()[0]
            failing = int(self._conn.execute(
                "SELECT COUNT(*) FROM outbox WHERE state = 'pending' AND last_error IS NOT NULL").fetchone()[0])
            max_attempts = self._conn.execute(
                "SELECT MAX(attempts) FROM outbox WHERE state = 'pending'").fetchone()[0]
            user_version = int(self._conn.execute("PRAGMA user_version").fetchone()[0])
        counts = self.counts()
        return {
            "db_path": str(self.db_path),
            "schema_version": user_version,
            "counts": counts,
            "pending_total": sum(c.get("pending", 0) for c in counts.values()),
            "pending_by_feed": pending_by_feed,
            "rejected_by_reason": rejected_by_reason,
            "oldest_pending_created_at": oldest,
            "last_acked_published_at": last_pub,
            "pending_with_error": failing,
            "max_pending_attempts": int(max_attempts) if max_attempts is not None else 0,
        }


# ======================================================================================================
# Dispatcher
# ======================================================================================================

class ProducerLike(Protocol):
    generation: int

    def send_envelope(self, env: Any, *, on_ack: Any = None, on_error: Any = None) -> None: ...

    def send_dlq(self, record: dict[str, Any], *, on_ack: Any = None, on_error: Any = None) -> None: ...

    def poll(self, timeout: float = 0.0) -> int: ...

    def flush(self, timeout: float = 30.0) -> int: ...


DlqFactory = Callable[[OutboxRow, str, str], DlqMessage]


class OutboxDispatcher:
    """Sends pending rows in ``seq`` order with at most ``max_in_flight`` unacknowledged messages.

    * a row is marked published only by the delivery callback (``on_ack``);
    * a failed send/delivery records ``last_error`` and pauses the dispatcher with exponential backoff
      (``base * 2**(n-1)`` capped at ``backoff_cap_seconds``); rows stay pending;
    * a size rejection (send-time re-check or broker ``MSG_SIZE_TOO_LARGE``) moves the row to
      ``rejected`` with a durable DLQ row built by ``make_dlq(row, reason, detail)``;
    * when the producer is recreated (``generation`` changes) or a message has been in flight longer
      than ``in_flight_timeout_seconds`` without a report, it becomes eligible for a resend.
    """

    def __init__(
        self,
        outbox: Outbox,
        producer: ProducerLike,
        *,
        make_dlq: DlqFactory,
        max_in_flight: int = 8,
        backoff_base_seconds: float = 1.0,
        backoff_cap_seconds: float = 30.0,
        in_flight_timeout_seconds: float = 90.0,
        on_result: Callable[[str, OutboxRow, str], None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.outbox = outbox
        self.producer = producer
        self.make_dlq = make_dlq
        self.max_in_flight = max(1, int(max_in_flight))
        self.backoff_base = float(backoff_base_seconds)
        self.backoff_cap = float(backoff_cap_seconds)
        self.in_flight_timeout = float(in_flight_timeout_seconds)
        self.on_result = on_result
        self._mono = monotonic
        self._in_flight: dict[str, float] = {}
        self._in_flight_lock = threading.Lock()
        self._failures = 0
        self._blocked_until = 0.0
        self._generation = getattr(producer, "generation", 0)
        self.stats = {"sent": 0, "acked": 0, "failed": 0, "rejected": 0}

    # ------------------------------------------------------------------ callbacks
    def _done(self, message_id: str) -> None:
        with self._in_flight_lock:
            self._in_flight.pop(message_id, None)

    def _notify(self, event: str, row: OutboxRow, detail: str) -> None:
        if self.on_result is not None:
            try:
                self.on_result(event, row, detail)
            except Exception:
                log.exception("dispatcher on_result callback failed")

    def _ack_cb(self, row: OutboxRow) -> Callable[[str, int, int, datetime], None]:
        def on_ack(topic: str, partition: int, offset: int, published_at: datetime) -> None:
            self._done(row.message_id)
            updated = self.outbox.mark_published(row.message_id, published_at=published_at, partition=partition,
                                                 offset=offset)
            self._failures = 0
            self._blocked_until = 0.0
            self.stats["acked"] += 1
            log_event(log, "kafka_ack", "ok" if updated else "reused",
                      f"{row.kind} acknowledged at {topic}[{partition}]@{offset}", feed=row.feed_type,
                      capture_id=row.capture_id, snapshot_id=row.snapshot_id, topic=topic, partition=partition,
                      offset=offset)
            self._notify("acked", row, f"{topic}:{partition}:{offset}")
        return on_ack

    def _error_cb(self, row: OutboxRow) -> Callable[[Any], None]:
        def on_error(err: Any) -> None:
            self._done(row.message_id)
            text = str(err)
            if is_size_error(err):
                self._reject(row, "PAYLOAD_TOO_LARGE", f"broker/producer rejected size: {text}")
                return
            self.outbox.mark_failed(row.message_id, text)
            self.stats["failed"] += 1
            self._backoff()
            log_event(log, "kafka_ack", "retry", f"delivery failed: {text}"[:500], level=logging.WARNING,
                      feed=row.feed_type, capture_id=row.capture_id, snapshot_id=row.snapshot_id,
                      reason="kafka_delivery_failed")
            self._notify("failed", row, text)
        return on_error

    # ------------------------------------------------------------------ helpers
    def _backoff(self) -> None:
        self._failures += 1
        delay = min(self.backoff_cap, self.backoff_base * (2 ** (self._failures - 1)))
        self._blocked_until = max(self._blocked_until, self._mono() + delay)

    def _reject(self, row: OutboxRow, reason: str, detail: str) -> None:
        dlq = self.make_dlq(row, reason, detail) if row.kind == "snapshot" else None
        changed = self.outbox.reject_pending(row.message_id, reason, dlq=dlq)
        self.stats["rejected"] += 1
        log_event(log, "kafka_send", "rejected", detail[:500], level=logging.ERROR, feed=row.feed_type,
                  capture_id=row.capture_id, snapshot_id=row.snapshot_id, reason=reason, changed=changed)
        self._notify("rejected", row, reason)

    def in_flight(self) -> int:
        with self._in_flight_lock:
            return len(self._in_flight)

    @property
    def blocked_for(self) -> float:
        return max(0.0, self._blocked_until - self._mono())

    def _sync_generation(self) -> None:
        gen = getattr(self.producer, "generation", 0)
        if gen != self._generation:
            with self._in_flight_lock:
                lost = len(self._in_flight)
                self._in_flight.clear()
            self._generation = gen
            if lost:
                log_event(log, "kafka_send", "retry", f"{lost} in-flight message(s) will be re-sent after "
                          f"producer recreation", level=logging.WARNING, reason="producer_recreated")

    def _expire_in_flight(self, now: float) -> None:
        with self._in_flight_lock:
            stale = [m for m, t in self._in_flight.items() if now - t > self.in_flight_timeout]
            for m in stale:
                self._in_flight.pop(m, None)
        for m in stale:
            log_event(log, "kafka_send", "retry", "no delivery report in time; row eligible for resend",
                      level=logging.WARNING, reason="in_flight_timeout", message_id=m)

    # ------------------------------------------------------------------ main loop
    def _send(self, row: OutboxRow) -> bool:
        """Send one row. Returns False when the loop should stop sending for this step."""
        payload = self.outbox.load_payload(row.message_id)
        if payload is None:
            return True  # no longer pending (acked/rejected concurrently)
        self.outbox.mark_attempt(row.message_id)
        with self._in_flight_lock:
            self._in_flight[row.message_id] = self._mono()
        try:
            if row.kind == "snapshot":
                self.producer.send_envelope(payload, on_ack=self._ack_cb(row), on_error=self._error_cb(row))
            else:
                self.producer.send_dlq(payload, on_ack=self._ack_cb(row), on_error=self._error_cb(row))
        except PayloadTooLarge as exc:
            self._done(row.message_id)
            if row.kind == "snapshot":
                self._reject(row, "PAYLOAD_TOO_LARGE", f"send-time size {exc.size} > limit {exc.limit}")
            else:
                self._reject(row, "PAYLOAD_TOO_LARGE", f"dlq record {exc.size} > limit {exc.limit}")
            return True
        except BufferError as exc:
            self._done(row.message_id)
            self.outbox.mark_failed(row.message_id, f"local producer queue full: {exc}")
            return False
        except Exception as exc:  # KafkaException and friends: transient until proven otherwise
            self._done(row.message_id)
            self.outbox.mark_failed(row.message_id, f"{type(exc).__name__}: {exc}")
            self.stats["failed"] += 1
            self._backoff()
            log_event(log, "kafka_send", "retry", f"produce failed: {exc}"[:500], level=logging.WARNING,
                      feed=row.feed_type, capture_id=row.capture_id, reason="kafka_produce_failed")
            return False
        self.stats["sent"] += 1
        log_event(log, "kafka_send", "ok", f"{row.kind} produced", level=logging.DEBUG, feed=row.feed_type,
                  capture_id=row.capture_id, snapshot_id=row.snapshot_id)
        return True

    def step(self, poll_timeout: float = 0.1) -> int:
        """One iteration: send eligible pending rows, then serve delivery reports. Returns rows sent."""
        self._sync_generation()
        now = self._mono()
        self._expire_in_flight(now)
        sent = 0
        if now >= self._blocked_until:
            with self._in_flight_lock:
                free = self.max_in_flight - len(self._in_flight)
                busy = list(self._in_flight)
            if free > 0:
                for row in self.outbox.pending_rows(free, exclude=busy):
                    if not self._send(row):
                        break
                    sent += 1
        try:
            self.producer.poll(poll_timeout)
        except Exception as exc:
            log.warning("producer poll raised: %s", exc)
        return sent

    def run(self, stop: threading.Event, *, idle_sleep: float = 0.5) -> None:
        """Loop until ``stop`` is set (the caller drains separately with :meth:`drain`)."""
        while not stop.is_set():
            try:
                sent = self.step(poll_timeout=0.1)
            except Exception:
                log.exception("dispatcher step failed")
                sent = 0
                stop.wait(1.0)
            if sent == 0 and self.in_flight() == 0:
                stop.wait(min(idle_sleep, max(0.05, self.blocked_for or idle_sleep)))

    def drain(self, timeout: float) -> int:
        """Keep sending until nothing is pending/in flight or ``timeout`` elapses; returns pending rows left."""
        deadline = self._mono() + max(0.0, timeout)
        while self._mono() < deadline:
            self.step(poll_timeout=0.2)
            if self.outbox.pending_count() == 0 and self.in_flight() == 0:
                break
            if self.blocked_for > 0:
                time.sleep(min(self.blocked_for, max(0.0, deadline - self._mono()), 0.5))
        try:
            self.producer.flush(max(0.0, min(5.0, deadline - self._mono())))
        except Exception as exc:
            log.warning("producer flush during drain raised: %s", exc)
        return self.outbox.pending_count()
