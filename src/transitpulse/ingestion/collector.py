"""Live collector: fetch -> raw archive -> parse -> scope -> envelope -> outbox -> Kafka (design E §1).

Threads
    * one poll thread per feed (:class:`~transitpulse.ingestion.scheduler.PollScheduler`), each with its
      own ``httpx.Client``; an exception in one feed never stops the others;
    * one outbox dispatcher thread (only when publishing) that sends pending rows and marks them
      published from the broker delivery callback;
    * one heartbeat thread (``collector_heartbeat`` every ``collector.heartbeat_seconds``).

Per capture, the durable write order is raw -> sidecar -> outbox insert -> capture-index line, so a
crash at any point is repaired at the next start (:meth:`Collector.reconcile`) or by
``finalize-recording``. Invariants:

* a failed fetch (timeout, connection error, HTTP error, too large) never produces an envelope: only
  a ``collector_status`` event and a ``fetch_event`` index line (the error body, at most 64 KiB, is
  archived under ``errors/`` for audit);
* HTTP 304 produces no raw file, no envelope and no observation;
* a transport rejection (INVALID_JSON, INVALID_HEADER, UNSUPPORTED_SCHEMA, UNSUPPORTED_INCREMENTALITY,
  PAYLOAD_TOO_LARGE) keeps the raw body, writes an outbox row ``rejected`` plus a durable DLQ row,
  emits ``collector_status`` and never becomes an (empty) snapshot;
* exceeding the raw disk budget is fail-stop: captures stop on all feeds, the outbox is drained for at
  most 30 s, the recording is finalized with ``complete=false`` and the process exits with code 3.

Clock/control events: the collector is the lifecycle source of the runs it feeds. It emits
``run_started`` / ``run_finished`` (``clock_sequence`` = epoch milliseconds, strictly increasing even
across restarts of the same run) and, for a ``clock_mode=source`` run (simulated-source-fault runs),
a ``clock_tick`` with the wall clock at every heartbeat.
"""

from __future__ import annotations

import errno
import json
import logging
import os
import shutil
import signal
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from transitpulse import __version__
from transitpulse.config import ProjectConfig
from transitpulse.contracts import control
from transitpulse.contracts.enums import FEED_ABBR, FEED_ORDER
from transitpulse.contracts.envelope import envelope_size, iso_utc, parse_iso_utc, utcnow
from transitpulse.contracts.ids import capture_id as make_capture_id
from transitpulse.contracts.ids import sha256_hex
from transitpulse.ingestion.archive import RawArchive, RawRef, atomic_write_json, iso_us, meta_path_for
from transitpulse.ingestion.disk_budget import BudgetDecision, DiskBudget, DiskUsageFn
from transitpulse.ingestion.envelope_builder import capture_dlq_record, envelope_from_capture, http_meta
from transitpulse.ingestion.feed_parse import PAYLOAD_TOO_LARGE, ParsedFeed, header_summary, parse_feed
from transitpulse.ingestion.http_fetch import FeedFetcher, FetchResult, FetchSettings
from transitpulse.ingestion.kafka_producer import EnvelopeProducer
from transitpulse.ingestion.outbox import DlqMessage, Outbox, OutboxDispatcher, OutboxLocked, OutboxRow
from transitpulse.ingestion.recording import build_manifest, static_info, write_manifest
from transitpulse.ingestion.scheduler import PollScheduler
from transitpulse.ingestion.scope_filter import ScopeFilter, include_unparsed_entities, quality_counts
from transitpulse.obs.logging import log_event
from transitpulse.obs.metrics import ComponentMetrics
from transitpulse.runtime.paths import WorkPaths

log = logging.getLogger(__name__)

FEEDS: tuple[str, ...] = ("vehicle_positions", "trip_updates", "alerts")
COMPONENT = "collector"
MBTA_HOST = "cdn.mbta.com"
SIMULATED_SCENARIO_PREFIX = "simulated_source_fault:"
DEFAULT_POLL_INTERVAL_SECONDS = 30.0

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_PREFLIGHT = 3
EXIT_CONFLICT = 4
EXIT_UNSUPPORTED = 6

META_RESERVE_BYTES = 16 * 1024          # sidecar + index line reserved with every capture
CAPTURE_GRACE_SECONDS = 30.0            # wait for in-progress captures at shutdown
DRAIN_TIMEOUT_DISK_SECONDS = 30.0       # fail-stop drain (design E §1.7)
DRAIN_TIMEOUT_STOP_SECONDS = 10.0       # normal stop drain
STATE_FILE = "collector_state.json"
POLL_STATE_FILE = "poll_state.json"
REASON_SOURCE_FETCH_FAILED = "SOURCE_FETCH_FAILED"
REASON_DISK_BUDGET = "DISK_BUDGET_EXCEEDED"
_NO_SPACE_ERRNOS = frozenset(e for e in (errno.ENOSPC, getattr(errno, "EDQUOT", None)) if e is not None)

_FINISH_REASONS = {
    "duration_budget": "duration_budget",
    "disk_budget": "disk_budget",
    "sigterm": "finish_requested",
    "sigint": "finish_requested",
    "finish_requested": "finish_requested",
}


# ======================================================================================================
# Errors, results, small helpers
# ======================================================================================================

class CollectorError(Exception):
    """Preflight/runtime failure with a CLI error code, hint and exit code."""

    def __init__(self, code: str, message: str, *, hint: str | None = None, exit_code: int = EXIT_FAILURE) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint
        self.exit_code = exit_code

    def to_json(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "hint": self.hint, "log_excerpt": None}


@dataclass
class CollectorResult:
    run_id: str
    publish: bool
    exit_code: int
    reason: str
    complete: bool
    started_at: datetime
    finished_at: datetime
    manifest_path: str | None = None
    manifest_sha256: str | None = None
    item_count: int = 0
    feeds: dict[str, Any] = field(default_factory=dict)
    outbox: dict[str, Any] = field(default_factory=dict)
    disk: dict[str, Any] = field(default_factory=dict)
    reconcile: dict[str, int] = field(default_factory=dict)
    error: dict[str, Any] | None = None

    @property
    def ok(self) -> bool:
        return self.exit_code == EXIT_OK

    def to_json(self, action: str) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "action": action,
            "run_id": self.run_id,
            "publish": self.publish,
            "exit_reason": self.reason,
            "complete": self.complete,
            "started_at": iso_utc(self.started_at),
            "finished_at": iso_utc(self.finished_at),
            "duration_seconds": round((self.finished_at - self.started_at).total_seconds(), 3),
            "recording_manifest": self.manifest_path,
            "manifest_sha256": self.manifest_sha256,
            "item_count": self.item_count,
            "feeds": self.feeds,
            "outbox": self.outbox,
            "disk": self.disk,
            "reconcile": self.reconcile,
            "error": self.error,
        }


@dataclass
class FeedState:
    """Request state of one feed (kept separate from the data state, i.e. the last feed timestamp)."""

    feed_type: str
    source_url: str
    etag: str | None = None
    last_modified: str | None = None
    last_success_fetched_at: datetime | None = None
    last_capture_at: datetime | None = None
    last_feed_timestamp: int | None = None
    consecutive_failures: int = 0
    feed_state: str = "starting"
    last_outcome: str | None = None
    last_http_status: int | None = None
    last_reason_code: str | None = None
    last_attempts: int = 0
    last_error: str | None = None
    ok: int = 0
    not_modified: int = 0
    failed: int = 0
    rejected_by_reason: dict[str, int] = field(default_factory=dict)
    missed_polls: int = 0

    def counters(self) -> dict[str, Any]:
        return {"ok": self.ok, "not_modified": self.not_modified, "failed": self.failed,
                "rejected_by_reason": dict(self.rejected_by_reason), "missed_polls": self.missed_polls}

    def summary(self) -> dict[str, Any]:
        return {
            "feed_state": self.feed_state,
            "last_outcome": self.last_outcome,
            "last_http_status": self.last_http_status,
            "last_reason_code": self.last_reason_code,
            "last_success_fetched_at": iso_utc(self.last_success_fetched_at) if self.last_success_fetched_at else None,
            "last_feed_timestamp": self.last_feed_timestamp,
            "consecutive_failures": self.consecutive_failures,
            "counters": self.counters(),
        }

    def poll_state(self) -> dict[str, Any]:
        return {
            "etag": self.etag,
            "last_modified": self.last_modified,
            "last_success_fetched_at": iso_us(self.last_success_fetched_at) if self.last_success_fetched_at else None,
            "last_feed_timestamp": self.last_feed_timestamp,
        }


@dataclass(frozen=True)
class CaptureOutcome:
    capture_id: str
    status: str                    # "ok" or the transport rejection reason
    detail: str | None
    snapshot_id: str | None
    feed_timestamp: int | None
    selected_entity_count: int | None
    source_entity_count: int | None
    outbox_state: str              # pending | rejected
    raw_ref: RawRef
    envelope_bytes: int | None


class CollectorMetrics:
    """Prometheus metrics of the collector (exported to ``W/metrics/collector.prom``)."""

    def __init__(self, cm: ComponentMetrics) -> None:
        self.cm = cm
        self.http_requests = cm.counter("http_requests_total", "HTTP polls by final outcome", ("feed", "outcome"))
        self.http_bytes = cm.counter("http_response_bytes_total", "Decoded response body bytes", ("feed",))
        self.fetch_duration = cm.histogram("fetch_duration_seconds", "Duration of one poll incl. retries",
                                           ("feed",), buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 40, 80))
        self.captures = cm.counter("captures_total", "Archived captures by transport status",
                                   ("feed", "parse_status"))
        self.outbox_pending = cm.gauge("outbox_pending", "Pending outbox records", ("feed",))
        self.outbox_oldest_age = cm.gauge("outbox_oldest_pending_age_seconds", "Age of the oldest pending record")
        self.kafka_messages = cm.counter("kafka_messages_total", "Outbox messages by delivery result",
                                         ("topic", "result"))
        self.kafka_bytes = cm.counter("kafka_bytes_total", "Acknowledged serialized bytes (without published_at)",
                                      ("topic",))
        self.raw_run_bytes = cm.gauge("raw_run_bytes", "Raw bytes on disk for this run")
        self.raw_budget_bytes = cm.gauge("raw_budget_bytes", "Raw disk budget of the run")
        self.disk_free_bytes = cm.gauge("disk_free_bytes", "Free bytes on the raw filesystem")
        self.last_success = cm.gauge("last_success_timestamp_seconds", "Last successful poll (200 ok or 304)",
                                     ("feed",))
        self.header_age = cm.gauge("feed_header_age_seconds", "fetched_at - header.timestamp of the last capture",
                                   ("feed",))
        self.selected_entities = cm.gauge("selected_entities", "Selected entities in the last capture", ("feed",))
        self.missed_polls = cm.counter("missed_polls_total", "Poll slots skipped because a poll overran", ("feed",))

    def observe_fetch(self, res: FetchResult) -> None:
        self.http_requests.labels(feed=res.feed_type, outcome=res.outcome).inc()
        if res.body is not None:
            self.http_bytes.labels(feed=res.feed_type).inc(len(res.body))
        total_ms = sum(a.elapsed_ms for a in res.attempt_log) or res.elapsed_ms
        self.fetch_duration.labels(feed=res.feed_type).observe(max(0.0, total_ms / 1000.0))

    def start(self, metrics_dir: Path, interval: float) -> None:
        self.cm.start_exporter(metrics_dir, interval)

    def stop(self, metrics_dir: Path) -> None:
        self.cm.stop(metrics_dir)


def poll_intervals(cfg: ProjectConfig, feeds: Sequence[str] = FEEDS) -> dict[str, float]:
    table = cfg.collector.poll_interval_seconds
    return {f: float(table.get(f, DEFAULT_POLL_INTERVAL_SECONDS)) for f in feeds}


def check_source_policy(run: Any, source_urls: Mapping[str, str]) -> None:
    """D-E28: a live run must poll the MBTA CDN; any other host is only allowed for a synthetic run whose
    scenario is ``simulated_source_fault:*``. Replay runs are fed by the replay publisher."""
    source_type = getattr(run, "source_type", None)
    if source_type == "replay":
        raise CollectorError("UNSUPPORTED_SOURCE_TYPE", "replay runs are published by the replay publisher, "
                             "not by the collector", hint="use python -m transitpulse.replay publish",
                             exit_code=EXIT_UNSUPPORTED)
    if source_type == "live":
        bad = sorted(u for u in source_urls.values()
                     if urlparse(u).hostname != MBTA_HOST or urlparse(u).scheme != "https")
        if bad:
            raise CollectorError("LIVE_REQUIRES_MBTA_SOURCE", f"live runs must poll https://{MBTA_HOST}; got {bad}",
                                 hint="restore sources.*_url or create a synthetic run with scenario "
                                      f"'{SIMULATED_SCENARIO_PREFIX}<case>'", exit_code=EXIT_UNSUPPORTED)
        return
    if source_type == "synthetic":
        if not str(getattr(run, "scenario_id", None) or "").startswith(SIMULATED_SCENARIO_PREFIX):
            raise CollectorError("UNSUPPORTED_SOURCE_TYPE", "a synthetic run may use the collector only for a "
                                 f"simulated source fault (scenario_id '{SIMULATED_SCENARIO_PREFIX}*')",
                                 exit_code=EXIT_UNSUPPORTED)
        return
    raise CollectorError("UNSUPPORTED_SOURCE_TYPE", f"unknown source_type {source_type!r}", exit_code=EXIT_UNSUPPORTED)


def check_static_match(run: Any, index: Any) -> None:
    """The scope index must belong to the run's pinned GTFS version and scope."""
    if getattr(index, "gtfs_version", None) != run.gtfs_version or getattr(index, "scope_id", None) != run.scope_id:
        raise CollectorError(
            "STATIC_VERSION_MISMATCH",
            f"scope index ({getattr(index, 'gtfs_version', None)}, {getattr(index, 'scope_id', None)}) does not match "
            f"the run ({run.gtfs_version}, {run.scope_id})",
            hint="the run pins its static version; reload static for that version, never silently switch",
            exit_code=EXIT_PREFLIGHT,
        )
    missing = sorted(set(run.route_ids) - {str(r) for r in index.route_ids})
    if missing:
        raise CollectorError("SCOPE_MISMATCH", f"run route_ids not in the scope index: {missing}",
                             exit_code=EXIT_PREFLIGHT)


def load_collector_state(paths: WorkPaths, run_id: str) -> dict[str, Any] | None:
    path = paths.collector_state_dir(run_id) / STATE_FILE
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (FileNotFoundError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def index_record_from_sidecar(meta: Mapping[str, Any], row: OutboxRow | None = None) -> dict[str, Any]:
    """Capture-index line rebuilt from a sidecar (and the outbox row when one exists)."""
    if row is not None:
        status = (row.reason or "rejected") if row.state == "rejected" else "ok"
        snapshot_id, outbox_state, feed_ts = row.snapshot_id, row.state, row.feed_timestamp
    else:
        status = str(meta.get("capture_status") or meta.get("parse_status") or "unknown")
        snapshot_id, outbox_state = meta.get("snapshot_id"), meta.get("outbox_state")
        header = meta.get("header") if isinstance(meta.get("header"), dict) else {}
        feed_ts = header.get("timestamp")
    fetched_at = parse_iso_utc(str(meta["fetched_at"]))
    return {
        "kind": "capture",
        "feed_type": meta.get("feed_type"),
        "at": iso_utc(fetched_at),
        "fetched_at": iso_us(fetched_at),
        "capture_id": meta.get("capture_id"),
        "raw_path": meta.get("raw_path"),
        "raw_sha256": meta.get("raw_sha256"),
        "raw_bytes": meta.get("raw_bytes"),
        "stored_sha256": meta.get("stored_sha256"),
        "stored_bytes": meta.get("stored_bytes"),
        "parse_status": status,
        "http_status": meta.get("http_status"),
        "feed_timestamp": feed_ts,
        "snapshot_id": snapshot_id,
        "outbox_state": outbox_state,
        "selected_entity_count": meta.get("selected_entity_count"),
        "source_entity_count": meta.get("source_entity_count"),
        "reconstructed": True,
    }


def finalize_recording(
    cfg: ProjectConfig,
    paths: WorkPaths,
    run: Any,
    *,
    complete: bool,
    include_sidecars: bool = False,
    outbox: Outbox | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Write ``W/data/raw/<run_id>/recording_manifest.json`` from the capture index.

    ``include_sidecars`` (crash finalization) also adds captures whose index line is missing, using
    their sidecar and, when available, the outbox row for the final status.
    """
    archive = RawArchive(paths.raw_root)
    run_dir = archive.run_dir(run.run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    records = archive.read_index(run.run_id)
    if include_sidecars:
        known = {r["capture_id"] for r in records if r.get("kind") == "capture" and isinstance(r.get("capture_id"), str)}
        for feed in FEEDS:
            for meta_path in archive.recent_sidecars(run.run_id, feed, n_dates=100_000):
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
                cid = meta.get("capture_id") if isinstance(meta, dict) else None
                if not isinstance(cid, str) or cid in known or "fetched_at" not in meta:
                    continue
                row = outbox.get(cid) if outbox is not None else None
                records.append(index_record_from_sidecar(meta, row))
                known.add(cid)
    state = load_collector_state(paths, run.run_id)
    created_at: datetime = run.created_at
    if state and isinstance(state.get("created_at"), str):
        created_at = parse_iso_utc(state["created_at"])
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    manifest = build_manifest(
        run=run,
        index_records=records,
        source_urls={f: cfg.sources.realtime_url(f) for f in FEEDS},
        poll_intervals=poll_intervals(cfg),
        user_agent=cfg.sources.user_agent,
        route_type=cfg.scope.route_type,
        scope_policy_version=cfg.scope.scope_policy_version,
        created_at=created_at,
        complete=complete,
        static=static_info(run, run_dir),
    )
    return write_manifest(run_dir, manifest), manifest


# ======================================================================================================
# Collector
# ======================================================================================================

class Collector:
    """One collector process for one run. Use :meth:`run` (blocking) or ``open``/``poll_feed``/``close``."""

    def __init__(
        self,
        cfg: ProjectConfig,
        paths: WorkPaths,
        run: Any,
        index: Any,
        *,
        publish: bool = True,
        duration_seconds: float | None = None,
        feeds: Sequence[str] = FEEDS,
        producer_factory: Callable[[], Any] | None = None,
        transport_factory: Callable[[str], httpx.BaseTransport | None] | None = None,
        disk_usage: DiskUsageFn = shutil.disk_usage,
        component: str = COMPONENT,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if duration_seconds is not None and duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive")
        unknown = [f for f in feeds if f not in FEED_ABBR]
        if unknown or not feeds:
            raise ValueError(f"invalid feeds {list(feeds)!r}")
        self.cfg = cfg
        self.paths = paths
        self.run = run
        self.index = index
        self.publish = publish
        self.duration_seconds = duration_seconds
        self.component = component
        self.feeds = tuple(sorted(feeds, key=lambda f: FEED_ORDER[f]))
        self.source_urls = {f: cfg.sources.realtime_url(f) for f in self.feeds}
        self.intervals = poll_intervals(cfg, self.feeds)
        self.max_snapshot_bytes = int(cfg.kafka.max_snapshot_bytes)
        self.archive = RawArchive(paths.raw_root)
        self.scope = ScopeFilter(index)
        self.metrics = CollectorMetrics(ComponentMetrics(component))
        self._fetch_settings = FetchSettings.from_config(cfg.collector.http, cfg.sources.user_agent)
        self._producer_factory = producer_factory
        self._transport_factory = transport_factory
        self._disk_usage = disk_usage
        self._mono = monotonic
        self.feed_states = {f: FeedState(feed_type=f, source_url=self.source_urls[f]) for f in self.feeds}
        self.outbox: Outbox | None = None
        self.producer: Any = None
        self.dispatcher: OutboxDispatcher | None = None
        self.budget: DiskBudget | None = None
        self.scheduler: PollScheduler | None = None
        self.reconcile_stats: dict[str, int] = {}
        self._fetchers: dict[str, FeedFetcher] = {}
        self._stop = threading.Event()
        self._stop_reason: str | None = None
        self._pending_signal: str | None = None
        self._state_lock = threading.Lock()
        self._poll_state_lock = threading.Lock()
        self._seq_lock = threading.Lock()
        self._gate = threading.Condition()
        self._closing = False
        self._active = 0
        self._last_seq = 0
        self._last_source_clock: datetime | None = None
        self._disk_failure: dict[str, Any] | None = None
        self._threads: list[threading.Thread] = []
        self._dispatcher_thread: threading.Thread | None = None
        self._opened = False
        self._closed = False
        self.started_at = utcnow()
        self._started_mono = self._mono()
        self._result: CollectorResult | None = None

    # ------------------------------------------------------------------ properties
    @property
    def run_id(self) -> str:
        return self.run.run_id

    @property
    def stop_reason(self) -> str | None:
        return self._stop_reason

    def request_stop(self, reason: str) -> None:
        """Ask every loop to stop; the first reason wins."""
        if self._stop_reason is None:
            self._stop_reason = reason
        self._stop.set()

    # ------------------------------------------------------------------ preflight / open
    def open(self) -> None:
        """Preflight checks, outbox lock, disk budget, poll state, restart reconciliation, producer."""
        if self._opened:
            return
        check_source_policy(self.run, self.source_urls)
        check_static_match(self.run, self.index)
        try:
            self.outbox = Outbox(self.paths.outbox_db(self.run_id), run_id=self.run_id,
                                 keep_published_payload=self.cfg.collector.outbox.keep_published_payload)
        except OutboxLocked as exc:
            raise CollectorError("OUTBOX_LOCKED", str(exc), hint="another collector is already running for this "
                                 "run; stop it before starting a new one", exit_code=EXIT_CONFLICT) from exc
        try:
            run_dir = self.archive.run_dir(self.run_id)
            run_dir.mkdir(parents=True, exist_ok=True)
            self.archive.quarantine_orphans(self.run_id)
            self.budget = DiskBudget(run_dir, budget_bytes=self.cfg.collector.raw_disk_budget_bytes,
                                     min_free_bytes=self.cfg.collector.min_free_bytes,
                                     warning_fraction=self.cfg.collector.disk_warning_fraction,
                                     disk_usage=self._disk_usage)
            decision = self.budget.check()
            if not decision.ok:
                raise CollectorError(
                    REASON_DISK_BUDGET,
                    f"raw disk budget already exhausted ({decision.reason}): run_bytes={decision.run_bytes} "
                    f"budget={decision.budget_bytes} free={decision.free_bytes} min_free={decision.min_free_bytes}",
                    hint="free disk space or start a new run; raw data is never deleted automatically",
                    exit_code=EXIT_PREFLIGHT,
                )
            if self.budget.status()["state"] == "warning":
                log_event(log, "disk_check", "ok", "raw disk usage already above the warning threshold",
                          level=logging.WARNING, reason="DISK_BUDGET_WARNING", disk=self.budget.status())
            self._load_poll_state()
            self._record_start()
            self.reconcile_stats = self.reconcile()
            if self.publish:
                factory = self._producer_factory or (lambda: EnvelopeProducer(self.cfg, self.component, self.run_id))
                self.producer = factory()
                ob = self.cfg.collector.outbox
                self.dispatcher = OutboxDispatcher(
                    self.outbox, self.producer, make_dlq=self._dlq_for_row, max_in_flight=ob.max_in_flight,
                    backoff_cap_seconds=ob.retry_backoff_cap_seconds,
                    in_flight_timeout_seconds=self.cfg.kafka.producer.message_timeout_ms / 1000.0 + 30.0,
                    on_result=self._on_dispatch_result,
                )
        except BaseException:
            self.outbox.close()
            self.outbox = None
            raise
        self._opened = True
        log_event(log, "preflight", "ok", "collector ready", publish=self.publish, feeds=list(self.feeds),
                  reconcile=self.reconcile_stats, intervals=self.intervals)

    # ------------------------------------------------------------------ state files
    def _state_dir(self) -> Path:
        d = self.paths.collector_state_dir(self.run_id)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _load_poll_state(self) -> None:
        path = self._state_dir() / POLL_STATE_FILE
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        except (FileNotFoundError, ValueError):
            return
        if not isinstance(data, dict):
            return
        for feed, st in self.feed_states.items():
            d = data.get(feed)
            if not isinstance(d, dict):
                continue
            st.etag = d.get("etag") if isinstance(d.get("etag"), str) else None
            st.last_modified = d.get("last_modified") if isinstance(d.get("last_modified"), str) else None
            if isinstance(d.get("last_success_fetched_at"), str):
                try:
                    st.last_success_fetched_at = parse_iso_utc(d["last_success_fetched_at"])
                except ValueError:
                    st.last_success_fetched_at = None
            ts = d.get("last_feed_timestamp")
            st.last_feed_timestamp = ts if isinstance(ts, int) and not isinstance(ts, bool) else None

    def _save_poll_state(self) -> None:
        with self._poll_state_lock:
            with self._state_lock:
                data = {f: st.poll_state() for f, st in self.feed_states.items()}
            try:
                atomic_write_json(self._state_dir() / POLL_STATE_FILE, data, indent=2)
            except OSError as exc:
                log.warning("cannot persist poll state: %s", exc)

    def _record_start(self) -> None:
        state = load_collector_state(self.paths, self.run_id) or {
            "run_id": self.run_id, "created_at": iso_us(self.started_at), "starts": []}
        starts = state.get("starts") if isinstance(state.get("starts"), list) else []
        starts = starts[-49:] + [{"started_at": iso_us(self.started_at), "pid": os.getpid(),
                                  "publish": self.publish, "version": __version__}]
        state["starts"] = starts
        atomic_write_json(self._state_dir() / STATE_FILE, state, indent=2)

    def _record_exit(self, result: CollectorResult) -> None:
        state = load_collector_state(self.paths, self.run_id) or {
            "run_id": self.run_id, "created_at": iso_us(self.started_at), "starts": []}
        state["last_exit"] = {"finished_at": iso_us(result.finished_at), "reason": result.reason,
                              "exit_code": result.exit_code, "complete": result.complete}
        try:
            atomic_write_json(self._state_dir() / STATE_FILE, state, indent=2)
        except OSError as exc:
            log.warning("cannot persist collector state: %s", exc)

    # ------------------------------------------------------------------ restart reconciliation
    def reconcile(self) -> dict[str, int]:
        """Re-enqueue captures whose sidecar has no outbox row; append missing index lines."""
        assert self.outbox is not None
        stats = {"checked": 0, "rebuilt": 0, "indexed": 0, "failed": 0}
        indexed = self.archive.indexed_capture_ids(self.run_id)
        for feed in self.feeds:
            for meta_path in self.archive.recent_sidecars(self.run_id, feed):
                stats["checked"] += 1
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    cid = str(meta["capture_id"])
                    row = self.outbox.get(cid)
                    if row is not None:
                        if cid not in indexed:
                            self.archive.append_index(self.run_id, index_record_from_sidecar(meta, row))
                            indexed.add(cid)
                            stats["indexed"] += 1
                        continue
                    raw_ref = RawRef(raw_path=str(meta["raw_path"]), raw_sha256=str(meta["raw_sha256"]),
                                     raw_bytes=int(meta["raw_bytes"]), stored_sha256=str(meta["stored_sha256"]),
                                     stored_bytes=int(meta["stored_bytes"]))
                    body = self.archive.read_capture(raw_ref.raw_path, expected_sha256=raw_ref.raw_sha256)
                    fetched_at = parse_iso_utc(str(meta["fetched_at"]))
                    http = meta.get("http") if isinstance(meta.get("http"), dict) else dict(
                        http_meta(meta.get("http_status"), meta.get("headers")))
                    self._process_body(feed, body, fetched_at, http=http, fetch_meta={}, capture_id=cid,
                                       raw_ref=raw_ref, reserved=0, append_index=cid not in indexed,
                                       http_status=meta.get("http_status"))
                    indexed.add(cid)
                    stats["rebuilt"] += 1
                except Exception as exc:
                    stats["failed"] += 1
                    log_event(log, "archive", "failed", f"reconciliation of {meta_path.name} failed: {exc}"[:500],
                              level=logging.ERROR, feed=feed, reason="reconcile_failed")
        if stats["rebuilt"] or stats["indexed"] or stats["failed"]:
            log_event(log, "archive", "ok", "restart reconciliation done", level=logging.WARNING, **stats)
        return stats

    # ------------------------------------------------------------------ capture gate (shutdown safety)
    def _enter_capture(self) -> bool:
        with self._gate:
            if self._closing:
                return False
            self._active += 1
            return True

    def _leave_capture(self) -> None:
        with self._gate:
            self._active -= 1
            self._gate.notify_all()

    def _close_gate(self, timeout: float) -> bool:
        deadline = self._mono() + timeout
        with self._gate:
            self._closing = True
            while self._active > 0:
                remaining = deadline - self._mono()
                if remaining <= 0:
                    return False
                self._gate.wait(remaining)
        return True

    # ------------------------------------------------------------------ polling
    def _fetcher(self, feed: str) -> FeedFetcher:
        fetcher = self._fetchers.get(feed)
        if fetcher is None:
            transport = self._transport_factory(feed) if self._transport_factory is not None else None
            fetcher = FeedFetcher(feed, self.source_urls[feed], self._fetch_settings, transport=transport)
            self._fetchers[feed] = fetcher
        return fetcher

    def poll_feed(self, feed: str) -> float | None:
        """One poll of ``feed`` (scheduler task). Returns the server-requested deferral, if any."""
        if not self._opened:
            raise RuntimeError("collector is not open")
        if self._stop.is_set():
            return None
        st = self.feed_states[feed]
        with self._state_lock:
            etag, last_modified = st.etag, st.last_modified
        res = self._fetcher(feed).fetch(etag=etag, last_modified=last_modified, stop=self._stop)
        self.metrics.observe_fetch(res)
        log_event(log, "fetch", "ok" if not res.failed else "failed",
                  f"{res.outcome} http={res.http_status} attempts={res.attempts}", feed=feed,
                  duration_ms=round(res.elapsed_ms, 3), reason=res.error_kind, bytes=res.body_bytes,
                  level=logging.INFO if not res.failed else logging.WARNING)
        if not self._enter_capture():
            log_event(log, "fetch", "skipped", "collector stopping; response discarded", feed=feed)
            return None
        try:
            try:
                if res.outcome == "ok":
                    self._on_capture(feed, res)
                elif res.outcome == "not_modified":
                    self._on_not_modified(feed, res)
                else:
                    self._on_failed_fetch(feed, res)
            except OSError as exc:
                if exc.errno in _NO_SPACE_ERRNOS:
                    self._disk_fail_stop(feed, detail=f"write failed: {exc}")
                    return None
                raise
        finally:
            self._leave_capture()
        return res.retry_after_s

    def _fetch_meta(self, feed: str, res: FetchResult, http: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "source_url": self.source_urls[feed],
            "http_status": res.http_status,
            "headers": dict(res.headers),
            "http": dict(http),
            "elapsed_ms": round(res.elapsed_ms, 3),
            "attempts": res.attempts,
            "attempt_log": [a.to_json() for a in res.attempt_log],
            "collector_version": __version__,
            "gtfs_version": self.run.gtfs_version,
            "scope_id": self.run.scope_id,
            "run_source_type": self.run.source_type,
        }

    def _on_capture(self, feed: str, res: FetchResult) -> None:
        assert self.budget is not None and res.body is not None
        body = res.body
        cid = make_capture_id(FEED_ABBR[feed], res.fetched_at, sha256_hex(body))
        reserve = len(body) + META_RESERVE_BYTES
        decision = self.budget.try_reserve(reserve)
        if decision.warning:
            self._disk_warning(decision)
        if not decision.ok:
            self._append_fetch_event(feed, res, outcome="disk_budget_exceeded", reason_code=REASON_DISK_BUDGET)
            self._disk_fail_stop(feed, decision=decision)
            return
        http = dict(http_meta(res.http_status, dict(res.headers)))
        outcome = self._process_body(feed, body, res.fetched_at, http=http, fetch_meta=self._fetch_meta(feed, res, http),
                                     capture_id=cid, raw_ref=None, reserved=reserve, append_index=True,
                                     http_status=res.http_status)
        st = self.feed_states[feed]
        with self._state_lock:
            st.last_http_status = res.http_status
            st.last_attempts = res.attempts
            st.last_capture_at = res.fetched_at
            if outcome.status == "ok":
                st.ok += 1
                st.consecutive_failures = 0
                st.feed_state = "healthy"
                st.last_outcome = "ok"
                st.last_reason_code = None
                st.last_error = None
                st.last_success_fetched_at = res.fetched_at
                st.last_feed_timestamp = outcome.feed_timestamp
                st.etag = res.headers.get("etag")
                st.last_modified = res.headers.get("last-modified")
            else:
                st.rejected_by_reason[outcome.status] = st.rejected_by_reason.get(outcome.status, 0) + 1
                st.consecutive_failures += 1
                st.feed_state = "degraded"
                st.last_outcome = "rejected"
                st.last_reason_code = outcome.status
                st.last_error = outcome.detail
                # Never keep validators of a rejected body: the next poll must fetch a fresh copy.
                st.etag = None
                st.last_modified = None
        self._save_poll_state()
        if outcome.status == "ok":
            self.metrics.last_success.labels(feed=feed).set(res.fetched_at.timestamp())
        self._send_status(feed, outcome="ok" if outcome.status == "ok" else "rejected",
                          http_status=res.http_status,
                          reason_code=None if outcome.status == "ok" else outcome.status, attempts=res.attempts)

    def _process_body(
        self,
        feed: str,
        body: bytes,
        fetched_at: datetime,
        *,
        http: dict[str, Any],
        fetch_meta: dict[str, Any],
        capture_id: str,
        raw_ref: RawRef | None,
        reserved: int,
        append_index: bool,
        http_status: int | None,
    ) -> CaptureOutcome:
        """Archive (unless ``raw_ref`` is given), parse, scope, build the envelope and enqueue it."""
        assert self.outbox is not None and self.budget is not None
        t0 = self._mono()
        result = parse_feed(body, fetched_at)
        parsed: ParsedFeed | None
        if isinstance(result, ParsedFeed):
            parsed = result
            selected, accounting = self.scope.select(feed, parsed.entities)
            accounting = include_unparsed_entities(accounting, parsed.source_entity_count)
            status, detail = "ok", None
        else:
            parsed = None
            selected, accounting = [], {}
            status, detail = result.reason_code, result.detail
        if raw_ref is None:
            meta = dict(fetch_meta)
            meta.update(
                parse_status=status,
                parse_detail=detail,
                header=header_summary(parsed),
                source_entity_count=parsed.source_entity_count if parsed else None,
                selected_entity_count=len(selected) if parsed else None,
                scope_accounting=accounting or None,
                quality_counts=quality_counts(feed, accounting) if parsed else {},
                parse_warnings=dict(parsed.warnings) if parsed else {},
            )
            raw_ref = self.archive.write_capture(self.run_id, feed, capture_id, fetched_at, body, meta)
            self.budget.settle(reserved, raw_ref.stored_bytes + self._sidecar_bytes(raw_ref))
        topic = self.cfg.kafka.topics.for_feed(feed)
        key = f"{self.run_id}|{feed}"
        snapshot_id: str | None = None
        env_bytes: int | None = None
        outbox_state = "rejected"
        if parsed is not None:
            env = envelope_from_capture(
                run=self.run, feed_type=feed, parsed=parsed, selected=selected, accounting=accounting,
                raw_ref=raw_ref, capture_id=capture_id, fetched_at=fetched_at, source_url=self.source_urls[feed],
                http=http, scenario_id=getattr(self.run, "scenario_id", None), component=self.component,
            )
            snapshot_id = env["snapshot_id"]
            env_bytes = envelope_size(env)
            if env_bytes > self.max_snapshot_bytes:
                status = PAYLOAD_TOO_LARGE
                detail = (f"envelope {env_bytes} bytes > limit {self.max_snapshot_bytes} bytes "
                          f"({len(selected)} selected entities); nothing truncated, raw kept")
                self.outbox.insert_rejected(
                    capture_id=capture_id, feed_type=feed, reason=status, topic=topic, kafka_key=key,
                    raw_path=raw_ref.raw_path, raw_sha256=raw_ref.raw_sha256, original_fetched_at=iso_utc(fetched_at),
                    feed_timestamp=parsed.feed_timestamp, snapshot_id=snapshot_id, payload_bytes=env_bytes,
                    dlq=self._capture_dlq(feed, status, detail, capture_id, raw_ref, snapshot_id, None),
                )
            else:
                self.outbox.insert_snapshot(env, topic=topic, kafka_key=key)
                outbox_state = "pending"
        else:
            self.outbox.insert_rejected(
                capture_id=capture_id, feed_type=feed, reason=status, topic=topic, kafka_key=key,
                raw_path=raw_ref.raw_path, raw_sha256=raw_ref.raw_sha256, original_fetched_at=iso_utc(fetched_at),
                dlq=self._capture_dlq(feed, status, detail or status, capture_id, raw_ref, None, body),
            )
        self.archive.update_meta(raw_ref.raw_path, {"capture_status": status, "status_detail": detail,
                                                    "snapshot_id": snapshot_id, "envelope_bytes": env_bytes,
                                                    "outbox_state": outbox_state})
        feed_ts = parsed.feed_timestamp if parsed else None
        if append_index:
            self.archive.append_index(self.run_id, {
                "kind": "capture",
                "feed_type": feed,
                "at": iso_utc(fetched_at),
                "fetched_at": iso_us(fetched_at),
                "capture_id": capture_id,
                "raw_path": raw_ref.raw_path,
                "raw_sha256": raw_ref.raw_sha256,
                "raw_bytes": raw_ref.raw_bytes,
                "stored_sha256": raw_ref.stored_sha256,
                "stored_bytes": raw_ref.stored_bytes,
                "parse_status": status,
                "http_status": http_status,
                "feed_timestamp": feed_ts,
                "snapshot_id": snapshot_id,
                "outbox_state": outbox_state,
                "selected_entity_count": len(selected) if parsed else None,
                "source_entity_count": parsed.source_entity_count if parsed else None,
            })
        self.metrics.captures.labels(feed=feed, parse_status=status).inc()
        if parsed is not None:
            self.metrics.selected_entities.labels(feed=feed).set(len(selected))
            self.metrics.header_age.labels(feed=feed).set(fetched_at.timestamp() - parsed.feed_timestamp)
        log_event(log, "outbox_insert", "ok" if status == "ok" else "rejected",
                  f"capture {status}: selected={len(selected)} source={parsed.source_entity_count if parsed else None}",
                  level=logging.INFO if status == "ok" else logging.WARNING, feed=feed, capture_id=capture_id,
                  snapshot_id=snapshot_id, reason=None if status == "ok" else status,
                  duration_ms=round((self._mono() - t0) * 1000.0, 3), envelope_bytes=env_bytes,
                  scope_accounting=accounting, detail=detail)
        return CaptureOutcome(capture_id=capture_id, status=status, detail=detail, snapshot_id=snapshot_id,
                              feed_timestamp=feed_ts, selected_entity_count=len(selected) if parsed else None,
                              source_entity_count=parsed.source_entity_count if parsed else None,
                              outbox_state=outbox_state, raw_ref=raw_ref, envelope_bytes=env_bytes)

    def _sidecar_bytes(self, raw_ref: RawRef) -> int:
        try:
            return self.archive.resolve(meta_path_for(raw_ref.raw_path)).stat().st_size
        except (OSError, ValueError):
            return 0

    def _on_not_modified(self, feed: str, res: FetchResult) -> None:
        st = self.feed_states[feed]
        with self._state_lock:
            st.not_modified += 1
            st.consecutive_failures = 0
            st.feed_state = "healthy"
            st.last_outcome = "not_modified"
            st.last_http_status = res.http_status
            st.last_reason_code = None
            st.last_error = None
            st.last_attempts = res.attempts
            st.last_success_fetched_at = res.fetched_at
        self._append_fetch_event(feed, res, outcome="not_modified", reason_code=None)
        self._save_poll_state()
        self.metrics.last_success.labels(feed=feed).set(res.fetched_at.timestamp())
        self._send_status(feed, outcome="not_modified", http_status=res.http_status, reason_code=None,
                          attempts=res.attempts)

    def _on_failed_fetch(self, feed: str, res: FetchResult) -> None:
        assert self.budget is not None
        error_path: str | None = None
        refused: BudgetDecision | None = None
        if res.error_body:
            decision = self.budget.try_reserve(len(res.error_body) + META_RESERVE_BYTES)
            if decision.warning:
                self._disk_warning(decision)
            if decision.ok:
                ts = res.fetched_at.astimezone(UTC).strftime("%Y%m%dT%H%M%S%f")
                attempt_id = f"{FEED_ABBR[feed]}-{ts}Z-e{min(res.attempts, 999):02d}"
                error_path = self.archive.write_error_body(
                    self.run_id, feed, res.fetched_at, attempt_id, res.error_body,
                    {"source_url": self.source_urls[feed], "http_status": res.http_status,
                     "headers": dict(res.headers), "outcome": res.outcome, "error": res.error,
                     "attempts": res.attempts, "attempt_log": [a.to_json() for a in res.attempt_log],
                     "collector_version": __version__},
                )
            else:
                refused = decision
        interval = self.intervals[feed]
        st = self.feed_states[feed]
        with self._state_lock:
            st.failed += 1
            st.consecutive_failures += 1
            st.feed_state = "backoff" if (res.retry_after_s or 0) > interval else "degraded"
            st.last_outcome = res.outcome
            st.last_http_status = res.http_status
            st.last_reason_code = REASON_SOURCE_FETCH_FAILED
            st.last_error = res.error
            st.last_attempts = res.attempts
        self._append_fetch_event(feed, res, outcome=res.outcome, reason_code=REASON_SOURCE_FETCH_FAILED,
                                 error_body_path=error_path)
        self._send_status(feed, outcome=res.outcome, http_status=res.http_status,
                          reason_code=REASON_SOURCE_FETCH_FAILED, attempts=res.attempts, error=res.error)
        if refused is not None:
            self._disk_fail_stop(feed, decision=refused)

    def _append_fetch_event(self, feed: str, res: FetchResult, *, outcome: str, reason_code: str | None,
                            error_body_path: str | None = None) -> None:
        record = {
            "kind": "fetch_event",
            "feed_type": feed,
            "at": iso_utc(res.fetched_at),
            "fetched_at": iso_us(res.fetched_at),
            "outcome": outcome,
            "http_status": res.http_status,
            "reason_code": reason_code,
            "attempts": res.attempts,
            "error": res.error,
            "error_kind": res.error_kind,
            "error_body_path": error_body_path,
            "retry_after_s": res.retry_after_s,
        }
        self.archive.append_index(self.run_id, record)
        if self.budget is not None:
            self.budget.add(len(json.dumps(record)) + 1)

    # ------------------------------------------------------------------ disk budget
    def _disk_warning(self, decision: BudgetDecision) -> None:
        log_event(log, "disk_check", "ok", "raw disk usage crossed the warning threshold", level=logging.WARNING,
                  reason="DISK_BUDGET_WARNING", decision=decision.to_json())
        for feed in self.feeds:
            self._send_status(feed, outcome="disk_budget_warning", http_status=None, reason_code=None, attempts=0,
                              extra={"disk": decision.to_json(), "warning_code": "DISK_BUDGET_WARNING"})

    def _disk_fail_stop(self, feed: str, *, decision: BudgetDecision | None = None, detail: str | None = None) -> None:
        with self._state_lock:
            first = self._disk_failure is None
            if first:
                self._disk_failure = {"feed_type": feed, "decision": decision.to_json() if decision else None,
                                      "detail": detail}
            for st in self.feed_states.values():
                st.feed_state = "failed"
                st.last_reason_code = REASON_DISK_BUDGET
        if first:
            log_event(log, "disk_check", "failed", "raw disk budget exceeded: fail-stop (no further captures)",
                      level=logging.ERROR, feed=feed, reason=REASON_DISK_BUDGET,
                      decision=decision.to_json() if decision else None, detail=detail)
            for f in self.feeds:
                self._send_status(f, outcome="disk_budget_exceeded", http_status=None, reason_code=REASON_DISK_BUDGET,
                                  attempts=0, extra={"disk": decision.to_json() if decision else {"detail": detail}})
        self.request_stop("disk_budget")

    # ------------------------------------------------------------------ DLQ helpers
    def _capture_dlq(self, feed: str, reason: str, detail: str, capture_id: str, raw_ref: RawRef,
                     snapshot_id: str | None, body: bytes | None) -> DlqMessage:
        record = capture_dlq_record(
            run_id=self.run_id, feed_type=feed, topic=self.cfg.kafka.topics.for_feed(feed), reason_code=reason,
            detail=detail, capture_id=capture_id, raw_path=raw_ref.raw_path, raw_sha256=raw_ref.raw_sha256,
            max_excerpt_chars=self.cfg.kafka.dlq_excerpt_max_chars, pipeline_version=self.cfg.project.pipeline_version,
            stage="collector_envelope_size" if snapshot_id is not None else "collector_parse",
            snapshot_id=snapshot_id, body=body,
        )
        return DlqMessage(record=record, topic=self.cfg.kafka.topics.dlq, kafka_key=f"{self.run_id}|{feed}")

    def _dlq_for_row(self, row: OutboxRow, reason: str, detail: str) -> DlqMessage:
        record = capture_dlq_record(
            run_id=row.run_id, feed_type=row.feed_type, topic=row.topic, reason_code=reason, detail=detail,
            capture_id=row.capture_id, raw_path=row.raw_path, raw_sha256=row.raw_sha256,
            max_excerpt_chars=self.cfg.kafka.dlq_excerpt_max_chars, pipeline_version=self.cfg.project.pipeline_version,
            stage="collector_send", snapshot_id=row.snapshot_id,
        )
        return DlqMessage(record=record, topic=self.cfg.kafka.topics.dlq, kafka_key=f"{row.run_id}|{row.feed_type}")

    def _on_dispatch_result(self, event: str, row: OutboxRow, detail: str) -> None:
        self.metrics.kafka_messages.labels(topic=row.topic, result=event).inc()
        if event == "acked" and row.payload_bytes:
            self.metrics.kafka_bytes.labels(topic=row.topic).inc(row.payload_bytes)

    # ------------------------------------------------------------------ control events
    def _send_control(self, event: dict[str, Any]) -> None:
        if self.producer is None:
            return
        try:
            self.producer.send_control(event)
        except Exception as exc:  # fire-and-forget: the next heartbeat repairs a lost status
            log_event(log, "kafka_send", "failed", f"control event not sent: {exc}"[:500], level=logging.WARNING,
                      reason="control_send_failed", event_type=event.get("event_type"))

    def _outbox_pending(self) -> int | None:
        try:
            return self.outbox.pending_count() if self.outbox is not None else None
        except Exception:
            return None

    def _send_status(self, feed: str, *, outcome: str, http_status: int | None, reason_code: str | None,
                     attempts: int, error: str | None = None, extra: dict[str, Any] | None = None) -> None:
        if self.producer is None:  # record mode: no Kafka, the capture index and logs keep the history
            return
        st = self.feed_states[feed]
        with self._state_lock:
            fields: dict[str, Any] = {
                "feed_type": feed,
                "outcome": outcome,
                "http_status": http_status,
                "reason_code": reason_code,
                "attempts": attempts,
                "last_success_fetched_at": iso_utc(st.last_success_fetched_at) if st.last_success_fetched_at else None,
                "last_feed_timestamp": st.last_feed_timestamp,
                "consecutive_failures": st.consecutive_failures,
                "feed_state": st.feed_state,
                "counters": st.counters(),
                "source_url": st.source_url,
                "error": error,
                "replayed": False,
                "historical_at": None,
            }
        fields["outbox_pending"] = self._outbox_pending()
        if extra:
            fields.update(extra)
        self._send_control(control.status_event(control.COLLECTOR_STATUS, run_id=self.run_id,
                                                source_type=self.run.source_type, component=self.component,
                                                version=__version__, fields=fields))

    def _next_clock_sequence(self) -> int:
        with self._seq_lock:
            seq = max(int(time.time() * 1000), self._last_seq + 1)
            self._last_seq = seq
            return seq

    def _source_clock(self) -> datetime:
        clock = utcnow()
        initial = getattr(self.run, "initial_clock", None)
        if getattr(self.run, "clock_mode", "wall") == "source" and isinstance(initial, datetime) and initial > clock:
            clock = initial
        if self._last_source_clock is not None and self._last_source_clock > clock:
            clock = self._last_source_clock
        self._last_source_clock = clock
        return clock

    def _emit_clock(self, event_type: str, *, reason: str | None = None, run_state: str | None = None) -> None:
        if self.producer is None:
            return
        source_mode = getattr(self.run, "clock_mode", "wall") == "source"
        clock = self._source_clock()
        extra: dict[str, Any] = {"clock_mode": getattr(self.run, "clock_mode", None)}
        if event_type == control.RUN_STARTED:
            extra.update(timeline_start=iso_utc(clock) if source_mode else None,
                         recording_id=getattr(self.run, "recording_id", None) or self.run_id,
                         scenario_id=getattr(self.run, "scenario_id", None), scope_id=self.run.scope_id,
                         gtfs_version=self.run.gtfs_version)
        try:
            event = control.clock_event(event_type, run_id=self.run_id, source_type=self.run.source_type,
                                        clock_sequence=self._next_clock_sequence(), source_clock=clock,
                                        component=self.component, version=__version__,
                                        speed=1.0 if source_mode else None, run_state=run_state, reason=reason,
                                        extra=extra)
        except ValueError as exc:
            log.error("cannot build %s event: %s", event_type, exc)
            return
        self._send_control(event)

    def _emit_heartbeat(self) -> None:
        with self._state_lock:
            feeds = {f: st.summary() for f, st in self.feed_states.items()}
        outbox_counts: dict[str, Any] = {}
        try:
            if self.outbox is not None:
                outbox_counts = self.outbox.counts()
        except Exception as exc:
            log.warning("outbox counts unavailable: %s", exc)
        fields = {
            "feeds": feeds,
            "outbox_pending": self._outbox_pending(),
            "outbox": outbox_counts,
            "disk": self.budget.status() if self.budget is not None else None,
            "publish_enabled": self.publish,
            "uptime_seconds": round(self._mono() - self._started_mono, 3),
            "run_state": "STOPPING" if self._stop.is_set() else "RUNNING",
            "reconcile": self.reconcile_stats,
        }
        if self.producer is not None:
            self._send_control(control.status_event(control.COLLECTOR_HEARTBEAT, run_id=self.run_id,
                                                    source_type=self.run.source_type, component=self.component,
                                                    version=__version__, fields=fields))
        log_event(log, "heartbeat", "ok", "collector heartbeat", level=logging.DEBUG,
                  outbox_pending=fields["outbox_pending"])

    def _update_gauges(self) -> None:
        if self.outbox is not None:
            status = self.outbox.status()
            for feed in self.feeds:
                self.metrics.outbox_pending.labels(feed=feed).set(status["pending_by_feed"].get(feed, 0))
            oldest = status.get("oldest_pending_created_at")
            age = (utcnow() - parse_iso_utc(oldest)).total_seconds() if isinstance(oldest, str) else 0.0
            self.metrics.outbox_oldest_age.set(max(0.0, age))
        if self.budget is not None:
            self.metrics.raw_run_bytes.set(self.budget.run_bytes)
            self.metrics.raw_budget_bytes.set(self.budget.budget_bytes)
            free = self.budget.free_bytes()
            if free is not None:
                self.metrics.disk_free_bytes.set(free)

    def _heartbeat_loop(self) -> None:
        interval = max(1.0, float(self.cfg.collector.heartbeat_seconds))
        while not self._stop.wait(interval):
            try:
                self._update_gauges()
                self._emit_heartbeat()
                if getattr(self.run, "clock_mode", "wall") == "source":
                    self._emit_clock(control.CLOCK_TICK)
            except Exception:
                log.exception("heartbeat iteration failed")

    def _on_missed(self, feed: str, missed: int) -> None:
        with self._state_lock:
            self.feed_states[feed].missed_polls += missed
        self.metrics.missed_polls.labels(feed=feed).inc(missed)
        log_event(log, "fetch", "skipped", f"{missed} poll slot(s) skipped (no catch-up burst)", feed=feed,
                  level=logging.WARNING, reason="missed_poll")

    def _on_poll_error(self, feed: str, exc: BaseException) -> None:
        with self._state_lock:
            st = self.feed_states[feed]
            if st.feed_state != "failed":
                st.feed_state = "degraded"
            st.consecutive_failures += 1
            st.last_outcome = "collector_error"
            st.last_error = f"{type(exc).__name__}: {exc}"[:500]
        self._send_status(feed, outcome="collector_error", http_status=None, reason_code=None, attempts=0,
                          error=f"{type(exc).__name__}: {exc}"[:500])

    # ------------------------------------------------------------------ lifecycle
    def _signal_handler(self, name: str) -> Callable[[int, Any], None]:
        def handler(signum: int, frame: Any) -> None:
            # Only set a flag here: taking locks inside a signal handler can deadlock the main thread.
            self._pending_signal = name
        return handler

    def _install_signals(self) -> dict[int, Any]:
        if threading.current_thread() is not threading.main_thread():
            return {}
        previous: dict[int, Any] = {}
        for signum, name in ((signal.SIGTERM, "sigterm"), (signal.SIGINT, "sigint")):
            try:
                previous[signum] = signal.signal(signum, self._signal_handler(name))
            except (ValueError, OSError):
                continue
        return previous

    @staticmethod
    def _restore_signals(previous: Mapping[int, Any]) -> None:
        for signum, handler in previous.items():
            try:
                signal.signal(signum, handler if handler is not None else signal.SIG_DFL)
            except (ValueError, OSError, TypeError):
                continue

    def _start_threads(self) -> None:
        if self.dispatcher is not None:
            self._dispatcher_thread = threading.Thread(target=self.dispatcher.run, args=(self._stop,),
                                                       name="outbox-dispatcher", daemon=True)
            self._dispatcher_thread.start()
        hb = threading.Thread(target=self._heartbeat_loop, name="collector-heartbeat", daemon=True)
        hb.start()
        self._threads.append(hb)
        self.scheduler = PollScheduler(self.feeds, self.intervals, self.poll_feed, self._stop,
                                       on_missed=self._on_missed, on_error=self._on_poll_error,
                                       monotonic=self._mono)
        self.scheduler.start()

    def _wait(self) -> None:
        deadline = None if self.duration_seconds is None else self._mono() + self.duration_seconds
        while not self._stop.is_set():
            if self._pending_signal is not None:
                self.request_stop(self._pending_signal)
                break
            if deadline is not None and self._mono() >= deadline:
                self.request_stop("duration_budget")
                break
            timeout = 0.5 if deadline is None else max(0.0, min(0.5, deadline - self._mono()))
            self._stop.wait(timeout)

    def run(self) -> CollectorResult:
        """Blocking main loop: preflight, threads, wait for stop (signal/duration/disk), shutdown."""
        self.started_at = utcnow()
        self._started_mono = self._mono()
        try:
            self.open()
        except CollectorError as exc:
            log_event(log, "preflight", "failed", exc.message, level=logging.ERROR, reason=exc.code)
            now = utcnow()
            return CollectorResult(run_id=self.run_id, publish=self.publish, exit_code=exc.exit_code,
                                   reason="preflight_failed", complete=False, started_at=self.started_at,
                                   finished_at=now, error=exc.to_json())
        previous = self._install_signals()
        error: dict[str, Any] | None = None
        try:
            self.metrics.start(self.paths.metrics_dir, float(self.cfg.observability.metrics_export_seconds))
            self._emit_clock(control.RUN_STARTED)
            self._start_threads()
            self._wait()
        except Exception as exc:
            log.exception("collector failed")
            error = {"code": "COLLECTOR_FAILED", "message": f"{type(exc).__name__}: {exc}"[:500],
                     "hint": "see the collector log", "log_excerpt": None}
            self.request_stop("error")
        finally:
            try:
                result = self.close(error=error)
            finally:
                self._restore_signals(previous)
        return result

    def close(self, *, error: dict[str, Any] | None = None) -> CollectorResult:
        """Stop polling, drain the outbox, emit final events, finalize the recording, release the lock."""
        if self._result is not None:
            return self._result
        if self._stop_reason is None:
            self._stop_reason = "finish_requested"
        reason = self._stop_reason
        self._stop.set()
        if not self._close_gate(CAPTURE_GRACE_SECONDS):
            log.warning("in-progress captures did not finish within %.0fs", CAPTURE_GRACE_SECONDS)
        alive = self.scheduler.join(2.0) if self.scheduler is not None else []
        for t in self._threads:
            t.join(2.0)
        pending_left: int | None = None
        if self.dispatcher is not None:
            if self._dispatcher_thread is not None:
                self._dispatcher_thread.join(5.0)
            drain_timeout = DRAIN_TIMEOUT_DISK_SECONDS if reason == "disk_budget" else DRAIN_TIMEOUT_STOP_SECONDS
            try:
                pending_left = self.dispatcher.drain(drain_timeout)
            except Exception as exc:
                log.warning("outbox drain failed: %s", exc)
        disk_failed = self._disk_failure is not None
        complete = error is None and not disk_failed and reason in _FINISH_REASONS and reason != "disk_budget"
        if self._opened:
            try:
                self._update_gauges()
            except Exception as exc:
                log.warning("final gauge update failed: %s", exc)
            self._emit_heartbeat()
            self._emit_clock(control.RUN_FINISHED, reason=_FINISH_REASONS.get(reason, "collector_error"),
                             run_state="FAILED" if error is not None else None)
        if self.producer is not None:
            try:
                self.producer.flush(5.0)
                self.producer.close()
            except Exception as exc:
                log.warning("producer close failed: %s", exc)
        manifest_path: str | None = None
        manifest_sha: str | None = None
        item_count = 0
        if self._opened:
            try:
                path, manifest = finalize_recording(self.cfg, self.paths, self.run, complete=complete)
                manifest_path, manifest_sha, item_count = str(path), manifest["manifest_sha256"], len(manifest["items"])
            except Exception as exc:
                log.exception("recording manifest finalization failed")
                if error is None:
                    error = {"code": "MANIFEST_FAILED", "message": f"{type(exc).__name__}: {exc}"[:500],
                             "hint": ("run: python -m transitpulse.ingestion finalize-recording "
                                      f"--workdir {self.paths.root} --run-id {self.run_id}"),
                             "log_excerpt": None}
        outbox_status: dict[str, Any] = {}
        if self.outbox is not None:
            try:
                outbox_status = self.outbox.status()
            except Exception as exc:
                log.warning("outbox status failed: %s", exc)
        if pending_left is not None:
            outbox_status["pending_after_drain"] = pending_left
        for feed, fetcher in self._fetchers.items():
            if feed not in alive:
                fetcher.close()
        if disk_failed:
            exit_code = EXIT_PREFLIGHT
            if error is None:
                error = {"code": REASON_DISK_BUDGET, "message": "raw disk budget exceeded; collector stopped (fail-stop)",
                         "hint": "free disk space or start a new run; raw data is never deleted automatically",
                         "log_excerpt": None, "details": self._disk_failure}
        elif error is not None:
            exit_code = EXIT_FAILURE
        else:
            exit_code = EXIT_OK
        with self._state_lock:
            feeds = {f: st.summary() for f, st in self.feed_states.items()}
        result = CollectorResult(
            run_id=self.run_id, publish=self.publish, exit_code=exit_code, reason=reason, complete=complete,
            started_at=self.started_at, finished_at=utcnow(), manifest_path=manifest_path,
            manifest_sha256=manifest_sha, item_count=item_count, feeds=feeds, outbox=outbox_status,
            disk=self.budget.status() if self.budget is not None else {}, reconcile=self.reconcile_stats,
            error=error,
        )
        if self._opened:
            self._record_exit(result)
        if self.outbox is not None:
            self.outbox.close()
            self.outbox = None
        try:
            self.metrics.stop(self.paths.metrics_dir)
        except Exception as exc:
            log.warning("final metrics export failed: %s", exc)
        log_event(log, "shutdown", "ok" if exit_code == EXIT_OK else "failed",
                  f"collector stopped: {reason}", level=logging.INFO if exit_code == EXIT_OK else logging.ERROR,
                  reason=reason, complete=complete, exit_code=exit_code)
        self._closed = True
        self._result = result
        return result


# ======================================================================================================
# Entry points used by the CLI
# ======================================================================================================

def load_run_record(paths: WorkPaths, run_id: str) -> Any:
    """Load ``W/runs/<run_id>/run.json`` through ``transitpulse.runs.load_run`` (WP3)."""
    from transitpulse.runs import load_run

    try:
        return load_run(paths, run_id)
    except FileNotFoundError as exc:
        raise CollectorError("RUN_NOT_FOUND", f"run {run_id} not found: {exc}",
                             hint="create the run first (scripts/demo.py start)", exit_code=EXIT_PREFLIGHT) from exc


def load_run_context(paths: WorkPaths, run_id: str) -> tuple[Any, Any]:
    """Load the run record (WP3) and the run's ``scope_index.json`` (WP1, ``static.index.load_scope_index``)."""
    from transitpulse.static.index import load_scope_index

    run = load_run_record(paths, run_id)
    try:
        # require_complete: the scope directory must carry the loader's _SUCCESS marker (published scope).
        index = load_scope_index(Path(run.static_scope_dir), require_complete=True)
    except FileNotFoundError as exc:
        raise CollectorError("SCOPE_INDEX_NOT_FOUND", f"scope index missing for run {run_id}: {exc}",
                             hint=("load static data first: python -m transitpulse.static load "
                                   f"--workdir {paths.root} --zip {run.static_zip_path} "
                                   f"--route-ids {','.join(run.route_ids)}"),
                             exit_code=EXIT_PREFLIGHT) from exc
    return run, index


def run_collector(cfg: ProjectConfig, paths: WorkPaths, run_id: str, *, publish: bool,
                  duration_seconds: float | None) -> CollectorResult:
    started = utcnow()
    try:
        run, index = load_run_context(paths, run_id)
    except CollectorError as exc:
        return CollectorResult(run_id=run_id, publish=publish, exit_code=exc.exit_code, reason="preflight_failed",
                               complete=False, started_at=started, finished_at=utcnow(), error=exc.to_json())
    collector = Collector(cfg, paths, run, index, publish=publish, duration_seconds=duration_seconds)
    return collector.run()
