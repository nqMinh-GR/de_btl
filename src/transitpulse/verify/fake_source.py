"""Local fake of the three MBTA realtime endpoints for SIMULATED source faults (design E §7.2, D-E28).

Runs that use it are synthetic runs with ``scenario_id="simulated_source_fault:<case>"`` — a fake server is never
presented as MBTA. Each feed serves the *current* scripted :class:`FakeResponse`; the test switches responses
explicitly (no time-based script), so every phase is observable before the next one starts.

Responses: a JSON body (optionally served once with 200 and then ``304 Not Modified``, so a re-poll never creates
a new capture), malformed bytes, a slow response longer than the collector's read timeout, an HTTP error with
``Retry-After``, or 304. Every request is logged with the kind of response it received.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

FEED_PATHS: dict[str, str] = {
    "vehicle_positions": "/realtime/VehiclePositions.json",
    "trip_updates": "/realtime/TripUpdates.json",
    "alerts": "/realtime/Alerts.json",
}
_FEED_BY_PATH = {v: k for k, v in FEED_PATHS.items()}


@dataclass(frozen=True)
class FakeResponse:
    kind: str = "body"                       # body | malformed | timeout | http_error | not_modified
    status: int = 200
    body: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)
    delay_seconds: float = 0.0
    once: bool = False                       # serve 200 once, then 304 Not Modified

    @classmethod
    def json(cls, doc: Mapping[str, Any], *, once: bool = True) -> FakeResponse:
        return cls("body", 200, body_bytes(doc), {"Content-Type": "application/json"}, 0.0, once)

    @classmethod
    def raw(cls, body: bytes, *, once: bool = True, content_type: str = "application/json") -> FakeResponse:
        return cls("body", 200, body, {"Content-Type": content_type}, 0.0, once)

    @classmethod
    def malformed(cls, text: bytes = b'{"header": {"gtfs_realtime_version": "2.0", "timestamp": ', *,
                  once: bool = False) -> FakeResponse:
        return cls("malformed", 200, text, {"Content-Type": "application/json"}, 0.0, once)

    @classmethod
    def timeout(cls, delay_seconds: float = 6.0) -> FakeResponse:
        return cls("timeout", 200, b"{}", {"Content-Type": "application/json"}, delay_seconds, False)

    @classmethod
    def http_error(cls, status: int = 503, retry_after: int | None = None) -> FakeResponse:
        headers = {"Content-Type": "text/plain"}
        if retry_after is not None:
            headers["Retry-After"] = str(retry_after)
        return cls("http_error", status, b"simulated upstream error", headers, 0.0, False)

    @classmethod
    def not_modified(cls) -> FakeResponse:
        return cls("not_modified", 304, b"", {}, 0.0, False)


@dataclass
class RequestLog:
    feed: str
    at: datetime
    served: str           # kind actually served (body / not_modified / malformed / timeout / http_error)
    status: int
    if_none_match: str | None
    phase: int
    completed: bool = False


def gtfs_rt_doc(header_timestamp: int, entities: list[dict[str, Any]]) -> dict[str, Any]:
    return {"header": {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET",
                       "timestamp": int(header_timestamp)}, "entity": entities}


def vp_entity(vehicle_id: str, *, route_id: str, lat: float, lon: float, timestamp: int | None,
              trip_id: str | None = None, start_date: str | None = None, extra: Mapping[str, Any] | None = None
              ) -> dict[str, Any]:
    trip: dict[str, Any] = {"route_id": route_id}
    if trip_id:
        trip["trip_id"] = trip_id
    if start_date:
        trip["start_date"] = start_date
    vehicle: dict[str, Any] = {"trip": trip, "vehicle": {"id": vehicle_id, "label": vehicle_id},
                               "position": {"latitude": lat, "longitude": lon}}
    if timestamp is not None:
        vehicle["timestamp"] = int(timestamp)
    ent: dict[str, Any] = {"id": f"vp-{vehicle_id}", "vehicle": vehicle}
    if extra:
        ent.update(extra)
    return ent


def body_bytes(doc: Mapping[str, Any]) -> bytes:
    return json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


class _Handler(BaseHTTPRequestHandler):
    server_version = "TransitPulseFakeSource/1"
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - signature fixed by the base class
        return

    def do_GET(self) -> None:  # noqa: N802 - name fixed by BaseHTTPRequestHandler
        owner: FakeMbtaSource = self.server.owner  # type: ignore[attr-defined]
        feed = _FEED_BY_PATH.get(self.path.split("?", 1)[0])
        if feed is None:
            self._send(404, b"not found", {"Content-Type": "text/plain"})
            return
        resp, served_before, phase = owner._take(feed)
        entry = RequestLog(feed, datetime.now(UTC), resp.kind, resp.status, self.headers.get("If-None-Match"), phase)
        if resp.once and served_before:
            entry.served, entry.status = "not_modified", 304
        owner._log(entry)
        if resp.delay_seconds > 0:
            time.sleep(resp.delay_seconds)
        try:
            if entry.status == 304:
                self._send(304, b"", {})
            else:
                self._send(resp.status, resp.body, dict(resp.headers))
            entry.completed = True
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            entry.completed = False

    def _send(self, status: int, body: bytes, headers: Mapping[str, str]) -> None:
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if body and status != 304:
            self.wfile.write(body)
        self.wfile.flush()


class FakeMbtaSource:
    """Threaded loopback HTTP server with one scripted response per feed."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self._lock = threading.Lock()
        self._responses: dict[str, FakeResponse] = {f: FakeResponse.not_modified() for f in FEED_PATHS}
        self._served: dict[str, bool] = {f: False for f in FEED_PATHS}
        self._phase: dict[str, int] = {f: 0 for f in FEED_PATHS}
        self._log_entries: list[RequestLog] = []
        self._server = ThreadingHTTPServer((host, port), _Handler)
        self._server.daemon_threads = True
        self._server.owner = self  # type: ignore[attr-defined]
        self._thread: threading.Thread | None = None

    # ---- lifecycle
    def start(self) -> FakeMbtaSource:
        if self._thread is None:
            self._thread = threading.Thread(target=self._server.serve_forever, name="fake-mbta", daemon=True)
            self._thread.start()
        return self

    def stop(self) -> None:
        if self._thread is not None:
            self._server.shutdown()
            self._thread.join(10)
            self._thread = None
        self._server.server_close()

    def __enter__(self) -> FakeMbtaSource:
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.stop()

    # ---- addresses
    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def url(self, feed: str) -> str:
        return self.base_url + FEED_PATHS[feed]

    def collector_env(self, *, poll_seconds: float = 2, read_timeout_seconds: float = 2,
                      max_attempts: int = 1) -> dict[str, str]:
        """Config overrides (``TP__section__key``) that point a collector process at this server."""
        env = {
            "TP__SOURCES__VEHICLE_POSITIONS_URL": self.url("vehicle_positions"),
            "TP__SOURCES__TRIP_UPDATES_URL": self.url("trip_updates"),
            "TP__SOURCES__ALERTS_URL": self.url("alerts"),
            "TP__COLLECTOR__HTTP__READ_TIMEOUT_SECONDS": str(read_timeout_seconds),
            "TP__COLLECTOR__HTTP__CONNECT_TIMEOUT_SECONDS": str(read_timeout_seconds),
            "TP__COLLECTOR__HTTP__MAX_ATTEMPTS": str(max_attempts),
            "TP__COLLECTOR__HEARTBEAT_SECONDS": "5",
        }
        for feed in FEED_PATHS:
            env[f"TP__COLLECTOR__POLL_INTERVAL_SECONDS__{feed.upper()}"] = str(poll_seconds)
        return env

    # ---- scripting
    def set(self, feed: str, response: FakeResponse) -> int:
        """Switch ``feed`` to ``response``; returns the new phase number."""
        with self._lock:
            self._responses[feed] = response
            self._served[feed] = False
            self._phase[feed] += 1
            return self._phase[feed]

    def _take(self, feed: str) -> tuple[FakeResponse, bool, int]:
        with self._lock:
            resp = self._responses[feed]
            before = self._served[feed]
            if resp.kind == "body" and resp.status == 200:
                self._served[feed] = True
            return resp, before, self._phase[feed]

    def _log(self, entry: RequestLog) -> None:
        with self._lock:
            self._log_entries.append(entry)

    # ---- observation
    def requests(self, feed: str | None = None, *, phase: int | None = None) -> list[RequestLog]:
        with self._lock:
            return [e for e in self._log_entries
                    if (feed is None or e.feed == feed) and (phase is None or e.phase == phase)]

    def wait_for(self, feed: str, *, served: str, phase: int | None = None, n: int = 1, timeout: float = 60.0,
                 completed: bool | None = None) -> list[RequestLog]:
        deadline = time.monotonic() + timeout
        while True:
            hits = [e for e in self.requests(feed, phase=phase)
                    if e.served == served and (completed is None or e.completed == completed)]
            if len(hits) >= n:
                return hits
            if time.monotonic() >= deadline:
                raise TimeoutError(f"fake source: {n} x {served} for {feed} (phase {phase}) not seen in {timeout}s; "
                                   f"log={[(e.served, e.phase) for e in self.requests(feed)][-10:]}")
            time.sleep(0.2)
