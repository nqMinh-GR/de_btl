"""Unit tests for the HTTP fetcher with simulated source faults (httpx.MockTransport, no network).

These are NOT observations of the real MBTA endpoints: every response below is scripted.
"""

from __future__ import annotations

import gzip
import random
import threading
from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest

from transitpulse.ingestion.http_fetch import (
    FeedFetcher,
    FetchSettings,
    backoff_delay,
    header_subset,
    parse_retry_after,
)

URL = "https://cdn.mbta.com/realtime/VehiclePositions.json"
BODY = b'{"header":{"gtfs_realtime_version":"2.0","timestamp":1791201630},"entity":[]}'


def settings(**over: object) -> FetchSettings:
    base: dict[str, object] = dict(
        connect_timeout_seconds=5.0, read_timeout_seconds=20.0, write_timeout_seconds=5.0, pool_timeout_seconds=5.0,
        max_attempts=3, backoff_base_seconds=1.0, backoff_cap_seconds=10.0, retry_after_max_seconds=300.0,
        max_response_bytes=1024, user_agent="TransitPulse-MBTA/test",
    )
    base.update(over)
    return FetchSettings(**base)  # type: ignore[arg-type]


class Harness:
    """A fetcher over a scripted transport; records requests and the backoff sleeps it asked for."""

    def __init__(self, handler: Callable[[httpx.Request], httpx.Response], **over: object) -> None:
        self.requests: list[httpx.Request] = []
        self.sleeps: list[float] = []

        def record(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            return handler(request)

        self.fetcher = FeedFetcher("vehicle_positions", URL, settings(**over), transport=httpx.MockTransport(record),
                                   rng=random.Random(7), sleep=self.sleeps.append)


def test_ok_response_returns_body_headers_and_sends_user_agent() -> None:
    h = Harness(lambda r: httpx.Response(200, content=BODY, headers={"ETag": '"v1"', "Content-Type": "application/json",
                                                                     "Set-Cookie": "secret=1"}))
    res = h.fetcher.fetch()
    assert res.ok and res.outcome == "ok"
    assert res.body == BODY and res.http_status == 200 and res.attempts == 1
    assert res.headers["etag"] == '"v1"' and "set-cookie" not in res.headers
    assert res.fetched_at.tzinfo is not None
    assert h.requests[0].headers["user-agent"] == "TransitPulse-MBTA/test"
    assert "if-none-match" not in h.requests[0].headers


def test_content_encoding_is_removed_before_hashing() -> None:
    h = Harness(lambda r: httpx.Response(200, content=gzip.compress(BODY), headers={"Content-Encoding": "gzip"}))
    res = h.fetcher.fetch()
    assert res.ok and res.body == BODY
    assert res.headers["content-encoding"] == "gzip"


def test_conditional_get_and_304() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.headers.get("if-none-match") == '"v1"':
            return httpx.Response(304)
        return httpx.Response(200, content=BODY, headers={"ETag": '"v1"'})

    h = Harness(handler)
    first = h.fetcher.fetch()
    assert first.ok
    second = h.fetcher.fetch(etag=first.headers["etag"], last_modified="Mon, 05 Oct 2026 12:00:00 GMT")
    assert second.outcome == "not_modified" and second.body is None and not second.failed
    assert h.requests[1].headers["if-modified-since"] == "Mon, 05 Oct 2026 12:00:00 GMT"
    assert second.attempts == 1 and h.sleeps == []


def test_503_with_short_retry_after_is_retried_then_succeeds() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503, content=b"busy", headers={"Retry-After": "2"})
        return httpx.Response(200, content=BODY)

    h = Harness(handler)
    res = h.fetcher.fetch()
    assert res.ok and res.attempts == 2
    assert h.sleeps == [2.0]                          # max(jittered backoff <= 1.5 s, Retry-After 2 s)
    assert res.attempt_log[0].http_status == 503 and res.attempt_log[0].retry_after_s == 2.0


def test_long_retry_after_is_returned_to_the_scheduler_instead_of_sleeping() -> None:
    h = Harness(lambda r: httpx.Response(503, content=b"busy", headers={"Retry-After": "120"}))
    res = h.fetcher.fetch()
    assert res.outcome == "http_error" and res.failed
    assert res.retry_after_s == 120.0 and res.attempts == 1 and h.sleeps == []
    assert res.body is None and res.error_body == b"busy" and res.error_kind == "http"


def test_retry_after_is_clamped_to_the_maximum() -> None:
    h = Harness(lambda r: httpx.Response(429, headers={"Retry-After": "100000"}))
    res = h.fetcher.fetch()
    assert res.retry_after_s == 300.0


def test_retry_after_parsing() -> None:
    now = datetime(2026, 10, 21, 7, 27, 0, tzinfo=UTC)
    assert parse_retry_after("Wed, 21 Oct 2026 07:28:00 GMT", now=now, max_seconds=300) == 60.0
    assert parse_retry_after("Wed, 21 Oct 2026 07:00:00 GMT", now=now, max_seconds=300) == 0.0
    assert parse_retry_after("3", now=now, max_seconds=300) == 3.0
    assert parse_retry_after("soon", now=now, max_seconds=300) is None
    assert parse_retry_after(None, now=now, max_seconds=300) is None
    assert parse_retry_after("", now=now, max_seconds=300) is None


def test_timeouts_are_retried_with_backoff_then_fail_without_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    h = Harness(handler)
    res = h.fetcher.fetch()
    assert res.outcome == "timeout" and res.error_kind == "timeout"
    assert res.attempts == 3 and len(h.requests) == 3 and len(h.sleeps) == 2
    assert 0.5 <= h.sleeps[0] <= 1.5 and 1.0 <= h.sleeps[1] <= 3.0
    assert res.body is None and res.failed


def test_dns_failure_is_classified() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("[Errno -2] Name or service not known", request=request)

    res = Harness(handler, max_attempts=1).fetcher.fetch()
    assert res.outcome == "connect_error" and res.error_kind == "dns" and res.attempts == 1


def test_connection_refused_is_retried() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("[Errno 111] Connection refused", request=request)

    h = Harness(handler, max_attempts=2)
    res = h.fetcher.fetch()
    assert res.outcome == "connect_error" and res.error_kind == "connect" and res.attempts == 2


@pytest.mark.parametrize("status", [400, 403, 404, 301])
def test_non_transient_statuses_are_not_retried(status: int) -> None:
    h = Harness(lambda r: httpx.Response(status, content=b"nope"))
    res = h.fetcher.fetch()
    assert res.outcome == "http_error" and res.http_status == status and res.attempts == 1 and h.sleeps == []


def test_declared_oversize_body_is_too_large() -> None:
    h = Harness(lambda r: httpx.Response(200, content=b"x" * 2048))
    res = h.fetcher.fetch()
    assert res.outcome == "too_large" and res.body is None and res.attempts == 1


def test_streamed_oversize_body_without_content_length_is_too_large() -> None:
    h = Harness(lambda r: httpx.Response(200, content=iter([b"a" * 600, b"b" * 600])))
    res = h.fetcher.fetch()
    assert res.outcome == "too_large" and res.error_kind == "too_large"


def test_stop_event_interrupts_the_backoff() -> None:
    stop = threading.Event()
    stop.set()
    h = Harness(lambda r: httpx.Response(503))
    res = h.fetcher.fetch(stop=stop)
    assert res.attempts == 1 and h.sleeps == []


def test_backoff_delay_bounds() -> None:
    rng = random.Random(1)
    for attempt in range(1, 8):
        raw = min(10.0, 1.0 * 2 ** (attempt - 1))
        for _ in range(20):
            d = backoff_delay(attempt, base=1.0, cap=10.0, rng=rng)
            assert 0.5 * raw <= d <= 1.5 * raw


def test_header_subset_keeps_only_known_headers() -> None:
    hs = header_subset(httpx.Headers({"ETag": "x", "Authorization": "secret", "Last-Modified": "y"}))
    assert hs == {"etag": "x", "last-modified": "y"}
