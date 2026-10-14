"""HTTP fetching of the MBTA GTFS-Realtime JSON feeds (design E §1.1, decision D-E01).

One :class:`FeedFetcher` (and therefore one ``httpx.Client``) per feed thread. A fetch is one logical
poll made of at most ``max_attempts`` HTTP attempts:

* conditional GET (``If-None-Match`` / ``If-Modified-Since``) with the validators of the last 200;
  a ``304`` is ``outcome="not_modified"`` and carries no body (it is never a new observation);
* only transient failures are retried: timeouts, connection/transport errors and HTTP 429, 500,
  502, 503, 504. Other statuses (403, 404, 3xx, ...) fail immediately;
* backoff before attempt ``k+1`` is ``min(cap, base * 2**(k-1)) * U(0.5, 1.5)``;
* ``Retry-After`` (delta-seconds or HTTP-date) is honoured up to ``retry_after_max_seconds``. A value
  longer than the backoff cap stops the retry loop and is returned in ``retry_after_s`` so that the
  scheduler defers the next poll instead of sleeping inside the fetch;
* the body is streamed and decoded (Content-Encoding removed) with a hard cap of
  ``max_response_bytes`` decoded bytes (``outcome="too_large"``), which also bounds gzip bombs;
* every attempt has a wall deadline of connect + read + write + pool timeouts, because the httpx read
  timeout applies per chunk and a slowly dripping body could otherwise block a feed thread forever.

A failed fetch never produces a body for a capture: the collector turns it into a status event only.
"""

from __future__ import annotations

import email.utils
import logging
import random
import re
import socket
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import httpx

from transitpulse.config import HttpSection

Outcome = Literal["ok", "not_modified", "http_error", "timeout", "connect_error", "too_large"]
ErrorKind = Literal["dns", "connect", "timeout", "http", "too_large", "decode"]

RETRYABLE_STATUS: frozenset[int] = frozenset({429, 500, 502, 503, 504})
ERROR_BODY_MAX_BYTES = 64 * 1024
CHUNK_SIZE = 64 * 1024
KEPT_HEADERS: tuple[str, ...] = (
    "content-type",
    "content-encoding",
    "content-length",
    "etag",
    "last-modified",
    "date",
    "age",
    "cache-control",
    "retry-after",
)

_DNS_MARKERS = (
    "name or service not known",
    "nodename nor servname",
    "temporary failure in name resolution",
    "getaddrinfo failed",
    "no address associated with hostname",
    "name resolution",
)
_DELTA_SECONDS = re.compile(r"^\d+(?:\.\d+)?$")

log = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class FetchSettings:
    """Effective HTTP settings of one fetcher (built from ``collector.http`` + ``sources.user_agent``)."""

    connect_timeout_seconds: float
    read_timeout_seconds: float
    write_timeout_seconds: float
    pool_timeout_seconds: float
    max_attempts: int
    backoff_base_seconds: float
    backoff_cap_seconds: float
    retry_after_max_seconds: float
    max_response_bytes: int
    user_agent: str

    @classmethod
    def from_config(cls, http: HttpSection, user_agent: str, *, max_attempts: int | None = None,
                    max_response_bytes: int | None = None) -> FetchSettings:
        return cls(
            connect_timeout_seconds=float(http.connect_timeout_seconds),
            read_timeout_seconds=float(http.read_timeout_seconds),
            write_timeout_seconds=float(http.write_timeout_seconds),
            pool_timeout_seconds=float(http.pool_timeout_seconds),
            max_attempts=int(max_attempts if max_attempts is not None else http.max_attempts),
            backoff_base_seconds=float(http.backoff_base_seconds),
            backoff_cap_seconds=float(http.backoff_cap_seconds),
            retry_after_max_seconds=float(http.retry_after_max_seconds),
            max_response_bytes=int(max_response_bytes if max_response_bytes is not None else http.max_response_bytes),
            user_agent=user_agent,
        )

    @property
    def attempt_deadline_seconds(self) -> float:
        return (self.connect_timeout_seconds + self.read_timeout_seconds + self.write_timeout_seconds
                + self.pool_timeout_seconds)

    def timeout(self) -> httpx.Timeout:
        return httpx.Timeout(
            connect=self.connect_timeout_seconds,
            read=self.read_timeout_seconds,
            write=self.write_timeout_seconds,
            pool=self.pool_timeout_seconds,
        )


@dataclass(frozen=True)
class AttemptRecord:
    """Audit record of one HTTP attempt inside a fetch."""

    attempt: int
    started_at: datetime
    outcome: str
    http_status: int | None
    elapsed_ms: float
    error: str | None
    retry_after_s: float | None

    def to_json(self) -> dict[str, object]:
        return {
            "attempt": self.attempt,
            "started_at": self.started_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "outcome": self.outcome,
            "http_status": self.http_status,
            "elapsed_ms": round(self.elapsed_ms, 3),
            "error": self.error,
            "retry_after_s": self.retry_after_s,
        }


@dataclass(frozen=True)
class FetchResult:
    """Result of one logical poll (after retries).

    ``body`` is the decoded entity body and is set only for ``outcome == "ok"``. ``fetched_at`` is the
    UTC wall time at which the final response headers were received (or the final attempt ended for a
    failure). ``error_body`` keeps at most 64 KiB of the final non-2xx body for the audit archive.
    """

    feed_type: str
    url: str
    outcome: Outcome
    http_status: int | None
    body: bytes | None
    headers: Mapping[str, str]
    fetched_at: datetime
    elapsed_ms: float
    attempts: int
    retry_after_s: float | None
    error: str | None
    error_kind: ErrorKind | None = None
    error_body: bytes | None = None
    attempt_log: tuple[AttemptRecord, ...] = ()

    @property
    def ok(self) -> bool:
        return self.outcome == "ok"

    @property
    def failed(self) -> bool:
        return self.outcome not in ("ok", "not_modified")

    @property
    def body_bytes(self) -> int:
        return len(self.body) if self.body is not None else 0


@dataclass(frozen=True)
class _Attempt:
    outcome: Outcome
    http_status: int | None
    body: bytes | None
    headers: dict[str, str]
    fetched_at: datetime
    elapsed_ms: float
    retryable: bool
    retry_after_s: float | None
    error: str | None
    error_kind: ErrorKind | None
    error_body: bytes | None


def header_subset(headers: Mapping[str, str] | httpx.Headers) -> dict[str, str]:
    """Lower-cased subset of response headers worth keeping (no cookies, no secrets)."""
    out: dict[str, str] = {}
    for name in KEPT_HEADERS:
        value = headers.get(name)
        if value is not None:
            out[name] = str(value)
    return out


def parse_retry_after(value: str | None, *, now: datetime, max_seconds: float) -> float | None:
    """Parse ``Retry-After`` (delta-seconds or HTTP-date) into seconds clamped to ``[0, max_seconds]``.

    Returns ``None`` for an absent or unparseable header (never guesses).
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if _DELTA_SECONDS.match(text):
        seconds = float(text)
    else:
        try:
            when = email.utils.parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
        if when is None:
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = (when - now).total_seconds()
    return max(0.0, min(seconds, float(max_seconds)))


def backoff_delay(attempt: int, *, base: float, cap: float, rng: random.Random) -> float:
    """Delay before attempt ``attempt + 1``: ``min(cap, base * 2**(attempt-1)) * U(0.5, 1.5)``."""
    raw = min(cap, base * (2 ** max(0, attempt - 1)))
    return raw * rng.uniform(0.5, 1.5)


def is_dns_error(exc: BaseException) -> bool:
    """True when a transport error was caused by name resolution (walks the exception chain)."""
    seen: set[int] = set()
    cur: BaseException | None = exc
    depth = 0
    while cur is not None and id(cur) not in seen and depth < 10:
        seen.add(id(cur))
        if isinstance(cur, socket.gaierror):
            return True
        text = str(cur).lower()
        if any(marker in text for marker in _DNS_MARKERS):
            return True
        cur = cur.__cause__ or cur.__context__
        depth += 1
    return False


def _short(exc: BaseException) -> str:
    text = f"{type(exc).__name__}: {exc}"
    return text if len(text) <= 300 else text[:300] + "…"


class FeedFetcher:
    """Synchronous fetcher for one feed URL with its own ``httpx.Client`` (not shared across threads)."""

    def __init__(
        self,
        feed_type: str,
        url: str,
        settings: FetchSettings,
        *,
        transport: httpx.BaseTransport | None = None,
        rng: random.Random | None = None,
        wall_clock: Callable[[], datetime] = _utcnow,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.feed_type = feed_type
        self.url = url
        self.settings = settings
        self._rng = rng or random.Random()
        self._now = wall_clock
        self._mono = monotonic
        self._sleep = sleep
        self._client = httpx.Client(
            timeout=settings.timeout(),
            headers={"User-Agent": settings.user_agent, "Accept": "application/json"},
            follow_redirects=False,
            transport=transport,
        )

    # ------------------------------------------------------------------ lifecycle
    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> FeedFetcher:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ public API
    def fetch(
        self,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
        stop: threading.Event | None = None,
    ) -> FetchResult:
        """Perform one logical poll with bounded retries. Never raises for network/HTTP problems."""
        headers: dict[str, str] = {}
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        attempt_log: list[AttemptRecord] = []
        result: _Attempt | None = None
        max_attempts = max(1, self.settings.max_attempts)
        for attempt in range(1, max_attempts + 1):
            started = self._now()
            result = self._attempt(headers)
            attempt_log.append(AttemptRecord(
                attempt=attempt, started_at=started, outcome=result.outcome, http_status=result.http_status,
                elapsed_ms=result.elapsed_ms, error=result.error, retry_after_s=result.retry_after_s,
            ))
            if not result.retryable or attempt >= max_attempts:
                break
            delay = backoff_delay(attempt, base=self.settings.backoff_base_seconds,
                                  cap=self.settings.backoff_cap_seconds, rng=self._rng)
            if result.retry_after_s is not None:
                if result.retry_after_s > self.settings.backoff_cap_seconds:
                    # Long server-requested pause: let the scheduler defer the next poll instead.
                    break
                delay = max(delay, result.retry_after_s)
            log.debug("fetch retry feed=%s attempt=%d delay=%.2fs", self.feed_type, attempt, delay)
            if stop is not None:
                if stop.wait(delay):
                    break
            else:
                self._sleep(delay)
        assert result is not None  # loop runs at least once
        return FetchResult(
            feed_type=self.feed_type,
            url=self.url,
            outcome=result.outcome,
            http_status=result.http_status,
            body=result.body if result.outcome == "ok" else None,
            headers=result.headers,
            fetched_at=result.fetched_at,
            elapsed_ms=result.elapsed_ms,
            attempts=len(attempt_log),
            retry_after_s=result.retry_after_s,
            error=result.error,
            error_kind=result.error_kind,
            error_body=result.error_body,
            attempt_log=tuple(attempt_log),
        )

    # ------------------------------------------------------------------ one attempt
    def _attempt(self, headers: dict[str, str]) -> _Attempt:
        t0 = self._mono()
        deadline = t0 + self.settings.attempt_deadline_seconds

        def done(outcome: Outcome, *, status: int | None = None, body: bytes | None = None,
                 hdrs: dict[str, str] | None = None, fetched_at: datetime | None = None, retryable: bool = False,
                 retry_after: float | None = None, error: str | None = None, kind: ErrorKind | None = None,
                 error_body: bytes | None = None) -> _Attempt:
            return _Attempt(
                outcome=outcome, http_status=status, body=body, headers=hdrs or {},
                fetched_at=fetched_at or self._now(), elapsed_ms=(self._mono() - t0) * 1000.0,
                retryable=retryable, retry_after_s=retry_after, error=error, error_kind=kind, error_body=error_body,
            )

        try:
            with self._client.stream("GET", self.url, headers=headers) as resp:
                status = resp.status_code
                hdrs = header_subset(resp.headers)
                received_at = self._now()
                if status == 304:
                    return done("not_modified", status=status, hdrs=hdrs, fetched_at=received_at)
                if status == 200:
                    limit = self.settings.max_response_bytes
                    declared = _content_length(resp.headers)
                    if declared is not None and declared > limit:
                        return done("too_large", status=status, hdrs=hdrs, fetched_at=received_at,
                                    error=f"content-length {declared} exceeds {limit} bytes", kind="too_large")
                    buf = bytearray()
                    for chunk in resp.iter_bytes(CHUNK_SIZE):
                        buf += chunk
                        if len(buf) > limit:
                            return done("too_large", status=status, hdrs=hdrs, fetched_at=received_at,
                                        error=f"decoded body exceeds {limit} bytes", kind="too_large")
                        if self._mono() > deadline:
                            return done("timeout", status=status, hdrs=hdrs, fetched_at=received_at,
                                        retryable=True, kind="timeout",
                                        error=f"body not complete within {self.settings.attempt_deadline_seconds:.0f}s")
                    return done("ok", status=status, body=bytes(buf), hdrs=hdrs, fetched_at=received_at)
                # Any other status (incl. non-200 2xx and 3xx) is an HTTP error; keep a bounded body excerpt.
                excerpt = bytearray()
                try:
                    for chunk in resp.iter_bytes(CHUNK_SIZE):
                        excerpt += chunk
                        if len(excerpt) >= ERROR_BODY_MAX_BYTES or self._mono() > deadline:
                            break
                except httpx.HTTPError:
                    pass  # the status is what matters; a broken error body is not retried separately
                retry_after = parse_retry_after(resp.headers.get("retry-after"), now=received_at,
                                                max_seconds=self.settings.retry_after_max_seconds)
                return done("http_error", status=status, hdrs=hdrs, fetched_at=received_at,
                            retryable=status in RETRYABLE_STATUS, retry_after=retry_after,
                            error=f"HTTP {status}", kind="http",
                            error_body=bytes(excerpt[:ERROR_BODY_MAX_BYTES]) or None)
        except httpx.TimeoutException as exc:
            return done("timeout", retryable=True, error=_short(exc), kind="timeout")
        except httpx.DecodingError as exc:
            return done("http_error", error=_short(exc), kind="decode")
        except (httpx.NetworkError, httpx.RemoteProtocolError, httpx.ProxyError) as exc:
            return done("connect_error", retryable=True, error=_short(exc),
                        kind="dns" if is_dns_error(exc) else "connect")
        except httpx.HTTPError as exc:  # UnsupportedProtocol, LocalProtocolError, TooManyRedirects, ...
            return done("connect_error", error=_short(exc), kind="connect")
        except httpx.InvalidURL as exc:
            return done("connect_error", error=_short(exc), kind="connect")
        except OSError as exc:  # raw socket errors not wrapped by httpx
            return done("connect_error", retryable=True, error=_short(exc),
                        kind="dns" if is_dns_error(exc) else "connect")


def _content_length(headers: httpx.Headers) -> int | None:
    value = headers.get("content-length")
    if value is None:
        return None
    try:
        n = int(value.strip())
    except ValueError:
        return None
    return n if n >= 0 else None
