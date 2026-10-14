"""Signed page tokens and bounded history paging (design E §4.5, D-E22).

Token format: ``base64url(canonical_json(payload)) + "." + base64url(HMAC-SHA256(key, body)[:16])``.
The payload binds the token to its query (kind, run, vehicle, half-open range ``[from, to)``) and carries
the cursor (bucket index, last ``event_time``, last ``observation_id``). Any change to the token, or use
with another run/vehicle/range, is rejected (HTTP 422 ``invalid_page_token``).

History order is ``(event_time ASC, observation_id DESC)`` — the exact reverse of the clustering order
of ``vehicle_observations_by_day`` — so every page is one reversed slice per UTC day bucket and
truncation by ``LIMIT`` never splits the order.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from transitpulse.contracts.ids import canonical_json

TOKEN_VERSION = 1
SIGNATURE_BYTES = 16
MAX_TOKEN_CHARS = 2048


class InvalidPageToken(ValueError):
    """The token is malformed, forged, expired by key rotation, or bound to another query."""


def _b64e(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64d(text: str) -> bytes:
    padded = text + "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("naive datetime in page token")
    return dt.astimezone(UTC).isoformat()


def _parse_iso(value: Any) -> datetime:
    if not isinstance(value, str):
        raise InvalidPageToken("page token timestamp is not a string")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidPageToken("page token timestamp is malformed") from exc
    if dt.tzinfo is None:
        raise InvalidPageToken("page token timestamp has no timezone")
    return dt.astimezone(UTC)


class TokenSigner:
    """HMAC-SHA256 signer/verifier of opaque page tokens."""

    def __init__(self, key: bytes) -> None:
        if len(key) < 16:
            raise ValueError("token key must have at least 16 bytes")
        self._key = bytes(key)

    def _sign(self, body: str) -> str:
        digest = hmac.new(self._key, body.encode("ascii"), hashlib.sha256).digest()
        return _b64e(digest[:SIGNATURE_BYTES])

    def encode(self, payload: Mapping[str, Any]) -> str:
        body = _b64e(canonical_json(dict(payload)))
        return f"{body}.{self._sign(body)}"

    def decode(self, token: str) -> dict[str, Any]:
        if not isinstance(token, str) or not token or len(token) > MAX_TOKEN_CHARS:
            raise InvalidPageToken("page token is empty or too long")
        body, sep, signature = token.partition(".")
        if not sep or not body or not signature:
            raise InvalidPageToken("page token is malformed")
        try:
            expected = self._sign(body)
        except UnicodeEncodeError as exc:
            raise InvalidPageToken("page token is malformed") from exc
        if not hmac.compare_digest(expected.encode("ascii"), signature.encode("utf-8")):
            raise InvalidPageToken("page token signature does not match")
        try:
            payload = json.loads(_b64d(body).decode("utf-8"))
        except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
            raise InvalidPageToken("page token payload is malformed") from exc
        if not isinstance(payload, dict) or payload.get("v") != TOKEN_VERSION:
            raise InvalidPageToken("page token version is not supported")
        return payload


# ================================================================================================
# Vehicle history
# ================================================================================================
@dataclass(frozen=True)
class HistoryCursor:
    """Position after the last returned observation (bucket index into the request's day buckets)."""

    bucket_index: int
    event_time: datetime
    observation_id: str


@dataclass(frozen=True)
class HistoryToken:
    start: datetime
    end: datetime
    cursor: HistoryCursor


def encode_history_token(
    signer: TokenSigner, *, run_id: str, vehicle_id: str, start: datetime, end: datetime, cursor: HistoryCursor
) -> str:
    return signer.encode(
        {
            "v": TOKEN_VERSION,
            "k": "history",
            "run": run_id,
            "vehicle": vehicle_id,
            "from": _iso(start),
            "to": _iso(end),
            "b": cursor.bucket_index,
            "et": _iso(cursor.event_time),
            "oid": cursor.observation_id,
        }
    )


def decode_history_token(signer: TokenSigner, token: str, *, run_id: str, vehicle_id: str) -> HistoryToken:
    payload = signer.decode(token)
    if payload.get("k") != "history":
        raise InvalidPageToken("page token was not issued for vehicle history")
    if payload.get("run") != run_id or payload.get("vehicle") != vehicle_id:
        raise InvalidPageToken("page token was issued for another run or vehicle")
    bucket = payload.get("b")
    oid = payload.get("oid")
    if not isinstance(bucket, int) or isinstance(bucket, bool) or bucket < 0 or not isinstance(oid, str):
        raise InvalidPageToken("page token cursor is malformed")
    start, end = _parse_iso(payload.get("from")), _parse_iso(payload.get("to"))
    if not start < end:
        raise InvalidPageToken("page token range is empty")
    return HistoryToken(start, end, HistoryCursor(bucket, _parse_iso(payload.get("et")), oid))


def history_buckets(start: datetime, end: datetime) -> list[date]:
    """UTC day buckets touched by the half-open interval ``[start, end)``, ascending."""
    first = start.astimezone(UTC).date()
    last = (end.astimezone(UTC) - timedelta(microseconds=1)).date()
    out: list[date] = []
    day = first
    while day <= last:
        out.append(day)
        day += timedelta(days=1)
    return out


def bucket_bounds(day: date) -> tuple[datetime, datetime]:
    """``[00:00Z, next 00:00Z)`` of one UTC day."""
    lo = datetime.combine(day, time(0, 0), tzinfo=UTC)
    return lo, lo + timedelta(days=1)


HistoryFetch = Callable[[int, date, datetime, datetime, tuple[datetime, str] | None, int], list[dict[str, Any]]]


def collect_history_page(
    *,
    buckets: list[date],
    start: datetime,
    end: datetime,
    cursor: HistoryCursor | None,
    page_size: int,
    fetch: HistoryFetch,
) -> tuple[list[dict[str, Any]], HistoryCursor | None]:
    """Collect up to ``page_size`` rows across day buckets; return ``(rows, next_cursor)``.

    ``fetch(bucket_index, day, lo, hi, after, limit)`` returns the bucket's rows in page order strictly
    after ``after``. One extra row (``page_size + 1``) is requested in total to learn whether another
    page exists, so ``next_cursor`` is ``None`` exactly when the range is exhausted.
    """
    if cursor is not None and cursor.bucket_index >= len(buckets):
        raise InvalidPageToken("page token cursor is outside the requested range")
    first = cursor.bucket_index if cursor is not None else 0
    collected: list[tuple[int, dict[str, Any]]] = []
    for index in range(first, len(buckets)):
        remaining = page_size + 1 - len(collected)
        if remaining <= 0:
            break
        day = buckets[index]
        day_lo, day_hi = bucket_bounds(day)
        lo, hi = max(start, day_lo), min(end, day_hi)
        after = (cursor.event_time, cursor.observation_id) if cursor is not None and index == first else None
        for row in fetch(index, day, lo, hi, after, remaining)[:remaining]:
            collected.append((index, row))
    page = collected[:page_size]
    if len(collected) > page_size and page:
        bucket_index, last = page[-1]
        return [r for _, r in page], HistoryCursor(bucket_index, last["event_time"], last["observation_id"])
    return [r for _, r in page], None


# ================================================================================================
# Run list
# ================================================================================================
@dataclass(frozen=True)
class RunsCursor:
    created_at: datetime
    run_id: str


def encode_runs_token(signer: TokenSigner, *, project_id: str, cursor: RunsCursor) -> str:
    return signer.encode(
        {"v": TOKEN_VERSION, "k": "runs", "project": project_id, "ca": _iso(cursor.created_at), "rid": cursor.run_id}
    )


def decode_runs_token(signer: TokenSigner, token: str, *, project_id: str) -> RunsCursor:
    payload = signer.decode(token)
    if payload.get("k") != "runs" or payload.get("project") != project_id:
        raise InvalidPageToken("page token was not issued for this run list")
    rid = payload.get("rid")
    if not isinstance(rid, str) or not rid:
        raise InvalidPageToken("page token cursor is malformed")
    return RunsCursor(_parse_iso(payload.get("ca")), rid)
