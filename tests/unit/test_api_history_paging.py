"""Vehicle history paging: HMAC page tokens, UTC day buckets, ties, range limits (unit, no Cassandra)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from transitpulse.api.app import create_app
from transitpulse.api.memory_repository import InMemoryRepository
from transitpulse.api.paging import (
    HistoryCursor,
    InvalidPageToken,
    TokenSigner,
    collect_history_page,
    decode_history_token,
    encode_history_token,
    history_buckets,
)
from transitpulse.api.settings import ApiSettings
from transitpulse.config import ProjectConfig, load_config
from transitpulse.contracts.policy import Policy

BASE = "/api/v1"
RUN = "7a7a7a7a-0000-4000-8000-000000000001"
CLOCK = datetime(2026, 10, 6, 0, 20, 0, tzinfo=UTC)
D1, D2 = date(2026, 10, 5), date(2026, 10, 6)
KEY = b"history-test-key-0123456789"


def at(day: date, hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hh, mm, ss, tzinfo=UTC)


# (observation_id, event_time) of vehicle V1; ob_c1/ob_c2 share one event_time (tie across a page boundary)
OBSERVATIONS = [
    ("ob_a", at(D1, 23, 30)),
    ("ob_b", at(D1, 23, 45)),
    ("ob_c1", at(D1, 23, 59, 59)),
    ("ob_c2", at(D1, 23, 59, 59)),
    ("ob_d", at(D2, 0, 0)),
    ("ob_e", at(D2, 0, 10)),
    ("ob_f", at(D2, 0, 25)),  # after the application clock: outside the default range
]
# page order is (event_time ASC, observation_id DESC)
DEFAULT_ORDER = ["ob_a", "ob_b", "ob_c2", "ob_c1", "ob_d", "ob_e"]


# ================================================================================================
# Fixtures
# ================================================================================================
@pytest.fixture()
def cfg(tmp_path: Any) -> ProjectConfig:
    return load_config("local-native", workdir=tmp_path, environ={})


@pytest.fixture()
def repo(cfg: ProjectConfig) -> InMemoryRepository:
    r = InMemoryRepository()
    r.put("run_metadata", {
        "run_id": RUN, "project_id": cfg.project.project_id, "source_type": "replay", "clock_mode": "source",
        "gtfs_version": "gv1", "agency_timezone": "America/New_York", "route_ids": ["R_A"],
        "policy_json": Policy.from_config(cfg).to_json(), "run_state": "RUNNING", "application_clock": CLOCK,
        "clock_sequence": 10,
    })
    for oid, event_time in OBSERVATIONS:
        r.put("vehicle_observations_by_day", {
            "run_id": RUN, "vehicle_id": "V1", "utc_date": event_time.date(), "event_time": event_time,
            "observation_id": oid, "record_kind": "observation", "event_time_origin": "vehicle_timestamp",
            "route_id": "R_A", "latitude": 42.35, "longitude": -71.06, "quality_flags": [],
        })
    r.put("vehicle_observations_by_day", {
        "run_id": RUN, "vehicle_id": "V2", "utc_date": D1, "event_time": at(D1, 23, 50), "observation_id": "ob_x",
    })
    return r


@pytest.fixture()
def client(cfg: ProjectConfig, repo: InMemoryRepository) -> Iterator[TestClient]:
    settings = ApiSettings.from_config(cfg, token_key=KEY, configure_logging=False,
                                       wall_clock=lambda: datetime(2026, 10, 6, 9, 0, tzinfo=UTC))
    with TestClient(create_app(settings, repo)) as c:
        yield c


def history(client: TestClient, vehicle_id: str = "V1", **params: Any) -> Any:
    return client.get(f"{BASE}/vehicles/{vehicle_id}/history", params={"run_id": RUN, **params})


def error_code(resp: Any) -> str:
    return resp.json()["error"]["code"]


def page_through(client: TestClient, **params: Any) -> list[list[str]]:
    pages: list[list[str]] = []
    token = None
    for _ in range(20):
        query = dict(params)
        if token is not None:
            query["page_token"] = token
        resp = history(client, **query)
        assert resp.status_code == 200, resp.text
        data = resp.json()["data"]
        pages.append([o["observation_id"] for o in data["observations"]])
        token = data["next_page_token"]
        if token is None:
            return pages
    raise AssertionError("paging did not terminate")


# ================================================================================================
# Token signer and pure paging helpers
# ================================================================================================
def test_token_round_trip_and_tampering() -> None:
    signer = TokenSigner(KEY)
    start, end = at(D1, 23, 0), at(D2, 0, 20)
    cursor = HistoryCursor(1, at(D2, 0, 0), "ob_d")
    token = encode_history_token(signer, run_id=RUN, vehicle_id="V1", start=start, end=end, cursor=cursor)
    decoded = decode_history_token(signer, token, run_id=RUN, vehicle_id="V1")
    assert decoded.start == start and decoded.end == end and decoded.cursor == cursor

    body, _, sig = token.partition(".")
    forged_sig = sig[:-1] + ("A" if sig[-1] != "A" else "B")
    with pytest.raises(InvalidPageToken):
        decode_history_token(signer, f"{body}.{forged_sig}", run_id=RUN, vehicle_id="V1")
    forged_body = ("A" if body[0] != "A" else "B") + body[1:]
    with pytest.raises(InvalidPageToken):
        decode_history_token(signer, f"{forged_body}.{sig}", run_id=RUN, vehicle_id="V1")
    with pytest.raises(InvalidPageToken):
        decode_history_token(TokenSigner(b"another-key-0123456789"), token, run_id=RUN, vehicle_id="V1")
    with pytest.raises(InvalidPageToken):
        decode_history_token(signer, token, run_id=RUN, vehicle_id="V2")
    for garbage in ("", "no-dot", ".", "abc.", "é.é"):
        with pytest.raises(InvalidPageToken):
            signer.decode(garbage)


def test_history_buckets_are_utc_days_of_half_open_range() -> None:
    assert history_buckets(at(D1, 23, 0), at(D2, 0, 20)) == [D1, D2]
    assert history_buckets(at(D1, 23, 0), at(D2, 0, 0)) == [D1]  # end is exclusive
    assert history_buckets(at(D2, 0, 0), at(D2, 1, 0)) == [D2]


def test_collect_history_page_never_skips_or_duplicates() -> None:
    rows_by_day: dict[date, list[dict[str, Any]]] = {}
    for oid, et in OBSERVATIONS:
        rows_by_day.setdefault(et.date(), []).append({"observation_id": oid, "event_time": et})

    def fetch(index: int, day: date, lo: datetime, hi: datetime, after: Any, limit: int) -> list[dict[str, Any]]:
        rows = [r for r in rows_by_day.get(day, []) if lo <= r["event_time"] < hi]
        rows.sort(key=lambda r: r["observation_id"], reverse=True)
        rows.sort(key=lambda r: r["event_time"])
        if after is not None:
            et, oid = after
            rows = [r for r in rows if r["event_time"] > et or (r["event_time"] == et and r["observation_id"] < oid)]
        return rows[:limit]

    start, end = at(D1, 23, 20), CLOCK
    buckets = history_buckets(start, end)
    for size in (1, 2, 3, 4, 6, 7):
        seen: list[str] = []
        cursor = None
        while True:
            page, cursor = collect_history_page(
                buckets=buckets, start=start, end=end, cursor=cursor, page_size=size, fetch=fetch
            )
            assert len(page) <= size
            seen.extend(r["observation_id"] for r in page)
            if cursor is None:
                break
        assert seen == DEFAULT_ORDER, size


# ================================================================================================
# API
# ================================================================================================
def test_default_range_is_last_hour_of_application_clock(client: TestClient) -> None:
    resp = history(client)
    assert resp.status_code == 200, resp.text
    data = resp.json()["data"]
    assert data["range"] == {"start": "2026-10-05T23:20:00.000Z", "end": "2026-10-06T00:20:00.000Z"}
    assert data["buckets"] == ["2026-10-05", "2026-10-06"]
    assert data["order"] == "event_time_asc"
    assert [o["observation_id"] for o in data["observations"]] == DEFAULT_ORDER
    assert data["observations"][0]["event_time"] == "2026-10-05T23:30:00.000Z"
    assert data["next_page_token"] is None


@pytest.mark.parametrize("size", [1, 2, 3, 5])
def test_paging_across_day_buckets_and_ties(client: TestClient, size: int) -> None:
    pages = page_through(client, page_size=size)
    assert all(len(p) <= size for p in pages)
    assert [oid for page in pages for oid in page] == DEFAULT_ORDER


def test_explicit_range_and_token_binding(client: TestClient) -> None:
    params = {"from": "2026-10-05T23:40:00Z", "to": "2026-10-06T00:05:00Z", "page_size": 2}
    first = history(client, **params)
    assert first.status_code == 200, first.text
    data = first.json()["data"]
    assert [o["observation_id"] for o in data["observations"]] == ["ob_b", "ob_c2"]
    token = data["next_page_token"]
    # the same range may be repeated, or omitted (taken from the token)
    again = history(client, page_token=token, **params).json()["data"]
    assert [o["observation_id"] for o in again["observations"]] == ["ob_c1", "ob_d"]
    omitted = history(client, page_token=token, page_size=2).json()["data"]
    assert [o["observation_id"] for o in omitted["observations"]] == ["ob_c1", "ob_d"]
    # another range, another vehicle, or a tampered token are rejected
    other_range = history(client, page_token=token, **{**params, "to": "2026-10-06T00:06:00Z"})
    assert other_range.status_code == 422 and error_code(other_range) == "invalid_page_token"
    other_vehicle = history(client, "V2", page_token=token)
    assert other_vehicle.status_code == 422 and error_code(other_vehicle) == "invalid_page_token"
    tampered = history(client, page_token=token[:-1] + ("A" if token[-1] != "A" else "B"))
    assert tampered.status_code == 422 and error_code(tampered) == "invalid_page_token"


def test_range_validation(client: TestClient) -> None:
    too_large = history(client, **{"from": "2026-10-04T00:00:00Z", "to": "2026-10-05T00:00:01Z"})
    assert too_large.status_code == 422
    assert error_code(too_large) == "history_range_too_large"
    assert too_large.json()["error"]["hint"]
    exactly_24h = history(client, **{"from": "2026-10-05T00:20:00Z", "to": "2026-10-06T00:20:00Z"})
    assert exactly_24h.status_code == 200
    naive = history(client, **{"from": "2026-10-05T23:00:00"})
    assert naive.status_code == 422 and error_code(naive) == "naive_datetime"
    inverted = history(client, **{"from": "2026-10-06T00:00:00Z", "to": "2026-10-06T00:00:00Z"})
    assert inverted.status_code == 422 and error_code(inverted) == "validation_error"
    too_big_page = history(client, page_size=501)
    assert too_big_page.status_code == 422 and error_code(too_big_page) == "validation_error"


def test_offset_timestamps_are_normalized_to_utc(client: TestClient) -> None:
    resp = history(client, **{"from": "2026-10-05T19:40:00-04:00", "to": "2026-10-05T20:05:00-04:00"})
    assert resp.status_code == 200, resp.text
    data = resp.json()["data"]
    assert data["range"] == {"start": "2026-10-05T23:40:00.000Z", "end": "2026-10-06T00:05:00.000Z"}
    assert [o["observation_id"] for o in data["observations"]] == ["ob_b", "ob_c2", "ob_c1", "ob_d"]


def test_empty_history_has_reason_and_no_token(client: TestClient) -> None:
    data = history(client, "V9").json()["data"]
    assert data["observations"] == []
    assert data["reason"] == "NO_OBSERVATIONS_IN_RANGE"
    assert data["next_page_token"] is None


def test_history_without_clock_needs_explicit_range(client: TestClient, repo: InMemoryRepository) -> None:
    repo.put("run_metadata", {"run_id": RUN, "application_clock": None})
    resp = history(client)
    assert resp.status_code == 503 and error_code(resp) == "run_clock_unavailable"
    explicit = history(client, **{"from": "2026-10-05T23:00:00Z", "to": "2026-10-06T00:00:00Z"})
    assert explicit.status_code == 200
    assert [o["observation_id"] for o in explicit.json()["data"]["observations"]] == ["ob_a", "ob_b", "ob_c2",
                                                                                      "ob_c1"]
