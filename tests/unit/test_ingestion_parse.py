"""Unit tests for transport-level feed validation (ingestion.feed_parse)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest

from transitpulse.ingestion.feed_parse import (
    INVALID_HEADER,
    INVALID_JSON,
    UNSUPPORTED_INCREMENTALITY,
    UNSUPPORTED_SCHEMA,
    ParsedFeed,
    Rejection,
    header_summary,
    parse_feed,
)

FETCHED = datetime(2026, 10, 5, 12, 0, 40, tzinfo=UTC)
NOW_S = int(FETCHED.timestamp())
TS = NOW_S - 10
VEHICLE = {"id": "v1", "vehicle": {"trip": {"route_id": "R1"}, "vehicle": {"id": "1"}, "timestamp": TS - 5}}


def _doc(entities: Any = ..., **header: Any) -> dict[str, Any]:
    h: dict[str, Any] = {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET", "timestamp": TS}
    h.update(header)
    doc: dict[str, Any] = {"header": h}
    if entities is not ...:
        doc["entity"] = entities
    return doc


def _body(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def _reject(body: bytes) -> Rejection:
    res = parse_feed(body, FETCHED)
    assert isinstance(res, Rejection), res
    return res


def test_valid_feed_is_returned_unchanged() -> None:
    doc = _doc([VEHICLE])
    res = parse_feed(_body(doc), FETCHED)
    assert isinstance(res, ParsedFeed)
    assert res.feed_timestamp == TS
    assert res.header == doc["header"]
    assert res.entities == [VEHICLE]
    assert res.warnings == {}
    assert res.source_entity_count == 1
    assert header_summary(res) == {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET", "timestamp": TS}


def test_missing_or_null_entity_is_a_valid_empty_snapshot() -> None:
    for doc in (_doc(), _doc(None), _doc([])):
        res = parse_feed(_body(doc), FETCHED)
        assert isinstance(res, ParsedFeed)
        assert res.entities == []


def test_non_ascii_values_are_preserved() -> None:
    ent = {"id": "a1", "alert": {"header_text": {"translation": [{"text": "Đường bị chặn", "language": "vi"}]}}}
    res = parse_feed(_body(_doc([ent])), FETCHED)
    assert isinstance(res, ParsedFeed)
    assert res.entities[0]["alert"]["header_text"]["translation"][0]["text"] == "Đường bị chặn"


@pytest.mark.parametrize("body", [
    b"\xff\xfe{}",                                   # not UTF-8
    b'{"header": {"gtfs_realtime_version": "2.0",',  # truncated
    b"",                                             # empty body
    b"\xef\xbb\xbf" + json.dumps({"header": {}}).encode(),  # UTF-8 BOM is not accepted as JSON
    b"<html>503 Service Unavailable</html>",
])
def test_invalid_json(body: bytes) -> None:
    assert _reject(body).reason_code == INVALID_JSON


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_constants_are_rejected(constant: str) -> None:
    body = ('{"header": {"gtfs_realtime_version": "2.0", "timestamp": %d}, '
            '"entity": [{"id": "x", "vehicle": {"position": {"latitude": %s}}}]}' % (TS, constant)).encode()
    rej = _reject(body)
    assert rej.reason_code == INVALID_JSON
    assert "non-finite" in rej.detail


@pytest.mark.parametrize("doc", [
    [],                                              # top level is not an object
    "text",
    {"entity": []},                                  # header missing
    {"header": [], "entity": []},                    # header not an object
    {"header": {"gtfs_realtime_version": "2.0", "timestamp": TS}, "entity": {"id": "x"}},  # entity not a list
])
def test_invalid_structure_is_invalid_header(doc: Any) -> None:
    assert _reject(_body(doc)).reason_code == INVALID_HEADER


def test_schema_version_checks() -> None:
    assert _reject(_body(_doc([], gtfs_realtime_version="3.0"))).reason_code == UNSUPPORTED_SCHEMA
    assert _reject(_body(_doc([], gtfs_realtime_version=2.0))).reason_code == INVALID_HEADER
    doc = _doc([])
    del doc["header"]["gtfs_realtime_version"]
    assert _reject(_body(doc)).reason_code == INVALID_HEADER
    assert isinstance(parse_feed(_body(_doc([], gtfs_realtime_version="1.0")), FETCHED), ParsedFeed)


def test_incrementality_checks() -> None:
    assert _reject(_body(_doc([], incrementality="DIFFERENTIAL"))).reason_code == UNSUPPORTED_INCREMENTALITY
    assert _reject(_body(_doc([], incrementality=1))).reason_code == UNSUPPORTED_INCREMENTALITY
    assert _reject(_body(_doc([], incrementality=False))).reason_code == UNSUPPORTED_INCREMENTALITY
    assert isinstance(parse_feed(_body(_doc([], incrementality=0)), FETCHED), ParsedFeed)
    doc = _doc([])
    del doc["header"]["incrementality"]                # absent = FULL_DATASET (GTFS-RT default)
    res = parse_feed(_body(doc), FETCHED)
    assert isinstance(res, ParsedFeed)
    assert "incrementality" not in res.header         # not normalized


def test_millisecond_timestamp_is_flagged_not_converted() -> None:
    rej = _reject(_body(_doc([], timestamp=TS * 1000)))
    assert rej.reason_code == INVALID_HEADER
    assert "timestamp_ms_suspected" in rej.detail
    assert str(TS * 1000) in rej.detail


@pytest.mark.parametrize("value", ["1791201630", 1791201630.5, True, None])
def test_non_integer_timestamp_is_invalid_header(value: Any) -> None:
    doc = _doc([])
    if value is None:
        del doc["header"]["timestamp"]
    else:
        doc["header"]["timestamp"] = value
    assert _reject(_body(doc)).reason_code == INVALID_HEADER


def test_timestamp_range() -> None:
    assert isinstance(parse_feed(_body(_doc([], timestamp=NOW_S + 300)), FETCHED), ParsedFeed)
    assert _reject(_body(_doc([], timestamp=NOW_S + 301))).reason_code == INVALID_HEADER
    assert _reject(_body(_doc([], timestamp=946_684_799))).reason_code == INVALID_HEADER
    assert isinstance(parse_feed(_body(_doc([], timestamp=946_684_800)), FETCHED), ParsedFeed)


def test_entity_level_issues_are_counted_not_rejected() -> None:
    ms_vehicle = {"id": "v2", "vehicle": {"vehicle": {"id": "2"}, "timestamp": TS * 1000}}
    res = parse_feed(_body(_doc([VEHICLE, "garbage", 5, ms_vehicle])), FETCHED)
    assert isinstance(res, ParsedFeed)
    assert res.entities == [VEHICLE, ms_vehicle]
    assert res.entities[1]["vehicle"]["timestamp"] == TS * 1000      # never converted
    assert res.warnings == {"entity_not_object": 2, "entity_timestamp_ms_suspected": 1}
    assert res.source_entity_count == 4


def test_naive_fetched_at_is_a_programming_error() -> None:
    with pytest.raises(ValueError):
        parse_feed(_body(_doc([])), datetime(2026, 10, 5, 12, 0, 40))
