"""Replay timeline ordering and validation (design E §3.2, D-E15)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from transitpulse.contracts.envelope import iso_utc
from transitpulse.replay.timeline import TimelineError, build_timeline, timeline_start

T0 = datetime(2026, 10, 5, 12, 0, 0, tzinfo=UTC)


def _at(seconds: float) -> str:
    return iso_utc(T0 + timedelta(seconds=seconds))


def _capture(seq: int, feed: str, seconds: float, **extra) -> dict:
    item = {
        "seq": seq, "kind": "capture", "feed_type": feed, "at": _at(seconds),
        "capture_id": f"{feed[:2]}-{seq}", "raw_path": f"{feed}/2026-10-05/c{seq}.json.gz",
        "raw_sha256": f"{seq:064x}", "raw_bytes": 10, "stored_sha256": f"{seq + 1000:064x}",
        "parse_status": "ok", "http_status": 200, "feed_timestamp": 1791201600, "params": {},
    }
    item.update(extra)
    return item


def _manifest(items: list[dict], source_type: str = "live", **extra) -> dict:
    return {"source_type": source_type, "items": items, "feeds": {
        "vehicle_positions": {"source_url": "https://cdn.example/vp"}}, **extra}


def test_real_recording_is_ordered_by_time_feed_order_then_seq():
    items = build_timeline(_manifest([
        _capture(1, "trip_updates", 0),
        _capture(2, "vehicle_positions", 0),
        _capture(3, "alerts", -1),
        _capture(4, "vehicle_positions", 5),
        _capture(5, "vehicle_positions", 0),
    ]))
    assert [(it.seq, it.feed_type) for it in items] == [
        (3, "alerts"), (2, "vehicle_positions"), (5, "vehicle_positions"), (1, "trip_updates"),
        (4, "vehicle_positions"),
    ]
    assert items[1].capture is not None and items[1].capture.source_url == "https://cdn.example/vp"


def test_synthetic_recording_keeps_explicit_seq_order_with_late_items():
    items = build_timeline(_manifest([
        _capture(2, "vehicle_positions", 30),
        _capture(1, "vehicle_positions", 0),
        _capture(3, "vehicle_positions", 15),            # late: older time after a newer item
        {"seq": 4, "kind": "republish", "params": {"of_seq": 1}},
        {"seq": 5, "kind": "barrier", "params": {"label": "S1"}},
    ], source_type="synthetic"))
    assert [it.seq for it in items] == [1, 2, 3, 4, 5]
    assert items[2].at < items[1].at
    assert items[3].at == items[2].at                    # special item without 'at' inherits
    assert items[3].feed_type == "vehicle_positions"
    assert items[4].label == "S1"


def test_special_kinds_only_for_synthetic():
    with pytest.raises(TimelineError, match="synthetic"):
        build_timeline(_manifest([_capture(1, "vehicle_positions", 0),
                                  {"seq": 2, "kind": "tick", "at": _at(10)}]))
    with pytest.raises(TimelineError, match="synthetic"):
        build_timeline(_manifest([{"seq": 1, "kind": "tick", "at": _at(0)}], source_type="synthetic"),
                       allow_special=False)
    ok = build_timeline(_manifest([{"seq": 1, "kind": "tick", "at": _at(0)}], source_type="synthetic"))
    assert ok[0].kind == "tick"


def test_republish_must_reference_an_earlier_capture():
    with pytest.raises(TimelineError, match="earlier capture"):
        build_timeline(_manifest([
            {"seq": 1, "kind": "republish", "params": {"of_seq": 2}, "at": _at(0)},
            _capture(2, "vehicle_positions", 0),
        ], source_type="synthetic"))


def test_invalid_items_are_rejected():
    with pytest.raises(TimelineError, match="duplicate"):
        build_timeline(_manifest([_capture(1, "alerts", 0), _capture(1, "alerts", 1)]))
    with pytest.raises(TimelineError, match="kind"):
        build_timeline(_manifest([{"seq": 1, "kind": "nonsense", "at": _at(0)}]))
    with pytest.raises(TimelineError, match="feed_type"):
        build_timeline(_manifest([_capture(1, "buses", 0)]))
    with pytest.raises(TimelineError, match="raw_path"):
        build_timeline(_manifest([_capture(1, "alerts", 0, raw_path=None)]))
    with pytest.raises(TimelineError, match="value_text"):
        build_timeline(_manifest([{"seq": 1, "kind": "malformed_envelope", "at": _at(0),
                                   "params": {"feed_type": "alerts"}}], source_type="synthetic"))


def test_rejected_capture_without_raw_and_fetch_events_are_valid():
    items = build_timeline(_manifest([
        _capture(1, "alerts", 0, raw_path=None, raw_sha256=None, parse_status="INVALID_JSON"),
        {"seq": 2, "kind": "fetch_event", "feed_type": "trip_updates", "at": _at(1),
         "params": {"outcome": "timeout", "http_status": None, "reason_code": "SOURCE_FETCH_FAILED"}},
    ]))
    assert items[0].capture is not None and not items[0].capture.is_ok
    assert items[1].kind == "fetch_event"


def test_timeline_start_prefers_explicit_start_clock():
    manifest = _manifest([_capture(1, "vehicle_positions", 10)], source_type="synthetic",
                         start_clock=_at(0))
    items = build_timeline(manifest)
    assert timeline_start(manifest, items) == T0
    plain = _manifest([_capture(1, "vehicle_positions", 10), _capture(2, "alerts", 3)])
    assert timeline_start(plain, build_timeline(plain)) == T0 + timedelta(seconds=3)
