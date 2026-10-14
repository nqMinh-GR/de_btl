"""Unit tests for contracts.ids: determinism, order independence and identifier formats."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta, timezone

import pytest

from transitpulse.contracts import ids

HEADER = {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET", "timestamp": 1791201630}
E1 = {"id": "v1", "vehicle": {"vehicle": {"id": "1"}, "position": {"latitude": 42.35, "longitude": -71.06}}}
E2 = {"id": "v2", "vehicle": {"vehicle": {"id": "2"}, "timestamp": 1791201600}}

OBS = dict(
    vehicle_id="y1", trip_id="t1", route_id="R", direction_id=0, start_date="20261005", start_time="12:00:00",
    trip_schedule_relationship="SCHEDULED", latitude=42.3, longitude=-71.1, bearing=90.0, speed=5.5,
    stop_id="s1", current_stop_sequence=3, current_status="IN_TRANSIT_TO",
)


def test_payload_sha_is_independent_of_entity_order() -> None:
    assert ids.payload_sha(HEADER, [E1, E2]) == ids.payload_sha(HEADER, [E2, E1])


def test_payload_sha_is_independent_of_key_order() -> None:
    reordered = {"vehicle": {"position": {"longitude": -71.06, "latitude": 42.35}, "vehicle": {"id": "1"}}, "id": "v1"}
    assert ids.payload_sha(HEADER, [E1]) == ids.payload_sha(HEADER, [reordered])


def test_payload_sha_changes_with_content_and_header() -> None:
    changed = {**E2, "vehicle": {**E2["vehicle"], "timestamp": 1791201601}}
    base = ids.payload_sha(HEADER, [E1, E2])
    assert base != ids.payload_sha(HEADER, [E1, changed])
    assert base != ids.payload_sha({**HEADER, "timestamp": 1791201631}, [E1, E2])
    assert base != ids.payload_sha(HEADER, [E1])


def test_snapshot_id_is_deterministic_and_well_formed() -> None:
    p = ids.payload_sha(HEADER, [E1, E2])
    a = ids.snapshot_id("run-a", "scope1", "vehicle_positions", 1791201630, p)
    b = ids.snapshot_id("run-a", "scope1", "vehicle_positions", 1791201630, ids.payload_sha(HEADER, [E2, E1]))
    assert a == b
    assert re.fullmatch(r"sn_[0-9a-f]{32}", a)


def test_snapshot_id_depends_on_run_scope_feed_and_header_timestamp() -> None:
    p = ids.payload_sha(HEADER, [E1])
    base = ids.snapshot_id("run-a", "s", "vehicle_positions", 100, p)
    assert base != ids.snapshot_id("run-b", "s", "vehicle_positions", 100, p)
    assert base != ids.snapshot_id("run-a", "t", "vehicle_positions", 100, p)
    assert base != ids.snapshot_id("run-a", "s", "trip_updates", 100, p)
    assert base != ids.snapshot_id("run-a", "s", "vehicle_positions", 101, p)
    assert base != ids.snapshot_id("run-a", "s", "vehicle_positions", None, p)


def test_observation_id_differs_from_state_sample_with_same_values() -> None:
    ob = ids.observation_id(vehicle_timestamp=1791201600, **OBS)
    ss = ids.state_sample_id(header_timestamp=1791201600, **OBS)
    assert ob.startswith("ob_") and ss.startswith("ss_")
    assert ob[3:] != ss[3:]
    assert ob == ids.observation_id(vehicle_timestamp=1791201600, **OBS)


def test_observation_id_excludes_run_and_capture_context() -> None:
    # Same source observation polled twice -> same id (no run/snapshot/capture input exists).
    assert ids.observation_id(vehicle_timestamp=5, **OBS) == ids.observation_id(vehicle_timestamp=5, **dict(OBS))
    assert ids.observation_id(vehicle_timestamp=5, **OBS) != ids.observation_id(vehicle_timestamp=6, **OBS)


def test_observation_id_uses_fixed_point_coordinates() -> None:
    a = ids.observation_id(vehicle_timestamp=1, **{**OBS, "latitude": 42.30000000001})
    b = ids.observation_id(vehicle_timestamp=1, **{**OBS, "latitude": 42.3})
    c = ids.observation_id(vehicle_timestamp=1, **{**OBS, "latitude": 42.3000002})
    assert a == b
    assert a != c
    assert ids.e7(None) is None and ids.e2(float("nan")) is None
    assert ids.e3(5.5) == 5500


def test_observation_id_null_is_not_zero() -> None:
    with_none = ids.observation_id(vehicle_timestamp=1, **{**OBS, "direction_id": None})
    with_zero = ids.observation_id(vehicle_timestamp=1, **{**OBS, "direction_id": 0})
    assert with_none != with_zero


def test_prediction_id_requires_trip_id() -> None:
    kwargs = dict(start_date="20261005", start_time=None, route_id="R", direction_id=None, stop_sequence=3,
                  stop_id="s", event_type="arrival")
    with pytest.raises(ValueError):
        ids.prediction_id(trip_id="", **kwargs)
    with pytest.raises(ValueError):
        ids.prediction_id(trip_id=None, **kwargs)  # type: ignore[arg-type]
    pid = ids.prediction_id(trip_id="t1", **kwargs)
    assert re.fullmatch(r"pr_[0-9a-f]{32}", pid)
    assert pid != ids.prediction_id(trip_id="t1", **{**kwargs, "event_type": "departure"})
    assert pid == ids.prediction_id(trip_id="t1", **kwargs)


def test_k_rejects_bool_and_float_and_encodes_null() -> None:
    with pytest.raises(TypeError):
        ids.K("a", True)
    with pytest.raises(TypeError):
        ids.K("a", 1.5)
    assert ids.K("a", None, 3) == "a\x1f\x00\x1f3"
    assert ids.K(None) != ids.K("")
    assert ids.K(0) == "0" and ids.K(None, None) == "\x00\x1f\x00"


def test_h_is_32_hex() -> None:
    assert re.fullmatch(r"[0-9a-f]{32}", ids.H("x"))


def test_source_order_lexicographic_equals_tuple_order() -> None:
    base = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    cases = [
        (1791201600, base, "sn_b"),
        (1791201600, base + timedelta(milliseconds=1), "sn_a"),
        (1791201599, base + timedelta(hours=1), "sn_z"),
        (1791201600, base, "sn_a"),
        (99, base, "sn_c"),
        (1791201601, base - timedelta(days=1), "sn_0"),
        (1791201600, base + timedelta(seconds=2), "sn_0"),
    ]
    keyed = []
    for ts, at, sid in cases:
        micros = int(at.astimezone(UTC).timestamp() * 1_000_000)  # same component the function uses
        keyed.append((ids.source_order(ts, at, sid), (ts, micros, sid)))
    by_string = [t for _, t in sorted(keyed, key=lambda kv: kv[0])]
    by_tuple = sorted(t for _, t in keyed)
    assert by_string == by_tuple
    first = ids.source_order(1791201600, base, "sn_x")
    assert re.fullmatch(r"\d{12}\.\d{17}\.sn_x", first)


def test_source_order_rejects_negative_components() -> None:
    with pytest.raises(ValueError):
        ids.source_order(-1, datetime(2026, 10, 5, tzinfo=UTC), "sn_x")


def test_capture_id_format_and_timezone_independence() -> None:
    at = datetime(2026, 10, 6, 14, 14, 3, 512345, tzinfo=UTC)
    sha = "1a2b3c4d5e6f" + "0" * 52
    cid = ids.capture_id("vp", at, sha)
    assert cid == "vp-20261006T141403512345Z-1a2b3c4d5e6f"
    assert ids.capture_id("vp", at.astimezone(timezone(timedelta(hours=-4))), sha) == cid
    assert re.fullmatch(r"(vp|tu|al)-\d{8}T\d{12}Z-[0-9a-f]{12}", ids.capture_id("al", at, sha))


def test_error_ids_are_stable() -> None:
    a = ids.error_id_message("run", "mbta.alerts.snapshots.v1", "ab" * 32, "INVALID_JSON")
    assert a == ids.error_id_message("run", "mbta.alerts.snapshots.v1", "ab" * 32, "INVALID_JSON")
    assert a.startswith("er_")
    assert a != ids.error_id_message("run", "mbta.alerts.snapshots.v1", "ab" * 32, "INVALID_HEADER")
    e = ids.error_id_entity("run", "vehicle_positions", "sn_x", "v1", None, "MISSING_VEHICLE_ID")
    assert e != ids.error_id_entity("run", "vehicle_positions", "sn_x", "v1", "3|arrival", "MISSING_VEHICLE_ID")


def test_entity_ref_uses_id_or_hash() -> None:
    assert ids.entity_ref({"id": "v1"}) == "v1"
    ref = ids.entity_ref({"vehicle": {}})
    assert re.fullmatch(r"h:[0-9a-f]{16}", ref)
    assert ref == ids.entity_ref({"vehicle": {}})


def test_scope_id_is_order_and_duplicate_independent() -> None:
    a = ids.scope_id(["B", "A", "A"], 3, "scope-v1", "g" * 64)
    assert a == ids.scope_id(["A", "B"], 3, "scope-v1", "g" * 64)
    assert len(a) == 16
    assert a != ids.scope_id(["A"], 3, "scope-v1", "g" * 64)


def test_generation_id_format() -> None:
    gid = ids.generation_id("run", "alerts", "sn_" + "0" * 32, "p1")
    assert re.fullmatch(r"gn_[0-9a-f]{32}", gid)


def test_canonical_json_rejects_nan_and_sorts_keys() -> None:
    with pytest.raises(ValueError):
        ids.canonical_json({"x": float("nan")})
    assert ids.canonical_json({"b": 1, "a": "Đ"}) == '{"a":"Đ","b":1}'.encode()
