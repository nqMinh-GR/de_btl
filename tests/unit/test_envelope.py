"""Unit tests for the snapshot envelope contract and the collector's envelope/DLQ builders."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from transitpulse.contracts import envelope as E
from transitpulse.contracts.dlq import DLQ_FIELDS
from transitpulse.contracts.ids import payload_sha, snapshot_id
from transitpulse.ingestion.archive import RawRef
from transitpulse.ingestion.envelope_builder import capture_dlq_record, envelope_from_capture, http_meta
from transitpulse.ingestion.feed_parse import ParsedFeed
from transitpulse.ingestion.scope_filter import ScopeFilter

RUN_ID = "0b6f3a52-9d8e-4c1b-a1f0-2d3c4b5a6978"
FETCHED = datetime(2026, 10, 5, 12, 0, 31, 250000, tzinfo=UTC)
HEADER = {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET", "timestamp": 1791201630}
E1 = {"id": "v1", "vehicle": {"trip": {"route_id": "R1"}, "vehicle": {"id": "1"}}}
E2 = {"id": "v2", "vehicle": {"trip": {"route_id": "R1"}, "vehicle": {"id": "2"}, "stop_id": "Đường"}}
RAW_SHA = "ab" * 32
RUN = SimpleNamespace(run_id=RUN_ID, scope_id="scope0001", source_type="live", gtfs_version="c" * 64)


def _env(**over: object) -> E.Envelope:
    kwargs: dict[str, object] = dict(
        run_id=RUN_ID, scope_id="scope0001", source_type="live", scenario_id=None, feed_type="vehicle_positions",
        snapshot_id="sn_" + "0" * 32, capture_id="vp-20261005T120031250000Z-abababababab",
        source_url="https://cdn.mbta.com/realtime/VehiclePositions.json", gtfs_version="c" * 64,
        header=dict(HEADER), entities=[E1, E2], original_fetched_at=FETCHED, raw_sha256=RAW_SHA,
        raw_path=f"{RUN_ID}/vehicle_positions/2026-10-05/vp-20261005T120031250000Z-abababababab.json.gz",
        raw_bytes=1234, http={"status": 200, "etag": None, "last_modified": None, "content_type": "application/json"},
        source_entity_count=5, scope_accounting={"selected": 2}, producer_component="collector",
        producer_version="0.1.0",
    )
    kwargs.update(over)
    return E.build_envelope(**kwargs)  # type: ignore[arg-type]


def test_build_envelope_has_required_fields_and_no_published_at() -> None:
    env = _env()
    assert "published_at" not in env
    assert env["schema_version"] == 1 and env["message_type"] == "feed_snapshot"
    assert env["feed_timestamp"] == HEADER["timestamp"]
    assert env["selected_entity_count"] == 2
    assert env["source_entity_count"] == 5
    assert env["original_fetched_at"] == "2026-10-05T12:00:31.250Z"
    assert env["scenario_id"] is None
    assert env["producer"] == {"component": "collector", "version": "0.1.0"}


@pytest.mark.parametrize("bad_ts", ["1791201630", True, 1791201630.0, None])
def test_build_envelope_rejects_non_integer_header_timestamp(bad_ts: object) -> None:
    with pytest.raises(ValueError):
        _env(header={**HEADER, "timestamp": bad_ts})


def test_serialize_sets_published_at_and_keeps_everything_else() -> None:
    env = _env()
    data = E.serialize_envelope(env, datetime(2026, 10, 5, 12, 0, 33, 7000, tzinfo=UTC))
    obj = json.loads(data.decode("utf-8"))
    assert obj.pop("published_at") == "2026-10-05T12:00:33.007Z"
    assert obj == json.loads(json.dumps(dict(env)))
    assert "published_at" not in env  # serialization does not mutate the stored envelope


def test_envelope_size_uses_a_fixed_width_placeholder() -> None:
    env = _env()
    assert len(E.PUBLISHED_AT_PLACEHOLDER) == len(E.iso_utc(FETCHED))
    size = E.envelope_size(env)
    for dt in (FETCHED, datetime(2030, 1, 1, tzinfo=UTC), datetime(2026, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)):
        assert len(E.serialize_envelope(env, dt)) == size


def test_envelope_size_counts_utf8_bytes() -> None:
    env = _env()
    text = E.serialize_envelope(env, FETCHED).decode("utf-8")
    assert E.envelope_size(env) == len(text.encode("utf-8"))
    assert E.envelope_size(env) > len(text)  # "Đường" is multi-byte


def test_iso_utc_rejects_naive_and_normalizes_offsets() -> None:
    with pytest.raises(ValueError):
        E.iso_utc(datetime(2026, 10, 5, 12, 0))
    assert E.iso_utc(datetime(2026, 10, 5, 8, 0, tzinfo=timezone(timedelta(hours=-4)))) == "2026-10-05T12:00:00.000Z"


def test_parse_iso_utc_roundtrip_and_rejects_naive() -> None:
    assert E.parse_iso_utc("2026-10-05T12:00:31.250Z") == FETCHED
    assert E.parse_iso_utc("2026-10-05T08:00:31.250-04:00") == FETCHED
    with pytest.raises(ValueError):
        E.parse_iso_utc("2026-10-05T12:00:31")


def test_kafka_framing() -> None:
    env = _env()
    assert E.kafka_key(RUN_ID, "alerts") == f"{RUN_ID}|alerts".encode()
    assert E.control_key(RUN_ID) == f"{RUN_ID}|control".encode()
    headers = dict(E.kafka_headers(env))
    assert headers == {"schema_version": b"1", "snapshot_id": env["snapshot_id"].encode(),
                       "capture_id": env["capture_id"].encode()}


# ------------------------------------------------------------------------------- envelope_from_capture
def _ref(cid: str) -> RawRef:
    return RawRef(raw_path=f"{RUN_ID}/vehicle_positions/2026-10-05/{cid}.json.gz", raw_sha256=RAW_SHA, raw_bytes=99,
                  stored_sha256="d" * 64, stored_bytes=50)


def _from_capture(selected: list[dict], cid: str, fetched_at: datetime, run: object = RUN,
                  parsed: ParsedFeed | None = None) -> E.Envelope:
    p = parsed or ParsedFeed(header=dict(HEADER), entities=[E1, E2], feed_timestamp=HEADER["timestamp"])
    return envelope_from_capture(
        run=run, feed_type="vehicle_positions", parsed=p, selected=selected, accounting={"selected": len(selected)},
        raw_ref=_ref(cid), capture_id=cid, fetched_at=fetched_at,
        source_url="https://cdn.mbta.com/realtime/VehiclePositions.json",
        http=dict(http_meta(200, {"etag": '"x"', "content-type": "application/json"})), scenario_id=None,
        component="collector",
    )


def test_snapshot_id_ignores_capture_id_fetch_time_and_entity_order() -> None:
    a = _from_capture([E1, E2], "vp-20261005T120031250000Z-abababababab", FETCHED)
    b = _from_capture([E2, E1], "vp-20261005T120101000000Z-abababababab", FETCHED + timedelta(seconds=30))
    expected = snapshot_id(RUN_ID, "scope0001", "vehicle_positions", HEADER["timestamp"], payload_sha(HEADER, [E1, E2]))
    assert a["snapshot_id"] == b["snapshot_id"] == expected
    assert b["entities"] == [E2, E1]                 # copied unchanged, in source order
    assert a["entities"][0] is E1
    assert a["http"]["etag"] == '"x"' and a["http"]["status"] == 200
    assert a["raw_path"].startswith(f"{RUN_ID}/")


def test_snapshot_id_is_run_specific() -> None:
    other = SimpleNamespace(run_id="11111111-2222-4333-8444-555566667777", scope_id="scope0001", source_type="replay",
                            gtfs_version="c" * 64)
    a = _from_capture([E1], "vp-20261005T120031250000Z-abababababab", FETCHED)
    b = _from_capture([E1], "vp-20261005T120031250000Z-abababababab", FETCHED, run=other)
    assert a["snapshot_id"] != b["snapshot_id"]
    assert b["source_type"] == "replay"


def test_source_entity_count_includes_non_object_entries() -> None:
    parsed = ParsedFeed(header=dict(HEADER), entities=[E1], feed_timestamp=HEADER["timestamp"],
                        warnings={"entity_not_object": 2})
    env = _from_capture([E1], "vp-20261005T120031250000Z-abababababab", FETCHED, parsed=parsed)
    assert env["source_entity_count"] == 3
    assert env["selected_entity_count"] == 1


def test_scope_accounting_matches_source_entity_count_with_non_object_entries() -> None:
    index = SimpleNamespace(route_ids=frozenset({"R1"}), route_type_by_route={"R1": 3}, agency_ids=frozenset({"1"}),
                            trip_to_route={}, scope_stop_ids=frozenset(), parent_station={})
    parsed = ParsedFeed(header=dict(HEADER), entities=[E1, {"id": "v9", "vehicle": {"trip": {"route_id": "R9"}}}],
                        feed_timestamp=HEADER["timestamp"], warnings={"entity_not_object": 2})
    selected, accounting = ScopeFilter(index).select("vehicle_positions", parsed.entities)
    assert accounting["source_entities"] == 2                        # the filter never saw the 2 non-objects
    env = envelope_from_capture(
        run=RUN, feed_type="vehicle_positions", parsed=parsed, selected=selected, accounting=accounting,
        raw_ref=_ref("vp-20261005T120031250000Z-abababababab"), capture_id="vp-20261005T120031250000Z-abababababab",
        fetched_at=FETCHED, source_url="https://cdn.mbta.com/realtime/VehiclePositions.json", http=None,
        scenario_id=None, component="collector")
    acc = env["scope_accounting"]
    assert acc["source_entities"] == env["source_entity_count"] == 4
    assert acc["invalid_entity"] == 2
    assert acc["source_entities"] == acc["selected"] + acc["out_of_scope"] + acc["unresolved"] + acc["invalid_entity"]
    assert accounting["source_entities"] == 2                        # caller's dict is not mutated
    again = envelope_from_capture(
        run=RUN, feed_type="vehicle_positions", parsed=parsed, selected=selected, accounting=acc,
        raw_ref=_ref("vp-20261005T120031250000Z-abababababab"), capture_id="vp-20261005T120031250000Z-abababababab",
        fetched_at=FETCHED, source_url="https://cdn.mbta.com/realtime/VehiclePositions.json", http=None,
        scenario_id=None, component="collector")
    assert again["scope_accounting"] == acc                          # reconciliation is idempotent


def test_capture_dlq_record_is_bounded_and_stable() -> None:
    kwargs = dict(run_id=RUN_ID, feed_type="alerts", topic="mbta.alerts.snapshots.v1", reason_code="INVALID_JSON",
                  detail="Expecting value at line 1 column 1", capture_id="al-20261005T120031250000Z-abababababab",
                  raw_path=f"{RUN_ID}/alerts/2026-10-05/al-20261005T120031250000Z-abababababab.json.gz",
                  raw_sha256=RAW_SHA, max_excerpt_chars=100, pipeline_version="p1", stage="collector_parse",
                  body=b"x" * 5000)
    rec = capture_dlq_record(**kwargs)
    assert set(rec) == set(DLQ_FIELDS)
    assert rec["excerpt"].startswith("detail=Expecting value")
    assert len(rec["excerpt"]) == 100 and rec["excerpt_truncated"] is True
    assert rec["severity"] == "error" and rec["disposition"] == "quarantined"
    assert rec["value_sha256"] == RAW_SHA and rec["raw_sha256"] == RAW_SHA
    assert rec["source_topic"] == "mbta.alerts.snapshots.v1"      # same as replay/Spark for this rejection
    assert rec["source_partition"] is None and rec["source_offset"] is None and rec["snapshot_id"] is None
    assert rec["error_id"].startswith("er_")
    assert capture_dlq_record(**kwargs)["error_id"] == rec["error_id"]
    other = capture_dlq_record(**{**kwargs, "reason_code": "INVALID_HEADER"})
    assert other["error_id"] != rec["error_id"]
