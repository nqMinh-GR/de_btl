"""Collector pipeline tests with a scripted source (httpx.MockTransport) and a fake producer.

Runs are ``source_type=synthetic`` with ``scenario_id=simulated_source_fault:*`` (D-E28): the responses
are simulated source faults, never MBTA observations. No network, no Kafka, no threads are started
(``open`` + ``poll_feed`` + ``close`` are driven directly).
"""

from __future__ import annotations

import json
import time
from collections import namedtuple
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from transitpulse.config import load_config
from transitpulse.contracts.ids import capture_id, sha256_hex
from transitpulse.ingestion.archive import RawArchive
from transitpulse.ingestion.collector import (
    EXIT_CONFLICT,
    EXIT_OK,
    EXIT_PREFLIGHT,
    EXIT_UNSUPPORTED,
    Collector,
    CollectorError,
    check_source_policy,
)
from transitpulse.ingestion.outbox import Outbox
from transitpulse.ingestion.recording import verify_manifest_sha256
from transitpulse.runtime.paths import WorkPaths

RUN_ID = "5b0c7e1e-1111-4222-8333-444455556666"
GTFS = "a" * 64
Usage = namedtuple("Usage", "total used free")
Handler = Callable[[httpx.Request], httpx.Response]


def big_disk(path: str) -> Usage:
    return Usage(10**13, 0, 10**12)


def make_run(**over: Any) -> SimpleNamespace:
    base: dict[str, Any] = dict(
        run_id=RUN_ID, scope_id="scope-unit", source_type="synthetic", scenario_id="simulated_source_fault:unit",
        clock_mode="source", initial_clock=datetime(2026, 10, 5, 12, 0, tzinfo=UTC), gtfs_version=GTFS,
        route_ids=("R1",), recording_id=None, fixture_kind=None, seed=None, static_zip_path=None,
        created_at=datetime(2026, 10, 5, 11, 0, tzinfo=UTC), static_scope_dir="/nonexistent",
    )
    base.update(over)
    return SimpleNamespace(**base)


INDEX = SimpleNamespace(
    gtfs_version=GTFS, scope_id="scope-unit", route_ids=frozenset({"R1"}), route_type_by_route={"R1": 3},
    agency_by_route={"R1": "1"}, agency_ids=frozenset({"1"}), agency_timezone="America/New_York",
    trip_to_route={"T1": "R1", "T9": "R9"}, scope_stop_ids=frozenset({"S1"}), stops_by_route={"R1": ["S1"]},
    parent_station={},
)


def vp_body(*, extra: int = 0, ts: int | None = None) -> bytes:
    stamp = ts if ts is not None else int(time.time()) - 5
    entities = [
        {"id": "e1", "vehicle": {"trip": {"route_id": "R1", "trip_id": "T1"}, "vehicle": {"id": "y1"},
                                 "position": {"latitude": 42.35, "longitude": -71.06}, "timestamp": stamp - 3}},
        {"id": "e2", "vehicle": {"trip": {"route_id": "R9"}, "vehicle": {"id": "y2"}}},
    ]
    entities += [{"id": f"x{i}", "vehicle": {"trip": {"trip_id": "T1"}, "vehicle": {"id": f"z{i}"}, "pad": "p" * 200}}
                 for i in range(extra)]
    return json.dumps({"header": {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET",
                                  "timestamp": stamp}, "entity": entities}).encode()


class Source:
    """Per-feed scripted responses (the last one repeats)."""

    def __init__(self) -> None:
        self.script: dict[str, list[Handler]] = {}
        self.requests: list[httpx.Request] = []

    def set(self, feed_path: str, *handlers: Handler) -> None:
        self.script[feed_path] = list(handlers)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        name = request.url.path.rsplit("/", 1)[-1]
        queue = self.script.get(name)
        if not queue:
            return httpx.Response(404)
        handler = queue.pop(0) if len(queue) > 1 else queue[0]
        return handler(request)


class FakeProducer:
    """Auto-acknowledging producer double: records control events and envelopes."""

    def __init__(self) -> None:
        self.generation = 0
        self.envelopes: list[dict[str, Any]] = []
        self.controls: list[dict[str, Any]] = []
        self.dlq: list[dict[str, Any]] = []
        self.offset = 0
        self.closed = False

    def _ack(self, on_ack: Any, topic: str) -> None:
        self.offset += 1
        if on_ack is not None:
            on_ack(topic, 0, self.offset, datetime.now(UTC))

    def send_envelope(self, env: dict[str, Any], *, on_ack: Any = None, on_error: Any = None) -> None:
        self.envelopes.append(env)
        self._ack(on_ack, "snapshots")

    def send_dlq(self, record: dict[str, Any], *, on_ack: Any = None, on_error: Any = None) -> None:
        self.dlq.append(record)
        self._ack(on_ack, "mbta.dlq.v1")

    def send_control(self, event: dict[str, Any]) -> None:
        self.controls.append(event)

    def poll(self, timeout: float = 0.0) -> int:
        return 0

    def flush(self, timeout: float = 30.0) -> int:
        return 0

    def close(self) -> None:
        self.closed = True


@pytest.fixture()
def env(tmp_path: Path) -> SimpleNamespace:
    source = Source()

    def build(*, publish: bool = False, overrides: dict[str, Any] | None = None, run: Any = None,
              producer: FakeProducer | None = None, disk: Any = big_disk) -> Collector:
        over = {"collector": {"http": {"max_attempts": 1}}}
        for key, value in (overrides or {}).items():
            over.setdefault(key, {}).update(value)
        cfg = load_config("local-native", workdir=tmp_path, environ={}, overrides=over)
        return Collector(cfg, WorkPaths.from_workdir(tmp_path), run or make_run(), INDEX, publish=publish,
                         transport_factory=lambda feed: httpx.MockTransport(source),
                         producer_factory=(lambda: producer) if producer is not None else None, disk_usage=disk)

    return SimpleNamespace(source=source, build=build, paths=WorkPaths.from_workdir(tmp_path))


def index_records(paths: WorkPaths) -> list[dict[str, Any]]:
    return RawArchive(paths.raw_root).read_index(RUN_ID)


def capture_files(paths: WorkPaths) -> list[Path]:
    return sorted((paths.raw_root / RUN_ID).glob("*/*/*.json.gz"))


def test_valid_capture_becomes_a_pending_scope_filtered_envelope(env: SimpleNamespace) -> None:
    env.source.set("VehiclePositions.json", lambda r: httpx.Response(200, content=vp_body(),
                                                                     headers={"ETag": '"v1"'}))
    c = env.build()
    c.open()
    assert c.poll_feed("vehicle_positions") is None
    rows = c.outbox.pending_rows(10)
    assert len(rows) == 1 and rows[0].kind == "snapshot" and rows[0].feed_type == "vehicle_positions"
    payload = c.outbox.load_payload(rows[0].message_id)
    assert [e["id"] for e in payload["entities"]] == ["e1"]
    assert payload["source_entity_count"] == 2 and payload["selected_entity_count"] == 1
    assert payload["scope_accounting"]["out_of_scope"] == 1
    assert payload["raw_path"].startswith(f"{RUN_ID}/vehicle_positions/")
    archive = RawArchive(env.paths.raw_root)
    body = archive.read_capture(payload["raw_path"], expected_sha256=payload["raw_sha256"])
    assert payload["capture_id"].endswith(sha256_hex(body)[:12])
    meta = archive.read_meta(payload["raw_path"])
    assert meta["capture_status"] == "ok" and meta["snapshot_id"] == payload["snapshot_id"]
    assert meta["outbox_state"] == "pending" and meta["headers"]["etag"] == '"v1"'
    [rec] = index_records(env.paths)
    assert rec["kind"] == "capture" and rec["parse_status"] == "ok" and rec["snapshot_id"] == payload["snapshot_id"]
    st = c.feed_states["vehicle_positions"]
    assert st.feed_state == "healthy" and st.etag == '"v1"' and st.ok == 1
    c.close()


def test_non_object_entities_are_accounted_as_invalid(env: SimpleNamespace) -> None:
    doc = json.loads(vp_body())
    doc["entity"] += ["junk", 7]
    env.source.set("VehiclePositions.json", lambda r: httpx.Response(200, content=json.dumps(doc).encode()))
    c = env.build()
    c.open()
    c.poll_feed("vehicle_positions")
    [row] = c.outbox.pending_rows(10)
    payload = c.outbox.load_payload(row.message_id)
    acc = payload["scope_accounting"]
    assert payload["source_entity_count"] == 4 == acc["source_entities"]
    assert acc["invalid_entity"] == 2 and acc["selected"] == 1 and acc["out_of_scope"] == 1
    assert acc["source_entities"] == acc["selected"] + acc["out_of_scope"] + acc["unresolved"] + acc["invalid_entity"]
    meta = RawArchive(env.paths.raw_root).read_meta(payload["raw_path"])
    assert meta["scope_accounting"] == acc and meta["parse_warnings"] == {"entity_not_object": 2}
    c.close()


def test_conditional_get_304_creates_no_observation(env: SimpleNamespace) -> None:
    def second(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("if-none-match") == '"v1"'
        return httpx.Response(304)

    env.source.set("VehiclePositions.json",
                   lambda r: httpx.Response(200, content=vp_body(), headers={"ETag": '"v1"'}), second)
    c = env.build()
    c.open()
    c.poll_feed("vehicle_positions")
    c.poll_feed("vehicle_positions")
    assert len(capture_files(env.paths)) == 1
    assert len(c.outbox.pending_rows(10)) == 1
    kinds = [(r["kind"], r.get("outcome")) for r in index_records(env.paths)]
    assert kinds == [("capture", None), ("fetch_event", "not_modified")]
    assert c.feed_states["vehicle_positions"].not_modified == 1
    c.close()


def test_malformed_body_is_kept_rejected_and_dead_lettered(env: SimpleNamespace) -> None:
    env.source.set("Alerts.json", lambda r: httpx.Response(200, content=b"<html>oops</html>", headers={"ETag": '"x"'}))
    c = env.build()
    c.open()
    c.poll_feed("alerts")
    assert c.outbox.pending_rows(10)[0].kind == "dlq"
    counts = c.outbox.counts()
    assert counts["snapshot"] == {"pending": 0, "published": 0, "rejected": 1}
    assert counts["dlq"]["pending"] == 1
    [raw] = capture_files(env.paths)
    assert raw.parent.parent.name == "alerts"
    [rec] = index_records(env.paths)
    assert rec["parse_status"] == "INVALID_JSON" and rec["snapshot_id"] is None
    st = c.feed_states["alerts"]
    assert st.feed_state == "degraded" and st.rejected_by_reason == {"INVALID_JSON": 1}
    assert st.etag is None                     # validators of a rejected body are never reused
    c.close()


def test_failed_fetch_never_becomes_a_snapshot(env: SimpleNamespace) -> None:
    env.source.set("TripUpdates.json", lambda r: httpx.Response(503, content=b"maintenance"))
    c = env.build()
    c.open()
    c.poll_feed("trip_updates")
    assert capture_files(env.paths) == []
    assert c.outbox.counts()["snapshot"] == {"pending": 0, "published": 0, "rejected": 0}
    [rec] = index_records(env.paths)
    assert rec["kind"] == "fetch_event" and rec["outcome"] == "http_error"
    assert rec["reason_code"] == "SOURCE_FETCH_FAILED" and rec["http_status"] == 503
    assert rec["error_body_path"].startswith(f"{RUN_ID}/trip_updates/") and "/errors/" in rec["error_body_path"]
    st = c.feed_states["trip_updates"]
    assert st.feed_state == "degraded" and st.consecutive_failures == 1 and st.failed == 1
    c.close()


def test_payload_too_large_is_rejected_without_truncation(env: SimpleNamespace) -> None:
    env.source.set("VehiclePositions.json", lambda r: httpx.Response(200, content=vp_body(extra=30)))
    c = env.build(overrides={"kafka": {"max_snapshot_bytes": 3000}})
    c.open()
    c.poll_feed("vehicle_positions")
    [rec] = index_records(env.paths)
    assert rec["parse_status"] == "PAYLOAD_TOO_LARGE" and rec["snapshot_id"] is not None
    row = c.outbox.get(rec["capture_id"])
    assert row.state == "rejected" and row.reason == "PAYLOAD_TOO_LARGE" and row.payload_bytes > 3000
    dlq = c.outbox.get(f"dlq:{rec['capture_id']}")
    assert dlq is not None and dlq.state == "pending"
    assert len(capture_files(env.paths)) == 1        # raw kept
    c.close()


def test_close_finalizes_a_complete_recording_and_releases_the_lock(env: SimpleNamespace) -> None:
    env.source.set("VehiclePositions.json", lambda r: httpx.Response(200, content=vp_body()))
    env.source.set("Alerts.json", lambda r: httpx.Response(502))
    c = env.build()
    c.open()
    c.poll_feed("vehicle_positions")
    c.poll_feed("alerts")
    result = c.close()
    assert result.exit_code == EXIT_OK and result.complete is True and result.reason == "finish_requested"
    manifest = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
    assert verify_manifest_sha256(manifest) and manifest["complete"] is True
    assert sorted(i["kind"] for i in manifest["items"]) == ["capture", "fetch_event"]
    assert [i["seq"] for i in manifest["items"]] == [1, 2]
    cap = next(i for i in manifest["items"] if i["kind"] == "capture")
    assert cap["raw_path"].startswith("vehicle_positions/")      # relative to W/data/raw/<run_id>/
    assert RawArchive(env.paths.raw_run_dir(RUN_ID)).read_capture(cap["raw_path"], expected_sha256=cap["raw_sha256"])
    assert manifest["feeds"]["alerts"]["failed_fetch_count"] == 1
    assert result.to_json("record")["ok"] is True
    Outbox(env.paths.outbox_db(RUN_ID), run_id=RUN_ID).close()   # lock released
    assert c.close() is result                                      # idempotent


def test_restart_reconciles_a_capture_whose_outbox_insert_was_lost(env: SimpleNamespace) -> None:
    body = vp_body()
    at = datetime.now(UTC) - timedelta(seconds=1)
    cid = capture_id("vp", at, sha256_hex(body))
    archive = RawArchive(env.paths.raw_root)
    archive.write_capture(RUN_ID, "vehicle_positions", cid, at, body,
                          {"http_status": 200, "http": {"status": 200, "etag": None, "last_modified": None,
                                                        "content_type": "application/json"}})
    c = env.build()
    c.open()
    assert c.reconcile_stats["rebuilt"] == 1
    row = c.outbox.get(cid)
    assert row is not None and row.state == "pending"
    assert [r["capture_id"] for r in index_records(env.paths)] == [cid]
    c.close()
    c2 = env.build()
    c2.open()                                          # second restart: nothing left to repair
    assert c2.reconcile_stats["rebuilt"] == 0 and c2.reconcile_stats["indexed"] == 0
    c2.close()


def test_publishing_path_acks_rows_and_emits_status_and_lifecycle_events(env: SimpleNamespace) -> None:
    env.source.set("VehiclePositions.json", lambda r: httpx.Response(200, content=vp_body()))
    env.source.set("Alerts.json", lambda r: httpx.Response(200, content=b"not json"))
    producer = FakeProducer()
    c = env.build(publish=True, producer=producer)
    c.open()
    c.poll_feed("vehicle_positions")
    c.poll_feed("alerts")
    statuses = [e for e in producer.controls if e["event_type"] == "collector_status"]
    assert [(s["feed_type"], s["outcome"], s["reason_code"]) for s in statuses] == [
        ("vehicle_positions", "ok", None), ("alerts", "rejected", "INVALID_JSON")]
    assert statuses[0]["feed_state"] == "healthy" and statuses[1]["feed_state"] == "degraded"
    assert statuses[0]["outbox_pending"] == 1
    assert c.dispatcher.step(poll_timeout=0) == 2      # snapshot + DLQ, acknowledged by the fake
    counts = c.outbox.counts()
    assert counts["snapshot"]["published"] == 1 and counts["dlq"]["published"] == 1
    [envelope] = producer.envelopes
    assert envelope["feed_type"] == "vehicle_positions" and "published_at" not in envelope
    assert producer.dlq[0]["reason_code"] == "INVALID_JSON"
    result = c.close()
    assert result.exit_code == EXIT_OK and producer.closed
    finished = [e for e in producer.controls if e["event_type"] == "run_finished"]
    assert len(finished) == 1 and finished[0]["reason"] == "finish_requested"
    assert any(e["event_type"] == "collector_heartbeat" for e in producer.controls)


def test_disk_budget_is_fail_stop(env: SimpleNamespace) -> None:
    env.source.set("VehiclePositions.json", lambda r: httpx.Response(200, content=vp_body()))
    c = env.build(overrides={"collector": {"raw_disk_budget_bytes": 1000}})
    c.open()
    c.poll_feed("vehicle_positions")
    assert c.stop_reason == "disk_budget"
    assert capture_files(env.paths) == []
    assert all(st.feed_state == "failed" for st in c.feed_states.values())
    assert c.poll_feed("trip_updates") is None           # no further polling once stopped
    result = c.close()
    assert result.exit_code == EXIT_PREFLIGHT and result.complete is False
    assert result.error["code"] == "DISK_BUDGET_EXCEEDED"
    manifest = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
    assert manifest["complete"] is False
    assert manifest["items"][0]["params"]["outcome"] == "disk_budget_exceeded"


def test_start_is_refused_when_free_space_is_below_the_minimum(env: SimpleNamespace) -> None:
    c = env.build(disk=lambda path: Usage(10**9, 10**9, 10))
    with pytest.raises(CollectorError) as info:
        c.open()
    assert info.value.exit_code == EXIT_PREFLIGHT
    Outbox(env.paths.outbox_db(RUN_ID), run_id=RUN_ID).close()   # lock released after the failed start


def test_second_collector_for_the_same_run_is_a_conflict(env: SimpleNamespace) -> None:
    first = env.build()
    first.open()
    with pytest.raises(CollectorError) as info:
        env.build().open()
    assert info.value.exit_code == EXIT_CONFLICT and info.value.code == "OUTBOX_LOCKED"
    first.close()


def test_static_mismatch_is_a_preflight_error(env: SimpleNamespace) -> None:
    with pytest.raises(CollectorError) as info:
        env.build(run=make_run(gtfs_version="b" * 64)).open()
    assert info.value.exit_code == EXIT_PREFLIGHT and info.value.code == "STATIC_VERSION_MISMATCH"


def test_source_policy() -> None:
    mbta = {"vehicle_positions": "https://cdn.mbta.com/realtime/VehiclePositions.json"}
    fake = {"vehicle_positions": "http://127.0.0.1:8099/VehiclePositions.json"}
    check_source_policy(make_run(source_type="live"), mbta)
    check_source_policy(make_run(), fake)
    for run, urls in ((make_run(source_type="live"), fake), (make_run(source_type="replay"), mbta),
                      (make_run(scenario_id=None), fake), (make_run(source_type="weird"), mbta)):
        with pytest.raises(CollectorError) as info:
            check_source_policy(run, urls)
        assert info.value.exit_code == EXIT_UNSUPPORTED
