"""Golden / IT fixture materialization (interfaces §10) and a step-mode replay of the golden run."""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from transitpulse.config import load_config
from transitpulse.contracts import ids
from transitpulse.contracts.accounting import count_candidates
from transitpulse.contracts.envelope import iso_utc, parse_iso_utc
from transitpulse.contracts.policy import Policy
from transitpulse.obs.metrics import ComponentMetrics
from transitpulse.replay.envelopes import EnvelopeOutcome, RawLocation
from transitpulse.replay.publisher import Publisher, PublisherOptions
from transitpulse.replay.recording_verify import verify_recording
from transitpulse.replay.synthetic.gtfs_builder import deterministic_zip
from transitpulse.replay.synthetic.materialize import (
    FixtureError,
    compute_snapshot_labels,
    default_fixture_dir,
    materialize_fixture,
    write_run_labels,
)
from transitpulse.runs import RunRecord
from transitpulse.runtime.paths import WorkPaths

S1 = datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC)
S2 = datetime(2026, 10, 5, 12, 1, 0, tzinfo=UTC)
S5 = datetime(2026, 10, 5, 12, 3, 10, tzinfo=UTC)

STATIC = {
    "agency.txt": "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
                  "SYN,TransitPulse Synthetic (NOT MBTA),https://example.invalid,America/New_York,en\n",
    "routes.txt": "route_id,agency_id,route_short_name,route_long_name,route_type\nR_A,SYN,A,Synthetic A,3\n"
                  "R_B,SYN,B,Synthetic B,3\n",
    "stops.txt": "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
                 "S1,Stop 1,42.35000,-71.06000,0,\nS2,Stop 2,42.35100,-71.05900,0,\n",
    "trips.txt": "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id\nR_A,WKDY,T1,Stop 2,0,SH_A0\n",
    "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence,timepoint\n"
                      "T1,07:55:00,07:55:00,S1,1,1\nT1,08:00:00,08:00:00,S2,2,1\n",
    "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    "WKDY,1,1,1,1,1,0,0,20260901,20261231\n",
    "calendar_dates.txt": "service_id,date,exception_type\nWKDY,20261012,2\n",
    "shapes.txt": "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\nSH_A0,42.35,-71.06,1\nSH_A0,42.351,-71.059,2\n",
    "feed_info.txt": "feed_publisher_name,feed_publisher_url,feed_lang,feed_start_date,feed_end_date,feed_version\n"
                     "TransitPulse synthetic fixture (NOT MBTA data),https://example.invalid,en,20260901,20261231,"
                     "golden-v1\n",
}


def _feed(ts: datetime, entities: list[dict]) -> dict:
    return {"header": {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET",
                       "timestamp": int(ts.timestamp())}, "entity": entities}


def _vehicle(vid: str, observed: datetime | None) -> dict:
    v = {"trip": {"trip_id": "T1", "route_id": "R_A", "direction_id": 0, "start_date": "20261005"},
         "vehicle": {"id": vid, "label": vid}, "position": {"latitude": 42.3515, "longitude": -71.0585}}
    if observed is not None:
        v["timestamp"] = int(observed.timestamp())
    return {"id": f"vp-{vid}", "vehicle": v}


def _write_fixture(root: Path) -> Path:
    (root / "static").mkdir(parents=True)
    (root / "rt").mkdir()
    for name, text in STATIC.items():
        (root / "static" / name).write_text(text, encoding="utf-8")
    rt = {
        "VP1.json": _feed(S1 - timedelta(seconds=10), [_vehicle("V1", S1 - timedelta(seconds=20)),
                                                       _vehicle("V3", None)]),
        "VP2.json": _feed(S2, [_vehicle("V1", S2 - timedelta(seconds=5)), _vehicle("V3", None)]),
        "TU1.json": _feed(S1 - timedelta(seconds=10), []),
        "ALERTS1.json": _feed(S1 - timedelta(seconds=10), [
            {"id": "A1", "alert": {"informed_entity": [{"route_id": "R_A"}]}}]),
        "IT06_VP.json": _feed(S1, [_vehicle("V4", S1)]),
    }
    for name, doc in rt.items():
        (root / "rt" / name).write_text(json.dumps(doc, indent=1), encoding="utf-8")

    def cap(feed: str, file: str, at: datetime, label: str) -> dict:
        return {"kind": "capture", "feed_type": feed, "file": file, "at": iso_utc(at), "label": label}

    timeline = {
        "fixture_id": "golden-v1",
        "provenance": "synthetic, hand-written, NOT MBTA data",
        "agency_timezone": "America/New_York",
        "scope_route_ids": ["R_A", "R_B"],
        "runs": {
            "golden": {"start_clock": iso_utc(S1), "scope_route_ids": ["R_A", "R_B"], "steps": [
                {"step": "S1", "items": [{"kind": "tick", "at": iso_utc(S1)},
                                         cap("vehicle_positions", "rt/VP1.json", S1, "VP1"),
                                         cap("trip_updates", "rt/TU1.json", S1, "TU1"),
                                         cap("alerts", "rt/ALERTS1.json", S1, "ALERTS1")]},
                {"step": "S2", "items": [{"kind": "tick", "at": iso_utc(S2)},
                                         cap("vehicle_positions", "rt/VP2.json", S2, "VP2")]},
                {"step": "S3", "items": [{"kind": "republish", "label": "VP2", "at": iso_utc(S2)}]},
                {"step": "S4", "items": [{"kind": "republish", "label": "VP1", "at": iso_utc(S2)}]},
                {"step": "S5", "items": [{"kind": "tick", "at": iso_utc(S5)}]},
            ]},
            "IT06": {"start_clock": iso_utc(S1), "scope_route_ids": ["R_A", "R_B"], "steps": [
                {"step": "S1", "items": [{"kind": "tick", "at": iso_utc(S1)},
                                         cap("vehicle_positions", "rt/IT06_VP.json", S1, "IT06_VP")]},
                {"step": "S2", "items": [{"kind": "malformed_envelope", "feed_type": "vehicle_positions",
                                          "at": iso_utc(S1), "label": "IT06_BAD",
                                          "value_text": "{\"schema_version\": 1, \"entities\": [\"{\\\"id"}]},
            ]},
        },
    }
    (root / "timeline.json").write_text(json.dumps(timeline, indent=2), encoding="utf-8")
    return root


def _env(tmp_path: Path, name: str = "w"):
    cfg = load_config("local-native", workdir=str(tmp_path / name), environ={})
    return cfg, WorkPaths.from_workdir(cfg.workdir)


def _manifest(res) -> dict:
    return json.loads(res.manifest_path.read_text(encoding="utf-8"))


def test_golden_items_follow_step_order_with_barriers(tmp_path):
    fixture = _write_fixture(tmp_path / "fixture")
    cfg, paths = _env(tmp_path)
    res = materialize_fixture(cfg, paths, run_name="golden", fixture_dir=fixture)
    manifest = _manifest(res)
    shape = [(it["kind"], it.get("label") or it.get("params", {}).get("of_seq")) for it in manifest["items"]]
    seq_of = {it["label"]: it["seq"] for it in manifest["items"] if it["kind"] == "capture"}
    assert shape == [
        ("tick", None), ("capture", "VP1"), ("capture", "TU1"), ("capture", "ALERTS1"), ("barrier", "S1"),
        ("tick", None), ("capture", "VP2"), ("barrier", "S2"),
        ("republish", seq_of["VP2"]), ("barrier", "S3"),
        ("republish", seq_of["VP1"]), ("barrier", "S4"),
        ("tick", None), ("barrier", "S5"),
    ]
    assert [it["seq"] for it in manifest["items"]] == list(range(1, len(manifest["items"]) + 1))
    barriers = [it for it in manifest["items"] if it["kind"] == "barrier"]
    assert [b["params"]["label"] for b in barriers] == ["S1", "S2", "S3", "S4", "S5"]
    assert barriers[-1]["at"] == iso_utc(S5)
    assert manifest["fixture_kind"] == "golden_handwritten" and manifest["seed"] is None
    assert manifest["scenario_ids"] == ["golden"] and manifest["start_clock"] == iso_utc(S1)
    assert manifest["scope"]["route_ids"] == ["R_A", "R_B"]
    assert "NOT MBTA" in manifest["provenance"]

    labels = json.loads((res.recording_dir / "labels.json").read_text(encoding="utf-8"))
    assert set(labels) == {"VP1", "TU1", "ALERTS1", "VP2"}
    for label, file in (("VP1", "VP1.json"), ("VP2", "VP2.json")):
        raw = (fixture / "rt" / file).read_bytes()
        assert labels[label]["raw_sha256"] == ids.sha256_hex(raw)
        item = next(it for it in manifest["items"] if it.get("label") == label)
        assert gzip.decompress((res.recording_dir / item["raw_path"]).read_bytes()) == raw
        assert item["capture_id"] == labels[label]["capture_id"]
    static_zip = deterministic_zip({n: t.encode("utf-8") for n, t in STATIC.items()})
    assert res.static_zip_path.read_bytes() == static_zip
    assert res.gtfs_version == ids.sha256_hex(static_zip)
    assert manifest["static"]["feed_version"] == "golden-v1"

    verified = verify_recording(res.manifest_path, paths=paths, route_ids=["R_A", "R_B"])
    assert verified.start_clock == S1 and verified.item_count == 14


def test_golden_materialization_is_idempotent_and_deterministic(tmp_path):
    fixture = _write_fixture(tmp_path / "fixture")
    cfg, paths = _env(tmp_path, "a")
    first = materialize_fixture(cfg, paths, run_name="golden", fixture_dir=fixture)
    again = materialize_fixture(cfg, paths, run_name="golden", fixture_dir=fixture)
    assert again.reused and again.manifest_sha256 == first.manifest_sha256
    cfg_b, paths_b = _env(tmp_path, "b")
    other = materialize_fixture(cfg_b, paths_b, run_name="golden", fixture_dir=fixture)
    assert other.manifest_sha256 == first.manifest_sha256 and other.recording_id == first.recording_id
    (fixture / "rt" / "VP2.json").write_text(json.dumps(_feed(S2, [])), encoding="utf-8")
    changed = materialize_fixture(cfg, paths, run_name="golden", fixture_dir=fixture)
    assert changed.recording_id != first.recording_id          # edited fixture -> new recording


def test_it_scenario_run_keeps_malformed_envelope(tmp_path):
    fixture = _write_fixture(tmp_path / "fixture")
    cfg, paths = _env(tmp_path)
    res = materialize_fixture(cfg, paths, run_name="IT06", fixture_dir=fixture)
    manifest = _manifest(res)
    bad = next(it for it in manifest["items"] if it["kind"] == "malformed_envelope")
    timeline = json.loads((fixture / "timeline.json").read_text(encoding="utf-8"))
    assert bad["params"]["value_text"] == timeline["runs"]["IT06"]["steps"][1]["items"][0]["value_text"]
    assert bad["feed_type"] == "vehicle_positions" and bad["label"] == "IT06_BAD"
    assert manifest["scenario_ids"] == ["IT06"]
    labels = json.loads((res.recording_dir / "labels.json").read_text(encoding="utf-8"))
    assert labels["IT06_BAD"]["kind"] == "malformed_envelope"
    verify_recording(res.manifest_path, paths=paths)
    with pytest.raises(FixtureError):
        materialize_fixture(cfg, paths, run_name="IT99", fixture_dir=fixture)


# ---------------------------------------------------------------------------------------- replay


class _Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def monotonic(self) -> float:
        return self.t

    def sleep(self, dt: float) -> None:
        self.t += max(0.0, dt)

    def wall(self) -> datetime:
        return datetime(2026, 10, 7, tzinfo=UTC) + timedelta(seconds=self.t)


class _Producer:
    def __init__(self, clock: _Clock) -> None:
        self.clock = clock
        self.ops: list[tuple[str, dict | None, float]] = []
        self._acks: list = []

    def send_envelope(self, env, *, on_ack=None, on_error=None):
        self.ops.append(("envelope", dict(env), self.clock.t))
        self._acks.append(on_ack)

    def send_control(self, event, *, on_ack=None, on_error=None):
        self.ops.append(("control", dict(event), self.clock.t))
        self._acks.append(on_ack)

    def send_dlq(self, record):
        self.ops.append(("dlq", dict(record), self.clock.t))

    def send_raw(self, topic, key, value):
        self.ops.append(("raw", {"topic": topic}, self.clock.t))

    def flush(self, timeout: float = 30.0) -> int:
        for i, cb in enumerate(self._acks):
            if cb is not None:
                cb("topic", 0, i, self.clock.wall())
        self._acks.clear()
        return 0

    def close(self):
        pass


class _DiskEnveloper:
    """Reads the materialized gz captures from disk; snapshot ids from contracts.ids (no WP2 modules)."""

    def __init__(self, run: RunRecord, recording_dir: Path) -> None:
        self.run = run
        self.dir = recording_dir

    def read_capture(self, capture) -> bytes:
        data = gzip.decompress((self.dir / capture.raw_path).read_bytes())
        assert ids.sha256_hex(data) == capture.raw_sha256
        return data

    def raw_location(self, capture) -> RawLocation:
        return RawLocation(capture.raw_path, capture.raw_sha256, capture.raw_bytes or 0, capture.stored_sha256, 0)

    def build(self, *, feed_type, body, fetched_at, raw, capture_id, source_url, http_status) -> EnvelopeOutcome:
        doc = json.loads(body)
        header, entities = doc["header"], doc.get("entity") or []
        sid = ids.snapshot_id(self.run.run_id, self.run.scope_id, feed_type, header["timestamp"],
                              ids.payload_sha(header, entities))
        env = {"snapshot_id": sid, "capture_id": capture_id, "feed_type": feed_type,
               "original_fetched_at": iso_utc(fetched_at), "entities": entities}
        return EnvelopeOutcome(env, None, None, count_candidates(feed_type, entities), len(entities),
                               header["timestamp"], len(body))

    def archive_injected(self, **kwargs) -> RawLocation:
        raise AssertionError("no injection expected in this test")


def _run_record(verified) -> RunRecord:
    return RunRecord(
        run_id="run-golden-0001", project_id="transitpulse-mbta", scope_id="00112233aabbccdd",
        source_type="synthetic", clock_mode="source", gtfs_version=verified.gtfs_version,
        agency_timezone="America/New_York", route_ids=("R_A", "R_B"), pipeline_version="p1",
        policy=Policy("policy-v1", 120, 90, 300, 300, 5, 43200, "mbta_bus_uncertainty_v1",
                      "first_future_resolved_stop_v1", 100000000000, 3600, 259200, 60),
        created_at=datetime(2026, 10, 7, tzinfo=UTC), initial_clock=verified.start_clock,
        static_zip_path=str(verified.static_zip_path), static_scope_dir="/unused", scenario_id="golden",
        fixture_kind="golden_handwritten", recording_id=verified.recording_id,
    )


def test_step_mode_replay_of_golden_steps(tmp_path):
    fixture = _write_fixture(tmp_path / "fixture")
    cfg, paths = _env(tmp_path)
    res = materialize_fixture(cfg, paths, run_name="golden", fixture_dir=fixture)
    verified = verify_recording(res.manifest_path, paths=paths)
    run = _run_record(verified)
    clock = _Clock()
    producer = _Producer(clock)
    enveloper = _DiskEnveloper(run, verified.recording_dir)

    script = [(1000.0 + k, f"rq_{k}") for k in range(1, 7)]

    class _Steps:
        def poll(self):
            if script and clock.t >= script[0][0]:
                return {"request_id": script.pop(0)[1], "command": "step", "args": {}}
            return None

    pub = Publisher(
        run=run, items=verified.items, start_clock=verified.start_clock, session_dir=tmp_path / "session",
        producer=producer, enveloper=enveloper, options=PublisherOptions(step_mode=True),
        fault_dir=tmp_path / "faults", run_dir=tmp_path / "run", control=_Steps(),
        metrics=ComponentMetrics("publisher"), monotonic=clock.monotonic, sleep=clock.sleep, wall_now=clock.wall,
    )
    result = pub.run()
    labels = json.loads((verified.recording_dir / "labels.json").read_text(encoding="utf-8"))
    label_of = {v["capture_id"]: k for k, v in labels.items()}
    sent = [(label_of[p["capture_id"]], int(t - 1000)) for k, p, t in producer.ops if k == "envelope"]
    assert sent == [("VP1", 1), ("TU1", 1), ("ALERTS1", 1), ("VP2", 2), ("VP2", 3), ("VP1", 4)]
    clocks = [p for k, p, _ in producer.ops if k == "control" and p["event_type"] in
              {"run_started", "clock_tick", "run_finished"}]
    values = [parse_iso_utc(c["source_clock"]) for c in clocks]
    assert values == sorted(values) and values[-1] == S5        # S4 late re-delivery never moved the clock back
    assert result.finished_reason == "completed" and result.source_clock == S5
    env = {label_of[p["capture_id"]]: p for k, p, _ in producer.ops if k == "envelope"}
    assert env["VP1"]["original_fetched_at"] == iso_utc(S1)

    snapshot_labels = compute_snapshot_labels(verified, enveloper)
    assert snapshot_labels["VP1"] == env["VP1"]["snapshot_id"] and snapshot_labels["VP2"] != snapshot_labels["VP1"]
    target = write_run_labels(tmp_path / "runlabels", run, verified, enveloper)
    doc = json.loads(target.read_text(encoding="utf-8"))
    assert doc["snapshots"]["VP2"] == env["VP2"]["snapshot_id"] and doc["captures"]["TU1"]["capture_id"]


@pytest.mark.skipif(not (default_fixture_dir() / "timeline.json").is_file(), reason="golden fixture not present")
@pytest.mark.parametrize("run_name", ["golden", "IT06", "IT07", "IT08", "IT09"])
def test_committed_golden_fixture_materializes(tmp_path, run_name):
    cfg, paths = _env(tmp_path)
    res = materialize_fixture(cfg, paths, run_name=run_name)
    verified = verify_recording(res.manifest_path, paths=paths)
    timeline = json.loads((default_fixture_dir() / "timeline.json").read_text(encoding="utf-8"))
    steps = [s["step"] for s in timeline["runs"][run_name]["steps"]]
    manifest = _manifest(res)
    assert [it["params"]["label"] for it in manifest["items"] if it["kind"] == "barrier"] == steps
    assert verified.start_clock == parse_iso_utc(timeline["runs"][run_name]["start_clock"])
    for item in manifest["items"]:
        if item["kind"] == "capture":
            assert item["parse_status"] == "ok", item
