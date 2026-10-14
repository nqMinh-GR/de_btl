"""Replay publisher clock/control behaviour with a simulated clock and fake Kafka (no services).

Covered: ticks precede (and are flushed before) the snapshots that follow them, periodic ticks while
waiting, the clock never goes backwards (late items, republish), pause freezes the source clock while
heartbeats continue, step mode stops at barriers, restart continues clock_sequence, fault arming,
malformed envelopes, replayed rejections/fetch events, injections and command rejections.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from transitpulse.contracts import ids
from transitpulse.contracts.accounting import count_candidates
from transitpulse.contracts.envelope import iso_utc, parse_iso_utc
from transitpulse.contracts.policy import Policy
from transitpulse.obs.metrics import ComponentMetrics
from transitpulse.replay import control as pc
from transitpulse.replay.envelopes import EnvelopeOutcome, RawLocation
from transitpulse.replay.publisher import Publisher, PublisherError, PublisherOptions
from transitpulse.replay.timeline import build_timeline
from transitpulse.runs import RunRecord

T0 = datetime(2026, 10, 5, 12, 0, 0, tzinfo=UTC)
WALL0 = datetime(2026, 10, 7, 9, 0, 0, tzinfo=UTC)
MONO0 = 1000.0
CLOCK_EVENTS = {"run_started", "clock_tick", "run_paused", "run_resumed", "run_finished"}
VP_TOPIC = "mbta.vehicle_positions.snapshots.v1"

POLICY = Policy(
    policy_version="policy-v1", vehicle_stale_seconds=120, prediction_stale_seconds=90, alert_stale_seconds=300,
    predicted_late_seconds=300, future_tolerance_seconds=5, service_date_inference_window_seconds=43200,
    uncertainty_policy="mbta_bus_uncertainty_v1", target_policy="first_future_resolved_stop_v1",
    timestamp_ms_threshold=100000000000, current_generation_ttl_seconds=3600, history_ttl_seconds=259200,
    min_publish_ttl_seconds=60,
)


def make_run(source_type: str = "synthetic") -> RunRecord:
    live = source_type == "live"
    return RunRecord(
        run_id="run-test-0001", project_id="transitpulse-mbta", scope_id="0123456789abcdef",
        source_type=source_type, clock_mode="wall" if live else "source", gtfs_version="a" * 64,
        agency_timezone="America/New_York", route_ids=("R_A",), pipeline_version="p1", policy=POLICY,
        created_at=WALL0, initial_clock=None if live else T0, static_zip_path="/w/x.zip", static_scope_dir="/w/x",
        scenario_id="unit",
    )


class FakeClock:
    def __init__(self) -> None:
        self.t = MONO0

    def monotonic(self) -> float:
        return self.t

    def sleep(self, dt: float) -> None:
        self.t += max(0.0, dt)

    def wall(self) -> datetime:
        return WALL0 + timedelta(seconds=self.t - MONO0)


class FakeProducer:
    """Implements the EnvelopeProducer methods used by the publisher; delivery reports are served on flush().

    * ``fail_control_type``: every delivery report of that control event type is an error (on_error).
    * ``envelope_reports``: per send_envelope call, one of ``ack`` (default), ``error``, ``hold_error`` or
      ``hold_ack``. ``hold_*`` copies stay queued through one flush (flush returns > 0, like a flush timeout)
      and get their error/ACK during the NEXT flush.
    * ``envelope_exceptions``: exceptions raised by successive send_envelope calls (local send failures).
    """

    def __init__(self, clock: FakeClock, fail_on_envelope: int | None = None,
                 fail_control_type: str | None = None, envelope_reports: list[str] | None = None,
                 envelope_exceptions: list[BaseException] | None = None) -> None:
        self.clock = clock
        self.ops: list[tuple[str, dict | None, float]] = []
        self._pending: list = []
        self._held: list = []
        self._offset = 0
        self.fail_on = fail_on_envelope
        self.fail_control_type = fail_control_type
        self.envelope_reports = list(envelope_reports or [])
        self.envelope_exceptions = list(envelope_exceptions or [])
        self.envelope_calls = 0

    def send_envelope(self, env, *, on_ack=None, on_error=None) -> None:
        self.envelope_calls += 1
        if self.fail_on is not None and self.envelope_calls >= self.fail_on:
            raise RuntimeError("broker unavailable")
        if self.envelope_exceptions:
            raise self.envelope_exceptions.pop(0)
        self.ops.append(("envelope", dict(env), self.clock.t))
        mode = self.envelope_reports.pop(0) if self.envelope_reports else "ack"
        self._pending.append((on_ack, on_error, mode))

    def send_control(self, event: dict, *, on_ack=None, on_error=None) -> None:
        self.ops.append(("control", dict(event), self.clock.t))
        if on_ack is not None or on_error is not None:
            failing = event.get("event_type") == self.fail_control_type
            self._pending.append((on_ack, on_error, "error" if failing else "ack"))

    def send_dlq(self, record: dict) -> None:
        self.ops.append(("dlq", dict(record), self.clock.t))

    def send_raw(self, topic: str, key: bytes, value: bytes) -> None:
        self.ops.append(("raw", {"topic": topic, "key": key, "value": value}, self.clock.t))

    def _report(self, on_ack, on_error, ok: bool) -> None:
        self._offset += 1
        if ok:
            if on_ack is not None:
                on_ack(VP_TOPIC, 0, self._offset, self.clock.wall())
        elif on_error is not None:
            on_error("KafkaError{code=_MSG_TIMED_OUT}")

    def flush(self, timeout: float = 30.0) -> int:
        self.ops.append(("flush", None, self.clock.t))
        held_before, self._held = self._held, []
        for on_ack, on_error, mode in held_before:            # late reports of copies queued earlier
            self._report(on_ack, on_error, mode == "hold_ack")
        pending, self._pending = self._pending, []
        for on_ack, on_error, mode in pending:
            if mode.startswith("hold_"):
                self._held.append((on_ack, on_error, mode))
            else:
                self._report(on_ack, on_error, mode == "ack")
        return len(self._held)

    def close(self) -> None:
        pass

    def controls(self, *types: str) -> list[dict]:
        return [p for k, p, _ in self.ops if k == "control" and (not types or p["event_type"] in types)]

    def envelopes(self) -> list[tuple[dict, float]]:
        return [(p, t) for k, p, t in self.ops if k == "envelope"]


class FakeEnveloper:
    def __init__(self, run: RunRecord, bodies: dict[str, bytes]) -> None:
        self.run = run
        self.bodies = dict(bodies)
        self.injected: dict[str, tuple[bytes, dict]] = {}

    def read_capture(self, capture) -> bytes:
        return self.bodies[capture.capture_id]

    def raw_location(self, capture) -> RawLocation:
        return RawLocation(capture.raw_path, capture.raw_sha256, capture.raw_bytes or 0,
                           capture.stored_sha256 or "", 0)

    def build(self, *, feed_type, body, fetched_at, raw, capture_id, source_url, http_status) -> EnvelopeOutcome:
        try:
            doc = json.loads(body)
        except ValueError:
            return EnvelopeOutcome(None, "INVALID_JSON", "bad json", 0, 0, None)
        header = doc["header"]
        entities = doc.get("entity") or []
        sid = ids.snapshot_id(self.run.run_id, self.run.scope_id, feed_type, header["timestamp"],
                              ids.payload_sha(header, entities))
        env = {"run_id": self.run.run_id, "feed_type": feed_type, "snapshot_id": sid, "capture_id": capture_id,
               "raw_path": raw.raw_path, "original_fetched_at": iso_utc(fetched_at),
               "feed_timestamp": header["timestamp"], "header": header, "entities": entities}
        return EnvelopeOutcome(env, None, None, count_candidates(feed_type, entities), len(entities),
                               header["timestamp"], len(body) + 200)

    def archive_injected(self, *, feed_type, capture_id, fetched_at, body, meta) -> RawLocation:
        self.injected[capture_id] = (body, meta)
        return RawLocation(f"{self.run.run_id}/{feed_type}/{capture_id}.json.gz", ids.sha256_hex(body), len(body),
                           "", 0)


class ScriptedControl:
    """Returns (request_id, command, args) when the simulated clock reaches MONO0 + offset."""

    def __init__(self, clock: FakeClock, script) -> None:
        self.clock = clock
        self.script = sorted(script, key=lambda s: s[0])
        self.n = 0

    def poll(self):
        if self.script and self.clock.t >= MONO0 + self.script[0][0]:
            _, command, args = self.script.pop(0)
            self.n += 1
            return {"request_id": f"rq_{self.n}", "command": command, "args": dict(args)}
        return None


class Recording:
    def __init__(self) -> None:
        self.items: list[dict] = []
        self.bodies: dict[str, bytes] = {}
        self.seq = 0

    def capture(self, feed: str, seconds: float, *, vehicles=(("V1", 42.35, -71.06),), body: bytes | None = None,
                parse_status: str = "ok") -> int:
        self.seq += 1
        at = T0 + timedelta(seconds=seconds)
        ts = int(at.timestamp()) - 5
        if body is None:
            entities = []
            if feed == "vehicle_positions":
                entities = [{"id": vid, "vehicle": {"trip": {"route_id": "R_A"}, "vehicle": {"id": vid},
                                                    "position": {"latitude": lat, "longitude": lon},
                                                    "timestamp": ts - 3}} for vid, lat, lon in vehicles]
            body = json.dumps({"header": {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET",
                                          "timestamp": ts}, "entity": entities}).encode("utf-8")
        cid = f"{feed[:2]}-{self.seq:04d}"
        self.bodies[cid] = body
        self.items.append({
            "seq": self.seq, "kind": "capture", "feed_type": feed, "at": iso_utc(at), "capture_id": cid,
            "raw_path": f"{feed}/2026-10-05/{cid}.json.gz", "raw_sha256": ids.sha256_hex(body),
            "raw_bytes": len(body), "stored_sha256": None, "parse_status": parse_status, "http_status": 200,
            "feed_timestamp": ts if parse_status == "ok" else None, "params": {},
        })
        return self.seq

    def special(self, kind: str, seconds: float | None = None, **fields) -> int:
        self.seq += 1
        item = {"seq": self.seq, "kind": kind, **fields}
        if seconds is not None:
            item["at"] = iso_utc(T0 + timedelta(seconds=seconds))
        self.items.append(item)
        return self.seq

    def timeline(self):
        return build_timeline({"source_type": "synthetic", "start_clock": iso_utc(T0), "items": self.items})


def make_publisher(tmp_path, rec: Recording, clock: FakeClock, *, producer: FakeProducer | None = None,
                   script=(), speed: float = 1.0, step_mode: bool = False, duration: float | None = None,
                   run: RunRecord | None = None):
    run = run or make_run()
    producer = producer or FakeProducer(clock)
    enveloper = FakeEnveloper(run, rec.bodies)
    pub = Publisher(
        run=run, items=rec.timeline(), start_clock=T0, session_dir=tmp_path / "session", producer=producer,
        enveloper=enveloper,
        options=PublisherOptions(speed=speed, step_mode=step_mode, duration_seconds=duration,
                                 tick_interval_seconds=1.0, heartbeat_seconds=5.0, control_poll_seconds=0.2,
                                 max_send_attempts=2, retry_backoff_seconds=0.1),
        fault_dir=tmp_path / "faults", run_dir=tmp_path / "run", session_id="sess-1", recording_id="rec-1",
        manifest_sha256="f" * 64, metrics=ComponentMetrics("publisher"), control=ScriptedControl(clock, script),
        monotonic=clock.monotonic, sleep=clock.sleep, wall_now=clock.wall,
    )
    return pub, producer, enveloper


def _clock_events(producer: FakeProducer) -> list[dict]:
    return producer.controls(*CLOCK_EVENTS)


def _assert_monotonic_clock(producer: FakeProducer) -> None:
    events = _clock_events(producer)
    seqs = [e["clock_sequence"] for e in events]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs), seqs
    clocks = [parse_iso_utc(e["source_clock"]) for e in events]
    assert all(b >= a for a, b in zip(clocks, clocks[1:], strict=False)), clocks


# --------------------------------------------------------------------------------------------------


def test_ticks_precede_and_are_flushed_before_snapshots(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.capture("vehicle_positions", 0)
    rec.capture("trip_updates", 10)
    rec.capture("vehicle_positions", 30)
    pub, producer, _ = make_publisher(tmp_path, rec, clock, speed=10.0)
    result = pub.run()

    assert result.state == "FINISHED" and result.finished_reason == "completed"
    assert result.source_clock == T0 + timedelta(seconds=30)
    assert len(producer.envelopes()) == 3
    last_clock = None
    flushed = True
    for kind, payload, _ in producer.ops:
        if kind == "control" and payload["event_type"] in CLOCK_EVENTS:
            last_clock, flushed = payload, False
        elif kind == "flush":
            flushed = True
        elif kind == "envelope":
            assert flushed, "a clock event must be flushed before the next snapshot is produced"
            assert parse_iso_utc(last_clock["source_clock"]) >= parse_iso_utc(payload["original_fetched_at"])
    item_ticks = {e["source_clock"] for e in producer.controls("clock_tick") if e.get("tick_reason") == "item"}
    assert {iso_utc(T0 + timedelta(seconds=10)), iso_utc(T0 + timedelta(seconds=30))} <= item_ticks
    started = producer.controls("run_started")[0]
    assert started["clock_sequence"] == 1 and started["source_clock"] == iso_utc(T0)
    assert producer.controls("run_finished")[0]["reason"] == "completed"
    _assert_monotonic_clock(producer)

    log = [json.loads(line) for line in (tmp_path / "session" / pc.PUBLISH_LOG_FILE).read_text().splitlines()]
    snaps = [line for line in log if line["kind"] == "snapshot"]
    assert len(snaps) == 3
    assert all(s["published_at"] and s["bytes"] > 0 and s["offset"] is not None for s in snaps)
    assert snaps[0]["candidates"] == 1
    state = pc.read_publisher_state(tmp_path / "session")
    assert state["state"] == "FINISHED" and state["next_index"] == 3 and state["items_total"] == 3


def test_periodic_ticks_advance_the_clock_while_waiting(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.capture("vehicle_positions", 0)
    rec.capture("vehicle_positions", 10)
    pub, producer, _ = make_publisher(tmp_path, rec, clock, speed=1.0)
    pub.run()
    interval = [e for e in producer.controls("clock_tick") if e.get("tick_reason") == "interval"]
    assert 8 <= len(interval) <= 10
    assert all(parse_iso_utc(e["source_clock"]) < T0 + timedelta(seconds=10) for e in interval)
    second_env_time = producer.envelopes()[1][1]
    assert second_env_time >= MONO0 + 10.0 - 1e-6
    _assert_monotonic_clock(producer)


def test_clock_never_goes_backwards_for_late_items_and_republish(tmp_path):
    clock = FakeClock()
    rec = Recording()
    first = rec.capture("vehicle_positions", 0)
    rec.capture("vehicle_positions", 30)
    rec.capture("vehicle_positions", 15)                       # late: older than the clock
    rec.special("republish", 30, params={"of_seq": first})     # re-delivery of the first snapshot
    pub, producer, _ = make_publisher(tmp_path, rec, clock, speed=50.0)
    result = pub.run()
    envs = [e for e, _ in producer.envelopes()]
    assert len(envs) == 4
    assert envs[3]["snapshot_id"] == envs[0]["snapshot_id"]
    assert envs[3]["capture_id"] == envs[0]["capture_id"]
    assert result.source_clock == T0 + timedelta(seconds=30)
    assert all(parse_iso_utc(e["source_clock"]) <= T0 + timedelta(seconds=30) for e in _clock_events(producer))
    _assert_monotonic_clock(producer)


def test_pause_freezes_source_clock_while_heartbeats_continue(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.capture("vehicle_positions", 0)
    rec.capture("vehicle_positions", 60)
    pub, producer, _ = make_publisher(tmp_path, rec, clock, script=[(5, "pause", {}), (25, "resume", {})])
    result = pub.run()

    controls = producer.controls()
    i_pause = next(i for i, e in enumerate(controls) if e["event_type"] == "run_paused")
    i_resume = next(i for i, e in enumerate(controls) if e["event_type"] == "run_resumed")
    paused_clock = parse_iso_utc(controls[i_pause]["source_clock"])
    assert T0 + timedelta(seconds=4) <= paused_clock <= T0 + timedelta(seconds=6)
    assert controls[i_pause]["run_state"] == "PAUSED"
    between = controls[i_pause + 1:i_resume]
    assert not [e for e in between if e["event_type"] == "clock_tick"]
    beats = [e for e in between if e["event_type"] == "publisher_heartbeat"]
    assert len(beats) >= 3
    assert all(parse_iso_utc(b["source_clock"]) == paused_clock for b in beats)
    walls = [parse_iso_utc(b["wall_heartbeat_at"]) for b in beats]
    assert walls == sorted(walls) and walls[-1] > walls[0]
    assert all(b["run_state"] == "PAUSED" for b in beats)
    assert parse_iso_utc(controls[i_resume]["source_clock"]) == paused_clock
    after = [e for e in controls[i_resume:] if e["event_type"] == "clock_tick"]
    assert after and parse_iso_utc(after[-1]["source_clock"]) == T0 + timedelta(seconds=60)
    assert producer.envelopes()[1][1] >= MONO0 + 60 + 19              # the pause shifted the schedule
    assert result.source_clock == T0 + timedelta(seconds=60)
    _assert_monotonic_clock(producer)


def test_step_mode_stops_at_barriers(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.special("tick", 0)
    rec.capture("vehicle_positions", 0)
    rec.special("barrier", params={"label": "S1"})
    rec.special("tick", 30)
    rec.capture("vehicle_positions", 30)
    rec.special("barrier", params={"label": "S2"})
    pub, producer, _ = make_publisher(tmp_path, rec, clock, step_mode=True,
                                      script=[(1, "step", {}), (10, "step", {}), (20, "step", {})])
    result = pub.run()
    envs = producer.envelopes()
    assert len(envs) == 2
    assert MONO0 + 1 <= envs[0][1] < MONO0 + 2          # nothing before the first step
    assert MONO0 + 10 <= envs[1][1] < MONO0 + 11        # stopped at barrier S1 until the second step
    ticks = producer.ops
    t30 = [t for k, p, t in ticks if k == "control" and p["event_type"] == "clock_tick"
           and p["source_clock"] == iso_utc(T0 + timedelta(seconds=30))]
    assert t30 and t30[0] >= MONO0 + 10                  # clock frozen while waiting at the barrier
    assert not [e for e in producer.controls("clock_tick") if e.get("tick_reason") == "interval"]
    assert result.finished_reason == "completed"
    state = pc.read_publisher_state(tmp_path / "session")
    assert state["state"] == "FINISHED" and state["last_barrier"] == "S2"
    finished_at = [t for k, p, t in producer.ops if k == "control" and p["event_type"] == "run_finished"][0]
    assert finished_at >= MONO0 + 20                      # the run waited after the last barrier


def test_restart_continues_clock_sequence_and_resumes_at_next_item(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.capture("vehicle_positions", 0)
    rec.capture("vehicle_positions", 30)
    rec.capture("vehicle_positions", 60)
    pub1, prod1, _ = make_publisher(tmp_path, rec, clock, producer=FakeProducer(clock, fail_on_envelope=2),
                                    speed=100.0)
    with pytest.raises(RuntimeError):
        pub1.run()
    failed = pc.read_publisher_state(tmp_path / "session")
    assert failed["state"] == "FAILED" and failed["next_index"] == 1
    max_seq1 = max(e["clock_sequence"] for e in _clock_events(prod1))
    assert failed["clock_sequence"] >= max_seq1

    pub2, prod2, _ = make_publisher(tmp_path, rec, clock, speed=100.0)
    result = pub2.run()
    assert result.restarted
    started = prod2.controls("run_started")[0]
    assert started["clock_sequence"] > max_seq1
    assert started["restarted"] is True and started["resume_index"] == 1
    envs = [e for e, _ in prod2.envelopes()]
    assert [e["original_fetched_at"] for e in envs] == [iso_utc(T0 + timedelta(seconds=30)),
                                                       iso_utc(T0 + timedelta(seconds=60))]
    _assert_monotonic_clock(prod2)
    run_clock = json.loads((tmp_path / "run" / "publisher_clock.json").read_text())
    assert run_clock["clock_sequence"] == result.clock_sequence

    pub3, prod3, _ = make_publisher(tmp_path, rec, clock, speed=100.0)
    again = pub3.run()
    assert again.already_finished and not prod3.ops


def test_fault_arm_malformed_envelope_and_replayed_rejections(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.special("arm_fault", 0, params={"point": "after_ready_before_cas",
                                        "match": {"feed_type": "vehicle_positions", "snapshot_ordinal": 1}})
    vp = rec.capture("vehicle_positions", 0)
    rec.special("malformed_envelope", 0, params={"feed_type": "vehicle_positions", "value_text": "{not json"})
    rec.special("fetch_event", 5, feed_type="trip_updates",
                params={"outcome": "timeout", "http_status": None, "reason_code": "SOURCE_FETCH_FAILED"})
    rec.capture("alerts", 10, body=b"{broken", parse_status="INVALID_JSON")
    rec.capture("trip_updates", 15)
    pub, producer, _ = make_publisher(tmp_path, rec, clock, speed=100.0)
    pub.run()

    envs = [e for e, _ in producer.envelopes()]
    assert [e["feed_type"] for e in envs] == ["vehicle_positions", "trip_updates"]
    arm = json.loads((tmp_path / "faults" / "after_ready_before_cas.json").read_text())
    assert arm["point"] == "after_ready_before_cas" and arm["run_id"] == "run-test-0001"
    assert arm["match"]["snapshot_id"] == envs[0]["snapshot_id"]
    assert arm["match"]["capture_id"] == envs[0]["capture_id"] and arm["match"]["timeline_seq"] == vp
    assert producer.controls("fault_armed")[0]["fault_id"] == arm["fault_id"]

    raws = [p for k, p, _ in producer.ops if k == "raw"]
    assert raws == [{"topic": VP_TOPIC, "key": b"run-test-0001|vehicle_positions", "value": b"{not json"}]
    dlqs = [p for k, p, _ in producer.ops if k == "dlq"]
    assert len(dlqs) == 1 and dlqs[0]["reason_code"] == "INVALID_JSON" and dlqs[0]["feed_type"] == "alerts"
    assert dlqs[0]["excerpt"] == "{broken" and dlqs[0]["raw_path"].startswith("alerts/")

    statuses = producer.controls("collector_status")
    timeout = [s for s in statuses if s["outcome"] == "timeout"]
    assert timeout and timeout[0]["feed_type"] == "trip_updates" and timeout[0]["replayed"] is True
    assert timeout[0]["historical_at"] == iso_utc(T0 + timedelta(seconds=5))
    assert timeout[0]["feed_state"] == "degraded"
    rejected = [s for s in statuses if s["outcome"] == "rejected"]
    assert rejected and rejected[0]["reason_code"] == "INVALID_JSON"
    recovered = [s for s in statuses if s["outcome"] == "ok" and s["feed_type"] == "trip_updates"]
    assert recovered and recovered[0]["feed_state"] == "healthy"


def test_inject_duplicate_late_and_invalid_gps(tmp_path):
    clock = FakeClock()
    rec = Recording()
    two = (("V1", 42.35, -71.06), ("V2", 42.36, -71.05))
    rec.capture("vehicle_positions", 0, vehicles=two)
    rec.capture("vehicle_positions", 30, vehicles=two)
    rec.capture("vehicle_positions", 300, vehicles=two)
    script = [(40, "inject", {"scenario": "duplicate_snapshot"}), (41, "inject", {"scenario": "late_snapshot"}),
              (42, "inject", {"scenario": "invalid_gps"}), (43, "finish", {})]
    pub, producer, enveloper = make_publisher(tmp_path, rec, clock, script=script)
    result = pub.run()

    envs = [e for e, _ in producer.envelopes()]
    assert len(envs) == 5
    assert envs[2]["snapshot_id"] == envs[1]["snapshot_id"]          # duplicate of the latest
    assert envs[3]["snapshot_id"] == envs[0]["snapshot_id"]          # older snapshot re-delivered (late)
    injected = envs[4]
    assert injected["snapshot_id"] not in {e["snapshot_id"] for e in envs[:4]}
    assert injected["entities"][0]["vehicle"]["position"]["latitude"] == 95.0
    assert injected["feed_timestamp"] > envs[1]["feed_timestamp"]
    assert len(enveloper.injected) == 1
    (_, meta), = enveloper.injected.values()
    assert meta["injected"] is True and meta["scenario"] == "invalid_gps"
    lines = (tmp_path / "session" / pc.INJECTIONS_FILE).read_text().splitlines()
    assert [json.loads(x)["kind"] for x in lines] == ["inject:duplicate_snapshot", "inject:late_snapshot",
                                                      "inject:invalid_gps"]
    assert result.finished_reason == "finish_requested"
    state = pc.read_publisher_state(tmp_path / "session")
    assert state["last_command"]["command"] == "finish" and state["last_command"]["ok"] is True
    _assert_monotonic_clock(producer)


def test_advance_clock_and_command_rejections(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.capture("vehicle_positions", 0)
    rec.special("barrier", params={"label": "S1"})
    target = T0 + timedelta(seconds=190)
    script = [(1, "advance-clock", {"to": iso_utc(target)}),
              (2, "advance-clock", {"to": iso_utc(T0 + timedelta(seconds=10))})]
    pub, producer, _ = make_publisher(tmp_path, rec, clock, step_mode=True, duration=5, script=script)
    result = pub.run()
    advanced = [e for e in producer.controls("clock_tick") if e.get("tick_reason") == "advance_clock"]
    assert advanced and advanced[0]["source_clock"] == iso_utc(target)
    assert result.source_clock == target and result.finished_reason == "duration_budget"
    state = pc.read_publisher_state(tmp_path / "session")
    assert state["last_command"]["ok"] is False
    assert state["last_command"]["error"]["code"] == "clock_regression"
    assert not producer.envelopes()                      # step mode: nothing published without a step

    running = Recording()                                # still waiting for its 2nd item when commands arrive
    running.capture("vehicle_positions", 0)
    running.capture("vehicle_positions", 60)
    clock2 = FakeClock()
    pub2, _, _ = make_publisher(tmp_path / "b", running, clock2, duration=3, script=[(1, "step", {})])
    pub2.run()
    state2 = pc.read_publisher_state(tmp_path / "b" / "session")
    assert state2["last_command"]["error"]["code"] == "unsupported_mode"

    clock3 = FakeClock()
    pub3, _, _ = make_publisher(tmp_path / "c", running, clock3, duration=3, run=make_run("replay"),
                                script=[(1, "inject", {"scenario": "invalid_gps"})])
    pub3.run()
    state3 = pc.read_publisher_state(tmp_path / "c" / "session")
    assert state3["last_command"]["error"]["code"] == "unsupported_mode"


def test_unacknowledged_clock_tick_fails_before_the_next_snapshot(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.capture("vehicle_positions", 0)
    rec.capture("vehicle_positions", 30)
    failing = FakeProducer(clock, fail_control_type="clock_tick")
    pub, producer, _ = make_publisher(tmp_path, rec, clock, producer=failing, speed=100.0)
    with pytest.raises(PublisherError) as info:
        pub.run()
    assert info.value.code == "kafka_delivery_failed"
    assert len(producer.envelopes()) == 1                 # the snapshot after the failed tick was never produced
    ticks = producer.controls("clock_tick")
    assert len(ticks) == 2                                # max_send_attempts=2: the same tick re-sent once
    assert ticks[0]["clock_sequence"] == ticks[1]["clock_sequence"]
    state = pc.read_publisher_state(tmp_path / "session")
    assert state["state"] == "FAILED" and state["next_index"] == 1


def _snapshot_log(tmp_path) -> list[dict]:
    lines = (tmp_path / "session" / pc.PUBLISH_LOG_FILE).read_text(encoding="utf-8").splitlines()
    return [json.loads(x) for x in lines if json.loads(x)["kind"] == "snapshot"]


def test_late_error_of_an_earlier_copy_does_not_veto_an_acked_retry(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.capture("vehicle_positions", 0)
    producer = FakeProducer(clock, envelope_reports=["hold_error", "ack"])   # copy 1 times out, fails late
    pub, producer, _ = make_publisher(tmp_path, rec, clock, producer=producer)
    result = pub.run()
    assert result.state == "FINISHED"
    envs = [e for e, _ in producer.envelopes()]
    assert len(envs) == 2 and envs[0]["snapshot_id"] == envs[1]["snapshot_id"]
    assert _snapshot_log(tmp_path)[0]["attempts"] == 2


def test_ack_of_an_earlier_copy_counts_even_if_the_retry_fails(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.capture("vehicle_positions", 0)
    producer = FakeProducer(clock, envelope_reports=["hold_ack", "error"])   # copy 1 ACKed late, copy 2 fails
    pub, producer, _ = make_publisher(tmp_path, rec, clock, producer=producer)
    result = pub.run()
    assert result.state == "FINISHED" and result.finished_reason == "completed"
    assert len(_snapshot_log(tmp_path)) == 1


def test_local_send_errors_are_retried_then_fail(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.capture("vehicle_positions", 0)
    retried = FakeProducer(clock, envelope_exceptions=[BufferError("Local: Queue full")])
    pub, producer, _ = make_publisher(tmp_path / "a", rec, clock, producer=retried)
    assert pub.run().state == "FINISHED"
    assert len(producer.envelopes()) == 1 and _snapshot_log(tmp_path / "a")[0]["attempts"] == 2

    exhausted = FakeProducer(clock, envelope_exceptions=[BufferError("Local: Queue full")] * 5)
    pub2, _, _ = make_publisher(tmp_path / "b", rec, clock, producer=exhausted)
    with pytest.raises(PublisherError) as info:
        pub2.run()
    assert info.value.code == "kafka_delivery_failed" and "BufferError" in info.value.message


def test_non_retryable_send_errors_propagate_unchanged(tmp_path):
    class PayloadTooLarge(Exception):          # stands in for ingestion.kafka_producer.PayloadTooLarge
        pass

    clock = FakeClock()
    rec = Recording()
    rec.capture("vehicle_positions", 0)
    producer = FakeProducer(clock, envelope_exceptions=[PayloadTooLarge("5000000 > 4194304")])
    pub, producer, _ = make_publisher(tmp_path, rec, clock, producer=producer)
    with pytest.raises(PayloadTooLarge):
        pub.run()
    assert producer.envelope_calls == 1                     # no retry
    assert pc.read_publisher_state(tmp_path / "session")["state"] == "FAILED"


def test_fault_target_is_the_next_capture_published_after_reparse(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.special("arm_fault", 0, params={"point": "after_partial_write",
                                        "match": {"feed_type": "vehicle_positions", "snapshot_ordinal": 1}})
    rec.capture("vehicle_positions", 0, body=b"{broken")                    # recorded ok, rejected on re-parse
    first = rec.capture("vehicle_positions", 10, parse_status="INVALID_HEADER")   # recorded rejected, valid raw
    rec.capture("vehicle_positions", 20)
    pub, producer, _ = make_publisher(tmp_path, rec, clock, speed=100.0)
    pub.run()
    envs = [e for e, _ in producer.envelopes()]
    assert len(envs) == 2
    arm = json.loads((tmp_path / "faults" / "after_partial_write.json").read_text(encoding="utf-8"))
    assert arm["match"]["timeline_seq"] == first
    assert arm["match"]["snapshot_id"] == envs[0]["snapshot_id"]


def test_restart_history_uses_persisted_publish_outcomes(tmp_path):
    clock = FakeClock()
    rec = Recording()
    a = rec.capture("vehicle_positions", 0, parse_status="INVALID_HEADER")  # published after re-parse
    rec.capture("vehicle_positions", 10, body=b"{broken")                   # recorded ok, never published
    c = rec.capture("vehicle_positions", 20)
    rec.capture("vehicle_positions", 300)                                   # 3rd envelope call fails
    pub1, _, _ = make_publisher(tmp_path, rec, clock, producer=FakeProducer(clock, fail_on_envelope=3),
                                speed=100.0)
    with pytest.raises(RuntimeError):
        pub1.run()
    state = pc.read_publisher_state(tmp_path / "session")
    assert state["published_capture_seqs"] == [a, c] and state["next_index"] == 3

    pub2, _, _ = make_publisher(tmp_path, rec, clock, speed=100.0)
    pub2._restore()
    assert [r.seq for r in pub2._published("vehicle_positions")] == [a, c]


def test_recorded_rejections_are_reparsed_when_raw_is_readable(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.capture("vehicle_positions", 0, parse_status="INVALID_HEADER")      # raw is valid for today's parser
    rec.capture("alerts", 5, body=b"{broken", parse_status="INVALID_JSON")
    gone = rec.capture("trip_updates", 10, body=b"{}", parse_status="UNSUPPORTED_SCHEMA")
    del rec.bodies[f"tr-{gone:04d}"]                                       # raw file missing/unreadable
    pub, producer, _ = make_publisher(tmp_path, rec, clock, speed=100.0)
    pub.run()
    envs = [e for e, _ in producer.envelopes()]
    assert [e["feed_type"] for e in envs] == ["vehicle_positions"]
    dlqs = [p for k, p, _ in producer.ops if k == "dlq"]
    assert [(d["feed_type"], d["reason_code"]) for d in dlqs] == [("alerts", "INVALID_JSON"),
                                                                ("trip_updates", "UNSUPPORTED_SCHEMA")]
    assert dlqs[0]["excerpt"] == "{broken" and dlqs[1]["excerpt"] is None
    rejected = [s for s in producer.controls("collector_status") if s["outcome"] == "rejected"]
    assert [s["feed_type"] for s in rejected] == ["alerts", "trip_updates"]


def test_live_runs_are_refused(tmp_path):
    clock = FakeClock()
    rec = Recording()
    rec.capture("vehicle_positions", 0)
    with pytest.raises(PublisherError) as info:
        make_publisher(tmp_path, rec, clock, run=make_run("live"))
    assert info.value.exit_code == 6


def test_options_validate_speed():
    with pytest.raises(ValueError):
        PublisherOptions(speed=0)
    with pytest.raises(ValueError):
        PublisherOptions(speed=101, max_speed=100)
    assert PublisherOptions(speed=50).speed == 50
