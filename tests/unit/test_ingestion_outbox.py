"""Unit tests for the SQLite outbox and its dispatcher (no Kafka: a fake producer records sends)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from transitpulse.contracts.envelope import build_envelope, envelope_size
from transitpulse.ingestion.kafka_producer import PayloadTooLarge, size_error_codes
from transitpulse.ingestion.outbox import DlqMessage, Outbox, OutboxDispatcher, OutboxLocked, OutboxRow

RUN = "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d"
TOPIC = "mbta.vehicle_positions.snapshots.v1"
KEY = f"{RUN}|vehicle_positions"
PUBLISHED = datetime(2026, 10, 5, 12, 0, 35, tzinfo=UTC)


def make_env(n: int) -> Any:
    cid = f"vp-20261005T1200{n:02d}000000Z-{n:012x}"
    return build_envelope(
        run_id=RUN, scope_id="scope0001", source_type="live", scenario_id=None, feed_type="vehicle_positions",
        snapshot_id=f"sn_{n:032x}", capture_id=cid, source_url="https://cdn.mbta.com/realtime/VehiclePositions.json",
        gtfs_version="g" * 64, header={"gtfs_realtime_version": "2.0", "timestamp": 1791201600 + n},
        entities=[{"id": f"v{n}", "vehicle": {"vehicle": {"id": str(n)}}}],
        original_fetched_at=datetime(2026, 10, 5, 12, 0, n, tzinfo=UTC), raw_sha256=f"{n:064x}",
        raw_path=f"{RUN}/vehicle_positions/2026-10-05/{cid}.json.gz", raw_bytes=100, http=None,
        source_entity_count=1, scope_accounting={"selected": 1}, producer_component="collector",
        producer_version="0.1.0",
    )


class FakeErr:
    def __init__(self, code: int = -192) -> None:
        self._code = code

    def code(self) -> int:
        return self._code

    def fatal(self) -> bool:
        return False

    def __str__(self) -> str:
        return f"Local: Message timed out ({self._code})"


class FakeProducer:
    """Records sends; delivery reports are fired by the test (never automatically)."""

    def __init__(self) -> None:
        self.generation = 0
        self.sent: list[tuple[str, dict[str, Any], Any, Any]] = []
        self.raise_next: Exception | None = None

    def send_envelope(self, env: dict[str, Any], *, on_ack: Any = None, on_error: Any = None) -> None:
        if self.raise_next is not None:
            exc, self.raise_next = self.raise_next, None
            raise exc
        self.sent.append(("snapshot", env, on_ack, on_error))

    def send_dlq(self, record: dict[str, Any], *, on_ack: Any = None, on_error: Any = None) -> None:
        self.sent.append(("dlq", record, on_ack, on_error))

    def poll(self, timeout: float = 0.0) -> int:
        return 0

    def flush(self, timeout: float = 30.0) -> int:
        return 0


def make_dlq(row: OutboxRow, reason: str, detail: str) -> DlqMessage:
    return DlqMessage(record={"capture_id": row.capture_id, "run_id": row.run_id, "feed_type": row.feed_type,
                              "reason_code": reason, "detail": detail},
                      topic="mbta.dlq.v1", kafka_key=f"{row.run_id}|{row.feed_type}")


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture()
def db(tmp_path: Path) -> Path:
    return tmp_path / "state" / "outbox" / f"{RUN}.sqlite"


def test_wal_full_sync_and_schema(db: Path) -> None:
    with Outbox(db, run_id=RUN) as ob:
        assert ob._conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert ob._conn.execute("PRAGMA synchronous").fetchone()[0] == 2     # FULL
        assert ob.status()["schema_version"] == 1
        assert ob.counts()["snapshot"] == {"pending": 0, "published": 0, "rejected": 0}


def test_one_collector_per_run_lock(db: Path) -> None:
    first = Outbox(db, run_id=RUN)
    try:
        assert Outbox.is_locked(first.lock_path)
        with pytest.raises(OutboxLocked):
            Outbox(db, run_id=RUN)
        reader = Outbox(db, run_id=RUN, lock=False, create=False)   # status readers never lock
        reader.close()
    finally:
        first.close()
    assert not Outbox.is_locked(first.lock_path)
    Outbox(db, run_id=RUN).close()


def test_missing_database_is_not_created_in_read_mode(db: Path) -> None:
    with pytest.raises(FileNotFoundError):
        Outbox(db, run_id=RUN, lock=False, create=False)
    assert not db.exists()


def test_insert_is_idempotent_and_payload_has_no_published_at(db: Path) -> None:
    env = make_env(1)
    with Outbox(db, run_id=RUN) as ob:
        assert ob.insert_snapshot(env, topic=TOPIC, kafka_key=KEY) is True
        assert ob.insert_snapshot(env, topic=TOPIC, kafka_key=KEY) is False
        row = ob.get(env["capture_id"])
        assert row is not None and row.state == "pending" and row.seq == 1
        assert row.payload_bytes == envelope_size(env)
        payload = ob.load_payload(env["capture_id"])
        assert payload == dict(env) and "published_at" not in payload


def test_pending_becomes_published_only_in_the_ack_callback(db: Path) -> None:
    clock = Clock()
    prod = FakeProducer()
    env = make_env(1)
    with Outbox(db, run_id=RUN) as ob:
        ob.insert_snapshot(env, topic=TOPIC, kafka_key=KEY)
        disp = OutboxDispatcher(ob, prod, make_dlq=make_dlq, monotonic=clock)
        assert disp.step(poll_timeout=0) == 1
        row = ob.get(env["capture_id"])
        assert row is not None and row.state == "pending" and row.attempts == 1   # produced, not acknowledged
        assert disp.step(poll_timeout=0) == 0                                      # still in flight: no resend
        kind, payload, on_ack, on_error = prod.sent[0]
        assert kind == "snapshot" and payload["snapshot_id"] == env["snapshot_id"]

        on_error(FakeErr())                                                        # failed delivery report
        row = ob.get(env["capture_id"])
        assert row is not None and row.state == "pending" and row.last_error is not None
        assert disp.step(poll_timeout=0) == 0                                      # backing off
        clock.t += 1.6
        assert disp.step(poll_timeout=0) == 1                                      # resent with the same ids
        _, payload2, on_ack2, _ = prod.sent[1]
        assert payload2 == payload
        on_ack2(TOPIC, 0, 42, PUBLISHED)
        row = ob.get(env["capture_id"])
        assert row is not None and row.state == "published"
        assert (row.kafka_partition, row.kafka_offset) == (0, 42) and row.attempts == 2
        assert row.acked_published_at == "2026-10-05T12:00:35.000Z" and row.last_error is None
        assert ob.load_payload(env["capture_id"]) is None                          # payload dropped after ACK
        on_ack(TOPIC, 0, 41, PUBLISHED)                                            # late duplicate ACK is harmless
        assert ob.get(env["capture_id"]).kafka_offset == 42                        # type: ignore[union-attr]


def test_pending_rows_resume_after_restart_with_the_same_ids(db: Path) -> None:
    envs = [make_env(i) for i in (1, 2, 3)]
    with Outbox(db, run_id=RUN) as ob:
        for env in envs:
            ob.insert_snapshot(env, topic=TOPIC, kafka_key=KEY)
        prod = FakeProducer()
        OutboxDispatcher(ob, prod, make_dlq=make_dlq).step(poll_timeout=0)
        prod.sent[0][2](TOPIC, 0, 1, PUBLISHED)                                    # only the first is acked
    # "crash": the process is gone with two unacknowledged rows
    with Outbox(db, run_id=RUN) as ob:
        assert [r.capture_id for r in ob.pending_rows(10)] == [envs[1]["capture_id"], envs[2]["capture_id"]]
        prod = FakeProducer()
        OutboxDispatcher(ob, prod, make_dlq=make_dlq).step(poll_timeout=0)
        assert [p["capture_id"] for _, p, _, _ in prod.sent] == [envs[1]["capture_id"], envs[2]["capture_id"]]
        assert [p["snapshot_id"] for _, p, _, _ in prod.sent] == [envs[1]["snapshot_id"], envs[2]["snapshot_id"]]
        assert prod.sent[0][1] == dict(envs[1])


def test_max_in_flight_is_respected(db: Path) -> None:
    with Outbox(db, run_id=RUN) as ob:
        for i in range(1, 6):
            ob.insert_snapshot(make_env(i), topic=TOPIC, kafka_key=KEY)
        prod = FakeProducer()
        disp = OutboxDispatcher(ob, prod, make_dlq=make_dlq, max_in_flight=2)
        assert disp.step(poll_timeout=0) == 2 and disp.in_flight() == 2
        prod.sent[0][2](TOPIC, 0, 1, PUBLISHED)
        assert disp.step(poll_timeout=0) == 1
        assert [p["capture_id"] for _, p, _, _ in prod.sent] == [make_env(i)["capture_id"] for i in (1, 2, 3)]


def test_rejected_rows_are_never_sent_but_their_dlq_row_is(db: Path) -> None:
    env = make_env(1)
    dlq = DlqMessage(record={"capture_id": env["capture_id"], "reason_code": "INVALID_JSON", "run_id": RUN,
                             "feed_type": "vehicle_positions"}, topic="mbta.dlq.v1", kafka_key=KEY)
    with Outbox(db, run_id=RUN) as ob:
        assert ob.insert_rejected(capture_id=env["capture_id"], feed_type="vehicle_positions", reason="INVALID_JSON",
                                  topic=TOPIC, kafka_key=KEY, raw_path=env["raw_path"], raw_sha256=env["raw_sha256"],
                                  original_fetched_at=env["original_fetched_at"], dlq=dlq)
        row = ob.get(env["capture_id"])
        assert row is not None and row.state == "rejected" and row.reason == "INVALID_JSON"
        assert ob.load_payload(env["capture_id"]) is None
        prod = FakeProducer()
        disp = OutboxDispatcher(ob, prod, make_dlq=make_dlq)
        assert disp.step(poll_timeout=0) == 1
        assert [k for k, *_ in prod.sent] == ["dlq"]
        prod.sent[0][2]("mbta.dlq.v1", 0, 5, PUBLISHED)
        counts = ob.counts()
        assert counts["snapshot"]["rejected"] == 1 and counts["dlq"]["published"] == 1
        assert ob.status()["rejected_by_reason"] == {"INVALID_JSON": 1}
        assert ob.reject_pending(env["capture_id"], "PAYLOAD_TOO_LARGE") is False   # not pending any more


def test_send_time_size_check_rejects_with_durable_dlq(db: Path) -> None:
    env = make_env(1)
    with Outbox(db, run_id=RUN) as ob:
        ob.insert_snapshot(env, topic=TOPIC, kafka_key=KEY)
        prod = FakeProducer()
        prod.raise_next = PayloadTooLarge(5_000_000, 4_194_304)
        disp = OutboxDispatcher(ob, prod, make_dlq=make_dlq)
        disp.step(poll_timeout=0)
        row = ob.get(env["capture_id"])
        assert row is not None and row.state == "rejected" and row.reason == "PAYLOAD_TOO_LARGE"
        dlq_row = ob.get(f"dlq:{env['capture_id']}")
        assert dlq_row is not None and dlq_row.state == "pending" and dlq_row.kind == "dlq"
        assert dlq_row.raw_path == env["raw_path"] and dlq_row.snapshot_id == env["snapshot_id"]


def test_broker_size_error_rejects_the_row(db: Path) -> None:
    env = make_env(1)
    with Outbox(db, run_id=RUN) as ob:
        ob.insert_snapshot(env, topic=TOPIC, kafka_key=KEY)
        prod = FakeProducer()
        disp = OutboxDispatcher(ob, prod, make_dlq=make_dlq)
        disp.step(poll_timeout=0)
        prod.sent[0][3](FakeErr(sorted(size_error_codes())[0]))
        row = ob.get(env["capture_id"])
        assert row is not None and row.state == "rejected" and row.reason == "PAYLOAD_TOO_LARGE"


def test_producer_recreation_makes_in_flight_rows_eligible_again(db: Path) -> None:
    env = make_env(1)
    with Outbox(db, run_id=RUN) as ob:
        ob.insert_snapshot(env, topic=TOPIC, kafka_key=KEY)
        prod = FakeProducer()
        disp = OutboxDispatcher(ob, prod, make_dlq=make_dlq)
        assert disp.step(poll_timeout=0) == 1
        assert disp.step(poll_timeout=0) == 0
        prod.generation = 1
        assert disp.step(poll_timeout=0) == 1
        assert [p["capture_id"] for _, p, _, _ in prod.sent] == [env["capture_id"]] * 2


def test_in_flight_timeout_allows_resend(db: Path) -> None:
    clock = Clock()
    env = make_env(1)
    with Outbox(db, run_id=RUN) as ob:
        ob.insert_snapshot(env, topic=TOPIC, kafka_key=KEY)
        prod = FakeProducer()
        disp = OutboxDispatcher(ob, prod, make_dlq=make_dlq, in_flight_timeout_seconds=90, monotonic=clock)
        assert disp.step(poll_timeout=0) == 1
        clock.t += 91
        assert disp.step(poll_timeout=0) == 1


def test_keep_published_payload_option(db: Path) -> None:
    env = make_env(1)
    with Outbox(db, run_id=RUN, keep_published_payload=True) as ob:
        ob.insert_snapshot(env, topic=TOPIC, kafka_key=KEY)
        assert ob.mark_published(env["capture_id"], published_at=PUBLISHED, partition=0, offset=1)
        blob = ob._conn.execute("SELECT payload FROM outbox WHERE message_id = ?", (env["capture_id"],)).fetchone()[0]
        assert blob is not None
