"""Unit tests for EnvelopeProducer framing and delivery semantics with a fake confluent producer.

No broker is involved: the fake records produce() calls and lets the test fire delivery reports.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException

from transitpulse.config import load_config
from transitpulse.contracts.envelope import build_envelope, iso_utc, parse_iso_utc
from transitpulse.ingestion.kafka_producer import EnvelopeProducer, PayloadTooLarge, size_error_codes

RUN = "7d3e2f1a-0b9c-4d8e-a7f6-5e4d3c2b1a09"


class FakeMsg:
    def __init__(self, topic: str, partition: int, offset: int) -> None:
        self._t, self._p, self._o = topic, partition, offset

    def topic(self) -> str:
        return self._t

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o


class FakeErr:
    def __init__(self, code: int = -192, fatal: bool = False) -> None:
        self._code, self._fatal = code, fatal

    def code(self) -> int:
        return self._code

    def fatal(self) -> bool:
        return self._fatal

    def __str__(self) -> str:
        return f"FakeErr({self._code})"


class FakeConfluent:
    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf
        self.produced: list[dict[str, Any]] = []
        self.purged = False
        self.raise_on_produce: Exception | None = None

    def produce(self, topic: str, value: bytes | None = None, key: bytes | None = None,
                headers: list[tuple[str, bytes]] | None = None, on_delivery: Any = None) -> None:
        if self.raise_on_produce is not None:
            raise self.raise_on_produce
        self.produced.append({"topic": topic, "value": value, "key": key, "headers": headers,
                              "on_delivery": on_delivery})

    def poll(self, timeout: float = 0.0) -> int:
        return 0

    def flush(self, timeout: float = 0.0) -> int:
        return 0

    def purge(self, in_queue: bool = True, in_flight: bool = True, blocking: bool = True) -> None:
        self.purged = True

    def __len__(self) -> int:
        return 0

    def deliver(self, i: int, err: Any = None, offset: int = 7) -> None:
        rec = self.produced[i]
        rec["on_delivery"](err, FakeMsg(rec["topic"], 0, offset))


@pytest.fixture()
def setup(tmp_path: Path) -> tuple[EnvelopeProducer, list[FakeConfluent], Any]:
    cfg = load_config("local-native", workdir=tmp_path, environ={})
    instances: list[FakeConfluent] = []

    def factory(conf: dict[str, Any]) -> FakeConfluent:
        inst = FakeConfluent(conf)
        instances.append(inst)
        return inst

    return EnvelopeProducer(cfg, "collector", RUN, producer_factory=factory), instances, cfg


def _env(entities: list[dict[str, Any]] | None = None) -> Any:
    return build_envelope(
        run_id=RUN, scope_id="scope0001", source_type="live", scenario_id=None, feed_type="vehicle_positions",
        snapshot_id="sn_" + "1" * 32, capture_id="vp-20261005T120031000000Z-abababababab",
        source_url="https://cdn.mbta.com/realtime/VehiclePositions.json", gtfs_version="g" * 64,
        header={"gtfs_realtime_version": "2.0", "timestamp": 1791201630},
        entities=entities if entities is not None else [{"id": "v1"}],
        original_fetched_at=datetime(2026, 10, 5, 12, 0, 31, tzinfo=UTC), raw_sha256="ab" * 32,
        raw_path=f"{RUN}/vehicle_positions/2026-10-05/vp-20261005T120031000000Z-abababababab.json.gz", raw_bytes=10,
        http=None, source_entity_count=1, scope_accounting={}, producer_component="collector", producer_version="0.1.0",
    )


def test_producer_configuration(setup: Any) -> None:
    _prod, instances, cfg = setup
    conf = instances[0].conf
    assert conf["bootstrap.servers"] == cfg.kafka.bootstrap_servers
    assert conf["enable.idempotence"] is True and conf["acks"] == "all"
    assert conf["compression.type"] == "none"
    assert conf["message.max.bytes"] == cfg.kafka.producer.message_max_bytes
    assert conf["message.max.bytes"] > cfg.kafka.max_snapshot_bytes
    assert conf["client.id"] == f"tp-collector-{RUN[:8]}"
    assert callable(conf["error_cb"])


def test_send_envelope_framing_and_ack_only_on_success(setup: Any) -> None:
    prod, instances, cfg = setup
    acks: list[tuple[Any, ...]] = []
    errors: list[Any] = []
    env = _env()
    prod.send_envelope(env, on_ack=lambda *a: acks.append(a), on_error=errors.append)
    rec = instances[0].produced[0]
    assert rec["topic"] == cfg.kafka.topics.vehicle_positions
    assert rec["key"] == f"{RUN}|vehicle_positions".encode()
    assert dict(rec["headers"])["snapshot_id"] == env["snapshot_id"].encode()
    body = json.loads(rec["value"])
    published_at = body.pop("published_at")
    assert body == json.loads(json.dumps(dict(env)))
    assert "published_at" not in env
    instances[0].deliver(0, err=FakeErr())
    assert acks == [] and len(errors) == 1
    instances[0].deliver(0, err=None, offset=42)
    assert len(acks) == 1
    topic, partition, offset, acked_at = acks[0]
    assert (topic, partition, offset) == (cfg.kafka.topics.vehicle_positions, 0, 42)
    assert acked_at.tzinfo is not None
    assert iso_utc(acked_at) == published_at       # on_ack receives the published_at that was sent
    assert parse_iso_utc(published_at) <= acked_at


def test_callback_exceptions_do_not_escape(setup: Any) -> None:
    prod, instances, _cfg = setup

    def bad(*args: Any) -> None:
        raise RuntimeError("callback bug")

    prod.send_envelope(_env(), on_ack=bad)
    instances[0].deliver(0)          # must not raise


def test_oversize_envelope_is_rejected_before_produce(tmp_path: Path) -> None:
    cfg = load_config("local-native", workdir=tmp_path, environ={}, overrides={"kafka": {"max_snapshot_bytes": 600}})
    instances: list[FakeConfluent] = []

    def factory(conf: dict[str, Any]) -> FakeConfluent:
        instances.append(FakeConfluent(conf))
        return instances[-1]

    prod = EnvelopeProducer(cfg, "collector", RUN, producer_factory=factory)
    with pytest.raises(PayloadTooLarge) as info:
        prod.send_envelope(_env([{"id": f"v{i}", "pad": "x" * 50} for i in range(20)]))
    assert info.value.size > info.value.limit == 600
    assert instances[0].produced == []


def test_broker_size_error_from_produce_becomes_payload_too_large(setup: Any) -> None:
    prod, instances, _cfg = setup
    code = sorted(size_error_codes())[0]
    instances[0].raise_on_produce = KafkaException(KafkaError(code))
    with pytest.raises(PayloadTooLarge):
        prod.send_envelope(_env())


def test_control_and_dlq_keys_and_topics(setup: Any) -> None:
    prod, instances, cfg = setup
    prod.send_control({"schema_version": 1, "event_type": "collector_status", "run_id": RUN})
    prod.send_dlq({"schema_version": 1, "error_id": "er_x", "run_id": RUN, "feed_type": "alerts"})
    control, dlq = instances[0].produced
    assert control["topic"] == cfg.kafka.topics.control and control["key"] == f"{RUN}|control".encode()
    assert dlq["topic"] == cfg.kafka.topics.dlq and dlq["key"] == f"{RUN}|alerts".encode()
    with pytest.raises(PayloadTooLarge):
        prod.send_control({"run_id": RUN, "blob": "x" * (cfg.kafka.control_max_message_bytes + 1)})


def test_fatal_error_recreates_the_underlying_producer(setup: Any) -> None:
    prod, instances, _cfg = setup
    instances[0].conf["error_cb"](FakeErr(fatal=True))
    assert prod.fatal_error is not None
    prod.send_envelope(_env())
    assert len(instances) == 2 and instances[0].purged
    assert prod.generation == 1 and prod.fatal_error is None
    assert len(instances[1].produced) == 1
    # A late fatal report from the old instance must not trigger another recreation.
    instances[0].conf["error_cb"](FakeErr(fatal=True))
    assert prod.fatal_error is None
