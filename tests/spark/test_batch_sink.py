"""Executor-side sink: value conversion, TTL, plan, and the partition writer with a fake session."""

from __future__ import annotations

import pickle
from datetime import UTC, date, datetime

import pytest
from cassandra.query import UNSET_VALUE

from transitpulse.processing import sink
from transitpulse.processing.faults import InjectedWriteError
from transitpulse.storage.migrate import table_schemas

VEH = "vehicles_by_route_snapshot"


def test_convert_value_rules():
    assert sink.convert_value(None, sink.KIND_SCALAR) is UNSET_VALUE
    assert sink.convert_value(1791201610123, sink.KIND_TS) == datetime(2026, 10, 5, 12, 0, 10, 123000, tzinfo=UTC)
    assert sink.convert_value(["b", "a", "a"], sink.KIND_SET) == {"a", "b"}
    assert sink.convert_value([], sink.KIND_SET) is UNSET_VALUE
    assert sink.convert_value(["x"], sink.KIND_LIST) == ["x"]
    assert sink.convert_value({}, sink.KIND_MAP) is UNSET_VALUE
    assert sink.convert_value({"k": 1}, sink.KIND_MAP) == {"k": 1}
    assert sink.convert_value(date(2026, 10, 5), sink.KIND_DATE) == date(2026, 10, 5)
    assert sink.convert_value(0, sink.KIND_SCALAR) == 0 and sink.convert_value(False, sink.KIND_SCALAR) is False
    with pytest.raises(TypeError):
        sink.convert_value("2026-10-05", sink.KIND_TS)
    with pytest.raises(TypeError):
        sink.convert_value(True, sink.KIND_TS)


def test_ttl_is_remaining_seconds_and_never_below_one():
    assert sink.ttl_seconds(1000.2, now_epoch_s=900.0) == 101
    assert sink.ttl_seconds(1000.0, now_epoch_s=999.5) == 1
    with pytest.raises(sink.GenerationExpiredError):
        sink.ttl_seconds(1000.0, now_epoch_s=1000.0)
    with pytest.raises(sink.GenerationExpiredError):
        sink.ttl_seconds(1000.0, now_epoch_s=1000.9)


def test_ttl_never_expires_rows_before_expires_at():
    # Cassandra: deletion = floor(server now) + ttl. ceil(expires - now) would give 50 here -> deletion at 100 < 100.5
    assert sink.ttl_seconds(100.5, now_epoch_s=50.9) == 51
    for now in (50.0, 50.1, 50.9, 99.0, 99.99):
        for exp in (100.0, 100.001, 100.5, 100.999):
            ttl = sink.ttl_seconds(exp, now_epoch_s=now)
            assert int(now) + ttl >= exp
    assert sink.remaining_seconds(100.0, 100.9) == 0 and sink.remaining_seconds(100.0, 101.0) == -1


def test_insert_cql_and_plan():
    assert sink.insert_cql("t", ["a", "b"]) == "INSERT INTO t (a, b) VALUES (?, ?) USING TTL ?"
    cols = table_schemas()[VEH].columns
    plan = sink.table_plan(VEH, ["run_id", "observation_time", "quality_flags"], cols, 2000.0)
    assert plan["kinds"] == [sink.KIND_SCALAR, sink.KIND_TS, sink.KIND_SET] and plan["expires_at"] == 2000.0
    with pytest.raises(ValueError):
        sink.table_plan(VEH, ["nope"], cols, 1.0)


class FakePrepared:
    def __init__(self, cql):
        self.query_string = cql
        self.is_idempotent = False


class FakeSession:
    def __init__(self):
        self.prepared = []
        self.is_shutdown = False

    def prepare(self, cql):
        self.prepared.append(cql)
        return FakePrepared(cql)


@pytest.fixture()
def fake_cluster(monkeypatch):
    import cassandra.concurrent

    sess = FakeSession()
    batches: list[tuple[str, list]] = []

    def fake_exec(session, stmt, params, concurrency=None, raise_on_first_error=None):
        assert raise_on_first_error is True and concurrency == 4
        batches.append((stmt.query_string, list(params)))
        return []

    monkeypatch.setattr(sink, "worker_session", lambda conf: sess)
    monkeypatch.setattr(cassandra.concurrent, "execute_concurrent_with_args", fake_exec)
    sink._PREPARED.clear()
    return sess, batches


CONF = {"hosts": ["127.0.0.1"], "port": 9042, "keyspace": "transitpulse", "local_dc": "datacenter1",
        "consistency": "LOCAL_ONE", "serial_consistency": "SERIAL", "connect_timeout": 5.0, "request_timeout": 20.0,
        "concurrency": 4, "buffer_rows": 2, "max_attempts": 1, "retry_backoff_seconds": 0.0}


def _plan(expires=4_000_000_000.0):
    return {
        "a": {"columns": ["k", "ts"], "kinds": [sink.KIND_SCALAR, sink.KIND_TS], "expires_at": expires},
        "b": {"columns": ["k", "flags"], "kinds": [sink.KIND_SCALAR, sink.KIND_SET], "expires_at": expires},
    }


def test_partition_writer_buffers_converts_and_counts(fake_cluster):
    sess, batches = fake_cluster
    writer = sink.make_partition_writer(CONF, _plan())
    rows = [("a", ("x1", 1791201610123)), ("a", ("x2", None)), ("a", ("x3", 0)), ("b", ("y1", ["f"]))]
    out = sorted((t, n) for t, n, _s in writer(iter(rows)))
    assert out == [("a", 3), ("b", 1)]
    assert [len(p) for _, p in batches] == [2, 1, 1]           # buffer of 2 rows, then final flushes
    first = batches[0][1][0]
    assert first[0] == "x1" and first[1] == datetime(2026, 10, 5, 12, 0, 10, 123000, tzinfo=UTC)
    assert isinstance(first[2], int) and first[2] > 1                # TTL bound last
    assert batches[0][1][1][1] is UNSET_VALUE
    assert batches[0][0].endswith("USING TTL ?")
    assert len(sess.prepared) == 2                                     # one statement per (table, columns)


def test_partition_writer_raises_when_expired(fake_cluster):
    writer = sink.make_partition_writer(CONF, _plan(expires=1.0))
    with pytest.raises(sink.GenerationExpiredError):
        list(writer(iter([("a", ("x", None))])))


def test_partition_writer_fault_injection(fake_cluster):
    _sess, batches = fake_cluster
    writer = sink.make_partition_writer(CONF, _plan(), {"fail_after_rows": 1})
    with pytest.raises(InjectedWriteError):
        list(writer(iter([("a", ("x1", None)), ("a", ("x2", None))])))
    assert len(batches) == 1 and len(batches[0][1]) == 1               # partial write happened first


def test_partition_writer_is_picklable_and_empty_partition_needs_no_session(monkeypatch):
    from pyspark import cloudpickle  # the serializer Spark uses for mapPartitions closures

    def boom(conf):  # pragma: no cover - must not be called
        raise AssertionError("no session for an empty partition")

    writer = sink.make_partition_writer(CONF, _plan())
    pickle.loads(cloudpickle.dumps(writer))
    monkeypatch.setattr(sink, "worker_session", boom)
    assert list(writer(iter([]))) == []


def test_connection_conf_is_plain_data():
    from types import SimpleNamespace

    c = SimpleNamespace(hosts=["h"], port=1, keyspace="k", local_dc="dc", consistency="LOCAL_ONE",
                        serial_consistency="SERIAL", connect_timeout_seconds=1, request_timeout_seconds=2,
                        write_concurrency=3, write_buffer_rows=4)
    conf = sink.connection_conf(SimpleNamespace(cassandra=c))
    assert pickle.loads(pickle.dumps(conf)) == conf and conf["concurrency"] == 3
