"""Pure/Spark helpers of processing/batch.py without Cassandra: conform + epoch-ms, expected counts,
checksum, frozen parameters, parent selection, Spark session configuration."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from transitpulse.contracts.ids import generation_id
from transitpulse.contracts.policy import Policy
from transitpulse.processing import batch as B
from transitpulse.processing.context import BuildParams, RunInfo
from transitpulse.storage.manifest import BUILDING_COLUMNS
from transitpulse.storage.migrate import table_schemas

SCHEMAS = table_schemas()
VEH = "vehicles_by_route_snapshot"
RM = "route_metrics_by_snapshot"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
TS1 = datetime(2026, 10, 5, 12, 0, 10, 123000, tzinfo=UTC)
TS2 = datetime(2026, 10, 5, 11, 56, 0, tzinfo=UTC)
POLICY = Policy(
    policy_version="policy-v1", vehicle_stale_seconds=120, prediction_stale_seconds=90, alert_stale_seconds=300,
    predicted_late_seconds=300, future_tolerance_seconds=5, service_date_inference_window_seconds=43200,
    uncertainty_policy="mbta_bus_uncertainty_v1", target_policy="first_future_resolved_stop_v1",
    timestamp_ms_threshold=100_000_000_000, current_generation_ttl_seconds=3600, history_ttl_seconds=259200,
    min_publish_ttl_seconds=60,
)


def run_info(clock_mode: str = "source", clock: datetime | None = datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC)):
    return RunInfo(run_id="run-1", project_id="p", scope_id="s", source_type="synthetic", clock_mode=clock_mode,
                   gtfs_version="g", agency_timezone="America/New_York", route_ids=("R_A", "R_B"),
                   pipeline_version="p1", policy=POLICY, application_clock=clock if clock_mode == "source" else None,
                   clock_sequence=4, run_state="RUNNING")


def meta(**over):
    m = {
        "feed_type": "vehicle_positions", "snapshot_id": "sn_b", "capture_id": "vp-1", "source_order": "o-b",
        "feed_timestamp": 1791201660, "original_fetched_at_ms": 1791201661123, "published_at_ms": 1791201662000,
        "kafka_topic": "t", "kafka_partition": 0, "kafka_offset": 9, "raw_path": "r", "raw_sha256": "h",
        "gtfs_version": "g", "scope_id": "s", "scenario_id": None, "source_type": "synthetic",
        "source_entity_count": 4, "selected_entity_count": 3, "value_sha256": "v",
    }
    m.update(over)
    return m


VEH_DDL = ("run_id string, snapshot_id string, route_id string, vehicle_id string, observation_time timestamp, "
           "direction_id bigint, quality_flags array<string>")


def veh_rows():
    return [
        ("run-1", "sn_a", "R_A", "V1", TS1, 0, ["B", "A"]),
        ("run-1", "sn_a", "R_A", "V2", None, None, None),
        ("run-1", "sn_a", "R_B", "V3", TS2, 1, ["X"]),
    ]


# ------------------------------------------------------------------------------------------ conform / ms
def test_conform_table_converts_timestamps_to_epoch_ms_and_casts(spark):
    df = spark.createDataFrame(veh_rows(), VEH_DDL)
    out = B.conform_table(df, VEH, SCHEMAS[VEH])
    types = {f.name: f.dataType for f in out.schema.fields}
    assert isinstance(types["observation_time"], T.LongType)
    assert isinstance(types["direction_id"], T.IntegerType)
    assert isinstance(types["quality_flags"], T.ArrayType)
    got = {r["vehicle_id"]: r["observation_time"] for r in out.collect()}
    assert got["V1"] == 1791201610123 and got["V2"] is None
    assert got["V3"] == (TS2 - EPOCH) // timedelta(milliseconds=1)
    # CQL column order
    assert out.columns == [c for c in SCHEMAS[VEH].columns if c in df.columns]


def test_conform_table_rejects_contract_violations(spark):
    df = spark.createDataFrame(veh_rows(), VEH_DDL)
    with pytest.raises(B.InvalidGenerationOutput):
        B.conform_table(df.withColumn("foo", F.lit(1)), VEH, SCHEMAS[VEH])
    with pytest.raises(B.InvalidGenerationOutput):
        B.conform_table(df.drop("vehicle_id"), VEH, SCHEMAS[VEH])
    with pytest.raises(B.InvalidGenerationOutput):
        B.conform_table(df.withColumn("observation_time", F.lit("2026-10-05T12:00:00Z")), VEH, SCHEMAS[VEH])


def test_spark_data_type_mapping():
    assert B.spark_data_type("timestamp", timestamp_ms=True) == T.LongType()
    assert B.spark_data_type("timestamp", timestamp_ms=False) == T.TimestampType()
    assert B.spark_data_type("map<text,int>", timestamp_ms=True) == T.MapType(T.StringType(), T.IntegerType(), True)
    assert B.spark_data_type("set<int>", timestamp_ms=True) == T.ArrayType(T.IntegerType(), True)
    assert B.spark_data_type("list<text>", timestamp_ms=True) == T.ArrayType(T.StringType(), True)
    assert B.spark_data_type("date", timestamp_ms=True) == T.DateType()
    with pytest.raises(ValueError):
        B.spark_data_type("tuple<int,int>", timestamp_ms=True)


def test_ts_lit_is_exact(spark):
    dt = datetime(2026, 10, 5, 12, 0, 30, 123456, tzinfo=UTC)
    row = spark.range(1).select(B._ts_lit(dt).alias("t")).select(F.expr("unix_micros(t)").alias("u")).collect()[0]
    assert row["u"] == (dt - EPOCH) // timedelta(microseconds=1)


# ------------------------------------------------------------------------------------------ expected counts
def _tables(spark, rows):
    veh = B.conform_table(spark.createDataFrame(rows, VEH_DDL), VEH, SCHEMAS[VEH])
    rm = B.conform_table(spark.createDataFrame(
        [("run-1", "vehicle_positions", "sn_a", "R_A", 2), ("run-1", "vehicle_positions", "sn_a", "R_B", 1)],
        "run_id string, feed_type string, snapshot_id string, route_id string, snapshot_membership_count int"),
        RM, SCHEMAS[RM])
    return {VEH: veh, RM: rm}


def test_expected_counts_totals_and_per_route_zero_fill(spark):
    gc = B.compute_generation_counts(_tables(spark, veh_rows()), SCHEMAS, ["R_A", "R_B", "R_C"])
    assert gc.expected_counts == {
        VEH: 3, f"{VEH}/R_A": 2, f"{VEH}/R_B": 1, f"{VEH}/R_C": 0, RM: 2,
    }
    assert len(gc.content_checksum) == 64


def test_checksum_is_order_insensitive_and_content_sensitive(spark):
    a = B.compute_generation_counts(_tables(spark, veh_rows()), SCHEMAS, ["R_A", "R_B"])
    rev = [(*r[:6], list(reversed(r[6])) if r[6] else r[6]) for r in reversed(veh_rows())]
    b = B.compute_generation_counts(_tables(spark, rev), SCHEMAS, ["R_A", "R_B"])
    assert a == b
    changed = veh_rows()
    changed[0] = (*changed[0][:5], 1, changed[0][6])
    c = B.compute_generation_counts(_tables(spark, changed), SCHEMAS, ["R_A", "R_B"])
    assert c.expected_counts == a.expected_counts and c.content_checksum != a.content_checksum


def test_duplicate_or_null_primary_keys_fail_the_generation(spark):
    dup = veh_rows() + [("run-1", "sn_a", "R_A", "V1", TS2, 0, None)]
    with pytest.raises(B.InvalidGenerationOutput):
        B.compute_generation_counts(_tables(spark, dup), SCHEMAS, ["R_A", "R_B"])
    null_pk = veh_rows() + [("run-1", "sn_a", "R_A", None, TS2, 0, None)]
    with pytest.raises(B.InvalidGenerationOutput):
        B.compute_generation_counts(_tables(spark, null_pk), SCHEMAS, ["R_A", "R_B"])


def test_expected_counts_from_aggregates_pure():
    rows = [
        {"t": VEH, "k": "R_A", "n": 2, "d": 2, "bad": 0, "hs": 10},
        {"t": "quality_by_snapshot", "k": None, "n": 3, "d": 3, "bad": 0, "hs": 7},
    ]
    gc = B.expected_counts_from_aggregates(rows, [VEH, "quality_by_snapshot", RM], ["R_A", "R_B"])
    assert gc.expected_counts == {VEH: 2, f"{VEH}/R_A": 2, f"{VEH}/R_B": 0, "quality_by_snapshot": 3, RM: 0}
    with pytest.raises(B.InvalidGenerationOutput):
        B.expected_counts_from_aggregates([{"t": VEH, "k": "R_A", "n": 2, "d": 1, "bad": 0, "hs": 0}], [VEH], [])
    with pytest.raises(B.InvalidGenerationOutput):
        B.expected_counts_from_aggregates([{"t": VEH, "k": "R_A", "n": 1, "d": 1, "bad": 1, "hs": 0}], [VEH], [])


# ------------------------------------------------------------------------------------------ frozen params
def test_building_row_freezes_parameters_at_ms_precision():
    now = datetime(2026, 10, 5, 12, 0, 40, 987654, tzinfo=UTC)
    parent = {"snapshot_id": "sn_a", "generation_id": "gn_a", "source_order": "o-a",
              "expires_at": now + timedelta(seconds=600)}
    row = B.building_row(meta(), run_info(), batch_id=5, now=now, parent=parent, late=False, pipeline_version="p1")
    assert row["parent_available"] is True
    assert set(row) <= set(BUILDING_COLUMNS)
    nowf = datetime(2026, 10, 5, 12, 0, 40, 987000, tzinfo=UTC)
    assert row["first_materialized_at"] == nowf
    assert row["expires_at"] == nowf + timedelta(seconds=3600)
    assert row["history_expires_at"] == nowf + timedelta(seconds=259200)
    assert row["metrics_as_of"] == datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC)       # run clock (source)
    assert row["generation_id"] == generation_id("run-1", "vehicle_positions", "sn_b", "p1")
    assert row["feed_timestamp"] == EPOCH + timedelta(seconds=1791201660)
    assert row["original_fetched_at"] == EPOCH + timedelta(milliseconds=1791201661123)
    assert (row["parent_snapshot_id"], row["parent_generation_id"]) == ("sn_a", "gn_a")
    assert row["first_batch_id"] == 5 and row["built_as_late"] is False and row["clock_sequence_at_build"] == 4

    late = B.building_row(meta(), run_info(), batch_id=5, now=now, parent=parent, late=True, pipeline_version="p1")
    assert late["parent_snapshot_id"] is None and late["built_as_late"] is True and late["parent_available"] is None

    expired = {**parent, "expires_at": now - timedelta(seconds=1)}
    row_exp = B.building_row(meta(), run_info(), batch_id=5, now=now, parent=expired, late=False,
                             pipeline_version="p1")
    assert row_exp["parent_snapshot_id"] == "sn_a" and row_exp["parent_available"] is False
    tu = B.building_row(meta(feed_type="trip_updates"), run_info(), batch_id=5, now=now, parent=None, late=False,
                        pipeline_version="p1")
    assert tu["parent_snapshot_id"] is None and tu["parent_available"] is None

    wall = B.building_row(meta(), run_info("wall"), batch_id=5, now=now, parent=None, late=False,
                          pipeline_version="p1")
    assert wall["metrics_as_of"] == nowf


def test_params_from_manifest_round_trip():
    now = datetime(2026, 10, 5, 12, 0, 40, tzinfo=UTC)
    row = B.building_row(meta(), run_info(), batch_id=5, now=now, parent=None, late=False, pipeline_version="p1")
    man = {**row, "status": "BUILDING"}
    p = B.params_from_manifest(man, parent_available=True)
    assert isinstance(p, BuildParams)
    assert (p.snapshot_id, p.first_batch_id, p.metrics_as_of, p.expires_at) == (
        "sn_b", 5, row["metrics_as_of"], row["expires_at"])
    assert p.parent_snapshot_id is None and p.parent_available is True
    assert B.params_from_manifest(man, parent_available=True) == p     # identical on every retry


def test_parent_availability_is_frozen_in_the_manifest():
    base = {**B.building_row(meta(), run_info(), batch_id=5, now=datetime(2026, 10, 5, 12, 0, 40, tzinfo=UTC),
                             parent=None, late=False, pipeline_version="p1"), "status": "BUILDING"}
    frozen_false = {**base, "parent_snapshot_id": "sn_a", "parent_generation_id": "gn_a", "parent_available": False}
    assert B.params_from_manifest(frozen_false).parent_available is False          # stored value reused
    frozen_true = {**frozen_false, "parent_available": True}
    assert B.params_from_manifest(frozen_true).parent_available is True
    # stored True but rows unreadable at build time: the caller downgrades for this build only
    assert B.params_from_manifest(frozen_true, parent_available=False).parent_available is False
    legacy = {**frozen_false, "parent_available": None}                               # pre-column manifest
    assert B.frozen_parent_available(legacy) is True


def test_remaining_s_uses_the_sink_ttl_rule():
    t = datetime(2026, 10, 5, 12, 0, 0, tzinfo=UTC)
    assert B.remaining_s(t + timedelta(milliseconds=500), t + timedelta(milliseconds=900)) == 1
    assert B.remaining_s(t, t + timedelta(milliseconds=900)) == 0
    assert B.remaining_s(None, t) == 0


def test_select_parent_takes_greatest_source_order():
    exp = datetime(2026, 10, 5, 13, 0, 0, tzinfo=UTC)
    ptr = {"snapshot_id": "sn_p", "generation_id": "gn_p", "source_order": "000000000010.x.sn_p"}
    prev = {"snapshot_id": "sn_q", "generation_id": "gn_q", "source_order": "000000000011.x.sn_q", "expires_at": exp}
    assert B.select_parent(ptr, prev) == {"snapshot_id": "sn_q", "generation_id": "gn_q",
                                          "source_order": "000000000011.x.sn_q", "expires_at": exp}
    assert B.select_parent(ptr, None)["snapshot_id"] == "sn_p"
    assert B.select_parent(None, prev)["snapshot_id"] == "sn_q"
    assert B.select_parent(None, None) is None


def test_small_pure_helpers():
    t0 = datetime(2026, 10, 5, 12, 0, 0, tzinfo=UTC)
    assert B.latency_ms(t0 + timedelta(milliseconds=1500), t0) == 1500
    assert B.latency_ms(None, t0) is None
    q = B.quality_row("run-1", "vehicle_positions", "sn_a", "LATE_SNAPSHOT_NOT_PUBLISHED", details={"x": 1})
    assert q["severity"] == "warning" and q["unit"] == "snapshot" and q["disposition_counts"] == {"retained": 1}
    acc = B.accounting_metrics('{"candidate_observations": 3, "unique_observations": 2, '
                               '"duplicate_observations": 1, "ok": true, "nested": {"a": 1}}')
    assert acc == {"candidates": {"candidate_observations": 3}, "unique": {"observations": 2},
                   "duplicate": {"observations": 1}}
    assert B.accounting_metrics("not json") == {"candidates": {}, "unique": {}, "duplicate": {}}
    pr = B.pointer_row({"run_id": "r", "feed_type": "f", "snapshot_id": "s", "generation_id": "g",
                        "source_order": "o", "expires_at": t0, "is_partial": None, "is_empty": True}, t0)
    assert pr["view_published_at"] == t0 and pr["is_partial"] is False and pr["is_empty"] is True


def test_quality_frame_and_rows_to_frame(spark):
    q = B.quality_frame(spark, [B.quality_row("run-1", "alerts", "sn_a", "PARENT_UNAVAILABLE",
                                              sample_refs=["sn_p"])], SCHEMAS["quality_by_snapshot"])
    r = q.collect()[0]
    assert r["reason_code"] == "PARENT_UNAVAILABLE" and r["sample_refs"] == ["sn_p"]
    parent = B.rows_to_frame(spark, [{"run_id": "run-1", "snapshot_id": "sn_p", "route_id": "R_A",
                                      "vehicle_id": "V1", "observation_time": TS1, "quality_flags": ["A"]}],
                             SCHEMAS[VEH])
    types = {f.name: f.dataType for f in parent.schema.fields}
    assert isinstance(types["observation_time"], T.TimestampType)
    assert parent.select(F.expr("unix_millis(observation_time)").alias("m")).collect()[0]["m"] == 1791201610123
    empty = B.rows_to_frame(spark, [], SCHEMAS[VEH])
    assert empty.count() == 0 and empty.columns == list(SCHEMAS[VEH].columns)


# ------------------------------------------------------------------------------------------ frozen parent read
def _parent_proc(monkeypatch, pman, rows_by_route, per_route=2, total=3):
    proc = object.__new__(B.BatchProcessor)
    proc.ctx = SimpleNamespace(
        run_id="run-1", session=object(), schemas=SCHEMAS,
        cfg=SimpleNamespace(cassandra=SimpleNamespace(parent_read_limit_per_route=per_route,
                                                      parent_read_total_limit=total)),
    )
    monkeypatch.setattr(B, "read_manifest", lambda *a, **k: pman)
    monkeypatch.setattr(B, "execute", lambda session, cql, params, **k: list(rows_by_route.get(params[2], [])))
    return proc


CHILD = {"feed_type": "vehicle_positions", "snapshot_id": "sn_b", "parent_snapshot_id": "sn_a",
         "parent_generation_id": "gn_a", "parent_available": True}


def _vrow(route, vid):
    return {"run_id": "run-1", "snapshot_id": "sn_a", "route_id": route, "vehicle_id": vid}


def test_frozen_unavailable_parent_is_never_read(monkeypatch):
    proc = _parent_proc(monkeypatch, None, {})
    assert proc._load_parent(None, {**CHILD, "parent_available": False}, run_info()) == (None, False,
                                                                                       "frozen_unavailable")


def test_frozen_available_parent_missing_rows_fails_the_batch(monkeypatch):
    proc = _parent_proc(monkeypatch, None, {})
    with pytest.raises(B.ParentUnavailable, match="NEW run"):
        proc._load_parent(None, CHILD, run_info())
    ready = {"status": "READY", "expires_at": datetime(2026, 10, 5, 13, 0, tzinfo=UTC),
             "expected_counts": {f"{VEH}/R_A": 2, f"{VEH}/R_B": 0}}
    proc = _parent_proc(monkeypatch, ready, {"R_A": [_vrow("R_A", "V1")]})
    with pytest.raises(B.ParentUnavailable):
        proc._load_parent(None, CHILD, run_info())


def test_frozen_available_parent_is_read_without_wall_clock_expiry_check(spark, monkeypatch):
    long_expired = {"status": "READY", "expires_at": datetime(2000, 1, 1, tzinfo=UTC),
                    "expected_counts": {f"{VEH}/R_A": 2, f"{VEH}/R_B": 0}}
    proc = _parent_proc(monkeypatch, long_expired, {"R_A": [_vrow("R_A", "V1"), _vrow("R_A", "V2")]})
    df, available, reason = proc._load_parent(spark, CHILD, run_info())
    assert available is True and reason is None and df.count() == 2


def test_parent_read_bound_overflow_is_deterministic_unavailability(monkeypatch):
    ready = {"status": "READY", "expires_at": datetime(2026, 10, 5, 13, 0, tzinfo=UTC), "expected_counts": {}}
    rows = {"R_A": [_vrow("R_A", f"V{i}") for i in range(3)]}
    proc = _parent_proc(monkeypatch, ready, rows, per_route=2)
    assert proc._load_parent(None, CHILD, run_info()) == (None, False, "parent_read_limit_per_route_exceeded")


# ------------------------------------------------------------------------------------------ status binds
def test_status_bind_values_and_clear_validation():
    from cassandra.query import UNSET_VALUE

    vals = B.status_bind_values({"a": 1, "b": None, "last_error": "x"}, ["a", "b", "last_error"],
                                clear=["last_error"])
    assert vals[0] == 1 and vals[1] is UNSET_VALUE and vals[2] is None
    now = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    for bad in (["last_batch_id"], ["input_messages_total"]):
        with pytest.raises(ValueError):
            B.update_status_guarded(None, "run-1", "processor", 1, deltas={}, sets={}, query_id=None, now=now,
                                    clear=bad)


# ------------------------------------------------------------------------------------------ heartbeat
def test_bounded_progress_json_drops_keys_instead_of_slicing():
    import json

    from transitpulse.processing.stream_job import bounded_progress_json

    small = {"query_id": "q", "batch_id": 1, "kafka_backlog": []}
    assert json.loads(bounded_progress_json(small)) == small
    offsets = {"t": {str(i): i for i in range(3000)}}
    big = {"query_id": "q", "batch_id": 3, "endOffsets": offsets, "latestOffsets": offsets,
           "kafka_backlog": [{"topic": "t", "partition": 0, "end_offset": 1, "latest_offset": 2, "backlog": 1}]}
    text = bounded_progress_json(big, limit=2000)
    doc = json.loads(text)                                  # always valid JSON
    assert len(text) <= 2000 and "endOffsets" not in doc and "latestOffsets" not in doc
    assert doc["kafka_backlog"] == big["kafka_backlog"] and doc["truncated_keys"] == ["endOffsets", "latestOffsets"]


def test_heartbeat_binds_explicit_null_error_and_keeps_backlog_when_idle():
    import json

    from cassandra.query import UNSET_VALUE

    from transitpulse.processing.stream_job import HEARTBEAT_COLUMNS, ProgressListener

    class Prepared:
        def __init__(self, cql):
            self.query_string = cql

    class Session:
        def __init__(self):
            self.calls = []

        def prepare(self, cql):
            return Prepared(cql)

        def execute(self, stmt, params=None, **kw):
            self.calls.append(dict(zip(HEARTBEAT_COLUMNS, params, strict=True)))

    sess = Session()
    lst = ProgressListener(run_id="run-1", progress_path=None, session=sess, metrics=None)  # type: ignore[arg-type]
    lst.query_id = "q1"
    lst.last_backlog = [{"topic": "t", "partition": 0, "end_offset": 5, "latest_offset": 7, "backlog": 2}]
    lst._heartbeat("FAILED", None, "boom")
    lst._heartbeat("IDLE")
    failed, idle = sess.calls
    assert failed["last_error"] == "boom"
    assert idle["last_error"] is None and idle["last_error"] is not UNSET_VALUE
    assert idle["last_batch_id"] is UNSET_VALUE
    assert json.loads(idle["progress_json"])["kafka_backlog"] == lst.last_backlog
def test_heartbeat_kafka_backlog_entries():
    from transitpulse.processing.stream_job import backlog_entries, progress_record

    rec = progress_record({"batchId": 3, "numInputRows": 4, "sources": [
        {"endOffset": {"mbta.control.v1": {"0": 10}, "mbta.alerts.snapshots.v1": {"0": 5}},
         "latestOffset": '{"mbta.control.v1": {"0": 12}, "mbta.alerts.snapshots.v1": {"0": 5}}'}]})
    assert rec["batchId"] == 3 and rec["endOffsets"]["mbta.control.v1"] == {"0": 10}
    assert backlog_entries(rec) == [
        {"topic": "mbta.alerts.snapshots.v1", "partition": 0, "end_offset": 5, "latest_offset": 5, "backlog": 0},
        {"topic": "mbta.control.v1", "partition": 0, "end_offset": 10, "latest_offset": 12, "backlog": 2},
    ]
    assert backlog_entries({"endOffsets": {"t": {"1": 3}}, "latestOffsets": {}}) == [
        {"topic": "t", "partition": 1, "end_offset": 3, "latest_offset": None, "backlog": None}]


# ------------------------------------------------------------------------------------------ spark conf
def test_session_conf(tmp_path):
    from transitpulse.config import load_config
    from transitpulse.processing.spark_session import session_conf

    cfg = load_config("local-native", workdir=tmp_path, environ={})
    conf = session_conf(cfg, None)
    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert conf["spark.driver.host"] == "127.0.0.1"
    assert conf["spark.ui.enabled"] == "false"
    assert conf["spark.sql.shuffle.partitions"] == str(cfg.spark.shuffle_partitions)
    assert conf["spark.local.dir"].startswith(str(tmp_path.resolve()))
    assert "spark.jars" not in conf
    jar = tmp_path / "spark-sql-kafka.jar"
    jar.write_bytes(b"x")
    conf2 = session_conf(cfg, SimpleNamespace(connector_jars=(str(jar),)), extra_conf={"spark.x": 1})
    assert conf2["spark.jars"] == str(jar) and conf2["spark.x"] == "1"
    assert "spark.jars" not in session_conf(cfg, SimpleNamespace(connector_jars=(str(jar),)), with_kafka=False)
    with pytest.raises(FileNotFoundError):
        session_conf(cfg, SimpleNamespace(connector_jars=(str(tmp_path / "missing.jar"),)))
