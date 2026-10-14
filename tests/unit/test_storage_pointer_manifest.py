"""Pointer / manifest storage functions: CQL shape, LWT interpretation, value handling (fake session)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from cassandra.query import UNSET_VALUE

from transitpulse.storage import cassandra as cs
from transitpulse.storage.manifest import (
    insert_building,
    mark_ready,
    read_manifest,
    set_outcome_once,
)
from transitpulse.storage.pointer import POINTER_COLUMNS, bootstrap_pointer, cas_pointer, read_pointer


class FakePrepared:
    def __init__(self, cql: str) -> None:
        self.query_string = cql
        self.is_idempotent = False


class FakeResult:
    def __init__(self, rows=None, applied=None) -> None:
        self._rows = list(rows or [])
        self._applied = applied

    @property
    def was_applied(self) -> bool:
        if self._applied is None:
            raise RuntimeError("not an LWT result")
        return self._applied

    def one(self):
        return self._rows[0] if self._rows else None


class FakeSession:
    def __init__(self, result: FakeResult | None = None) -> None:
        self.result = result or FakeResult()
        self.calls: list[tuple[str, tuple, dict]] = []
        self.prepared: list[str] = []

    def prepare(self, cql: str) -> FakePrepared:
        self.prepared.append(cql)
        return FakePrepared(cql)

    def execute(self, stmt, params=None, **kwargs):
        self.calls.append((stmt.query_string, tuple(params or ()), kwargs))
        return self.result

    @property
    def last(self):
        return self.calls[-1]


T0 = datetime(2026, 10, 5, 12, 0, 30, 123000, tzinfo=UTC)
NAIVE = datetime(2026, 10, 5, 12, 0, 30, 123000)


def _pointer_row(**over):
    row = {
        "run_id": "run-1", "feed_type": "vehicle_positions", "snapshot_id": "sn_b", "generation_id": "gn_b",
        "source_order": "000001791201660.00001791201660000000.sn_b", "feed_timestamp": T0, "metrics_as_of": T0,
        "view_published_at": T0, "expires_at": T0, "is_partial": False, "is_empty": False,
    }
    row.update(over)
    return row


# ------------------------------------------------------------------------------------------ pointer
def test_read_pointer_serial_profile_and_aware_datetimes():
    sess = FakeSession(FakeResult([{"run_id": "run-1", "snapshot_id": "sn_a", "view_published_at": NAIVE}]))
    row = read_pointer(sess, "run-1", "vehicle_positions", serial=True)
    cql, params, kwargs = sess.last
    assert cql.startswith("SELECT * FROM feed_current WHERE run_id = ? AND feed_type = ?")
    assert params == ("run-1", "vehicle_positions")
    assert kwargs["execution_profile"] == cs.SERIAL_PROFILE
    assert row["view_published_at"] == T0 and row["view_published_at"].tzinfo is not None


def test_read_pointer_none():
    assert read_pointer(FakeSession(FakeResult([])), "run-1", "alerts") is None


def test_bootstrap_pointer_applied_and_not_applied():
    sess = FakeSession(FakeResult([{"[applied]": True}], applied=True))
    assert bootstrap_pointer(sess, _pointer_row()) == (True, None)
    cql, params, _ = sess.last
    assert cql.endswith("IF NOT EXISTS") and len(params) == len(POINTER_COLUMNS)

    existing = {"[applied]": False, **_pointer_row(snapshot_id="sn_a", view_published_at=NAIVE)}
    sess2 = FakeSession(FakeResult([existing], applied=False))
    applied, row = bootstrap_pointer(sess2, _pointer_row())
    assert applied is False
    assert row["snapshot_id"] == "sn_a" and "[applied]" not in row
    assert row["view_published_at"].tzinfo is not None


def test_cas_pointer_condition_and_result():
    sess = FakeSession(FakeResult([{"[applied]": False, "snapshot_id": "sn_x"}], applied=False))
    assert cas_pointer(sess, _pointer_row(), "sn_a") is False
    cql, params, kwargs = sess.last
    assert cql.startswith("UPDATE feed_current SET snapshot_id = ?")
    assert cql.endswith("WHERE run_id = ? AND feed_type = ? IF snapshot_id = ?")
    assert params[-3:] == ("run-1", "vehicle_positions", "sn_a")
    assert params[0] == "sn_b"
    sess.result = FakeResult([{"[applied]": True}], applied=True)
    assert cas_pointer(sess, _pointer_row(), "sn_a") is True


def test_cas_pointer_requires_full_row_and_expected():
    with pytest.raises(ValueError):
        cas_pointer(FakeSession(), {"run_id": "run-1"}, "sn_a")
    with pytest.raises(ValueError):
        cas_pointer(FakeSession(), _pointer_row(), "")


# ------------------------------------------------------------------------------------------ manifest
def _building(**over):
    row = {
        "run_id": "run-1", "feed_type": "vehicle_positions", "snapshot_id": "sn_b", "generation_id": "gn_b",
        "source_order": "x", "first_batch_id": 7, "metrics_as_of": T0, "first_materialized_at": T0,
        "expires_at": T0, "history_expires_at": T0, "parent_snapshot_id": None, "built_as_late": False,
    }
    row.update(over)
    return row


def test_insert_building_applied_returns_frozen_row_with_unset_for_none():
    sess = FakeSession(FakeResult([{"[applied]": True}], applied=True))
    applied, row = insert_building(sess, _building())
    assert applied is True
    assert row["status"] == "BUILDING" and row["first_batch_id"] == 7
    cql, params, _ = sess.last
    assert cql.startswith("INSERT INTO snapshot_manifest (") and cql.endswith("IF NOT EXISTS")
    cols = cql[cql.index("(") + 1:cql.index(")")].split(", ")
    assert params[cols.index("status")] == "BUILDING"
    assert params[cols.index("parent_snapshot_id")] is UNSET_VALUE


def test_insert_building_not_applied_adopts_existing_frozen_params():
    existing = {"[applied]": False, **_building(first_batch_id=3, parent_snapshot_id="sn_a"), "status": "READY",
                "metrics_as_of": NAIVE}
    sess = FakeSession(FakeResult([existing], applied=False))
    applied, row = insert_building(sess, _building())
    assert applied is False
    assert row["first_batch_id"] == 3 and row["parent_snapshot_id"] == "sn_a" and row["status"] == "READY"
    assert row["metrics_as_of"] == T0


def test_insert_building_freezes_parent_available():
    sess = FakeSession(FakeResult([{"[applied]": True}], applied=True))
    applied, row = insert_building(sess, _building(parent_snapshot_id="sn_a", parent_generation_id="gn_a",
                                                   parent_available=False))
    assert applied and row["parent_available"] is False
    cql, params, _ = sess.last
    cols = cql[cql.index("(") + 1:cql.index(")")].split(", ")
    assert params[cols.index("parent_available")] is False
    assert cols.index("parent_generation_id") < cols.index("parent_available") < cols.index("built_as_late")


def test_insert_building_rejects_non_frozen_or_missing_columns():
    with pytest.raises(ValueError):
        insert_building(FakeSession(), _building(ready_at=T0))
    with pytest.raises(ValueError):
        insert_building(FakeSession(), _building(expires_at=None))


def test_mark_ready_is_conditional_on_building():
    sess = FakeSession(FakeResult([{"[applied]": True}], applied=True))
    ok = mark_ready(sess, ("run-1", "vehicle_positions", "sn_b"), ready_at=T0,
                    expected_counts={"vehicles_by_route_snapshot": 3, "vehicles_by_route_snapshot/R_A": 3},
                    content_checksum="c" * 64, accounting_json="{}",
                    counts={"is_partial": False, "is_empty": False, "rejected_entity_count": 0})
    assert ok is True
    cql, params, _ = sess.last
    assert "status = 'READY'" in cql and cql.endswith("IF status = 'BUILDING'")
    assert params[1] == {"vehicles_by_route_snapshot": 3, "vehicles_by_route_snapshot/R_A": 3}
    assert params[-3:] == ("run-1", "vehicle_positions", "sn_b")
    sess.result = FakeResult([{"[applied]": False, "status": "READY"}], applied=False)
    assert mark_ready(sess, ("run-1", "vehicle_positions", "sn_b"), ready_at=T0, expected_counts={},
                      content_checksum="c", accounting_json="{}", counts={}) is False
    with pytest.raises(ValueError):
        mark_ready(sess, ("run-1", "vehicle_positions", "sn_b"), ready_at=T0, expected_counts={},
                   content_checksum="c", accounting_json="{}", counts={"status": "READY"})


def test_set_outcome_once_condition_and_validation():
    sess = FakeSession(FakeResult([{"[applied]": True}], applied=True))
    assert set_outcome_once(sess, ("run-1", "alerts", "sn_c"), "PUBLISHED", T0, 1234) is True
    cql, params, _ = sess.last
    assert cql.endswith("IF publish_outcome = null")
    assert params[:3] == ("PUBLISHED", T0, 1234)
    assert set_outcome_once(sess, ("run-1", "alerts", "sn_c"), "LATE_NOT_PUBLISHED") is True
    _, params, _ = sess.last
    assert params[1] is UNSET_VALUE and params[2] is UNSET_VALUE
    with pytest.raises(ValueError):
        set_outcome_once(sess, ("run-1", "alerts", "sn_c"), "WHATEVER")
    with pytest.raises(ValueError):
        set_outcome_once(sess, ("run-1", "alerts"), "PUBLISHED")  # type: ignore[arg-type]


def test_read_manifest_normalizes_maps_and_times():
    class Om(dict):
        pass

    sess = FakeSession(FakeResult([{"snapshot_id": "sn_b", "expected_counts": Om(a=1), "ready_at": NAIVE}]))
    row = read_manifest(sess, "run-1", "vehicle_positions", "sn_b", serial=True)
    assert row == {"snapshot_id": "sn_b", "expected_counts": {"a": 1}, "ready_at": T0}
    assert sess.last[2]["execution_profile"] == cs.SERIAL_PROFILE


def test_prepared_statements_are_cached_per_session():
    sess = FakeSession(FakeResult([]))
    read_pointer(sess, "run-1", "alerts")
    read_pointer(sess, "run-1", "trip_updates")
    assert len(sess.prepared) == 1
