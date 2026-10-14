"""Run records: creation order, LWT bootstrap values, idempotency, run.json round trip (interfaces §2)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from transitpulse.config import load_config
from transitpulse.contracts.policy import Policy
from transitpulse.runs import RunConflictError, RunRecord, create_run, list_local_runs, load_run
from transitpulse.runtime.paths import WorkPaths

T0 = datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC)


class FakeStatement:
    def __init__(self, cql: str) -> None:
        self.cql = cql


class FakeResult:
    def __init__(self, row: dict | None, applied: bool | None = None) -> None:
        self._row = row
        self._applied = applied

    def one(self):
        return self._row

    @property
    def was_applied(self) -> bool:
        if self._applied is None:
            raise RuntimeError("not an LWT result")
        return self._applied


class FakeSession:
    """Records prepared CQL and executions; simulates INSERT IF NOT EXISTS on run_metadata."""

    def __init__(self) -> None:
        self.prepared: list[str] = []
        self.executed: list[tuple[str, tuple]] = []
        self.run_metadata: dict[str, dict] = {}

    def prepare(self, cql: str) -> FakeStatement:
        self.prepared.append(cql)
        return FakeStatement(cql)

    def execute(self, stmt: FakeStatement, params: tuple) -> FakeResult:
        self.executed.append((stmt.cql, params))
        if "IF NOT EXISTS" in stmt.cql and ".run_metadata" in stmt.cql:
            cols = stmt.cql.split("(", 1)[1].split(")", 1)[0].split(", ")
            row = dict(zip(cols, params, strict=True))
            existing = self.run_metadata.get(row["run_id"])
            if existing is not None:
                stored = dict(existing)
                stored["created_at"] = stored["created_at"].replace(tzinfo=None)  # driver returns naive UTC
                return FakeResult({"[applied]": False, **stored}, applied=False)
            self.run_metadata[row["run_id"]] = row
            return FakeResult({"[applied]": True}, applied=True)
        return FakeResult(None)


@pytest.fixture()
def cfg(tmp_path):
    return load_config("local-native", workdir=str(tmp_path), environ={})


def _create(cfg, session, **kw):
    base = dict(
        source_type="synthetic",
        route_ids=["R_A", "R_B"],
        gtfs_version="a" * 64,
        scope_id="0123456789abcdef",
        agency_timezone="America/New_York",
        static_zip_path="/w/data/static/x/MBTA_GTFS.zip",
        static_scope_dir="/w/data/static/x/0123456789abcdef",
        initial_clock=T0,
        scenario_id="golden",
        fixture_kind="golden_handwritten",
        seed=None,
        recording_id="fx-golden",
        session_id="s1",
        speed=10.0,
        label="golden smoke",
        provenance={"note": "synthetic"},
    )
    base.update(kw)
    return create_run(cfg, session, **base)


def test_create_run_writes_lwt_then_listing_then_run_json(cfg):
    session = FakeSession()
    rec = _create(cfg, session)
    assert len(session.executed) == 2
    first_cql, first_params = session.executed[0]
    second_cql, _ = session.executed[1]
    assert ".run_metadata" in first_cql and first_cql.endswith("IF NOT EXISTS")
    assert ".runs_by_project" in second_cql and "IF NOT EXISTS" not in second_cql
    row = session.run_metadata[rec.run_id]
    assert row["run_state"] == "CREATED"
    assert row["clock_sequence"] == 0
    assert row["application_clock"] == T0
    assert row["clock_mode"] == "source"
    assert row["policy_json"] == Policy.from_config(cfg).to_json()
    assert row["route_ids"] == ["R_A", "R_B"]
    paths = WorkPaths.from_workdir(cfg.workdir)
    assert paths.run_json(rec.run_id).is_file()
    assert not list(paths.run_dir(rec.run_id).glob(".*tmp*"))


def test_run_json_round_trip(cfg):
    rec = _create(cfg, FakeSession())
    paths = WorkPaths.from_workdir(cfg.workdir)
    loaded = load_run(paths, rec.run_id)
    assert loaded == rec
    assert RunRecord.from_json(json.loads(json.dumps(rec.to_json()))) == rec
    assert loaded.created_at.microsecond % 1000 == 0
    assert loaded.initial_clock == T0
    assert loaded.route_ids == ("R_A", "R_B")


def test_live_run_uses_wall_clock_and_requires_routes(cfg):
    session = FakeSession()
    rec = _create(cfg, session, source_type="live", initial_clock=None, scenario_id=None, fixture_kind=None,
                  speed=None)
    assert rec.clock_mode == "wall"
    assert rec.initial_clock is None
    assert session.run_metadata[rec.run_id]["application_clock"] is None
    assert session.run_metadata[rec.run_id]["clock_sequence"] == 0
    with pytest.raises(ValueError, match="route_ids"):
        _create(cfg, FakeSession(), source_type="live", route_ids=[], initial_clock=None)


def test_source_clock_run_requires_initial_clock(cfg):
    with pytest.raises(ValueError, match="initial_clock"):
        _create(cfg, FakeSession(), source_type="replay", initial_clock=None)
    with pytest.raises(ValueError):
        _create(cfg, FakeSession(), source_type="bogus")


def test_recreate_same_run_is_idempotent(cfg):
    session = FakeSession()
    first = _create(cfg, session, run_id="run-0001-abcdef")
    again = _create(cfg, session, run_id="run-0001-abcdef")
    assert again.created_at == first.created_at
    assert again.policy == first.policy
    assert len(session.run_metadata) == 1


def test_recreate_with_different_identity_conflicts(cfg):
    session = FakeSession()
    _create(cfg, session, run_id="run-0002-abcdef")
    with pytest.raises(RunConflictError) as info:
        _create(cfg, session, run_id="run-0002-abcdef", scope_id="fedcba9876543210")
    assert "scope_id" in info.value.differences


def test_list_local_runs_newest_first(cfg):
    session = FakeSession()
    a = _create(cfg, session)
    b = _create(cfg, session)
    paths = WorkPaths.from_workdir(cfg.workdir)
    # make the order unambiguous by rewriting created_at of the first run
    older = RunRecord.from_json({**a.to_json(), "created_at": (b.created_at - timedelta(minutes=5)).isoformat()})
    paths.run_json(a.run_id).write_text(json.dumps(older.to_json()), encoding="utf-8")
    (paths.root / "runs" / "broken").mkdir(parents=True)
    (paths.root / "runs" / "broken" / "run.json").write_text("{not json", encoding="utf-8")
    listed = list_local_runs(paths)
    assert [r.run_id for r in listed] == [b.run_id, a.run_id]
