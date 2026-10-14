"""Control-event folding (pure Python; no Spark/Cassandra needed)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from transitpulse.processing import control_clock as cc
from transitpulse.processing.control_clock import ClockEvent, ClockState, fold_clock_events

T0 = datetime(2026, 10, 5, 12, 0, 0, tzinfo=UTC)


def ev(kind: str, seq: int, secs: int | None, state: str | None = None, offset: int | None = None) -> ClockEvent:
    return ClockEvent(kind, seq, None if secs is None else T0 + timedelta(seconds=secs), state, None, None, offset)


def test_source_fold_applies_increasing_sequences_and_ignores_regressions():
    start = ClockState(0, T0, "CREATED")
    res = fold_clock_events(start, [
        ev("clock_tick", 3, 190, "RUNNING"),
        ev("run_started", 1, 30),
        ev("clock_tick", 2, 60, "RUNNING"),
        ev("clock_tick", 4, 100, "RUNNING"),        # clock regression -> ignored
        ev("clock_tick", 3, 200, "RUNNING"),        # stale sequence -> ignored
    ], clock_mode="source")
    assert res.changed
    assert res.state.clock_sequence == 3 and res.state.application_clock == T0 + timedelta(seconds=190)
    assert res.state.run_state == "RUNNING"
    reasons = sorted(r for _, r in res.ignored)
    assert reasons == ["clock_regression_ignored", "stale_sequence"]


def test_pause_freezes_state_and_resume_continues():
    s = ClockState(5, T0, "RUNNING")
    paused = fold_clock_events(s, [ev("run_paused", 6, 0), ev("clock_tick", 7, 0, "PAUSED")], clock_mode="source")
    assert paused.state.run_state == "PAUSED" and paused.state.application_clock == T0
    resumed = fold_clock_events(paused.state, [ev("run_resumed", 8, 0), ev("clock_tick", 9, 10, "RUNNING")],
                                clock_mode="source")
    assert resumed.state.run_state == "RUNNING" and resumed.state.clock_sequence == 9


def test_finished_is_terminal():
    s = ClockState(5, T0, "RUNNING")
    fin = fold_clock_events(s, [ev("run_finished", 6, 10), ev("clock_tick", 7, 20, "RUNNING")], clock_mode="source")
    assert fin.state.run_state == "FINISHED" and fin.state.clock_sequence == 6
    assert [r for _, r in fin.ignored] == ["invalid_transition:FINISHED->RUNNING"]


def test_failed_run_can_restart_and_finish_failed():
    s = ClockState(5, T0, "FAILED")
    res = fold_clock_events(s, [ev("run_started", 6, 0)], clock_mode="source")
    assert res.state.run_state == "RUNNING"
    res2 = fold_clock_events(res.state, [ev("run_finished", 7, 5, "FAILED")], clock_mode="source")
    assert res2.state.run_state == "FAILED"


def test_bare_tick_on_created_run_means_running():
    res = fold_clock_events(ClockState(0, T0, "CREATED"), [ev("clock_tick", 1, 5)], clock_mode="source")
    assert res.state.run_state == "RUNNING"


def test_wall_mode_ignores_clock_values():
    s = ClockState(0, None, "CREATED")
    res = fold_clock_events(s, [ev("run_started", 1, None)], clock_mode="wall")
    assert res.state == ClockState(1, None, "RUNNING", "run_started")


def test_no_change_when_everything_is_stale():
    s = ClockState(9, T0, "RUNNING")
    res = fold_clock_events(s, [ev("clock_tick", 9, 10, "RUNNING"), ev("clock_tick", 8, 20)], clock_mode="source")
    assert not res.changed and res.state == s


def test_events_from_rows_and_status_rows():
    rows = [{"event_type": "clock_tick", "event_id": "e", "clock_sequence": 3, "source_clock_ms": 1791201630000,
             "run_state": "RUNNING", "reason": None, "kafka_offset": 1}]
    (e,) = cc.events_from_rows(rows)
    assert e.source_clock == datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC) and e.clock_sequence == 3
    st = cc.status_rows_from_events("run-1", [
        {"event_type": "collector_status", "feed_type": "alerts", "outcome": "rejected", "reason_code": "INVALID_JSON",
         "feed_state": "degraded", "emitted_at_ms": 1791201630000, "last_success_fetched_at_ms": None,
         "progress_text": "{}"},
        {"event_type": "publisher_heartbeat", "feed_type": None, "run_state": "PAUSED", "emitted_at_ms": 1791201631000,
         "progress_text": None},
        {"event_type": "fault_armed", "feed_type": None},
    ])
    assert [r["component"] for r in st] == ["collector:alerts", "publisher"]
    assert st[0]["status"] == "degraded" and st[0]["last_rejected_reason"] == "INVALID_JSON"
    assert st[1]["status"] == "PAUSED" and st[1]["progress_json"]


def _status(outcome, reason=None):
    return {"event_type": "collector_status", "feed_type": "vehicle_positions", "outcome": outcome,
            "reason_code": reason, "feed_state": "healthy", "emitted_at_ms": 1791201630000,
            "last_success_fetched_at_ms": None, "progress_text": "{}"}


def test_collector_failure_outcomes_and_success_clearing():
    from cassandra.query import UNSET_VALUE

    for outcome in ("http_error", "connect_error", "too_large", "timeout", "collector_error",
                    "disk_budget_exceeded"):
        (row,) = cc.status_rows_from_events("run-1", [_status(outcome)])
        assert row["last_error"] == outcome and cc.CLEAR_KEY not in row
    (row,) = cc.status_rows_from_events("run-1", [_status("http_error", "SOURCE_FETCH_FAILED")])
    assert row["last_error"] == "SOURCE_FETCH_FAILED"
    (warn,) = cc.status_rows_from_events("run-1", [_status("disk_budget_warning", "DISK_BUDGET_WARNING")])
    assert warn["last_error"] is None and cc.CLEAR_KEY not in warn
    (ok,) = cc.status_rows_from_events("run-1", [_status("ok")])
    assert ok["last_success_at"] == datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC)
    params = dict(zip(cc.STATUS_UPSERT_COLUMNS, cc.status_params(ok), strict=True))
    assert params["last_error"] is None and params["last_rejected_at"] is None \
        and params["last_rejected_reason"] is None                      # explicit NULLs clear stale values
    (failed,) = cc.status_rows_from_events("run-1", [_status("timeout")])
    fparams = dict(zip(cc.STATUS_UPSERT_COLUMNS, cc.status_params(failed), strict=True))
    assert fparams["last_rejected_at"] is UNSET_VALUE and fparams["last_error"] == "timeout"


def test_rejected_body_records_rejection_and_clears_stale_fetch_error():
    from cassandra.query import UNSET_VALUE

    (row,) = cc.status_rows_from_events("run-1", [_status("rejected", "INVALID_JSON")])
    assert row["last_rejected_reason"] == "INVALID_JSON" and row["last_rejected_at"] is not None
    params = dict(zip(cc.STATUS_UPSERT_COLUMNS, cc.status_params(row), strict=True))
    assert params["last_error"] is None                                  # explicit NULL, not UNSET
    assert params["last_rejected_reason"] == "INVALID_JSON"
    assert params["last_success_at"] is UNSET_VALUE


def test_upsert_status_refuses_lwt_components():
    import pytest

    with pytest.raises(ValueError):
        cc.upsert_status(object(), {"run_id": "r", "component": "processor"})
    with pytest.raises(ValueError):
        cc.upsert_status(object(), {"run_id": "r", "component": "feed:alerts"})
