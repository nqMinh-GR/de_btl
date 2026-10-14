"""Boundaries of the run-pinned policy reference classifiers (contracts/policy.py).

These pure functions are the reference that the Spark builders mirror (tests/spark asserts parity) and that
the API uses to recompute freshness / alert state at serving time. Values come from design C §4-§8 and
decisions D-BIZ-01, D-BIZ-04, D-BIZ-05, D-X-05.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from transitpulse.contracts.enums import AlertActiveState, FreshnessClass, UncertaintyClass
from transitpulse.contracts.policy import (
    Policy,
    alert_active_state,
    classify_uncertainty,
    feed_is_stale,
    vehicle_freshness,
)

CLOCK = datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC)
E = 1791201630  # epoch of CLOCK


@pytest.fixture()
def policy() -> Policy:
    return Policy(
        policy_version="policy-v1", vehicle_stale_seconds=120, prediction_stale_seconds=90, alert_stale_seconds=300,
        predicted_late_seconds=300, future_tolerance_seconds=5, service_date_inference_window_seconds=43200,
        uncertainty_policy="mbta_bus_uncertainty_v1", target_policy="first_future_resolved_stop_v1",
        timestamp_ms_threshold=100_000_000_000, current_generation_ttl_seconds=3600, history_ttl_seconds=259200,
        min_publish_ttl_seconds=60,
    )


def test_epoch_constant_matches_clock():
    assert int(CLOCK.timestamp()) == E


# --------------------------------------------------------------------------------------------------
# classify_uncertainty (mbta_bus_uncertainty_v1)
# --------------------------------------------------------------------------------------------------

@pytest.mark.parametrize(("value", "expected"), [
    (None, UncertaintyClass.UNKNOWN_UNCERTAINTY),   # absent is never treated as 0
    (0, UncertaintyClass.REALTIME),
    (60, UncertaintyClass.REALTIME),
    (299, UncertaintyClass.REALTIME),
    (300, UncertaintyClass.SCHEDULE_ONLY),
    (301, UncertaintyClass.DEGRADED),
    (302, UncertaintyClass.INVALID),
    (360, UncertaintyClass.INVALID),
    (-1, UncertaintyClass.INVALID),
    (30.5, UncertaintyClass.INVALID),
    (300.0, UncertaintyClass.INVALID),
    ("60", UncertaintyClass.INVALID),
    (True, UncertaintyClass.INVALID),
    (False, UncertaintyClass.INVALID),
])
def test_classify_uncertainty(value, expected):
    assert classify_uncertainty(value) is expected


def test_uncertainty_class_storage_values_are_lower_case():
    assert [str(c) for c in UncertaintyClass] == [
        "realtime", "unknown_uncertainty", "schedule_only", "degraded", "invalid",
    ]


# --------------------------------------------------------------------------------------------------
# vehicle_freshness
# --------------------------------------------------------------------------------------------------

def _fresh(policy, age_seconds: float | None, *, valid: bool = True, clock: datetime = CLOCK):
    obs = None if age_seconds is None else clock - timedelta(seconds=age_seconds)
    return vehicle_freshness(position_valid=valid, observation_time=obs, application_clock=clock, policy=policy)


@pytest.mark.parametrize(("age", "cls", "shown", "flags"), [
    (0, FreshnessClass.FRESH, 0.0, ()),
    (20, FreshnessClass.FRESH, 20.0, ()),            # golden V1 at S1
    (120, FreshnessClass.FRESH, 120.0, ()),          # boundary inclusive
    (120.000001, FreshnessClass.STALE, 120.000001, ()),
    (135, FreshnessClass.STALE, 135.0, ()),          # golden V1 at S5 (12:03:10Z)
    (210, FreshnessClass.STALE, 210.0, ()),          # golden V2 at S1
    (-0.5, FreshnessClass.FRESH, 0.0, ("CLOCK_SKEW",)),
    (-3, FreshnessClass.FRESH, 0.0, ("CLOCK_SKEW",)),  # IT06 V7
    (-5, FreshnessClass.FRESH, 0.0, ("CLOCK_SKEW",)),  # tolerance inclusive
    (-5.000001, FreshnessClass.INVALID, None, ("FUTURE_TIMESTAMP",)),
    (-10, FreshnessClass.INVALID, None, ("FUTURE_TIMESTAMP",)),   # IT06 V6
])
def test_vehicle_freshness_boundaries(policy, age, cls, shown, flags):
    r = _fresh(policy, age)
    assert r.freshness is cls
    assert r.age_seconds == (pytest.approx(shown) if shown is not None else None)
    assert r.flags == flags


def test_vehicle_freshness_unknown_and_invalid(policy):
    unknown = _fresh(policy, None)
    assert unknown.freshness is FreshnessClass.UNKNOWN and unknown.age_seconds is None and unknown.flags == ()
    invalid = _fresh(policy, 20, valid=False)
    assert invalid.freshness is FreshnessClass.INVALID and invalid.age_seconds is None
    invalid_unknown = _fresh(policy, None, valid=False)
    assert invalid_unknown.freshness is FreshnessClass.INVALID


def test_golden_s5_recompute_at_later_clock(policy):
    """Serving-time recompute: V1 observed 12:00:55Z is fresh at 12:01:00Z and stale at 12:03:10Z."""
    obs = datetime(2026, 10, 5, 12, 0, 55, tzinfo=UTC)
    at_s2 = vehicle_freshness(position_valid=True, observation_time=obs,
                              application_clock=datetime(2026, 10, 5, 12, 1, 0, tzinfo=UTC), policy=policy)
    at_s5 = vehicle_freshness(position_valid=True, observation_time=obs,
                              application_clock=datetime(2026, 10, 5, 12, 3, 10, tzinfo=UTC), policy=policy)
    assert (at_s2.freshness, at_s2.age_seconds) == (FreshnessClass.FRESH, 5.0)
    assert (at_s5.freshness, at_s5.age_seconds) == (FreshnessClass.STALE, 135.0)


def test_vehicle_freshness_respects_policy_values(policy):
    strict = Policy(**{**policy.__dict__, "vehicle_stale_seconds": 30, "future_tolerance_seconds": 0})
    assert _fresh(strict, 31).freshness is FreshnessClass.STALE
    assert _fresh(strict, 30).freshness is FreshnessClass.FRESH
    assert _fresh(strict, -0.001).freshness is FreshnessClass.INVALID


# --------------------------------------------------------------------------------------------------
# feed_is_stale
# --------------------------------------------------------------------------------------------------

@pytest.mark.parametrize(("feed", "limit"), [("vehicle_positions", 120), ("trip_updates", 90), ("alerts", 300)])
def test_feed_is_stale_thresholds(policy, feed, limit):
    assert feed_is_stale(feed, CLOCK - timedelta(seconds=limit), CLOCK, policy) is False
    assert feed_is_stale(feed, CLOCK - timedelta(seconds=limit, microseconds=1), CLOCK, policy) is True
    assert feed_is_stale(feed, CLOCK, CLOCK, policy) is False
    assert feed_is_stale(feed, None, CLOCK, policy) is True


def test_golden_trip_updates_stale_at_s5(policy):
    """D-X-05: TU1 header 12:00:20Z is 10 s old at S1 and 170 s old (> 90) at S5 12:03:10Z."""
    header = datetime(2026, 10, 5, 12, 0, 20, tzinfo=UTC)
    assert feed_is_stale("trip_updates", header, CLOCK, policy) is False
    assert feed_is_stale("trip_updates", header, datetime(2026, 10, 5, 12, 3, 10, tzinfo=UTC), policy) is True


def test_feed_is_stale_unknown_feed_type(policy):
    with pytest.raises(KeyError):
        feed_is_stale("control", CLOCK, CLOCK, policy)


# --------------------------------------------------------------------------------------------------
# alert_active_state
# --------------------------------------------------------------------------------------------------

@pytest.mark.parametrize(("periods", "expected"), [
    (None, AlertActiveState.ALWAYS),
    ([], AlertActiveState.ALWAYS),
    ([{"start": E - 10, "end": E + 10}], AlertActiveState.ACTIVE),
    ([{"start": E, "end": E + 10}], AlertActiveState.ACTIVE),          # start inclusive
    ([{"start": E - 10, "end": E}], AlertActiveState.EXPIRED),         # end exclusive
    ([{"start": E + 1, "end": E + 10}], AlertActiveState.UPCOMING),
    ([{"start": E + 1}], AlertActiveState.UPCOMING),
    ([{"end": E + 1}], AlertActiveState.ACTIVE),                      # missing start = -inf
    ([{"start": E - 1}], AlertActiveState.ACTIVE),                    # missing end = +inf
    ([{}], AlertActiveState.ACTIVE),                                  # both missing
    ([{"start": "abc", "end": E - 1}], AlertActiveState.EXPIRED),      # non-int start treated as missing
    ([{"start": E - 100, "end": E - 50}, {"start": E + 50, "end": E + 100}], AlertActiveState.UPCOMING),
    ([{"start": E - 100, "end": E - 50}, {"start": E - 10, "end": E + 10}], AlertActiveState.ACTIVE),
    ([{"start": E - 100, "end": E - 50}, {"start": E - 40, "end": E - 30}], AlertActiveState.EXPIRED),
])
def test_alert_active_state_boundaries(periods, expected):
    assert alert_active_state(periods, E) is expected


def test_golden_alerts1_states_at_s1():
    a1 = [{"start": 1791198000, "end": 1791205200}]
    a2 = [{"start": 1791212400, "end": 1791219600}]
    a4 = [{"start": 1791190800, "end": 1791198000}]
    a5 = [{"start": 1791198000}]
    assert alert_active_state(a1, E) is AlertActiveState.ACTIVE
    assert alert_active_state(a2, E) is AlertActiveState.UPCOMING
    assert alert_active_state(None, E) is AlertActiveState.ALWAYS          # A3, A8
    assert alert_active_state(a4, E) is AlertActiveState.EXPIRED
    assert alert_active_state(a5, E) is AlertActiveState.ACTIVE


# --------------------------------------------------------------------------------------------------
# Policy serialization
# --------------------------------------------------------------------------------------------------

def test_policy_json_round_trip(policy):
    assert Policy.from_json(policy.to_json()) == policy
    assert '"vehicle_stale_seconds": 120' in policy.to_json()


# --------------------------------------------------------------------------------------------------
# hand-written golden vectors (tests/fixtures/golden/expected.json, WP0b)
# --------------------------------------------------------------------------------------------------

def _expected(golden_dir):
    path = golden_dir / "expected.json"
    if not path.is_file():
        pytest.skip("tests/fixtures/golden/expected.json not materialized yet")
    return json.loads(path.read_text(encoding="utf-8"))


def test_expected_policy_block_matches_reference_policy(golden_dir, policy):
    p = _expected(golden_dir)["policy"]
    for key in ("vehicle_stale_seconds", "prediction_stale_seconds", "alert_stale_seconds", "predicted_late_seconds",
                "future_tolerance_seconds", "service_date_inference_window_seconds", "timestamp_ms_threshold",
                "uncertainty_policy", "target_policy"):
        assert getattr(policy, key) == p[key], key


def test_expected_policy_vectors(golden_dir, policy):
    vec = _expected(golden_dir)["policy_vectors"]
    for v in vec["uncertainty"]:
        assert str(classify_uncertainty(v["uncertainty"])) == v["class"], v
    for v in vec["freshness"]:
        clock = datetime.fromisoformat(v["clock"].replace("Z", "+00:00"))
        obs = None if v["observation_epoch"] is None else datetime.fromtimestamp(v["observation_epoch"], UTC)
        r = vehicle_freshness(position_valid=v["position_valid"], observation_time=obs, application_clock=clock,
                              policy=policy)
        e = v["expected"]
        assert (str(r.freshness), r.age_seconds, list(r.flags)) == (e["class"], e["age_s"], e["flags"]), v
    for v in vec["alert_state"]:
        assert str(alert_active_state(v["active_period"], v["clock_epoch"])) == v["state"], v
