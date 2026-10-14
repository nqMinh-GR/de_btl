"""Run-pinned policy and pure-Python reference classifiers.

The Spark implementation (processing/*) must produce exactly the same classes as these functions;
``tests/spark`` asserts parity. The API uses :func:`vehicle_freshness` and :func:`alert_active_state`
to re-evaluate freshness at request time (application clock), because stored Spark metrics are only
valid ``as of`` their generation.

These thresholds are project policy, not MBTA standards.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from transitpulse.config import ProjectConfig
from transitpulse.contracts.enums import AlertActiveState, FreshnessClass, UncertaintyClass


@dataclass(frozen=True)
class Policy:
    policy_version: str
    vehicle_stale_seconds: int
    prediction_stale_seconds: int
    alert_stale_seconds: int
    predicted_late_seconds: int
    future_tolerance_seconds: int
    service_date_inference_window_seconds: int
    uncertainty_policy: str
    target_policy: str
    timestamp_ms_threshold: int
    current_generation_ttl_seconds: int
    history_ttl_seconds: int
    min_publish_ttl_seconds: int

    @classmethod
    def from_config(cls, cfg: ProjectConfig) -> Policy:
        p, s = cfg.policy, cfg.storage
        return cls(
            policy_version=cfg.project.policy_version,
            vehicle_stale_seconds=p.vehicle_stale_seconds,
            prediction_stale_seconds=p.prediction_stale_seconds,
            alert_stale_seconds=p.alert_stale_seconds,
            predicted_late_seconds=p.predicted_late_seconds,
            future_tolerance_seconds=p.future_tolerance_seconds,
            service_date_inference_window_seconds=p.service_date_inference_window_seconds,
            uncertainty_policy=p.uncertainty_policy,
            target_policy=p.target_policy,
            timestamp_ms_threshold=p.timestamp_ms_threshold,
            current_generation_ttl_seconds=s.current_generation_ttl_seconds,
            history_ttl_seconds=s.history_ttl_seconds,
            min_publish_ttl_seconds=s.min_publish_ttl_seconds,
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def from_json(cls, s: str) -> Policy:
        return cls(**json.loads(s))


# --------------------------------------------------------------------------------------------------
# Reference classifiers (pure functions; mirrored by Spark expressions)
# --------------------------------------------------------------------------------------------------

def classify_uncertainty(value: Any) -> UncertaintyClass:
    """mbta_bus_uncertainty_v1 for route_type=3.

    absent -> UNKNOWN_UNCERTAINTY (never 0); 0 <= u < 300 -> REALTIME; 300 -> SCHEDULE_ONLY;
    301 -> DEGRADED; > 301, negative or non-integer -> INVALID.
    """
    if value is None:
        return UncertaintyClass.UNKNOWN_UNCERTAINTY
    if isinstance(value, bool) or not isinstance(value, int):
        return UncertaintyClass.INVALID
    if value < 0:
        return UncertaintyClass.INVALID
    if value < 300:
        return UncertaintyClass.REALTIME
    if value == 300:
        return UncertaintyClass.SCHEDULE_ONLY
    if value == 301:
        return UncertaintyClass.DEGRADED
    return UncertaintyClass.INVALID


@dataclass(frozen=True)
class FreshnessResult:
    freshness: FreshnessClass
    age_seconds: float | None      # displayed age; negative skew within tolerance is shown as 0
    flags: tuple[str, ...]         # e.g. ("CLOCK_SKEW",) or ("FUTURE_TIMESTAMP",)


def vehicle_freshness(
    *,
    position_valid: bool,
    observation_time: datetime | None,
    application_clock: datetime,
    policy: Policy,
) -> FreshnessResult:
    """Serving/processing freshness of one vehicle at ``application_clock``.

    invalid  : position not valid, or observation more than future_tolerance in the future
    unknown  : no source observation timestamp (snapshot_state_sample) -> age None
    fresh    : -future_tolerance <= age <= vehicle_stale_seconds (negative age -> CLOCK_SKEW, shown as 0)
    stale    : age > vehicle_stale_seconds
    """
    if not position_valid:
        return FreshnessResult(FreshnessClass.INVALID, None, ())
    if observation_time is None:
        return FreshnessResult(FreshnessClass.UNKNOWN, None, ())
    age = (application_clock - observation_time).total_seconds()
    if age < -policy.future_tolerance_seconds:
        return FreshnessResult(FreshnessClass.INVALID, None, ("FUTURE_TIMESTAMP",))
    if age < 0:
        return FreshnessResult(FreshnessClass.FRESH, 0.0, ("CLOCK_SKEW",))
    if age <= policy.vehicle_stale_seconds:
        return FreshnessResult(FreshnessClass.FRESH, age, ())
    return FreshnessResult(FreshnessClass.STALE, age, ())


def feed_is_stale(feed_type: str, feed_timestamp: datetime | None, application_clock: datetime, policy: Policy) -> bool:
    """Feed-level staleness: header age above the per-feed threshold (unknown header -> stale)."""
    if feed_timestamp is None:
        return True
    limit = {
        "vehicle_positions": policy.vehicle_stale_seconds,
        "trip_updates": policy.prediction_stale_seconds,
        "alerts": policy.alert_stale_seconds,
    }[feed_type]
    return (application_clock - feed_timestamp).total_seconds() > limit


def alert_active_state(active_periods: list[dict[str, Any]] | None, clock_epoch: int) -> AlertActiveState:
    """ALWAYS when no period; ACTIVE if any start<=c<end (missing start=-inf, end=+inf);
    else UPCOMING if any start>c; else EXPIRED."""
    if not active_periods:
        return AlertActiveState.ALWAYS
    upcoming = False
    for p in active_periods:
        start = p.get("start")
        end = p.get("end")
        lo = start if isinstance(start, int) else None
        hi = end if isinstance(end, int) else None
        if (lo is None or lo <= clock_epoch) and (hi is None or clock_epoch < hi):
            return AlertActiveState.ACTIVE
        if lo is not None and lo > clock_epoch:
            upcoming = True
    return AlertActiveState.UPCOMING if upcoming else AlertActiveState.EXPIRED
