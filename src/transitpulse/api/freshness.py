"""Serving-time vehicle freshness (spec md §11.1, PDF p17, D-BIZ-04, D-PUB-07).

Stored Spark metrics are only valid as of their generation's ``metrics_as_of``; the API re-evaluates
every current-generation vehicle row at the request's application clock with the run-pinned
:class:`~transitpulse.contracts.policy.Policy` and the shared reference classifier
:func:`~transitpulse.contracts.policy.vehicle_freshness`:

* a row with a non-null ``invalid_reason`` stays ``invalid`` (decision frozen at processing time);
* a row whose position validity is not ``true`` is ``invalid``;
* a row without a source observation time (``snapshot_state_sample`` / ``event_time_origin =
  feed_header`` / ``freshness_known = false``) is ``unknown`` with age ``null`` — the feed header is
  never used as a fake observation time;
* otherwise ``fresh`` (``-future_tolerance <= age <= vehicle_stale_seconds``, negative age shown as 0
  with ``CLOCK_SKEW``), ``stale`` above the threshold, ``invalid`` (``FUTURE_TIMESTAMP``) beyond the
  future tolerance.

Counts: fresh/stale/unknown-age/invalid count rows (entities); ``snapshot_membership_count`` counts
distinct vehicle IDs (D-BIZ-07) — the classes are not forced to sum to membership.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from transitpulse.contracts.enums import EventTimeOrigin, FreshnessClass, RecordKind
from transitpulse.contracts.policy import Policy, vehicle_freshness

CLOCK_SKEW = "CLOCK_SKEW"
FUTURE_TIMESTAMP = "FUTURE_TIMESTAMP"
INVALID_COORDINATES = "INVALID_COORDINATES"


@dataclass(frozen=True)
class VehicleFreshness:
    status: FreshnessClass
    age_seconds: float | None
    flags: tuple[str, ...]
    invalid_reason: str | None


def observation_time_of(row: Mapping[str, Any]) -> datetime | None:
    """Source observation time of a vehicle row, or ``None`` when the source gave none."""
    if row.get("freshness_known") is False:
        return None
    if row.get("event_time_origin") == EventTimeOrigin.FEED_HEADER.value:
        return None
    if row.get("record_kind") == RecordKind.SNAPSHOT_STATE_SAMPLE.value:
        return None
    value = row.get("observation_time")
    return value if isinstance(value, datetime) else None


def evaluate_vehicle(row: Mapping[str, Any], application_clock: datetime, policy: Policy) -> VehicleFreshness:
    """Freshness of one ``vehicles_by_route_snapshot`` row at ``application_clock``."""
    frozen_reason = row.get("invalid_reason")
    if frozen_reason:
        return VehicleFreshness(FreshnessClass.INVALID, None, (), str(frozen_reason))
    result = vehicle_freshness(
        position_valid=row.get("position_valid") is True,
        observation_time=observation_time_of(row),
        application_clock=application_clock,
        policy=policy,
    )
    reason: str | None = None
    if result.freshness == FreshnessClass.INVALID:
        reason = FUTURE_TIMESTAMP if FUTURE_TIMESTAMP in result.flags else INVALID_COORDINATES
    age = round(result.age_seconds, 3) if result.age_seconds is not None else None
    return VehicleFreshness(result.freshness, age, tuple(result.flags), reason)


@dataclass(frozen=True)
class VehicleCountSummary:
    fresh_vehicle_count: int
    stale_vehicle_count: int
    unknown_age_vehicle_count: int
    invalid_vehicle_count: int
    invalid_without_vehicle_id_count: int
    snapshot_membership_count: int
    inherited_vehicle_count: int
    clock_skew_vehicle_count: int

    def as_dict(self) -> dict[str, int]:
        return {
            "fresh_vehicle_count": self.fresh_vehicle_count,
            "stale_vehicle_count": self.stale_vehicle_count,
            "unknown_age_vehicle_count": self.unknown_age_vehicle_count,
            "invalid_vehicle_count": self.invalid_vehicle_count,
            "invalid_without_vehicle_id_count": self.invalid_without_vehicle_id_count,
            "snapshot_membership_count": self.snapshot_membership_count,
            "inherited_vehicle_count": self.inherited_vehicle_count,
            "clock_skew_vehicle_count": self.clock_skew_vehicle_count,
        }


def stored_invalid_row_count(rows: Iterable[Mapping[str, Any]]) -> int:
    """Rows that were invalid at processing time (frozen ``invalid_reason`` or position not valid)."""
    return sum(1 for r in rows if r.get("invalid_reason") or r.get("position_valid") is not True)


def invalid_without_vehicle_id(stored_invalid_vehicle_count: Any, rows: Iterable[Mapping[str, Any]]) -> int:
    """Invalid entities of a route that have no ``vehicles_by_route_snapshot`` row (MISSING_VEHICLE_ID).

    ``invalid_vehicle_count`` of the route's ``route_metrics_by_snapshot`` row (same generation) counts every
    invalid entity, rows without a vehicle id included; the rows that do exist are subtracted. Entities
    without an id are invalid whatever the clock, so this part is carried over unchanged.
    """
    if not isinstance(stored_invalid_vehicle_count, int) or isinstance(stored_invalid_vehicle_count, bool):
        return 0
    return max(0, stored_invalid_vehicle_count - stored_invalid_row_count(rows))


def count_vehicles(
    evaluated: Iterable[tuple[Mapping[str, Any], VehicleFreshness]], *, invalid_without_id: int = 0
) -> VehicleCountSummary:
    """Serving-time counts over evaluated rows plus ``invalid_without_id`` row-less invalid entities."""
    by_status = {cls: 0 for cls in FreshnessClass}
    members: set[str] = set()
    inherited = 0
    skew = 0
    for row, state in evaluated:
        by_status[state.status] += 1
        vehicle_id = row.get("vehicle_id")
        if vehicle_id is not None:
            members.add(str(vehicle_id))
        if row.get("is_inherited") is True:
            inherited += 1
        if CLOCK_SKEW in state.flags:
            skew += 1
    return VehicleCountSummary(
        fresh_vehicle_count=by_status[FreshnessClass.FRESH],
        stale_vehicle_count=by_status[FreshnessClass.STALE],
        unknown_age_vehicle_count=by_status[FreshnessClass.UNKNOWN],
        invalid_vehicle_count=by_status[FreshnessClass.INVALID] + max(0, invalid_without_id),
        invalid_without_vehicle_id_count=max(0, invalid_without_id),
        snapshot_membership_count=len(members),
        inherited_vehicle_count=inherited,
        clock_skew_vehicle_count=skew,
    )


TIME_RELATIVE_FLAGS = frozenset({CLOCK_SKEW})
"""Flags that depend on the evaluation clock: the stored value (as of ``metrics_as_of``) is replaced by the
serving-time evaluation. Frozen flags (INVALID_COORDINATES, FUTURE_TIMESTAMP, ...) are kept."""


def merged_flags(stored: Any, computed: Iterable[str]) -> list[str]:
    """Stored frozen quality flags plus serving-time flags, de-duplicated and sorted."""
    out: set[str] = set()
    if stored:
        out.update(str(f) for f in stored if str(f) not in TIME_RELATIVE_FLAGS)
    out.update(computed)
    return sorted(out)
