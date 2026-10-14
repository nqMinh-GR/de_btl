"""Pure-Python trip-instance and stop resolution (reference for ``processing/schedule.py``; no Spark).

Trip instance (design C §3, decisions D-BIZ-03), first rule that applies:

1. ``ADDED``/``NEW`` -> ``not_applicable`` with NO resolution reason (the KPI/quality layers report
   ADDED_TRIP themselves; matches ``expected.json`` and the Spark resolver). Other unsupported relationships
   (``UNSCHEDULED``, ``DUPLICATED``, ``REPLACEMENT``, ``DELETED``) -> ``not_applicable`` + UNRESOLVED_TRIP.
2. trip_id not in the scoped static trips -> ``unresolved``, UNKNOWN_ROUTE_OR_TRIP.
3. descriptor ``route_id``/``direction_id`` contradicts static -> ``unresolved``, DESCRIPTOR_STATIC_MISMATCH.
   A static trip with ``schedule_valid=false`` (per-trip defect reported as a static warning) ->
   ``unresolved``, UNRESOLVED_TRIP: predictions stay visible, no delay, excluded from the KPI.
4. ``start_date`` present (preferred): unparsable -> DESCRIPTOR_STATIC_MISMATCH; service not active on it ->
   SERVICE_NOT_ACTIVE_ON_START_DATE (never fall back to guessing); ``start_time`` present and different from
   the trip's first departure -> START_TIME_MISMATCH; otherwise ``resolved`` / ``descriptor``.
5. ``start_date`` absent: candidates ``D in {L-1, L, L+1}`` (``L`` = local date of ``metrics_as_of``) where
   the service is active and ``metrics_as_of`` lies in
   ``[anchor(D) + first_departure - W, anchor(D) + last_arrival + W]`` (and ``start_time`` matches when
   given). One -> ``resolved``/``inferred`` (SERVICE_DATE_INFERRED); two or more -> ``ambiguous``
   (AMBIGUOUS_SERVICE_DATE); none -> ``unresolved`` (START_TIME_MISMATCH when only the start time failed,
   else UNRESOLVED_TRIP).

``CANCELED`` trips resolve normally (for audit) and carry ``cancelled=True``; they never get a target or a
delay. ``trip_instance_key = trip_id|<ISO service date>`` (e.g. ``T1|2026-10-05``, as the Spark resolver and
``expected.json``) when resolved, else ``trip_id|?<start_date or none>`` (start_date kept as sent, yyyyMMdd).

Stop join (independent of the service date): ``stop_sequence`` present -> ``(trip_id, stop_sequence)``;
a differing ``stop_id`` gives STOP_ID_SEQUENCE_MISMATCH. Else ``stop_id`` present -> allowed only when the
stop occurs exactly once in the trip (repeated -> UNRESOLVED_STOP_SEQUENCE, absent -> UNKNOWN_STOP_FOR_TRIP).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from transitpulse.contracts.enums import ResolutionMethod, ScheduleResolution
from transitpulse.static.calendar import ServiceCalendar
from transitpulse.static.gtfs_time import (
    local_date,
    parse_gtfs_time,
    parse_service_date,
    service_anchor_utc,
)

__all__ = [
    "ADDED_RELATIONSHIPS",
    "UNSUPPORTED_RELATIONSHIPS",
    "CANCELED_RELATIONSHIPS",
    "DEFAULT_INFERENCE_WINDOW_SECONDS",
    "StaticTrip",
    "TripResolution",
    "StopResolution",
    "trip_instance_key",
    "candidate_service_dates",
    "resolve_trip_instance",
    "resolve_stop",
    "scheduled_event_utc",
]

ADDED_RELATIONSHIPS: tuple[str, ...] = ("ADDED", "NEW")
UNSUPPORTED_RELATIONSHIPS: tuple[str, ...] = ("UNSCHEDULED", "DUPLICATED", "REPLACEMENT", "DELETED")
CANCELED_RELATIONSHIPS: tuple[str, ...] = ("CANCELED", "CANCELLED")
DEFAULT_INFERENCE_WINDOW_SECONDS = 43_200


@dataclass(frozen=True)
class StaticTrip:
    """The static facts of one scoped trip needed for resolution (a ``dim_trips`` row)."""

    trip_id: str
    route_id: str
    service_id: str
    direction_id: int | None = None
    first_departure_secs: int | None = None
    last_arrival_secs: int | None = None
    schedule_valid: bool = True


@dataclass(frozen=True)
class TripResolution:
    """Outcome of :func:`resolve_trip_instance`."""

    schedule_resolution: str                 # contracts.enums.ScheduleResolution value
    method: str | None                       # descriptor | inferred | None
    service_date: date | None
    reason: str | None                       # quality reason code (warning/info) or None
    trip_instance_key: str
    cancelled: bool = False
    candidates: tuple[date, ...] | None = None   # only set when the service date was inferred (no start_date)

    @property
    def resolved(self) -> bool:
        return self.schedule_resolution == ScheduleResolution.RESOLVED.value


@dataclass(frozen=True)
class StopResolution:
    """Outcome of :func:`resolve_stop`: the static stop occurrence, or a reason."""

    resolved: bool
    stop_sequence: int | None
    stop_id: str | None
    reason: str | None


def trip_instance_key(trip_id: str, service_date: date | None, start_date: str | None = None) -> str:
    """``trip_id|YYYY-MM-DD`` (ISO service date) when resolved, else ``trip_id|?<start_date or none>``."""
    if service_date is not None:
        return f"{trip_id}|{service_date.isoformat()}"
    return f"{trip_id}|?{start_date if start_date else 'none'}"


def _start_time_matches(start_time: str | None, trip: StaticTrip) -> bool:
    """A present start_time must equal the trip's first departure (unparsable never matches)."""
    if start_time is None or str(start_time).strip() == "":
        return True
    try:
        secs = parse_gtfs_time(start_time)
    except ValueError:
        return False
    return secs is not None and trip.first_departure_secs is not None and secs == trip.first_departure_secs


def candidate_service_dates(
    trip: StaticTrip,
    calendar: ServiceCalendar,
    metrics_as_of: datetime,
    agency_timezone: str,
    *,
    window_seconds: int = DEFAULT_INFERENCE_WINDOW_SECONDS,
) -> list[date]:
    """Active service dates in ``{L-1, L, L+1}`` whose widened trip window contains ``metrics_as_of``.

    The start_time filter is NOT applied here (see :func:`resolve_trip_instance`).
    """
    if trip.first_departure_secs is None or trip.last_arrival_secs is None:
        return []
    clock = metrics_as_of
    if clock.tzinfo is None:
        raise ValueError("metrics_as_of must be timezone-aware")
    base = local_date(clock, agency_timezone)
    window = timedelta(seconds=window_seconds)
    out: list[date] = []
    for offset in (-1, 0, 1):
        d = base + timedelta(days=offset)
        if not calendar.is_active(trip.service_id, d):
            continue
        anchor = service_anchor_utc(d, agency_timezone)
        lo = anchor + timedelta(seconds=trip.first_departure_secs) - window
        hi = anchor + timedelta(seconds=trip.last_arrival_secs) + window
        if lo <= clock <= hi:
            out.append(d)
    return out


def resolve_trip_instance(
    *,
    trip_id: str,
    route_id: str | None,
    direction_id: int | None,
    start_date: str | None,
    start_time: str | None,
    schedule_relationship: str | None,
    static_trip: StaticTrip | None,
    calendar: ServiceCalendar,
    metrics_as_of: datetime,
    agency_timezone: str,
    window_seconds: int = DEFAULT_INFERENCE_WINDOW_SECONDS,
) -> TripResolution:
    """Resolve one TripUpdate's trip descriptor to a scheduled trip instance (rules in the module doc)."""
    rel = (schedule_relationship or "SCHEDULED").strip().upper()
    start_date = start_date.strip() if isinstance(start_date, str) and start_date.strip() else None
    unresolved_key = trip_instance_key(trip_id, None, start_date)
    na = ScheduleResolution.NOT_APPLICABLE.value
    unres = ScheduleResolution.UNRESOLVED.value

    if rel in ADDED_RELATIONSHIPS:
        # no resolution reason: ADDED_TRIP is reported separately by the KPI/quality layers (expected.json)
        return TripResolution(na, None, None, None, unresolved_key)
    if rel in UNSUPPORTED_RELATIONSHIPS:
        return TripResolution(na, None, None, "UNRESOLVED_TRIP", unresolved_key)
    cancelled = rel in CANCELED_RELATIONSHIPS

    if static_trip is None:
        return TripResolution(unres, None, None, "UNKNOWN_ROUTE_OR_TRIP", unresolved_key, cancelled)
    if (route_id is not None and route_id != static_trip.route_id) or (
        direction_id is not None and static_trip.direction_id is not None and direction_id != static_trip.direction_id
    ):
        return TripResolution(unres, None, None, "DESCRIPTOR_STATIC_MISMATCH", unresolved_key, cancelled)
    if not static_trip.schedule_valid:
        # static schedule defect (warning in the static report): never compute a delay from it
        return TripResolution(unres, None, None, "UNRESOLVED_TRIP", unresolved_key, cancelled)

    if start_date is not None:
        try:
            d = parse_service_date(start_date)
        except ValueError:
            return TripResolution(unres, None, None, "DESCRIPTOR_STATIC_MISMATCH", unresolved_key, cancelled)
        if not calendar.is_active(static_trip.service_id, d):
            return TripResolution(unres, None, None, "SERVICE_NOT_ACTIVE_ON_START_DATE", unresolved_key, cancelled)
        if not _start_time_matches(start_time, static_trip):
            return TripResolution(unres, None, None, "START_TIME_MISMATCH", unresolved_key, cancelled)
        return TripResolution(
            ScheduleResolution.RESOLVED.value, ResolutionMethod.DESCRIPTOR.value, d, None,
            trip_instance_key(trip_id, d), cancelled,
        )

    by_window = candidate_service_dates(
        static_trip, calendar, metrics_as_of, agency_timezone, window_seconds=window_seconds
    )
    matching = [d for d in by_window if _start_time_matches(start_time, static_trip)]
    if len(matching) == 1:
        d = matching[0]
        return TripResolution(
            ScheduleResolution.RESOLVED.value, ResolutionMethod.INFERRED.value, d, "SERVICE_DATE_INFERRED",
            trip_instance_key(trip_id, d), cancelled, tuple(matching),
        )
    if len(matching) >= 2:
        return TripResolution(
            ScheduleResolution.AMBIGUOUS.value, None, None, "AMBIGUOUS_SERVICE_DATE", unresolved_key, cancelled,
            tuple(matching),
        )
    reason = "START_TIME_MISMATCH" if by_window else "UNRESOLVED_TRIP"
    return TripResolution(unres, None, None, reason, unresolved_key, cancelled, ())


def resolve_stop(
    *,
    stop_sequence: int | None,
    stop_id: str | None,
    trip_stops: Iterable[tuple[int, str]],
) -> StopResolution:
    """Join a stop-time update to the static stop occurrence ``(stop_sequence, stop_id)`` of its trip."""
    stops: Sequence[tuple[int, str]] = list(trip_stops)
    if stop_sequence is not None:
        match = [s for s in stops if s[0] == stop_sequence]
        if not match:
            return StopResolution(False, None, stop_id, "UNKNOWN_STOP_FOR_TRIP")
        seq, sid = match[0]
        if stop_id is not None and stop_id != sid:
            return StopResolution(False, None, stop_id, "STOP_ID_SEQUENCE_MISMATCH")
        return StopResolution(True, seq, sid, None)
    if stop_id is not None:
        match = [s for s in stops if s[1] == stop_id]
        if not match:
            return StopResolution(False, None, stop_id, "UNKNOWN_STOP_FOR_TRIP")
        if len(match) > 1:
            return StopResolution(False, None, stop_id, "UNRESOLVED_STOP_SEQUENCE")
        return StopResolution(True, match[0][0], stop_id, None)
    return StopResolution(False, None, None, "UNRESOLVED_STOP_SEQUENCE")


def scheduled_event_utc(service_date: date, gtfs_seconds: int | None, agency_timezone: str) -> datetime | None:
    """Scheduled UTC instant of a same-type event, or ``None`` when the static seconds are null."""
    if gtfs_seconds is None:
        return None
    return service_anchor_utc(service_date, agency_timezone) + timedelta(seconds=gtfs_seconds)
