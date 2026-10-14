"""Trip-instance candidate dates and stop resolution (design C §3, IT07 unit vector, D-BIZ-03)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from transitpulse.static.calendar import ServiceCalendar
from transitpulse.static.gtfs_time import parse_gtfs_time
from transitpulse.static.schedule import (
    StaticTrip,
    candidate_service_dates,
    resolve_stop,
    resolve_trip_instance,
    scheduled_event_utc,
    trip_instance_key,
)
from transitpulse.static.validation import read_csv_text

NY = "America/New_York"
CLOCK_S1 = datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC)   # golden S1 clock

_CAL = read_csv_text(
    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
    "WKDY,1,1,1,1,1,0,0,20260901,20261231\n"
    "DAILY,1,1,1,1,1,1,1,20260901,20261231\n"
)[1]
_CD = read_csv_text("service_id,date,exception_type\nWKDY,20261012,2\nWKDY,20261010,1\n")[1]


def _secs(t: str) -> int:
    v = parse_gtfs_time(t)
    assert v is not None
    return v


@pytest.fixture(scope="module")
def calendar() -> ServiceCalendar:
    return ServiceCalendar.from_rows(_CAL, _CD)


T1 = StaticTrip("T1", "R_A", "WKDY", 0, _secs("07:55:00"), _secs("08:10:00"))
T2 = StaticTrip("T2", "R_A", "WKDY", 1, _secs("07:58:00"), _secs("08:12:00"))
T4 = StaticTrip("T4", "R_A", "DAILY", 0, _secs("19:50:00"), _secs("25:10:00"))


def _resolve(trip: StaticTrip | None, calendar: ServiceCalendar, *, trip_id: str | None = None,
             start_date: str | None = None, start_time: str | None = None, rel: str | None = None,
             route_id: str | None = None, direction_id: int | None = None, clock: datetime = CLOCK_S1):
    return resolve_trip_instance(
        trip_id=trip_id or (trip.trip_id if trip else "UNKNOWN"),
        route_id=route_id, direction_id=direction_id, start_date=start_date, start_time=start_time,
        schedule_relationship=rel, static_trip=trip, calendar=calendar, metrics_as_of=clock,
        agency_timezone=NY, window_seconds=43200,
    )


def test_descriptor_resolution(calendar: ServiceCalendar) -> None:
    res = _resolve(T1, calendar, start_date="20261005", start_time="07:55:00", route_id="R_A", direction_id=0)
    assert (res.schedule_resolution, res.method, res.service_date, res.reason) == (
        "resolved", "descriptor", date(2026, 10, 5), None)
    assert res.trip_instance_key == "T1|2026-10-05"
    assert res.resolved
    assert res.candidates is None          # candidates only exist for an inferred service date


def test_added_exception_date_resolves(calendar: ServiceCalendar) -> None:
    res = _resolve(T1, calendar, start_date="20261010")
    assert res.schedule_resolution == "resolved" and res.service_date == date(2026, 10, 10)


@pytest.mark.parametrize("start_date", ["20261011", "20261012", "20270104"])
def test_service_not_active_on_start_date_is_never_guessed(calendar: ServiceCalendar, start_date: str) -> None:
    res = _resolve(T1, calendar, start_date=start_date)
    assert res.schedule_resolution == "unresolved"
    assert res.reason == "SERVICE_NOT_ACTIVE_ON_START_DATE"
    assert res.service_date is None
    assert res.trip_instance_key == f"T1|?{start_date}"


def test_start_time_mismatch_with_descriptor(calendar: ServiceCalendar) -> None:
    res = _resolve(T1, calendar, start_date="20261005", start_time="08:10:00")
    assert (res.schedule_resolution, res.reason) == ("unresolved", "START_TIME_MISMATCH")


def test_unparsable_start_date(calendar: ServiceCalendar) -> None:
    res = _resolve(T1, calendar, start_date="2026-10-05")
    assert (res.schedule_resolution, res.reason) == ("unresolved", "DESCRIPTOR_STATIC_MISMATCH")


def test_it07_overnight_trip_without_start_date_is_ambiguous(calendar: ServiceCalendar) -> None:
    """T4 (19:50 -> 25:10, DAILY) at 12:00:30Z: Oct 4 window [Oct 4 11:50Z, Oct 5 17:10Z] and Oct 5 both match."""
    assert candidate_service_dates(T4, calendar, CLOCK_S1, NY) == [date(2026, 10, 4), date(2026, 10, 5)]
    res = _resolve(T4, calendar)
    assert res.schedule_resolution == "ambiguous"
    assert res.reason == "AMBIGUOUS_SERVICE_DATE"
    assert res.service_date is None
    assert res.candidates == (date(2026, 10, 4), date(2026, 10, 5))
    assert res.trip_instance_key == "T4|?none"


def test_it07_unit_vector_inferred_service_date(calendar: ServiceCalendar) -> None:
    """The same T4 TripUpdate at 2026-10-06T03:00Z resolves (inferred) to service date 2026-10-05."""
    clock = datetime(2026, 10, 6, 3, 0, tzinfo=UTC)
    res = _resolve(T4, calendar, clock=clock)
    assert (res.schedule_resolution, res.method, res.service_date, res.reason) == (
        "resolved", "inferred", date(2026, 10, 5), "SERVICE_DATE_INFERRED")
    assert res.trip_instance_key == "T4|2026-10-05"


def test_single_candidate_weekday_trip(calendar: ServiceCalendar) -> None:
    # L = Oct 5 (Mon). Oct 4 is a Sunday (WKDY inactive); Oct 6 window starts Oct 5 23:55Z.
    res = _resolve(T1, calendar)
    assert (res.schedule_resolution, res.method, res.service_date) == ("resolved", "inferred", date(2026, 10, 5))


def test_no_candidate_is_unresolved(calendar: ServiceCalendar) -> None:
    clock = datetime(2026, 10, 11, 15, 0, tzinfo=UTC)      # Sunday 11:00 local; Oct 12 removed
    assert candidate_service_dates(T1, calendar, clock, NY) == []
    res = _resolve(T1, calendar, clock=clock)
    assert (res.schedule_resolution, res.reason) == ("unresolved", "UNRESOLVED_TRIP")


def test_start_time_filters_inferred_candidates(calendar: ServiceCalendar) -> None:
    res = _resolve(T1, calendar, start_time="09:00:00")
    assert (res.schedule_resolution, res.reason) == ("unresolved", "START_TIME_MISMATCH")
    ok = _resolve(T1, calendar, start_time="07:55:00")
    assert ok.schedule_resolution == "resolved" and ok.method == "inferred"


def test_relationships_and_unknown_trips(calendar: ServiceCalendar) -> None:
    added = _resolve(None, calendar, trip_id="ADDED_1", rel="ADDED", route_id="R_B")
    assert (added.schedule_resolution, added.reason, added.candidates) == ("not_applicable", None, None)
    dup = _resolve(T1, calendar, rel="DUPLICATED", start_date="20261005")
    assert (dup.schedule_resolution, dup.reason) == ("not_applicable", "UNRESOLVED_TRIP")
    unknown = _resolve(None, calendar, trip_id="NOPE", start_date="20261005")
    assert (unknown.schedule_resolution, unknown.reason) == ("unresolved", "UNKNOWN_ROUTE_OR_TRIP")
    cancelled = _resolve(T1, calendar, rel="CANCELED", start_date="20261005")
    assert cancelled.schedule_resolution == "resolved" and cancelled.cancelled is True


@pytest.mark.parametrize(("route_id", "direction_id"), [("R_B", None), (None, 1), ("R_B", 1)])
def test_descriptor_contradicting_static(calendar: ServiceCalendar, route_id, direction_id) -> None:
    res = _resolve(T1, calendar, start_date="20261005", route_id=route_id, direction_id=direction_id)
    assert (res.schedule_resolution, res.reason) == ("unresolved", "DESCRIPTOR_STATIC_MISMATCH")


def test_schedule_invalid_trip_is_never_resolved(calendar: ServiceCalendar) -> None:
    """A trip kept with schedule_valid=false (static warning) yields no schedule, hence no delay."""
    bad = StaticTrip("T8", "R_A", "WKDY", 0, _secs("09:00:00"), _secs("09:10:00"), schedule_valid=False)
    with_date = _resolve(bad, calendar, start_date="20261005")
    assert (with_date.schedule_resolution, with_date.reason, with_date.service_date) == (
        "unresolved", "UNRESOLVED_TRIP", None)
    assert with_date.trip_instance_key == "T8|?20261005"
    inferred = _resolve(bad, calendar)
    assert (inferred.schedule_resolution, inferred.reason) == ("unresolved", "UNRESOLVED_TRIP")
    added = _resolve(bad, calendar, rel="ADDED")
    assert added.schedule_resolution == "not_applicable"


def test_trip_instance_key_format() -> None:
    assert trip_instance_key("T1", date(2026, 10, 5)) == "T1|2026-10-05"     # ISO service date (lead decision)
    assert trip_instance_key("T1", None, "20261011") == "T1|?20261011"
    assert trip_instance_key("T1", None, None) == "T1|?none"


T6_STOPS = [(1, "S5"), (2, "S6"), (3, "S7"), (4, "S5")]


def test_stop_resolution_on_loop_trip() -> None:
    assert resolve_stop(stop_sequence=None, stop_id="S5", trip_stops=T6_STOPS).reason == "UNRESOLVED_STOP_SEQUENCE"
    s7 = resolve_stop(stop_sequence=None, stop_id="S7", trip_stops=T6_STOPS)
    assert (s7.resolved, s7.stop_sequence) == (True, 3)
    by_seq = resolve_stop(stop_sequence=4, stop_id="S5", trip_stops=T6_STOPS)
    assert (by_seq.resolved, by_seq.stop_sequence, by_seq.stop_id) == (True, 4, "S5")
    assert resolve_stop(stop_sequence=2, stop_id="S7", trip_stops=T6_STOPS).reason == "STOP_ID_SEQUENCE_MISMATCH"
    assert resolve_stop(stop_sequence=9, stop_id=None, trip_stops=T6_STOPS).reason == "UNKNOWN_STOP_FOR_TRIP"
    assert resolve_stop(stop_sequence=None, stop_id="S9", trip_stops=T6_STOPS).reason == "UNKNOWN_STOP_FOR_TRIP"
    assert resolve_stop(stop_sequence=None, stop_id=None, trip_stops=T6_STOPS).reason == "UNRESOLVED_STOP_SEQUENCE"


def test_golden_predicted_delays() -> None:
    """T1 seq3 (08:05) -> 360 s and T2 seq2 (08:05) -> 60 s on 2026-10-05 (design C §8 S1)."""
    d = date(2026, 10, 5)
    t1 = scheduled_event_utc(d, _secs("08:05:00"), NY)
    assert t1 is not None and 1791202260 - int(t1.timestamp()) == 360
    t2 = scheduled_event_utc(d, _secs("08:05:00"), NY)
    assert t2 is not None and 1791201960 - int(t2.timestamp()) == 60
    assert scheduled_event_utc(d, None, NY) is None


# --------------------------------------------------------------------------------------------------
# expected.json vectors (hand-written oracle values; the Spark resolver is checked against the same file)
# --------------------------------------------------------------------------------------------------

def _golden_static(golden_dir) -> tuple[ServiceCalendar, dict[str, StaticTrip], dict[str, list[tuple[int, str]]]]:
    static = golden_dir / "static"
    cal = ServiceCalendar.from_rows(
        read_csv_text((static / "calendar.txt").read_text(encoding="utf-8"))[1],
        read_csv_text((static / "calendar_dates.txt").read_text(encoding="utf-8"))[1],
    )
    trips = read_csv_text((static / "trips.txt").read_text(encoding="utf-8"))[1]
    stop_times = read_csv_text((static / "stop_times.txt").read_text(encoding="utf-8"))[1]
    by_trip: dict[str, list[dict]] = {}
    for st in stop_times:
        by_trip.setdefault(str(st["trip_id"]), []).append(st)
    scoped: dict[str, StaticTrip] = {}
    stops: dict[str, list[tuple[int, str]]] = {}
    for t in trips:
        if t["route_id"] not in ("R_A", "R_B"):
            continue                                   # scoped static only (TX of R_X is unknown here)
        rows = sorted(by_trip[str(t["trip_id"])], key=lambda r: int(str(r["stop_sequence"])))
        first, last = rows[0], rows[-1]
        scoped[str(t["trip_id"])] = StaticTrip(
            str(t["trip_id"]), str(t["route_id"]), str(t["service_id"]),
            int(str(t["direction_id"])) if t["direction_id"] is not None else None,
            parse_gtfs_time(first["departure_time"] or first["arrival_time"]),
            parse_gtfs_time(last["arrival_time"] or last["departure_time"]),
        )
        stops[str(t["trip_id"])] = [(int(str(r["stop_sequence"])), str(r["stop_id"])) for r in rows]
    return cal, scoped, stops


def _expected(golden_dir) -> dict:
    import json

    return json.loads((golden_dir / "expected.json").read_text(encoding="utf-8"))


def test_expected_json_resolution_vectors(golden_dir) -> None:
    cal, scoped, _ = _golden_static(golden_dir)
    vectors = _expected(golden_dir)["static"]["resolution_vectors"]
    assert vectors, "expected.json has no resolution_vectors"
    for vec in vectors:
        trip = vec["trip"]
        clock = datetime.fromisoformat(vec["clock"].replace("Z", "+00:00"))
        res = resolve_trip_instance(
            trip_id=trip["trip_id"], route_id=trip.get("route_id"), direction_id=trip.get("direction_id"),
            start_date=trip.get("start_date"), start_time=trip.get("start_time"),
            schedule_relationship=trip.get("schedule_relationship"), static_trip=scoped.get(trip["trip_id"]),
            calendar=cal, metrics_as_of=clock, agency_timezone=NY, window_seconds=43200,
        )
        got = {
            "schedule_resolution": res.schedule_resolution,
            "resolution_method": res.method,
            "service_date": res.service_date.isoformat() if res.service_date else None,
            "reason": res.reason,
            "candidates": None if res.candidates is None else [d.isoformat() for d in res.candidates],
        }
        assert got == vec["expected"], vec["name"]


def test_expected_json_stop_resolution_vectors(golden_dir) -> None:
    _, _, stops = _golden_static(golden_dir)
    vectors = _expected(golden_dir)["static"]["stop_resolution_vectors"]
    assert vectors, "expected.json has no stop_resolution_vectors"
    for vec in vectors:
        res = resolve_stop(stop_sequence=vec["stop_sequence"], stop_id=vec["stop_id"],
                           trip_stops=stops[vec["trip_id"]])
        got = {"resolved": res.resolved, "stop_sequence": res.stop_sequence, "stop_id": res.stop_id,
               "reason": res.reason}
        assert got == vec["expected"], vec
