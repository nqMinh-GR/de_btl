"""Independent GTFS static oracle (stdlib only).

Implements design C §1-§3 without reusing any project code:

* CSV reading: every value stays a string (IDs are never converted); BOM and header whitespace are
  stripped; an empty cell means "absent" (None).
* GTFS time ``H:MM:SS`` with hours >= 24 allowed (``25:10:00`` -> 90600 s).
* Service anchor = UTC(local noon of the service date) - 12 h; scheduled instant = anchor + seconds.
  (Local noon never falls into a New York DST gap/overlap.)
* Calendar activity: a removal (exception 2) wins, then an addition (exception 1), then the weekly
  pattern inside [start_date, end_date].
* Scope: verified bus routes (route_type 3) -> trips -> stop_times -> stops (+ parent stations) ->
  shapes / services / agencies.
* Trip-instance resolution (ADDED, unknown trip, descriptor mismatch, start_date, inference window) and
  stop resolution (stop_sequence first, else a stop_id that is unique in the trip).

This module MUST NOT import ``transitpulse`` or ``pyspark`` (tests/unit/test_oracle_isolation.py).
"""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

UTC = timezone.utc
SECONDS_PER_DAY = 86400
BUS_ROUTE_TYPE = 3

TABLE_NAMES: tuple[str, ...] = (
    "agency", "routes", "stops", "trips", "stop_times", "calendar", "calendar_dates", "shapes", "feed_info",
)
REQUIRED_TABLES: tuple[str, ...] = ("agency", "routes", "trips", "stops", "stop_times")
WEEKDAY_COLUMNS: tuple[str, ...] = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "agency": ("agency_id", "agency_name", "agency_url", "agency_timezone"),
    "routes": ("route_id", "agency_id", "route_type"),
    "stops": ("stop_id", "stop_lat", "stop_lon"),
    "trips": ("route_id", "service_id", "trip_id"),
    "stop_times": ("trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"),
    "calendar": ("service_id", *WEEKDAY_COLUMNS, "start_date", "end_date"),
    "calendar_dates": ("service_id", "date", "exception_type"),
    "shapes": ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"),
    "feed_info": ("feed_publisher_name", "feed_publisher_url", "feed_lang"),
}

_GTFS_TIME = re.compile(r"^\s*(\d{1,3}):([0-5]\d):([0-5]\d)\s*$")
_SERVICE_DATE = re.compile(r"^(\d{4})(\d{2})(\d{2})$")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


# --------------------------------------------------------------------------------------------------
# Instants
# --------------------------------------------------------------------------------------------------

def iso_z(dt: datetime) -> str:
    """UTC ISO-8601 with second precision and ``Z`` (the fixture uses whole seconds only)."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(text: str) -> datetime:
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"timestamp without timezone: {text!r}")
    return dt.astimezone(UTC)


def from_epoch(seconds: int) -> datetime:
    return _EPOCH + timedelta(seconds=int(seconds))


def to_epoch(dt: datetime) -> int:
    delta = dt.astimezone(UTC) - _EPOCH
    return delta.days * SECONDS_PER_DAY + delta.seconds


def iso_epoch(seconds: int | None) -> str | None:
    return None if seconds is None else iso_z(from_epoch(seconds))


# --------------------------------------------------------------------------------------------------
# GTFS time and service dates
# --------------------------------------------------------------------------------------------------

def parse_gtfs_time(text: str | None) -> int | None:
    """``"25:10:00"`` -> 90600; ``None``/``""`` -> None; malformed -> ValueError."""
    if text is None or text.strip() == "":
        return None
    m = _GTFS_TIME.match(text)
    if m is None:
        raise ValueError(f"invalid GTFS time {text!r}")
    hours, minutes, seconds = (int(g) for g in m.groups())
    return hours * 3600 + minutes * 60 + seconds


def format_gtfs_seconds(seconds: int) -> str:
    hours, rem = divmod(int(seconds), 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def parse_service_date(text: str | None) -> date:
    m = _SERVICE_DATE.match(text or "")
    if m is None:
        raise ValueError(f"invalid service date {text!r}")
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def service_anchor_utc(service_date: date, tz: str) -> datetime:
    """UTC(local noon) - 12 h. Equals local midnight except on DST transition days."""
    local_noon = datetime.combine(service_date, time(12, 0), tzinfo=ZoneInfo(tz))
    return local_noon.astimezone(UTC) - timedelta(hours=12)


def scheduled_utc(service_date: date, gtfs_seconds: int, tz: str) -> datetime:
    return service_anchor_utc(service_date, tz) + timedelta(seconds=int(gtfs_seconds))


def local_midnight_utc(service_date: date, tz: str) -> datetime:
    """The WRONG convention (UTC of local midnight); used only to document the DST difference."""
    return datetime.combine(service_date, time(0, 0), tzinfo=ZoneInfo(tz)).astimezone(UTC)


def time_vector(service_date_iso: str, gtfs_time: str, tz: str) -> dict[str, Any]:
    """Output of one DST/overnight test vector (same keys as expected.json static.time_vectors[*].expected)."""
    d = date.fromisoformat(service_date_iso)
    secs = parse_gtfs_time(gtfs_time)
    if secs is None:
        raise ValueError("time vector needs a GTFS time")
    anchor = service_anchor_utc(d, tz)
    scheduled = anchor + timedelta(seconds=secs)
    wrong = local_midnight_utc(d, tz) + timedelta(seconds=secs)
    return {
        "seconds": secs,
        "anchor_utc": iso_z(anchor),
        "scheduled_utc": iso_z(scheduled),
        "local_time": scheduled.astimezone(ZoneInfo(tz)).isoformat(),
        "wrong_local_midnight_utc": iso_z(wrong),
    }


# --------------------------------------------------------------------------------------------------
# CSV tables
# --------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Table:
    header: tuple[str, ...]
    rows: tuple[dict[str, str | None], ...]


def read_table(path: Path) -> Table | None:
    """Read one GTFS CSV; every cell stays a string. Returns None when the file does not exist."""
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        try:
            raw_header = next(reader)
        except StopIteration:
            return Table((), ())
        header = tuple(h.strip() for h in raw_header)
        rows: list[dict[str, str | None]] = []
        for record in reader:
            if not record or all(not cell.strip() for cell in record):
                continue
            row: dict[str, str | None] = {}
            for i, name in enumerate(header):
                row[name] = record[i].strip() if i < len(record) else None
            rows.append(row)
    return Table(header, tuple(rows))


def opt(row: dict[str, Any], key: str) -> str | None:
    """Optional CSV value: missing or empty -> None (never "")."""
    value = row.get(key)
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _coords_text_ok(lat_text: str | None, lon_text: str | None) -> bool:
    try:
        lat = float(lat_text) if lat_text is not None else None
        lon = float(lon_text) if lon_text is not None else None
    except ValueError:
        return False
    if lat is None or lon is None or lat != lat or lon != lon:
        return False
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return False
    return not (lat == 0.0 and lon == 0.0)


# --------------------------------------------------------------------------------------------------
# Calendar
# --------------------------------------------------------------------------------------------------

@dataclass
class Calendar:
    weekly: dict[str, tuple[tuple[bool, ...], date, date]]
    added: dict[str, set[date]]
    removed: dict[str, set[date]]

    @classmethod
    def from_rows(cls, calendar_rows: list[dict], calendar_date_rows: list[dict]) -> Calendar:
        weekly: dict[str, tuple[tuple[bool, ...], date, date]] = {}
        for r in calendar_rows:
            sid = opt(r, "service_id")
            if sid is None:
                continue
            try:
                start = parse_service_date(opt(r, "start_date"))
                end = parse_service_date(opt(r, "end_date"))
            except ValueError:
                continue
            days = tuple(opt(r, c) == "1" for c in WEEKDAY_COLUMNS)
            weekly[sid] = (days, start, end)
        added: dict[str, set[date]] = defaultdict(set)
        removed: dict[str, set[date]] = defaultdict(set)
        for r in calendar_date_rows:
            sid = opt(r, "service_id")
            try:
                d = parse_service_date(opt(r, "date"))
            except ValueError:
                continue
            exception_type = opt(r, "exception_type")
            if exception_type == "1":
                added[sid].add(d)
            elif exception_type == "2":
                removed[sid].add(d)
        return cls(weekly, dict(added), dict(removed))

    def active(self, service_id: str, d: date) -> bool:
        if d in self.removed.get(service_id, set()):
            return False
        if d in self.added.get(service_id, set()):
            return True
        pattern = self.weekly.get(service_id)
        if pattern is None:
            return False
        days, start, end = pattern
        return start <= d <= end and days[d.weekday()]   # weekday(): Monday=0 .. Sunday=6

    def service_dates(self, service_id: str) -> list[date]:
        out: set[date] = set()
        pattern = self.weekly.get(service_id)
        if pattern is not None:
            days, start, end = pattern
            d = start
            while d <= end:
                if days[d.weekday()]:
                    out.add(d)
                d += timedelta(days=1)
        out |= self.added.get(service_id, set())
        out -= self.removed.get(service_id, set())
        return sorted(out)


# --------------------------------------------------------------------------------------------------
# Trips
# --------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class StopTime:
    stop_sequence: int
    stop_id: str | None
    arrival_secs: int | None
    departure_secs: int | None


@dataclass
class TripInfo:
    trip_id: str
    route_id: str | None
    service_id: str | None
    direction_id: int | None
    shape_id: str | None
    stop_times: list[StopTime]
    by_sequence: dict[int, StopTime]
    occurrences: dict[str, list[int]]
    first_departure_secs: int | None
    last_arrival_secs: int | None
    has_repeated_stop: bool
    schedule_valid: bool

    @property
    def stop_count(self) -> int:
        return len(self.stop_times)


def _monotonic(stop_times: list[StopTime]) -> bool:
    previous: int | None = None
    for st in stop_times:
        if st.arrival_secs is not None and st.departure_secs is not None and st.departure_secs < st.arrival_secs:
            return False
        for t in (st.arrival_secs, st.departure_secs):
            if t is None:
                continue
            if previous is not None and t < previous:
                return False
            previous = t
    return True


def build_trip_info(trip_row: dict[str, Any], stop_time_rows: list[dict[str, Any]]) -> TripInfo:
    sts = sorted(
        (
            StopTime(
                stop_sequence=int(opt(r, "stop_sequence") or ""),
                stop_id=opt(r, "stop_id"),
                arrival_secs=parse_gtfs_time(opt(r, "arrival_time")),
                departure_secs=parse_gtfs_time(opt(r, "departure_time")),
            )
            for r in stop_time_rows
        ),
        key=lambda s: s.stop_sequence,
    )
    occurrences: dict[str, list[int]] = defaultdict(list)
    for st in sts:
        if st.stop_id is not None:
            occurrences[st.stop_id].append(st.stop_sequence)
    first = sts[0] if sts else None
    last = sts[-1] if sts else None
    first_dep = None
    if first is not None:
        first_dep = first.departure_secs if first.departure_secs is not None else first.arrival_secs
    last_arr = None
    if last is not None:
        last_arr = last.arrival_secs if last.arrival_secs is not None else last.departure_secs
    direction = opt(trip_row, "direction_id")
    return TripInfo(
        trip_id=str(opt(trip_row, "trip_id")),
        route_id=opt(trip_row, "route_id"),
        service_id=opt(trip_row, "service_id"),
        direction_id=int(direction) if direction is not None else None,
        shape_id=opt(trip_row, "shape_id"),
        stop_times=sts,
        by_sequence={st.stop_sequence: st for st in sts},
        occurrences=dict(occurrences),
        first_departure_secs=first_dep,
        last_arrival_secs=last_arr,
        has_repeated_stop=any(len(v) > 1 for v in occurrences.values()),
        schedule_valid=bool(sts) and first_dep is not None and last_arr is not None and _monotonic(sts),
    )


# --------------------------------------------------------------------------------------------------
# Feed and scope
# --------------------------------------------------------------------------------------------------

class GtfsFeed:
    """All static tables of one GTFS version (no scope applied)."""

    def __init__(self, tables: dict[str, Table | None]):
        self.tables = tables
        self.routes: dict[str, dict] = {}
        for r in self.rows("routes"):
            rid = opt(r, "route_id")
            if rid is not None:
                self.routes.setdefault(rid, r)
        self.stops: dict[str, dict] = {}
        for r in self.rows("stops"):
            sid = opt(r, "stop_id")
            if sid is not None:
                self.stops.setdefault(sid, r)
        self.trips: dict[str, dict] = {}
        for r in self.rows("trips"):
            tid = opt(r, "trip_id")
            if tid is not None:
                self.trips.setdefault(tid, r)
        self.stop_times_by_trip: dict[str, list[dict]] = defaultdict(list)
        for r in self.rows("stop_times"):
            self.stop_times_by_trip[str(opt(r, "trip_id"))].append(r)
        self.shapes: dict[str, list[dict]] = defaultdict(list)
        for r in self.rows("shapes"):
            self.shapes[str(opt(r, "shape_id"))].append(r)
        self.calendar = Calendar.from_rows(self.rows("calendar"), self.rows("calendar_dates"))
        self.agency_timezones: list[str] = sorted(
            {tz for tz in (opt(a, "agency_timezone") for a in self.rows("agency")) if tz is not None}
        )
        self.agency_timezone: str | None = self.agency_timezones[0] if len(self.agency_timezones) == 1 else None
        self.trip_to_route: dict[str, str | None] = {tid: opt(t, "route_id") for tid, t in self.trips.items()}
        self.children: dict[str, set[str]] = defaultdict(set)
        for sid, s in self.stops.items():
            parent = opt(s, "parent_station")
            if parent is not None:
                self.children[parent].add(sid)

    @classmethod
    def load(cls, static_dir: Path | str) -> GtfsFeed:
        base = Path(static_dir)
        return cls({name: read_table(base / f"{name}.txt") for name in TABLE_NAMES})

    def rows(self, name: str) -> list[dict]:
        table = self.tables.get(name)
        return list(table.rows) if table is not None else []


@dataclass
class Scope:
    feed: GtfsFeed
    route_ids: tuple[str, ...]                      # verified bus routes, configured order
    route_set: frozenset[str]
    rejected: list[dict[str, str]]
    agency_timezone: str
    route_type_by_route: dict[str, int]
    agency_by_route: dict[str, str | None]
    trips: dict[str, TripInfo]                      # scoped trips only
    route_stop_dirs: dict[str, dict[str, set[int | None]]]
    route_directions: dict[str, set[int | None]]
    stop_ids: set[str]                              # stops of scoped stop_times + their parents
    shape_ids: set[str]
    service_ids: set[str]

    @property
    def calendar(self) -> Calendar:
        return self.feed.calendar


def build_scope(feed: GtfsFeed, requested_route_ids: list[str] | tuple[str, ...]) -> Scope:
    verified: list[str] = []
    rejected: list[dict[str, str]] = []
    for rid in requested_route_ids:
        route = feed.routes.get(rid)
        route_type = opt(route, "route_type") if route is not None else None
        if route is None:
            rejected.append({"route_id": rid, "reason": "SCOPE_ROUTE_NOT_FOUND"})
        elif route_type is None or not route_type.isdigit() or int(route_type) != BUS_ROUTE_TYPE:
            rejected.append({"route_id": rid, "reason": "SCOPE_ROUTE_NOT_BUS"})
        elif rid not in verified:
            verified.append(rid)
    route_set = frozenset(verified)
    trips = {
        tid: build_trip_info(t, feed.stop_times_by_trip.get(tid, []))
        for tid, t in feed.trips.items()
        if opt(t, "route_id") in route_set
    }
    route_stop_dirs: dict[str, dict[str, set[int | None]]] = {rid: defaultdict(set) for rid in verified}
    route_directions: dict[str, set[int | None]] = {rid: set() for rid in verified}
    stop_ids: set[str] = set()
    shape_ids: set[str] = set()
    service_ids: set[str] = set()
    for info in trips.values():
        rid = str(info.route_id)
        route_directions[rid].add(info.direction_id)
        for st in info.stop_times:
            if st.stop_id is not None:
                route_stop_dirs[rid][st.stop_id].add(info.direction_id)
                stop_ids.add(st.stop_id)
        if info.shape_id is not None:
            shape_ids.add(info.shape_id)
        if info.service_id is not None:
            service_ids.add(info.service_id)
    parents = {opt(feed.stops[s], "parent_station") for s in stop_ids if s in feed.stops}
    stop_ids |= {p for p in parents if p is not None}
    return Scope(
        feed=feed,
        route_ids=tuple(verified),
        route_set=route_set,
        rejected=rejected,
        agency_timezone=str(feed.agency_timezone),
        route_type_by_route={rid: int(str(opt(feed.routes[rid], "route_type"))) for rid in verified},
        agency_by_route={rid: opt(feed.routes[rid], "agency_id") for rid in verified},
        trips=trips,
        route_stop_dirs={rid: dict(v) for rid, v in route_stop_dirs.items()},
        route_directions=route_directions,
        stop_ids=stop_ids,
        shape_ids=shape_ids,
        service_ids=service_ids,
    )


# --------------------------------------------------------------------------------------------------
# Static validation summary
# --------------------------------------------------------------------------------------------------

_DUPLICATE_KEYS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("agency", ("agency_id",)),
    ("routes", ("route_id",)),
    ("trips", ("trip_id",)),
    ("stops", ("stop_id",)),
    ("calendar", ("service_id",)),
    ("calendar_dates", ("service_id", "date")),
    ("stop_times", ("trip_id", "stop_sequence")),
    ("shapes", ("shape_id", "shape_pt_sequence")),
)


def validate(feed: GtfsFeed, scope: Scope) -> list[dict[str, Any]]:
    """Checks as ``[{code, severity, keys}]`` sorted by (code, severity). Simplified design C §1 rules:
    the out-of-scope downgrade is modelled only for stop coordinates (enough for the golden fixture)."""
    found: dict[tuple[str, str], set[str]] = defaultdict(set)

    def add(code: str, severity: str, key: object) -> None:
        found[(code, severity)].add(str(key))

    for name in TABLE_NAMES:
        table = feed.tables.get(name)
        if table is None:
            if name in REQUIRED_TABLES:
                add("STATIC_FILE_MISSING", "error", name)
            elif name == "feed_info":
                add("FEED_INFO_MISSING", "info", name)
            elif name == "shapes":
                add("STATIC_FILE_MISSING", "warning", name)
            continue
        if not table.rows and name in REQUIRED_TABLES:
            add("STATIC_FILE_EMPTY", "error", name)
        for column in REQUIRED_COLUMNS.get(name, ()):
            if column not in table.header:
                add("MISSING_REQUIRED_COLUMN", "error", f"{name}.{column}")
    if not feed.rows("calendar") and not feed.rows("calendar_dates"):
        add("STATIC_FILE_EMPTY", "error", "calendar+calendar_dates")

    if not feed.agency_timezones:
        add("INVALID_TIMEZONE", "error", "agency")
    if len(feed.agency_timezones) > 1:
        add("MULTIPLE_AGENCY_TIMEZONES", "error", ",".join(feed.agency_timezones))
    for tz in feed.agency_timezones:
        try:
            ZoneInfo(tz)
        except Exception:  # ZoneInfoNotFoundError / ValueError for malformed keys
            add("INVALID_TIMEZONE", "error", tz)

    for name, columns in _DUPLICATE_KEYS:
        seen: set[tuple[str | None, ...]] = set()
        for r in feed.rows(name):
            key = tuple(opt(r, c) for c in columns)
            if key in seen:
                add("DUPLICATE_KEY", "error", f"{name}:{'|'.join(str(k) for k in key)}")
            seen.add(key)

    services = {opt(r, "service_id") for r in feed.rows("calendar")}
    services |= {opt(r, "service_id") for r in feed.rows("calendar_dates")}
    for tid, t in feed.trips.items():
        if opt(t, "route_id") not in feed.routes:
            add("FK_MISSING", "error", f"trips.route_id:{tid}")
        if opt(t, "service_id") not in services:
            add("FK_MISSING", "error", f"trips.service_id:{tid}")
        shape = opt(t, "shape_id")
        if shape is not None and shape not in feed.shapes:
            add("FK_MISSING", "warning", f"trips.shape_id:{tid}")
    for r in feed.rows("stop_times"):
        if opt(r, "trip_id") not in feed.trips:
            add("FK_MISSING", "error", f"stop_times.trip_id:{opt(r, 'trip_id')}")
        if opt(r, "stop_id") not in feed.stops:
            add("FK_MISSING", "error", f"stop_times.stop_id:{opt(r, 'stop_id')}")

    for sid, s in feed.stops.items():
        if not _coords_text_ok(opt(s, "stop_lat"), opt(s, "stop_lon")):
            add("INVALID_COORDINATES", "error" if sid in scope.stop_ids else "warning", f"stops:{sid}")
    for shape_id, points in feed.shapes.items():
        if any(not _coords_text_ok(opt(p, "shape_pt_lat"), opt(p, "shape_pt_lon")) for p in points):
            add("INVALID_COORDINATES", "warning", f"shapes:{shape_id}")

    for tid in feed.trips:
        rows = feed.stop_times_by_trip.get(tid, [])
        if not rows:
            add("FIRST_LAST_STOP_TIME_MISSING", "error", tid)
            continue
        malformed = False
        for r in rows:
            for column in ("arrival_time", "departure_time"):
                try:
                    parse_gtfs_time(opt(r, column))
                except ValueError:
                    add("INVALID_GTFS_TIME", "error", f"{tid}:{opt(r, 'stop_sequence')}")
                    malformed = True
            if not (opt(r, "stop_sequence") or "").isdigit():
                add("INVALID_NUMBER", "error", f"stop_times.stop_sequence:{tid}")
                malformed = True
        if malformed:
            continue
        info = build_trip_info(feed.trips[tid], rows)
        if info.first_departure_secs is None or info.last_arrival_secs is None:
            add("FIRST_LAST_STOP_TIME_MISSING", "error", tid)
        if not _monotonic(info.stop_times):
            add("NON_MONOTONIC_STOP_TIMES", "error", tid)
        if tid in scope.trips:
            if info.has_repeated_stop:
                add("REPEATED_STOP_IN_TRIP", "info", tid)
            if any(
                t is not None and t >= SECONDS_PER_DAY
                for st in info.stop_times
                for t in (st.arrival_secs, st.departure_secs)
            ):
                add("SERVICE_TIME_GE_24H", "info", tid)

    for r in feed.rows("calendar"):
        try:
            start = parse_service_date(opt(r, "start_date"))
            end = parse_service_date(opt(r, "end_date"))
            if end < start or any(opt(r, c) not in ("0", "1") for c in WEEKDAY_COLUMNS):
                raise ValueError("calendar row")
        except ValueError:
            add("INVALID_CALENDAR", "error", f"calendar:{opt(r, 'service_id')}")
    for r in feed.rows("calendar_dates"):
        try:
            parse_service_date(opt(r, "date"))
            if opt(r, "exception_type") not in ("1", "2"):
                raise ValueError("exception_type")
        except ValueError:
            add("INVALID_CALENDAR", "error", f"calendar_dates:{opt(r, 'service_id')}")

    for rejected in scope.rejected:
        add(rejected["reason"], "error", rejected["route_id"])
    for rid in scope.route_ids:
        if not any(t.route_id == rid for t in scope.trips.values()):
            add("ROUTE_WITHOUT_TRIPS", "warning", rid)
    for sid in scope.service_ids:
        if not feed.calendar.service_dates(sid):
            add("SERVICE_NO_ACTIVE_DATES", "warning", sid)
    for rid in feed.routes:
        if rid not in scope.route_set:
            add("OUT_OF_SCOPE_FILTERED", "info", rid)

    checks = [
        {"code": code, "severity": severity, "keys": sorted(keys)} for (code, severity), keys in found.items()
    ]
    return sorted(checks, key=lambda c: (c["code"], c["severity"]))


def static_summary(static_dir: Path | str, route_ids: list[str] | tuple[str, ...]) -> dict[str, Any]:
    """Everything expected.json ``static.summary`` asserts, computed from the CSV files."""
    feed = GtfsFeed.load(static_dir)
    scope = build_scope(feed, list(route_ids))
    checks = validate(feed, scope)
    severities = {c["severity"] for c in checks}
    status = "FAILED" if "error" in severities else ("PASSED_WITH_WARNINGS" if "warning" in severities else "PASSED")

    scoped_agencies = {scope.agency_by_route[rid] for rid in scope.route_ids}

    def count(name: str) -> int:
        return len(feed.rows(name))

    row_counts = {
        "agency": [count("agency"), sum(1 for r in feed.rows("agency") if opt(r, "agency_id") in scoped_agencies)],
        "routes": [count("routes"), len(scope.route_ids)],
        "trips": [count("trips"), len(scope.trips)],
        "stop_times": [count("stop_times"), sum(t.stop_count for t in scope.trips.values())],
        "stops": [count("stops"), len(scope.stop_ids)],
        "calendar": [count("calendar"),
                     sum(1 for r in feed.rows("calendar") if opt(r, "service_id") in scope.service_ids)],
        "calendar_dates": [count("calendar_dates"),
                           sum(1 for r in feed.rows("calendar_dates") if opt(r, "service_id") in scope.service_ids)],
        "shapes": [count("shapes"), sum(len(feed.shapes.get(s, [])) for s in scope.shape_ids)],
        "shape_ids": [len(feed.shapes), len(scope.shape_ids)],
        "feed_info": [count("feed_info"), count("feed_info")],
    }

    service_dates: dict[str, int] = {}
    all_dates: list[date] = []
    for sid in sorted(scope.service_ids):
        dates = feed.calendar.service_dates(sid)
        service_dates[sid] = len(dates)
        all_dates.extend(dates)
    service_dates["total"] = len(all_dates)

    stops_by_route = {
        rid: {stop: sorted(d for d in dirs if d is not None) for stop, dirs in sorted(scope.route_stop_dirs[rid].items())}
        for rid in scope.route_ids
    }

    geometries: dict[str, dict[str, dict[str, Any]]] = {rid: {} for rid in scope.route_ids}
    for info in scope.trips.values():
        if info.shape_id is None:
            continue
        entry = geometries[str(info.route_id)].setdefault(
            info.shape_id,
            {"direction_ids": set(), "trip_count": 0, "point_count": len(feed.shapes.get(info.shape_id, []))},
        )
        entry["trip_count"] += 1
        if info.direction_id is not None:
            entry["direction_ids"].add(info.direction_id)
    geometries_by_route = {
        rid: {
            shape: {"direction_ids": sorted(e["direction_ids"]), "trip_count": e["trip_count"],
                    "point_count": e["point_count"]}
            for shape, e in sorted(shapes.items())
        }
        for rid, shapes in geometries.items()
    }

    trips = {
        tid: {
            "route_id": info.route_id,
            "service_id": info.service_id,
            "direction_id": info.direction_id,
            "shape_id": info.shape_id,
            "first_departure_secs": info.first_departure_secs,
            "last_arrival_secs": info.last_arrival_secs,
            "stop_count": info.stop_count,
            "has_repeated_stop": info.has_repeated_stop,
            "schedule_valid": info.schedule_valid,
        }
        for tid, info in sorted(scope.trips.items())
    }

    parent_station = {
        s: p for s in sorted(scope.stop_ids) if s in feed.stops
        for p in [opt(feed.stops[s], "parent_station")] if p is not None
    }
    scope_index = {
        "route_ids": list(scope.route_ids),
        "route_type_by_route": dict(scope.route_type_by_route),
        "agency_by_route": dict(scope.agency_by_route),
        "agency_ids": sorted({a for a in scope.agency_by_route.values() if a is not None}),
        "agency_timezone": scope.agency_timezone,
        "trip_to_route": dict(sorted(feed.trip_to_route.items())),
        "scope_stop_ids": sorted(scope.stop_ids),
        "stops_by_route": {rid: sorted(scope.route_stop_dirs[rid]) for rid in scope.route_ids},
        "parent_station": parent_station,
    }

    return {
        "status": status,
        "agency_timezone": feed.agency_timezone,
        "verified_route_ids": list(scope.route_ids),
        "rejected_routes": list(scope.rejected),
        "checks": checks,
        "row_counts": row_counts,
        "service_dates": service_dates,
        "service_date_range": {
            "min": min(all_dates).isoformat() if all_dates else None,
            "max": max(all_dates).isoformat() if all_dates else None,
        },
        "stops_by_route": stops_by_route,
        "geometries_by_route": geometries_by_route,
        "trips": trips,
        "scope_index": scope_index,
    }


# --------------------------------------------------------------------------------------------------
# Trip-instance and stop resolution (design C §3)
# --------------------------------------------------------------------------------------------------

def resolve_trip_instance(scope: Scope, trip: dict[str, Any], clock: datetime, window_seconds: int) -> dict[str, Any]:
    """Resolve one TripDescriptor at ``clock`` (= metrics_as_of).

    Returns ``{trip_id, trip_relationship, route_id, direction_id, schedule_resolution, resolution_method,
    service_date (ISO|None), reason, candidates (list[ISO]|None)}``.
    """
    trip_id = trip.get("trip_id")
    relationship = trip.get("schedule_relationship") or "SCHEDULED"
    start_date = trip.get("start_date")
    start_time = trip.get("start_time")
    out: dict[str, Any] = {
        "trip_id": trip_id,
        "trip_relationship": relationship,
        "route_id": trip.get("route_id"),
        "direction_id": trip.get("direction_id"),
        "schedule_resolution": None,
        "resolution_method": None,
        "service_date": None,
        "reason": None,
        "candidates": None,
    }
    if relationship == "ADDED":
        out["schedule_resolution"] = "not_applicable"
        return out
    info = scope.trips.get(trip_id) if trip_id is not None else None
    if info is None:
        out.update(schedule_resolution="unresolved", reason="UNKNOWN_ROUTE_OR_TRIP")
        return out
    out["route_id"] = info.route_id
    out["direction_id"] = info.direction_id
    desc_route = trip.get("route_id")
    desc_direction = trip.get("direction_id")
    if (desc_route is not None and desc_route != info.route_id) or (
        desc_direction is not None and info.direction_id is not None and desc_direction != info.direction_id
    ):
        out.update(schedule_resolution="unresolved", reason="DESCRIPTOR_STATIC_MISMATCH")
        return out

    start_secs: int | None = None
    if start_time is not None:
        try:
            start_secs = parse_gtfs_time(start_time)
        except ValueError:
            start_secs = -1   # never equals a schedule time
    if start_date is not None:
        try:
            d = parse_service_date(start_date)
        except ValueError:
            out.update(schedule_resolution="unresolved", reason="SERVICE_NOT_ACTIVE_ON_START_DATE")
            return out
        if info.service_id is None or not scope.calendar.active(info.service_id, d):
            out.update(schedule_resolution="unresolved", reason="SERVICE_NOT_ACTIVE_ON_START_DATE")
            return out
        if start_secs is not None and start_secs != info.first_departure_secs:
            out.update(schedule_resolution="unresolved", reason="START_TIME_MISMATCH")
            return out
        out.update(schedule_resolution="resolved", resolution_method="descriptor", service_date=d.isoformat())
        return out

    # start_date absent: candidate service dates L-1, L, L+1 inside the inference window.
    tz = scope.agency_timezone
    local_day = clock.astimezone(ZoneInfo(tz)).date()
    candidates: list[date] = []
    if info.first_departure_secs is not None and info.last_arrival_secs is not None and info.service_id is not None:
        for offset in (-1, 0, 1):
            d = local_day + timedelta(days=offset)
            if not scope.calendar.active(info.service_id, d):
                continue
            anchor = service_anchor_utc(d, tz)
            lo = anchor + timedelta(seconds=info.first_departure_secs - window_seconds)
            hi = anchor + timedelta(seconds=info.last_arrival_secs + window_seconds)
            if not (lo <= clock <= hi):
                continue
            if start_secs is not None and start_secs != info.first_departure_secs:
                continue
            candidates.append(d)
    out["candidates"] = [d.isoformat() for d in candidates]
    if len(candidates) == 1:
        out.update(schedule_resolution="resolved", resolution_method="inferred",
                   service_date=candidates[0].isoformat(), reason="SERVICE_DATE_INFERRED")
    elif not candidates:
        out.update(schedule_resolution="unresolved", reason="UNRESOLVED_TRIP")
    else:
        out.update(schedule_resolution="ambiguous", reason="AMBIGUOUS_SERVICE_DATE")
    return out


def resolution_summary(res: dict[str, Any]) -> dict[str, Any]:
    """The five keys asserted by expected.json static.resolution_vectors[*].expected."""
    return {
        "schedule_resolution": res["schedule_resolution"],
        "resolution_method": res["resolution_method"],
        "service_date": res["service_date"],
        "reason": res["reason"],
        "candidates": res["candidates"],
    }


def resolve_stop(info: TripInfo | None, stop_sequence: int | None, stop_id: str | None) -> dict[str, Any]:
    """Stop join (design C §3): stop_sequence first; else stop_id only when unique in the trip.

    Returns ``{resolved, stop_sequence (resolved|None), stop_id (static when resolved, else source), reason}``.
    Without static stop_times (ADDED / unknown trip) the stop is unresolved with reason None.
    """
    if info is None:
        return {"resolved": False, "stop_sequence": None, "stop_id": stop_id, "reason": None}
    if stop_sequence is not None:
        st = info.by_sequence.get(stop_sequence)
        if st is None:
            return {"resolved": False, "stop_sequence": None, "stop_id": stop_id, "reason": "UNKNOWN_STOP_FOR_TRIP"}
        if stop_id is not None and stop_id != st.stop_id:
            return {"resolved": False, "stop_sequence": None, "stop_id": stop_id,
                    "reason": "STOP_ID_SEQUENCE_MISMATCH"}
        return {"resolved": True, "stop_sequence": st.stop_sequence, "stop_id": st.stop_id, "reason": None}
    if stop_id is not None:
        sequences = info.occurrences.get(stop_id, [])
        if not sequences:
            return {"resolved": False, "stop_sequence": None, "stop_id": stop_id, "reason": "UNKNOWN_STOP_FOR_TRIP"}
        if len(sequences) > 1:
            return {"resolved": False, "stop_sequence": None, "stop_id": stop_id,
                    "reason": "UNRESOLVED_STOP_SEQUENCE"}
        return {"resolved": True, "stop_sequence": sequences[0], "stop_id": stop_id, "reason": None}
    return {"resolved": False, "stop_sequence": None, "stop_id": None, "reason": "UNKNOWN_STOP_FOR_TRIP"}
