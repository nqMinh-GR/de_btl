"""Static GTFS validation rules, severities and pure-Python check helpers (design C §1, lead decision on WP1).

Severity policy:

* **FAILED only for structural problems** (an *error*; nothing is published and no run can pin the
  version): missing required files/columns, invalid or multiple agency timezones, duplicate keys in scope,
  missing foreign keys of scoped rows, rejected configured routes. The same structural rule violated only by
  out-of-scope rows is a warning, so a broken subway row does not block a bus scope.
* **Per-trip / per-row data defects are warnings even in scope** (bad or missing stop times, non-monotonic
  times, bad stop_sequence/stop_id values, bad coordinates, bad calendar rows, bad optional fields). The
  affected trip stays in ``dim_trips`` with ``schedule_valid=false`` and downstream code must treat it as an
  unresolved schedule (no delay, excluded from the KPI). One bad MBTA trip never fails the whole load.
* Infos never change the status.

The Spark loader (:mod:`transitpulse.static.build`) evaluates every rule over DataFrames and turns the
results into :class:`Check` rows with :func:`make_check`; the pure-Python helpers below implement the same
rules over small in-memory rows (unit tests, ``inspect``, the oracle comparison).
"""

from __future__ import annotations

import csv
import io
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from transitpulse.static.gtfs_time import SECONDS_PER_DAY, parse_gtfs_time, parse_service_date

__all__ = [
    "STRUCTURAL_CODES",
    "ERROR",
    "WARNING",
    "INFO",
    "STATUS_PASSED",
    "STATUS_PASSED_WITH_WARNINGS",
    "STATUS_FAILED",
    "FileSpec",
    "FILE_SPECS",
    "REQUIRED_FILES",
    "CHECK_RULES",
    "Check",
    "make_check",
    "status_from_checks",
    "severity_counts",
    "normalize_header",
    "missing_columns",
    "peek_csv",
    "read_csv_text",
    "coordinates_valid",
    "coordinates_required",
    "find_duplicate_keys",
    "stop_time_issues",
    "calendar_row_issues",
    "calendar_date_row_issues",
]

ERROR, WARNING, INFO = "error", "warning", "info"
STATUS_PASSED = "PASSED"
STATUS_PASSED_WITH_WARNINGS = "PASSED_WITH_WARNINGS"
STATUS_FAILED = "FAILED"
SAMPLE_LIMIT = 10


@dataclass(frozen=True)
class FileSpec:
    """Columns the loader projects from one GTFS file (missing optional columns become null)."""

    name: str
    required_file: bool
    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...] = ()
    key_columns: tuple[str, ...] = ()

    @property
    def filename(self) -> str:
        return f"{self.name}.txt"

    @property
    def columns(self) -> tuple[str, ...]:
        return self.required_columns + self.optional_columns


_WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

FILE_SPECS: dict[str, FileSpec] = {
    spec.name: spec
    for spec in (
        FileSpec("agency", True, ("agency_name", "agency_timezone"),
                 ("agency_id", "agency_url", "agency_lang"), ("agency_id",)),
        FileSpec("routes", True, ("route_id", "route_type"),
                 ("agency_id", "route_short_name", "route_long_name", "route_color", "route_text_color",
                  "route_sort_order"), ("route_id",)),
        FileSpec("trips", True, ("route_id", "service_id", "trip_id"),
                 ("direction_id", "shape_id", "trip_headsign", "block_id"), ("trip_id",)),
        FileSpec("stops", True, ("stop_id",),
                 ("stop_code", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station",
                  "platform_code", "wheelchair_boarding"), ("stop_id",)),
        FileSpec("stop_times", True, ("trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"),
                 ("timepoint", "stop_headsign", "pickup_type", "drop_off_type", "shape_dist_traveled"),
                 ("trip_id", "stop_sequence")),
        # calendar / calendar_dates: at least one of the two must be present and non-empty
        FileSpec("calendar", False, ("service_id", *_WEEKDAYS, "start_date", "end_date"), (), ("service_id",)),
        FileSpec("calendar_dates", False, ("service_id", "date", "exception_type"), (), ("service_id", "date")),
        FileSpec("shapes", False, ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"),
                 ("shape_dist_traveled",), ("shape_id", "shape_pt_sequence")),
        FileSpec("feed_info", False, (),
                 ("feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_start_date", "feed_end_date",
                  "feed_version")),
    )
}
REQUIRED_FILES: tuple[str, ...] = tuple(n for n, s in FILE_SPECS.items() if s.required_file)

#: code -> (severity when in scope / required, severity when out of scope / optional, description)
CHECK_RULES: dict[str, tuple[str, str, str]] = {
    "STATIC_FILE_MISSING": (ERROR, WARNING, "required file missing (optional file: warning)"),
    "STATIC_FILE_EMPTY": (ERROR, WARNING, "file has no data rows (calendar may be empty if calendar_dates is not)"),
    "MISSING_REQUIRED_COLUMN": (ERROR, ERROR, "a column required by the loader is absent"),
    "INVALID_TIMEZONE": (ERROR, ERROR, "agency_timezone is not a valid IANA zone (zoneinfo)"),
    "MULTIPLE_AGENCY_TIMEZONES": (ERROR, ERROR, "agencies use different timezones"),
    "DUPLICATE_KEY": (ERROR, WARNING, "primary key value occurs more than once"),
    "FK_MISSING": (ERROR, WARNING, "reference to a missing row (trips->shapes is always a warning)"),
    "SCOPE_ROUTE_NOT_FOUND": (ERROR, ERROR, "configured route_id does not exist in routes.txt"),
    "SCOPE_ROUTE_NOT_BUS": (ERROR, ERROR, "configured route_id is not route_type 3 (bus)"),
    # data defects: warnings even in scope (trip -> schedule_valid=false; bad shape -> dropped)
    "INVALID_COORDINATES": (WARNING, WARNING, "lat/lon missing, out of range or (0,0); bad shape points drop the shape"),
    "INVALID_GTFS_TIME": (WARNING, WARNING, "time not H:MM:SS with MM/SS in 00-59 (trip schedule_valid=false)"),
    "FIRST_LAST_STOP_TIME_MISSING": (WARNING, WARNING, "first or last stop of a trip has no time (schedule_valid=false)"),
    "NON_MONOTONIC_STOP_TIMES": (WARNING, WARNING, "stop times decrease along stop_sequence (schedule_valid=false)"),
    "INVALID_CALENDAR": (WARNING, WARNING, "bad date, end < start, bad weekday flag or exception_type not 1/2 "
                         "(row contributes no service dates)"),
    "INVALID_FIELD_VALUE": (WARNING, WARNING, "value cannot be cast to its GTFS type (stop_sequence: trip "
                            "schedule_valid=false)"),
    "MISSING_REQUIRED_VALUE": (WARNING, WARNING, "required field is empty (stop_times: trip schedule_valid=false; "
                               "agency_timezone: error)"),
    "SERVICE_NO_ACTIVE_DATES": (WARNING, WARNING, "scoped service has no active service date"),
    "ROUTE_WITHOUT_TRIPS": (WARNING, WARNING, "scoped route has no trips"),
    "SHAPE_DIST_DECREASING": (WARNING, WARNING, "shape_dist_traveled decreases along the shape"),
    "REPEATED_STOP_IN_TRIP": (INFO, INFO, "a stop occurs more than once in a trip (stop_id join is ambiguous)"),
    "SERVICE_TIME_GE_24H": (INFO, INFO, "trip has times at or after 24:00:00"),
    "FEED_INFO_MISSING": (INFO, INFO, "feed_info.txt missing or empty"),
    "OUT_OF_SCOPE_FILTERED": (INFO, INFO, "rows removed by the scope filter"),
}

#: The only codes that can make a load FAILED (agency_timezone missing is reported as an explicit error).
STRUCTURAL_CODES: frozenset[str] = frozenset(
    code for code, (sev_in, _sev_out, _d) in CHECK_RULES.items() if sev_in == ERROR
)


@dataclass(frozen=True)
class Check:
    """One row of ``static_quality_report.json`` ``checks``."""

    code: str
    severity: str
    table: str
    in_scope: bool
    count: int
    sample_keys: tuple[str, ...] = field(default_factory=tuple)
    detail: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "table": self.table,
            "in_scope": self.in_scope,
            "count": int(self.count),
            "sample_keys": list(self.sample_keys)[:SAMPLE_LIMIT],
            "detail": self.detail,
        }


def make_check(
    code: str,
    table: str,
    *,
    in_scope: bool,
    count: int,
    sample_keys: Iterable[Any] = (),
    detail: str | None = None,
    severity: str | None = None,
) -> Check:
    """Build a :class:`Check`; the severity comes from :data:`CHECK_RULES` unless explicitly overridden."""
    if code not in CHECK_RULES:
        raise KeyError(f"unregistered static check code {code!r}")
    sev_in, sev_out, _ = CHECK_RULES[code]
    sev = severity or (sev_in if in_scope else sev_out)
    if sev not in (ERROR, WARNING, INFO):
        raise ValueError(f"invalid severity {sev!r}")
    samples = tuple(_sample_text(k) for k in list(sample_keys)[:SAMPLE_LIMIT])
    return Check(code, sev, table, bool(in_scope), int(count), samples, detail)


def _sample_text(key: Any) -> str:
    if isinstance(key, (tuple, list)):
        return "|".join("" if k is None else str(k) for k in key)
    return "" if key is None else str(key)


def status_from_checks(checks: Iterable[Check]) -> str:
    """FAILED if any error, PASSED_WITH_WARNINGS if any warning, else PASSED (infos never count)."""
    sev = {c.severity for c in checks if c.count > 0}
    if ERROR in sev:
        return STATUS_FAILED
    if WARNING in sev:
        return STATUS_PASSED_WITH_WARNINGS
    return STATUS_PASSED


def severity_counts(checks: Iterable[Check]) -> dict[str, int]:
    """Number of check rows (not violating records) per severity."""
    out = {ERROR: 0, WARNING: 0, INFO: 0}
    for c in checks:
        if c.count > 0:
            out[c.severity] += 1
    return out


# --------------------------------------------------------------------------------------------------
# CSV helpers
# --------------------------------------------------------------------------------------------------

def normalize_header(names: Sequence[str]) -> list[str]:
    """Strip the UTF-8 BOM and surrounding whitespace from header names (GTFS column order varies)."""
    return [str(n).replace("﻿", "").strip() for n in names]


def missing_columns(header: Sequence[str], required: Sequence[str]) -> list[str]:
    have = set(normalize_header(header))
    return [c for c in required if c not in have]


def peek_csv(path: Path) -> tuple[list[str] | None, bool]:
    """Return ``(normalized header or None, has at least one non-blank data row)`` without reading it all."""
    if not path.is_file() or path.stat().st_size == 0:
        return None, False
    with open(path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None:
            return None, False
        for row in reader:
            if any(cell.strip() for cell in row):
                return normalize_header(header), True
        return normalize_header(header), False


def read_csv_text(text: str) -> tuple[list[str], list[dict[str, str | None]]]:
    """Parse small GTFS CSV text: normalized header + rows with trimmed values (blank -> None)."""
    reader = csv.reader(io.StringIO(text.lstrip("﻿")))
    header = normalize_header(next(reader, []))
    rows: list[dict[str, str | None]] = []
    for raw in reader:
        if not any(cell.strip() for cell in raw):
            continue
        row: dict[str, str | None] = {}
        for i, name in enumerate(header):
            value = raw[i].strip() if i < len(raw) else ""
            row[name] = value if value != "" else None
        rows.append(row)
    return header, rows


# --------------------------------------------------------------------------------------------------
# Pure rule helpers (mirrored by the Spark loader)
# --------------------------------------------------------------------------------------------------

def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(str(v).strip())
    except ValueError:
        return None


def coordinates_valid(lat: Any, lon: Any) -> bool:
    """Both present, within [-90, 90] x [-180, 180] and not the (0, 0) placeholder."""
    la, lo = _to_float(lat), _to_float(lon)
    if la is None or lo is None:
        return False
    if not (-90.0 <= la <= 90.0 and -180.0 <= lo <= 180.0):
        return False
    return not (la == 0.0 and lo == 0.0)


def coordinates_required(location_type: Any) -> bool:
    """GTFS: lat/lon are required for location_type 0/1/2 (empty means 0); optional for 3 and 4."""
    if location_type is None or str(location_type).strip() == "":
        return True
    try:
        return int(str(location_type).strip()) in (0, 1, 2)
    except ValueError:
        return True


def find_duplicate_keys(rows: Iterable[Mapping[str, Any]], key_columns: Sequence[str]) -> list[tuple[Any, ...]]:
    """Key tuples that occur more than once (sorted by first occurrence)."""
    counts: Counter[tuple[Any, ...]] = Counter(tuple(r.get(k) for k in key_columns) for r in rows)
    return [k for k, n in counts.items() if n > 1]


def stop_time_issues(rows: Iterable[Mapping[str, Any]]) -> dict[str, set[str]]:
    """Per trip_id, the stop_times rule codes it violates (or triggers as info).

    Codes: INVALID_GTFS_TIME, INVALID_FIELD_VALUE (stop_sequence), FIRST_LAST_STOP_TIME_MISSING,
    NON_MONOTONIC_STOP_TIMES, REPEATED_STOP_IN_TRIP, SERVICE_TIME_GE_24H. Event time of a stop is
    ``coalesce(arrival, departure)``; a stop's departure must not precede its arrival and no time may be
    earlier than the maximum time seen at a previous stop.
    """
    by_trip: dict[str, list[tuple[int, str | None, int | None, int | None]]] = defaultdict(list)
    issues: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        trip = str(r.get("trip_id"))
        try:
            seq = int(str(r.get("stop_sequence")).strip())
        except ValueError:
            issues[trip].add("INVALID_FIELD_VALUE")
            continue
        times: list[int | None] = []
        for col in ("arrival_time", "departure_time"):
            try:
                times.append(parse_gtfs_time(r.get(col)))
            except ValueError:
                issues[trip].add("INVALID_GTFS_TIME")
                times.append(None)
        by_trip[trip].append((seq, r.get("stop_id"), times[0], times[1]))
    for trip, stops in by_trip.items():
        stops.sort(key=lambda s: s[0])
        first, last = stops[0], stops[-1]
        if (first[3] if first[3] is not None else first[2]) is None or (
            last[2] if last[2] is not None else last[3]
        ) is None:
            issues[trip].add("FIRST_LAST_STOP_TIME_MISSING")
        prev_max: int | None = None
        for _, _, arr, dep in stops:
            if arr is not None and dep is not None and dep < arr:
                issues[trip].add("NON_MONOTONIC_STOP_TIMES")
            event = arr if arr is not None else dep
            if event is not None and prev_max is not None and event < prev_max:
                issues[trip].add("NON_MONOTONIC_STOP_TIMES")
            for t in (arr, dep):
                if t is not None:
                    prev_max = t if prev_max is None else max(prev_max, t)
                    if t >= SECONDS_PER_DAY:
                        issues[trip].add("SERVICE_TIME_GE_24H")
        stop_counts = Counter(s[1] for s in stops)
        if any(n > 1 for n in stop_counts.values()):
            issues[trip].add("REPEATED_STOP_IN_TRIP")
    return {k: v for k, v in issues.items() if v}


def calendar_row_issues(row: Mapping[str, Any]) -> list[str]:
    """Problems of one ``calendar.txt`` row (empty list when valid)."""
    out: list[str] = []
    for day in _WEEKDAYS:
        if str(row.get(day) or "").strip() not in ("0", "1"):
            out.append(f"bad {day} flag {row.get(day)!r}")
    try:
        start = parse_service_date(str(row.get("start_date") or ""))
        end = parse_service_date(str(row.get("end_date") or ""))
    except ValueError as exc:
        out.append(str(exc))
        return out
    if end < start:
        out.append("end_date < start_date")
    return out


def calendar_date_row_issues(row: Mapping[str, Any]) -> list[str]:
    """Problems of one ``calendar_dates.txt`` row (empty list when valid)."""
    out: list[str] = []
    try:
        parse_service_date(str(row.get("date") or ""))
    except ValueError as exc:
        out.append(str(exc))
    if str(row.get("exception_type") or "").strip() not in ("1", "2"):
        out.append(f"exception_type {row.get('exception_type')!r} is not 1 or 2")
    return out
