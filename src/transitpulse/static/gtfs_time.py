"""Pure-Python GTFS time helpers (reference implementation mirrored by the Spark expressions).

GTFS ``HH:MM:SS`` values are seconds since the *service-day anchor*, which the GTFS reference defines
as "noon minus 12 hours" in the agency timezone. Hours may be 24 or more (``25:10:00`` is the next
calendar day of the same service day). On DST change days the anchor is NOT local midnight:

    service_anchor_utc  = UTC(local noon of service_date in agency_timezone) - 12 hours
    scheduled_event_utc = service_anchor_utc + gtfs_seconds

Local noon never falls into a gap or an overlap for America/New_York, so the conversion is unambiguous.
The Spark mirror lives in :mod:`transitpulse.static.sparkio` (``service_anchor_expr``,
``gtfs_seconds_col``); both use the same regular expression :data:`GTFS_TIME_PATTERN`.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

__all__ = [
    "GTFS_TIME_PATTERN",
    "SECONDS_PER_DAY",
    "parse_gtfs_time",
    "format_gtfs_seconds",
    "service_anchor_utc",
    "scheduled_utc",
    "parse_service_date",
    "format_service_date",
    "local_date",
    "validate_timezone",
]

#: Regular expression shared with Spark (Java regex syntax is identical for this pattern).
GTFS_TIME_PATTERN = r"^\s*(\d{1,3}):([0-5]\d):([0-5]\d)\s*$"
_GTFS_TIME_RE = re.compile(GTFS_TIME_PATTERN)
_SERVICE_DATE_RE = re.compile(r"^\s*(\d{4})(\d{2})(\d{2})\s*$")

SECONDS_PER_DAY = 86_400
_TWELVE_HOURS = timedelta(hours=12)


def parse_gtfs_time(s: str | None) -> int | None:
    """Parse a GTFS ``HH:MM:SS`` value into seconds since the service-day anchor.

    ``None`` or a blank string returns ``None`` (an allowed empty time of an intermediate stop).
    Hours of 24 or more are valid (``"25:10:00" -> 90600``). Minutes/seconds outside 0-59, a
    missing component or any other text raises :class:`ValueError`.
    """
    if s is None:
        return None
    if not isinstance(s, str):
        raise ValueError(f"GTFS time must be a string, got {type(s).__name__}")
    if s.strip() == "":
        return None
    m = _GTFS_TIME_RE.match(s)
    if not m:
        raise ValueError(f"invalid GTFS time {s!r} (expected H:MM:SS with MM and SS in 00-59)")
    hours, minutes, seconds = (int(g) for g in m.groups())
    return hours * 3600 + minutes * 60 + seconds


def format_gtfs_seconds(sec: int) -> str:
    """Format seconds since the service-day anchor as zero-padded ``HH:MM:SS`` (``90600 -> "25:10:00"``)."""
    if isinstance(sec, bool) or not isinstance(sec, int):
        raise ValueError(f"GTFS seconds must be an int, got {sec!r}")
    if sec < 0:
        raise ValueError(f"GTFS seconds must be non-negative, got {sec}")
    hours, rem = divmod(sec, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def validate_timezone(tz: str) -> ZoneInfo:
    """Return the :class:`ZoneInfo` for an IANA name; raise :class:`ValueError` when it is unknown."""
    if not isinstance(tz, str) or not tz.strip():
        raise ValueError(f"invalid timezone {tz!r}")
    try:
        return ZoneInfo(tz.strip())
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"unknown IANA timezone {tz!r}") from exc


def service_anchor_utc(service_date: date, tz: str) -> datetime:
    """UTC instant of the GTFS service-day anchor: UTC(local noon of ``service_date``) - 12 hours."""
    if isinstance(service_date, datetime):
        raise ValueError("service_date must be a date, not a datetime")
    zone = validate_timezone(tz)
    local_noon = datetime.combine(service_date, time(12, 0, 0), tzinfo=zone)
    return local_noon.astimezone(UTC) - _TWELVE_HOURS


def scheduled_utc(service_date: date, gtfs_seconds: int, tz: str) -> datetime:
    """UTC instant of a scheduled event: service anchor + GTFS seconds (never naive local midnight)."""
    if isinstance(gtfs_seconds, bool) or not isinstance(gtfs_seconds, int):
        raise ValueError(f"gtfs_seconds must be an int, got {gtfs_seconds!r}")
    if gtfs_seconds < 0:
        raise ValueError(f"gtfs_seconds must be non-negative, got {gtfs_seconds}")
    return service_anchor_utc(service_date, tz) + timedelta(seconds=gtfs_seconds)


def parse_service_date(s: str) -> date:
    """Parse a GTFS service date ``YYYYMMDD`` (``"20261005" -> date(2026, 10, 5)``); ValueError otherwise."""
    if not isinstance(s, str):
        raise ValueError(f"service date must be a string, got {type(s).__name__}")
    m = _SERVICE_DATE_RE.match(s)
    if not m:
        raise ValueError(f"invalid GTFS service date {s!r} (expected YYYYMMDD)")
    year, month, day = (int(g) for g in m.groups())
    try:
        return date(year, month, day)
    except ValueError as exc:
        raise ValueError(f"invalid GTFS service date {s!r}: {exc}") from exc


def format_service_date(d: date) -> str:
    """Format a date as GTFS ``YYYYMMDD``."""
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


def local_date(instant: datetime, tz: str) -> date:
    """Calendar date of an aware instant in the agency timezone."""
    if instant.tzinfo is None:
        raise ValueError("naive datetime is not allowed; use timezone-aware UTC datetimes")
    return instant.astimezone(validate_timezone(tz)).date()
