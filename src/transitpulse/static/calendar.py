"""Pure-Python GTFS service calendar (reference for the Spark ``dim_service_dates`` derivation).

Rule (design C §3), for a service ``s`` and a service date ``D``:

1. if ``calendar_dates`` has ``(s, D, exception_type=2)`` -> NOT active (removed);
2. else if ``calendar_dates`` has ``(s, D, exception_type=1)`` -> active (added);
3. else active iff a ``calendar`` row of ``s`` has ``start_date <= D <= end_date`` and the weekday flag
   of ``D`` equal to 1.

A service is never considered active from its name or from a single weekday flag alone. An invalid row
(INVALID_CALENDAR: bad/missing weekday flag, bad date, end < start, exception_type not 1/2) contributes no
service dates at all - here, in the Spark ``dim_service_dates`` and in the static quality report alike.
Rows may be raw GTFS strings (``"1"``, ``"20261005"``) or already typed values (``True``/``1``,
:class:`datetime.date`), so the same helpers serve the API, replay, tests and the oracle comparison.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from transitpulse.static.gtfs_time import parse_service_date

__all__ = [
    "WEEKDAY_COLUMNS",
    "EXCEPTION_ADDED",
    "EXCEPTION_REMOVED",
    "ServiceCalendar",
    "active_services",
    "is_service_active",
    "service_dates",
]

#: GTFS weekday columns in ``date.weekday()`` order (Monday == 0).
WEEKDAY_COLUMNS: tuple[str, ...] = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
EXCEPTION_ADDED = 1
EXCEPTION_REMOVED = 2


def _flag(value: Any) -> bool:
    """GTFS weekday flag: ``1``/``"1"``/``True`` is set, ``0``/``"0"``/``False`` is not.

    Anything else (including a missing/blank value) is invalid and raises ``ValueError``; the row is then
    skipped by :meth:`ServiceCalendar.from_rows`, exactly like the Spark loader (INVALID_CALENDAR).
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value not in (0, 1):
            raise ValueError(f"invalid weekday flag {value!r}")
        return value == 1
    text = "" if value is None else str(value).strip()
    if text == "0":
        return False
    if text == "1":
        return True
    raise ValueError(f"invalid weekday flag {value!r}")


def _as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    if value is None:
        raise ValueError("missing date")
    return parse_service_date(str(value))


def _service_id(row: Mapping[str, Any]) -> str | None:
    value = row.get("service_id")
    text = "" if value is None else str(value).strip()
    return text or None


def _as_exception(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError(f"invalid exception_type {value!r}")
    try:
        exc = int(str(value).strip())
    except ValueError as err:
        raise ValueError(f"invalid exception_type {value!r}") from err
    if exc not in (EXCEPTION_ADDED, EXCEPTION_REMOVED):
        raise ValueError(f"invalid exception_type {value!r} (expected 1 or 2)")
    return exc


@dataclass(frozen=True)
class _CalendarRule:
    service_id: str
    weekdays: tuple[bool, ...]   # Monday..Sunday
    start_date: date
    end_date: date

    def covers(self, d: date) -> bool:
        return self.start_date <= d <= self.end_date and self.weekdays[d.weekday()]


@dataclass
class ServiceCalendar:
    """Indexed calendar + calendar_dates for repeated ``is_active`` queries."""

    rules: dict[str, list[_CalendarRule]] = field(default_factory=dict)
    added: dict[str, set[date]] = field(default_factory=dict)
    removed: dict[str, set[date]] = field(default_factory=dict)
    #: skipped invalid rows: {"code": "INVALID_CALENDAR", "table", "service_id", "detail"} (Spark: warning)
    warnings: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_rows(
        cls,
        calendar_rows: Iterable[Mapping[str, Any]] | None,
        calendar_dates_rows: Iterable[Mapping[str, Any]] | None,
    ) -> ServiceCalendar:
        """Build from GTFS rows (raw strings or typed values).

        An invalid row (bad or missing weekday flag, bad date, ``end_date < start_date``, exception_type not
        1/2, missing service_id) contributes NO service dates: it is skipped and recorded in
        :attr:`warnings` as ``INVALID_CALENDAR`` - the same outcome as the Spark ``dim_service_dates``.
        """
        cal = cls()
        for row in calendar_rows or ():
            sid = _service_id(row)
            try:
                if sid is None:
                    raise ValueError("missing service_id")
                rule = _CalendarRule(
                    service_id=sid,
                    weekdays=tuple(_flag(row.get(day)) for day in WEEKDAY_COLUMNS),
                    start_date=_as_date(row.get("start_date")),
                    end_date=_as_date(row.get("end_date")),
                )
                if rule.end_date < rule.start_date:
                    raise ValueError("end_date < start_date")
            except ValueError as exc:
                cal._skip("calendar.txt", sid, exc)
                continue
            cal.rules.setdefault(sid, []).append(rule)
        for row in calendar_dates_rows or ():
            sid = _service_id(row)
            try:
                if sid is None:
                    raise ValueError("missing service_id")
                d = _as_date(row.get("date"))
                removed = _as_exception(row.get("exception_type")) == EXCEPTION_REMOVED
            except ValueError as exc:
                cal._skip("calendar_dates.txt", sid, exc)
                continue
            (cal.removed if removed else cal.added).setdefault(sid, set()).add(d)
        return cal

    def _skip(self, table: str, service_id: str | None, exc: ValueError) -> None:
        self.warnings.append(
            {"code": "INVALID_CALENDAR", "table": table, "service_id": service_id, "detail": str(exc)}
        )

    @property
    def service_ids(self) -> set[str]:
        return set(self.rules) | set(self.added) | set(self.removed)

    def is_active(self, service_id: str, d: date) -> bool:
        """Calendar rule with exceptions (removed wins over added, added wins over the weekly rule)."""
        if d in self.removed.get(service_id, ()):
            return False
        if d in self.added.get(service_id, ()):
            return True
        return any(rule.covers(d) for rule in self.rules.get(service_id, ()))

    def active_services(self, d: date) -> set[str]:
        return {sid for sid in self.service_ids if self.is_active(sid, d)}

    def service_dates(self, service_id: str) -> list[tuple[date, str]]:
        """All active dates of a service with their source (``calendar`` or ``added``), sorted.

        Mirrors ``dim_service_dates``: weekly dates inside each calendar range, plus added dates not
        already produced by the weekly rule, minus removed dates.
        """
        out: dict[date, str] = {}
        for rule in self.rules.get(service_id, ()):
            d = rule.start_date
            while d <= rule.end_date:
                if rule.weekdays[d.weekday()]:
                    out.setdefault(d, "calendar")
                d += timedelta(days=1)
        for d in self.added.get(service_id, ()):
            out.setdefault(d, "added")
        for d in self.removed.get(service_id, ()):
            out.pop(d, None)
        return sorted(out.items())


def active_services(
    calendar_rows: Iterable[Mapping[str, Any]] | None,
    calendar_dates_rows: Iterable[Mapping[str, Any]] | None,
    d: date,
) -> set[str]:
    """Service ids active on service date ``d`` (interfaces §3)."""
    return ServiceCalendar.from_rows(calendar_rows, calendar_dates_rows).active_services(d)


def is_service_active(
    service_id: str,
    calendar_rows: Iterable[Mapping[str, Any]] | None,
    calendar_dates_rows: Iterable[Mapping[str, Any]] | None,
    d: date,
) -> bool:
    """Whether one service is active on ``d``."""
    return ServiceCalendar.from_rows(calendar_rows, calendar_dates_rows).is_active(service_id, d)


def service_dates(
    calendar_rows: Iterable[Mapping[str, Any]] | None,
    calendar_dates_rows: Iterable[Mapping[str, Any]] | None,
) -> dict[str, list[tuple[date, str]]]:
    """``{service_id: [(date, source), ...]}`` for every service (pure-Python ``dim_service_dates``)."""
    cal = ServiceCalendar.from_rows(calendar_rows, calendar_dates_rows)
    return {sid: cal.service_dates(sid) for sid in sorted(cal.service_ids)}
