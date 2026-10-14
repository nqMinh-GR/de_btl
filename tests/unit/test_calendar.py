"""Service calendar rule with exceptions (design C §3) on the golden fixture's calendar."""

from __future__ import annotations

from datetime import date

import pytest

from transitpulse.static.calendar import (
    ServiceCalendar,
    active_services,
    is_service_active,
    service_dates,
)
from transitpulse.static.validation import read_csv_text

CALENDAR_TXT = """service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
WKDY,1,1,1,1,1,0,0,20260901,20261231
DAILY,1,1,1,1,1,1,1,20260901,20261231
"""
CALENDAR_DATES_TXT = """service_id,date,exception_type
WKDY,20261012,2
WKDY,20261010,1
"""


@pytest.fixture()
def rows() -> tuple[list[dict], list[dict]]:
    return read_csv_text(CALENDAR_TXT)[1], read_csv_text(CALENDAR_DATES_TXT)[1]


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        (date(2026, 10, 5), {"WKDY", "DAILY"}),     # Monday
        (date(2026, 10, 9), {"WKDY", "DAILY"}),     # Friday
        (date(2026, 10, 10), {"WKDY", "DAILY"}),    # Saturday, WKDY added by exception_type 1
        (date(2026, 10, 11), {"DAILY"}),            # Sunday
        (date(2026, 10, 12), {"DAILY"}),            # Monday, WKDY removed by exception_type 2
        (date(2026, 10, 17), {"DAILY"}),            # an ordinary Saturday
        (date(2026, 9, 1), {"WKDY", "DAILY"}),      # first day (inclusive)
        (date(2026, 12, 31), {"WKDY", "DAILY"}),    # last day (inclusive, Thursday)
        (date(2026, 8, 31), set()),                 # before the range
        (date(2027, 1, 1), set()),                  # after the range
    ],
)
def test_active_services(rows, day: date, expected: set[str]) -> None:
    cal, cd = rows
    assert active_services(cal, cd, day) == expected


def test_golden_spot_checks(rows) -> None:
    cal, cd = rows
    assert is_service_active("WKDY", cal, cd, date(2026, 10, 5)) is True
    assert is_service_active("WKDY", cal, cd, date(2026, 10, 10)) is True
    assert is_service_active("WKDY", cal, cd, date(2026, 10, 11)) is False
    assert is_service_active("WKDY", cal, cd, date(2026, 10, 12)) is False
    assert is_service_active("UNKNOWN", cal, cd, date(2026, 10, 5)) is False


def test_service_dates_counts_match_dim_service_dates(rows) -> None:
    cal, cd = rows
    out = service_dates(cal, cd)
    assert len(out["WKDY"]) == 88          # 88 weekdays - Oct 12 + Oct 10
    assert len(out["DAILY"]) == 122
    sources = dict(out["WKDY"])
    assert sources[date(2026, 10, 10)] == "added"
    assert sources[date(2026, 10, 5)] == "calendar"
    assert date(2026, 10, 12) not in sources
    assert [d for d, _ in out["WKDY"]] == sorted(d for d, _ in out["WKDY"])


def test_removed_wins_over_added_and_added_on_active_day_is_not_duplicated() -> None:
    cal = [{"service_id": "S", "monday": "1", "tuesday": "0", "wednesday": "0", "thursday": "0", "friday": "0",
            "saturday": "0", "sunday": "0", "start_date": "20261005", "end_date": "20261012"}]
    cd = [
        {"service_id": "S", "date": "20261006", "exception_type": "1"},
        {"service_id": "S", "date": "20261006", "exception_type": "2"},
        {"service_id": "S", "date": "20261005", "exception_type": "1"},
    ]
    sc = ServiceCalendar.from_rows(cal, cd)
    assert sc.is_active("S", date(2026, 10, 6)) is False
    assert sc.service_dates("S") == [(date(2026, 10, 5), "calendar"), (date(2026, 10, 12), "calendar")]


def test_calendar_dates_only_service() -> None:
    cd = [{"service_id": "HOLIDAY", "date": "20261126", "exception_type": "1"}]
    assert active_services([], cd, date(2026, 11, 26)) == {"HOLIDAY"}
    assert active_services(None, cd, date(2026, 11, 27)) == set()


def test_typed_rows_are_accepted() -> None:
    cal = [{"service_id": "T", "monday": True, "tuesday": False, "wednesday": 0, "thursday": 1, "friday": False,
            "saturday": False, "sunday": False, "start_date": date(2026, 10, 1), "end_date": date(2026, 10, 31)}]
    sc = ServiceCalendar.from_rows(cal, [{"service_id": "T", "date": date(2026, 10, 5), "exception_type": 2}])
    assert sc.warnings == []
    assert sc.is_active("T", date(2026, 10, 5)) is False      # Monday removed
    assert sc.is_active("T", date(2026, 10, 12)) is True      # Monday
    assert sc.is_active("T", date(2026, 10, 8)) is True       # Thursday
    assert sc.is_active("T", date(2026, 10, 9)) is False      # Friday flag 0


def test_weekday_flag_alone_never_makes_a_service_active() -> None:
    """Outside start..end the weekday flag does not matter (never infer from the name or one weekday)."""
    cal = [{"service_id": "WKDY", "monday": "1", "tuesday": "1", "wednesday": "1", "thursday": "1", "friday": "1",
            "saturday": "0", "sunday": "0", "start_date": "20261005", "end_date": "20261009"}]
    assert active_services(cal, [], date(2026, 10, 12)) == set()


_GOOD_ROW = {"service_id": "X", "monday": "1", "tuesday": "1", "wednesday": "1", "thursday": "1", "friday": "1",
             "saturday": "0", "sunday": "0", "start_date": "20261001", "end_date": "20261031"}


@pytest.mark.parametrize(
    ("cal", "cd", "table"),
    [
        ([{**_GOOD_ROW, "saturday": "2"}], [], "calendar.txt"),                 # invalid flag on ANOTHER day
        ([{**_GOOD_ROW, "sunday": None}], [], "calendar.txt"),                  # missing (null) flag
        ([{**_GOOD_ROW, "sunday": ""}], [], "calendar.txt"),                    # blank flag
        ([{**_GOOD_ROW, "start_date": "20261031", "end_date": "20261001"}], [], "calendar.txt"),
        ([{**_GOOD_ROW, "start_date": "2026-10-01"}], [], "calendar.txt"),
        ([], [{"service_id": "X", "date": "20261005", "exception_type": "3"}], "calendar_dates.txt"),
        ([], [{"service_id": "X", "date": "20261332", "exception_type": "1"}], "calendar_dates.txt"),
    ],
)
def test_invalid_rows_contribute_no_service_dates(cal: list[dict], cd: list[dict], table: str) -> None:
    """INVALID_CALENDAR rows are skipped with a warning (same as Spark dim_service_dates), never raised."""
    sc = ServiceCalendar.from_rows(cal, cd)
    assert [(w["code"], w["table"], w["service_id"]) for w in sc.warnings] == [("INVALID_CALENDAR", table, "X")]
    assert sc.service_dates("X") == []
    assert sc.is_active("X", date(2026, 10, 5)) is False                  # a Monday inside the range


def test_invalid_row_does_not_hide_valid_rows() -> None:
    cal = [{**_GOOD_ROW, "saturday": "2"}, {**_GOOD_ROW, "service_id": "Y"}]
    cd = [{"service_id": "Y", "date": "20261010", "exception_type": "1"},
          {"service_id": "X", "date": "20261005", "exception_type": "9"}]
    sc = ServiceCalendar.from_rows(cal, cd)
    assert len(sc.warnings) == 2
    assert active_services(cal, cd, date(2026, 10, 5)) == {"Y"}
    assert active_services(cal, cd, date(2026, 10, 10)) == {"Y"}             # added Saturday
    assert len(sc.service_dates("Y")) == 23 and sc.service_dates("X") == []


def test_expected_json_active_checks_and_counts(golden_dir) -> None:
    """static.active_checks and static.summary.service_dates of tests/fixtures/golden/expected.json."""
    import json

    expected = json.loads((golden_dir / "expected.json").read_text(encoding="utf-8"))["static"]
    static = golden_dir / "static"
    sc = ServiceCalendar.from_rows(
        read_csv_text((static / "calendar.txt").read_text(encoding="utf-8"))[1],
        read_csv_text((static / "calendar_dates.txt").read_text(encoding="utf-8"))[1],
    )
    checks = expected["active_checks"]
    assert checks, "expected.json has no active_checks"
    for chk in checks:
        assert sc.is_active(chk["service_id"], date.fromisoformat(chk["date"])) is chk["active"], chk
    counts = expected["summary"]["service_dates"]
    assert len(sc.service_dates("WKDY")) == counts["WKDY"]
    assert len(sc.service_dates("DAILY")) == counts["DAILY"]
    assert len(sc.service_dates("WKDY")) + len(sc.service_dates("DAILY")) == counts["total"]
