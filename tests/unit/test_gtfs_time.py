"""GTFS time parsing and the service-day anchor (design C §2 vectors, incl. DST days and 25:10:00)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from transitpulse.static.gtfs_time import (
    format_gtfs_seconds,
    format_service_date,
    local_date,
    parse_gtfs_time,
    parse_service_date,
    scheduled_utc,
    service_anchor_utc,
    validate_timezone,
)

NY = "America/New_York"


def _utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(UTC)


# (service_date, gtfs time, expected anchor or None, expected scheduled UTC) -- design C §2 table
VECTORS = [
    ("2026-10-05", "08:05:00", "2026-10-05T04:00:00Z", "2026-10-05T12:05:00Z"),
    ("2026-10-05", "25:10:00", "2026-10-05T04:00:00Z", "2026-10-06T05:10:00Z"),
    ("2026-03-08", "00:00:00", "2026-03-08T04:00:00Z", "2026-03-08T04:00:00Z"),
    ("2026-03-08", "01:30:00", None, "2026-03-08T05:30:00Z"),
    ("2026-03-08", "08:05:00", None, "2026-03-08T12:05:00Z"),
    ("2026-11-01", "00:00:00", "2026-11-01T05:00:00Z", "2026-11-01T05:00:00Z"),
    ("2026-11-01", "08:05:00", None, "2026-11-01T13:05:00Z"),
    ("2026-10-31", "25:10:00", "2026-10-31T04:00:00Z", "2026-11-01T05:10:00Z"),
    ("2026-03-07", "25:10:00", "2026-03-07T05:00:00Z", "2026-03-08T06:10:00Z"),
]


@pytest.mark.parametrize(("service_date", "gtfs", "anchor", "expected"), VECTORS)
def test_design_vectors(service_date: str, gtfs: str, anchor: str | None, expected: str) -> None:
    d = date.fromisoformat(service_date)
    secs = parse_gtfs_time(gtfs)
    assert secs is not None
    if anchor is not None:
        assert service_anchor_utc(d, NY) == _utc(anchor)
    got = scheduled_utc(d, secs, NY)
    assert got == _utc(expected)
    assert got.tzinfo is not None and got.utcoffset() == timedelta(0)


@pytest.mark.parametrize(
    ("service_date", "gtfs", "wrong_utc"),
    [
        ("2026-03-08", "00:00:00", "2026-03-08T05:00:00Z"),
        ("2026-03-08", "01:30:00", "2026-03-08T06:30:00Z"),
        ("2026-03-08", "08:05:00", "2026-03-08T13:05:00Z"),
        ("2026-11-01", "00:00:00", "2026-11-01T04:00:00Z"),
        ("2026-11-01", "08:05:00", "2026-11-01T12:05:00Z"),
    ],
)
def test_naive_local_midnight_is_wrong_on_dst_days(service_date: str, gtfs: str, wrong_utc: str) -> None:
    """UTC(local midnight) + seconds gives the 'wrong' column of the design table; the anchor rule differs."""
    d = date.fromisoformat(service_date)
    secs = parse_gtfs_time(gtfs)
    assert secs is not None
    naive_midnight = datetime(d.year, d.month, d.day, tzinfo=ZoneInfo(NY)).astimezone(UTC) + timedelta(seconds=secs)
    assert naive_midnight == _utc(wrong_utc)
    assert scheduled_utc(d, secs, NY) != naive_midnight


def test_fall_back_day_is_25_hours_and_spring_forward_23_hours() -> None:
    assert service_anchor_utc(date(2026, 11, 1), NY) - service_anchor_utc(date(2026, 10, 31), NY) == timedelta(hours=25)
    assert service_anchor_utc(date(2026, 3, 8), NY) - service_anchor_utc(date(2026, 3, 7), NY) == timedelta(hours=23)
    assert service_anchor_utc(date(2026, 10, 6), NY) - service_anchor_utc(date(2026, 10, 5), NY) == timedelta(hours=24)


def test_golden_epoch_reference() -> None:
    """E0 = 1791201600 = 2026-10-05T12:00:00Z used by every golden vector."""
    assert int(_utc("2026-10-05T12:00:00Z").timestamp()) == 1791201600
    assert int(service_anchor_utc(date(2026, 10, 5), NY).timestamp()) == 1791172800
    # T1 seq3 S3 08:05:00 -> 12:05Z; TU1 predicts 1791202260 -> delay 360 s (design C §8)
    assert 1791202260 - int(scheduled_utc(date(2026, 10, 5), parse_gtfs_time("08:05:00") or 0, NY).timestamp()) == 360


@pytest.mark.parametrize(
    ("text", "seconds"),
    [
        ("25:10:00", 90600),
        ("08:05:00", 29100),
        ("8:05:00", 29100),
        (" 07:55:00 ", 28500),
        ("00:00:00", 0),
        ("24:00:00", 86400),
        ("100:00:00", 360000),
        ("23:59:59", 86399),
    ],
)
def test_parse_valid(text: str, seconds: int) -> None:
    assert parse_gtfs_time(text) == seconds


@pytest.mark.parametrize("text", [None, "", "   "])
def test_parse_empty_is_none(text: str | None) -> None:
    assert parse_gtfs_time(text) is None


@pytest.mark.parametrize(
    "text",
    ["08:60:00", "08:05:60", "08:5:00", "08:05", "abc", "-1:00:00", "08:05:00:00", "1000:00:00", "08:05:0a", "8h05"],
)
def test_parse_invalid_raises(text: str) -> None:
    with pytest.raises(ValueError):
        parse_gtfs_time(text)


def test_parse_rejects_non_string() -> None:
    with pytest.raises(ValueError):
        parse_gtfs_time(29100)  # type: ignore[arg-type]


@pytest.mark.parametrize(("seconds", "text"), [(90600, "25:10:00"), (0, "00:00:00"), (86399, "23:59:59"),
                                               (360000, "100:00:00")])
def test_format_and_roundtrip(seconds: int, text: str) -> None:
    assert format_gtfs_seconds(seconds) == text
    assert parse_gtfs_time(format_gtfs_seconds(seconds)) == seconds


@pytest.mark.parametrize("bad", [-1, 1.5, True, "10"])
def test_format_rejects_bad_input(bad: object) -> None:
    with pytest.raises(ValueError):
        format_gtfs_seconds(bad)  # type: ignore[arg-type]


def test_scheduled_rejects_negative_and_naive_inputs() -> None:
    with pytest.raises(ValueError):
        scheduled_utc(date(2026, 10, 5), -5, NY)
    with pytest.raises(ValueError):
        service_anchor_utc(datetime(2026, 10, 5, 12), NY)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        local_date(datetime(2026, 10, 5, 12), NY)


def test_service_date_parse_and_format() -> None:
    assert parse_service_date("20261005") == date(2026, 10, 5)
    assert parse_service_date(" 20261231 ") == date(2026, 12, 31)
    assert format_service_date(date(2026, 3, 8)) == "20260308"
    for bad in ("20261305", "2026105", "20260230", "abcdefgh", "2026-10-05", ""):
        with pytest.raises(ValueError):
            parse_service_date(bad)


def test_local_date_in_agency_timezone() -> None:
    assert local_date(_utc("2026-10-06T03:00:00Z"), NY) == date(2026, 10, 5)
    assert local_date(_utc("2026-10-05T12:00:30Z"), NY) == date(2026, 10, 5)
    assert local_date(_utc("2026-10-06T04:00:00Z"), NY) == date(2026, 10, 6)


def test_timezone_validation() -> None:
    assert str(validate_timezone(NY)) == NY
    for bad in ("Mars/Olympus", "", "America/New York"):
        with pytest.raises(ValueError):
            validate_timezone(bad)


def test_expected_json_time_vectors(golden_dir) -> None:
    """Every hand-written time vector of tests/fixtures/golden/expected.json (static.time_vectors)."""
    import json

    expected = json.loads((golden_dir / "expected.json").read_text(encoding="utf-8"))
    tz = expected["agency_timezone"]
    vectors = expected["static"]["time_vectors"]
    assert vectors, "expected.json has no time_vectors"
    for vec in vectors:
        d = date.fromisoformat(vec["service_date"])
        exp = vec["expected"]
        secs = parse_gtfs_time(vec["gtfs_time"])
        assert secs == exp["seconds"], vec
        assert service_anchor_utc(d, tz) == _utc(exp["anchor_utc"]), vec
        got = scheduled_utc(d, exp["seconds"], tz)
        assert got == _utc(exp["scheduled_utc"]), vec
        local, exp_local = got.astimezone(ZoneInfo(tz)), datetime.fromisoformat(exp["local_time"])
        # PEP 495: == across different tzinfo is always False when utcoffset() depends on
        # fold (the repeated hour on a fall-back day), even when both instants match.
        assert local.replace(tzinfo=None) == exp_local.replace(tzinfo=None), vec
        assert local.utcoffset() == exp_local.utcoffset(), vec
        midnight = datetime(d.year, d.month, d.day, tzinfo=ZoneInfo(tz)).astimezone(UTC)
        assert midnight + timedelta(seconds=exp["seconds"]) == _utc(exp["wrong_local_midnight_utc"]), vec
