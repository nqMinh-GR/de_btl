"""Pure helpers of storage/cassandra.py (no cluster)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import UNSET_VALUE

from transitpulse.storage import cassandra as cs


def test_consistency_level_mapping():
    assert cs.consistency_level("LOCAL_ONE") == ConsistencyLevel.LOCAL_ONE
    assert cs.consistency_level("serial") == ConsistencyLevel.SERIAL
    with pytest.raises(ValueError):
        cs.consistency_level("ALMOST_ONE")


def test_time_helpers_are_exact():
    dt = datetime(2026, 10, 5, 12, 0, 30, 123456, tzinfo=UTC)
    assert cs.ms_floor(dt) == datetime(2026, 10, 5, 12, 0, 30, 123000, tzinfo=UTC)
    assert cs.epoch_ms(dt) == 1791201630123
    assert cs.epoch_ms(datetime(2026, 10, 5, 12, 0, 30, 123000)) == 1791201630123   # naive = UTC
    est = timezone(timedelta(hours=-4))
    assert cs.to_utc(datetime(2026, 10, 5, 8, 0, 30, tzinfo=est)) == datetime(2026, 10, 5, 12, 0, 30, tzinfo=UTC)
    assert cs.to_utc(None) is None


def test_normalize_row_converts_driver_types():
    class SortedSet(list):          # name-based detection like cassandra.util.SortedSet
        pass

    class Date:                     # mimics cassandra.util.Date
        def date(self):
            return date(2026, 10, 5)

    row = cs.normalize_row({
        "ts": datetime(2026, 10, 5, 12, 0),
        "flags": SortedSet(["B", "A"]),
        "plain_set": {"z", "y"},
        "d": Date(),
        "m": {"k": 1},
        "n": None,
        "l": ["x"],
    })
    assert row["ts"] == datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    assert row["flags"] == ["A", "B"] and row["plain_set"] == ["y", "z"]
    assert row["d"] == date(2026, 10, 5)
    assert row["m"] == {"k": 1} and row["n"] is None and row["l"] == ["x"]
    assert cs.normalize_row(None) is None


def test_unset_if_none():
    assert cs.unset_if_none([1, None, "a"]) == (1, UNSET_VALUE, "a")
