"""Structural consistency of the hand-written golden fixture (no oracle, no project code).

* every file referenced by timeline.json exists and every rt/*.json is referenced;
* RT documents parse, have the MBTA JSON header shape, integer epoch timestamps and string IDs;
  the only deliberately malformed value is ALERTS3/A5 ``active_period.start = "abc"``;
* static CSVs have header rows, the required columns, rectangular rows and the documented ID sets;
* every epoch <-> ISO pair in PROVENANCE.md and expected.json is arithmetically correct, every epoch used in
  the RT files is documented, and every ISO instant of the fixture day in expected/timeline is documented;
* expected.json runs/steps/inputs/clocks align with timeline.json; capture metadata matches the files.
"""

from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

GOLDEN = Path(__file__).resolve().parents[1] / "fixtures" / "golden"
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
E0 = 1791201600
ISO_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
FEED_TYPES = ("vehicle_positions", "trip_updates", "alerts")
ITEM_KINDS = {"tick", "capture", "republish", "malformed_envelope", "barrier"}
ID_KEYS = {"id", "trip_id", "route_id", "stop_id", "agency_id", "start_date", "start_time", "label"}
INT_KEYS = {"timestamp", "time", "start", "end", "direction_id", "route_type", "stop_sequence",
            "current_stop_sequence", "uncertainty", "delay"}
ENUM_VALUES = {
    "schedule_relationship": {"SCHEDULED", "ADDED", "UNSCHEDULED", "CANCELED", "SKIPPED", "NO_DATA", "DUPLICATED"},
    "current_status": {"INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"},
}
STATIC_REQUIRED = {
    "agency": ("agency_id", "agency_name", "agency_url", "agency_timezone"),
    "routes": ("route_id", "agency_id", "route_short_name", "route_long_name", "route_type"),
    "stops": ("stop_id", "stop_name", "stop_lat", "stop_lon"),
    "trips": ("route_id", "service_id", "trip_id", "direction_id", "shape_id"),
    "stop_times": ("trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"),
    "calendar": ("service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
                 "start_date", "end_date"),
    "calendar_dates": ("service_id", "date", "exception_type"),
    "shapes": ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"),
    "feed_info": ("feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_start_date", "feed_end_date",
                  "feed_version"),
}
FIXTURE_DAY_START = datetime(2026, 10, 5, tzinfo=UTC)
FIXTURE_DAY_END = datetime(2026, 10, 7, tzinfo=UTC)


def _load(name: str) -> Any:
    return json.loads((GOLDEN / name).read_text(encoding="utf-8"))


TIMELINE = _load("timeline.json")
EXPECTED = _load("expected.json")
RT_FILES = sorted((GOLDEN / "rt").glob("*.json"))


def iso_of(epoch: int) -> str:
    seconds = epoch // 1000 if epoch > 100_000_000_000 else epoch
    return (EPOCH + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)


def epoch_of(text: str) -> int:
    delta = parse_iso(text) - EPOCH
    return delta.days * 86400 + delta.seconds


def walk(obj: Any, path: str = "$"):
    """Yield (path, key, value) for every dict entry and (path, None, value) for list items/leaves."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield path, k, v
            yield from walk(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from walk(v, f"{path}[{i}]")


def strings(obj: Any):
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for k, v in obj.items():
            yield k
            yield from strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from strings(v)


def provenance_table() -> dict[int, str]:
    text = (GOLDEN / "PROVENANCE.md").read_text(encoding="utf-8")
    rows = re.findall(r"^\|\s*`?(\d{10}|\d{13})`?\s*\|\s*`?(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)`?\s*\|", text, re.M)
    return {int(e): iso for e, iso in rows}


def capture_items() -> list[tuple[str, dict]]:
    out = []
    for run_name, run in TIMELINE["runs"].items():
        for step in run["steps"]:
            for item in step["items"]:
                if item["kind"] == "capture":
                    out.append((run_name, item))
    return out


# --------------------------------------------------------------------------------------------------
# Provenance and epochs
# --------------------------------------------------------------------------------------------------

def test_e0_is_documented_instant():
    assert iso_of(E0) == "2026-10-05T12:00:00Z"
    assert datetime(2026, 10, 5, tzinfo=UTC).weekday() == 0      # Monday -> WKDY active


def test_provenance_declares_synthetic_not_mbta():
    text = (GOLDEN / "PROVENANCE.md").read_text(encoding="utf-8")
    assert "NOT MBTA" in text
    assert "NOT MBTA" in (GOLDEN / "static" / "agency.txt").read_text(encoding="utf-8")
    assert "NOT MBTA" in (GOLDEN / "static" / "feed_info.txt").read_text(encoding="utf-8")
    assert "NOT MBTA" in TIMELINE["provenance"] and "NOT MBTA" in EXPECTED["provenance"]


def test_provenance_epoch_table_is_correct():
    table = provenance_table()
    assert len(table) >= 30, "PROVENANCE.md must contain the epoch table"
    wrong = {e: iso for e, iso in table.items() if iso_of(e) != iso}
    assert not wrong, f"epoch/ISO mismatch in PROVENANCE.md: {wrong}"


def test_expected_epoch_table_is_correct_and_matches_provenance():
    epochs = {int(k): v for k, v in EXPECTED["epochs"].items()}
    wrong = {e: iso for e, iso in epochs.items() if iso_of(e) != iso}
    assert not wrong, f"epoch/ISO mismatch in expected.json: {wrong}"
    assert epochs == provenance_table()


def test_every_rt_epoch_is_documented():
    documented = set(provenance_table())
    missing = set()
    for path in RT_FILES:
        for _p, key, value in walk(json.loads(path.read_text(encoding="utf-8"))):
            if isinstance(value, int) and not isinstance(value, bool) and value >= 1_000_000_000:
                if value not in documented:
                    missing.add((path.name, key, value))
    assert not missing, f"undocumented epochs: {sorted(missing)}"


def test_every_fixture_day_iso_is_documented():
    documented_iso = set(provenance_table().values())
    missing = set()
    for name, doc in (("expected.json", EXPECTED), ("timeline.json", TIMELINE)):
        for s in strings(doc):
            if ISO_Z.match(s):
                instant = parse_iso(s)
                if FIXTURE_DAY_START <= instant < FIXTURE_DAY_END and s not in documented_iso:
                    missing.add((name, s))
    assert not missing, f"ISO instants of the fixture day missing from the PROVENANCE table: {sorted(missing)}"


def test_all_iso_strings_parse():
    for doc in (EXPECTED, TIMELINE):
        for s in strings(doc):
            if re.match(r"^\d{4}-\d{2}-\d{2}T", s):
                parse_iso(s)     # raises on malformed values


# --------------------------------------------------------------------------------------------------
# Timeline
# --------------------------------------------------------------------------------------------------

def test_timeline_header():
    assert TIMELINE["fixture_id"] == EXPECTED["fixture_id"] == "golden-v1"
    assert TIMELINE["agency_timezone"] == EXPECTED["agency_timezone"] == "America/New_York"
    assert TIMELINE["scope_route_ids"] == EXPECTED["scope_route_ids"] == ["R_A", "R_B"]
    assert set(TIMELINE["runs"]) == {"golden", "IT06", "IT07", "IT08", "IT09"}


@pytest.mark.parametrize("run_name", sorted(TIMELINE["runs"]))
def test_timeline_run_structure(run_name: str):
    run = TIMELINE["runs"][run_name]
    assert ISO_Z.match(run["start_clock"])
    assert all(isinstance(r, str) for r in run["scope_route_ids"])
    labels: set[str] = set()
    clock = parse_iso(run["start_clock"])
    step_names = [s["step"] for s in run["steps"]]
    assert len(step_names) == len(set(step_names))
    for step in run["steps"]:
        assert step["items"], f"{run_name}/{step['step']} has no items"
        for item in step["items"]:
            kind = item["kind"]
            assert kind in ITEM_KINDS, item
            if "at" in item:
                assert ISO_Z.match(item["at"]), item
                if kind == "tick":
                    assert parse_iso(item["at"]) >= clock, "ticks never move the clock backwards"
                clock = max(clock, parse_iso(item["at"]))
            if kind == "capture":
                assert item["feed_type"] in FEED_TYPES
                assert (GOLDEN / item["file"]).is_file(), item["file"]
                assert item["label"] not in labels, f"duplicate capture label {item['label']}"
                labels.add(item["label"])
            elif kind == "republish":
                assert item["label"] in labels, f"republish of a label not captured earlier: {item['label']}"
            elif kind == "malformed_envelope":
                assert item["feed_type"] in FEED_TYPES
                with pytest.raises(ValueError):
                    json.loads(item["value_text"])


def test_every_rt_file_is_referenced_and_labels_map_to_one_file():
    referenced = {item["file"] for _run, item in capture_items()}
    assert referenced == {f"rt/{p.name}" for p in RT_FILES}
    label_to_file: dict[str, str] = {}
    for _run, item in capture_items():
        assert label_to_file.setdefault(item["label"], item["file"]) == item["file"]


def test_expected_runs_align_with_timeline():
    assert set(EXPECTED["runs"]) == set(TIMELINE["runs"])
    for name, run in TIMELINE["runs"].items():
        exp = EXPECTED["runs"][name]
        assert exp["start_clock"] == run["start_clock"]
        assert exp["scope_route_ids"] == run["scope_route_ids"]
        assert [s["step"] for s in exp["steps"]] == [s["step"] for s in run["steps"]]
        clock = parse_iso(run["start_clock"])
        for exp_step, step in zip(exp["steps"], run["steps"]):
            for item in step["items"]:
                if "at" in item:
                    clock = max(clock, parse_iso(item["at"]))
            assert exp_step["inputs"] == [i["label"] for i in step["items"] if "label" in i], (name, step["step"])
            assert parse_iso(exp_step["clock"]) == clock, (name, step["step"])
            pointer = exp_step["expect"]["pointer"]
            assert set(pointer) == set(FEED_TYPES)


# --------------------------------------------------------------------------------------------------
# Realtime documents
# --------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("path", RT_FILES, ids=lambda p: p.name)
def test_rt_document_shape(path: Path):
    doc = json.loads(path.read_text(encoding="utf-8"))
    header = doc["header"]
    assert header["gtfs_realtime_version"] == "2.0"
    assert header["incrementality"] == "FULL_DATASET"
    assert isinstance(header["timestamp"], int) and not isinstance(header["timestamp"], bool)
    assert E0 - 3600 <= header["timestamp"] <= E0 + 3600
    assert isinstance(doc["entity"], list) and doc["entity"]
    entity_ids = [e["id"] for e in doc["entity"]]
    assert len(entity_ids) == len(set(entity_ids)), "entity ids must be unique within a snapshot"
    for p, key, value in walk(doc):
        if key in ID_KEYS and value is not None:
            assert isinstance(value, str) and value == value.strip() and value, (path.name, p, key, value)
        if key in ENUM_VALUES:
            assert value in ENUM_VALUES[key], (path.name, p, key, value)
        if key in INT_KEYS and value is not None:
            malformed = path.name == "ALERTS3.json" and key == "start" and value == "abc"
            if not malformed:
                assert isinstance(value, int) and not isinstance(value, bool), (path.name, p, key, value)
        if key in ("latitude", "longitude"):
            assert isinstance(value, (int, float)) and not isinstance(value, bool), (path.name, p, key, value)
        if key == "start_date":
            assert re.fullmatch(r"\d{8}", value), value
        if key == "start_time":
            assert re.fullmatch(r"\d{2}:\d{2}:\d{2}", value), value


def test_alerts3_has_exactly_one_malformed_entity():
    doc = _load("rt/ALERTS3.json")
    malformed = [
        e["id"] for e in doc["entity"]
        for period in e["alert"].get("active_period", [])
        for v in period.values() if not isinstance(v, int)
    ]
    assert malformed == ["A5"]


def test_expected_captures_match_files_and_timeline():
    captures = EXPECTED["captures"]
    timeline_items = capture_items()
    assert set(captures) == {item["label"] for _run, item in timeline_items}
    for _run, item in timeline_items:
        meta = captures[item["label"]]
        assert meta["file"] == item["file"] and meta["feed_type"] == item["feed_type"]
        assert meta["fetched_at"] == item["at"]
        doc = _load(item["file"])
        assert meta["header_timestamp"] == doc["header"]["timestamp"]
        assert meta["header_iso"] == iso_of(meta["header_timestamp"])
        assert parse_iso(meta["header_iso"]) <= parse_iso(meta["fetched_at"])
        assert meta["source_entity_count"] == len(doc["entity"])
        assert meta["candidate_count"] == _count_candidates(item["feed_type"], doc["entity"])


def _count_candidates(feed_type: str, entities: list[dict]) -> int:
    """Units of md §16.2 (restated from contracts/accounting.py, not imported)."""
    n = 0
    for e in entities:
        if feed_type == "vehicle_positions":
            n += 1
        elif feed_type == "trip_updates":
            for stu in (e.get("trip_update") or {}).get("stop_time_update") or []:
                n += int(isinstance(stu.get("arrival"), dict)) + int(isinstance(stu.get("departure"), dict))
        else:
            n += len((e.get("alert") or {}).get("informed_entity") or [])
    return n


# --------------------------------------------------------------------------------------------------
# Static CSV files
# --------------------------------------------------------------------------------------------------

def _read_csv(name: str) -> tuple[list[str], list[list[str]]]:
    with (GOLDEN / "static" / f"{name}.txt").open("r", encoding="utf-8", newline="") as fh:
        rows = list(csv.reader(fh))
    return rows[0], rows[1:]


@pytest.mark.parametrize("name", sorted(STATIC_REQUIRED))
def test_static_csv_headers_and_rows(name: str):
    header, rows = _read_csv(name)
    assert header == [h.strip() for h in header] and not header[0].startswith("﻿")
    missing = [c for c in STATIC_REQUIRED[name] if c not in header]
    assert not missing, f"{name}.txt missing columns {missing}"
    assert rows, f"{name}.txt has no data rows"
    for row in rows:
        assert len(row) == len(header), f"{name}.txt row {row} is not rectangular"
        for col, value in zip(header, row):
            assert value == value.strip(), (name, col, value)
            if col.endswith("_id") and col not in ("parent_station",):
                assert value != "" or col in ("agency_id",), (name, col, row)


def _column(name: str, col: str) -> list[str]:
    header, rows = _read_csv(name)
    idx = header.index(col)
    return [r[idx] for r in rows]


def test_static_id_sets_and_keys():
    assert _column("agency", "agency_id") == ["SYN"]
    assert _column("routes", "route_id") == ["R_A", "R_B", "R_X"]
    assert sorted(_column("stops", "stop_id")) == ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S9"]
    assert sorted(_column("trips", "trip_id")) == ["T1", "T2", "T3", "T4", "T5", "T6", "T7", "TX"]
    assert sorted(set(_column("shapes", "shape_id"))) == ["SH_A0", "SH_A1", "SH_B0", "SH_B1", "SH_X"]
    assert _column("calendar", "service_id") == ["WKDY", "DAILY"]
    assert len(_column("stop_times", "trip_id")) == 28
    assert len(_column("shapes", "shape_id")) == 17
    header, rows = _read_csv("stop_times")
    keys = [(r[header.index("trip_id")], r[header.index("stop_sequence")]) for r in rows]
    assert len(keys) == len(set(keys))
    stop_ids = set(_column("stops", "stop_id"))
    trip_ids = set(_column("trips", "trip_id"))
    assert {r[header.index("stop_id")] for r in rows} <= stop_ids
    assert {r[header.index("trip_id")] for r in rows} == trip_ids


def test_static_special_cases_present():
    header, rows = _read_csv("stop_times")
    t4 = [r for r in rows if r[0] == "T4"]
    assert {r[header.index("arrival_time")] for r in t4} >= {"24:00:00", "25:10:00"}
    t6_stops = [r[header.index("stop_id")] for r in rows if r[0] == "T6"]
    assert t6_stops.count("S5") == 2, "T6 is the loop trip revisiting S5"
    assert _read_csv("calendar_dates")[1] == [["WKDY", "20261012", "2"], ["WKDY", "20261010", "1"]]
    routes_header, routes = _read_csv("routes")
    types = {r[0]: r[routes_header.index("route_type")] for r in routes}
    assert types == {"R_A": "3", "R_B": "3", "R_X": "2"}
