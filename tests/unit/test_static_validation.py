"""Static validation helpers, ZIP store/inspection, scope index and catalog row shaping (pure Python)."""

from __future__ import annotations

import json
import stat
import zipfile
from pathlib import Path

import pytest

from transitpulse.runtime.paths import WorkPaths
from transitpulse.static.index import ScopeIndex, build_scope_index_doc, load_scope_index, write_scope_index
from transitpulse.static.ingest import (
    ArtifactError,
    UsageError,
    extract_gtfs,
    get_static_version,
    ingest_zip,
    inspect_zip,
    read_feed_info,
)
from transitpulse.static.loader import normalize_route_ids, read_trip_to_route
from transitpulse.static.routes import bus_routes_from_zip, count_vehicles_by_route
from transitpulse.static.validation import (
    CHECK_RULES,
    ERROR,
    FILE_SPECS,
    INFO,
    REQUIRED_FILES,
    STRUCTURAL_CODES,
    WARNING,
    calendar_date_row_issues,
    calendar_row_issues,
    coordinates_required,
    coordinates_valid,
    find_duplicate_keys,
    make_check,
    missing_columns,
    normalize_header,
    peek_csv,
    read_csv_text,
    severity_counts,
    status_from_checks,
    stop_time_issues,
)
from transitpulse.storage.static_writer import (
    STATIC_TABLE_COLUMNS,
    downsample_points,
    geometry_row,
    insert_cql,
    row_params,
)

GOLDEN_FILES = ("agency", "routes", "trips", "stops", "stop_times", "calendar", "calendar_dates", "shapes",
                "feed_info")


def _golden_zip(golden_dir: Path, target: Path) -> Path:
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name in GOLDEN_FILES:
            info = zipfile.ZipInfo(f"{name}.txt", date_time=(2026, 9, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, (golden_dir / "static" / f"{name}.txt").read_bytes())
    return target


# ------------------------------------------------------------------------------------------ CSV helpers

def test_read_csv_text_strips_bom_whitespace_and_blanks() -> None:
    header, rows = read_csv_text("﻿ route_id , route_type ,agency_id\nR1, 3 ,\n\n R2 ,2,A\n")
    assert header == ["route_id", "route_type", "agency_id"]
    assert rows == [{"route_id": "R1", "route_type": "3", "agency_id": None},
                    {"route_id": "R2", "route_type": "2", "agency_id": "A"}]


def test_normalize_header_and_missing_columns() -> None:
    assert normalize_header(["﻿trip_id", " route_id "]) == ["trip_id", "route_id"]
    assert missing_columns(["﻿trip_id", "route_id"], ("trip_id", "route_id", "service_id")) == ["service_id"]


def test_peek_csv(tmp_path: Path) -> None:
    empty = tmp_path / "empty.txt"
    empty.write_bytes(b"")
    header_only = tmp_path / "header.txt"
    header_only.write_text("﻿service_id,date,exception_type\n\n", encoding="utf-8")
    data = tmp_path / "data.txt"
    data.write_text("service_id,date,exception_type\nWKDY,20261012,2\n", encoding="utf-8")
    assert peek_csv(tmp_path / "missing.txt") == (None, False)
    assert peek_csv(empty) == (None, False)
    assert peek_csv(header_only) == (["service_id", "date", "exception_type"], False)
    assert peek_csv(data) == (["service_id", "date", "exception_type"], True)


def test_file_specs() -> None:
    assert set(REQUIRED_FILES) == {"agency", "routes", "trips", "stops", "stop_times"}
    assert FILE_SPECS["stop_times"].key_columns == ("trip_id", "stop_sequence")
    assert "arrival_time" in FILE_SPECS["stop_times"].required_columns
    assert FILE_SPECS["feed_info"].required_columns == ()


# ------------------------------------------------------------------------------------------ rules

@pytest.mark.parametrize(
    ("lat", "lon", "ok"),
    [("42.35", "-71.06", True), ("90", "180", True), ("-90", "-180", True), ("0", "0", False),
     ("91", "-71", False), ("42", "-181", False), (None, "-71", False), ("abc", "-71", False), ("", "", False)],
)
def test_coordinates_valid(lat, lon, ok: bool) -> None:
    assert coordinates_valid(lat, lon) is ok


def test_coordinates_required_by_location_type() -> None:
    assert coordinates_required(None) and coordinates_required("") and coordinates_required("0")
    assert coordinates_required("1") and coordinates_required("2")
    assert not coordinates_required("3") and not coordinates_required("4")


def test_find_duplicate_keys() -> None:
    _, rows = read_csv_text("trip_id,stop_sequence\nT1,1\nT1,2\nT1,1\nT2,1\n")
    assert find_duplicate_keys(rows, ("trip_id", "stop_sequence")) == [("T1", "1")]
    assert find_duplicate_keys(rows, ("trip_id",)) == [("T1",)]


GOLDEN_LIKE_STOP_TIMES = """trip_id,arrival_time,departure_time,stop_id,stop_sequence
T4,19:50:00,19:50:00,S1,1
T4,22:00:00,22:00:00,S2,2
T4,24:00:00,24:00:00,S3,3
T4,25:10:00,25:10:00,S4,4
T6,08:00:00,08:00:00,S5,1
T6,08:03:00,08:03:00,S6,2
T6,08:06:00,08:06:00,S7,3
T6,08:09:00,08:09:00,S5,4
T1,07:55:00,07:55:00,S1,1
T1,,,S2,2
T1,08:05:00,08:05:00,S3,3
BACK,08:10:00,08:10:00,S1,1
BACK,08:05:00,08:05:00,S2,2
DEPB,08:10:00,08:05:00,S1,1
DEPB,08:20:00,08:20:00,S2,2
NOEND,08:00:00,08:00:00,S1,1
NOEND,,,S2,2
BADT,08:61:00,08:00:00,S1,1
BADT,09:00:00,09:00:00,S2,2
BADSEQ,08:00:00,08:00:00,S1,x
"""


def test_stop_time_issues() -> None:
    _, rows = read_csv_text(GOLDEN_LIKE_STOP_TIMES)
    issues = stop_time_issues(rows)
    assert issues["T4"] == {"SERVICE_TIME_GE_24H"}
    assert issues["T6"] == {"REPEATED_STOP_IN_TRIP"}
    assert "T1" not in issues                                   # empty intermediate time is allowed
    assert issues["BACK"] == {"NON_MONOTONIC_STOP_TIMES"}
    assert issues["DEPB"] == {"NON_MONOTONIC_STOP_TIMES"}         # departure before arrival
    assert issues["NOEND"] == {"FIRST_LAST_STOP_TIME_MISSING"}
    assert "INVALID_GTFS_TIME" in issues["BADT"]
    assert issues["BADSEQ"] == {"INVALID_FIELD_VALUE"}


def test_calendar_row_issues() -> None:
    good = {"service_id": "W", "monday": "1", "tuesday": "1", "wednesday": "1", "thursday": "1", "friday": "1",
            "saturday": "0", "sunday": "0", "start_date": "20260901", "end_date": "20261231"}
    assert calendar_row_issues(good) == []
    assert calendar_row_issues({**good, "end_date": "20260801"}) == ["end_date < start_date"]
    assert calendar_row_issues({**good, "monday": "2"})
    assert calendar_row_issues({**good, "start_date": "2026-09-01"})
    assert calendar_date_row_issues({"service_id": "W", "date": "20261012", "exception_type": "2"}) == []
    assert calendar_date_row_issues({"service_id": "W", "date": "20261012", "exception_type": "3"})
    assert calendar_date_row_issues({"service_id": "W", "date": "20261340", "exception_type": "1"})


def test_check_severity_and_status() -> None:
    in_err = make_check("DUPLICATE_KEY", "trips.txt", in_scope=True, count=2, sample_keys=[("T1",), "T2"])
    tz_missing = make_check("MISSING_REQUIRED_VALUE", "agency.txt", in_scope=True, count=1, severity=ERROR)
    assert tz_missing.severity == ERROR
    out_warn = make_check("DUPLICATE_KEY", "trips.txt", in_scope=False, count=1)
    info = make_check("REPEATED_STOP_IN_TRIP", "stop_times.txt", in_scope=True, count=1, sample_keys=["T6"])
    shape_fk = make_check("FK_MISSING", "trips.txt", in_scope=True, count=1, severity=WARNING)
    assert (in_err.severity, out_warn.severity, info.severity, shape_fk.severity) == (ERROR, WARNING, INFO, WARNING)
    assert in_err.to_json()["sample_keys"] == ["T1", "T2"]
    assert status_from_checks([info]) == "PASSED"
    assert status_from_checks([info, out_warn]) == "PASSED_WITH_WARNINGS"
    assert status_from_checks([info, out_warn, in_err]) == "FAILED"
    assert status_from_checks([]) == "PASSED"
    assert severity_counts([info, out_warn, in_err, shape_fk]) == {ERROR: 1, WARNING: 2, INFO: 1}
    with pytest.raises(KeyError):
        make_check("NOT_A_CODE", "x", in_scope=True, count=1)
    many = make_check("INVALID_GTFS_TIME", "stop_times.txt", in_scope=True, count=50,
                      sample_keys=[f"T{i}" for i in range(50)])
    assert len(many.sample_keys) == 10


def test_design_codes_are_registered() -> None:
    for code in ("STATIC_FILE_MISSING", "STATIC_FILE_EMPTY", "MISSING_REQUIRED_COLUMN", "INVALID_TIMEZONE",
                 "MULTIPLE_AGENCY_TIMEZONES", "DUPLICATE_KEY", "FK_MISSING", "SCOPE_ROUTE_NOT_FOUND",
                 "SCOPE_ROUTE_NOT_BUS", "INVALID_COORDINATES", "INVALID_GTFS_TIME", "FIRST_LAST_STOP_TIME_MISSING",
                 "NON_MONOTONIC_STOP_TIMES", "INVALID_CALENDAR", "SERVICE_NO_ACTIVE_DATES", "ROUTE_WITHOUT_TRIPS",
                 "SHAPE_DIST_DECREASING", "REPEATED_STOP_IN_TRIP", "SERVICE_TIME_GE_24H", "FEED_INFO_MISSING",
                 "OUT_OF_SCOPE_FILTERED"):
        assert code in CHECK_RULES
    assert CHECK_RULES["SCOPE_ROUTE_NOT_BUS"][:2] == (ERROR, ERROR)
    assert CHECK_RULES["OUT_OF_SCOPE_FILTERED"][:2] == (INFO, INFO)


def test_only_structural_problems_fail_a_load() -> None:
    """Lead decision: per-trip/per-row defects are warnings even in scope; FAILED is structural only."""
    assert STRUCTURAL_CODES == frozenset({
        "STATIC_FILE_MISSING", "STATIC_FILE_EMPTY", "MISSING_REQUIRED_COLUMN", "INVALID_TIMEZONE",
        "MULTIPLE_AGENCY_TIMEZONES", "DUPLICATE_KEY", "FK_MISSING", "SCOPE_ROUTE_NOT_FOUND", "SCOPE_ROUTE_NOT_BUS",
    })
    for code in ("FIRST_LAST_STOP_TIME_MISSING", "NON_MONOTONIC_STOP_TIMES", "INVALID_GTFS_TIME",
                 "INVALID_FIELD_VALUE", "MISSING_REQUIRED_VALUE", "INVALID_COORDINATES", "INVALID_CALENDAR"):
        assert CHECK_RULES[code][:2] == (WARNING, WARNING), code
        assert make_check(code, "stop_times.txt", in_scope=True, count=1).severity == WARNING
    bad_trip = [make_check("NON_MONOTONIC_STOP_TIMES", "stop_times.txt", in_scope=True, count=1),
                make_check("FIRST_LAST_STOP_TIME_MISSING", "stop_times.txt", in_scope=True, count=1)]
    assert status_from_checks(bad_trip) == "PASSED_WITH_WARNINGS"
    assert status_from_checks(bad_trip + [make_check("DUPLICATE_KEY", "trips.txt", in_scope=True, count=1)]) \
        == "FAILED"
    assert make_check("FK_MISSING", "stop_times.txt", in_scope=True, count=1).severity == ERROR
    assert make_check("FK_MISSING", "stop_times.txt", in_scope=False, count=1).severity == WARNING


def test_normalize_route_ids() -> None:
    assert normalize_route_ids(" R_A, R_B ,,R_A") == ("R_A", "R_B")
    assert normalize_route_ids(["R_B", "R_A", "R_B", " "]) == ("R_B", "R_A")
    assert normalize_route_ids(None) == ()


# ------------------------------------------------------------------------------------------ ZIP store

def test_ingest_zip_is_content_addressed_and_idempotent(tmp_path: Path, golden_dir: Path) -> None:
    zpath = _golden_zip(golden_dir, tmp_path / "golden.zip")
    paths = WorkPaths.from_workdir(tmp_path / "W")
    v1 = ingest_zip(paths, zpath, download_url=None, user_agent="test")
    assert len(v1.gtfs_version) == 64 and v1.gtfs_version == v1.zip_sha256
    assert v1.zip_path == paths.static_version_dir(v1.gtfs_version) / "MBTA_GTFS.zip"
    assert v1.zip_path.read_bytes() == zpath.read_bytes()
    assert v1.feed_version == "golden-v1"
    assert not (v1.zip_path.stat().st_mode & stat.S_IWUSR)          # immutable copy
    version_json = json.loads((v1.zip_path.parent / "version.json").read_text(encoding="utf-8"))
    assert version_json["zip_sha256"] == v1.zip_sha256
    v2 = ingest_zip(paths, zpath, download_url=None, user_agent="test")
    assert v2 == v1                                                   # first provenance kept
    assert get_static_version(paths, v1.gtfs_version) == v1


def test_ingest_zip_errors(tmp_path: Path) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "W")
    with pytest.raises(UsageError):
        ingest_zip(paths, None, download_url=None, user_agent="t")
    with pytest.raises(ArtifactError):
        ingest_zip(paths, tmp_path / "missing.zip", download_url=None, user_agent="t")
    not_zip = tmp_path / "x.zip"
    not_zip.write_text("not a zip", encoding="utf-8")
    with pytest.raises(ArtifactError) as err:
        ingest_zip(paths, not_zip, download_url=None, user_agent="t")
    assert err.value.exit_code == 7
    with pytest.raises(ArtifactError):
        get_static_version(paths, "0" * 64)


def test_inspect_and_extract(tmp_path: Path, golden_dir: Path) -> None:
    zpath = _golden_zip(golden_dir, tmp_path / "golden.zip")
    summary = inspect_zip(zpath)
    files = {f["name"]: f for f in summary["files"]}
    assert files["stop_times.txt"]["row_count"] == 28
    assert files["trips.txt"]["row_count"] == 8
    assert files["shapes.txt"]["row_count"] == 17
    assert all(f["present"] for f in summary["files"])
    assert summary["feed_version"] == "golden-v1"
    assert summary["service_date_range"] == {"min": "20260901", "max": "20261231"}
    assert summary["bus_route_count"] == 2
    assert read_feed_info(zpath)["feed_publisher_name"].startswith("TransitPulse synthetic")
    extracted = extract_gtfs(zpath, tmp_path / "csv")
    assert set(extracted) == set(GOLDEN_FILES)
    assert extracted["trips"].path.read_bytes() == (golden_dir / "static" / "trips.txt").read_bytes()
    trip_to_route = read_trip_to_route(extracted["trips"].path)
    assert trip_to_route == {"T1": "R_A", "T2": "R_A", "T3": "R_A", "T4": "R_A", "T5": "R_B", "T6": "R_B",
                             "T7": "R_B", "TX": "R_X"}


def test_list_routes_helpers(tmp_path: Path, golden_dir: Path) -> None:
    zpath = _golden_zip(golden_dir, tmp_path / "golden.zip")
    routes = bus_routes_from_zip(zpath, 3)
    assert [(r["route_id"], r["trip_count"]) for r in routes] == [("R_A", 4), ("R_B", 3)]
    feed = {"header": {"timestamp": 1791201620}, "entity": [
        {"id": "e1", "vehicle": {"trip": {"route_id": "R_A"}, "vehicle": {"id": "V1"}}},
        {"id": "e2", "vehicle": {"trip": {"route_id": "R_A"}, "vehicle": {"id": "V1"}}},
        {"id": "e3", "vehicle": {"trip": {"route_id": "R_A"}, "vehicle": {"id": "V2"}}},
        {"id": "e4", "vehicle": {"trip": {}, "vehicle": {"id": "V3"}}},
        {"id": "e5", "alert": {}},
    ]}
    assert count_vehicles_by_route(feed) == {"R_A": 2}


# ------------------------------------------------------------------------------------------ scope index

def test_scope_index_roundtrip(tmp_path: Path) -> None:
    doc = build_scope_index_doc(
        gtfs_version="g" * 64, scope_id="0123456789abcdef", route_ids=["R_B", "R_A"],
        route_type_by_route={"R_A": 3, "R_B": 3}, agency_by_route={"R_A": "SYN", "R_B": "SYN"},
        agency_timezone="America/New_York", trip_to_route={"T1": "R_A", "TX": "R_X"},
        scope_stop_ids=["S2", "S1", "P1"], stops_by_route={"R_A": ["S2", "S1", "P1"], "R_B": []},
        parent_station={"S1": "P1"},
    )
    assert doc["route_ids"] == ["R_A", "R_B"] and doc["agency_ids"] == ["SYN"]
    write_scope_index(tmp_path, doc)
    with pytest.raises(FileNotFoundError):
        load_scope_index(tmp_path, require_complete=True)
    idx = load_scope_index(tmp_path)
    assert isinstance(idx, ScopeIndex)
    assert idx.route_ids == frozenset({"R_A", "R_B"})
    assert idx.route_of_trip("TX") == "R_X" and idx.is_scope_trip("T1") and not idx.is_scope_trip("TX")
    assert idx.routes_serving_stop("P1") == frozenset({"R_A"})
    assert idx.to_json() == doc


# ------------------------------------------------------------------------------------------ catalog rows

def test_downsample_points() -> None:
    pts = [[float(i), float(-i)] for i in range(10)]
    same, simplified = downsample_points(pts, 10)
    assert same == pts and simplified is False
    kept, simplified = downsample_points(pts, 4)
    assert simplified is True and len(kept) == 4
    assert kept[0] == pts[0] and kept[-1] == pts[-1]
    big = [[float(i), 0.0] for i in range(12001)]
    kept, simplified = downsample_points(big, 5000)
    assert simplified and len(kept) == 5000 and kept[0] == big[0] and kept[-1] == big[-1]
    assert [p[0] for p in kept] == sorted(p[0] for p in kept)
    with pytest.raises(ValueError):
        downsample_points(pts, 1)


def test_geometry_row_and_params() -> None:
    row = geometry_row({"gtfs_version": "g", "route_id": "R_A", "shape_id": "SH_A0", "direction_ids": [0],
                        "trip_count": 3, "is_representative": True,
                        "points": [{"seq": 1, "lon": -71.06, "lat": 42.35}, {"seq": 2, "lon": -71.059, "lat": 42.351}]},
                       5000)
    assert json.loads(row["points_json"]) == [[-71.06, 42.35], [-71.059, 42.351]]
    assert (row["point_count"], row["simplified"], row["is_representative"]) == (2, False, True)
    params = row_params("geometries_by_route", row)
    assert len(params) == len(STATIC_TABLE_COLUMNS["geometries_by_route"])
    assert params[3] == {0}
    stop_row = {"gtfs_version": "g", "route_id": "R_A", "stop_id": "S1", "stop_name": "S", "stop_lat": 42.35,
                "stop_lon": -71.06, "direction_ids": [], "pattern_count": 2}
    assert row_params("stops_by_route", stop_row)[6] is None          # empty set -> unset
    with pytest.raises(ValueError):
        row_params("stops_by_route", {**stop_row, "route_id": None})
    cql = insert_cql("transitpulse", "routes_by_version")
    assert cql.startswith("INSERT INTO transitpulse.routes_by_version (gtfs_version, route_id,")
    assert "TTL" not in cql and "IF NOT EXISTS" not in cql and cql.count("?") == 9
