"""Static loader end-to-end on the golden fixture (design C §8 expected static output; no Cassandra)."""

from __future__ import annotations

import json
import zipfile
from datetime import date
from pathlib import Path

import pytest

from transitpulse.config import load_config
from transitpulse.runtime.paths import WorkPaths
from transitpulse.static.dims import DIM_NAMES, load_dims, scope_dir_is_complete
from transitpulse.static.index import load_scope_index
from transitpulse.static.ingest import ingest_zip
from transitpulse.static.loader import failed_report_path, load_static

GOLDEN_FILES = ("agency", "routes", "trips", "stops", "stop_times", "calendar", "calendar_dates", "shapes",
                "feed_info")
SCOPE = ["R_A", "R_B"]

#: exact dim schemas (static/dims.py contract): column -> Spark simpleString type
EXPECTED_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "agency": [("agency_id", "string"), ("agency_name", "string"), ("agency_timezone", "string"),
               ("gtfs_version", "string")],
    "routes": [("route_id", "string"), ("agency_id", "string"), ("route_short_name", "string"),
               ("route_long_name", "string"), ("route_type", "int"), ("route_color", "string"),
               ("route_text_color", "string"), ("route_sort_order", "int"), ("gtfs_version", "string")],
    "trips": [("trip_id", "string"), ("route_id", "string"), ("service_id", "string"), ("direction_id", "int"),
              ("shape_id", "string"), ("trip_headsign", "string"), ("block_id", "string"),
              ("first_departure_secs", "int"), ("last_arrival_secs", "int"), ("stop_count", "int"),
              ("schedule_valid", "boolean"), ("has_repeated_stop", "boolean"), ("gtfs_version", "string")],
    "stop_times": [("trip_id", "string"), ("stop_sequence", "int"), ("stop_id", "string"),
                   ("arrival_time_raw", "string"), ("departure_time_raw", "string"), ("arrival_secs", "int"),
                   ("departure_secs", "int"), ("timepoint", "int"), ("stop_occurrence", "int"),
                   ("stop_id_unique_in_trip", "boolean"), ("gtfs_version", "string")],
    "stops": [("stop_id", "string"), ("stop_code", "string"), ("stop_name", "string"), ("stop_lat", "double"),
              ("stop_lon", "double"), ("location_type", "int"), ("parent_station", "string"),
              ("platform_code", "string"), ("wheelchair_boarding", "int"), ("gtfs_version", "string")],
    "route_stops": [("route_id", "string"), ("direction_id", "int"), ("stop_id", "string"),
                    ("gtfs_version", "string")],
    "shapes": [("shape_id", "string"), ("shape_pt_sequence", "int"), ("shape_pt_lat", "double"),
               ("shape_pt_lon", "double"), ("shape_dist_traveled", "double"), ("gtfs_version", "string")],
    "calendar": [("service_id", "string"), ("monday", "boolean"), ("tuesday", "boolean"),
                 ("wednesday", "boolean"), ("thursday", "boolean"), ("friday", "boolean"),
                 ("saturday", "boolean"), ("sunday", "boolean"), ("start_date", "date"), ("end_date", "date"),
                 ("gtfs_version", "string")],
    "calendar_dates": [("service_id", "string"), ("date", "date"), ("exception_type", "int"),
                       ("gtfs_version", "string")],
    "service_dates": [("service_id", "string"), ("service_date", "date"), ("service_anchor_utc", "timestamp"),
                      ("source", "string"), ("gtfs_version", "string")],
    "feed_info": [("feed_publisher_name", "string"), ("feed_lang", "string"), ("feed_start_date", "string"),
                  ("feed_end_date", "string"), ("feed_version", "string"), ("gtfs_version", "string")],
}


def _golden_zip(golden_dir: Path, target: Path, extra: dict[str, str] | None = None) -> Path:
    """Deterministic ZIP of the golden static files; ``extra`` appends CSV lines to a file."""
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name in GOLDEN_FILES:
            data = (golden_dir / "static" / f"{name}.txt").read_text(encoding="utf-8")
            if extra and name in extra:
                data = data.rstrip("\n") + "\n" + extra[name]
            info = zipfile.ZipInfo(f"{name}.txt", date_time=(2026, 9, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, data.encode("utf-8"))
    return target


# Per-trip schedule defects on scoped route R_A (lead decision: warnings, never FAILED).
DEFECT_TRIPS = (
    "R_A,WKDY,T8,Synthetic Stop 3,0,SH_A0\n"
    "R_A,WKDY,T9,Synthetic Stop 3,0,SH_A0\n"
    "R_A,WKDY,T10,Synthetic Stop 3,0,SH_A0\n"
    "R_A,WKDY,T11,Synthetic Stop 3,0,SH_A0\n"
    "R_A,BADCAL,T12,Synthetic Stop 2,0,SH_A0\n"   # valid schedule, but its only calendar row is invalid
)
# INVALID_CALENDAR (saturday=2): the row must contribute NO service dates (not even the valid weekdays).
DEFECT_CALENDAR = "BADCAL,1,1,1,1,1,2,0,20260901,20261231\n"
DEFECT_STOP_TIMES = (
    "T8,09:00:00,09:00:00,S1,1,1\n"      # NON_MONOTONIC_STOP_TIMES (09:00 -> 08:50)
    "T8,08:50:00,08:50:00,S2,2,1\n"
    "T8,09:10:00,09:10:00,S3,3,1\n"
    "T9,09:00:00,09:00:00,S1,1,1\n"      # FIRST_LAST_STOP_TIME_MISSING (last stop has no time)
    "T9,09:05:00,09:05:00,S2,2,1\n"
    "T9,,,S3,3,0\n"
    "T10,09:00:00,09:00:00,S1,1,1\n"     # INVALID_GTFS_TIME (minutes 61)
    "T10,09:61:00,09:61:00,S2,2,1\n"
    "T10,09:20:00,09:20:00,S3,3,1\n"
    "T11,09:00:00,09:00:00,S1,1,1\n"     # MISSING_REQUIRED_VALUE (empty stop_sequence)
    "T11,09:05:00,09:05:00,S2,,1\n"
    "T11,09:10:00,09:10:00,S3,3,1\n"
    "T12,10:00:00,10:00:00,S1,1,1\n"
    "T12,10:05:00,10:05:00,S2,2,1\n"
)


@pytest.fixture(scope="module")
def golden_load(spark, golden_dir: Path, tmp_path_factory):
    work = tmp_path_factory.mktemp("static_loader")
    zpath = _golden_zip(golden_dir, work / "golden.zip")
    cfg = load_config("local-native", workdir=str(work / "W"), environ={})
    paths = WorkPaths.from_workdir(cfg.workdir)
    version = ingest_zip(paths, zpath, download_url=None, user_agent="transitpulse-tests")
    result = load_static(cfg, paths, version.gtfs_version, SCOPE, spark=spark, write_cassandra=False)
    return cfg, paths, version, result


def _report(result) -> dict:
    return json.loads(Path(result.report_path).read_text(encoding="utf-8"))


def test_status_and_scope(golden_load) -> None:
    _, _, version, result = golden_load
    assert result.status == "PASSED"
    assert result.verified_route_ids == ("R_A", "R_B")
    assert result.rejected == ()
    assert result.agency_timezone == "America/New_York"
    assert result.gtfs_version == version.gtfs_version
    assert len(result.scope_id) == 16
    assert scope_dir_is_complete(result.scope_dir)
    assert result.report_path == result.scope_dir / "static_quality_report.json"


def test_manifest_and_success_marker(golden_load) -> None:
    cfg, _, version, result = golden_load
    manifest = json.loads((result.scope_dir / "manifest.json").read_text(encoding="utf-8"))
    assert set(manifest) == {"gtfs_version", "scope_id", "agency_timezone", "verified_route_ids", "route_type",
                             "scope_policy_version", "status", "row_counts", "created_at", "parser_version"}
    assert manifest["status"] == "PASSED"
    assert manifest["route_type"] == 3 and manifest["scope_policy_version"] == cfg.scope.scope_policy_version
    assert manifest["gtfs_version"] == version.gtfs_version
    assert (result.scope_dir / "_SUCCESS").is_file()
    for name in DIM_NAMES:
        assert (result.scope_dir / f"dim_{name}").is_dir()


def test_report_checks_and_counts(golden_load) -> None:
    _, _, _, result = golden_load
    report = _report(result)
    assert report["status"] == "PASSED"
    assert report["severity_counts"]["error"] == 0 and report["severity_counts"]["warning"] == 0
    by_code = {}
    for c in report["checks"]:
        by_code.setdefault(c["code"], []).append(c)
    assert {"REPEATED_STOP_IN_TRIP", "SERVICE_TIME_GE_24H", "OUT_OF_SCOPE_FILTERED"} <= set(by_code)
    assert all(c["severity"] == "info" for c in report["checks"])
    assert by_code["REPEATED_STOP_IN_TRIP"][0]["sample_keys"] == ["T6"]
    assert by_code["SERVICE_TIME_GE_24H"][0]["sample_keys"] == ["T4"]
    files = {f["name"]: f for f in report["files"]}
    expected = {"routes.txt": (3, 2), "trips.txt": (8, 7), "stop_times.txt": (28, 26), "stops.txt": (8, 7),
                "shapes.txt": (17, 15)}
    for name, (src, scoped) in expected.items():
        assert (files[name]["row_count"], files[name]["scoped_row_count"]) == (src, scoped), name
    assert report["scope"]["verified_route_ids"] == SCOPE
    assert report["service_date_range"] == {"min": "20260901", "max": "20261231"}
    assert report["cassandra"]["status"] == "PENDING"
    assert report["feed_version"] == "golden-v1"
    assert report["schedule_invalid_trips"] == {"count": 0, "sample_trip_ids": []}
    assert report["severity_policy"].startswith("static-severity-v2")


def test_dim_schemas_match_contract(spark, golden_load) -> None:
    _, _, _, result = golden_load
    for name, expected in EXPECTED_SCHEMAS.items():
        df = spark.read.parquet(str(result.scope_dir / f"dim_{name}"))
        got = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
        assert got == expected, name


def test_dim_row_counts(spark, golden_load) -> None:
    _, _, _, result = golden_load
    dims = load_dims(spark, result.scope_dir, cache=False)
    assert dims.routes.count() == 2
    assert dims.trips.count() == 7
    assert dims.stop_times.count() == 26
    assert dims.stops.count() == 7
    assert dims.shapes.count() == 15
    assert dims.shapes.select("shape_id").distinct().count() == 4
    assert dims.agency.count() == 1
    assert dims.feed_info.count() == 1
    assert result.row_counts["service_dates"] == 210


def test_service_dates(spark, golden_load) -> None:
    from pyspark.sql import functions as F

    _, _, _, result = golden_load
    sd = spark.read.parquet(str(result.scope_dir / "dim_service_dates"))
    counts = {r["service_id"]: r["count"] for r in sd.groupBy("service_id").count().collect()}
    assert counts == {"WKDY": 88, "DAILY": 122}
    wkdy = {r["service_date"]: r["source"] for r in sd.where(F.col("service_id") == "WKDY").collect()}
    assert date(2026, 10, 5) in wkdy and wkdy[date(2026, 10, 5)] == "calendar"
    assert wkdy.get(date(2026, 10, 10)) == "added"
    assert date(2026, 10, 11) not in wkdy and date(2026, 10, 12) not in wkdy
    anchors = {
        (r["service_id"], r["service_date"]): r["anchor"]
        for r in sd.select("service_id", "service_date", F.col("service_anchor_utc").cast("long").alias("anchor"))
        .collect()
    }
    assert anchors[("WKDY", date(2026, 10, 5))] == 1791172800            # 2026-10-05T04:00:00Z
    assert anchors[("DAILY", date(2026, 10, 31))] == 1793419200          # 2026-10-31T04:00:00Z (EDT)
    assert anchors[("DAILY", date(2026, 11, 1))] == 1793509200           # 2026-11-01T05:00:00Z (EST)
    assert anchors[("DAILY", date(2026, 11, 1))] - anchors[("DAILY", date(2026, 10, 31))] == 25 * 3600


def test_trips_and_stop_times(spark, golden_load) -> None:
    from pyspark.sql import functions as F

    _, _, _, result = golden_load
    dims = load_dims(spark, result.scope_dir, cache=False)
    trips = {r["trip_id"]: r for r in dims.trips.collect()}
    assert set(trips) == {"T1", "T2", "T3", "T4", "T5", "T6", "T7"}
    assert (trips["T4"]["first_departure_secs"], trips["T4"]["last_arrival_secs"]) == (71400, 90600)
    assert (trips["T1"]["first_departure_secs"], trips["T1"]["last_arrival_secs"]) == (28500, 29400)
    assert all(t["schedule_valid"] for t in trips.values())
    assert trips["T6"]["has_repeated_stop"] is True and trips["T6"]["stop_count"] == 4
    assert trips["T1"]["has_repeated_stop"] is False and trips["T2"]["direction_id"] == 1
    t6 = {(r["stop_sequence"]): r for r in dims.stop_times.where(F.col("trip_id") == "T6").collect()}
    assert (t6[1]["stop_occurrence"], t6[4]["stop_occurrence"]) == (1, 2)
    assert t6[1]["stop_id_unique_in_trip"] is False and t6[2]["stop_id_unique_in_trip"] is True
    t4 = {(r["stop_sequence"]): r for r in dims.stop_times.where(F.col("trip_id") == "T4").collect()}
    assert (t4[4]["arrival_time_raw"], t4[4]["arrival_secs"]) == ("25:10:00", 90600)
    stops_by_route = {
        r["route_id"]: r["n"]
        for r in dims.route_stops.groupBy("route_id").agg(F.countDistinct("stop_id").alias("n")).collect()
    }
    assert stops_by_route == {"R_A": 4, "R_B": 3}


def test_scope_index(golden_load) -> None:
    _, _, version, result = golden_load
    idx = load_scope_index(result.scope_dir, require_complete=True)
    assert idx.gtfs_version == version.gtfs_version and idx.scope_id == result.scope_id
    assert idx.route_ids == frozenset(SCOPE)
    assert idx.route_type_by_route == {"R_A": 3, "R_B": 3}
    assert idx.agency_by_route == {"R_A": "SYN", "R_B": "SYN"} and idx.agency_ids == frozenset({"SYN"})
    assert idx.agency_timezone == "America/New_York"
    assert idx.trip_to_route["TX"] == "R_X" and len(idx.trip_to_route) == 8
    assert idx.scope_stop_ids == frozenset({"S1", "S2", "S3", "S4", "S5", "S6", "S7"})
    assert idx.stops_by_route["R_A"] == frozenset({"S1", "S2", "S3", "S4"})
    assert idx.stops_by_route["R_B"] == frozenset({"S5", "S6", "S7"})
    assert idx.parent_station == {}


def test_catalog_rows_without_cassandra(spark, golden_load) -> None:
    from transitpulse.static.catalog import catalog_frames
    from transitpulse.storage.static_writer import STATIC_TABLE_COLUMNS, geometry_row

    _, _, version, result = golden_load
    frames = catalog_frames(spark, result.scope_dir, version.gtfs_version)
    assert frames["routes_by_version"].count() == 2
    assert frames["stops_by_version"].count() == 7
    assert frames["stop_times_by_trip"].count() == 26
    for table in ("routes_by_version", "stops_by_version", "stops_by_route", "stop_times_by_trip"):
        assert set(frames[table].columns) <= set(STATIC_TABLE_COLUMNS[table]), table
    sbr = frames["stops_by_route"].collect()
    per_route: dict[str, int] = {}
    for r in sbr:
        per_route[r["route_id"]] = per_route.get(r["route_id"], 0) + 1
    assert per_route == {"R_A": 4, "R_B": 3}
    s1 = next(r for r in sbr if r["route_id"] == "R_A" and r["stop_id"] == "S1")
    assert list(s1["direction_ids"]) == [0, 1] and s1["pattern_count"] == 2
    geo = {(r["route_id"], r["shape_id"]): r.asDict(recursive=True) for r in frames["geometries_by_route"].collect()}
    assert set(geo) == {("R_A", "SH_A0"), ("R_A", "SH_A1"), ("R_B", "SH_B0"), ("R_B", "SH_B1")}
    assert (geo[("R_A", "SH_A0")]["trip_count"], geo[("R_A", "SH_A0")]["is_representative"]) == (3, True)
    assert geo[("R_B", "SH_B1")]["is_representative"] is False
    shaped = geometry_row(geo[("R_A", "SH_A0")], 5000)
    assert json.loads(shaped["points_json"])[0] == [-71.06, 42.35]
    assert shaped["point_count"] == 4 and shaped["simplified"] is False


def test_reload_is_idempotent(spark, golden_load) -> None:
    cfg, paths, version, result = golden_load
    again = load_static(cfg, paths, version.gtfs_version, ["R_B", "R_A", "R_A"], spark=spark,
                        write_cassandra=False)
    assert again.reused is True
    assert again.scope_id == result.scope_id and again.scope_dir == result.scope_dir
    assert again.status == "PASSED"


@pytest.mark.parametrize(
    ("route_ids", "reason"),
    [(["R_A", "R_X"], "SCOPE_ROUTE_NOT_BUS"), (["R_A", "R_Z"], "SCOPE_ROUTE_NOT_FOUND")],
)
def test_invalid_scope_fails_without_publishing(spark, golden_load, route_ids, reason) -> None:
    cfg, paths, version, _ = golden_load
    res = load_static(cfg, paths, version.gtfs_version, route_ids, spark=spark, write_cassandra=False)
    assert res.status == "FAILED"
    assert res.verified_route_ids == ("R_A",)
    assert [r["reason"] for r in res.rejected] == [reason]
    assert not scope_dir_is_complete(res.scope_dir)
    assert not res.scope_dir.exists()
    assert res.report_path == failed_report_path(paths, res.scope_id)
    report = json.loads(res.report_path.read_text(encoding="utf-8"))
    assert report["status"] == "FAILED"
    assert any(c["code"] == reason and c["severity"] == "error" for c in report["checks"])


def test_per_trip_defects_are_warnings_not_failures(spark, golden_dir: Path, tmp_path_factory) -> None:
    """Bad in-scope trips keep the load PASSED_WITH_WARNINGS; they stay with schedule_valid=false."""
    from pyspark.sql import functions as F

    from transitpulse.static.catalog import catalog_frames

    work = tmp_path_factory.mktemp("static_loader_defects")
    zpath = _golden_zip(golden_dir, work / "defects.zip",
                        {"trips": DEFECT_TRIPS, "stop_times": DEFECT_STOP_TIMES, "calendar": DEFECT_CALENDAR})
    cfg = load_config("local-native", workdir=str(work / "W"), environ={})
    paths = WorkPaths.from_workdir(cfg.workdir)
    version = ingest_zip(paths, zpath, download_url=None, user_agent="transitpulse-tests")
    res = load_static(cfg, paths, version.gtfs_version, SCOPE, spark=spark, write_cassandra=False)

    assert res.status == "PASSED_WITH_WARNINGS"
    assert scope_dir_is_complete(res.scope_dir)
    report = _report(res)
    assert report["severity_counts"]["error"] == 0
    warnings = {(c["code"], tuple(c["sample_keys"])) for c in report["checks"]
                if c["severity"] == "warning" and c["in_scope"]}
    assert ("NON_MONOTONIC_STOP_TIMES", ("T8",)) in warnings
    assert ("FIRST_LAST_STOP_TIME_MISSING", ("T9",)) in warnings
    assert ("INVALID_GTFS_TIME", ("T10|2",)) in warnings
    assert ("MISSING_REQUIRED_VALUE", ("T11|",)) in warnings
    assert ("INVALID_CALENDAR", ("BADCAL",)) in warnings
    assert ("SERVICE_NO_ACTIVE_DATES", ("BADCAL",)) in warnings
    assert report["schedule_invalid_trips"] == {"count": 4, "sample_trip_ids": ["T10", "T11", "T8", "T9"]}

    sd = spark.read.parquet(str(res.scope_dir / "dim_service_dates"))
    assert sd.where(F.col("service_id") == "BADCAL").count() == 0          # invalid row: no dates at all
    assert {r["service_id"]: r["count"] for r in sd.groupBy("service_id").count().collect()} == {
        "WKDY": 88, "DAILY": 122}

    dims = load_dims(spark, res.scope_dir, cache=False)
    validity = {r["trip_id"]: r["schedule_valid"] for r in dims.trips.select("trip_id", "schedule_valid").collect()}
    assert {t for t, ok in validity.items() if not ok} == {"T8", "T9", "T10", "T11"}
    assert all(validity[t] for t in ("T1", "T2", "T3", "T4", "T5", "T6", "T7"))
    t9 = dims.trips.where(F.col("trip_id") == "T9").collect()[0]
    assert t9["first_departure_secs"] == 32400 and t9["last_arrival_secs"] is None
    t10 = {r["stop_sequence"]: r for r in dims.stop_times.where(F.col("trip_id") == "T10").collect()}
    assert t10[2]["arrival_time_raw"] == "09:61:00" and t10[2]["arrival_secs"] is None
    assert sorted(r["stop_sequence"] for r in dims.stop_times.where(F.col("trip_id") == "T11").collect()) == [1, 3]

    idx = load_scope_index(res.scope_dir, require_complete=True)
    assert all(idx.trip_to_route[t] == "R_A" for t in ("T8", "T9", "T10", "T11"))
    assert idx.stops_by_route["R_A"] == frozenset({"S1", "S2", "S3", "S4"})

    frames = catalog_frames(spark, res.scope_dir, version.gtfs_version)
    st_rows = frames["stop_times_by_trip"].where(F.col("trip_id").isin("T8", "T9", "T10", "T11")).collect()
    assert len(st_rows) == 11                                   # T11's row without stop_sequence is skipped
    assert all(r["stop_sequence"] is not None for r in st_rows)
    sbr = frames["stops_by_route"].collect()
    assert all(r["stop_id"] is not None for r in sbr)
    assert sorted(r["stop_id"] for r in sbr if r["route_id"] == "R_A") == ["S1", "S2", "S3", "S4"]
