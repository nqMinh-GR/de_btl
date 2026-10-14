"""Shared fixtures for every test under tests/spark/ (WP1, WP4, WP5).

* ``spark``: ONE session-scoped local SparkSession (local[1], session time zone UTC, 1 shuffle partition,
  UI disabled, no Kafka jars). ``TZ=UTC`` is exported (and ``time.tzset()`` applied) before the JVM starts,
  so collected TimestampType values are naive UTC datetimes; use ``GoldenKit.rows`` to get aware ones.
* ``tp``: a :class:`GoldenKit` that builds WP5 builder inputs from the hand-written golden fixture
  (``tests/fixtures/golden``): static dims straight from ``static/*.txt`` (plain csv, no WP1 code), one-row
  ``snap`` DataFrames from ``rt/*.json`` and matching frozen ``BuildParams`` / ``RunInfo``.
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

SCOPE_ROUTE_IDS: tuple[str, ...] = ("R_A", "R_B")
GOLDEN_RUN_ID = "00000000-0000-4000-8000-000000000001"
GOLDEN_SCOPE_ID = "goldenscope00001"
GOLDEN_GTFS_VERSION = "golden-gtfs-version"
PIPELINE_VERSION = "p1"
TOPICS = {
    "vehicle_positions": "mbta.vehicle_positions.snapshots.v1",
    "trip_updates": "mbta.trip_updates.snapshots.v1",
    "alerts": "mbta.alerts.snapshots.v1",
}
FEED_ABBR = {"vehicle_positions": "vp", "trip_updates": "tu", "alerts": "al"}


def utc(y: int, mo: int, d: int, h: int = 0, mi: int = 0, s: int = 0, us: int = 0) -> datetime:
    return datetime(y, mo, d, h, mi, s, us, tzinfo=UTC)


def at_epoch(seconds: int) -> datetime:
    return datetime.fromtimestamp(seconds, UTC)


# --------------------------------------------------------------------------------------------------
# Spark session
# --------------------------------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    # One session serves every Spark test. The Colab VM is ~51 GiB and already runs Kafka (1g)
    # plus Cassandra (4G heap + 2G direct). Whole-stage codegen of the prediction plan needs a
    # large compiler heap (4g died on IT07 with a ~1.2 MiB task binary), so the driver is 16g.
    # ExitOnOutOfMemoryError makes a full heap fail the test instead of retrying page allocation
    # until the cell timeout.
    session = (
        SparkSession.builder.master("local[1]")
        .appName("transitpulse-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "16g")
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.python.worker.reuse", "true")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC -XX:+ExitOnOutOfMemoryError")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC -XX:+ExitOnOutOfMemoryError")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _release_spark_cache(request: pytest.FixtureRequest):
    """Drop cached plans between tests so the shared session does not keep growing."""
    yield
    if "spark" not in request.fixturenames:
        return
    try:
        request.getfixturevalue("spark").catalog.clearCache()
    except Exception:  # noqa: BLE001 - a dead Spark JVM must not hide the test result
        return


# --------------------------------------------------------------------------------------------------
# Golden static -> dims (plain csv; mirrors the WP1 dim column contract in static/dims.py)
# --------------------------------------------------------------------------------------------------

def _read_csv(path: Path) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        out = []
        for row in csv.DictReader(fh):
            out.append({(k or "").strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()})
        return out


def _blank(v: str | None) -> str | None:
    return v if v not in (None, "") else None


def _int(v: str | None) -> int | None:
    v = _blank(v)
    return int(v) if v is not None else None


def gtfs_seconds(s: str | None) -> int | None:
    s = _blank(s)
    if s is None:
        return None
    h, m, sec = s.split(":")
    return int(h) * 3600 + int(m) * 60 + int(sec)


def service_anchor_utc(d: date, tz: str) -> datetime:
    return datetime.combine(d, dtime(12), ZoneInfo(tz)).astimezone(UTC) - timedelta(hours=12)


def _yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


_WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

DIM_SCHEMAS: dict[str, str] = {
    "agency": "agency_id string, agency_name string, agency_timezone string, gtfs_version string",
    "routes": ("route_id string, agency_id string, route_short_name string, route_long_name string, "
               "route_type int, route_color string, route_text_color string, route_sort_order int, "
               "gtfs_version string"),
    "trips": ("trip_id string, route_id string, service_id string, direction_id int, shape_id string, "
              "trip_headsign string, block_id string, first_departure_secs int, last_arrival_secs int, "
              "stop_count int, schedule_valid boolean, has_repeated_stop boolean, gtfs_version string"),
    "stop_times": ("trip_id string, stop_sequence int, stop_id string, arrival_time_raw string, "
                   "departure_time_raw string, arrival_secs int, departure_secs int, timepoint int, "
                   "stop_occurrence int, stop_id_unique_in_trip boolean, gtfs_version string"),
    "stops": ("stop_id string, stop_code string, stop_name string, stop_lat double, stop_lon double, "
              "location_type int, parent_station string, platform_code string, wheelchair_boarding int, "
              "gtfs_version string"),
    "route_stops": "route_id string, direction_id int, stop_id string, gtfs_version string",
    "service_dates": ("service_id string, service_date date, service_anchor_utc timestamp, source string, "
                      "gtfs_version string"),
}


def golden_static_rows(static_dir: Path, scope: Sequence[str]) -> dict[str, Any]:
    """Pure-Python scoped dimension rows from the golden GTFS text files."""
    gv = GOLDEN_GTFS_VERSION
    agency = _read_csv(static_dir / "agency.txt")
    tz = agency[0]["agency_timezone"]
    routes = [r for r in _read_csv(static_dir / "routes.txt") if r["route_id"] in scope]
    trips = [t for t in _read_csv(static_dir / "trips.txt") if t["route_id"] in scope]
    trip_by_id = {t["trip_id"]: t for t in trips}
    st_rows = [s for s in _read_csv(static_dir / "stop_times.txt") if s["trip_id"] in trip_by_id]
    st_rows.sort(key=lambda s: (s["trip_id"], int(s["stop_sequence"])))
    by_trip: dict[str, list[dict[str, str]]] = {}
    for s in st_rows:
        by_trip.setdefault(s["trip_id"], []).append(s)

    stop_times, trip_rows = [], []
    for tid, rows in by_trip.items():
        counts: dict[str, int] = {}
        for s in rows:
            counts[s["stop_id"]] = counts.get(s["stop_id"], 0) + 1
        seen: dict[str, int] = {}
        for s in rows:
            seen[s["stop_id"]] = seen.get(s["stop_id"], 0) + 1
            stop_times.append((
                tid, int(s["stop_sequence"]), s["stop_id"], s.get("arrival_time"), s.get("departure_time"),
                gtfs_seconds(s.get("arrival_time")), gtfs_seconds(s.get("departure_time")), _int(s.get("timepoint")),
                seen[s["stop_id"]], counts[s["stop_id"]] == 1, gv,
            ))
        first, last = rows[0], rows[-1]
        first_dep = gtfs_seconds(first.get("departure_time"))
        if first_dep is None:
            first_dep = gtfs_seconds(first.get("arrival_time"))
        last_arr = gtfs_seconds(last.get("arrival_time"))
        if last_arr is None:
            last_arr = gtfs_seconds(last.get("departure_time"))
        t = trip_by_id[tid]
        trip_rows.append((
            tid, t["route_id"], t["service_id"], _int(t.get("direction_id")), _blank(t.get("shape_id")),
            _blank(t.get("trip_headsign")), _blank(t.get("block_id")), first_dep, last_arr, len(rows), True,
            any(c > 1 for c in counts.values()), gv,
        ))

    all_stops = {s["stop_id"]: s for s in _read_csv(static_dir / "stops.txt")}
    used = {s["stop_id"] for s in st_rows}
    parents = {_blank(all_stops[s].get("parent_station")) for s in used if s in all_stops} - {None}
    stops = []
    for sid in sorted(used | parents):
        s = all_stops[sid]
        stops.append((
            sid, _blank(s.get("stop_code")), s.get("stop_name"), float(s["stop_lat"]), float(s["stop_lon"]),
            _int(s.get("location_type")), _blank(s.get("parent_station")), _blank(s.get("platform_code")),
            _int(s.get("wheelchair_boarding")), gv,
        ))
    rs = set()
    for s in st_rows:
        t = trip_by_id[s["trip_id"]]
        d = _int(t.get("direction_id"))
        rs.add((t["route_id"], d, s["stop_id"]))
        parent = _blank(all_stops.get(s["stop_id"], {}).get("parent_station"))
        if parent:
            rs.add((t["route_id"], d, parent))
    route_stops = [(r, d, s, gv) for (r, d, s) in sorted(rs, key=lambda x: (x[0], -1 if x[1] is None else x[1], x[2]))]

    services = {t["service_id"] for t in trips}
    active: dict[tuple[str, date], str] = {}
    cal_path = static_dir / "calendar.txt"
    for c in (_read_csv(cal_path) if cal_path.is_file() else []):
        if c["service_id"] not in services:
            continue
        d, end = _yyyymmdd(c["start_date"]), _yyyymmdd(c["end_date"])
        while d <= end:
            if c[_WEEKDAYS[d.weekday()]] == "1":
                active[(c["service_id"], d)] = "calendar"
            d += timedelta(days=1)
    cd_path = static_dir / "calendar_dates.txt"
    for x in (_read_csv(cd_path) if cd_path.is_file() else []):
        if x["service_id"] not in services:
            continue
        key = (x["service_id"], _yyyymmdd(x["date"]))
        if x["exception_type"] == "1":
            active.setdefault(key, "added")
        elif x["exception_type"] == "2":
            active.pop(key, None)
    service_dates = [
        (sid, d, service_anchor_utc(d, tz), src, gv) for (sid, d), src in sorted(active.items())
    ]
    return {
        "tz": tz,
        "agency": [(a["agency_id"], a["agency_name"], a["agency_timezone"], gv) for a in agency],
        "routes": [
            (r["route_id"], r["agency_id"], _blank(r.get("route_short_name")), _blank(r.get("route_long_name")),
             int(r["route_type"]), _blank(r.get("route_color")), _blank(r.get("route_text_color")),
             _int(r.get("route_sort_order")), gv)
            for r in routes
        ],
        "trips": trip_rows,
        "stop_times": stop_times,
        "stops": stops,
        "route_stops": route_stops,
        "service_dates": service_dates,
    }


# --------------------------------------------------------------------------------------------------
# Kit
# --------------------------------------------------------------------------------------------------

class GoldenKit:
    """Builds WP5 builder inputs (snap, params, run, dims) from the golden fixture."""

    def __init__(self, spark, golden_dir: Path):
        self.spark = spark
        self.golden_dir = golden_dir
        self._dims = None
        self._offset = 0

    # ------------------------------------------------------------------ policy / run
    @staticmethod
    def policy(**overrides):
        from transitpulse.contracts.policy import Policy

        values = dict(
            policy_version="policy-v1", vehicle_stale_seconds=120, prediction_stale_seconds=90,
            alert_stale_seconds=300, predicted_late_seconds=300, future_tolerance_seconds=5,
            service_date_inference_window_seconds=43200, uncertainty_policy="mbta_bus_uncertainty_v1",
            target_policy="first_future_resolved_stop_v1", timestamp_ms_threshold=100_000_000_000,
            current_generation_ttl_seconds=3600, history_ttl_seconds=259200, min_publish_ttl_seconds=60,
        )
        values.update(overrides)
        return Policy(**values)

    def run(self, route_ids: Sequence[str] = SCOPE_ROUTE_IDS, *, clock: datetime | None = None):
        from transitpulse.processing.context import RunInfo

        return RunInfo(
            run_id=GOLDEN_RUN_ID, project_id="transitpulse-mbta", scope_id=GOLDEN_SCOPE_ID,
            source_type="synthetic", clock_mode="source", gtfs_version=GOLDEN_GTFS_VERSION,
            agency_timezone="America/New_York", route_ids=tuple(route_ids), pipeline_version=PIPELINE_VERSION,
            policy=self.policy(), application_clock=clock or utc(2026, 10, 5, 12, 0, 30), clock_sequence=1,
            run_state="RUNNING", scenario_id=None,
        )

    # ------------------------------------------------------------------ dims
    @property
    def dims(self):
        if self._dims is None:
            from transitpulse.static.dims import Dims

            static_dir = self.golden_dir / "static"
            if not (static_dir / "stop_times.txt").is_file():
                pytest.skip("golden static fixture not materialized yet (tests/fixtures/golden/static)")
            rows = golden_static_rows(static_dir, SCOPE_ROUTE_IDS)
            frames = {name: self.spark.createDataFrame(rows[name], DIM_SCHEMAS[name]) for name in DIM_SCHEMAS}
            self._dims = Dims(
                gtfs_version=GOLDEN_GTFS_VERSION, scope_id=GOLDEN_SCOPE_ID, agency_timezone=rows["tz"],
                route_ids=SCOPE_ROUTE_IDS, frames=frames,
            )
        return self._dims

    # ------------------------------------------------------------------ expected.json (WP0b, hand-written)
    def expected(self) -> dict[str, Any]:
        path = self.golden_dir / "expected.json"
        if not path.is_file():
            pytest.skip("tests/fixtures/golden/expected.json not materialized yet")
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)

    def expect(self, run: str, step: str) -> dict[str, Any]:
        for s in self.expected()["runs"][run]["steps"]:
            if s["step"] == step:
                return s["expect"]
        raise KeyError(f"no step {step} in run {run}")

    @staticmethod
    def instant(s: str | None) -> datetime | None:
        return None if s is None else datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(UTC)

    # ------------------------------------------------------------------ feeds / snapshots
    def feed(self, label: str) -> tuple[dict[str, Any], bytes]:
        path = self.golden_dir / "rt" / f"{label}.json"
        if not path.is_file():
            pytest.skip(f"golden realtime fixture {path} not materialized yet")
        raw = path.read_bytes()
        return json.loads(raw), raw

    def snapshot(self, label: str, feed_type: str, *, metrics_as_of: datetime, **kw):
        feed, raw = self.feed(label)
        return self.snapshot_from_feed(feed, feed_type, metrics_as_of=metrics_as_of, raw=raw, label=label, **kw)

    def snapshot_from_feed(
        self,
        feed: dict[str, Any],
        feed_type: str,
        *,
        metrics_as_of: datetime,
        raw: bytes | None = None,
        label: str = "custom",
        run=None,
        parent_snapshot_id: str | None = None,
        parent_available: bool = True,
        fetched_at: datetime | None = None,
    ):
        """Return ``(snap_df, params)`` for one feed document (MBTA JSON shape: header + entity)."""
        from pyspark.sql import types as T

        from transitpulse.contracts import ids
        from transitpulse.processing.context import PARSED_SNAPSHOT_COLUMNS, BuildParams
        from transitpulse.processing.schemas import HEADER_SCHEMA

        run = run or self.run()
        header = feed["header"]
        entities = list(feed.get("entity") or [])
        hts = int(header["timestamp"])
        raw = raw if raw is not None else json.dumps(feed, sort_keys=True).encode("utf-8")
        raw_sha = ids.sha256_hex(raw)
        fetched = fetched_at or metrics_as_of
        sid = ids.snapshot_id(run.run_id, run.scope_id, feed_type, hts, ids.payload_sha(header, entities))
        cap = ids.capture_id(FEED_ABBR[feed_type], fetched, raw_sha)
        feed_ts = at_epoch(hts)
        self._offset += 1
        texts = [json.dumps(e, separators=(",", ":"), ensure_ascii=False) for e in entities]
        schema = T.StructType([
            T.StructField("run_id", T.StringType()), T.StructField("scope_id", T.StringType()),
            T.StructField("source_type", T.StringType()), T.StructField("scenario_id", T.StringType()),
            T.StructField("feed_type", T.StringType()), T.StructField("snapshot_id", T.StringType()),
            T.StructField("capture_id", T.StringType()), T.StructField("gtfs_version", T.StringType()),
            T.StructField("feed_timestamp", T.LongType()), T.StructField("feed_ts", T.TimestampType()),
            T.StructField("original_fetched_at", T.TimestampType()), T.StructField("published_at", T.TimestampType()),
            T.StructField("raw_path", T.StringType()), T.StructField("raw_sha256", T.StringType()),
            T.StructField("source_entity_count", T.IntegerType()),
            T.StructField("selected_entity_count", T.IntegerType()),
            T.StructField("header", HEADER_SCHEMA), T.StructField("entities", T.ArrayType(T.StringType())),
            T.StructField("kafka_topic", T.StringType()), T.StructField("kafka_partition", T.IntegerType()),
            T.StructField("kafka_offset", T.LongType()), T.StructField("source_order", T.StringType()),
        ])
        assert tuple(f.name for f in schema.fields) == PARSED_SNAPSHOT_COLUMNS
        raw_path = f"{run.run_id}/{feed_type}/2026-10-05/{cap}.json.gz"
        row = (
            run.run_id, run.scope_id, run.source_type, None, feed_type, sid, cap, run.gtfs_version,
            hts, feed_ts, fetched, fetched, raw_path, raw_sha, len(entities), len(entities),
            (header.get("gtfs_realtime_version"), header.get("incrementality"), hts), texts,
            TOPICS[feed_type], 0, self._offset, ids.source_order(hts, fetched, sid),
        )
        snap = self.spark.createDataFrame([row], schema)
        params = BuildParams(
            run_id=run.run_id, feed_type=feed_type, snapshot_id=sid,
            generation_id=ids.generation_id(run.run_id, feed_type, sid, PIPELINE_VERSION),
            source_order=ids.source_order(hts, fetched, sid), feed_timestamp=feed_ts, original_fetched_at=fetched,
            published_at=fetched, capture_id=cap, raw_path=raw_path, raw_sha256=raw_sha,
            kafka_topic=TOPICS[feed_type], kafka_partition=0, kafka_offset=self._offset, first_batch_id=7,
            metrics_as_of=metrics_as_of, clock_sequence_at_build=1, parent_snapshot_id=parent_snapshot_id,
            parent_generation_id=None, built_as_late=False, first_materialized_at=metrics_as_of,
            expires_at=metrics_as_of + timedelta(hours=1), history_expires_at=metrics_as_of + timedelta(days=3),
            gtfs_version=run.gtfs_version, scope_id=run.scope_id, scenario_id=None,
            parent_available=parent_available,
        )
        return snap, params

    # ------------------------------------------------------------------ result helpers
    def materialize(self, df):
        """Collect + re-create (breaks lineage), like WP4 handing over parent rows read from Cassandra."""
        return self.spark.createDataFrame(df.collect(), df.schema)

    @staticmethod
    def rows(df) -> list[dict[str, Any]]:
        """Collect as dicts; naive datetimes (UTC under TZ=UTC) become aware UTC datetimes."""

        def fix(v: Any) -> Any:
            if isinstance(v, datetime) and v.tzinfo is None:
                return v.replace(tzinfo=UTC)
            if isinstance(v, dict):
                return {k: fix(x) for k, x in v.items()}
            if isinstance(v, list):
                return [fix(x) for x in v]
            return v

        return [fix(r.asDict(recursive=True)) for r in df.collect()]

    @classmethod
    def one(cls, df) -> dict[str, Any]:
        rows = cls.rows(df)
        assert len(rows) == 1, rows
        return rows[0]

    @classmethod
    def by(cls, df, *keys: str) -> dict[Any, dict[str, Any]]:
        out: dict[Any, dict[str, Any]] = {}
        for r in cls.rows(df):
            k = r[keys[0]] if len(keys) == 1 else tuple(r[x] for x in keys)
            assert k not in out, f"duplicate key {k}"
            out[k] = r
        return out

    @classmethod
    def quality_counts(cls, df) -> dict[str, int]:
        return {r["reason_code"]: r["record_count"] for r in cls.rows(df)}

    @classmethod
    def stats(cls, out) -> dict[str, Any]:
        s = cls.one(out.stats)
        s["accounting"] = json.loads(s["accounting_json"])
        return s


@pytest.fixture(scope="session")
def tp(spark, golden_dir) -> GoldenKit:
    return GoldenKit(spark, golden_dir)
