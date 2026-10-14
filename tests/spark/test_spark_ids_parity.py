"""Spark Column mirrors (processing/spark_ids.py) == Python reference (contracts/ids.py), byte for byte.

Inputs are deterministic pseudo-random rows that include nulls, empty strings, separator/NUL characters,
non-ASCII text, negative and very large integers, NaN, signed zero and .5 rounding boundaries.
"""

from __future__ import annotations

import math
import random

import pytest

from transitpulse.contracts import ids

STRINGS = [None, "", "V1", "y1234", "R_A", "T1", "Ångström-é", "🚌 bus", "a\x1fb", "x\x00y", "07:55:00",
           "20261005", "SCHEDULED", "IN_TRANSIT_TO", " padded "]
INTS = [None, 0, 1, -1, 3, 2_147_483_647, -2_147_483_648]
LONGS = [None, 0, -5, 1_791_201_610, 1_791_201_610_000, 9_007_199_254_740_993, 100_000_000_000]
LATS = [None, math.nan, 0.0, -0.0, 42.3515, -71.0585, 0.00000005, -0.00000005, 89.99999995, 1e-12, 90.0,
        -180.0, 42.35, 42.3519]
BEARINGS = [None, math.nan, 0.0, 359.995, 123.456, 0.005, 0.015, 270.125]
SPEEDS = [None, 0.0, 12.3456, 0.0005, 0.0015, 33.3335, 1e-9]


def _rows(n: int = 300) -> list[tuple]:
    rng = random.Random(20261005)
    out = []
    for i in range(n):
        lat = rng.choice(LATS) if i % 3 else rng.uniform(-90.0, 90.0)
        lon = rng.choice(LATS) if i % 4 else rng.uniform(-180.0, 180.0)
        out.append((
            i,
            rng.choice(STRINGS), rng.choice(STRINGS), rng.choice(STRINGS), rng.choice(INTS),
            rng.choice(STRINGS), rng.choice(STRINGS), rng.choice(STRINGS),
            rng.choice(LONGS), rng.choice([1_791_201_620, 1_791_201_660, 0, 1]),
            lat, lon, rng.choice(BEARINGS) if i % 5 else rng.uniform(0, 360),
            rng.choice(SPEEDS) if i % 6 else rng.uniform(0, 40),
            rng.choice(STRINGS), rng.choice(INTS), rng.choice(STRINGS),
            rng.choice(["arrival", "departure"]),
            rng.choice(["run-1", "00000000-0000-4000-8000-000000000001"]),
            rng.choice(["vehicle_positions", "trip_updates", "alerts"]),
            rng.choice(["sn_" + "a" * 32, "sn_x"]),
            rng.choice(["vp-V1", "h:0123456789abcdef", "Ä"]),
            rng.choice([None, "3|arrival", "sid:S5|departure"]),
            rng.choice(["INVALID_ENTITY", "MISSING_VEHICLE_ID", "INVALID_PREDICTION"]),
            rng.choice([None, "", "A1", "vp-V1", "Ω"]),
            rng.choice(['{"id":"A1"}', '{"vehicle":{"id":"V9"}}', '{"x":"é\\u0000"}', "{}"]),
        ))
    return out


SCHEMA_DDL = (
    "i int, vehicle_id string, trip_id string, route_id string, direction_id int, start_date string, "
    "start_time string, rel string, ts long, hts long, lat double, lon double, bearing double, speed double, "
    "stop_id string, seq int, status string, event_type string, run_id string, feed_type string, "
    "snapshot_id string, entity_ref string, sub_ref string, reason string, eid string, ejson string"
)


@pytest.fixture(scope="module")
def parity_rows(spark):
    from pyspark.sql import functions as F

    from transitpulse.processing import spark_ids as S

    rows = _rows()
    df = spark.createDataFrame(rows, SCHEMA_DDL)
    common = dict(
        vehicle_id="vehicle_id", trip_id="trip_id", route_id="route_id", direction_id="direction_id",
        start_date="start_date", start_time="start_time", trip_schedule_relationship="rel", latitude="lat",
        longitude="lon", bearing="bearing", speed="speed", stop_id="stop_id", current_stop_sequence="seq",
        current_status="status",
    )
    out = df.select(
        "i",
        S.observation_id_col(vehicle_timestamp="ts", **common).alias("obs"),
        S.state_sample_id_col(header_timestamp="hts", **common).alias("ss"),
        S.prediction_id_col(
            trip_id="trip_id", start_date="start_date", start_time="start_time", route_id="route_id",
            direction_id="direction_id", stop_sequence="seq", stop_id="stop_id", event_type="event_type",
        ).alias("pred"),
        S.error_id_entity_col("run_id", "feed_type", "snapshot_id", "entity_ref", "sub_ref", "reason").alias("err"),
        S.entity_ref_col("eid", "ejson").alias("eref"),
        S.e7_col("lat").alias("e7_lat"),
        S.e7_col("lon").alias("e7_lon"),
        S.e2_col("bearing").alias("e2"),
        S.e3_col("speed").alias("e3"),
        S.k_cols(F.col("vehicle_id"), F.col("direction_id"), F.col("ts"), None, "lit", 7).alias("k"),
        S.h_col(F.coalesce(F.col("vehicle_id"), F.lit("fallback"))).alias("h"),
    ).collect()
    return rows, {r["i"]: r for r in out}


def _py(row: tuple) -> dict:
    (i, vehicle_id, trip_id, route_id, direction_id, start_date, start_time, rel, ts, hts, lat, lon, bearing,
     speed, stop_id, seq, status, event_type, run_id, feed_type, snapshot_id, entity_ref, sub_ref, reason,
     eid, ejson) = row
    common = dict(
        vehicle_id=vehicle_id, trip_id=trip_id, route_id=route_id, direction_id=direction_id, start_date=start_date,
        start_time=start_time, trip_schedule_relationship=rel, latitude=lat, longitude=lon, bearing=bearing,
        speed=speed, stop_id=stop_id, current_stop_sequence=seq, current_status=status,
    )
    try:
        pred = ids.prediction_id(trip_id=trip_id, start_date=start_date, start_time=start_time, route_id=route_id,
                                 direction_id=direction_id, stop_sequence=seq, stop_id=stop_id, event_type=event_type)
    except ValueError:
        pred = None
    entity = {"id": eid} if eid is not None else {}
    return {
        "obs": ids.observation_id(vehicle_timestamp=ts, **common),
        "ss": ids.state_sample_id(header_timestamp=hts, **common),
        "pred": pred,
        "err": ids.error_id_entity(run_id, feed_type, snapshot_id, entity_ref, sub_ref, reason),
        "eref": ids.entity_ref(entity, ejson),
        "e7_lat": ids.e7(lat),
        "e7_lon": ids.e7(lon),
        "e2": ids.e2(bearing),
        "e3": ids.e3(speed),
        "k": ids.K(vehicle_id, direction_id, ts, None, "lit", 7),
        "h": ids.H(vehicle_id if vehicle_id is not None else "fallback"),
    }


@pytest.mark.parametrize("field", ["obs", "ss", "pred", "err", "eref", "e7_lat", "e7_lon", "e2", "e3", "k", "h"])
def test_spark_ids_match_python_reference(parity_rows, field):
    rows, spark_out = parity_rows
    mismatches = []
    for row in rows:
        expected = _py(row)[field]
        actual = spark_out[row[0]][field]
        if expected != actual:
            mismatches.append((row[0], expected, actual))
    assert not mismatches, f"{len(mismatches)} mismatches for {field}, first: {mismatches[:3]}"


def test_id_prefixes_and_lengths(parity_rows):
    _, spark_out = parity_rows
    for r in spark_out.values():
        assert r["obs"].startswith("ob_") and len(r["obs"]) == 35
        assert r["ss"].startswith("ss_") and len(r["ss"]) == 35
        assert r["err"].startswith("er_") and len(r["err"]) == 35
        assert len(r["h"]) == 32


def test_null_and_empty_parts_do_not_collide(spark):
    """concat_ws would drop nulls; the mirror must keep NUL placeholders (D-ID-01)."""
    from pyspark.sql import functions as F

    from transitpulse.processing import spark_ids as S

    df = spark.createDataFrame([("a", None, "b"), ("a", "", "b"), (None, "a", "b")], "x string, y string, z string")
    got = [r["k"] for r in df.select(S.k_cols(F.col("x"), F.col("y"), F.col("z")).alias("k")).collect()]
    assert got == [ids.K("a", None, "b"), ids.K("a", "", "b"), ids.K(None, "a", "b")]
    assert len(set(got)) == 3


def test_k_cols_rejects_floats_and_bools():
    pytest.importorskip("pyspark")
    from transitpulse.processing import spark_ids as S

    with pytest.raises(TypeError):
        S.k_cols(1.5)
    with pytest.raises(TypeError):
        S.k_cols(True)


def test_golden_vehicle_ids_match_python(spark):
    """The exact golden V1 observation and V3 state sample keys (VP1)."""
    from pyspark.sql import functions as F

    from transitpulse.processing import spark_ids as S

    df = spark.createDataFrame(
        [("V1", "T1", "R_A", 0, "20261005", "07:55:00", "SCHEDULED", 1791201610, 42.3515, -71.0585, None, None,
          "S3", 3, "IN_TRANSIT_TO"),
         ("V3", None, "R_A", None, None, None, None, None, 42.35, -71.06, None, None, None, None, None)],
        "v string, t string, r string, d int, sd string, st string, rel string, ts long, lat double, lon double, "
        "b double, sp double, stop string, seq int, status string",
    )
    common = dict(vehicle_id="v", trip_id="t", route_id="r", direction_id="d", start_date="sd", start_time="st",
                  trip_schedule_relationship="rel", latitude="lat", longitude="lon", bearing="b", speed="sp",
                  stop_id="stop", current_stop_sequence="seq", current_status="status")
    rows = df.select(
        S.observation_id_col(vehicle_timestamp="ts", **common).alias("obs"),
        S.state_sample_id_col(header_timestamp=F.lit(1791201620).cast("long"), **common).alias("ss"),
    ).collect()
    assert rows[0]["obs"] == ids.observation_id(
        vehicle_id="V1", trip_id="T1", route_id="R_A", direction_id=0, start_date="20261005", start_time="07:55:00",
        trip_schedule_relationship="SCHEDULED", vehicle_timestamp=1791201610, latitude=42.3515, longitude=-71.0585,
        bearing=None, speed=None, stop_id="S3", current_stop_sequence=3, current_status="IN_TRANSIT_TO",
    )
    assert rows[1]["ss"] == ids.state_sample_id(
        vehicle_id="V3", trip_id=None, route_id="R_A", direction_id=None, start_date=None, start_time=None,
        trip_schedule_relationship=None, header_timestamp=1791201620, latitude=42.35, longitude=-71.06,
        bearing=None, speed=None, stop_id=None, current_stop_sequence=None, current_status=None,
    )
