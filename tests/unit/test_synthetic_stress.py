"""Stress generator: exact normalized-candidate counts per interval and static consistency (D-E17)."""

from __future__ import annotations

import csv
import gzip
import io
import json
import zipfile
from collections import defaultdict
from pathlib import Path

import pytest

from transitpulse.config import load_config
from transitpulse.contracts.accounting import count_candidates
from transitpulse.replay.recording_verify import verify_recording
from transitpulse.replay.synthetic.stress import generate_stress, split_counts, tu_layout
from transitpulse.runtime.paths import WorkPaths


def _env(tmp_path: Path, name: str = "w"):
    cfg = load_config("local-native", workdir=str(tmp_path / name), environ={})
    return cfg, WorkPaths.from_workdir(cfg.workdir)


def _static_ids(zip_path: Path) -> tuple[set[str], set[str]]:
    with zipfile.ZipFile(zip_path) as zf:
        trips = {r["trip_id"] for r in csv.DictReader(io.StringIO(zf.read("trips.txt").decode("utf-8")))}
        stops = {r["stop_id"] for r in csv.DictReader(io.StringIO(zf.read("stops.txt").decode("utf-8")))}
    return trips, stops


@pytest.mark.parametrize("rate,expected", [(100, (100, 400)), (500, (500, 2000)), (1000, (1000, 4000))])
def test_split_is_exact_80_20(rate, expected):
    vp, tu = split_counts(rate, 5)
    assert (vp, tu) == expected and vp + tu == rate * 5


@pytest.mark.parametrize("n", [400, 2000, 4000, 37, 1])
def test_tu_layout_sums_to_target(n):
    assert sum(2 * full + extra for full, extra in tu_layout(n)) == n


def test_stress_snapshots_carry_exact_candidate_counts(tmp_path):
    cfg, paths = _env(tmp_path)
    res = generate_stress(cfg, paths, rate=100, seed=3, duration_seconds=20)
    manifest = json.loads(res.manifest_path.read_text(encoding="utf-8"))
    assert manifest["fixture_kind"] == "stress" and manifest["source_type"] == "synthetic"
    assert manifest["stress"]["per_snapshot_candidates"] == {"vehicle_positions": 100, "trip_updates": 400}
    assert "Not MBTA" in manifest["stress"]["transformation"]
    trips, stops = _static_ids(res.static_zip_path)
    per_interval: dict[str, int] = defaultdict(int)
    shas = set()
    for item in manifest["items"]:
        body = gzip.decompress((res.recording_dir / item["raw_path"]).read_bytes())
        doc = json.loads(body)
        n = count_candidates(item["feed_type"], doc["entity"])
        assert n == item["params"]["candidate_count"]
        per_interval[item["at"]] += n
        shas.add(item["raw_sha256"])
        for ent in doc["entity"]:
            body_obj = ent.get("vehicle") or ent.get("trip_update")
            assert body_obj["trip"]["trip_id"] in trips
            if "vehicle" in ent:
                assert ent["vehicle"]["stop_id"] in stops
            else:
                assert {stu["stop_id"] for stu in ent["trip_update"]["stop_time_update"]} <= stops
    assert len(per_interval) == 4
    assert set(per_interval.values()) == {500}                 # 100 candidates/s x 5 s, every interval
    assert len(shas) == len(manifest["items"])                  # every snapshot is unique
    verified = verify_recording(res.manifest_path, paths=paths)
    assert verified.capture_count == 8


def test_stress_1000_per_second(tmp_path):
    cfg, paths = _env(tmp_path)
    res = generate_stress(cfg, paths, rate=1000, seed=1, duration_seconds=5)
    manifest = json.loads(res.manifest_path.read_text(encoding="utf-8"))
    counts = {item["feed_type"]: item["params"]["candidate_count"] for item in manifest["items"]}
    assert counts == {"vehicle_positions": 1000, "trip_updates": 4000}


def test_stress_is_deterministic_and_validates_rate(tmp_path):
    cfg_a, paths_a = _env(tmp_path, "a")
    cfg_b, paths_b = _env(tmp_path, "b")
    a = generate_stress(cfg_a, paths_a, rate=100, seed=9, duration_seconds=10)
    b = generate_stress(cfg_b, paths_b, rate=100, seed=9, duration_seconds=10)
    assert a.manifest_sha256 == b.manifest_sha256 and a.gtfs_version == b.gtfs_version
    with pytest.raises(ValueError):
        generate_stress(cfg_a, paths_a, rate=250, seed=9, duration_seconds=10)
