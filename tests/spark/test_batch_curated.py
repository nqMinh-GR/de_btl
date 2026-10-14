"""Curated batch commit protocol: staged units, atomic rename, _COMMITTED, crash recovery, readers."""

from __future__ import annotations

import json

from transitpulse.processing.curated import (
    COMMIT_MARKER,
    CuratedBatch,
    committed_batches,
    committed_files,
    read_committed,
)
from transitpulse.runtime.paths import WorkPaths

RUN = "run-1"
Q = "realtime_main"


def test_units_commit_and_readers(spark, tmp_path):
    paths = WorkPaths.from_workdir(tmp_path)
    cb = CuratedBatch(paths, RUN, Q, 3)
    assert cb.begin() is False
    obs1 = spark.createDataFrame([("o1", "R_A"), ("o2", "R_A")], "observation_id string, route_id string")
    obs2 = spark.createDataFrame([("o3", "R_B")], "observation_id string, route_id string")
    cb.write_unit(CuratedBatch.unit_name("vp", "sn_a"), {"observations": obs1})
    cb.write_unit(CuratedBatch.unit_name("vp", "sn_b"), {"observations": obs2})
    # an unmarked unit (crash in the middle of a write) is never listed
    (cb.staging_dir / "observations" / "vp-sn_c").mkdir(parents=True)
    (cb.staging_dir / "observations" / "vp-sn_c" / "part-0.parquet").write_bytes(b"garbage")
    assert cb.has_unit("vp-sn_a") and not cb.has_unit("vp-sn_c")
    doc = cb.commit(query_id="q", snapshot_ids=["sn_a", "sn_b"], accounting={"input_messages": 2},
                    row_counts={"observations": 3})
    assert (cb.final_dir / COMMIT_MARKER).is_file() and not cb.staging_dir.exists()
    assert all("vp-sn_c" not in f for f in doc["datasets"]["observations"]["files"])
    batches = committed_batches(paths, RUN, Q)
    assert [b["batch_id"] for b in batches] == [3]
    files = committed_files(batches, "observations")
    assert files and all(f.endswith(".parquet") for f in files)
    df = read_committed(spark, files)
    assert sorted(r["observation_id"] for r in df.collect()) == ["o1", "o2", "o3"]
    # idempotent: a replay of the committed batch skips all curated work
    again = CuratedBatch(paths, RUN, Q, 3)
    assert again.begin() is True and again.committed
    assert again.commit(query_id="q2", snapshot_ids=[], accounting={})["query_id"] == "q"


def test_uncommitted_batches_are_invisible_and_recoverable(spark, tmp_path):
    paths = WorkPaths.from_workdir(tmp_path)
    cb = CuratedBatch(paths, RUN, Q, 4)
    cb.begin()
    cb.write_unit("batch", {"snapshots": spark.createDataFrame([("sn_a",)], "snapshot_id string")})
    assert committed_batches(paths, RUN, Q) == []
    # crash between the rename and _COMMITTED: final exists without marker
    cb.staging_dir.rename(cb.final_dir)
    assert committed_batches(paths, RUN, Q) == []
    replay = CuratedBatch(paths, RUN, Q, 4)
    assert replay.begin() is False
    assert replay.has_unit("batch") and not replay.final_dir.exists()       # moved back to staging, reused
    replay.commit(query_id=None, snapshot_ids=["sn_a"], accounting={})
    doc = json.loads((replay.final_dir / COMMIT_MARKER).read_text(encoding="utf-8"))
    assert doc["batch_id"] == 4 and doc["datasets"]["snapshots"]["files"]


def test_batch_dir_layout(tmp_path):
    paths = WorkPaths.from_workdir(tmp_path)
    cb = CuratedBatch(paths, RUN, Q, 12)
    assert cb.final_dir == paths.curated_root / f"run_id={RUN}" / f"query={Q}" / "batch_id=000000000012"
    assert cb.staging_dir.parent.name == "_staging"
