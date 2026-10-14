"""Curated Parquet with a per-batch commit protocol (design B §6, spec §10.4, PDF p14).

Layout (``Q`` = query name, ``N`` = 12-digit batch id)::

    W/data/curated/run_id=<run>/query=Q/
        _staging/batch_id=N/<dataset>/<unit>/part-*.parquet      units written during the batch
        _staging/batch_id=N/_units/<unit>.json                    unit completion markers
        batch_id=N/...                                            same tree after the atomic rename
        batch_id=N/_COMMITTED                                     commit manifest (written LAST)

A *unit* is the output of one snapshot generation (``<feed abbr>-<snapshot_id>``) or of the batch itself
(``batch``: deliveries, message-level DLQ, publication outcomes). Units are written with
``mode("overwrite")`` and marked done; a replay of an uncommitted batch reuses marked units and rewrites
the rest (content is a deterministic function of the frozen generation parameters).

Commit: (1) skip when ``_COMMITTED`` exists; (2) ``os.rename(staging, final)`` (atomic on one local
filesystem); (3) ``_COMMITTED`` via temp file + ``os.replace`` + fsync, listing every file per dataset.
A crash between rename and marker is recovered by moving the uncommitted final directory back to staging
(nothing is deleted). Readers list ONLY committed batches and read the exact files recorded in
``_COMMITTED`` (no directory discovery, no partially written output).
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from transitpulse.contracts.envelope import iso_utc, utcnow
from transitpulse.runtime.paths import WorkPaths, safe_component

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

COMMIT_MARKER = "_COMMITTED"
UNITS_DIR = "_units"
BATCH_UNIT = "batch"


def _fsync_dir(d: Path) -> None:
    try:
        fd = os.open(str(d), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, sort_keys=True, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def query_dir(paths: WorkPaths, run_id: str, query_name: str) -> Path:
    return paths.curated_run_dir(run_id) / f"query={safe_component(query_name, 'query_name')}"


def batch_dir_name(batch_id: int) -> str:
    if batch_id < 0:
        raise ValueError("batch_id must be non-negative")
    return f"batch_id={int(batch_id):012d}"


def _parquet_files(d: Path) -> list[Path]:
    if not d.is_dir():
        return []
    return sorted(p for p in d.rglob("*.parquet") if p.is_file() and not p.name.startswith((".", "_")))


class CuratedBatch:
    """Staging/commit handle for one micro-batch."""

    def __init__(self, paths: WorkPaths, run_id: str, query_name: str, batch_id: int) -> None:
        self.run_id = run_id
        self.batch_id = int(batch_id)
        self.query_dir = query_dir(paths, run_id, query_name)
        self.query_name = query_name
        self.final_dir = self.query_dir / batch_dir_name(batch_id)
        self.staging_dir = self.query_dir / "_staging" / batch_dir_name(batch_id)
        self._committed = (self.final_dir / COMMIT_MARKER).is_file()

    # ---------------------------------------------------------------- state
    @property
    def committed(self) -> bool:
        return self._committed

    def begin(self) -> bool:
        """Prepare staging; returns True when the batch is ALREADY committed (skip all curated work)."""
        if (self.final_dir / COMMIT_MARKER).is_file():
            self._committed = True
            return True
        if self.final_dir.exists():
            if not self.staging_dir.exists():
                # crash between rename and _COMMITTED: resume from the renamed tree
                self.staging_dir.parent.mkdir(parents=True, exist_ok=True)
                os.rename(self.final_dir, self.staging_dir)
            else:
                trash = self.query_dir / "_trash" / f"{self.final_dir.name}.{int(time.time())}.{uuid.uuid4().hex[:6]}"
                trash.parent.mkdir(parents=True, exist_ok=True)
                os.rename(self.final_dir, trash)
        (self.staging_dir / UNITS_DIR).mkdir(parents=True, exist_ok=True)
        self._committed = False
        return False

    @staticmethod
    def unit_name(feed_abbr: str, snapshot_id: str) -> str:
        return safe_component(f"{feed_abbr}-{snapshot_id}", "curated unit")

    def _marker(self, unit: str) -> Path:
        return self.staging_dir / UNITS_DIR / f"{safe_component(unit, 'curated unit')}.json"

    def has_unit(self, unit: str) -> bool:
        return self._marker(unit).is_file()

    def write_unit(self, unit: str, datasets: Mapping[str, DataFrame], *, info: dict | None = None) -> None:
        """Write (overwrite) every dataset of ``unit`` then its completion marker."""
        if self._committed:
            raise RuntimeError(f"batch {self.batch_id} is already committed")
        safe_component(unit, "curated unit")
        names = []
        for ds, df in sorted(datasets.items()):
            safe_component(ds, "curated dataset")
            if df is None:
                continue
            target = self.staging_dir / ds / unit
            target.parent.mkdir(parents=True, exist_ok=True)
            df.write.mode("overwrite").parquet(str(target))
            names.append(ds)
        atomic_write_json(self._marker(unit), {"unit": unit, "datasets": names, "written_at": iso_utc(utcnow()),
                                               **(info or {})})

    def unit_files(self, dataset: str) -> list[Path]:
        """Parquet files of ``dataset`` in staging, only from units that have a completion marker."""
        root = self.staging_dir / dataset
        if not root.is_dir():
            return []
        out: list[Path] = []
        for unit_dir in sorted(p for p in root.iterdir() if p.is_dir()):
            if self.has_unit(unit_dir.name):
                out.extend(_parquet_files(unit_dir))
        return out

    def datasets(self) -> list[str]:
        if not self.staging_dir.is_dir():
            return []
        return sorted(p.name for p in self.staging_dir.iterdir()
                      if p.is_dir() and not p.name.startswith("_"))

    def commit(self, *, query_id: str | None, snapshot_ids: list[str], accounting: dict,
               row_counts: Mapping[str, int] | None = None) -> dict:
        """Atomic rename + ``_COMMITTED``. Idempotent (no-op when already committed)."""
        marker = self.final_dir / COMMIT_MARKER
        if marker.is_file():
            self._committed = True
            return json.loads(marker.read_text(encoding="utf-8"))
        files: dict[str, list[str]] = {}
        for ds in self.datasets():
            files[ds] = [str(p.relative_to(self.staging_dir)) for p in self.unit_files(ds)]
        units = sorted(p.stem for p in (self.staging_dir / UNITS_DIR).glob("*.json"))
        self.final_dir.parent.mkdir(parents=True, exist_ok=True)
        os.rename(self.staging_dir, self.final_dir)
        _fsync_dir(self.final_dir.parent)
        doc = {
            "manifest_version": 1,
            "run_id": self.run_id,
            "query_name": self.query_name,
            "query_id": query_id,
            "batch_id": self.batch_id,
            "committed_at": iso_utc(utcnow()),
            "units": units,
            "snapshot_ids": sorted(set(snapshot_ids)),
            "accounting": accounting,
            "datasets": {ds: {"files": fl, "rows": (row_counts or {}).get(ds)} for ds, fl in sorted(files.items())},
        }
        atomic_write_json(marker, doc)
        self._committed = True
        return doc


# --------------------------------------------------------------------------------------------------
# Readers (committed batches only)
# --------------------------------------------------------------------------------------------------

def committed_batches(paths: WorkPaths, run_id: str, query_name: str) -> list[dict]:
    """Parsed ``_COMMITTED`` documents (+ ``dir``) of every committed batch, ordered by batch id."""
    qdir = query_dir(paths, run_id, query_name)
    if not qdir.is_dir():
        return []
    out: list[dict] = []
    for d in sorted(qdir.glob("batch_id=*")):
        marker = d / COMMIT_MARKER
        if not d.is_dir() or not marker.is_file():
            continue
        doc = json.loads(marker.read_text(encoding="utf-8"))
        doc["dir"] = str(d)
        out.append(doc)
    out.sort(key=lambda x: int(x["batch_id"]))
    return out


def committed_files(batches: list[dict], dataset: str) -> list[str]:
    """Absolute file paths of ``dataset`` recorded in the given committed batch documents."""
    files: list[str] = []
    for b in batches:
        spec = (b.get("datasets") or {}).get(dataset) or {}
        for rel in spec.get("files") or []:
            files.append(str(Path(b["dir"]) / rel))
    return files


def read_committed(spark: SparkSession, files: list[str]):
    """Read exact committed parquet files (schema merged across units); ``None`` when there are none."""
    if not files:
        return None
    return spark.read.option("mergeSchema", "true").parquet(*files)
