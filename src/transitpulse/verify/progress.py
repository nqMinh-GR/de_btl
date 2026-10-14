"""Readers for processor progress evidence (stdlib only).

* ``query_progress.jsonl`` (one JSON object per ``onQueryProgress``, written by the processor): batch id,
  input rows, duration and the Kafka **end offsets** per topic/partition. Spark's Kafka end offset is
  exclusive: a message at offset ``o`` has been processed by a committed batch once ``endOffset > o``.
* Prometheus textfiles (``<W>/metrics/<component>.prom``) written by :mod:`transitpulse.obs.metrics`.
* Structured-streaming checkpoints (``offsets/<n>`` and ``commits/<n>``) used to prove that a failed batch was
  never committed.

The parsers are deliberately tolerant about the exact JSON layout of a progress record (a flat
``endOffsets`` mapping, a JSON string of it, or Spark's native ``sources[*].endOffset``) because the record is
an evidence file, not an API.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

TopicPartition = tuple[str, int]

_END_KEYS = ("endOffsets", "end_offsets", "endOffset", "end_offset")
_DURATION_KEYS = ("durationMs", "duration_ms", "batchDurationMs", "batch_duration_ms")
_DURATION_SUBKEYS = ("triggerExecution", "addBatch")


@dataclass(frozen=True)
class ProgressRecord:
    batch_id: int | None
    timestamp: str | None
    num_input_rows: int | None
    duration_ms: float | None
    end_offsets: dict[TopicPartition, int] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
        return int(value.strip())
    return None


def _maybe_json(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if text[:1] in ("{", "["):
            try:
                return json.loads(text)
            except ValueError:
                return None
        return None
    return value


def _partition_map(value: Any) -> dict[int, int] | None:
    """``{"0": 12, "1": 7}`` -> ``{0: 12, 1: 7}``; anything else -> None."""
    if not isinstance(value, dict) or not value:
        return None
    out: dict[int, int] = {}
    for k, v in value.items():
        part, off = _as_int(k), _as_int(v)
        if part is None or off is None:
            return None
        out[part] = off
    return out


def _collect_offsets(blob: Any, out: dict[TopicPartition, int]) -> None:
    blob = _maybe_json(blob)
    if blob is None:
        return
    if isinstance(blob, list):
        for item in blob:
            if isinstance(item, dict) and {"topic", "partition", "offset"} <= set(item):
                part, off = _as_int(item["partition"]), _as_int(item["offset"])
                if isinstance(item["topic"], str) and part is not None and off is not None:
                    key = (item["topic"], part)
                    out[key] = max(out.get(key, off), off)
            else:
                _collect_offsets(item, out)
        return
    if not isinstance(blob, dict):
        return
    for topic, value in blob.items():
        parts = _partition_map(value)
        if parts is not None and isinstance(topic, str):
            for part, off in parts.items():
                key = (topic, part)
                out[key] = max(out.get(key, off), off)
        elif isinstance(value, dict) and any(k in value for k in _END_KEYS):
            for k in _END_KEYS:
                if k in value:
                    _collect_offsets(value[k], out)


def parse_end_offsets(record: dict[str, Any]) -> dict[TopicPartition, int]:
    """End offsets of one progress record, keyed by (topic, partition)."""
    out: dict[TopicPartition, int] = {}
    for key in _END_KEYS:
        if key in record:
            _collect_offsets(record[key], out)
    sources = _maybe_json(record.get("sources"))
    if isinstance(sources, list):
        for src in sources:
            if isinstance(src, dict):
                for key in _END_KEYS:
                    if key in src:
                        _collect_offsets(src[key], out)
    return out


def _duration(record: dict[str, Any]) -> float | None:
    for key in _DURATION_KEYS:
        if key not in record:
            continue
        value = record[key]
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        value = _maybe_json(value) if isinstance(value, str) else value
        if isinstance(value, dict):
            for sub in _DURATION_SUBKEYS:
                v = value.get(sub)
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    return float(v)
    return None


def parse_progress_line(line: str) -> ProgressRecord | None:
    line = line.strip()
    if not line:
        return None
    try:
        rec = json.loads(line)
    except ValueError:
        return None
    if not isinstance(rec, dict):
        return None
    batch = _as_int(rec.get("batchId", rec.get("batch_id")))
    rows = _as_int(rec.get("numInputRows", rec.get("num_input_rows")))
    ts = rec.get("timestamp", rec.get("ts"))
    return ProgressRecord(
        batch_id=batch,
        timestamp=ts if isinstance(ts, str) else None,
        num_input_rows=rows,
        duration_ms=_duration(rec),
        end_offsets=parse_end_offsets(rec),
        raw=rec,
    )


def iter_progress(path: Path) -> Iterator[ProgressRecord]:
    """Yield complete records; a torn last line (writer mid-append) is ignored."""
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if not line.endswith("\n"):
                    break
                rec = parse_progress_line(line)
                if rec is not None:
                    yield rec
    except FileNotFoundError:
        return


def read_progress(path: Path) -> list[ProgressRecord]:
    return list(iter_progress(path))


def latest_end_offsets(path: Path) -> dict[TopicPartition, int]:
    """Max end offset per (topic, partition) over every progress record (monotonic across restarts)."""
    out: dict[TopicPartition, int] = {}
    for rec in iter_progress(path):
        for key, off in rec.end_offsets.items():
            out[key] = max(out.get(key, off), off)
    return out


def last_batch_id(path: Path) -> int | None:
    best: int | None = None
    for rec in iter_progress(path):
        if rec.batch_id is not None and (best is None or rec.batch_id > best):
            best = rec.batch_id
    return best


def first_batch_covering(path: Path, topic: str, partition: int, offset: int) -> ProgressRecord | None:
    """First progress record whose end offset for (topic, partition) is beyond ``offset``."""
    for rec in iter_progress(path):
        end = rec.end_offsets.get((topic, partition))
        if end is not None and end > offset:
            return rec
    return None


# --------------------------------------------------------------------------------------------------
# Prometheus text format (subset used by obs.metrics textfiles)
# --------------------------------------------------------------------------------------------------

_SAMPLE_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+([^\s]+)(\s+-?\d+)?\s*$")
_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


@dataclass(frozen=True)
class PromSample:
    name: str
    labels: dict[str, str]
    value: float


def _unescape(v: str) -> str:
    return v.replace("\\n", "\n").replace('\\"', '"').replace("\\\\", "\\")


def parse_prom_text(text: str) -> list[PromSample]:
    out: list[PromSample] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _SAMPLE_RE.match(line)
        if not m:
            continue
        name, _, label_text, value = m.group(1), m.group(2), m.group(3), m.group(4)
        labels = {k: _unescape(v) for k, v in _LABEL_RE.findall(label_text or "")}
        try:
            num = float(value)
        except ValueError:
            continue
        out.append(PromSample(name, labels, num))
    return out


def read_prom_file(path: Path) -> list[PromSample]:
    try:
        return parse_prom_text(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []


def prom_sum(samples: Iterable[PromSample], name: str, **labels: str) -> float | None:
    """Sum of samples named ``name`` whose labels include ``labels``; None when no sample matches."""
    total: float | None = None
    for s in samples:
        if s.name != name:
            continue
        if any(s.labels.get(k) != v for k, v in labels.items()):
            continue
        total = (total or 0.0) + s.value
    return total


# --------------------------------------------------------------------------------------------------
# Structured streaming checkpoint
# --------------------------------------------------------------------------------------------------

def _batch_files(directory: Path) -> list[int]:
    if not directory.is_dir():
        return []
    out = []
    for p in directory.iterdir():
        if p.is_file() and p.name.isdigit():
            out.append(int(p.name))
    return sorted(out)


def checkpoint_batches(checkpoint_dir: Path) -> dict[str, list[int]]:
    """Batch ids present in ``offsets/`` (planned) and ``commits/`` (completed)."""
    return {
        "offsets": _batch_files(checkpoint_dir / "offsets"),
        "commits": _batch_files(checkpoint_dir / "commits"),
    }


def checkpoint_offsets(checkpoint_dir: Path, batch_id: int) -> dict[TopicPartition, int]:
    """Kafka end offsets planned for ``batch_id`` (``offsets/<n>``: version, metadata, one line per source)."""
    path = checkpoint_dir / "offsets" / str(batch_id)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return {}
    out: dict[TopicPartition, int] = {}
    for line in lines[2:]:
        line = line.strip()
        if not line or line == "-":
            continue
        _collect_offsets(line, out)
    return out
