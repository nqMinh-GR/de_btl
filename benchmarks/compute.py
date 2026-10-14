"""Benchmark computations (design E §8, decision D-E25). Pure functions over collected evidence.

Every metric is returned as ``{"value": ..., "unit": ..., "source": ..., "method": ...}`` or with ``value: None``
and a ``reason`` — nothing is estimated or filled in.

Percentiles use ``statistics.quantiles(method="inclusive")`` (p50 = median). Minimum sample counts: p95 needs n >= 20,
p99 needs n >= 100; below that the percentile is null with the reason, while n, min and max are always reported.
"""

from __future__ import annotations

import json
import statistics
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MIN_P95 = 20
MIN_P99 = 100


def metric(value: Any, unit: str, source: str, method: str, **extra: Any) -> dict[str, Any]:
    return {"value": value, "unit": unit, "source": source, "method": method, **extra}


def missing(unit: str, source: str, reason: str) -> dict[str, Any]:
    return {"value": None, "unit": unit, "source": source, "method": None, "reason": reason}


def percentiles(values: Iterable[float], *, unit: str, source: str, min_p95: int = MIN_P95,
                min_p99: int = MIN_P99) -> dict[str, Any]:
    data = sorted(float(v) for v in values if v is not None)
    n = len(data)
    out: dict[str, Any] = {"n": n, "unit": unit, "source": source,
                           "method": f"statistics.quantiles(method='inclusive'); p95 needs n>={min_p95}, "
                                     f"p99 needs n>={min_p99}"}
    if n == 0:
        out.update(p50=None, p95=None, p99=None, min=None, max=None, mean=None, reason="no samples in the window")
        return out
    out.update(min=data[0], max=data[-1], mean=statistics.fmean(data), p50=statistics.median(data))
    q = statistics.quantiles(data, n=100, method="inclusive") if n >= 2 else None
    out["p95"] = q[94] if q is not None and n >= min_p95 else None
    out["p99"] = q[98] if q is not None and n >= min_p99 else None
    reasons = []
    if out["p95"] is None:
        reasons.append(f"p95 null: n={n} < {min_p95}")
    if out["p99"] is None:
        reasons.append(f"p99 null: n={n} < {min_p99}")
    if reasons:
        out["reason"] = "; ".join(reasons)
    return out


def parse_instant(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    return None


def in_window(ts: float | None, start: float, end: float) -> bool:
    return ts is not None and start <= ts < end


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                try:
                    doc = json.loads(line)
                except ValueError:
                    continue
                if isinstance(doc, dict):
                    out.append(doc)
    except FileNotFoundError:
        pass
    return out


def publish_log_rates(path: Path, start: float, end: float) -> dict[str, Any]:
    """Snapshots/s, bytes/s and publisher-declared candidates/s from ``publish_log.jsonl`` within the window."""
    rows = [r for r in read_jsonl(path) if r.get("kind") == "snapshot"]
    secs = end - start
    if not rows:
        reason = f"{path} has no snapshot lines" if path.exists() else f"{path} not found"
        return {k: missing(u, "publisher publish_log.jsonl", reason) for k, u in
                (("snapshots_per_s", "snapshot/s"), ("bytes_per_s", "B/s"), ("declared_candidates_per_s", "record/s"))}
    win = [r for r in rows if in_window(_ts(r.get("published_at")), start, end)]
    by_feed: dict[str, int] = {}
    for r in win:
        by_feed[str(r.get("feed_type"))] = by_feed.get(str(r.get("feed_type")), 0) + 1
    src = "publisher publish_log.jsonl (snapshot lines, published_at in window)"
    if secs <= 0:
        return {k: missing(u, src, "empty window") for k, u in
                (("snapshots_per_s", "snapshot/s"), ("bytes_per_s", "B/s"), ("declared_candidates_per_s", "record/s"))}
    return {
        "snapshots_per_s": metric(len(win) / secs, "snapshot/s", src, "count / window seconds", count=len(win),
                                  by_feed=by_feed),
        "bytes_per_s": metric(sum(int(r.get("bytes") or 0) for r in win) / secs, "B/s", src,
                              "sum(serialized envelope bytes) / window seconds"),
        "declared_candidates_per_s": metric(sum(int(r.get("candidates") or 0) for r in win) / secs, "record/s", src,
                                            "sum(contracts.accounting.count_candidates) / window seconds"),
    }


def _ts(value: Any) -> float | None:
    dt = parse_instant(value)
    return dt.timestamp() if dt else None


def kafka_scan_rates(messages: Iterable[Mapping[str, Any]], start: float, end: float) -> dict[str, Any]:
    """Cross-check from the broker: messages of the run on the snapshot topics, by producer CreateTime."""
    win = [m for m in messages if in_window((m.get("timestamp_ms") or 0) / 1000.0, start, end)]
    secs = end - start
    src = "Kafka scan of the run's messages on the snapshot topics (record CreateTime in window)"
    if secs <= 0:
        return {"messages_per_s": missing("message/s", src, "empty window")}
    return {"messages_per_s": metric(len(win) / secs, "message/s", src, "count / window seconds", count=len(win)),
            "message_bytes_per_s": metric(sum(len(m.get("value") or b"") for m in win) / secs, "B/s", src,
                                          "sum(len(value)) / window seconds")}


def prom_delta(before: Mapping[str, float | None], after: Mapping[str, float | None], name: str) -> float | None:
    a, b = before.get(name), after.get(name)
    if a is None or b is None:
        return None
    return b - a


def processor_rates(before: Mapping[str, float | None] | None, after: Mapping[str, float | None] | None,
                    secs: float) -> dict[str, Any]:
    src = "processor.prom textfile (exported every observability.metrics_export_seconds) at window edges"
    names = {
        "normalized_candidates_per_s": "tp_processor_normalized_candidates_total",
        "unique_records_per_s": "tp_processor_unique_records_total",
        "duplicate_records_per_s": "tp_processor_duplicate_records_total",
    }
    if before is None or after is None or secs <= 0:
        reason = "no processor metrics snapshot at both window edges"
        out = {k: missing("record/s", src, reason) for k in names}
        out["duplicate_fraction"] = missing("fraction", src, reason)
        return out
    out: dict[str, Any] = {}
    deltas: dict[str, float | None] = {}
    for key, name in names.items():
        d = prom_delta(before, after, name)
        deltas[key] = d
        out[key] = metric(d / secs, "record/s", src, f"delta({name}) / window seconds", delta=d) if d is not None \
            else missing("record/s", src, f"{name} not exported")
    u, dup = deltas["unique_records_per_s"], deltas["duplicate_records_per_s"]
    if u is None or dup is None or (u + dup) == 0:
        out["duplicate_fraction"] = missing("fraction", src, "no valid candidate observations in the window")
    else:
        out["duplicate_fraction"] = metric(dup / (u + dup), "fraction", src, "duplicates / (unique + duplicates)",
                                           denominator=u + dup, denominator_unit="valid candidate observations")
    return out


def visibility_latency(manifests: Iterable[Mapping[str, Any]], start: float, end: float) -> dict[str, Any]:
    """first visible (pointer) minus first enqueue (envelope published_at) for generations enqueued in the window."""
    lat: list[float] = []
    late = expired = never = 0
    for m in manifests:
        pub = parse_instant(m.get("published_at"))
        if pub is None or not in_window(pub.timestamp(), start, end):
            continue
        outcome = m.get("publish_outcome")
        if outcome == "LATE_NOT_PUBLISHED":
            late += 1
            continue
        if outcome == "EXPIRED_NOT_PUBLISHED":
            expired += 1
            continue
        if m.get("visibility_latency_ms") is not None:
            lat.append(float(m["visibility_latency_ms"]) / 1000.0)
            continue
        vis = parse_instant(m.get("first_visible_at"))
        if vis is None:
            never += 1
            continue
        lat.append((vis - pub).total_seconds())
    out = percentiles(lat, unit="s", source="snapshot_manifest (visibility_latency_ms, else first_visible_at - "
                                            "published_at) of generations enqueued in the window")
    out.update(late_not_published=late, expired_not_published=expired, never_visible=never)
    return out


def batch_stats(records: Iterable[Any], start: float, end: float) -> dict[str, Any]:
    durs, rows, n = [], 0, 0
    for r in records:
        ts = _ts(getattr(r, "timestamp", None))
        if not in_window(ts, start, end):
            continue
        n += 1
        if r.duration_ms is not None:
            durs.append(r.duration_ms / 1000.0)
        rows += int(r.num_input_rows or 0)
    out = percentiles(durs, unit="s", source="query_progress.jsonl durationMs (triggerExecution)")
    out.update(batches=n, input_rows=rows)
    return out


def log_stage_durations(log_path: Path | None, stage: str, start: float, end: float) -> dict[str, Any]:
    if log_path is None or not log_path.is_file():
        return percentiles([], unit="s", source=f"processor log stage={stage}") | {"reason": "processor log missing"}
    vals = []
    for r in read_jsonl(log_path):
        if r.get("stage") == stage and isinstance(r.get("duration_ms"), (int, float)) and in_window(
                _ts(r.get("ts")), start, end):
            vals.append(float(r["duration_ms"]) / 1000.0)
    out = percentiles(vals, unit="s", source=f"processor JSON log, stage={stage}, duration_ms")
    if not vals:
        out["reason"] = f"no '{stage}' log lines with duration_ms in the window"
    return out


def resources(samples: Iterable[Mapping[str, Any]], start: float, end: float) -> dict[str, Any]:
    by_comp: dict[str, list[Mapping[str, Any]]] = {}
    for s in samples:
        if in_window(s.get("t_wall"), start, end):
            by_comp.setdefault(str(s["component"]), []).append(s)
    out: dict[str, Any] = {"source": "psutil over supervisor pids + children", "unit_cpu": "% of one core",
                           "unit_rss": "MiB", "components": {}}
    totals: dict[float, dict[str, float]] = {}
    for comp, rows in sorted(by_comp.items()):
        live = [r for r in rows if r.get("processes")]
        cpus = [float(r["cpu_percent"]) for r in live]
        rss = [int(r["rss_bytes"]) for r in live]
        out["components"][comp] = {
            "samples": len(live),
            "cpu_mean_percent": statistics.fmean(cpus) if cpus else None,
            "cpu_max_percent": max(cpus) if cpus else None,
            "rss_max_mib": max(rss) / 2**20 if rss else None,
        }
        for r in live:
            slot = totals.setdefault(round(float(r["t_wall"]), 0), {"cpu": 0.0, "rss": 0.0})
            slot["cpu"] += float(r["cpu_percent"])
            slot["rss"] += int(r["rss_bytes"])
    if totals:
        out["total"] = {"cpu_mean_percent": statistics.fmean(v["cpu"] for v in totals.values()),
                        "cpu_max_percent": max(v["cpu"] for v in totals.values()),
                        "rss_max_mib": max(v["rss"] for v in totals.values()) / 2**20}
    else:
        out["total"] = {"cpu_mean_percent": None, "cpu_max_percent": None, "rss_max_mib": None,
                        "reason": "no resource samples in the window"}
    return out


def backlog(samples: Iterable[Mapping[str, Any]], start: float, end: float) -> dict[str, Any]:
    per: dict[str, list[int]] = {}
    for s in samples:
        if in_window(s.get("t_wall"), start, end) and s.get("backlog_messages") is not None:
            per.setdefault(f"{s['topic']}[{s['partition']}]", []).append(int(s["backlog_messages"]))
    return {"source": "high watermark - latest query_progress endOffset", "unit": "message",
            "per_partition": {k: {"max": max(v), "mean": statistics.fmean(v), "samples": len(v)}
                              for k, v in sorted(per.items())},
            "max": max((max(v) for v in per.values()), default=None)}


def rows_written(manifests: Iterable[Mapping[str, Any]], start: float, end: float) -> dict[str, Any]:
    total, n = 0, 0
    for m in manifests:
        ready = parse_instant(m.get("ready_at"))
        if m.get("status") != "READY" or ready is None or not in_window(ready.timestamp(), start, end):
            continue
        n += 1
        total += sum(int(v) for k, v in (m.get("expected_counts") or {}).items() if "/" not in str(k))
    return metric(total, "row", "snapshot_manifest.expected_counts (table totals) of generations READY in the window",
                  "sum over tables (Cassandra serving + history + quality rows)", generations=n)


def disk_delta(start: Mapping[str, Any], end: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {"unit": "B", "method": start.get("method")}
    for k, v in start.items():
        if k in ("t_wall", "method"):
            continue
        e = end.get(k)
        out[k] = None if v is None or e is None else int(e) - int(v)
    return out


def status_for(measured_s: float, measure_s: float, finished_early: bool) -> str:
    if measured_s <= 0:
        return "NOT_RUN"
    if measured_s + 1.0 >= measure_s and not finished_early:
        return "MEASURED"
    return "PARTIAL"
