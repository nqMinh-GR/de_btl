"""Benchmark outputs under ``<W>/reports/benchmarks/<bench_id>/`` (design E §8).

* ``benchmark_results.json`` / ``benchmark_results.csv`` — one row per scenario x repetition, status
  ``MEASURED`` | ``PARTIAL`` | ``NOT_RUN``; every value comes from collected evidence (no hand-filled numbers);
* ``environment.json``, ``resource_samples.csv``, ``backlog_samples.csv``;
* ``benchmark_report.md`` — the protocol actually executed, reference TARGETS kept separate from MEASURED values,
  synthetic and replay results in separate sections, limitations;
* ``fault_recovery_report.md`` — per fault: injection time, expected vs actual, recovery time, log references.
"""

from __future__ import annotations

import csv
import json
import os
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CSV_COLUMNS = (
    "bench_id", "scenario", "kind", "repetition", "run_id", "source_type", "speed", "target_rate", "warmup_s",
    "measured_s", "status", "snapshots_per_s", "bytes_per_s", "declared_candidates_per_s",
    "normalized_candidates_per_s", "unique_records_per_s", "duplicate_fraction", "visibility_latency_p50_s",
    "visibility_latency_p95_s", "visibility_latency_p99_s", "visibility_latency_n", "visibility_latency_max_s",
    "source_age_p50_s", "source_age_p95_s", "batch_duration_p50_s", "batch_duration_p95_s",
    "cassandra_write_p50_s", "cassandra_write_p95_s", "cpu_total_mean_percent", "cpu_total_max_percent",
    "rss_total_max_mib", "backlog_max_messages", "cassandra_rows_written", "error_rate", "recovery_s", "samples_n",
    "notes",
)


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _v(d: Any, *path: str) -> Any:
    node = d
    for p in path:
        if not isinstance(node, Mapping):
            return None
        node = node.get(p)
    return node


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def csv_row(r: Mapping[str, Any]) -> dict[str, Any]:
    m = r.get("metrics") or {}
    return {
        "bench_id": r.get("bench_id"), "scenario": r.get("scenario"), "kind": r.get("kind"),
        "repetition": r.get("repetition"), "run_id": r.get("run_id"), "source_type": r.get("source_type"),
        "speed": r.get("speed"), "target_rate": r.get("target_rate"), "warmup_s": r.get("warmup_s"),
        "measured_s": r.get("measured_s"), "status": r.get("status"),
        "snapshots_per_s": _v(m, "snapshots_per_s", "value"), "bytes_per_s": _v(m, "bytes_per_s", "value"),
        "declared_candidates_per_s": _v(m, "declared_candidates_per_s", "value"),
        "normalized_candidates_per_s": _v(m, "normalized_candidates_per_s", "value"),
        "unique_records_per_s": _v(m, "unique_records_per_s", "value"),
        "duplicate_fraction": _v(m, "duplicate_fraction", "value"),
        "visibility_latency_p50_s": _v(m, "visibility_latency", "p50"),
        "visibility_latency_p95_s": _v(m, "visibility_latency", "p95"),
        "visibility_latency_p99_s": _v(m, "visibility_latency", "p99"),
        "visibility_latency_n": _v(m, "visibility_latency", "n"),
        "visibility_latency_max_s": _v(m, "visibility_latency", "max"),
        "source_age_p50_s": _v(m, "source_age", "p50"), "source_age_p95_s": _v(m, "source_age", "p95"),
        "batch_duration_p50_s": _v(m, "batch_duration", "p50"), "batch_duration_p95_s": _v(m, "batch_duration", "p95"),
        "cassandra_write_p50_s": _v(m, "cassandra_write", "p50"), "cassandra_write_p95_s": _v(m, "cassandra_write", "p95"),
        "cpu_total_mean_percent": _v(m, "resources", "total", "cpu_mean_percent"),
        "cpu_total_max_percent": _v(m, "resources", "total", "cpu_max_percent"),
        "rss_total_max_mib": _v(m, "resources", "total", "rss_max_mib"),
        "backlog_max_messages": _v(m, "backlog", "max"),
        "cassandra_rows_written": _v(m, "cassandra_rows_written", "value"),
        "error_rate": _v(m, "error_rate", "value"),
        "recovery_s": _v(r, "recovery", "recovery_seconds"),
        "samples_n": r.get("samples_n"),
        "notes": "; ".join(r.get("notes") or [])[:1000],
    }


def write_csv(path: Path, rows: Iterable[Mapping[str, Any]], columns: Iterable[str]) -> None:
    cols = list(columns)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in cols})
    os.replace(tmp, path)


def _fmt(v: Any, digits: int = 3) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.{digits}f}"
    return str(v)


def _section(results: list[Mapping[str, Any]], title: str) -> list[str]:
    lines = [f"### {title}", ""]
    if not results:
        return lines + ["No scenario of this kind was requested in this run.", ""]
    lines += ["| scenario | rep | status | measured s | snapshots/s | candidates/s (processor) | unique/s | "
              "dup fraction | latency p50 / p95 / p99 (n) s | batch p95 s | CPU max % | RSS max MiB | backlog max |",
              "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in results:
        m = r.get("metrics") or {}
        lat = m.get("visibility_latency") or {}
        lines.append(
            f"| {r['scenario']} | {r['repetition']} | {r['status']} | {_fmt(r.get('measured_s'), 1)} | "
            f"{_fmt(_v(m, 'snapshots_per_s', 'value'))} | {_fmt(_v(m, 'normalized_candidates_per_s', 'value'))} | "
            f"{_fmt(_v(m, 'unique_records_per_s', 'value'))} | {_fmt(_v(m, 'duplicate_fraction', 'value'), 4)} | "
            f"{_fmt(lat.get('p50'))} / {_fmt(lat.get('p95'))} / {_fmt(lat.get('p99'))} ({lat.get('n', 0)}) | "
            f"{_fmt(_v(m, 'batch_duration', 'p95'))} | {_fmt(_v(m, 'resources', 'total', 'cpu_max_percent'), 1)} | "
            f"{_fmt(_v(m, 'resources', 'total', 'rss_max_mib'), 0)} | {_fmt(_v(m, 'backlog', 'max'))} |")
    notes = [f"* {r['scenario']} rep {r['repetition']}: {n}" for r in results for n in (r.get("notes") or [])]
    return lines + [""] + (["Notes:", ""] + notes + [""] if notes else [])


def benchmark_report_md(doc: Mapping[str, Any]) -> str:
    env = doc.get("environment") or {}
    results = list(doc.get("results") or [])
    replay = [r for r in results if r.get("kind") == "replay"]
    stress = [r for r in results if r.get("kind") == "stress"]
    recovery = [r for r in results if r.get("kind") == "recovery"]
    protocol = doc.get("protocol") or {}
    lines = [
        f"# Benchmark report `{doc.get('bench_id')}`", "",
        f"Generated {doc.get('generated_at')} from the evidence in this directory. Values below are **measured** by "
        "the harness; anything not measured is shown as `—` with a reason in `benchmark_results.json`.", "",
        "## Reference targets (NOT results)", "",
        "| target | value | scope | status |", "|---|---|---|---|",
    ]
    for name, t in (doc.get("targets") or {}).items():
        lines.append(f"| {name} | {t.get('value')} {t.get('unit', '')} | {t.get('scope', '')} | target, not a result |")
    lines += ["", "## Protocol actually executed", "",
              f"* command: `{protocol.get('command')}`",
              f"* warm-up {protocol.get('warmup_s')} s, measurement window {protocol.get('measure_s')} s "
              f"(truncated at the end of the timeline -> PARTIAL), repetitions {protocol.get('repetitions')}, "
              f"drain timeout {protocol.get('drain_timeout_s')} s; every repetition uses a NEW run",
              "* samplers: psutil every "
              f"{protocol.get('resource_interval_s')} s (supervisor pids + children), Kafka backlog every "
              f"{protocol.get('backlog_interval_s')} s (high watermark - committed end offset), disk at start/end",
              "* latency = first view publication (pointer) - first enqueue (envelope published_at), per generation; "
              "source age (application clock - observation time) is reported separately and never added",
              "* percentiles: statistics.quantiles(method='inclusive'); p95 needs n >= 20, p99 needs n >= 100", "",
              "## Environment", "",
              f"* CPU: {_v(env, 'cpu', 'model')} x {_v(env, 'cpu', 'count')}; RAM {env.get('ram_gib')} GiB; "
              f"disk free {env.get('disk_free_gib')} GiB",
              f"* versions: Python {env.get('python')}, Java {env.get('java')}, Spark {env.get('spark')}, "
              f"Kafka {env.get('kafka')}, Cassandra {env.get('cassandra')}",
              f"* commit {env.get('commit')}, config hash {env.get('config_hash')}, profile {env.get('profile')}",
              f"* cache state: {env.get('cache_state')}", "",
              "## Results", ""]
    lines += _section(replay, "Replay of a real recording (reported separately from synthetic)")
    lines += _section(stress, "Synthetic stress (synthetic data, not MBTA traffic)")
    lines += ["### Recovery", ""]
    if recovery:
        lines += ["| scenario | rep | status | recovery s | matches expected | lost | duplicated | fault scope |",
                  "|---|---|---|---|---|---|---|---|"]
        for r in recovery:
            rec = r.get("recovery") or {}
            lines.append(f"| {r['scenario']} | {r['repetition']} | {r['status']} | {_fmt(rec.get('recovery_seconds'), 1)}"
                         f" | {rec.get('matches_expected')} | {_fmt(rec.get('records_lost'))} | "
                         f"{_fmt(rec.get('records_duplicated'))} | {rec.get('fault_scope', '—')} |")
        lines += ["", "Details: `fault_recovery_report.md`.", ""]
    else:
        lines += ["No recovery scenario was requested in this run.", ""]
    lines += ["## Limitations", "",
              "* single Kafka broker, single Cassandra node (RF1), local[2] Spark: numbers describe this machine only;",
              "* the processor JVM starts cold for every repetition (catch-up of earlier topic data is inside the "
              "warm-up, D-PUB-08); OS page cache is not controlled;",
              "* processor rates come from textfile metrics exported every few seconds, so window edges are "
              "approximate by up to one export interval;",
              "* API latency and UI latency are not part of this protocol and are not reported.", ""]
    return "\n".join(lines)


def fault_recovery_md(doc: Mapping[str, Any]) -> str:
    rec = [r for r in doc.get("results") or [] if r.get("kind") == "recovery"]
    lines = [f"# Fault recovery report `{doc.get('bench_id')}`", "",
             "Each fault is injected on an owned process only (one-shot arm file, owned Cassandra stop, or SIGKILL of "
             "the owned collector). Expected values come from `tests/fixtures/golden/expected.json`.", ""]
    if not rec:
        return "\n".join(lines + ["No recovery scenario was requested in this run.", ""])
    for r in rec:
        d = r.get("recovery") or {}
        lines += [f"## {r['scenario']} (repetition {r['repetition']}) — {r['status']}", "",
                  f"* run: `{r.get('run_id')}`",
                  f"* fault: {d.get('fault')} injected at {d.get('injected_at')}; exit code {d.get('exit_code')}",
                  f"* recovery started at {d.get('recovery_started_at')}; ready again after "
                  f"{_fmt(d.get('recovery_seconds'), 1)} s",
                  f"* expected vs actual: matches_expected={d.get('matches_expected')}; failed checks: "
                  f"{d.get('failed_checks') or 'none'}",
                  f"* records lost: {_fmt(d.get('records_lost'))}; duplicated: {_fmt(d.get('records_duplicated'))}",
                  f"* scope: {d.get('fault_scope')}",
                  f"* logs: {', '.join(d.get('logs') or []) or '—'}"]
        lines += [f"* note: {n}" for n in r.get("notes") or []]
        lines.append("")
    return "\n".join(lines)


def write_outputs(out_dir: Path, doc: dict[str, Any], resource_samples: list[dict[str, Any]],
                  backlog_samples: list[dict[str, Any]]) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "results_json": out_dir / "benchmark_results.json",
        "results_csv": out_dir / "benchmark_results.csv",
        "environment": out_dir / "environment.json",
        "resource_samples": out_dir / "resource_samples.csv",
        "backlog_samples": out_dir / "backlog_samples.csv",
        "report_md": out_dir / "benchmark_report.md",
        "fault_recovery_md": out_dir / "fault_recovery_report.md",
    }
    doc.setdefault("generated_at", _now())
    _atomic_text(files["results_json"], json.dumps(doc, indent=2, default=str) + "\n")
    _atomic_text(files["environment"], json.dumps(doc.get("environment") or {}, indent=2, default=str) + "\n")
    write_csv(files["results_csv"], [csv_row(r) for r in doc.get("results") or []], CSV_COLUMNS)
    write_csv(files["resource_samples"], resource_samples,
              ("scenario", "repetition", "t_wall", "component", "cpu_percent", "rss_bytes", "processes",
               "cpu_primed_processes"))
    write_csv(files["backlog_samples"], backlog_samples,
              ("scenario", "repetition", "t_wall", "topic", "partition", "high_watermark", "processed_end_offset",
               "backlog_messages"))
    _atomic_text(files["report_md"], benchmark_report_md(doc))
    _atomic_text(files["fault_recovery_md"], fault_recovery_md(doc))
    return {k: str(v) for k, v in files.items()}
