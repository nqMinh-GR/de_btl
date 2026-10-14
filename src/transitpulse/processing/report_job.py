"""FR09 batch analytics over COMMITTED curated batches (design B §7, spec §10.5).

    python -m transitpulse.processing.report_job --workdir W --run-id R [--profile P] [--query-name Q] [--force]

Input: only batches with ``_COMMITTED`` and only the files listed there (never directory discovery).
Output: ``W/reports/<run_id>/analytics/<report_id>/`` where ``report_id = H(K(report, v1, run, query,
<committed batch ids>))``, written to a staging directory, renamed, then ``_COMMITTED`` (a rerun over the
same committed input is a no-op unless ``--force``).

Metrics (units and denominators are written to ``definitions.json``):
  * vehicle activity per route and 1-minute / 5-minute event-time window (``F.window`` on
    ``event_time``): distinct vehicles, observation count (observations deduplicated by
    ``observation_id`` BEFORE aggregation; snapshot_state_samples and INVALID_* / FUTURE_TIMESTAMP /
    OBSERVATION_TIME_REGRESSION records excluded from time-based metrics and the samples reported
    separately as ``unknown_age_sample_count``), optional derived speed (consecutive observations of a
    vehicle, haversine / dt, dt > 0, GPS-jump cutoff 40 m/s);
  * predicted-late rate per route and 5-minute window of the generation ``metrics_as_of``: target KPI
    records (``is_target`` and ``kpi_eligible``) deduplicated by (snapshot_id, prediction_id), latest
    generation per trip instance in the window, ``late = predicted_delay_seconds >= policy.predicted_late_seconds``;
    rate = late / eligible, NULL when the denominator is 0;
  * pipeline visibility latency p50/p95 (exact percentiles) per feed with the sample count, from the
    committed ``publications`` dataset (first publication of each generation only).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from transitpulse.contracts.envelope import iso_utc, utcnow
from transitpulse.contracts.ids import H, K
from transitpulse.obs.logging import configure, get_logger, log_event
from transitpulse.runtime.paths import WorkPaths, safe_component

_log = get_logger("transitpulse.processing.report_job")
ACTION = "report"
REPORT_VERSION = "v1"
GPS_JUMP_MAX_MPS = 40.0
EXCLUDED_FLAGS: tuple[str, ...] = (
    "INVALID_COORDINATES", "FUTURE_TIMESTAMP", "INVALID_TIMESTAMP_UNIT", "OBSERVATION_TIME_REGRESSION",
    "MISSING_VEHICLE_ID", "INVALID_ENTITY",
)
OBS_SPEC: dict[str, str] = {
    "observation_id": "string", "vehicle_id": "string", "route_id": "string", "event_time": "timestamp",
    "record_kind": "string", "quality_flags": "array<string>", "latitude": "double", "longitude": "double",
}
PRED_SPEC: dict[str, str] = {
    "snapshot_id": "string", "prediction_id": "string", "route_id": "string", "is_target": "boolean",
    "kpi_eligible": "boolean", "predicted_delay_seconds": "int", "trip_id": "string", "start_date": "string",
    "service_date": "string", "trip_instance_key": "string",
}
PUB_SPEC: dict[str, str] = {
    "feed_type": "string", "snapshot_id": "string", "source_order": "string", "metrics_as_of": "timestamp",
    "publish_outcome": "string", "visibility_latency_ms": "bigint", "first_visible_at": "timestamp",
}

DEFINITIONS: dict[str, Any] = {
    "report_version": REPORT_VERSION,
    "vehicle_windows": {
        "window": "tumbling event-time window on observation event_time (UTC), 1 minute and 5 minutes",
        "distinct_vehicles": {"unit": "vehicle", "definition": "distinct vehicle_id with >= 1 timed observation"},
        "observation_count": {"unit": "observation",
                              "definition": "unique observation_id (deduplicated before aggregation), "
                                            "excluding snapshot_state_samples and invalid/regressed records"},
        "unknown_age_sample_count": {"unit": "snapshot_state_sample",
                                     "definition": "vehicles without a source timestamp (event_time = feed header)"},
        "mean_derived_speed_mps": {"unit": "m/s", "denominator": "speed_sample_count",
                                   "definition": "haversine distance / dt between consecutive observations of a "
                                                 f"vehicle, dt > 0, speeds above {GPS_JUMP_MAX_MPS} m/s dropped; "
                                                 "NULL when there is no sample"},
    },
    "prediction_late_rate": {
        "window": "5-minute window of the generation metrics_as_of (UTC)",
        "eligible_trip_count": {"unit": "trip_instance", "definition": "target KPI predictions (is_target and "
                                "kpi_eligible), latest generation per trip instance in the window"},
        "predicted_late_trip_count": {"unit": "trip_instance",
                                      "definition": "eligible with predicted_delay_seconds >= predicted_late_seconds"},
        "predicted_late_rate": {"unit": "ratio", "numerator": "predicted_late_trip_count",
                                "denominator": "eligible_trip_count", "null_when": "denominator == 0"},
        "mean_predicted_delay_seconds": {"unit": "s", "denominator": "eligible_trip_count",
                                         "null_when": "denominator == 0"},
    },
    "visibility_latency": {
        "definition": "view_published_at - envelope published_at of the first publication of each generation "
                      "(wall clocks of one host)",
        "unit": "ms", "statistics": "exact p50 and p95 with sample_count; NULL when sample_count == 0",
    },
}


def emit(doc: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(doc, default=str, sort_keys=True) + "\n")
    sys.stdout.flush()


class _JsonArgParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # type: ignore[override]
        emit({"ok": False, "action": ACTION, "error": {"code": "usage", "message": message,
              "hint": "python -m transitpulse.processing.report_job --workdir W --run-id R", "log_excerpt": None}})
        sys.exit(2)


def report_id_for(run_id: str, query_name: str, batch_ids: list[int]) -> str:
    return "an_" + H(K("report", REPORT_VERSION, run_id, query_name, json.dumps(sorted(batch_ids))))


# --------------------------------------------------------------------------------------------------
# Spark computations
# --------------------------------------------------------------------------------------------------

def _ensure(df, spec: dict[str, str]):
    from pyspark.sql import functions as F

    present = set(df.columns)
    missing = [n for n in spec if n not in present]
    return df.select(*[(F.col(n).cast(t) if n in present else F.lit(None).cast(t)).alias(n)
                       for n, t in spec.items()]), missing


def vehicle_windows(obs, window: str):
    """Per route and event-time window: distinct vehicles, observation count, samples, derived speed."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    obs = obs.dropDuplicates(["observation_id"])
    flags = F.coalesce(F.col("quality_flags"), F.array().cast("array<string>"))
    is_sample = F.coalesce(F.col("record_kind") == F.lit("snapshot_state_sample"), F.lit(False)) | \
        F.coalesce(F.col("observation_id").startswith("ss_"), F.lit(False))
    excluded = F.coalesce(F.arrays_overlap(flags, F.array(*[F.lit(x) for x in EXCLUDED_FLAGS])), F.lit(False))
    timed = obs.where(~is_sample & ~excluded & F.col("event_time").isNotNull() & F.col("vehicle_id").isNotNull())
    samples = obs.where(is_sample & F.col("event_time").isNotNull())

    wv = Window.partitionBy("vehicle_id").orderBy("event_time", "observation_id")
    lat1, lon1 = F.radians(F.lag("latitude").over(wv)), F.radians(F.lag("longitude").over(wv))
    lat2, lon2 = F.radians(F.col("latitude")), F.radians(F.col("longitude"))
    dt = F.col("event_time").cast("double") - F.lag("event_time").over(wv).cast("double")
    a = F.pow(F.sin((lat2 - lat1) / 2), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin((lon2 - lon1) / 2), 2)
    dist = F.lit(2 * 6371000.0) * F.asin(F.sqrt(a))
    speeds = timed.withColumn("_dt", dt).withColumn("_speed", F.when(dt > 0, dist / dt))
    speeds = speeds.withColumn("_speed", F.when(F.col("_speed") <= F.lit(GPS_JUMP_MAX_MPS), F.col("_speed")))

    w = F.window("event_time", window)
    main = speeds.groupBy(w.alias("w"), "route_id").agg(
        F.countDistinct("vehicle_id").alias("distinct_vehicles"),
        F.count(F.lit(1)).alias("observation_count"),
        F.count("_speed").alias("speed_sample_count"),
        F.avg("_speed").alias("mean_derived_speed_mps"),
    )
    smp = samples.groupBy(F.window("event_time", window).alias("w"), "route_id").agg(
        F.count(F.lit(1)).alias("unknown_age_sample_count"))
    out = main.join(smp, ["w", "route_id"], "full_outer")
    return out.select(
        F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "route_id",
        F.coalesce("distinct_vehicles", F.lit(0)).alias("distinct_vehicles"),
        F.coalesce("observation_count", F.lit(0)).alias("observation_count"),
        F.coalesce("unknown_age_sample_count", F.lit(0)).alias("unknown_age_sample_count"),
        F.coalesce("speed_sample_count", F.lit(0)).alias("speed_sample_count"),
        F.col("mean_derived_speed_mps"),
    ).orderBy("window_start", "route_id")


def late_rate(pred, pubs, *, late_seconds: int, window: str = "5 minutes"):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    pred = pred.dropDuplicates(["snapshot_id", "prediction_id"])
    targets = pred.where(F.coalesce(F.col("is_target"), F.lit(False)) & F.coalesce(F.col("kpi_eligible"), F.lit(False))
                         & F.col("predicted_delay_seconds").isNotNull())
    gens = pubs.select("snapshot_id", "metrics_as_of", "source_order").where(F.col("metrics_as_of").isNotNull()) \
        .dropDuplicates(["snapshot_id"])
    t = targets.join(gens, "snapshot_id", "inner")
    trip_key = F.coalesce(F.col("trip_instance_key"),
                          F.concat_ws("|", F.col("trip_id"), F.coalesce(F.col("service_date"), F.col("start_date"))))
    t = t.withColumn("_trip", trip_key).withColumn("w", F.window("metrics_as_of", window))
    rank = Window.partitionBy("route_id", "w", "_trip").orderBy(F.col("source_order").desc(), F.col("prediction_id"))
    latest = t.withColumn("_rn", F.row_number().over(rank)).where(F.col("_rn") == 1)
    agg = latest.groupBy("w", "route_id").agg(
        F.count(F.lit(1)).alias("eligible_trip_count"),
        F.sum(F.when(F.col("predicted_delay_seconds") >= F.lit(int(late_seconds)), 1).otherwise(0))
        .alias("predicted_late_trip_count"),
        F.avg(F.col("predicted_delay_seconds").cast("double")).alias("_mean"),
    )
    return agg.select(
        F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "route_id",
        "eligible_trip_count", "predicted_late_trip_count",
        F.when(F.col("eligible_trip_count") > 0,
               F.col("predicted_late_trip_count") / F.col("eligible_trip_count")).alias("predicted_late_rate"),
        F.when(F.col("eligible_trip_count") > 0, F.col("_mean")).alias("mean_predicted_delay_seconds"),
        F.lit(int(late_seconds)).alias("late_threshold_seconds"),
    ).orderBy("window_start", "route_id")


def latency_stats(pubs) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    first = pubs.where((F.col("publish_outcome") == F.lit("PUBLISHED")) & F.col("visibility_latency_ms").isNotNull()) \
        .dropDuplicates(["feed_type", "snapshot_id"])
    rows = first.groupBy("feed_type").agg(
        F.count(F.lit(1)).alias("sample_count"),
        F.expr("percentile(visibility_latency_ms, array(0.5, 0.95))").alias("p"),
    ).collect()
    out = []
    for r in rows:
        p = r["p"] or [None, None]
        out.append({"feed_type": r["feed_type"], "sample_count": int(r["sample_count"]), "unit": "ms",
                    "p50": p[0], "p95": p[1]})
    return sorted(out, key=lambda d: d["feed_type"] or "")


# --------------------------------------------------------------------------------------------------
# Output (staging -> rename -> _COMMITTED)
# --------------------------------------------------------------------------------------------------

def _write_df(df, base: Path, name: str) -> dict[str, Any]:
    df = df.cache()
    try:
        n = df.count()
        df.coalesce(1).write.mode("overwrite").parquet(str(base / name / "parquet"))
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(base / name / "csv"))
    finally:
        df.unpersist()
    return {"rows": int(n), "parquet": f"{name}/parquet", "csv": f"{name}/csv"}


def build_report(spark, paths: WorkPaths, run_rec, batches: list[dict], query_name: str, out_dir: Path,
                 report_id: str) -> dict[str, Any]:
    from transitpulse.processing.curated import atomic_write_json, committed_files, read_committed

    staging = out_dir.parent / f"_staging-{report_id}-{uuid.uuid4().hex[:8]}"
    staging.mkdir(parents=True, exist_ok=True)
    late_seconds = int(run_rec.policy.predicted_late_seconds)
    outputs: dict[str, Any] = {}
    warnings: list[str] = []

    obs = read_committed(spark, committed_files(batches, "observations"))
    if obs is not None:
        obs, missing = _ensure(obs, OBS_SPEC)
        if missing:
            warnings.append(f"observations lack columns {missing} (treated as NULL)")
        outputs["vehicle_windows_1m"] = _write_df(vehicle_windows(obs, "1 minute"), staging, "vehicle_windows_1m")
        outputs["vehicle_windows_5m"] = _write_df(vehicle_windows(obs, "5 minutes"), staging, "vehicle_windows_5m")
    else:
        warnings.append("no committed observations")

    pubs = read_committed(spark, committed_files(batches, "publications"))
    if pubs is not None:
        pubs, _ = _ensure(pubs, PUB_SPEC)
    pred = read_committed(spark, committed_files(batches, "predictions"))
    if pred is not None and pubs is not None:
        pred, missing = _ensure(pred, PRED_SPEC)
        if missing:
            warnings.append(f"predictions lack columns {missing} (treated as NULL)")
        outputs["prediction_late_rate_5m"] = _write_df(late_rate(pred, pubs, late_seconds=late_seconds), staging,
                                                       "prediction_late_rate_5m")
    else:
        warnings.append("no committed predictions/publications")

    latency = latency_stats(pubs) if pubs is not None else []
    atomic_write_json(staging / "latency.json", {"unit": "ms", "feeds": latency})
    atomic_write_json(staging / "definitions.json", {**DEFINITIONS, "late_threshold_seconds": late_seconds})
    summary = {
        "report_id": report_id, "run_id": run_rec.run_id, "query_name": query_name,
        "batch_ids": [int(b["batch_id"]) for b in batches], "batch_count": len(batches),
        "generated_at": iso_utc(utcnow()), "outputs": outputs, "latency": latency, "warnings": warnings,
        "status": "COMPLETE" if outputs else "NO_DATA",
    }
    atomic_write_json(staging / "summary.json", summary)
    if out_dir.exists():   # uncommitted leftover of a crashed run: keep it aside, never delete
        os.rename(out_dir, out_dir.parent / f"_trash-{report_id}-{int(time.time())}")
    os.rename(staging, out_dir)
    atomic_write_json(out_dir / "_COMMITTED", {"report_id": report_id, "committed_at": iso_utc(utcnow()),
                                              "files": sorted(str(p.relative_to(out_dir)) for p in out_dir.rglob("*")
                                                              if p.is_file() and not p.name.startswith("."))})
    return summary


def run(args: argparse.Namespace) -> int:
    from transitpulse.config import load_config
    from transitpulse.processing.curated import committed_batches
    from transitpulse.processing.spark_session import build_session
    from transitpulse.runs import load_run

    try:
        cfg = load_config(args.profile, workdir=args.workdir)
        paths = WorkPaths.from_workdir(cfg.workdir)
        run_id = safe_component(args.run_id, "run_id")
    except (ValueError, OSError) as exc:
        emit({"ok": False, "action": ACTION, "error": {"code": "config", "message": str(exc), "hint": None,
                                                         "log_excerpt": None}})
        return 2
    configure("report", run_id=run_id)
    for h in logging.getLogger().handlers:
        if getattr(h, "_transitpulse", False) and isinstance(h, logging.StreamHandler):
            h.setStream(sys.stderr)
    query_name = args.query_name or cfg.spark.query_name
    try:
        run_rec = load_run(paths, run_id)
    except FileNotFoundError as exc:
        emit({"ok": False, "action": ACTION, "run_id": run_id, "error": {
            "code": "run_not_found", "message": str(exc), "hint": "create the run first", "log_excerpt": None}})
        return 3
    batches = committed_batches(paths, run_id, query_name)
    rid = report_id_for(run_id, query_name, [int(b["batch_id"]) for b in batches])
    out_dir = paths.reports / run_id / "analytics" / rid
    if (out_dir / "_COMMITTED").is_file() and not args.force:
        summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
        emit({"ok": True, "action": ACTION, "run_id": run_id, "report_id": rid, "output_dir": str(out_dir),
              "reused": True, "status": summary.get("status"), "summary": summary, "error": None})
        return 0
    if args.force and out_dir.exists():
        os.rename(out_dir, out_dir.parent / f"_trash-{rid}-{int(time.time())}")
    spark = None
    try:
        spark = build_session(cfg, None, app_name="transitpulse-report", with_kafka=False)
        summary = build_report(spark, paths, run_rec, batches, query_name, out_dir, rid)
        log_event(_log, "report", "ok", "analytics report committed", report_id=rid, batches=len(batches))
        emit({"ok": True, "action": ACTION, "run_id": run_id, "report_id": rid, "output_dir": str(out_dir),
              "reused": False, "status": summary["status"], "summary": summary, "error": None})
        return 0
    except Exception as exc:  # noqa: BLE001 - one JSON document, non-zero exit
        log_event(_log, "report", "failed", f"{type(exc).__name__}: {exc}"[:2000], level=logging.ERROR)
        emit({"ok": False, "action": ACTION, "run_id": run_id, "report_id": rid, "error": {
            "code": "report_failed", "message": f"{type(exc).__name__}: {exc}"[:2000],
            "hint": "inspect the report log", "log_excerpt": None}})
        return 1
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:  # noqa: BLE001
                pass


def main(argv: list[str] | None = None) -> int:
    p = _JsonArgParser(prog="python -m transitpulse.processing.report_job",
                       description="FR09 batch analytics over committed curated batches")
    p.add_argument("--workdir", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--profile", default=None)
    p.add_argument("--query-name", default=None)
    p.add_argument("--force", action="store_true", help="recompute even if a committed report exists")
    return run(p.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
