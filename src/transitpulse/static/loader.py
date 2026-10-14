"""Static GTFS loader: immutable ZIP store -> Spark validation/scope filter -> ``dim_*`` Parquet -> Cassandra.

Output of :func:`load_static` for a passing load (``<STATIC_ROOT>/<gtfs_version>/<scope_id>/``)::

    dim_<name>/ (every name of DIM_NAMES)   catalog_stop_times/   scope_index.json
    static_quality_report.json              manifest.json         _SUCCESS (written last)

Publication protocol: everything is written into a private staging directory next to the scope directory,
``_SUCCESS`` is written last inside it, and the staging directory is renamed onto ``<scope_id>`` in one
``os.replace`` (an exclusive ``flock`` serializes concurrent loads of the same scope). A scope directory that
is already complete is reused as-is (dimensions are immutable once a run may have pinned them). A FAILED
load publishes nothing: its report goes to ``W/reports/static/<scope_id>.static_quality_report.json``.
Cassandra static tables are written AFTER publication (idempotent upserts); the report's ``cassandra``
section records the outcome, and a re-run of ``load`` only (re)writes Cassandra when it is not ``WRITTEN``.
"""

from __future__ import annotations

import csv
import fcntl
import os
import shutil
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from transitpulse.config import ProjectConfig
from transitpulse.contracts.envelope import iso_utc
from transitpulse.contracts.ids import scope_id as compute_scope_id
from transitpulse.obs.logging import get_logger, log_event, timed
from transitpulse.runtime.paths import WorkPaths
from transitpulse.static.dims import DIM_NAMES, read_scope_manifest, scope_dir_is_complete
from transitpulse.static.fsutil import atomic_write_bytes, atomic_write_json, fsync_dir, read_json
from transitpulse.static.index import build_scope_index_doc, write_scope_index
from transitpulse.static.ingest import (
    PARSER_VERSION,
    StaticError,
    StaticVersion,
    UsageError,
    extract_gtfs,
    get_static_version,
    ingest_zip,
)
from transitpulse.static.validation import (
    STATUS_FAILED,
    Check,
    normalize_header,
    severity_counts,
    status_from_checks,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

__all__ = [
    "REPORT_FILE",
    "MANIFEST_FILE",
    "SUCCESS_FILE",
    "StaticLoadResult",
    "StaticVersion",
    "ingest_zip",
    "load_static",
    "normalize_route_ids",
    "failed_report_path",
    "read_trip_to_route",
]

REPORT_FILE = "static_quality_report.json"
MANIFEST_FILE = "manifest.json"
SUCCESS_FILE = "_SUCCESS"
REPORT_SCHEMA_VERSION = 1
SEVERITY_POLICY = (
    "static-severity-v2: FAILED only for structural errors in scope (missing required files/columns, invalid or "
    "multiple agency timezones, duplicate keys, missing foreign keys of scoped rows, rejected configured routes); "
    "per-trip/per-row data defects are warnings and set dim_trips.schedule_valid=false (no delay, no KPI)"
)

_log = get_logger("transitpulse.static.loader")


@dataclass(frozen=True)
class StaticLoadResult:
    """Outcome of :func:`load_static` (interfaces §3; extra fields have defaults)."""

    gtfs_version: str
    scope_id: str
    status: str
    agency_timezone: str | None
    verified_route_ids: tuple[str, ...]
    rejected: tuple[dict[str, Any], ...]
    scope_dir: Path
    report_path: Path
    reused: bool = False
    row_counts: dict[str, int] = field(default_factory=dict)
    cassandra: dict[str, Any] | None = None
    static_zip_path: Path | None = None

    @property
    def rejected_route_ids(self) -> tuple[str, ...]:
        return tuple(r["route_id"] for r in self.rejected)


def normalize_route_ids(route_ids: Sequence[str] | str | None) -> tuple[str, ...]:
    """Strip, drop blanks and duplicates (order kept). Accepts a comma-separated string."""
    if route_ids is None:
        return ()
    items = route_ids.split(",") if isinstance(route_ids, str) else list(route_ids)
    return tuple(dict.fromkeys(r.strip() for r in items if r and r.strip()))


def failed_report_path(paths: WorkPaths, scope_id: str) -> Path:
    return paths.reports / "static" / f"{scope_id}.{REPORT_FILE}"


def read_trip_to_route(trips_txt: Path) -> dict[str, str]:
    """``{trip_id: route_id}`` for EVERY trip of the version, streamed with the csv module (no Spark)."""
    out: dict[str, str] = {}
    with open(trips_txt, encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        header = normalize_header(next(reader, []))
        if "trip_id" not in header or "route_id" not in header:
            return out
        ti, ri = header.index("trip_id"), header.index("route_id")
        need = max(ti, ri)
        for row in reader:
            if len(row) <= need:
                continue
            trip, route = row[ti].strip(), row[ri].strip()
            if trip and route:
                out.setdefault(trip, route)
    return out


@contextmanager
def _scope_lock(version_dir: Path, scope_id: str) -> Iterator[None]:
    version_dir.mkdir(parents=True, exist_ok=True)
    lock_path = version_dir / f".{scope_id}.lock"
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _pin_spark_conf(spark: SparkSession) -> dict[str, str | None]:
    """Session time zone UTC (GTFS anchor semantics) and NULL-on-bad-cast (non-ANSI) during the load."""
    previous: dict[str, str | None] = {}
    for key, value in (("spark.sql.session.timeZone", "UTC"), ("spark.sql.ansi.enabled", "false")):
        try:
            previous[key] = spark.conf.get(key)
        except Exception:  # noqa: BLE001 - key without default on this Spark build
            previous[key] = None
        if previous[key] != value:
            log_event(_log, "static_spark_conf", "ok", f"pinning {key}={value} for the static load",
                      previous=previous[key])
        spark.conf.set(key, value)
    return previous


def _restore_spark_conf(spark: SparkSession, previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        try:
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)
        except Exception:  # noqa: BLE001 - restoring is best effort
            pass


def _report(
    *,
    version: StaticVersion,
    scope_id: str,
    requested: tuple[str, ...],
    verified: tuple[str, ...],
    rejected: tuple[dict[str, Any], ...],
    agency_timezone: str | None,
    file_stats: dict[str, dict[str, Any]],
    checks: list[Check],
    status: str,
    service_date_range: dict[str, str | None],
    outputs: dict[str, Any],
    cassandra: dict[str, Any],
    schedule_invalid_trips: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "severity_policy": SEVERITY_POLICY,
        "schedule_invalid_trips": schedule_invalid_trips,
        "schema_version": REPORT_SCHEMA_VERSION,
        "gtfs_version": version.gtfs_version,
        "zip_sha256": version.zip_sha256,
        "source_url": version.source_url,
        "downloaded_at": iso_utc(version.downloaded_at),
        "feed_version": version.feed_version,
        "parser_version": PARSER_VERSION,
        "agency_timezone": agency_timezone,
        "scope": {
            "scope_id": scope_id,
            "requested_route_ids": list(requested),
            "verified_route_ids": list(verified),
            "rejected": [dict(r) for r in rejected],
        },
        "files": [dict(v) for v in file_stats.values()],
        "service_date_range": service_date_range,
        "checks": [c.to_json() for c in checks],
        "severity_counts": severity_counts(checks),
        "status": status,
        "outputs": outputs,
        "cassandra": cassandra,
        "generated_at": iso_utc(datetime.now(UTC)),
    }


def _result_from_published(
    version: StaticVersion, scope_id: str, scope_dir: Path, *, reused: bool, cassandra: dict[str, Any] | None
) -> StaticLoadResult:
    manifest = read_scope_manifest(scope_dir)
    report = read_json(scope_dir / REPORT_FILE)
    return StaticLoadResult(
        gtfs_version=version.gtfs_version,
        scope_id=scope_id,
        status=str(manifest["status"]),
        agency_timezone=manifest.get("agency_timezone"),
        verified_route_ids=tuple(manifest.get("verified_route_ids") or ()),
        rejected=tuple(report.get("scope", {}).get("rejected") or ()),
        scope_dir=scope_dir,
        report_path=scope_dir / REPORT_FILE,
        reused=reused,
        row_counts=dict(manifest.get("row_counts") or {}),
        cassandra=cassandra if cassandra is not None else report.get("cassandra"),
        static_zip_path=version.zip_path,
    )


def _write_cassandra(
    cfg: ProjectConfig, spark: SparkSession, scope_dir: Path, gtfs_version: str, session: Any | None
) -> dict[str, Any]:
    """Write the static catalog and record the outcome in the report (raises StaticError on failure)."""
    from transitpulse.static.catalog import catalog_row_iterators
    from transitpulse.storage.static_writer import write_static_catalog

    report_path = scope_dir / REPORT_FILE
    try:
        if session is None:
            from transitpulse.storage.cassandra import get_session

            session = get_session(cfg)
        with timed(_log, "static_cassandra", gtfs_version=gtfs_version):
            rows = write_static_catalog(cfg, session, catalog_row_iterators(spark, scope_dir, gtfs_version))
        outcome = {"status": "WRITTEN", "rows": rows, "written_at": iso_utc(datetime.now(UTC)), "error": None}
    except Exception as exc:
        outcome = {"status": "FAILED", "rows": None, "written_at": None,
                   "error": f"{type(exc).__name__}: {str(exc)[:500]}"}
        _update_report_cassandra(report_path, outcome)
        raise StaticError(
            "CASSANDRA_WRITE_FAILED", f"static catalog write to Cassandra failed: {outcome['error']}",
            "start Cassandra and apply migrations, then re-run the same load command (Parquet is reused)",
        ) from exc
    _update_report_cassandra(report_path, outcome)
    return outcome


def _update_report_cassandra(report_path: Path, outcome: dict[str, Any]) -> None:
    report = read_json(report_path)
    report["cassandra"] = outcome
    atomic_write_json(report_path, report)


def load_static(
    cfg: ProjectConfig,
    paths: WorkPaths,
    gtfs_version: str,
    route_ids: Sequence[str],
    *,
    spark: SparkSession | None = None,
    write_cassandra: bool = True,
    session: Any | None = None,
    rewrite_cassandra: bool = False,
) -> StaticLoadResult:
    """Validate, scope-filter and publish one (gtfs_version, route scope); then write the Cassandra catalog.

    ``spark=None`` builds a local session without connector jars (``processing.spark_session``) and stops it
    afterwards. ``write_cassandra=False`` skips Cassandra entirely (tests, offline). Raises
    :class:`~transitpulse.static.ingest.UsageError` for an empty/oversized scope and
    :class:`~transitpulse.static.ingest.ArtifactError` when the version was never ingested.
    """
    requested = normalize_route_ids(route_ids)
    if not requested:
        raise UsageError("USAGE", "no route_ids given", "pass --route-ids A,B (verified bus route ids)")
    if len(requested) > cfg.scope.max_routes:
        raise UsageError("USAGE", f"{len(requested)} route ids exceed scope.max_routes={cfg.scope.max_routes}",
                         "select fewer routes")
    version = get_static_version(paths, gtfs_version)
    sid = compute_scope_id(requested, cfg.scope.route_type, cfg.scope.scope_policy_version, gtfs_version)
    version_dir = paths.static_version_dir(gtfs_version)
    scope_dir = paths.static_scope_dir(gtfs_version, sid)

    own_spark = False

    def _spark() -> SparkSession:
        nonlocal spark, own_spark
        if spark is None:
            from transitpulse.processing.spark_session import build_session

            spark = build_session(cfg, None, app_name="transitpulse-static", with_kafka=False)
            own_spark = True
        return spark

    try:
        with _scope_lock(version_dir, sid):
            if scope_dir_is_complete(scope_dir):
                manifest = read_scope_manifest(scope_dir)
                if manifest.get("parser_version") != PARSER_VERSION:
                    log_event(_log, "static_load", "reused",
                              "complete scope directory built by another parser version is kept (immutable)",
                              scope_dir=str(scope_dir), parser_version=manifest.get("parser_version"))
                log_event(_log, "static_load", "reused", "static scope already published", scope_id=sid,
                          gtfs_version=gtfs_version)
                cassandra = None
                if write_cassandra:
                    report = read_json(scope_dir / REPORT_FILE)
                    if rewrite_cassandra or (report.get("cassandra") or {}).get("status") != "WRITTEN":
                        cassandra = _write_cassandra(cfg, _spark(), scope_dir, gtfs_version, session)
                return _result_from_published(version, sid, scope_dir, reused=True, cassandra=cassandra)

            result = _build_and_publish(cfg, paths, version, sid, requested, scope_dir, _spark())
        if result.status == STATUS_FAILED or not write_cassandra:
            return result
        cassandra = _write_cassandra(cfg, _spark(), scope_dir, gtfs_version, session)
        return _result_from_published(version, sid, scope_dir, reused=False, cassandra=cassandra)
    finally:
        if own_spark and spark is not None:
            spark.stop()


def _build_and_publish(
    cfg: ProjectConfig,
    paths: WorkPaths,
    version: StaticVersion,
    sid: str,
    requested: tuple[str, ...],
    scope_dir: Path,
    spark: SparkSession,
) -> StaticLoadResult:
    from transitpulse.static.build import DIM_FILE_OF, build_static, out_of_scope_checks
    from transitpulse.static.catalog import CATALOG_STOP_TIMES

    gtfs_version = version.gtfs_version
    job_dir = paths.tmp / f"static-{sid}-{uuid.uuid4().hex[:12]}"
    version_dir = paths.static_version_dir(gtfs_version)
    staging = version_dir / f".staging-{sid}-{uuid.uuid4().hex[:12]}"
    previous_conf = _pin_spark_conf(spark)
    published = False
    try:
        with timed(_log, "static_extract", gtfs_version=gtfs_version):
            files = extract_gtfs(version.zip_path, job_dir / "csv")
        with timed(_log, "static_build", gtfs_version=gtfs_version, scope_id=sid):
            b = build_static(spark, files, gtfs_version=gtfs_version, route_ids=requested,
                             route_type=cfg.scope.route_type, stage_dir=job_dir / "stage")

        if b.status == STATUS_FAILED:
            report_path = failed_report_path(paths, sid)
            report = _report(
                version=version, scope_id=sid, requested=requested, verified=b.verified_route_ids,
                rejected=b.rejected, agency_timezone=b.agency_timezone, file_stats=b.file_stats, checks=b.checks,
                status=STATUS_FAILED, service_date_range={"min": None, "max": None}, outputs={},
                cassandra={"status": "SKIPPED", "rows": None, "written_at": None, "error": "load FAILED"},
            )
            atomic_write_json(report_path, report)
            log_event(_log, "static_load", "failed", "static load FAILED; nothing published",
                      scope_id=sid, report_path=str(report_path))
            return StaticLoadResult(gtfs_version, sid, STATUS_FAILED, b.agency_timezone, b.verified_route_ids,
                                    b.rejected, scope_dir, report_path, static_zip_path=version.zip_path)

        staging.mkdir(parents=True, exist_ok=False)
        row_counts: dict[str, int] = {}
        with timed(_log, "static_write_parquet", scope_id=sid):
            for name in DIM_NAMES:
                target = staging / f"dim_{name}"
                b.dims[name].write.mode("overwrite").parquet(str(target))
                row_counts[name] = spark.read.parquet(str(target)).count()
            if b.catalog_stop_times is None:
                raise StaticError("STATIC_BUILD_INCOMPLETE", "passing build without catalog_stop_times", None)
            b.catalog_stop_times.write.mode("overwrite").parquet(str(staging / CATALOG_STOP_TIMES))

        for dim, fname in DIM_FILE_OF.items():
            if fname in b.file_stats and b.file_stats[fname]["present"]:
                b.file_stats[fname]["scoped_row_count"] = row_counts.get(dim)
        checks = list(b.checks) + out_of_scope_checks(b.file_stats)
        status = status_from_checks(checks)

        from pyspark.sql import functions as F

        sd = spark.read.parquet(str(staging / "dim_service_dates")).agg(
            F.min("service_date").alias("lo"), F.max("service_date").alias("hi")).collect()[0]
        service_date_range = {
            "min": sd["lo"].strftime("%Y%m%d") if sd["lo"] is not None else None,
            "max": sd["hi"].strftime("%Y%m%d") if sd["hi"] is not None else None,
        }
        invalid_trips = spark.read.parquet(str(staging / "dim_trips")).where(~F.col("schedule_valid"))
        schedule_invalid_trips = {
            "count": invalid_trips.count(),
            "sample_trip_ids": [r["trip_id"] for r in
                                invalid_trips.select("trip_id").orderBy("trip_id").limit(10).collect()],
        }

        # scope_index.json (bounded collects from the scoped Parquet + csv streaming for trip_to_route)
        stop_rows = spark.read.parquet(str(staging / "dim_stops")).select("stop_id", "parent_station").collect()
        rs_rows = spark.read.parquet(str(staging / "dim_route_stops")).select("route_id", "stop_id").collect()
        stops_by_route: dict[str, set[str]] = {rid: set() for rid in b.verified_route_ids}
        for row in rs_rows:
            stops_by_route.setdefault(row["route_id"], set()).add(row["stop_id"])
        trip_to_route = read_trip_to_route(files["trips"].path)
        index_doc = build_scope_index_doc(
            gtfs_version=gtfs_version,
            scope_id=sid,
            route_ids=b.verified_route_ids,
            route_type_by_route={r: i["route_type"] for r, i in b.route_info.items()},
            agency_by_route={r: i["agency_id"] for r, i in b.route_info.items()},
            agency_timezone=b.agency_timezone or "",
            trip_to_route=trip_to_route,
            scope_stop_ids=[r["stop_id"] for r in stop_rows],
            stops_by_route={k: sorted(v) for k, v in stops_by_route.items()},
            parent_station={r["stop_id"]: r["parent_station"] for r in stop_rows if r["parent_station"]},
        )
        write_scope_index(staging, index_doc)

        created_at = iso_utc(datetime.now(UTC))
        parquet_paths = {f"dim_{n}": f"dim_{n}" for n in DIM_NAMES}
        parquet_paths[CATALOG_STOP_TIMES] = CATALOG_STOP_TIMES
        outputs = {"parquet_paths": parquet_paths, "scope_index": "scope_index.json", "base_dir": str(scope_dir)}
        report = _report(
            version=version, scope_id=sid, requested=requested, verified=b.verified_route_ids, rejected=b.rejected,
            agency_timezone=b.agency_timezone, file_stats=b.file_stats, checks=checks, status=status,
            service_date_range=service_date_range, outputs=outputs,
            cassandra={"status": "PENDING", "rows": None, "written_at": None, "error": None},
            schedule_invalid_trips=schedule_invalid_trips,
        )
        atomic_write_json(staging / REPORT_FILE, report)
        manifest = {
            "gtfs_version": gtfs_version,
            "scope_id": sid,
            "agency_timezone": b.agency_timezone,
            "verified_route_ids": list(b.verified_route_ids),
            "route_type": cfg.scope.route_type,
            "scope_policy_version": cfg.scope.scope_policy_version,
            "status": status,
            "row_counts": row_counts,
            "created_at": created_at,
            "parser_version": PARSER_VERSION,
        }
        atomic_write_json(staging / MANIFEST_FILE, manifest)
        atomic_write_bytes(staging / SUCCESS_FILE, (created_at + "\n").encode("utf-8"))

        if scope_dir.exists():
            # an incomplete directory left by an older/crashed writer: keep it aside, never delete data
            aside = version_dir / f".incomplete-{sid}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
            os.replace(scope_dir, aside)
            log_event(_log, "static_publish", "ok", "moved incomplete scope directory aside", aside=str(aside))
        os.replace(staging, scope_dir)
        fsync_dir(version_dir)
        published = True
        log_event(_log, "static_load", "ok", "static scope published", scope_id=sid, gtfs_version=gtfs_version,
                  status=status, row_counts=row_counts, scope_dir=str(scope_dir))
        return StaticLoadResult(gtfs_version, sid, status, b.agency_timezone, b.verified_route_ids, b.rejected,
                                scope_dir, scope_dir / REPORT_FILE, row_counts=row_counts,
                                cassandra=report["cassandra"], static_zip_path=version.zip_path)
    finally:
        _restore_spark_conf(spark, previous_conf)
        shutil.rmtree(job_dir, ignore_errors=True)          # private scratch of this job only
        if not published and staging.exists():
            shutil.rmtree(staging, ignore_errors=True)      # unpublished private staging only
