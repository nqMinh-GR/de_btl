"""``source-check``: probe the four MBTA sources once and write ``source_probe_report.json``
(spec §3.2, design E §2).

* One unconditional GET per source (no conditional headers, a single attempt, configured timeouts and
  the project User-Agent).
* A source is listed under ``summary.verified_sources`` ONLY if it answered HTTP 200 and its body was
  parsed (and, for the static ZIP, all nine required files are present and non-empty). A failed or
  skipped probe is reported as such: nothing is synthesized, an HTTP 200 is never fabricated.
* ``--offline`` makes no request and writes ``status: not_checked`` entries.
* Response bodies are kept as evidence with their sha256: realtime bodies under
  ``W/data/source_check/<checked_at>/<feed>.json.gz``; the static ZIP content-addressed under
  ``W/data/source_check/static/<sha256>.zip`` (re-runs do not duplicate it).
* With route IDs, the report includes a dry run of the collector scope filter and envelope builder
  (``in_scope_counts``, ``estimated_envelope_bytes`` against ``kafka.max_snapshot_bytes``).
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import os
import uuid
import zipfile
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import httpx

from transitpulse.config import ProjectConfig
from transitpulse.contracts.enums import FEED_ABBR
from transitpulse.contracts.envelope import envelope_size, iso_utc, utcnow
from transitpulse.contracts.ids import capture_id as make_capture_id
from transitpulse.contracts.ids import scope_id as make_scope_id
from transitpulse.contracts.ids import sha256_hex
from transitpulse.contracts.policy import classify_uncertainty
from transitpulse.ingestion.archive import RawRef, atomic_write_bytes, atomic_write_json, gzip_bytes
from transitpulse.ingestion.envelope_builder import envelope_from_capture, http_meta
from transitpulse.ingestion.feed_parse import ParsedFeed, Rejection, parse_feed
from transitpulse.ingestion.http_fetch import FeedFetcher, FetchResult, FetchSettings, header_subset, is_dns_error
from transitpulse.ingestion.scope_filter import ScopeFilter, include_unparsed_entities
from transitpulse.obs.logging import log_event
from transitpulse.runtime.paths import WorkPaths

log = logging.getLogger(__name__)

REPORT_VERSION = 1
REPORT_NAME = "source_probe_report.json"
FEEDS: tuple[str, ...] = ("vehicle_positions", "trip_updates", "alerts")
SOURCES: tuple[str, ...] = ("gtfs_static", *FEEDS)
REQUIRED_STATIC_FILES: tuple[str, ...] = (
    "agency.txt", "routes.txt", "stops.txt", "trips.txt", "stop_times.txt", "calendar.txt",
    "calendar_dates.txt", "shapes.txt", "feed_info.txt",
)
STATIC_MAX_BYTES = 1 << 30
BUS_ROUTE_TYPE = "3"
TOP_N_ROUTES = 50
PLACEHOLDER_RUN_ID = "00000000-0000-4000-8000-000000000000"

VP_FIELDS: dict[str, tuple[str, ...]] = {
    "vehicle.timestamp": ("timestamp",),
    "position.latitude": ("position", "latitude"),
    "position.longitude": ("position", "longitude"),
    "position.bearing": ("position", "bearing"),
    "position.speed": ("position", "speed"),
    "trip.trip_id": ("trip", "trip_id"),
    "trip.route_id": ("trip", "route_id"),
    "trip.direction_id": ("trip", "direction_id"),
    "trip.start_date": ("trip", "start_date"),
    "trip.start_time": ("trip", "start_time"),
    "vehicle.vehicle.id": ("vehicle", "id"),
    "stop_id": ("stop_id",),
    "current_stop_sequence": ("current_stop_sequence",),
    "current_status": ("current_status",),
    "occupancy_status": ("occupancy_status",),
}


# ======================================================================================================
# Static ZIP inspection (stdlib only, no Spark)
# ======================================================================================================

@dataclass
class StaticProbe:
    files_present: list[str]
    missing: list[str]
    empty: list[str]
    row_counts: dict[str, int]
    feed_info: dict[str, str | None]
    calendar_range: dict[str, str | None]
    calendar_dates_range: dict[str, str | None]
    agency_timezones: list[str]
    bus_routes: list[dict[str, str | None]]
    route_type_by_route: dict[str, str]
    agency_by_route: dict[str, str]
    trip_to_route: dict[str, str]
    parent_station: dict[str, str]
    scope_stop_ids: set[str] = field(default_factory=set)

    @property
    def complete(self) -> bool:
        return not self.missing and not self.empty

    def report(self) -> dict[str, Any]:
        return {
            "files_present": self.files_present,
            "missing": self.missing,
            "empty": self.empty,
            "row_counts": self.row_counts,
            "feed_info": self.feed_info,
            "feed_version": self.feed_info.get("feed_version"),
            "calendar_range": self.calendar_range,
            "calendar_dates_range": self.calendar_dates_range,
            "agency_timezones": self.agency_timezones,
            "bus_route_count": len(self.bus_routes),
            "bus_routes": self.bus_routes,
        }


def _member_names(zf: zipfile.ZipFile) -> dict[str, str]:
    """basename -> member name, preferring root-level members (GTFS files belong at the root)."""
    out: dict[str, str] = {}
    for name in zf.namelist():
        if name.endswith("/"):
            continue
        base = PurePosixPath(name).name
        if base not in out or "/" not in name:
            out[base] = name
    return out


def _text(zf: zipfile.ZipFile, member: str) -> io.TextIOWrapper:
    return io.TextIOWrapper(zf.open(member), encoding="utf-8-sig", newline="")


def _dict_rows(zf: zipfile.ZipFile, member: str) -> Iterator[dict[str, str]]:
    with _text(zf, member) as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None:
            return
        cols = [h.strip() for h in header]
        for row in reader:
            if not row:
                continue
            yield {cols[i]: (row[i].strip() if i < len(row) else "") for i in range(len(cols))}


def _min_max(values: Iterable[str]) -> dict[str, str | None]:
    vals = [v for v in values if v]
    return {"start": min(vals) if vals else None, "end": max(vals) if vals else None}


def inspect_static_zip(path: Path, route_ids: Sequence[str] = ()) -> StaticProbe:
    """Inspect a GTFS ZIP: required files, row counts, versions, date ranges, bus routes, trip->route."""
    scope = set(route_ids)
    with zipfile.ZipFile(path) as zf:
        members = _member_names(zf)
        present = [f for f in REQUIRED_STATIC_FILES if f in members]
        missing = [f for f in REQUIRED_STATIC_FILES if f not in members]
        row_counts: dict[str, int] = {}
        feed_info: dict[str, str | None] = {"feed_version": None, "feed_start_date": None, "feed_end_date": None,
                                            "feed_publisher_name": None}
        calendar_range: dict[str, str | None] = {"start": None, "end": None}
        calendar_dates_range: dict[str, str | None] = {"start": None, "end": None}
        tzs: set[str] = set()
        bus_routes: list[dict[str, str | None]] = []
        route_type_by_route: dict[str, str] = {}
        agency_by_route: dict[str, str] = {}
        trip_to_route: dict[str, str] = {}
        parent_station: dict[str, str] = {}
        scope_trips: set[str] = set()
        scope_stops: set[str] = set()

        if "agency.txt" in members:
            rows = list(_dict_rows(zf, members["agency.txt"]))
            row_counts["agency.txt"] = len(rows)
            tzs = {r.get("agency_timezone", "") for r in rows if r.get("agency_timezone")}
        if "feed_info.txt" in members:
            rows = list(_dict_rows(zf, members["feed_info.txt"]))
            row_counts["feed_info.txt"] = len(rows)
            if rows:
                for k in feed_info:
                    feed_info[k] = rows[0].get(k) or None
        if "routes.txt" in members:
            n = 0
            for r in _dict_rows(zf, members["routes.txt"]):
                n += 1
                rid = r.get("route_id", "")
                if not rid:
                    continue
                route_type_by_route[rid] = r.get("route_type", "")
                agency_by_route[rid] = r.get("agency_id", "")
                if r.get("route_type", "") == BUS_ROUTE_TYPE:
                    bus_routes.append({"route_id": rid, "short_name": r.get("route_short_name") or None,
                                       "long_name": r.get("route_long_name") or None,
                                       "agency_id": r.get("agency_id") or None})
            row_counts["routes.txt"] = n
        if "trips.txt" in members:
            n = 0
            for r in _dict_rows(zf, members["trips.txt"]):
                n += 1
                tid, rid = r.get("trip_id", ""), r.get("route_id", "")
                if tid and rid:
                    trip_to_route[tid] = rid
                    if rid in scope:
                        scope_trips.add(tid)
            row_counts["trips.txt"] = n
        if "stops.txt" in members:
            n = 0
            for r in _dict_rows(zf, members["stops.txt"]):
                n += 1
                sid, parent = r.get("stop_id", ""), r.get("parent_station", "")
                if sid and parent:
                    parent_station[sid] = parent
            row_counts["stops.txt"] = n
        if "stop_times.txt" in members:
            n = 0
            with _text(zf, members["stop_times.txt"]) as fh:
                reader = csv.reader(fh)
                header = [h.strip() for h in (next(reader, None) or [])]
                i_trip = header.index("trip_id") if "trip_id" in header else -1
                i_stop = header.index("stop_id") if "stop_id" in header else -1
                for row in reader:
                    if not row:
                        continue
                    n += 1
                    if scope_trips and 0 <= i_trip < len(row) and 0 <= i_stop < len(row):
                        if row[i_trip].strip() in scope_trips:
                            scope_stops.add(row[i_stop].strip())
            row_counts["stop_times.txt"] = n
        if "calendar.txt" in members:
            rows = list(_dict_rows(zf, members["calendar.txt"]))
            row_counts["calendar.txt"] = len(rows)
            starts = _min_max(r.get("start_date", "") for r in rows)
            ends = _min_max(r.get("end_date", "") for r in rows)
            calendar_range = {"start": starts["start"], "end": ends["end"]}
        if "calendar_dates.txt" in members:
            rows = list(_dict_rows(zf, members["calendar_dates.txt"]))
            row_counts["calendar_dates.txt"] = len(rows)
            calendar_dates_range = _min_max(r.get("date", "") for r in rows)
        for name in REQUIRED_STATIC_FILES:
            if name in members and name not in row_counts:
                n = 0
                with _text(zf, members[name]) as fh:
                    reader = csv.reader(fh)
                    if next(reader, None) is not None:
                        n = sum(1 for row in reader if row)
                row_counts[name] = n
    for stop in list(scope_stops):
        parent = parent_station.get(stop)
        if parent:
            scope_stops.add(parent)
    empty = [f for f in present if row_counts.get(f, 0) == 0]
    bus_routes.sort(key=lambda r: str(r["route_id"]))
    return StaticProbe(
        files_present=present, missing=missing, empty=empty, row_counts=row_counts, feed_info=feed_info,
        calendar_range=calendar_range, calendar_dates_range=calendar_dates_range, agency_timezones=sorted(tzs),
        bus_routes=bus_routes, route_type_by_route=route_type_by_route, agency_by_route=agency_by_route,
        trip_to_route=trip_to_route, parent_station=parent_station, scope_stop_ids=scope_stops,
    )


# ======================================================================================================
# Realtime analysis
# ======================================================================================================

def _get(obj: Any, path: Sequence[str]) -> Any:
    for key in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


def _top(counter: Counter[str], n: int) -> dict[str, int]:
    return {k: v for k, v in sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))[:n]}


def analyze_vehicle_positions(entities: Sequence[dict[str, Any]]) -> dict[str, Any]:
    vehicles = [e.get("vehicle") for e in entities]
    missing = {name: sum(1 for v in vehicles if _get(v, path) is None) for name, path in VP_FIELDS.items()}
    return {
        "entities_without_vehicle": sum(1 for v in vehicles if not isinstance(v, dict)),
        "missing_fields": missing,
        "current_status_distribution": _top(Counter(str(_get(v, ("current_status",)) or "absent") for v in vehicles), 20),
    }


def analyze_trip_updates(entities: Sequence[dict[str, Any]]) -> dict[str, Any]:
    tus = [e.get("trip_update") for e in entities]
    sched = Counter(str(_get(t, ("trip", "schedule_relationship")) or "absent") for t in tus)
    stu_total = miss_seq = miss_stop = miss_arr = miss_dep = events = delay_present = absent = 0
    values: Counter[str] = Counter()
    classes: Counter[str] = Counter()
    stu_sched: Counter[str] = Counter()
    for t in tus:
        stus = t.get("stop_time_update") if isinstance(t, dict) else None
        for stu in stus if isinstance(stus, list) else []:
            if not isinstance(stu, dict):
                continue
            stu_total += 1
            miss_seq += stu.get("stop_sequence") is None
            miss_stop += stu.get("stop_id") is None
            miss_arr += _get(stu, ("arrival", "time")) is None
            miss_dep += _get(stu, ("departure", "time")) is None
            stu_sched[str(stu.get("schedule_relationship") or "absent")] += 1
            for kind in ("arrival", "departure"):
                ev = stu.get(kind)
                if not isinstance(ev, dict):
                    continue
                events += 1
                delay_present += ev.get("delay") is not None
                unc = ev.get("uncertainty")
                if unc is None:
                    absent += 1          # reported separately, never as 0
                else:
                    values[str(unc)] += 1
                    classes[str(classify_uncertainty(unc))] += 1
    return {
        "trip_update_timestamp_missing": sum(1 for t in tus if _get(t, ("timestamp",)) is None),
        "schedule_relationship_distribution": dict(sched),
        "stop_time_update_total": stu_total,
        "stop_time_update_schedule_relationship_distribution": dict(stu_sched),
        "missing_stop_sequence": miss_seq,
        "missing_stop_id": miss_stop,
        "missing_arrival_time": miss_arr,
        "missing_departure_time": miss_dep,
        "event_count": events,
        "uncertainty_distribution": {"absent": absent, "values": _top(values, 20), "classes": dict(classes)},
        "delay_present_count": delay_present,
    }


def analyze_alerts(entities: Sequence[dict[str, Any]]) -> dict[str, Any]:
    kinds: Counter[str] = Counter()
    severity: Counter[str] = Counter()
    effect: Counter[str] = Counter()
    with_period = without_period = periods = 0
    for e in entities:
        alert = e.get("alert")
        if not isinstance(alert, dict):
            continue
        for sel in alert.get("informed_entity") or []:
            if isinstance(sel, dict):
                kinds["+".join(sorted(k for k, v in sel.items() if v is not None)) or "empty"] += 1
        ap = alert.get("active_period")
        if isinstance(ap, list) and ap:
            with_period += 1
            periods += len(ap)
        else:
            without_period += 1
        severity[str(alert.get("severity_level") or "absent")] += 1
        effect[str(alert.get("effect") or "absent")] += 1
    return {
        "selector_kind_distribution": _top(kinds, 50),
        "alerts_with_active_period": with_period,
        "alerts_without_active_period": without_period,
        "active_period_total": periods,
        "severity_level_distribution": dict(severity),
        "effect_distribution": _top(effect, 30),
    }


def route_id_distribution(feed: str, entities: Sequence[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    if feed == "alerts":
        for e in entities:
            for sel in _get(e, ("alert", "informed_entity")) or []:
                if isinstance(sel, dict) and sel.get("route_id") is not None:
                    counter[str(sel["route_id"])] += 1
    else:
        key = "vehicle" if feed == "vehicle_positions" else "trip_update"
        for e in entities:
            counter[str(_get(e, (key, "trip", "route_id")) or "<absent>")] += 1
    return _top(counter, TOP_N_ROUTES)


# ======================================================================================================
# Probing
# ======================================================================================================

@dataclass
class _RealtimeProbe:
    entry: dict[str, Any]
    parsed: ParsedFeed | None = None
    body: bytes | None = None
    result: FetchResult | None = None


@dataclass(frozen=True)
class _ProbeIndex:
    """Minimal scope index for the dry run (same attributes the collector's ScopeFilter reads)."""

    route_ids: frozenset[str]
    route_type_by_route: dict[str, int]
    agency_ids: frozenset[str]
    trip_to_route: Mapping[str, str]
    scope_stop_ids: frozenset[str]
    parent_station: Mapping[str, str]


@dataclass(frozen=True)
class _ProbeRun:
    run_id: str
    scope_id: str
    source_type: str
    gtfs_version: str


def _error_kind(res: FetchResult) -> str:
    return {"dns": "dns", "connect": "connect", "timeout": "timeout", "http": "http", "too_large": "http",
            "decode": "http"}.get(res.error_kind or "", "connect")


def _not_checked(url: str, checked_at: datetime, why: str) -> dict[str, Any]:
    return {"url": url, "checked_at": iso_utc(checked_at), "status": "not_checked", "http_status": None,
            "content_type": None, "content_encoding": None, "bytes": None, "elapsed_ms": None, "has_etag": None,
            "has_last_modified": None, "parse_ok": None, "error": None, "not_checked_reason": why}


def _save_evidence(evidence_dir: Path, name: str, body: bytes) -> dict[str, Any]:
    gz = gzip_bytes(body)
    path = evidence_dir / name
    atomic_write_bytes(path, gz)
    return {"path": str(path), "sha256": sha256_hex(body), "stored_sha256": sha256_hex(gz), "bytes": len(body)}


def probe_realtime(cfg: ProjectConfig, feed: str, evidence_dir: Path, checked_at: datetime,
                   transport: httpx.BaseTransport | None = None) -> _RealtimeProbe:
    url = cfg.sources.realtime_url(feed)
    settings = FetchSettings.from_config(cfg.collector.http, cfg.sources.user_agent, max_attempts=1)
    with FeedFetcher(feed, url, settings, transport=transport) as fetcher:
        res = fetcher.fetch()
    entry: dict[str, Any] = {
        "url": url,
        "checked_at": iso_utc(checked_at),
        "fetched_at": iso_utc(res.fetched_at),
        "status": "failed",
        "http_status": res.http_status,
        "content_type": res.headers.get("content-type"),
        "content_encoding": res.headers.get("content-encoding"),
        "bytes": res.body_bytes if res.body is not None else None,
        "elapsed_ms": round(res.elapsed_ms, 3),
        "has_etag": "etag" in res.headers,
        "has_last_modified": "last-modified" in res.headers,
        "parse_ok": False,
        "error": None,
    }
    if res.outcome != "ok" or res.body is None:
        entry["error"] = {"kind": _error_kind(res), "message": res.error or res.outcome}
        return _RealtimeProbe(entry=entry, result=res)
    entry["evidence"] = _save_evidence(evidence_dir, f"{feed}.json.gz", res.body)
    parsed = parse_feed(res.body, res.fetched_at)
    if isinstance(parsed, Rejection):
        entry["error"] = {"kind": "parse", "message": f"{parsed.reason_code}: {parsed.detail}"}
        entry["parse_reason"] = parsed.reason_code
        return _RealtimeProbe(entry=entry, body=res.body, result=res)
    header_dt = datetime.fromtimestamp(parsed.feed_timestamp, UTC)
    entry.update(
        status="ok",
        parse_ok=True,
        header={"timestamp": parsed.feed_timestamp, "timestamp_iso": iso_utc(header_dt),
                "gtfs_realtime_version": parsed.header.get("gtfs_realtime_version"),
                "incrementality": parsed.header.get("incrementality")},
        header_age_seconds=round((res.fetched_at - header_dt).total_seconds(), 3),
        gtfs_realtime_version=parsed.header.get("gtfs_realtime_version"),
        incrementality=parsed.header.get("incrementality"),
        incrementality_effective="FULL_DATASET",   # absent means FULL_DATASET (GTFS-RT default)
        entity_count=parsed.source_entity_count,
        parse_warnings=dict(parsed.warnings),
        route_id_distribution=route_id_distribution(feed, parsed.entities),
    )
    if feed == "vehicle_positions":
        entry["analysis"] = analyze_vehicle_positions(parsed.entities)
    elif feed == "trip_updates":
        entry["analysis"] = analyze_trip_updates(parsed.entities)
    else:
        entry["analysis"] = analyze_alerts(parsed.entities)
    return _RealtimeProbe(entry=entry, parsed=parsed, body=res.body, result=res)


def probe_static(cfg: ProjectConfig, paths: WorkPaths, checked_at: datetime, route_ids: Sequence[str],
                 transport: httpx.BaseTransport | None = None) -> tuple[dict[str, Any], StaticProbe | None]:
    url = cfg.sources.gtfs_static_url
    settings = FetchSettings.from_config(cfg.collector.http, cfg.sources.user_agent, max_attempts=1)
    entry: dict[str, Any] = {"url": url, "checked_at": iso_utc(checked_at), "status": "failed", "http_status": None,
                             "content_type": None, "content_encoding": None, "bytes": None, "elapsed_ms": None,
                             "has_etag": None, "has_last_modified": None, "parse_ok": False, "error": None}
    static_dir = paths.root / "data" / "source_check" / "static"
    static_dir.mkdir(parents=True, exist_ok=True)
    tmp = static_dir / f".download.tmp-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    digest = hashlib.sha256()
    size = 0
    t0 = datetime.now(UTC)
    client = httpx.Client(timeout=settings.timeout(), headers={"User-Agent": settings.user_agent},
                          follow_redirects=False, transport=transport)
    try:
        with client.stream("GET", url) as resp:
            hdrs = header_subset(resp.headers)
            entry.update(http_status=resp.status_code, content_type=hdrs.get("content-type"),
                         content_encoding=hdrs.get("content-encoding"), has_etag="etag" in hdrs,
                         has_last_modified="last-modified" in hdrs)
            if resp.status_code != 200:
                entry["error"] = {"kind": "http", "message": f"HTTP {resp.status_code}"}
                return entry, None
            with open(tmp, "wb") as fh:
                for chunk in resp.iter_bytes(1 << 20):
                    size += len(chunk)
                    if size > STATIC_MAX_BYTES:
                        entry["error"] = {"kind": "http", "message": f"static ZIP exceeds {STATIC_MAX_BYTES} bytes"}
                        return entry, None
                    digest.update(chunk)
                    fh.write(chunk)
                fh.flush()
                os.fsync(fh.fileno())
    except httpx.TimeoutException as exc:
        entry["error"] = {"kind": "timeout", "message": f"{type(exc).__name__}: {exc}"}
        return entry, None
    except (httpx.HTTPError, httpx.InvalidURL, OSError) as exc:
        entry["error"] = {"kind": "dns" if is_dns_error(exc) else "connect", "message": f"{type(exc).__name__}: {exc}"}
        return entry, None
    finally:
        client.close()
        entry["elapsed_ms"] = round((datetime.now(UTC) - t0).total_seconds() * 1000.0, 3)
        if entry.get("error") is not None and tmp.exists():
            tmp.unlink()   # incomplete temporary download, never evidence
    sha = digest.hexdigest()
    final = static_dir / f"{sha}.zip"
    if final.exists():
        tmp.unlink()
    else:
        os.replace(tmp, final)
    entry.update(bytes=size, zip_sha256=sha, evidence={"path": str(final), "sha256": sha, "bytes": size})
    try:
        probe = inspect_static_zip(final, route_ids)
    except (zipfile.BadZipFile, OSError, ValueError, csv.Error, UnicodeDecodeError) as exc:
        entry["error"] = {"kind": "parse", "message": f"{type(exc).__name__}: {exc}"}
        return entry, None
    entry.update(probe.report())
    entry["parse_ok"] = True
    if probe.complete:
        entry["status"] = "ok"
    else:
        entry["error"] = {"kind": "parse", "message": f"required GTFS files missing={probe.missing} empty={probe.empty}"}
    return entry, probe


def _scope_dry_run(cfg: ProjectConfig, route_ids: Sequence[str], probe: StaticProbe | None, zip_sha: str | None,
                   realtime: Mapping[str, _RealtimeProbe]) -> dict[str, Any]:
    out: dict[str, Any] = {"route_ids": list(route_ids), "static_available": probe is not None,
                           "max_snapshot_bytes": int(cfg.kafka.max_snapshot_bytes)}
    if not route_ids:
        out["note"] = "no route IDs given (--route-ids or scope.route_ids); scope dry run skipped"
        return out
    if probe is not None:
        out["unknown_route_ids"] = sorted(r for r in route_ids if r not in probe.route_type_by_route)
        out["non_bus_route_ids"] = sorted(r for r in route_ids if r in probe.route_type_by_route
                                          and probe.route_type_by_route[r] != BUS_ROUTE_TYPE)
        types = {r: int(probe.route_type_by_route[r]) for r in route_ids
                 if probe.route_type_by_route.get(r, "").isdigit()}
        agencies = frozenset(probe.agency_by_route[r] for r in route_ids if probe.agency_by_route.get(r))
        # Same shape as the static loader's scope_index.json: parent_station only for scope stops, so the
        # dry run keeps exactly what the collector would keep (unknown stops are kept and flagged).
        scoped_parents = {s: p for s, p in probe.parent_station.items() if s in probe.scope_stop_ids}
        index = _ProbeIndex(route_ids=frozenset(route_ids), route_type_by_route=types, agency_ids=agencies,
                            trip_to_route=probe.trip_to_route, scope_stop_ids=frozenset(probe.scope_stop_ids),
                            parent_station=scoped_parents)
        out["scope_stop_count"] = len(probe.scope_stop_ids)
    else:
        out["note"] = ("static ZIP unavailable: only descriptor route_id matching is possible (no trip lookup, "
                       "no agency/stop selectors)")
        index = _ProbeIndex(route_ids=frozenset(route_ids), route_type_by_route={r: 3 for r in route_ids},
                            agency_ids=frozenset(), trip_to_route={}, scope_stop_ids=frozenset(), parent_station={})
    gtfs_version = zip_sha or "unknown"
    run = _ProbeRun(run_id=PLACEHOLDER_RUN_ID,
                    scope_id=make_scope_id(route_ids, cfg.scope.route_type, cfg.scope.scope_policy_version, gtfs_version),
                    source_type="live", gtfs_version=gtfs_version)
    scope_filter = ScopeFilter(index)
    counts: dict[str, Any] = {}
    sizes: dict[str, Any] = {}
    for feed, rp in realtime.items():
        if rp.parsed is None or rp.body is None or rp.result is None:
            counts[feed] = None
            sizes[feed] = None
            continue
        selected, accounting = scope_filter.select(feed, rp.parsed.entities)
        accounting = include_unparsed_entities(accounting, rp.parsed.source_entity_count)
        raw_sha = sha256_hex(rp.body)
        cid = make_capture_id(FEED_ABBR[feed], rp.result.fetched_at, raw_sha)
        date = rp.result.fetched_at.astimezone(UTC).strftime("%Y-%m-%d")
        raw_ref = RawRef(raw_path=f"{PLACEHOLDER_RUN_ID}/{feed}/{date}/{cid}.json.gz", raw_sha256=raw_sha,
                         raw_bytes=len(rp.body), stored_sha256="0" * 64, stored_bytes=0)
        env = envelope_from_capture(run=run, feed_type=feed, parsed=rp.parsed, selected=selected,
                                    accounting=accounting, raw_ref=raw_ref, capture_id=cid,
                                    fetched_at=rp.result.fetched_at, source_url=cfg.sources.realtime_url(feed),
                                    http=dict(http_meta(rp.result.http_status, dict(rp.result.headers))),
                                    scenario_id=None, component="source-check")
        size = envelope_size(env)
        counts[feed] = accounting
        sizes[feed] = {"bytes": size, "fits_limit": size <= int(cfg.kafka.max_snapshot_bytes),
                       "selected_entity_count": len(selected)}
    out["in_scope_counts"] = counts
    out["estimated_envelope_bytes"] = sizes
    return out


def _active_bus_routes(probe: StaticProbe | None, vp: _RealtimeProbe | None) -> list[dict[str, Any]] | None:
    if probe is None or vp is None or vp.parsed is None:
        return None
    bus = {r["route_id"]: r for r in probe.bus_routes}
    counts: Counter[str] = Counter()
    for e in vp.parsed.entities:
        trip = _get(e, ("vehicle", "trip"))
        rid = _get(trip, ("route_id",))
        if not rid:
            tid = _get(trip, ("trip_id",))
            rid = probe.trip_to_route.get(tid) if isinstance(tid, str) else None
        if isinstance(rid, str) and rid in bus:
            counts[rid] += 1
    return [{"route_id": rid, "short_name": bus[rid]["short_name"], "long_name": bus[rid]["long_name"],
             "vehicle_count": n} for rid, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]


def run_source_check(
    cfg: ProjectConfig,
    paths: WorkPaths,
    *,
    route_ids: Sequence[str] | None = None,
    offline: bool = False,
    out: Path | None = None,
    transport: httpx.BaseTransport | None = None,
    checked_at: datetime | None = None,
) -> tuple[dict[str, Any], Path]:
    """Probe the sources (unless offline), write the report atomically, return ``(report, path)``."""
    checked = checked_at or utcnow()
    rids = [r for r in (route_ids if route_ids is not None else cfg.scope.route_ids) if r]
    evidence_dir = paths.root / "data" / "source_check" / checked.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    sources: dict[str, Any] = {}
    realtime: dict[str, _RealtimeProbe] = {}
    probe: StaticProbe | None = None
    zip_sha: str | None = None
    if offline:
        why = "offline mode (--offline): no request was made"
        sources["gtfs_static"] = _not_checked(cfg.sources.gtfs_static_url, checked, why)
        for feed in FEEDS:
            sources[feed] = _not_checked(cfg.sources.realtime_url(feed), checked, why)
    else:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        static_entry, probe = probe_static(cfg, paths, checked, rids, transport)
        sources["gtfs_static"] = static_entry
        zip_sha = static_entry.get("zip_sha256")
        for feed in FEEDS:
            realtime[feed] = probe_realtime(cfg, feed, evidence_dir, checked, transport)
            sources[feed] = realtime[feed].entry
        if probe is not None:
            sources["gtfs_static"]["realtime_active_bus_routes"] = _active_bus_routes(probe, realtime.get(
                "vehicle_positions"))
    verified = [s for s in SOURCES if sources[s]["status"] == "ok"]
    failed = [s for s in SOURCES if sources[s]["status"] == "failed"]
    not_checked = [s for s in SOURCES if sources[s]["status"] == "not_checked"]
    report: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "kind": "transitpulse.source_probe",
        "checked_at": iso_utc(checked),
        "offline": offline,
        "user_agent": cfg.sources.user_agent,
        "route_ids": rids,
        "sources": sources,
        "scope": _scope_dry_run(cfg, rids, probe, zip_sha, realtime) if not offline else
        {"route_ids": rids, "note": "offline mode: no scope dry run"},
        "summary": {"all_ok": len(verified) == len(SOURCES), "verified_sources": verified,
                    "failed_sources": failed, "not_checked_sources": not_checked},
        "evidence_dir": None if offline else str(evidence_dir),
        "notes": [
            "A source is verified only after a real HTTP 200 whose body parsed; nothing here is synthesized.",
            "header_age_seconds = local fetched_at - header.timestamp (includes any local clock skew).",
            "estimated_envelope_bytes is a dry run of the collector scope filter + envelope with placeholder ids.",
            "Polling/staleness thresholds are project policy, not MBTA commitments.",
        ],
    }
    path = out or (paths.reports / REPORT_NAME)
    atomic_write_json(path, report, indent=2)
    log_event(log, "source_check", "ok" if report["summary"]["all_ok"] else "failed",
              f"source check: verified={verified} failed={failed} not_checked={not_checked}",
              level=logging.INFO if report["summary"]["all_ok"] else logging.WARNING, report=str(path))
    return report, path
