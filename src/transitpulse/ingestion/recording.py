"""Recording manifest of a live run's raw directory (docs/interfaces.md §4, design E §1.8, D-E11).

Every live run's raw directory ``W/data/raw/<run_id>/`` is a recording. The manifest is always rebuilt
from ``capture_index.jsonl`` (the durable, append-only list of captures and fetch events), so the
same code serves a clean stop (``complete=true``) and a crash/disk-budget finalization
(``complete=false``). It is written atomically (tmp + fsync + rename).

Items are ordered by ``(at, feed order, capture_id)`` and numbered from 1. For a live recording an
item's ``raw_path`` is relative to ``W/data/raw/<run_id>/`` (the envelope's ``raw_path`` is relative to
``W/data/raw``). ``manifest_sha256`` is the sha256 of the canonical JSON of the manifest without that
field (``contracts.ids.canonical_json``).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from transitpulse import __version__
from transitpulse.contracts.enums import FEED_ORDER
from transitpulse.contracts.envelope import iso_utc, parse_iso_utc, utcnow
from transitpulse.contracts.ids import canonical_json, sha256_hex
from transitpulse.ingestion.archive import STATIC_LINK_DIR, atomic_write_json, hard_link

log = logging.getLogger(__name__)

MANIFEST_NAME = "recording_manifest.json"
MANIFEST_VERSION = 1
MANIFEST_KIND = "transitpulse.recording"
STATIC_ZIP_NAME = "MBTA_GTFS.zip"
ATTRIBUTION = (
    "Massachusetts Bay Transportation Authority (MBTA) GTFS/GTFS-Realtime data from cdn.mbta.com, "
    "recorded by the TransitPulse course project. Check the MBTA developer terms "
    "(https://www.mbta.com/developers) before redistributing this recording."
)
FAILED_FETCH_OUTCOMES = frozenset({"http_error", "timeout", "connect_error", "too_large"})


def manifest_sha256(manifest: Mapping[str, Any]) -> str:
    body = {k: v for k, v in manifest.items() if k != "manifest_sha256"}
    return sha256_hex(canonical_json(body))


def verify_manifest_sha256(manifest: Mapping[str, Any]) -> bool:
    return manifest.get("manifest_sha256") == manifest_sha256(manifest)


def _strip_run_prefix(raw_path: str | None, run_id: str) -> str | None:
    if raw_path is None:
        return None
    prefix = f"{run_id}/"
    return raw_path[len(prefix):] if raw_path.startswith(prefix) else raw_path


def _sort_key(rec: Mapping[str, Any]) -> tuple[str, int, str, str]:
    return (
        str(rec.get("at") or ""),
        FEED_ORDER.get(str(rec.get("feed_type")), 99),
        str(rec.get("capture_id") or ""),
        str(rec.get("outcome") or ""),
    )


def build_items(index_records: Iterable[Mapping[str, Any]], run_id: str) -> list[dict[str, Any]]:
    """Manifest items from capture-index records (duplicates from restart reconciliation removed)."""
    seen_captures: set[str] = set()
    seen_events: set[tuple[str, str, str]] = set()
    picked: list[Mapping[str, Any]] = []
    for rec in index_records:
        kind = rec.get("kind")
        if kind == "capture":
            cid = rec.get("capture_id")
            if not isinstance(cid, str) or cid in seen_captures:
                continue
            seen_captures.add(cid)
            picked.append(rec)
        elif kind == "fetch_event":
            key = (str(rec.get("feed_type")), str(rec.get("at")), str(rec.get("outcome")))
            if key in seen_events:
                continue
            seen_events.add(key)
            picked.append(rec)
    picked.sort(key=_sort_key)
    items: list[dict[str, Any]] = []
    for seq, rec in enumerate(picked, start=1):
        if rec.get("kind") == "capture":
            items.append({
                "seq": seq,
                "kind": "capture",
                "feed_type": rec.get("feed_type"),
                "at": rec.get("at"),
                "capture_id": rec.get("capture_id"),
                "raw_path": _strip_run_prefix(rec.get("raw_path"), run_id),
                "raw_sha256": rec.get("raw_sha256"),
                "raw_bytes": rec.get("raw_bytes"),
                "stored_sha256": rec.get("stored_sha256"),
                "parse_status": rec.get("parse_status"),
                "http_status": rec.get("http_status"),
                "feed_timestamp": rec.get("feed_timestamp"),
                "params": {},
            })
        else:
            items.append({
                "seq": seq,
                "kind": "fetch_event",
                "feed_type": rec.get("feed_type"),
                "at": rec.get("at"),
                "capture_id": None,
                "raw_path": None,
                "raw_sha256": None,
                "raw_bytes": None,
                "stored_sha256": None,
                "parse_status": None,
                "http_status": rec.get("http_status"),
                "feed_timestamp": None,
                "params": {
                    "outcome": rec.get("outcome"),
                    "http_status": rec.get("http_status"),
                    "reason_code": rec.get("reason_code"),
                },
            })
    return items


def feed_summaries(items: Sequence[Mapping[str, Any]], source_urls: Mapping[str, str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for feed in sorted(source_urls, key=lambda f: FEED_ORDER.get(f, 99)):
        out[feed] = {"source_url": source_urls[feed], "capture_count": 0, "rejected_count": 0,
                     "not_modified_count": 0, "failed_fetch_count": 0}
    for it in items:
        feed = str(it.get("feed_type"))
        s = out.setdefault(feed, {"source_url": None, "capture_count": 0, "rejected_count": 0,
                                  "not_modified_count": 0, "failed_fetch_count": 0})
        if it["kind"] == "capture":
            s["capture_count"] += 1
            if it.get("parse_status") != "ok":
                s["rejected_count"] += 1
        elif it["kind"] == "fetch_event":
            outcome = (it.get("params") or {}).get("outcome")
            if outcome == "not_modified":
                s["not_modified_count"] += 1
            elif outcome is not None:  # timeout, http_error, connect_error, too_large, disk_budget_exceeded
                s["failed_fetch_count"] += 1
    return out


def compute_gaps(items: Sequence[Mapping[str, Any]], poll_intervals: Mapping[str, float]) -> list[dict[str, Any]]:
    """Per-feed periods longer than 2 poll intervals without a successful poll (capture ok or 304)."""
    gaps: list[dict[str, Any]] = []
    for feed, interval in sorted(poll_intervals.items(), key=lambda kv: FEED_ORDER.get(kv[0], 99)):
        limit = 2.0 * float(interval)
        prev: datetime | None = None
        failed_between = 0
        for it in items:
            if it.get("feed_type") != feed or not it.get("at"):
                continue
            at = parse_iso_utc(str(it["at"]))
            success = (it["kind"] == "capture" and it.get("parse_status") == "ok") or (
                it["kind"] == "fetch_event" and (it.get("params") or {}).get("outcome") == "not_modified")
            if not success:
                failed_between += 1
                continue
            if prev is not None:
                seconds = (at - prev).total_seconds()
                if seconds > limit:
                    gaps.append({"feed_type": feed, "start": iso_utc(prev), "end": iso_utc(at),
                                 "seconds": round(seconds, 3), "failed_or_rejected_polls": failed_between})
            prev = at
            failed_between = 0
    return gaps


def static_info(run: Any, raw_run_dir: Path, *, link_zip: bool = True) -> dict[str, Any]:
    """``static`` block: hard-links the run's immutable ZIP into ``<raw run dir>/static/`` when possible."""
    zip_path = Path(run.static_zip_path) if getattr(run, "static_zip_path", None) else None
    version: dict[str, Any] = {}
    if zip_path is not None:
        vfile = zip_path.parent / "version.json"
        try:
            with open(vfile, encoding="utf-8") as fh:
                loaded = json.load(fh)
            if isinstance(loaded, dict):
                version = loaded
        except (OSError, ValueError):
            version = {}
    relpath: str | None = None
    if link_zip and zip_path is not None and zip_path.is_file():
        if hard_link(zip_path, raw_run_dir / STATIC_LINK_DIR / STATIC_ZIP_NAME):
            relpath = f"{STATIC_LINK_DIR}/{STATIC_ZIP_NAME}"
    elif (raw_run_dir / STATIC_LINK_DIR / STATIC_ZIP_NAME).is_file():
        relpath = f"{STATIC_LINK_DIR}/{STATIC_ZIP_NAME}"
    return {
        "gtfs_version": run.gtfs_version,
        # gtfs_version is by definition the sha256 of the ZIP bytes (contracts.ids.gtfs_version_of_bytes)
        "zip_sha256": version.get("zip_sha256") or run.gtfs_version,
        "zip_relpath": relpath,
        "feed_version": version.get("feed_version"),
        "fetched_at": version.get("downloaded_at") or version.get("fetched_at"),
        "source_url": version.get("source_url"),
    }


def build_manifest(
    *,
    run: Any,
    index_records: Iterable[Mapping[str, Any]],
    source_urls: Mapping[str, str],
    poll_intervals: Mapping[str, float],
    user_agent: str,
    route_type: int,
    scope_policy_version: str,
    created_at: datetime,
    complete: bool,
    static: Mapping[str, Any],
    finalized_at: datetime | None = None,
) -> dict[str, Any]:
    """The full manifest (with ``manifest_sha256``) for ``run`` from its capture-index records."""
    items = build_items(index_records, run.run_id)
    times = [parse_iso_utc(str(it["at"])) for it in items if it.get("at")]
    first = min(times) if times else None
    last = max(times) if times else None
    intervals = {k: float(v) for k, v in poll_intervals.items()}
    scenario_ids = [run.scenario_id] if getattr(run, "scenario_id", None) else []
    manifest: dict[str, Any] = {
        "manifest_version": MANIFEST_VERSION,
        "kind": MANIFEST_KIND,
        "recording_id": getattr(run, "recording_id", None) or run.run_id,
        "source_type": run.source_type,
        "fixture_kind": getattr(run, "fixture_kind", None),
        "seed": getattr(run, "seed", None),
        "scenario_ids": scenario_ids,
        "provenance": (f"Live capture of the MBTA GTFS-Realtime JSON feeds by the TransitPulse collector "
                       f"(run {run.run_id}, source_type {run.source_type}); raw bodies are unmodified."),
        "created_at": iso_utc(created_at),
        "finalized_at": iso_utc(finalized_at or utcnow()),
        "complete": bool(complete),
        "static": dict(static),
        "scope": {
            "scope_id": run.scope_id,
            "route_ids": list(run.route_ids),
            "route_type": int(route_type),
            "policy_version": scope_policy_version,
        },
        "collector": {
            "poll_interval_seconds": min(intervals.values()) if intervals else None,
            "poll_interval_seconds_by_feed": intervals,
            "user_agent": user_agent,
            "version": __version__,
        },
        "time_range": {
            "first_fetched_at": iso_utc(first) if first else None,
            "last_fetched_at": iso_utc(last) if last else None,
            "source_duration_seconds": round((last - first).total_seconds(), 3) if first and last else 0,
        },
        "feeds": feed_summaries(items, source_urls),
        "items": items,
        "gaps": compute_gaps(items, intervals),
        "attribution": ATTRIBUTION,
    }
    manifest["manifest_sha256"] = manifest_sha256(manifest)
    return manifest


def write_manifest(raw_run_dir: Path, manifest: Mapping[str, Any]) -> Path:
    path = raw_run_dir / MANIFEST_NAME
    atomic_write_json(path, manifest, indent=2)
    return path


def read_manifest(raw_run_dir: Path) -> dict[str, Any] | None:
    path = raw_run_dir / MANIFEST_NAME
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return None
    return data if isinstance(data, dict) else None
