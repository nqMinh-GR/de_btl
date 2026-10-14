"""Write generated recordings in the shared recording layout (interfaces §4, D-E15).

Layout (same as ``data/raw/<run_id>``)::

    <recordings_root>/<recording_id>/
        static/MBTA_GTFS.zip
        <feed_type>/<YYYY-MM-DD>/<capture_id>.json.gz      (raw_path is relative to the recording dir)
        labels.json                                         ({label: {capture_id, raw_sha256, ...}})
        recording_manifest.json                             (written last)

Determinism: no wall-clock value enters the output (``created_at``/``finalized_at`` are the synthetic
start clock), sequence numbers follow the plan order, raw bodies are compact JSON and gzip uses
``mtime=0``; the same inputs therefore give the same ``manifest_sha256``.

Idempotency: ``recording_id`` is derived from the generation inputs. When the directory already holds a
self-consistent manifest generated from the same inputs it is reused untouched; a directory with
different content is a conflict (never overwritten, never deleted). The recording is assembled in a
temporary directory and renamed into place.
"""

from __future__ import annotations

import json
import os
import shutil
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from transitpulse.contracts.enums import FEED_ABBR, FEED_ORDER
from transitpulse.contracts.envelope import iso_utc, parse_iso_utc
from transitpulse.contracts.ids import canonical_json, capture_id, scope_id, sha256_hex
from transitpulse.ingestion.feed_parse import Rejection, parse_feed
from transitpulse.replay.fileio import atomic_write_bytes, atomic_write_json, gzip_deterministic, manifest_sha256
from transitpulse.replay.recording_verify import MANIFEST_KIND, MANIFEST_VERSION
from transitpulse.runtime.paths import safe_component

GENERATOR_NAME = "transitpulse.replay.synthetic"
GENERATOR_VERSION = "synth-v1"
MANIFEST_NAME = "recording_manifest.json"
LABELS_NAME = "labels.json"
STATIC_RELPATH = "static/MBTA_GTFS.zip"
PARSE_OK = "ok"


class RecordingConflict(RuntimeError):
    """The target recording directory exists with different content (CLI exit code 4)."""


def doc_bytes(doc: Mapping[str, Any]) -> bytes:
    """Raw body of a generated feed document: compact UTF-8 JSON, insertion order kept."""
    return json.dumps(doc, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def classify_body(body: bytes, fetched_at: datetime) -> tuple[str, int | None]:
    """(parse_status, header timestamp) from the collector's own parser (single source of truth, D-E06).

    The manifest value is informative; the publisher re-parses every capture with the same parser.
    """
    result = parse_feed(body, fetched_at)
    if isinstance(result, Rejection):
        return result.reason_code, None
    return PARSE_OK, result.feed_timestamp


@dataclass
class PlannedItem:
    """One item of a generated timeline before sequence numbers and raw files are assigned."""

    kind: str
    at: datetime
    feed_type: str | None = None
    doc: dict[str, Any] | None = None        # capture: JSON document (serialized at write time)
    body: bytes | None = None                # capture: raw bytes override (e.g. malformed JSON)
    parse_status: str | None = None          # capture: None -> classified from the body
    http_status: int | None = 200
    params: dict[str, Any] = field(default_factory=dict)
    label: str | None = None
    key: str | None = None                   # unique key of a capture (republish references)
    ref: str | None = None                   # republish: key of the referenced capture

    def raw_body(self) -> bytes:
        if self.body is not None:
            return self.body
        if self.doc is None:
            raise ValueError("capture item without doc or body")
        return doc_bytes(self.doc)


@dataclass(frozen=True)
class RecordingSpec:
    recording_id: str
    fixture_kind: str
    seed: int | None
    scenario_ids: tuple[str, ...]
    provenance: str
    attribution: str
    static_zip: bytes
    static_feed_version: str | None
    static_source_url: str
    route_ids: tuple[str, ...]
    route_type: int
    scope_policy_version: str
    poll_interval_seconds: float | None
    feed_source_urls: Mapping[str, str]
    start_clock: datetime
    generator: Mapping[str, Any]             # must contain inputs_sha256
    gaps: tuple[Mapping[str, Any], ...] = ()
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RecordingResult:
    recording_id: str
    recording_dir: Path
    manifest_path: Path
    static_zip_path: Path
    route_ids: tuple[str, ...]
    start_clock: datetime
    fixture_kind: str | None
    gtfs_version: str
    scope_id: str
    manifest_sha256: str
    item_count: int
    capture_count: int
    reused: bool
    labels_path: Path | None

    def to_json(self) -> dict[str, Any]:
        return {
            "recording_id": self.recording_id,
            "recording_dir": str(self.recording_dir),
            "manifest_path": str(self.manifest_path),
            "static_zip_path": str(self.static_zip_path),
            "route_ids": list(self.route_ids),
            "start_clock": iso_utc(self.start_clock),
            "source_type": "synthetic",
            "fixture_kind": self.fixture_kind,
            "gtfs_version": self.gtfs_version,
            "scope_id": self.scope_id,
            "manifest_sha256": self.manifest_sha256,
            "item_count": self.item_count,
            "capture_count": self.capture_count,
            "reused": self.reused,
            "labels_path": str(self.labels_path) if self.labels_path else None,
        }


def inputs_sha256(inputs: Mapping[str, Any]) -> str:
    return sha256_hex(canonical_json(dict(inputs)))


def _result_from_manifest(rec_dir: Path, manifest: Mapping[str, Any], *, reused: bool) -> RecordingResult:
    static = manifest.get("static") or {}
    scope = manifest.get("scope") or {}
    items = manifest.get("items") or []
    labels = rec_dir / LABELS_NAME
    start_raw = manifest.get("start_clock") or (manifest.get("time_range") or {}).get("first_fetched_at")
    return RecordingResult(
        recording_id=str(manifest["recording_id"]),
        recording_dir=rec_dir,
        manifest_path=rec_dir / MANIFEST_NAME,
        static_zip_path=rec_dir / str(static.get("zip_relpath") or STATIC_RELPATH),
        route_ids=tuple(scope.get("route_ids") or ()),
        start_clock=parse_iso_utc(start_raw),
        fixture_kind=manifest.get("fixture_kind"),
        gtfs_version=str(static.get("gtfs_version")),
        scope_id=str(scope.get("scope_id")),
        manifest_sha256=str(manifest.get("manifest_sha256")),
        item_count=len(items),
        capture_count=sum(1 for it in items if it.get("kind") == "capture"),
        reused=reused,
        labels_path=labels if labels.is_file() else None,
    )


def find_reusable(recordings_root: Path, recording_id: str, inputs_hash: str) -> RecordingResult | None:
    """Existing recording generated from the same inputs (and intact), else None.

    Raises :class:`RecordingConflict` when the directory exists but holds something else.
    """
    rec_dir = Path(recordings_root) / safe_component(recording_id, "recording_id")
    manifest_file = rec_dir / MANIFEST_NAME
    if not rec_dir.exists():
        return None
    if not manifest_file.is_file():
        raise RecordingConflict(
            f"{rec_dir} exists but has no {MANIFEST_NAME}; it was not produced by the generator. Move it aside "
            "(nothing is deleted automatically) and run synth-generate again."
        )
    try:
        with open(manifest_file, encoding="utf-8") as fh:
            manifest = json.load(fh)
    except (OSError, ValueError) as exc:
        raise RecordingConflict(f"{manifest_file} is unreadable ({exc}); move the directory aside") from exc
    if manifest.get("manifest_sha256") != manifest_sha256(manifest):
        raise RecordingConflict(f"{manifest_file} fails its manifest_sha256 check; move the directory aside")
    if (manifest.get("generator") or {}).get("inputs_sha256") != inputs_hash:
        raise RecordingConflict(
            f"{rec_dir} was generated from different inputs (inputs_sha256 mismatch); move it aside"
        )
    return _result_from_manifest(rec_dir, manifest, reused=True)


def _feeds_summary(items: Sequence[Mapping[str, Any]], urls: Mapping[str, str]) -> dict[str, Any]:
    feeds: dict[str, Any] = {}
    for feed in sorted(FEED_ORDER, key=FEED_ORDER.get):
        caps = [i for i in items if i.get("kind") == "capture" and i.get("feed_type") == feed]
        events = [i for i in items if i.get("kind") == "fetch_event" and i.get("feed_type") == feed]
        feeds[feed] = {
            "source_url": urls.get(feed),
            "capture_count": len(caps),
            "rejected_count": sum(1 for i in caps if i.get("parse_status") != PARSE_OK),
            "not_modified_count": sum(1 for i in events if (i.get("params") or {}).get("outcome") == "not_modified"),
            "failed_fetch_count": sum(1 for i in events if (i.get("params") or {}).get("outcome") != "not_modified"),
        }
    return feeds


def write_recording(recordings_root: Path, spec: RecordingSpec, items: Sequence[PlannedItem]) -> RecordingResult:
    """Materialize ``items`` as a recording directory (or reuse an identical existing one)."""
    recordings_root = Path(recordings_root)
    rid = safe_component(spec.recording_id, "recording_id")
    in_hash = str(spec.generator.get("inputs_sha256") or "")
    if not in_hash:
        raise ValueError("spec.generator.inputs_sha256 is required")
    existing = find_reusable(recordings_root, rid, in_hash)
    if existing is not None:
        return existing
    if not items:
        raise ValueError("a recording needs at least one item")

    tmp_root = recordings_root / ".tmp"
    tmp_dir = tmp_root / f"{rid}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    tmp_dir.mkdir(parents=True, exist_ok=False)
    try:
        gtfs_version = sha256_hex(spec.static_zip)
        atomic_write_bytes(tmp_dir / STATIC_RELPATH, spec.static_zip, fsync=False)
        sid = scope_id(spec.route_ids, spec.route_type, spec.scope_policy_version, gtfs_version)

        manifest_items: list[dict[str, Any]] = []
        labels: dict[str, dict[str, Any]] = {}
        key_to_seq: dict[str, int] = {}
        written: set[str] = set()
        times: list[datetime] = []
        for seq, it in enumerate(items, start=1):
            entry: dict[str, Any] = {"seq": seq, "kind": it.kind, "at": iso_utc(it.at)}
            if it.kind == "capture":
                if it.feed_type not in FEED_ORDER:
                    raise ValueError(f"capture #{seq} has invalid feed_type {it.feed_type!r}")
                body = it.raw_body()
                status, feed_ts = classify_body(body, it.at)
                if it.parse_status is not None:
                    status = it.parse_status
                raw_sha = sha256_hex(body)
                cap_id = capture_id(FEED_ABBR[it.feed_type], it.at, raw_sha)
                raw_path = f"{it.feed_type}/{it.at.astimezone(UTC):%Y-%m-%d}/{cap_id}.json.gz"
                gz = gzip_deterministic(body)
                if raw_path not in written:
                    atomic_write_bytes(tmp_dir / raw_path, gz, fsync=False)
                    written.add(raw_path)
                entry.update({
                    "feed_type": it.feed_type,
                    "capture_id": cap_id,
                    "raw_path": raw_path,
                    "raw_sha256": raw_sha,
                    "raw_bytes": len(body),
                    "stored_sha256": sha256_hex(gz),
                    "parse_status": status,
                    "http_status": it.http_status,
                    "feed_timestamp": feed_ts if status == PARSE_OK else None,
                    "params": dict(it.params),
                })
                if it.key:
                    if it.key in key_to_seq:
                        raise ValueError(f"duplicate capture key {it.key!r}")
                    key_to_seq[it.key] = seq
                times.append(it.at)
                if it.label:
                    if it.label in labels:
                        raise ValueError(f"duplicate label {it.label!r}")
                    labels[it.label] = {"capture_id": cap_id, "raw_sha256": raw_sha, "seq": seq,
                                        "kind": "capture", "feed_type": it.feed_type, "at": iso_utc(it.at),
                                        **({"file": it.params["file"]} if "file" in it.params else {})}
            else:
                params = dict(it.params)
                if it.kind == "republish":
                    if it.ref is None or it.ref not in key_to_seq:
                        raise ValueError(f"republish #{seq} references unknown or later capture {it.ref!r}")
                    params["of_seq"] = key_to_seq[it.ref]
                if it.feed_type is not None:
                    entry["feed_type"] = it.feed_type
                entry["params"] = params
                if it.kind == "fetch_event":
                    times.append(it.at)
                if it.label and it.kind != "barrier":
                    if it.label in labels:
                        raise ValueError(f"duplicate label {it.label!r}")
                    labels[it.label] = {"seq": seq, "kind": it.kind, "feed_type": it.feed_type,
                                        "at": iso_utc(it.at)}
            if it.label:
                entry["label"] = it.label
            manifest_items.append(entry)

        first = min(times) if times else spec.start_clock
        last = max(times) if times else spec.start_clock
        manifest: dict[str, Any] = {
            "manifest_version": MANIFEST_VERSION,
            "kind": MANIFEST_KIND,
            "recording_id": rid,
            "source_type": "synthetic",
            "fixture_kind": spec.fixture_kind,
            "seed": spec.seed,
            "scenario_ids": list(spec.scenario_ids),
            "provenance": spec.provenance,
            "created_at": iso_utc(spec.start_clock),
            "finalized_at": iso_utc(spec.start_clock),
            "complete": True,
            "start_clock": iso_utc(spec.start_clock),
            "static": {
                "gtfs_version": gtfs_version,
                "zip_sha256": gtfs_version,
                "zip_relpath": STATIC_RELPATH,
                "feed_version": spec.static_feed_version,
                "fetched_at": None,
                "source_url": spec.static_source_url,
            },
            "scope": {
                "scope_id": sid,
                "route_ids": list(spec.route_ids),
                "route_type": spec.route_type,
                "policy_version": spec.scope_policy_version,
            },
            "collector": {
                "poll_interval_seconds": spec.poll_interval_seconds,
                "user_agent": None,
                "version": GENERATOR_VERSION,
            },
            "time_range": {
                "first_fetched_at": iso_utc(first),
                "last_fetched_at": iso_utc(last),
                "source_duration_seconds": int((last - first).total_seconds()),
            },
            "feeds": _feeds_summary(manifest_items, spec.feed_source_urls),
            "items": manifest_items,
            "gaps": [dict(g) for g in spec.gaps],
            "attribution": spec.attribution,
            "generator": {"name": GENERATOR_NAME, "version": GENERATOR_VERSION,
                          "deterministic": True, **dict(spec.generator)},
        }
        for k, v in spec.extra.items():
            if k in manifest:
                raise ValueError(f"extra manifest key {k!r} collides with a standard key")
            manifest[k] = v
        manifest["manifest_sha256"] = manifest_sha256(manifest)
        atomic_write_json(tmp_dir / LABELS_NAME, labels, fsync=False)
        atomic_write_json(tmp_dir / MANIFEST_NAME, manifest)

        final_dir = recordings_root / rid
        try:
            os.rename(tmp_dir, final_dir)
        except OSError:
            # a concurrent generator finished first: accept it only when it is identical
            reused = find_reusable(recordings_root, rid, in_hash)
            if reused is None:
                raise
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return reused
        return _result_from_manifest(final_dir, manifest, reused=False)
    except BaseException:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)  # our own temporary build directory only
        raise
