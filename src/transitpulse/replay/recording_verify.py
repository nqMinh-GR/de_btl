"""Recording verification before a replay/synthetic run is created or published (design E §3.1, CT11).

Checks, in order (the first failure stops verification):
  1. the manifest is a ``transitpulse.recording`` v1 document and ``manifest_sha256`` matches;
  2. the static ZIP is found (``--static-zip``, then ``<recording_dir>/<static.zip_relpath>``, then
     ``<W>/data/static/<gtfs_version>/MBTA_GTFS.zip``) and ``sha256(zip) == static.zip_sha256 ==
     static.gtfs_version``;
  3. every archived capture exists and its gz file hash equals ``stored_sha256`` (``raw_sha256`` of the
     decoded body is re-verified when the publisher reads it);
  4. the requested route ids are a subset of ``scope.route_ids``;
  5. the items form a valid timeline (kinds, references, synthetic-only items).

The current MBTA_GTFS.zip is NEVER downloaded here: a recording is only meaningful with the exact
static version it was captured against.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from transitpulse.contracts.envelope import iso_utc
from transitpulse.replay.fileio import manifest_sha256, sha256_file
from transitpulse.replay.timeline import (
    CAPTURE,
    TimelineError,
    TimelineItem,
    build_timeline,
    timeline_start,
)
from transitpulse.runtime.paths import WorkPaths, resolve_under

MANIFEST_KIND = "transitpulse.recording"
MANIFEST_VERSION = 1
STATIC_ZIP_NAME = "MBTA_GTFS.zip"


class RecordingVerificationError(Exception):
    """Verification failed; ``code``/``hint`` are surfaced by the CLI (exit code 2)."""

    def __init__(self, code: str, message: str, hint: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint
        self.details = details or {}

    def to_json(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "hint": self.hint, "details": self.details}


@dataclass(frozen=True)
class VerifiedRecording:
    manifest_path: Path
    recording_dir: Path
    manifest: Mapping[str, Any]
    manifest_sha256: str
    recording_id: str
    source_type: str
    fixture_kind: str | None
    seed: int | None
    scenario_ids: tuple[str, ...]
    gtfs_version: str
    static_zip_path: Path
    recording_scope_id: str | None
    recording_route_ids: tuple[str, ...]
    route_ids: tuple[str, ...]
    start_clock: datetime
    item_count: int
    capture_count: int
    complete: bool
    items: tuple[TimelineItem, ...] = field(repr=False)
    warnings: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return {
            "recording_id": self.recording_id,
            "manifest_path": str(self.manifest_path),
            "recording_dir": str(self.recording_dir),
            "manifest_sha256": self.manifest_sha256,
            "source_type": self.source_type,
            "fixture_kind": self.fixture_kind,
            "seed": self.seed,
            "scenario_ids": list(self.scenario_ids),
            "gtfs_version": self.gtfs_version,
            "static_zip_path": str(self.static_zip_path),
            "recording_scope_id": self.recording_scope_id,
            "recording_route_ids": list(self.recording_route_ids),
            "route_ids": list(self.route_ids),
            "start_clock": iso_utc(self.start_clock),
            "item_count": self.item_count,
            "capture_count": self.capture_count,
            "complete": self.complete,
            "warnings": list(self.warnings),
        }


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    if not manifest_path.is_file():
        raise RecordingVerificationError(
            "MANIFEST_NOT_FOUND",
            f"recording manifest {manifest_path} does not exist",
            "Pass the path of a recording_manifest.json (live recordings: <W>/data/raw/<run_id>/; generated "
            "recordings: <W>/data/recordings/<recording_id>/) or import the input bundle first.",
        )
    try:
        with open(manifest_path, encoding="utf-8") as fh:
            manifest = json.load(fh)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecordingVerificationError(
            "MANIFEST_INVALID", f"cannot read recording manifest {manifest_path}: {exc}",
            "The manifest is not valid JSON; re-import the recording bundle.",
        ) from exc
    if not isinstance(manifest, dict):
        raise RecordingVerificationError("MANIFEST_INVALID", "recording manifest is not a JSON object",
                                         "Re-import the recording bundle.")
    return manifest


def _finalize_command(manifest_path: Path, paths: WorkPaths | None) -> str:
    """Exact ``finalize-recording`` command for a live recording (placeholders when not derivable).

    A live recording manifest lives at ``<W>/data/raw/<run_id>/recording_manifest.json``.
    """
    run_dir = manifest_path.parent
    if paths is not None and run_dir.parent == paths.raw_root:
        workdir, run_id = str(paths.root), run_dir.name
    elif run_dir.parent.name == "raw" and run_dir.parent.parent.name == "data":
        workdir, run_id = str(run_dir.parent.parent.parent), run_dir.name
    else:
        workdir, run_id = (str(paths.root) if paths is not None else "<W>"), "<run_id>"
    return f"python -m transitpulse.ingestion finalize-recording --workdir {workdir} --run-id {run_id}"


def _static_candidates(manifest_dir: Path, static: Mapping[str, Any], static_zip: Path | None,
                       paths: WorkPaths | None, gtfs_version: str) -> list[Path]:
    if static_zip is not None:
        return [static_zip]
    out: list[Path] = []
    rel = static.get("zip_relpath")
    if isinstance(rel, str) and rel:
        try:
            out.append(resolve_under(manifest_dir, rel))
        except ValueError:
            pass
    if paths is not None:
        try:
            out.append(paths.static_version_dir(gtfs_version) / STATIC_ZIP_NAME)
        except ValueError:
            pass
    return out


def verify_recording(
    manifest_path: Path,
    *,
    static_zip: Path | None = None,
    route_ids: Sequence[str] | None = None,
    paths: WorkPaths | None = None,
    check_raw: bool = True,
) -> VerifiedRecording:
    """Verify a recording; raise :class:`RecordingVerificationError` with guidance on any mismatch."""
    manifest_path = Path(manifest_path).expanduser().resolve()
    manifest = load_manifest(manifest_path)
    recording_dir = manifest_path.parent
    warnings: list[str] = []

    if manifest.get("kind") != MANIFEST_KIND or manifest.get("manifest_version") != MANIFEST_VERSION:
        raise RecordingVerificationError(
            "MANIFEST_INVALID",
            f"unsupported manifest kind/version {manifest.get('kind')!r}/{manifest.get('manifest_version')!r}",
            f"Expected kind={MANIFEST_KIND!r} manifest_version={MANIFEST_VERSION}.",
        )
    stored_hash = manifest.get("manifest_sha256")
    actual_hash = manifest_sha256(manifest)
    if stored_hash != actual_hash:
        raise RecordingVerificationError(
            "MANIFEST_CHECKSUM_MISMATCH",
            f"manifest_sha256 mismatch (stored {stored_hash}, computed {actual_hash})",
            "The manifest was modified or truncated after finalization; re-import the original recording "
            f"bundle (for an interrupted live recording run `{_finalize_command(manifest_path, paths)}`).",
            {"stored": stored_hash, "computed": actual_hash},
        )
    source_type = str(manifest.get("source_type") or "")
    if source_type not in {"live", "replay", "synthetic"}:
        raise RecordingVerificationError("MANIFEST_INVALID", f"invalid source_type {source_type!r}",
                                         "Recordings are produced by the collector (live) or the generator.")
    if not manifest.get("complete", False):
        warnings.append("recording is marked complete=false (finalized after an interruption)")

    static = manifest.get("static") or {}
    gtfs_version = static.get("gtfs_version")
    if not isinstance(gtfs_version, str) or not gtfs_version:
        raise RecordingVerificationError("MANIFEST_INVALID", "manifest has no static.gtfs_version",
                                         "The recording must pin the GTFS static version it was captured with.")
    if static.get("zip_sha256") not in (None, gtfs_version):
        raise RecordingVerificationError(
            "MANIFEST_INVALID",
            "static.zip_sha256 differs from static.gtfs_version",
            "gtfs_version is defined as sha256(ZIP bytes); the manifest is inconsistent.",
        )
    candidates = _static_candidates(recording_dir, static, static_zip, paths, gtfs_version)
    found = [p for p in candidates if p.is_file()]
    if not found:
        raise RecordingVerificationError(
            "STATIC_ZIP_NOT_FOUND",
            f"recording requires GTFS version {gtfs_version} but no matching static ZIP was found "
            f"(looked at: {', '.join(str(p) for p in candidates) or 'nothing'})",
            f"Recording requires GTFS version {gtfs_version}; import the input bundle or pass --static-zip. "
            "The current MBTA_GTFS.zip will not be downloaded because it may be a different version.",
            {"gtfs_version": gtfs_version, "searched": [str(p) for p in candidates]},
        )
    zip_path = found[0]
    zip_hash = sha256_file(zip_path)
    if zip_hash != gtfs_version:
        raise RecordingVerificationError(
            "STATIC_VERSION_MISMATCH",
            f"static ZIP {zip_path} has sha256 {zip_hash} but the recording requires {gtfs_version}",
            f"Recording requires GTFS version {gtfs_version}; import the input bundle or pass --static-zip with "
            "that exact file. The current MBTA_GTFS.zip will not be downloaded because it may be a different "
            "version.",
            {"expected": gtfs_version, "actual": zip_hash, "zip_path": str(zip_path)},
        )

    scope = manifest.get("scope") or {}
    rec_routes = tuple(str(r) for r in scope.get("route_ids") or ())
    if route_ids is None or len(route_ids) == 0:
        selected = rec_routes
    else:
        selected = tuple(dict.fromkeys(str(r) for r in route_ids))
        missing = [r for r in selected if r not in rec_routes]
        if missing:
            raise RecordingVerificationError(
                "ROUTE_NOT_IN_RECORDING",
                f"route ids {missing} are not part of the recording scope {list(rec_routes)}",
                "A replay run's scope must be a subset of the recorded scope; choose route ids from the "
                "recording or record a new recording for these routes.",
                {"missing": missing, "recording_route_ids": list(rec_routes)},
            )

    try:
        items = build_timeline(manifest)
        start = timeline_start(manifest, items)
    except TimelineError as exc:
        raise RecordingVerificationError("TIMELINE_INVALID", f"invalid recording timeline: {exc}",
                                         "The manifest items are inconsistent; regenerate or re-import it.") from exc
    if not items:
        raise RecordingVerificationError("TIMELINE_INVALID", "the recording has no items",
                                         "Record for longer or regenerate the synthetic recording.")

    capture_count = 0
    if check_raw:
        for it in items:
            if it.kind != CAPTURE or it.capture is None:
                continue
            capture_count += 1
            cap = it.capture
            if cap.raw_path is None:
                continue
            try:
                raw_file = resolve_under(recording_dir, cap.raw_path)
            except ValueError as exc:
                raise RecordingVerificationError(
                    "RAW_PATH_INVALID", f"capture {cap.capture_id}: unsafe raw_path {cap.raw_path!r}",
                    "raw_path must be relative to the recording directory without '..'.",
                ) from exc
            if not raw_file.is_file():
                raise RecordingVerificationError(
                    "RAW_MISSING",
                    f"capture {cap.capture_id} (seq {it.seq}) is missing: {raw_file}",
                    "The recording is incomplete; re-import the full input bundle (raw files + manifest).",
                    {"capture_id": cap.capture_id, "raw_path": cap.raw_path},
                )
            if cap.stored_sha256:
                actual = sha256_file(raw_file)
                if actual != cap.stored_sha256:
                    raise RecordingVerificationError(
                        "RAW_CHECKSUM_MISMATCH",
                        f"capture {cap.capture_id} (seq {it.seq}): stored sha256 {actual} != manifest "
                        f"{cap.stored_sha256}",
                        "A raw file was modified or corrupted; re-import the original recording bundle.",
                        {"capture_id": cap.capture_id, "raw_path": cap.raw_path},
                    )
            else:
                warnings.append(f"capture {cap.capture_id} has no stored_sha256; only raw_sha256 is verified")
    else:
        capture_count = sum(1 for it in items if it.kind == CAPTURE)

    seed = manifest.get("seed")
    return VerifiedRecording(
        manifest_path=manifest_path,
        recording_dir=recording_dir,
        manifest=manifest,
        manifest_sha256=actual_hash,
        recording_id=str(manifest.get("recording_id") or recording_dir.name),
        source_type=source_type,
        fixture_kind=manifest.get("fixture_kind"),
        seed=int(seed) if isinstance(seed, int) and not isinstance(seed, bool) else None,
        scenario_ids=tuple(str(s) for s in manifest.get("scenario_ids") or ()),
        gtfs_version=gtfs_version,
        static_zip_path=zip_path,
        recording_scope_id=scope.get("scope_id"),
        recording_route_ids=rec_routes,
        route_ids=selected,
        start_clock=start,
        item_count=len(items),
        capture_count=capture_count,
        complete=bool(manifest.get("complete", False)),
        items=tuple(items),
        warnings=tuple(warnings),
    )
