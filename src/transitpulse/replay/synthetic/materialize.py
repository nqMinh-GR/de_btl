"""Materialize the hand-written golden fixture into replayable recordings (interfaces §10, design C §8).

``synth-generate --kind golden`` converts ``tests/fixtures/golden/`` run ``golden`` and
``--kind scenario --scenario-name IT06|IT07|IT08|IT09`` the run of that name:

* ``static/*.txt``  -> deterministic ``static/MBTA_GTFS.zip`` (files stored byte-for-byte);
* ``rt/*.json``     -> raw ``.json.gz`` captures (raw_sha256 = sha256 of the fixture file bytes);
* ``timeline.json`` steps -> items in step order, with a ``barrier`` (label = step name) after EVERY step,
  so that ``--step-mode`` stops after each step (also after the last one, which keeps the publisher
  alive for interactive injections; a further ``step`` finishes the run);
* ``labels.json``   -> ``{label: {capture_id, raw_sha256, seq, kind, feed_type, at, file}}``.

Supported item kinds in ``timeline.json``: ``tick{at}``, ``capture{feed_type, file, at, label}``,
``republish{label|of_label|of_step_item, at?}`` (refers to an earlier capture label),
``malformed_envelope{feed_type, value_text|file, at?, label?}``, ``fetch_event{feed_type, at, outcome,
http_status, reason_code}``, ``arm_fault{point, match, at?}``, ``barrier{label}``. Item fields may be given
at top level or inside ``params``.

The expected values are NOT computed here (they live in ``expected.json``, written by hand).
"""

from __future__ import annotations

import csv
import io
import json
import re
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from transitpulse.config import ProjectConfig, repo_root
from transitpulse.contracts.enums import FEED_ORDER
from transitpulse.contracts.envelope import iso_utc, parse_iso_utc
from transitpulse.replay.envelopes import Enveloper
from transitpulse.replay.fileio import atomic_write_json, read_json, sha256_file
from transitpulse.replay.recording_verify import VerifiedRecording
from transitpulse.replay.synthetic.gtfs_builder import deterministic_zip
from transitpulse.replay.synthetic.recording import (
    GENERATOR_VERSION,
    LABELS_NAME,
    PlannedItem,
    RecordingResult,
    RecordingSpec,
    find_reusable,
    inputs_sha256,
    write_recording,
)
from transitpulse.replay.timeline import CAPTURE, TimelineItem
from transitpulse.runs import RunRecord
from transitpulse.runtime.paths import WorkPaths, resolve_under

FIXTURE_KIND_GOLDEN = "golden_handwritten"
GOLDEN_RUN = "golden"
IT_SCENARIO_RUNS: tuple[str, ...] = ("IT06", "IT07", "IT08", "IT09")
TIMELINE_FILE = "timeline.json"
RUN_LABELS_FILE = "labels.json"
_SAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


class FixtureError(ValueError):
    """The fixture directory or timeline is unusable (CLI exit code 2)."""


def default_fixture_dir() -> Path:
    return repo_root() / "tests" / "fixtures" / "golden"


def _field(item: Mapping[str, Any], name: str) -> Any:
    if name in item:
        return item[name]
    params = item.get("params")
    return params.get(name) if isinstance(params, Mapping) else None


def _parse_time(value: Any, where: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise FixtureError(f"{where}: missing or invalid 'at' {value!r}")
    try:
        return parse_iso_utc(value)
    except ValueError as exc:
        raise FixtureError(f"{where}: invalid 'at' {value!r}: {exc}") from exc


def _feed_version(static_files: Mapping[str, bytes]) -> str | None:
    raw = static_files.get("feed_info.txt")
    if raw is None:
        return None
    try:
        rows = list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))))
    except (UnicodeDecodeError, csv.Error):
        return None
    if rows and rows[0].get("feed_version"):
        return rows[0]["feed_version"].strip() or None
    return None


def _fixture_inputs(fixture_dir: Path) -> dict[str, str]:
    """sha256 of every input file (static/*.txt, rt/*, timeline.json), keyed by relative POSIX path."""
    out: dict[str, str] = {}
    for sub in ("static", "rt"):
        d = fixture_dir / sub
        if d.is_dir():
            for f in sorted(d.iterdir()):
                if f.is_file():
                    out[f"{sub}/{f.name}"] = sha256_file(f)
    out[TIMELINE_FILE] = sha256_file(fixture_dir / TIMELINE_FILE)
    return out


def load_fixture_timeline(fixture_dir: Path) -> dict[str, Any]:
    path = fixture_dir / TIMELINE_FILE
    if not path.is_file():
        raise FixtureError(f"fixture timeline {path} not found")
    try:
        data = read_json(path)
    except (OSError, ValueError) as exc:
        raise FixtureError(f"cannot read {path}: {exc}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("runs"), dict):
        raise FixtureError(f"{path} has no 'runs' object")
    return data


def build_fixture_items(fixture_dir: Path, run: Mapping[str, Any], run_name: str) -> list[PlannedItem]:
    steps = run.get("steps")
    if not isinstance(steps, list) or not steps:
        raise FixtureError(f"run {run_name!r} has no steps")
    items: list[PlannedItem] = []
    capture_keys: dict[str, str] = {}                        # label -> key
    step_items: dict[str, list[PlannedItem]] = {}
    current: datetime | None = _parse_time(run["start_clock"], f"{run_name}.start_clock") \
        if run.get("start_clock") else None
    for s_index, step in enumerate(steps):
        if not isinstance(step, Mapping):
            raise FixtureError(f"{run_name}: step #{s_index} is not an object")
        step_name = str(step.get("step") or f"S{s_index + 1}")
        produced: list[PlannedItem] = []
        for i_index, raw in enumerate(step.get("items") or []):
            where = f"{run_name}/{step_name}/item#{i_index}"
            if not isinstance(raw, Mapping):
                raise FixtureError(f"{where}: not an object")
            kind = raw.get("kind")
            at_raw = _field(raw, "at")
            at = _parse_time(at_raw, where) if at_raw is not None else current
            if at is None:
                raise FixtureError(f"{where}: 'at' is required (no start_clock to inherit)")
            label = _field(raw, "label")
            label = str(label) if label else None
            feed = _field(raw, "feed_type")
            if kind == "tick":
                item = PlannedItem(kind="tick", at=at, label=None)
            elif kind == "capture":
                if feed not in FEED_ORDER:
                    raise FixtureError(f"{where}: invalid feed_type {feed!r}")
                rel = _field(raw, "file")
                if not isinstance(rel, str) or not rel:
                    raise FixtureError(f"{where}: capture requires 'file'")
                try:
                    path = resolve_under(fixture_dir, rel)
                except ValueError as exc:
                    raise FixtureError(f"{where}: unsafe file path {rel!r}") from exc
                if not path.is_file():
                    raise FixtureError(f"{where}: file {rel} not found under {fixture_dir}")
                label = label or Path(rel).stem
                if label in capture_keys:
                    raise FixtureError(f"{where}: capture label {label!r} is used twice")
                key = f"label:{label}"
                capture_keys[label] = key
                http_status = _field(raw, "http_status")
                item = PlannedItem(kind="capture", at=at, feed_type=feed, body=path.read_bytes(),
                                   parse_status=_field(raw, "parse_status"),
                                   http_status=int(http_status) if isinstance(http_status, int) else 200,
                                   params={"file": rel, "step": step_name}, label=label, key=key)
            elif kind == "republish":
                ref = _field(raw, "of_label") or _field(raw, "of") or _field(raw, "label")
                step_ref = _field(raw, "of_step_item")
                key = None
                if isinstance(ref, str) and ref in capture_keys:
                    key = capture_keys[ref]
                elif isinstance(step_ref, str):
                    key = _resolve_step_item(step_ref, step_items, capture_keys, where)
                if key is None:
                    raise FixtureError(f"{where}: republish refers to unknown earlier capture {ref or step_ref!r}")
                target = next(i for i in items if i.key == key)
                item = PlannedItem(kind="republish", at=at, feed_type=target.feed_type, ref=key,
                                   params={"step": step_name, "of_label": target.label})
                label = None
            elif kind == "malformed_envelope":
                if feed not in FEED_ORDER:
                    raise FixtureError(f"{where}: invalid feed_type {feed!r}")
                value = _field(raw, "value_text")
                if value is None and _field(raw, "file"):
                    value = resolve_under(fixture_dir, str(_field(raw, "file"))).read_text(encoding="utf-8")
                if not isinstance(value, str):
                    raise FixtureError(f"{where}: malformed_envelope requires 'value_text' or 'file'")
                item = PlannedItem(kind="malformed_envelope", at=at, feed_type=feed,
                                   params={"feed_type": feed, "value_text": value, "step": step_name}, label=label)
            elif kind == "fetch_event":
                if feed not in FEED_ORDER:
                    raise FixtureError(f"{where}: invalid feed_type {feed!r}")
                item = PlannedItem(kind="fetch_event", at=at, feed_type=feed, label=label, params={
                    "outcome": _field(raw, "outcome") or "failed",
                    "http_status": _field(raw, "http_status"),
                    "reason_code": _field(raw, "reason_code") or "SOURCE_FETCH_FAILED",
                    "step": step_name,
                })
            elif kind == "arm_fault":
                point, match = _field(raw, "point"), _field(raw, "match")
                if not isinstance(point, str) or not isinstance(match, Mapping):
                    raise FixtureError(f"{where}: arm_fault requires 'point' and 'match'")
                params = {"point": point, "match": dict(match), "step": step_name}
                if _field(raw, "fraction") is not None:
                    params["fraction"] = _field(raw, "fraction")
                item = PlannedItem(kind="arm_fault", at=at, feed_type=match.get("feed_type"), params=params,
                                   label=label)
            elif kind == "barrier":
                item = PlannedItem(kind="barrier", at=at, params={"label": label or step_name}, label=label or step_name)
            else:
                raise FixtureError(f"{where}: unsupported item kind {kind!r}")
            items.append(item)
            produced.append(item)
            if item.kind in ("tick", "capture", "fetch_event"):
                current = at
            elif current is None:
                current = at
        step_items[step_name] = produced
        last_at = current if current is not None else items[-1].at
        items.append(PlannedItem(kind="barrier", at=last_at, params={"label": step_name}, label=step_name))
    return items


def _resolve_step_item(ref: str, step_items: Mapping[str, list[PlannedItem]], capture_keys: Mapping[str, str],
                       where: str) -> str | None:
    if ref in capture_keys:
        return capture_keys[ref]
    m = re.fullmatch(r"([^/:]+)[/:](\d+)", ref)
    if m is None:
        return None
    step, idx = m.group(1), int(m.group(2))
    produced = step_items.get(step) or []
    if not 0 <= idx < len(produced) or produced[idx].kind != "capture":
        raise FixtureError(f"{where}: of_step_item {ref!r} does not reference a capture")
    return produced[idx].key


def materialize_fixture(
    cfg: ProjectConfig,
    paths: WorkPaths,
    *,
    run_name: str = GOLDEN_RUN,
    fixture_dir: Path | None = None,
) -> RecordingResult:
    """Golden (``run_name='golden'``) or IT scenario run of the hand-written fixture -> recording dir."""
    fixture_dir = Path(fixture_dir or default_fixture_dir()).resolve()
    timeline = load_fixture_timeline(fixture_dir)
    runs = timeline["runs"]
    if run_name not in runs:
        raise FixtureError(f"fixture run {run_name!r} not found in {fixture_dir / TIMELINE_FILE}; "
                           f"available: {sorted(runs)}")
    run = runs[run_name]
    fixture_id = str(timeline.get("fixture_id") or "golden")
    static_dir = fixture_dir / "static"
    static_files = {f.name: f.read_bytes() for f in sorted(static_dir.glob("*.txt"))} if static_dir.is_dir() else {}
    required = ("agency.txt", "routes.txt", "stops.txt", "trips.txt", "stop_times.txt")
    missing = [name for name in required if name not in static_files]
    if missing:
        raise FixtureError(f"fixture static directory {static_dir} lacks {missing}")
    route_ids = tuple(str(r) for r in (run.get("scope_route_ids") or timeline.get("scope_route_ids") or ()))
    if not route_ids:
        raise FixtureError(f"fixture run {run_name!r} has no scope_route_ids")

    kind = "golden" if run_name == GOLDEN_RUN else "scenario"
    file_hashes = _fixture_inputs(fixture_dir)
    inputs = {
        "kind": kind,
        "run_name": run_name,
        "fixture_id": fixture_id,
        "files": file_hashes,
        "scope_policy_version": cfg.scope.scope_policy_version,
        "route_type": cfg.scope.route_type,
        "generator_version": GENERATOR_VERSION,
    }
    in_hash = inputs_sha256(inputs)
    recording_id = f"fx-{_SAFE_CHARS.sub('-', fixture_id)}-{_SAFE_CHARS.sub('-', run_name)}-{in_hash[:12]}"
    existing = find_reusable(paths.recordings_root, recording_id, in_hash)
    if existing is not None:
        return existing

    items = build_fixture_items(fixture_dir, run, run_name)
    start_raw = run.get("start_clock")
    start = _parse_time(start_raw, f"{run_name}.start_clock") if start_raw else min(i.at for i in items)
    provenance = str(timeline.get("provenance") or "synthetic, hand-written, NOT MBTA data")
    spec = RecordingSpec(
        recording_id=recording_id,
        fixture_kind=FIXTURE_KIND_GOLDEN,
        seed=None,
        scenario_ids=(run_name,),
        provenance=f"fixture {fixture_id} run {run_name} (tests/fixtures/golden): {provenance}",
        attribution="Hand-written synthetic test fixture; schema modelled on MBTA GTFS/GTFS-Realtime JSON. "
                    "Not MBTA data.",
        static_zip=deterministic_zip(static_files),
        static_feed_version=_feed_version(static_files) or fixture_id,
        static_source_url=f"fixture://{fixture_id}/static",
        route_ids=route_ids,
        route_type=cfg.scope.route_type,
        scope_policy_version=cfg.scope.scope_policy_version,
        poll_interval_seconds=None,
        feed_source_urls={f: f"fixture://{fixture_id}/{f}" for f in FEED_ORDER},
        start_clock=start,
        generator={"kind": kind, "inputs_sha256": in_hash, "inputs": inputs},
        extra={"fixture": {
            "fixture_id": fixture_id,
            "run_name": run_name,
            "description": run.get("description"),
            "agency_timezone": timeline.get("agency_timezone"),
            "steps": [str(s.get("step")) for s in run.get("steps") or [] if isinstance(s, Mapping)],
            "source": "tests/fixtures/golden",
        }},
    )
    return write_recording(paths.recordings_root, spec, items)


# --------------------------------------------------------------------------------------------------
# Run-specific label map (label -> snapshot_id), computed once the run_id is known
# --------------------------------------------------------------------------------------------------

def compute_snapshot_labels(verified: VerifiedRecording, enveloper: Enveloper) -> dict[str, str]:
    """{label: snapshot_id} for every labelled ok capture, through the same path the publisher uses."""
    out: dict[str, str] = {}
    for item in verified.items:
        if item.kind != CAPTURE or item.label is None or item.capture is None or not item.capture.is_ok:
            continue
        outcome = _build(item, enveloper)
        if outcome is not None:
            out[item.label] = outcome
    return out


def _build(item: TimelineItem, enveloper: Enveloper) -> str | None:
    cap = item.capture
    assert cap is not None
    body = enveloper.read_capture(cap)
    res = enveloper.build(feed_type=str(item.feed_type), body=body, fetched_at=item.at,
                          raw=enveloper.raw_location(cap), capture_id=cap.capture_id, source_url=cap.source_url,
                          http_status=cap.http_status)
    if not res.ok or res.envelope is None:
        return None
    return str(res.envelope.get("snapshot_id"))


def write_run_labels(run_dir: Path, run: RunRecord, verified: VerifiedRecording, enveloper: Enveloper) -> Path | None:
    """Write ``W/runs/<run_id>/labels.json`` ({captures, snapshots}) when the recording has labels."""
    labels_file = verified.recording_dir / LABELS_NAME
    if not labels_file.is_file():
        return None
    try:
        captures = json.loads(labels_file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    doc = {
        "run_id": run.run_id,
        "recording_id": verified.recording_id,
        "manifest_sha256": verified.manifest_sha256,
        "captures": captures,
        "snapshots": compute_snapshot_labels(verified, enveloper),
        "computed_at_clock": iso_utc(verified.start_clock),
    }
    target = Path(run_dir) / RUN_LABELS_FILE
    atomic_write_json(target, doc)
    return target
