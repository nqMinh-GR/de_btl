"""Fixture materialization and envelope building shared by golden smoke, integration tests and benchmarks.

Everything goes through the frozen package interfaces (docs/interfaces.md):
* recordings are materialized with ``python -m transitpulse.replay synth-generate`` (golden / scenario / stress);
* static scopes are loaded with ``python -m transitpulse.static load`` (Spark batch);
* envelopes are rebuilt from raw captures with ``ingestion.feed_parse`` + ``ingestion.scope_filter`` +
  ``ingestion.envelope_builder.envelope_from_capture`` exactly like the replay publisher does, so the snapshot id
  of a fixture label in a run is the one the publisher would produce.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from transitpulse.contracts.envelope import parse_iso_utc
from transitpulse.runtime.paths import WorkPaths, resolve_under


class CliError(RuntimeError):
    """A project CLI exited non-zero or printed ``ok: false``."""

    def __init__(self, argv: Sequence[str], returncode: int, payload: dict[str, Any] | None, stderr_tail: str) -> None:
        self.argv = list(argv)
        self.returncode = returncode
        self.payload = payload
        self.stderr_tail = stderr_tail
        err = (payload or {}).get("error") if isinstance(payload, dict) else None
        msg = err.get("message") if isinstance(err, dict) else None
        super().__init__(
            f"{' '.join(argv[:6])} ... exited {returncode}: {msg or 'no JSON error'}; stderr tail: {stderr_tail[-800:]}"
        )


def parse_cli_json(stdout: str) -> dict[str, Any] | None:
    """The single JSON document a project CLI prints on stdout (last parseable line as a fallback)."""
    text = stdout.strip()
    if not text:
        return None
    try:
        doc = json.loads(text)
        return doc if isinstance(doc, dict) else None
    except ValueError:
        pass
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                doc = json.loads(line)
            except ValueError:
                continue
            if isinstance(doc, dict):
                return doc
    return None


def run_json_cli(
    argv: Sequence[str],
    *,
    env: Mapping[str, str],
    cwd: Path,
    timeout: float,
    check: bool = True,
) -> dict[str, Any]:
    """Run a project CLI (one JSON document on stdout, progress on stderr) and return the parsed JSON."""
    proc = subprocess.run(
        list(argv), env=dict(env), cwd=str(cwd), capture_output=True, text=True, timeout=timeout, check=False
    )
    payload = parse_cli_json(proc.stdout)
    if check and (proc.returncode != 0 or payload is None or payload.get("ok") is False):
        raise CliError(argv, proc.returncode, payload, proc.stderr or "")
    return payload or {}


# --------------------------------------------------------------------------------------------------
# Recordings
# --------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Recording:
    recording_id: str
    recording_dir: Path
    manifest_path: Path
    static_zip_path: Path
    route_ids: tuple[str, ...]
    start_clock: datetime | None
    source_type: str
    fixture_kind: str | None
    manifest: dict[str, Any] = field(repr=False)
    labels: dict[str, dict[str, Any]] = field(repr=False)

    @property
    def items(self) -> list[dict[str, Any]]:
        return [i for i in self.manifest.get("items", []) if isinstance(i, dict)]

    def item_by_capture_id(self, capture_id: str) -> dict[str, Any] | None:
        for item in self.items:
            if item.get("kind") == "capture" and item.get("capture_id") == capture_id:
                return item
        return None

    def item_by_raw_sha256(self, raw_sha256: str) -> dict[str, Any] | None:
        for item in self.items:
            if item.get("kind") == "capture" and item.get("raw_sha256") == raw_sha256:
                return item
        return None

    def item_for_label(self, label: str) -> dict[str, Any] | None:
        info = self.labels.get(label)
        if isinstance(info, dict):
            cid = info.get("capture_id")
            if isinstance(cid, str):
                found = self.item_by_capture_id(cid)
                if found:
                    return found
            sha = info.get("raw_sha256")
            if isinstance(sha, str):
                return self.item_by_raw_sha256(sha)
        return None

    def raw_file(self, item: Mapping[str, Any]) -> Path:
        return resolve_under(self.recording_dir, str(item["raw_path"]))

    def raw_bytes(self, item: Mapping[str, Any]) -> bytes:
        """Decoded capture body, verified against ``raw_sha256``."""
        return read_raw_verified(self.raw_file(item), str(item["raw_sha256"]))

    def source_url(self, feed_type: str) -> str:
        feeds = self.manifest.get("feeds") or {}
        info = feeds.get(feed_type) if isinstance(feeds, dict) else None
        url = info.get("source_url") if isinstance(info, dict) else None
        return url if isinstance(url, str) and url else f"synthetic://{self.recording_id}/{feed_type}"


def read_raw_verified(path: Path, expected_sha256: str | None) -> bytes:
    data = path.read_bytes()
    body = gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data
    if expected_sha256 is not None:
        got = hashlib.sha256(body).hexdigest()
        if got != expected_sha256:
            raise ValueError(f"raw sha256 mismatch for {path}: expected {expected_sha256}, got {got}")
    return body


def load_recording(manifest_path: Path, info: Mapping[str, Any] | None = None) -> Recording:
    manifest_path = Path(manifest_path).resolve()
    with open(manifest_path, encoding="utf-8") as fh:
        manifest = json.load(fh)
    rec_dir = manifest_path.parent
    labels: dict[str, dict[str, Any]] = {}
    labels_file = rec_dir / "labels.json"
    if labels_file.is_file():
        with open(labels_file, encoding="utf-8") as fh:
            raw = json.load(fh)
        if isinstance(raw, dict):
            labels = {str(k): v for k, v in raw.items() if isinstance(v, dict)}
    info = dict(info or {})
    static = manifest.get("static") or {}
    zip_path = info.get("static_zip_path")
    if not zip_path:
        rel = static.get("zip_relpath") or "static/MBTA_GTFS.zip"
        zip_path = str(rec_dir / rel)
    scope = manifest.get("scope") or {}
    route_ids = info.get("route_ids") or scope.get("route_ids") or []
    start = info.get("start_clock")
    if not start:
        tr = manifest.get("time_range") or {}
        start = tr.get("first_fetched_at")
        items = [i for i in manifest.get("items", []) if isinstance(i, dict) and i.get("at")]
        if items:
            start = min(str(i["at"]) for i in items)
    return Recording(
        recording_id=str(info.get("recording_id") or manifest.get("recording_id") or rec_dir.name),
        recording_dir=rec_dir,
        manifest_path=manifest_path,
        static_zip_path=Path(str(zip_path)),
        route_ids=tuple(str(r) for r in route_ids),
        start_clock=parse_iso_utc(start) if isinstance(start, str) else None,
        source_type=str(info.get("source_type") or manifest.get("source_type") or "synthetic"),
        fixture_kind=info.get("fixture_kind", manifest.get("fixture_kind")),
        manifest=manifest,
        labels=labels,
    )


def python_module_argv(core_python: Path | str, module: str, *args: str) -> list[str]:
    return [str(core_python), "-m", module, *args]


def materialize(
    *,
    core_python: Path | str,
    env: Mapping[str, str],
    repo_dir: Path,
    workdir: Path,
    kind: str,
    seed: int = 0,
    scenario_name: str | None = None,
    scenarios: Sequence[str] = (),
    rate: int | None = None,
    duration_seconds: int | None = None,
    timeout: float = 900.0,
) -> Recording:
    """``replay synth-generate`` (idempotent) and load the resulting recording + ``labels.json``."""
    args = ["synth-generate", "--workdir", str(workdir), "--kind", kind, "--seed", str(seed)]
    if scenarios:
        args += ["--scenarios", ",".join(scenarios)]
    if rate is not None:
        args += ["--rate", str(rate)]
    if duration_seconds is not None:
        args += ["--duration-seconds", str(duration_seconds)]
    if scenario_name:
        args += ["--scenario-name", scenario_name]
    payload = run_json_cli(python_module_argv(core_python, "transitpulse.replay", *args), env=env, cwd=repo_dir,
                           timeout=timeout)
    manifest_path = payload.get("manifest_path") or str(Path(payload["recording_dir"]) / "recording_manifest.json")
    return load_recording(Path(manifest_path), payload)


def verify_recording_cli(
    *, core_python: Path | str, env: Mapping[str, str], repo_dir: Path, workdir: Path, manifest_path: Path,
    static_zip: Path | None = None, route_ids: Sequence[str] = (), timeout: float = 600.0,
) -> dict[str, Any]:
    args = ["verify", "--workdir", str(workdir), "--manifest", str(manifest_path)]
    if static_zip is not None:
        args += ["--static-zip", str(static_zip)]
    if route_ids:
        args += ["--route-ids", ",".join(route_ids)]
    return run_json_cli(python_module_argv(core_python, "transitpulse.replay", *args), env=env, cwd=repo_dir,
                        timeout=timeout)


# --------------------------------------------------------------------------------------------------
# Static scope
# --------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class StaticScope:
    gtfs_version: str
    scope_id: str
    agency_timezone: str
    static_zip_path: Path
    scope_dir: Path
    route_ids: tuple[str, ...]
    status: str


def load_static_scope(
    *, core_python: Path | str, env: Mapping[str, str], repo_dir: Path, workdir: Path, profile: str,
    zip_path: Path, route_ids: Sequence[str], timeout: float = 1200.0,
) -> StaticScope:
    """``python -m transitpulse.static load --zip`` (Spark batch, idempotent per version and scope)."""
    payload = run_json_cli(
        python_module_argv(core_python, "transitpulse.static", "load", "--workdir", str(workdir), "--profile",
                           profile, "--zip", str(zip_path), "--route-ids", ",".join(route_ids)),
        env=env, cwd=repo_dir, timeout=timeout,
    )
    status = str(payload.get("status"))
    if status not in ("PASSED", "PASSED_WITH_WARNINGS"):
        raise CliError(["static", "load"], 1, payload, f"static load status {status}")
    verified = payload.get("verified_route_ids") or list(route_ids)
    return StaticScope(
        gtfs_version=str(payload["gtfs_version"]),
        scope_id=str(payload["scope_id"]),
        agency_timezone=str(payload["agency_timezone"]),
        static_zip_path=Path(str(payload["static_zip_path"])),
        scope_dir=Path(str(payload["scope_dir"])),
        route_ids=tuple(str(r) for r in verified),
        status=status,
    )


# --------------------------------------------------------------------------------------------------
# Envelopes from captures
# --------------------------------------------------------------------------------------------------

def raw_ref_for_item(recording: Recording, item: Mapping[str, Any]) -> Any:
    from transitpulse.ingestion.archive import RawRef

    path = recording.raw_file(item)
    stored_sha = item.get("stored_sha256")
    if not isinstance(stored_sha, str):
        stored_sha = hashlib.sha256(path.read_bytes()).hexdigest()
    return RawRef(
        raw_path=str(item["raw_path"]),
        raw_sha256=str(item["raw_sha256"]),
        raw_bytes=int(item.get("raw_bytes") or len(recording.raw_bytes(item))),
        stored_sha256=stored_sha,
        stored_bytes=path.stat().st_size,
    )


class EnvelopeFactory:
    """Builds envelopes for one run exactly like the replay publisher (re-parse, run scope filter, new id)."""

    def __init__(self, run: Any, *, component: str = "pytest-harness") -> None:
        from transitpulse.ingestion.scope_filter import ScopeFilter
        from transitpulse.static.index import load_scope_index

        self.run = run
        self.component = component
        self.scope_filter = ScopeFilter(load_scope_index(Path(run.static_scope_dir)))

    def from_body(
        self, *, feed_type: str, body: bytes, raw_ref: Any, capture_id: str, fetched_at: datetime, source_url: str,
        http: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        from transitpulse.ingestion.envelope_builder import envelope_from_capture
        from transitpulse.ingestion.feed_parse import Rejection, parse_feed

        parsed = parse_feed(body, fetched_at)
        if isinstance(parsed, Rejection):
            raise ValueError(f"capture {capture_id} is rejected by parse_feed: {parsed.reason_code} {parsed.detail}")
        selected, accounting = self.scope_filter.select(feed_type, parsed.entities)
        env = envelope_from_capture(
            run=self.run, feed_type=feed_type, parsed=parsed, selected=selected, accounting=accounting,
            raw_ref=raw_ref, capture_id=capture_id, fetched_at=fetched_at, source_url=source_url, http=http,
            scenario_id=self.run.scenario_id, component=self.component,
        )
        return dict(env)

    def from_recording_item(self, recording: Recording, item: Mapping[str, Any]) -> dict[str, Any]:
        return self.from_body(
            feed_type=str(item["feed_type"]),
            body=recording.raw_bytes(item),
            raw_ref=raw_ref_for_item(recording, item),
            capture_id=str(item["capture_id"]),
            fetched_at=parse_iso_utc(str(item["at"])),
            source_url=recording.source_url(str(item["feed_type"])),
        )

    def archive_and_build(
        self, paths: WorkPaths, *, feed_type: str, body: bytes, fetched_at: datetime, source_url: str,
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Archive ``body`` under ``raw/<run_id>/`` (RawArchive) and build its envelope (test-made captures)."""
        from transitpulse.contracts.enums import FEED_ABBR
        from transitpulse.contracts.ids import capture_id as make_capture_id
        from transitpulse.ingestion.archive import RawArchive

        sha = hashlib.sha256(body).hexdigest()
        cid = make_capture_id(FEED_ABBR[feed_type], fetched_at, sha)
        ref = RawArchive(paths.raw_root).write_capture(
            self.run.run_id, feed_type, cid, fetched_at, body, dict(meta or {"producer": self.component}))
        return self.from_body(feed_type=feed_type, body=body, raw_ref=ref, capture_id=cid, fetched_at=fetched_at,
                              source_url=source_url)


def compute_run_labels(recording: Recording, run: Any, *, component: str = "label-helper") -> dict[str, str]:
    """``{label: snapshot_id}`` for ``run`` (interfaces §10), computed through the same envelope path."""
    factory = EnvelopeFactory(run, component=component)
    out: dict[str, str] = {}
    for label in recording.labels:
        item = recording.item_for_label(label)
        if item is None:
            continue
        out[label] = str(factory.from_recording_item(recording, item)["snapshot_id"])
    return out


def labels_for_run(paths: WorkPaths, run_id: str, recording_dir: Path | None = None) -> dict[str, str]:
    """``{label: snapshot_id}`` for ``demo.py golden-smoke``.

    Prefers ``W/runs/<run_id>/labels.json`` (written by the replay publisher at publish start for synthetic runs);
    otherwise recomputes the ids from the recording through the publisher's envelope path.
    """
    from transitpulse.runs import load_run
    from transitpulse.verify.golden import labels_from_run_file

    labels = labels_from_run_file(paths, run_id)
    if labels:
        return labels
    run = load_run(paths, run_id)
    rec_dir = Path(recording_dir) if recording_dir else paths.recordings_root / str(run.recording_id)
    return compute_run_labels(load_recording(rec_dir / "recording_manifest.json"), run)


# --------------------------------------------------------------------------------------------------
# Timeline plans (timeline.json runs -> steps -> items)
# --------------------------------------------------------------------------------------------------

def header_timestamp_of(golden_dir: Path, file_rel: str | None) -> int | None:
    if not file_rel:
        return None
    try:
        with open(golden_dir / file_rel, encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, ValueError):
        return None
    ts = (doc.get("header") or {}).get("timestamp") if isinstance(doc, dict) else None
    return ts if isinstance(ts, int) else None


def capture_labels_in_order(steps: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """label -> capture item (first occurrence) over a timeline run."""
    out: dict[str, dict[str, Any]] = {}
    for step in steps:
        for item in step.get("items", []):
            if isinstance(item, dict) and item.get("kind") == "capture":
                label = item.get("label") or item.get("file")
                if isinstance(label, str) and label not in out:
                    out[label] = dict(item)
    return out


def classify_golden_steps(steps: Sequence[Mapping[str, Any]], golden_dir: Path) -> dict[str, list[str]]:
    """Map the golden run's steps to IT01-IT04.

    An explicit ``it_case``/``it`` key on a step wins. Otherwise: the first step is IT01; a trailing step with
    only ticks is IT04; a middle step that (re)publishes a vehicle snapshot whose header is older than the newest
    already published is IT03 (late); every other middle step is IT02 (new snapshot / duplicate delivery).
    """
    out: dict[str, list[str]] = {"IT01": [], "IT02": [], "IT03": [], "IT04": []}
    labels = capture_labels_in_order(steps)
    newest: dict[str, int] = {}
    last = len(steps) - 1
    for idx, step in enumerate(steps):
        name = str(step.get("step", f"#{idx}"))
        explicit = step.get("it_case") or step.get("it")
        items = [i for i in step.get("items", []) if isinstance(i, dict)]
        late = False
        for item in items:
            kind = item.get("kind")
            if kind == "capture":
                ref = item
            elif kind == "republish":
                key = item.get("label") or item.get("of_label") or item.get("of_step_item")
                ref = labels.get(key) if isinstance(key, str) else None
            else:
                continue
            if not ref:
                continue
            feed = str(ref.get("feed_type", ""))
            ts = header_timestamp_of(golden_dir, ref.get("file"))
            if ts is None:
                continue
            if feed in newest and ts < newest[feed]:
                late = True
            newest[feed] = max(newest.get(feed, ts), ts)
        if isinstance(explicit, str) and explicit in out:
            out[explicit].append(name)
        elif idx == 0:
            out["IT01"].append(name)
        elif idx == last and items and all(i.get("kind") in ("tick", "barrier") for i in items):
            out["IT04"].append(name)
        elif late:
            out["IT03"].append(name)
        else:
            out["IT02"].append(name)
    return out


def env_with(base: Mapping[str, str], **extra: str) -> dict[str, str]:
    env = dict(base)
    env.update({k: v for k, v in extra.items() if v is not None})
    return env


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write_json(path: Path, doc: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2, sort_keys=True, default=str)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
