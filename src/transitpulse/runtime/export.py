"""``scripts/export_artifacts.py``: evidence + reproducible-input ZIP (spec §26.2, design D §7).

    export_artifacts.py --workdir W [--session-id S | --run-id R] [--max-zip-mb 200] [--log-tail-lines 2000]
                        [--no-static-zip] [--no-api-samples]

Writes ``W/artifacts/transitpulse_<run>_<ts>.zip`` (renamed into place only when complete) and the same
``manifest.json`` next to it (``.manifest.json``). Contents, by priority under the size budget:
1. manifests/reports/run+session records (always): runtime summary WITHOUT the env dump, bootstrap report,
   connector lock, W/reports/** (incl. reports/notebook/), run.json, session.json, publisher state,
   generated uv.lock when lock_origin is generated_*;
2. replay inputs: recording manifest + raw captures + static GTFS ZIP (REPLAYABLE only if all present);
3. bounded, redacted log tails; 4. committed curated batches (``_COMMITTED`` only); 5. API samples.
Never included: runtime.env / environment dumps, tokens, W/infra (live Kafka/Cassandra files), checkpoints,
tools, caches, partial files. Exporting while the run is RUNNING is a point-in-time export (stated in the
manifest).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from transitpulse.runtime.cli_common import JsonArgumentParser, Result, add_common_args, http_get, load_ctx, run_main
from transitpulse.runtime.paths import resolve_under
from transitpulse.runtime.util import git_info, iso_now, read_json_or_none, sha256_file, tail_lines
from transitpulse.runtime.verification import benchmark_status, golden_status, integration_status, junit_status

MiB = 1024 * 1024
TEXT_SUFFIXES = (".log", ".txt", ".jsonl", ".json", ".md", ".csv", ".xml", ".properties", ".yaml", ".yml")
_SECRET_PATTERNS = (
    re.compile(r"(?i)\b(authorization|token|password|passwd|secret|api[_-]?key)\b(\s*[:=]\s*)([^\n]*)"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com"),  # live public quick-tunnel URL
)
TUNNEL_URL_PATTERN = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")


def mask_tunnel_urls(text: str) -> str:
    """Replace live quick-tunnel URLs (anyone holding one can open the dashboard) in any exported text."""
    return TUNNEL_URL_PATTERN.sub("<redacted>", text)


def masked_public_urls(path: Path) -> bytes | None:
    """``public_urls.json`` with every ``public_url``/``public_api_base`` replaced by ``"<redacted>"``."""
    data = read_json_or_none(path)
    if not isinstance(data, dict):
        return None

    def scrub(node: Any) -> Any:
        if isinstance(node, dict):
            return {k: ("<redacted>" if k in ("public_url", "public_api_base") and v else scrub(v))
                    for k, v in node.items()}
        if isinstance(node, list):
            return [scrub(v) for v in node]
        if isinstance(node, str):
            return mask_tunnel_urls(node)
        return node

    return json.dumps(scrub(data), indent=2, sort_keys=True).encode()
_EXCLUDED_NAMES = frozenset({"runtime.env", ".env", ".git-credentials"})
_EXCLUDED_SUFFIXES = (".pem", ".key", ".partial", ".tmp")


def redact(text: str) -> str:
    out = _SECRET_PATTERNS[0].sub(lambda m: f"{m.group(1)}{m.group(2)}[REDACTED]", text)
    for pat in _SECRET_PATTERNS[1:]:
        out = pat.sub("[REDACTED]", out)
    return out


def is_excluded(path: Path, workdir: Path) -> bool:
    """Files that must never enter an export ZIP."""
    if path.name in _EXCLUDED_NAMES or path.name.endswith(_EXCLUDED_SUFFIXES):
        return True
    try:
        rel = path.resolve().relative_to(workdir.resolve())
    except ValueError:
        return False
    top = rel.parts[0] if rel.parts else ""
    return top in ("infra", "tools", "cache", "checkpoints", "spark-local", "tmp", "artifacts") or \
        (top == "processes" and path.name != "last_start.json")


@dataclass
class Item:
    arcname: str
    priority: int
    path: Path | None = None
    data: bytes | None = None
    group: str = "evidence"

    def size(self) -> int:
        if self.data is not None:
            return len(self.data)
        try:
            return self.path.stat().st_size if self.path else 0
        except OSError:
            return 0


class Collector:
    def __init__(self, workdir: Path) -> None:
        self.workdir = workdir
        self.items: dict[str, Item] = {}
        self.excluded: list[str] = []

    def add_file(self, path: Path, arcname: str, priority: int, group: str = "evidence") -> bool:
        if not path.is_file() or path.is_symlink():
            return False
        if is_excluded(path, self.workdir):
            self.excluded.append(str(path))
            return False
        self.items.setdefault(arcname, Item(arcname, priority, path=path, group=group))
        return True

    def add_bytes(self, data: bytes, arcname: str, priority: int, group: str = "evidence") -> None:
        self.items[arcname] = Item(arcname, priority, data=data, group=group)

    def add_tree(self, root: Path, prefix: str, priority: int, group: str, *, max_files: int = 20000) -> int:
        n = 0
        if not root.is_dir():
            return 0
        for dirpath, _dirs, names in os.walk(root, followlinks=False):
            for name in sorted(names):
                p = Path(dirpath) / name
                rel = p.relative_to(root).as_posix()
                if self.add_file(p, f"{prefix}/{rel}", priority, group):
                    n += 1
                if n >= max_files:
                    return n
        return n


def _select_run(ctx: Any, args: argparse.Namespace) -> tuple[str | None, dict[str, Any] | None]:
    from transitpulse.runtime.session import list_sessions, read_session

    session = None
    if args.session_id:
        session = read_session(ctx.paths, args.session_id)
        if session is None:
            from transitpulse.runtime.cli_common import EXIT_USAGE, CliError

            raise CliError("NO_SESSION", f"session {args.session_id} not found", exit_code=EXIT_USAGE)
    run_id = args.run_id or (session or {}).get("run_id")
    if run_id is None:
        reg = read_json_or_none(ctx.paths.registry_json) or {}
        session = read_session(ctx.paths, reg.get("current_session"))
        if session is None:
            demo = [s for s in list_sessions(ctx.paths) if s.get("mode") != "golden-smoke" and s.get("run_id")]
            session = demo[-1] if demo else None
        run_id = (session or {}).get("run_id")
    if run_id is None:
        runs = sorted((ctx.paths.root / "runs").glob("*/run.json"), key=lambda p: p.stat().st_mtime)
        if runs:
            run_id = runs[-1].parent.name
    if session is None and run_id:
        for s in list_sessions(ctx.paths):
            if s.get("run_id") == run_id:
                session = s
    return run_id, session


def _junit_or_report_statuses(paths: Any) -> dict[str, Any]:
    return {
        "unit_tests": junit_status(paths.reports / "unit" / "junit.xml", command="pytest tests/unit")["status"],
        "integration_tests": integration_status(paths.reports / "test_report.json")["status"],
        "golden_smoke": golden_status(paths)["status"],
        "benchmark_measured": benchmark_status(paths)["status"],
        "fault_demo": "RECORDED" if (paths.reports / "fault_demo").is_dir() and
                      any((paths.reports / "fault_demo").glob("*.jsonl")) else "NOT_RUN",
    }


def export(args: argparse.Namespace) -> Result:
    from transitpulse.runtime.session import derive_state

    ctx = load_ctx(args, need_runtime=False)
    paths = ctx.paths
    w = paths.root
    run_id, session = _select_run(ctx, args)
    git = git_info(ctx.repo_dir)
    runtime = read_json_or_none(paths.runtime_json) or {}
    col = Collector(w)
    notes: list[str] = []

    # ---- priority 1: manifests, reports, records
    if runtime:
        summary = {k: v for k, v in runtime.items() if k != "env"}
        summary["env_keys"] = sorted((runtime.get("env") or {}).keys())
        col.add_bytes(json.dumps(summary, indent=2, sort_keys=True).encode(), "runtime/runtime_summary.json", 1)
    # Small metadata files that live in otherwise excluded areas (cache, infra, processes) are listed explicitly.
    for p, arc in ((paths.bootstrap_report, "runtime/bootstrap_report.json"),
                   (paths.runtime_dir / "manifest_pin_patch.json", "runtime/manifest_pin_patch.json"),
                   (paths.runtime_dir / "infra_init.json", "runtime/infra_init.json"),
                   (paths.runtime_dir / "lock_origin.json", "runtime/lock_origin.json"),
                   (paths.conf / "rendered.json", "runtime/rendered_conf.json"),
                   (paths.connector_jars / "connector-lock.json", "runtime/connector-lock.json"),
                   (paths.kafka_cluster_json, "runtime/kafka_cluster.json"),
                   (paths.processes / "last_start.json", "runtime/last_start.json")):
        if p.is_file() and not p.is_symlink():
            col.items.setdefault(arc, Item(arc, 1, path=p))
    public_urls = masked_public_urls(paths.runtime_dir / "public_urls.json")
    if public_urls is not None:
        col.add_bytes(public_urls, "runtime/public_urls.json", 1)
    col.add_tree(paths.reports, "reports", 1, "reports")
    lock_origin = (runtime.get("python") or {}).get("lock_origin")
    if lock_origin and lock_origin != "committed" and (ctx.repo_dir / "uv.lock").is_file():
        col.add_file(ctx.repo_dir / "uv.lock", "repo/uv.lock", 1, "lock")
        notes.append(f"uv.lock was generated ({lock_origin}): commit repo/uv.lock to the repository")
    run_json: dict[str, Any] = {}
    if run_id:
        rdir = paths.run_dir(run_id)
        col.add_tree(rdir, f"runs/{run_id}", 1, "run")
        run_json = read_json_or_none(rdir / "run.json") or {}
        qp = paths.query_progress_log(run_id)
        if qp.is_file():
            col.add_bytes(tail_lines(qp, max(1, args.log_tail_lines), 4 * MiB).encode(),
                          f"logs/runs/{run_id}/query_progress.jsonl", 1, "logs")
    if session:
        sdir = paths.session_dir(str(session["session_id"]))
        for name in ("session.json", "publisher_state.json"):
            col.add_file(sdir / name, f"sessions/{session['session_id']}/{name}", 1, "session")
        pl = sdir / "publish_log.jsonl"
        if pl.is_file():
            col.add_bytes(tail_lines(pl, max(1, args.log_tail_lines), 4 * MiB).encode(),
                          f"sessions/{session['session_id']}/publish_log.jsonl", 3, "logs")

    # ---- priority 2: replay inputs (recording + static ZIP)
    completeness_reasons: list[str] = []
    recording_dir: Path | None = None
    source_type = run_json.get("source_type") or (session or {}).get("source_type")
    if run_id:
        prov = run_json.get("provenance") or {}
        if source_type == "live":
            recording_dir = paths.raw_run_dir(run_id)
        elif prov.get("recording_dir"):
            recording_dir = Path(prov["recording_dir"])
        elif run_json.get("recording_id"):
            recording_dir = paths.recordings_root / str(run_json["recording_id"])
    rec_manifest = read_json_or_none(recording_dir / "recording_manifest.json") if recording_dir else None
    recording_included = False
    if recording_dir and isinstance(rec_manifest, dict):
        try:
            inside = recording_dir.resolve().is_relative_to(w)
        except OSError:
            inside = False
        if not inside:
            completeness_reasons.append(f"recording dir {recording_dir} is outside the work directory; not exported")
        else:
            col.add_file(recording_dir / "recording_manifest.json", "inputs/recording/recording_manifest.json", 2,
                         "recording")
            for extra in ("labels.json", "capture_index.jsonl"):
                col.add_file(recording_dir / extra, f"inputs/recording/{extra}", 2, "recording")
            missing = 0
            for it in rec_manifest.get("items", []):
                rp = it.get("raw_path")
                if not rp:
                    continue
                try:
                    f = resolve_under(recording_dir, str(rp))
                except ValueError:
                    missing += 1
                    continue
                if not col.add_file(f, f"inputs/recording/{rp}", 2, "recording"):
                    missing += 1
                meta = f.with_name(f.name.replace(".json.gz", ".meta.json"))
                if meta != f:
                    col.add_file(meta, f"inputs/recording/{Path(rp).parent.as_posix()}/{meta.name}", 2, "recording")
            static_rel = (rec_manifest.get("static") or {}).get("zip_relpath")
            if static_rel and not args.no_static_zip:
                try:
                    col.add_file(resolve_under(recording_dir, str(static_rel)), f"inputs/recording/{static_rel}", 2,
                                 "recording")
                except ValueError:
                    completeness_reasons.append(f"unsafe static zip_relpath {static_rel!r} in the recording manifest")
            recording_included = missing == 0
            if missing:
                completeness_reasons.append(f"{missing} capture file(s) of the recording are missing")
            if rec_manifest.get("complete") is False:
                completeness_reasons.append("recording manifest is not finalized (complete=false)")
    elif run_id and source_type is not None:
        completeness_reasons.append("no recording manifest found for the run")
    gv = run_json.get("gtfs_version")
    static_included = False
    if gv and not args.no_static_zip:
        try:
            vdir = paths.static_version_dir(str(gv))
        except ValueError:
            vdir = None
        if vdir is not None:
            static_included = col.add_file(vdir / "MBTA_GTFS.zip", f"inputs/static/{gv}/MBTA_GTFS.zip", 2, "static")
            col.add_file(vdir / "version.json", f"inputs/static/{gv}/version.json", 1, "static")
            scope_dir = Path(run_json.get("static_scope_dir") or "")
            for name in ("manifest.json", "scope_index.json", "static_quality_report.json"):
                if scope_dir.name:
                    col.add_file(scope_dir / name, f"inputs/static/{gv}/{scope_dir.name}/{name}", 1, "static")
    if gv and not static_included:
        completeness_reasons.append("static GTFS ZIP not included" + (" (--no-static-zip)" if args.no_static_zip else ""))

    # ---- priority 3: bounded, redacted log tails
    for comp, files in (("processor", ("stdout.log",)), ("publisher", ("stdout.log",)), ("collector", ("stdout.log",)),
                        ("api", ("stdout.log",)), ("kafka", ("server.log", "stdout.log")),
                        ("cassandra", ("system.log",)), ("bootstrap", ("uv.log", "spark_probe.log")),
                        ("dashboard", ("stdout.log",)), ("react", ("stdout.log",)),
                        ("tunnel", ("stdout.log",)), ("tunnel-api", ("stdout.log",)),
                        ("tunnel-react", ("stdout.log",))):
        for name in files:
            p = paths.log_dir(comp) / name
            if p.is_file():
                text = redact(tail_lines(p, max(1, args.log_tail_lines), 2 * MiB))
                col.add_bytes(text.encode(), f"logs/{comp}/{name}.tail", 3, "logs")

    # ---- priority 4: committed curated batches only
    if run_id:
        croot = paths.curated_run_dir(run_id)
        for committed in sorted(croot.glob("query=*/batch_id=*/_COMMITTED")) if croot.is_dir() else []:
            bdir = committed.parent
            col.add_tree(bdir, f"curated/run_id={run_id}/{bdir.parent.name}/{bdir.name}", 4, "curated")

    # ---- priority 5: API samples (point-in-time)
    if run_id and not args.no_api_samples:
        samples: dict[str, Any] = {}
        for ep in (f"/runs/{run_id}", f"/pipeline/status?run_id={run_id}", f"/quality?run_id={run_id}",
                   f"/routes?run_id={run_id}", f"/vehicles?run_id={run_id}&limit=50", f"/alerts?run_id={run_id}"):
            status, body = http_get(f"{ctx.api_base}{ep}", timeout=5.0)
            samples[ep] = {"http_status": status, "fetched_at": iso_now(), "body": body}
        col.add_bytes(json.dumps(samples, indent=1, default=str).encode(), "api_samples/samples.json", 5, "api")

    # ---- budget
    budget = int(args.max_zip_mb) * MiB
    chosen: list[Item] = []
    skipped: list[dict[str, Any]] = []
    used = 0
    by_priority: dict[int, list[Item]] = {}
    for it in col.items.values():
        by_priority.setdefault(it.priority, []).append(it)
    for prio in sorted(by_priority):
        group_items = sorted(by_priority[prio], key=lambda i: i.arcname)
        if prio == 2:
            # Replay inputs are all-or-nothing: a partial recording is not a replay bundle.
            total = sum(i.size() for i in group_items)
            if used + total > budget:
                skipped.append({"group": "replay_inputs", "bytes": total, "reason": "exceeds --max-zip-mb"})
                completeness_reasons.append(f"replay inputs ({total // MiB} MiB) exceed the {args.max_zip_mb} MiB budget")
                recording_included = False
                static_included = False
                continue
        for it in group_items:
            size = it.size()
            if used + size > budget:
                skipped.append({"arcname": it.arcname, "bytes": size, "reason": "exceeds --max-zip-mb"})
                continue
            chosen.append(it)
            used += size

    completeness = "REPLAYABLE" if (recording_included and static_included and not completeness_reasons) \
        else "EVIDENCE_ONLY"
    run_state = None
    if session:
        from transitpulse.runtime.supervisor import Supervisor

        sid = str(session["session_id"])
        sup = Supervisor(paths)
        alive = {c: bool(sup.entry(c)) and (sup.entry(c) or {}).get("session_id") == sid and sup.is_alive(c)
                 for c in ("processor", "publisher", "collector")}
        run_state = derive_state(session, processor_alive=alive["processor"],
                                 input_alive=alive["collector" if session.get("mode") == "live" else "publisher"],
                                 publisher_state=read_json_or_none(paths.session_dir(sid) / "publisher_state.json"))
    point_in_time = run_state in ("RUNNING", "PAUSED", "STARTING")
    if point_in_time:
        notes.append("point-in-time export: the run is still RUNNING; logs are cut at exported_at and only "
                     "committed curated batches are included")

    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    base = f"transitpulse_{run_id or 'no-run'}_{ts}"
    paths.artifacts.mkdir(parents=True, exist_ok=True)
    zip_path = paths.artifacts / f"{base}.zip"
    tmp = paths.artifacts / f".{base}.zip.partial"
    checksums: dict[str, dict[str, Any]] = {}
    with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for it in chosen:
            if it.data is not None:
                data = it.data
            else:
                try:
                    data = it.path.read_bytes()  # type: ignore[union-attr]
                except OSError:
                    skipped.append({"arcname": it.arcname, "reason": "unreadable"})
                    continue
                if it.path is not None and it.path.suffix in (".log", ".txt"):
                    data = redact(data.decode("utf-8", errors="replace")).encode()
            if it.arcname.endswith(TEXT_SUFFIXES) and b"trycloudflare.com" in data:
                # e.g. reports/notebook/*.json written by the notebook may contain the live public URL.
                data = mask_tunnel_urls(data.decode("utf-8", errors="replace")).encode()
            compress =zipfile.ZIP_STORED if it.arcname.endswith((".zip", ".gz", ".parquet")) else zipfile.ZIP_DEFLATED
            zf.writestr(_zinfo(it.arcname), data, compress_type=compress)
            checksums[it.arcname] = {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data), "group": it.group}
        manifest = {
            "manifest_version": 1, "kind": "transitpulse.export", "exported_at": iso_now(),
            "commit_sha": git.get("commit"), "commit_reason": git.get("reason"), "dirty": git.get("dirty"),
            "runtime_sha256": sha256_file(paths.runtime_json) if paths.runtime_json.is_file() else None,
            "config_sha256": (runtime.get("hashes") or {}).get("config"),
            "lock_sha256": (runtime.get("python") or {}).get("lock_sha256"), "lock_origin": lock_origin,
            "run_id": run_id, "session_id": (session or {}).get("session_id"), "source_type": source_type,
            "mode": (session or {}).get("mode"),
            "scope": {"route_ids": run_json.get("route_ids"), "scope_id": run_json.get("scope_id"),
                      "verified_mbta": ((session or {}).get("scope") or {}).get("verified_mbta")},
            "gtfs_version": gv, "input_hashes": {
                "static_zip_sha256": gv,
                "recording_manifest_sha256": (rec_manifest or {}).get("manifest_sha256") if rec_manifest else None},
            "run_state": run_state, "point_in_time": point_in_time,
            "completeness": completeness, "completeness_reasons": completeness_reasons,
            "test_statuses": _junit_or_report_statuses(paths),
            "budget": {"max_zip_mb": args.max_zip_mb, "included_bytes": used, "skipped": skipped},
            "excluded_by_policy": ["runtime.env and environment dumps", "W/infra (live Kafka/Cassandra files)",
                                   "checkpoints", "tools/caches", "tokens/credentials"],
            "notes": notes, "checksums": checksums,
            "attribution": "MBTA GTFS / GTFS-Realtime data (live/replay runs) is provided by the Massachusetts Bay "
                           "Transportation Authority; synthetic runs contain NO MBTA data.",
        }
        manifest_bytes = json.dumps(manifest, indent=2, sort_keys=True, default=str).encode()
        zf.writestr(_zinfo("manifest.json"), manifest_bytes, compress_type=zipfile.ZIP_DEFLATED)
        zf.writestr(_zinfo("README_REPRODUCE.md"), _readme(manifest).encode(), compress_type=zipfile.ZIP_DEFLATED)
    with open(tmp, "rb") as fh:
        os.fsync(fh.fileno())
    os.replace(tmp, zip_path)
    manifest_path = paths.artifacts / f"{base}.manifest.json"
    manifest_path.write_bytes(manifest_bytes)
    zip_sha = sha256_file(zip_path)
    return Result({"zip_path": str(zip_path), "zip_sha256": zip_sha, "zip_bytes": zip_path.stat().st_size,
                   "manifest_path": str(manifest_path), "run_id": run_id, "session_id": (session or {}).get("session_id"),
                   "source_type": source_type, "run_state": run_state, "point_in_time": point_in_time,
                   "completeness": completeness, "completeness_reasons": completeness_reasons,
                   "file_count": len(checksums), "skipped": skipped[:50], "notes": notes,
                   "test_statuses": manifest["test_statuses"], "commit_sha": git.get("commit"),
                   "commit_reason": git.get("reason")})


def _zinfo(arcname: str) -> zipfile.ZipInfo:
    zi = zipfile.ZipInfo(arcname, date_time=time.gmtime()[:6])
    zi.external_attr = 0o644 << 16  # rw-r--r-- when extracted with unzip
    return zi


def _readme(m: dict[str, Any]) -> str:
    lines = [
        "# TransitPulse export bundle", "",
        f"* exported_at: {m['exported_at']}", f"* commit: {m['commit_sha'] or 'unknown (' + str(m['commit_reason']) + ')'}",
        f"* run_id: {m['run_id']}  source_type: {m['source_type']}  mode: {m['mode']}",
        f"* completeness: {m['completeness']}" + (f" ({'; '.join(m['completeness_reasons'])})"
                                                   if m['completeness_reasons'] else ""),
        "", "## Reproduce (new VM: new run and new checkpoint, spec §26.4)", "",
        "1. Clone the repository at the commit above and run `bash scripts/bootstrap_colab.sh --workdir W`.",
        "2. `infra.py start` and `infra.py init`.",
        "3. Unzip this bundle into `W/data/inputs/<name>/`.",
        "4. `demo.py start --mode replay --recording-manifest W/data/inputs/<name>/inputs/recording/"
        "recording_manifest.json --static-zip W/data/inputs/<name>/inputs/static/<gtfs_version>/MBTA_GTFS.zip "
        "--speed 10 --detach`.",
        "", "Logical results are compared through the oracle with run-specific ids normalized; file hashes of reports",
        "differ between runs because run ids and timestamps differ.", "",
        "Checksums of every file are listed in manifest.json (`checksums`).",
    ]
    return "\n".join(lines) + "\n"


def build_parser() -> JsonArgumentParser:
    p = JsonArgumentParser(prog="export_artifacts.py", description="Package evidence and reproducible inputs as a ZIP.")
    add_common_args(p)
    p.add_argument("--session-id", default=None)
    p.add_argument("--run-id", default=None)
    p.add_argument("--max-zip-mb", type=int, default=200)
    p.add_argument("--log-tail-lines", type=int, default=2000)
    p.add_argument("--no-static-zip", action="store_true", help="do not include the GTFS ZIP (distribution rights)")
    p.add_argument("--no-api-samples", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    return run_main(lambda _a: "export", build_parser(), export, argv)


if __name__ == "__main__":  # python -m transitpulse.runtime.export ...
    import sys

    sys.exit(main())
