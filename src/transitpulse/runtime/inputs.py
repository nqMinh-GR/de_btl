"""Input preparation per demo mode (``demo.py prepare`` and the first half of ``demo.py start``).

* synthetic: ``replay synth-generate --kind demo`` -> static load of the recording's own ZIP.
* replay:    locate the recording's static ZIP (never downloads a current MBTA ZIP), ``replay verify``,
             static load of that exact ZIP. Missing static / checksum mismatch fails clearly (CT11).
* live:      ``static load --download --route-ids`` (verified route ids required; no random fallback).

Every step calls the owning package's CLI (WP1 static, WP3 replay) with the core interpreter and the
canonical environment; nothing is reimplemented here. The prepared record is stored in
``W/data/inputs/prepared/<mode>.json`` and reused by ``start`` when the parameters, the static scope
directory and the Cassandra host id are unchanged.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from transitpulse.runtime.cli_common import (
    EXIT_PREFLIGHT,
    EXIT_UNSUPPORTED,
    EXIT_USAGE,
    CliError,
    Ctx,
    run_json_cli,
)
from transitpulse.runtime.manifest import subprocess_env
from transitpulse.runtime.session import params_key
from transitpulse.runtime.spark_launch import spark_env
from transitpulse.runtime.util import atomic_write_json, iso_now, read_json_or_none, sha256_file

log = logging.getLogger("transitpulse.runtime.inputs")


def prepared_path(ctx: Ctx, mode: str) -> Path:
    return ctx.paths.inputs_root / "prepared" / f"{mode}.json"


def split_routes(text: str | Sequence[str] | None) -> list[str]:
    if text is None:
        return []
    items = text.split(",") if isinstance(text, str) else list(text)
    out: list[str] = []
    for r in items:
        r = str(r).strip()
        if r and r not in out:
            out.append(r)
    return out


def _core_env(ctx: Ctx) -> dict[str, str]:
    rt = ctx.require_runtime()
    return subprocess_env(rt, {"TP_WORKDIR": str(ctx.paths.root), "TP_PROFILE": ctx.profile,
                               "TP_REPO_DIR": str(ctx.repo_dir)})


def synth_generate(ctx: Ctx, *, kind: str, seed: int, duration_seconds: int | None = None) -> dict[str, Any]:
    argv = [ctx.python, "-m", "transitpulse.replay", "synth-generate", "--workdir", str(ctx.paths.root),
            "--kind", kind, "--seed", str(seed)]
    if duration_seconds:
        argv += ["--duration-seconds", str(int(duration_seconds))]
    return run_json_cli(argv, env=_core_env(ctx), cwd=ctx.repo_dir, timeout=900, what=f"synth-generate {kind}")


def static_load(ctx: Ctx, *, route_ids: Sequence[str], zip_path: str | None = None, download: bool = False
                ) -> dict[str, Any]:
    rt = ctx.require_runtime()
    argv = [ctx.python, "-m", "transitpulse.static", "load", "--workdir", str(ctx.paths.root), "--profile",
            ctx.profile]
    argv += ["--download"] if download else ["--zip", str(zip_path)]
    argv += ["--route-ids", ",".join(route_ids)]
    env = {**spark_env(rt, ctx.paths), "TP_WORKDIR": str(ctx.paths.root), "TP_PROFILE": ctx.profile,
           "TP_REPO_DIR": str(ctx.repo_dir)}
    doc = run_json_cli(argv, env=env, cwd=ctx.repo_dir, timeout=1800, what="static load",
                       exit_map={1: EXIT_PREFLIGHT, 2: EXIT_USAGE})
    if doc.get("status") == "FAILED":
        raise CliError("STATIC_LOAD_FAILED", f"static validation FAILED (report {doc.get('report_path')})",
                       hint="inspect the static quality report", exit_code=EXIT_PREFLIGHT)
    return doc


def replay_verify(ctx: Ctx, *, manifest: Path, static_zip: Path | None, route_ids: Sequence[str]) -> dict[str, Any]:
    argv = [ctx.python, "-m", "transitpulse.replay", "verify", "--workdir", str(ctx.paths.root),
            "--manifest", str(manifest)]
    if static_zip is not None:
        argv += ["--static-zip", str(static_zip)]
    if route_ids:
        argv += ["--route-ids", ",".join(route_ids)]
    return run_json_cli(argv, env=_core_env(ctx), cwd=ctx.repo_dir, timeout=600, what="replay verify",
                        exit_map={2: EXIT_PREFLIGHT, 1: EXIT_PREFLIGHT})


def locate_recording_static(ctx: Ctx, manifest_path: Path, manifest: dict[str, Any],
                            explicit: str | None) -> Path | None:
    """--static-zip, else ``<recording_dir>/<static.zip_relpath>``, else ``data/static/<gtfs_version>/MBTA_GTFS.zip``."""
    if explicit:
        p = Path(explicit).expanduser().resolve()
        if not p.is_file():
            raise CliError("STATIC_MISSING", f"--static-zip {p} does not exist", exit_code=EXIT_PREFLIGHT)
        return p
    static = manifest.get("static") or {}
    rel = static.get("zip_relpath")
    if isinstance(rel, str) and rel and not rel.startswith("/") and ".." not in Path(rel).parts:
        cand = (manifest_path.parent / rel).resolve()
        if cand.is_file():
            return cand
    gv = static.get("gtfs_version")
    if isinstance(gv, str) and gv:
        try:
            cand = ctx.paths.static_version_dir(gv) / "MBTA_GTFS.zip"
        except ValueError:
            cand = None
        if cand is not None and cand.is_file():
            return cand
    return None


def prepare_inputs(ctx: Ctx, *, mode: str, seed: int | None, recording_manifest: str | None,
                   static_zip: str | None, route_ids: Sequence[str], speed: float | None, duration_seconds: int,
                   reuse: bool = True, cassandra_host_id: str | None = None) -> dict[str, Any]:
    """Prepare (or reuse) the inputs of a session. Returns the prepared record."""
    manifest_abs = str(Path(recording_manifest).expanduser().resolve()) if recording_manifest else None
    # The key covers what determines the INPUTS only. The replay speed and the session budget do not change
    # them (start may pass --speed while prepare does not); for synthetic data the generated source duration
    # (budget x speed) does.
    source_seconds = int(math.ceil(duration_seconds * float(speed or 1.0))) if mode == "synthetic" else None
    params = {"mode": mode, "seed": seed if mode == "synthetic" else None, "recording_manifest": manifest_abs,
              "static_zip": static_zip, "speed": source_seconds, "route_ids": sorted(route_ids),
              "fault_injection": None}
    key = params_key(params)
    record_path = prepared_path(ctx, mode)
    if reuse:
        prev = read_json_or_none(record_path)
        if isinstance(prev, dict) and prev.get("key") == key and _still_valid(ctx, prev, cassandra_host_id):
            log.info("reusing prepared %s inputs from %s", mode, prev.get("prepared_at"))
            return {**prev, "reused": True}

    if mode == "synthetic":
        rec = synth_generate(ctx, kind="demo", seed=int(seed if seed is not None else ctx.cfg.synthetic.default_seed),
                             duration_seconds=int(math.ceil(duration_seconds * float(speed or 1.0))))
        available = split_routes(rec.get("route_ids"))
        chosen = list(route_ids) or available
        bad = [r for r in chosen if r not in available]
        if bad:
            raise CliError("ROUTES_NOT_IN_RECORDING", f"route ids {bad} are not in the synthetic recording {available}",
                           exit_code=EXIT_USAGE)
        static = static_load(ctx, route_ids=chosen, zip_path=rec["static_zip_path"])
        record = {
            "mode": mode, "source_type": "synthetic", "recording_id": rec.get("recording_id"),
            "recording_dir": rec.get("recording_dir"), "recording_manifest": rec.get("manifest_path"),
            "start_clock": rec.get("start_clock"), "fixture_kind": rec.get("fixture_kind"),
            "seed": int(seed if seed is not None else ctx.cfg.synthetic.default_seed), "speed": float(speed or 1.0),
            "verified_mbta": False, "provenance": "synthetic recording generated by replay synth-generate (NOT MBTA data)",
        }
    elif mode == "replay":
        if not recording_manifest:
            raise CliError("RECORDING_REQUIRED", "--recording-manifest is required for --mode replay",
                           hint="pass the recording_manifest.json of an imported input bundle", exit_code=EXIT_USAGE)
        mpath = Path(recording_manifest).expanduser().resolve()
        manifest = read_json_or_none(mpath)
        if not isinstance(manifest, dict):
            raise CliError("RECORDING_MISSING", f"recording manifest {mpath} is missing or not JSON",
                           hint=f"import the input bundle (export ZIP) into {ctx.paths.inputs_root} and pass its "
                                "inputs/recording/recording_manifest.json",
                           exit_code=EXIT_PREFLIGHT)
        zip_path = locate_recording_static(ctx, mpath, manifest, static_zip)
        if zip_path is None:
            gv = (manifest.get("static") or {}).get("gtfs_version")
            raise CliError(
                "STATIC_MISSING",
                f"the static GTFS ZIP of recording {manifest.get('recording_id')} (gtfs_version {gv}) was not found",
                hint="import the input bundle that contains static/MBTA_GTFS.zip or pass --static-zip. The current "
                     "MBTA_GTFS.zip is never downloaded for a replay because it may be a different version.",
                exit_code=EXIT_PREFLIGHT)
        ver = replay_verify(ctx, manifest=mpath, static_zip=zip_path, route_ids=route_ids)
        chosen = list(route_ids) or split_routes(ver.get("route_ids"))
        static = static_load(ctx, route_ids=chosen, zip_path=str(zip_path))
        if static.get("gtfs_version") != ver.get("gtfs_version"):
            raise CliError("STATIC_VERSION_MISMATCH", f"loaded static {static.get('gtfs_version')} differs from the "
                           f"recording's {ver.get('gtfs_version')}", exit_code=EXIT_PREFLIGHT)
        src = str(manifest.get("source_type") or "live")
        record = {
            "mode": mode, "source_type": "synthetic" if src == "synthetic" else "replay",
            "recording_id": ver.get("recording_id") or manifest.get("recording_id"), "recording_dir": str(mpath.parent),
            "recording_manifest": str(mpath), "start_clock": ver.get("start_clock"),
            "fixture_kind": manifest.get("fixture_kind"), "seed": manifest.get("seed"), "speed": float(speed or 10.0),
            "verified_mbta": src == "live", "item_count": ver.get("item_count"),
            "provenance": f"replay of recording {manifest.get('recording_id')} ({src})",
        }
    elif mode == "live":
        if not route_ids:
            raise CliError("ROUTES_REQUIRED", "--route-ids is required for --mode live (verified bus route ids)",
                           hint=f"run `{ctx.python} -m transitpulse.static list-routes --workdir {ctx.paths.root} "
                                "--download --check-realtime` and choose 5 bus routes; there is no random fallback",
                           exit_code=EXIT_USAGE)
        try:
            static = static_load(ctx, route_ids=route_ids, download=True)
        except CliError as exc:
            raise CliError("SOURCE_FAILURE", f"live static download/load failed: {exc.message}",
                           hint="live mode needs network access to cdn.mbta.com; the demo never switches to "
                                "synthetic data under a LIVE label", exit_code=EXIT_PREFLIGHT,
                           log_excerpt=exc.log_excerpt) from exc
        record = {"mode": mode, "source_type": "live", "recording_id": None, "recording_dir": None,
                  "recording_manifest": None, "start_clock": None, "fixture_kind": None, "seed": None, "speed": None,
                  "verified_mbta": True, "provenance": "live MBTA GTFS-Realtime"}
    else:
        raise CliError("UNSUPPORTED_MODE", f"unknown mode {mode!r}", exit_code=EXIT_UNSUPPORTED)

    verified = split_routes(static.get("verified_route_ids"))
    if not verified:
        raise CliError("NO_VERIFIED_ROUTES", f"none of the requested routes was verified in the static feed "
                       f"(rejected: {static.get('rejected_route_ids')})", exit_code=EXIT_PREFLIGHT)
    input_hashes: dict[str, Any] = {"static_zip_sha256": static.get("gtfs_version")}
    if record.get("recording_manifest"):
        mp = Path(record["recording_manifest"])
        man = read_json_or_none(mp) or {}
        input_hashes["recording_manifest_sha256"] = man.get("manifest_sha256")
        input_hashes["recording_manifest_file_sha256"] = sha256_file(mp) if mp.is_file() else None
    record.update({
        "key": key, "params": params, "prepared_at": iso_now(), "route_ids": verified,
        "rejected_route_ids": static.get("rejected_route_ids") or [], "gtfs_version": static.get("gtfs_version"),
        "scope_id": static.get("scope_id"), "agency_timezone": static.get("agency_timezone"),
        "static_zip_path": static.get("static_zip_path"), "static_scope_dir": static.get("scope_dir"),
        "static_status": static.get("status"), "static_report": static.get("report_path"),
        "input_hashes": input_hashes, "cassandra_host_id": cassandra_host_id, "reused": False,
    })
    atomic_write_json(record_path, record)
    return record


def _still_valid(ctx: Ctx, prev: dict[str, Any], cassandra_host_id: str | None) -> bool:
    from transitpulse.static.dims import scope_dir_is_complete

    scope_dir = prev.get("static_scope_dir")
    if not scope_dir or not scope_dir_is_complete(Path(scope_dir)):
        return False
    zip_path = prev.get("static_zip_path")
    if not zip_path or not Path(zip_path).is_file():
        return False
    if prev.get("recording_manifest") and not Path(prev["recording_manifest"]).is_file():
        return False
    # Static tables live in Cassandra too: a wiped/re-created node has a new host id.
    return bool(cassandra_host_id) and prev.get("cassandra_host_id") == cassandra_host_id
