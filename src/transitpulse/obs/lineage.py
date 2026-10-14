"""End-to-end lineage of one snapshot (design E §6; explanatory evidence only, never part of processing).

``trace(cfg, paths, run_id, feed, snapshot_id=None)`` follows a snapshot through every stage and verifies what can
be verified independently:

1. ``feed_current`` pointer -> ``snapshot_manifest`` (status, capture, Kafka coordinates, first batch, outcome);
2. raw capture file located with RawArchive in the documented root order and verified against ``raw_sha256``;
3. the Kafka message at (topic, partition, offset): assign at that offset, read one message, compare key, headers and
   envelope ``snapshot_id``/``capture_id`` (reports ``NOT_AVAILABLE`` once retention removed it);
4. the micro-batch: ``first_batch_id`` from the manifest, the first ``query_progress.jsonl`` record whose end offset
   covers the message, and the committed curated batch (``_COMMITTED`` listing the snapshot);
5. the API: ``meta.feeds[feed].snapshot_id`` served for the run.

Each stage reports ``OK``, ``MISMATCH``, ``NOT_AVAILABLE`` or ``ERROR`` with details; ``consistent`` is true when no
stage is ``MISMATCH``/``ERROR``. Used by ``scripts/demo.py lineage`` and notebook cell 15.

CLI: ``python -m transitpulse.obs.lineage --workdir W --run-id R --feed vehicle_positions [--snapshot-id S]
[--profile P] [--api-base URL]`` prints one JSON document.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from transitpulse.config import ProjectConfig
from transitpulse.contracts.envelope import iso_utc
from transitpulse.contracts.topics import TOPIC_BY_FEED
from transitpulse.runtime.paths import WorkPaths

FEEDS = ("vehicle_positions", "trip_updates", "alerts")
OK, MISMATCH, NOT_AVAILABLE, ERROR = "OK", "MISMATCH", "NOT_AVAILABLE", "ERROR"


def _jsonable(v: Any) -> Any:
    if isinstance(v, datetime):
        return iso_utc(v if v.tzinfo else v.replace(tzinfo=UTC))
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set, frozenset)):
        return [_jsonable(x) for x in v]
    return v


def _stage(name: str, status: str, **detail: Any) -> dict[str, Any]:
    return {"stage": name, "status": status, **{k: _jsonable(v) for k, v in detail.items()}}


def _raw_roots(paths: WorkPaths, run_id: str) -> list[tuple[str, Path]]:
    """Candidate archive roots of a manifest ``raw_path``, in the documented order (never guessed):

    0. replay/synthetic runs: ``run.provenance["recording_dir"]`` when set (imported replay bundles may live
       anywhere under the work directory; same rule as ``runtime/export.py``);
    1. ``W/data/raw`` — live runs: ``<run_id>/<feed>/<date>/<capture_id>.json.gz`` (RAW_ROOT-relative);
    2. ``W/data/recordings/<recording_id>`` — replay/synthetic runs of a generated recording (no run prefix);
    3. ``W/data/raw/<recording_id>`` — replay of a live recording (relative to the original live run directory).
    """
    run: Any = None
    try:
        from transitpulse.runs import load_run

        run = load_run(paths, run_id)
    except (FileNotFoundError, ValueError, KeyError, OSError):
        run = None
    source_type = getattr(run, "source_type", None)
    recording_id = getattr(run, "recording_id", None)
    provenance = getattr(run, "provenance", None) or {}
    roots: list[tuple[str, Path]] = []
    rec_dir = provenance.get("recording_dir") if isinstance(provenance, dict) else None
    if source_type in ("replay", "synthetic") and isinstance(rec_dir, str) and rec_dir:
        roots.append(("provenance_recording_dir", Path(rec_dir)))
    roots.append(("raw_root", paths.raw_root))
    if recording_id:
        roots.append(("recording_dir", paths.recordings_root / str(recording_id)))
        roots.append(("live_recording_dir", paths.raw_root / str(recording_id)))
    unique: list[tuple[str, Path]] = []
    for name, root in roots:
        if all(root != r for _, r in unique):
            unique.append((name, root))
    return unique


def verify_raw(paths: WorkPaths, run_id: str, raw_path: str | None, raw_sha256: str | None) -> dict[str, Any]:
    """Locate and verify the raw capture with ``RawArchive(root).read_capture(raw_path, expected_sha256=...)``."""
    from transitpulse.ingestion.archive import RawArchive, RawIntegrityError

    if not raw_path:
        return _stage("raw", NOT_AVAILABLE, reason="manifest has no raw_path")
    tried: list[dict[str, str]] = []
    for name, root in _raw_roots(paths, run_id):
        archive = RawArchive(root)
        try:
            body = archive.read_capture(raw_path, expected_sha256=raw_sha256)
        except FileNotFoundError:
            tried.append({"root": name, "path": str(root), "result": "not found"})
            continue
        except RawIntegrityError as exc:
            return _stage("raw", MISMATCH, root=name, root_path=str(root), raw_path=raw_path,
                          expected_sha256=raw_sha256, detail=str(exc)[:500], tried=tried)
        except ValueError as exc:
            tried.append({"root": name, "path": str(root), "result": f"rejected: {exc}"[:200]})
            continue
        stored = archive.resolve(raw_path)
        return _stage("raw", OK, root=name, root_path=str(root), path=str(stored),
                      stored_bytes=stored.stat().st_size, raw_bytes=len(body), expected_sha256=raw_sha256,
                      actual_sha256=hashlib.sha256(body).hexdigest(), tried=tried)
    return _stage("raw", NOT_AVAILABLE, reason="raw_not_found", raw_path=raw_path, tried=tried)


def read_kafka_message(cfg: ProjectConfig, topic: str, partition: int, offset: int, *,
                       timeout: float = 20.0) -> tuple[str, dict[str, Any]]:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": cfg.kafka.bootstrap_servers, "group.id": f"tp-lineage-{uuid.uuid4().hex[:8]}",
                  "enable.auto.commit": False, "auto.offset.reset": "error", "max.partition.fetch.bytes": 8388608})
    try:
        wm = c.get_watermark_offsets(TopicPartition(topic, partition), timeout=timeout, cached=False)
        if wm is None:
            return ERROR, {"reason": "watermark request timed out"}
        low, high = int(wm[0]), int(wm[1])
        if offset < low:
            return NOT_AVAILABLE, {"reason": "removed by Kafka retention", "low_watermark": low, "high_watermark": high}
        if offset >= high:
            return MISMATCH, {"reason": "offset beyond the high watermark", "high_watermark": high}
        c.assign([TopicPartition(topic, partition, offset)])
        deadline = datetime.now(UTC).timestamp() + timeout
        while datetime.now(UTC).timestamp() < deadline:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                return ERROR, {"reason": str(msg.error())}
            if msg.offset() != offset:
                return MISMATCH, {"reason": f"read offset {msg.offset()} instead of {offset}"}
            value = msg.value() or b""
            try:
                env = json.loads(value)
            except ValueError:
                env = None
            return OK, {"key": (msg.key() or b"").decode("utf-8", errors="replace"),
                        "headers": {k: (v or b"").decode("utf-8", errors="replace") for k, v in (msg.headers() or [])},
                        "value_bytes": len(value), "timestamp_ms": msg.timestamp()[1],
                        "envelope": env if isinstance(env, dict) else None}
        return ERROR, {"reason": f"no message read within {timeout}s"}
    finally:
        c.close()


def _progress_batch(paths: WorkPaths, run_id: str, topic: str, partition: int, offset: int) -> dict[str, Any] | None:
    from transitpulse.verify.progress import first_batch_covering

    rec = first_batch_covering(paths.query_progress_log(run_id), topic, partition, offset)
    if rec is None:
        return None
    return {"batch_id": rec.batch_id, "timestamp": rec.timestamp, "num_input_rows": rec.num_input_rows,
            "end_offset": rec.end_offsets.get((topic, partition))}


def _curated_commit(paths: WorkPaths, cfg: ProjectConfig, run_id: str, batch_id: int | None,
                    snapshot_id: str) -> dict[str, Any] | None:
    if batch_id is None:
        return None
    marker = (paths.curated_run_dir(run_id) / f"query={cfg.spark.query_name}" / f"batch_id={int(batch_id):012d}"
              / "_COMMITTED")
    if not marker.is_file():
        return {"committed": False, "marker": str(marker)}
    try:
        doc = json.loads(marker.read_text(encoding="utf-8"))
    except ValueError:
        doc = {}
    sids = doc.get("snapshot_ids") if isinstance(doc, dict) else None
    return {"committed": True, "marker": str(marker),
            "lists_snapshot": (snapshot_id in sids) if isinstance(sids, list) else None}


def _api_snapshot(api_base: str, run_id: str, feed: str, timeout: float = 10.0) -> tuple[str, dict[str, Any]]:
    import httpx

    path = {"vehicle_positions": "vehicles", "alerts": "alerts"}.get(feed, "quality")
    try:
        r = httpx.get(f"{api_base.rstrip('/')}/{path}", params={"run_id": run_id}, timeout=timeout, trust_env=False)
    except httpx.HTTPError as exc:
        return NOT_AVAILABLE, {"reason": f"API unreachable: {exc}"}
    try:
        body = r.json()
    except ValueError:
        return ERROR, {"http_status": r.status_code, "reason": "non-JSON response"}
    feeds = (body.get("meta") or {}).get("feeds") or {} if isinstance(body, dict) else {}
    block = feeds.get(feed) if isinstance(feeds, dict) else None
    return OK, {"http_status": r.status_code, "endpoint": path,
                "snapshot_id": (block or {}).get("snapshot_id"), "state": (block or {}).get("state")}


def trace(cfg: ProjectConfig | None = None, paths: WorkPaths | None = None, run_id: str | None = None,
          feed: str | None = None, snapshot_id: str | None = None, *, api_base: str | None = None,
          kafka_timeout: float = 20.0, workdir: str | None = None, feed_type: str | None = None,
          profile: str | None = None) -> dict[str, Any]:
    """Follow one snapshot end to end; returns ``{ok, consistent, run_id, feed, snapshot_id, stages: [...]}``.

    ``paths`` may be given as ``workdir`` and ``feed`` as ``feed_type``; ``cfg`` defaults to the profile config of
    that work directory (keyword aliases used by ``scripts/demo.py lineage``).
    """
    from transitpulse.config import load_config
    from transitpulse.storage.cassandra import get_session
    from transitpulse.storage.manifest import read_manifest
    from transitpulse.storage.pointer import read_pointer

    if paths is None:
        if workdir is None:
            raise ValueError("trace() needs paths or workdir")
        paths = WorkPaths.from_workdir(workdir)
    if cfg is None:
        cfg = load_config(profile, workdir=paths.root)
    feed = feed or feed_type
    if not run_id:
        raise ValueError("trace() needs run_id")
    if feed not in FEEDS:
        raise ValueError(f"feed must be one of {FEEDS}, got {feed!r}")
    stages: list[dict[str, Any]] = []
    session = get_session(cfg)
    pointer = read_pointer(session, run_id, feed)
    if snapshot_id is None:
        if pointer is None:
            stages.append(_stage("pointer", NOT_AVAILABLE, reason="no feed_current row for this run/feed"))
            return {"ok": False, "consistent": False, "run_id": run_id, "feed": feed, "snapshot_id": None,
                    "stages": stages}
        snapshot_id = str(pointer["snapshot_id"])
    stages.append(_stage(
        "pointer", OK if pointer else NOT_AVAILABLE,
        current_snapshot_id=(pointer or {}).get("snapshot_id"), is_current=bool(pointer)
        and pointer.get("snapshot_id") == snapshot_id, generation_id=(pointer or {}).get("generation_id"),
        view_published_at=(pointer or {}).get("view_published_at"), expires_at=(pointer or {}).get("expires_at")))
    man = read_manifest(session, run_id, feed, snapshot_id)
    if man is None:
        stages.append(_stage("manifest", NOT_AVAILABLE, reason="no snapshot_manifest row"))
        return {"ok": False, "consistent": False, "run_id": run_id, "feed": feed, "snapshot_id": snapshot_id,
                "stages": stages}
    stages.append(_stage("manifest", OK if man.get("status") == "READY" else MISMATCH,
                         manifest_status=man.get("status"), publish_outcome=man.get("publish_outcome"),
                         capture_id=man.get("capture_id"), raw_path=man.get("raw_path"),
                         kafka_topic=man.get("kafka_topic"), kafka_partition=man.get("kafka_partition"),
                         kafka_offset=man.get("kafka_offset"), first_batch_id=man.get("first_batch_id"),
                         published_at=man.get("published_at"), ready_at=man.get("ready_at"),
                         first_visible_at=man.get("first_visible_at"),
                         visibility_latency_ms=man.get("visibility_latency_ms"),
                         generation_id=man.get("generation_id")))
    stages.append(verify_raw(paths, run_id, man.get("raw_path"), man.get("raw_sha256")))
    topic = man.get("kafka_topic") or TOPIC_BY_FEED[feed]
    partition, offset = man.get("kafka_partition"), man.get("kafka_offset")
    if isinstance(partition, int) and isinstance(offset, int):
        try:
            status, info = read_kafka_message(cfg, topic, partition, offset, timeout=kafka_timeout)
        except Exception as exc:  # noqa: BLE001 - reported, lineage is explanatory
            status, info = ERROR, {"reason": f"{type(exc).__name__}: {exc}"}
        if status == OK:
            env = info.get("envelope") or {}
            want_key = f"{run_id}|{feed}"
            mismatches = []
            if info.get("key") != want_key:
                mismatches.append(f"key {info.get('key')!r} != {want_key!r}")
            if env.get("snapshot_id") != snapshot_id:
                mismatches.append(f"envelope snapshot_id {env.get('snapshot_id')!r}")
            if info.get("headers", {}).get("snapshot_id") not in (None, snapshot_id):
                mismatches.append("header snapshot_id differs")
            if man.get("capture_id") and env.get("capture_id") != man.get("capture_id"):
                mismatches.append(f"envelope capture_id {env.get('capture_id')!r} != manifest {man.get('capture_id')!r}")
            info = {"topic": topic, "partition": partition, "offset": offset, "key": info.get("key"),
                    "headers": info.get("headers"), "value_bytes": info.get("value_bytes"),
                    "envelope_snapshot_id": env.get("snapshot_id"), "envelope_capture_id": env.get("capture_id"),
                    "published_at": env.get("published_at"), "mismatches": mismatches}
            status = MISMATCH if mismatches else OK
        else:
            info = {"topic": topic, "partition": partition, "offset": offset, **info}
        stages.append(_stage("kafka", status, **info))
        prog = _progress_batch(paths, run_id, topic, partition, offset)
    else:
        stages.append(_stage("kafka", NOT_AVAILABLE, reason="manifest has no Kafka coordinates"))
        prog = None
    first_batch = man.get("first_batch_id")
    curated = _curated_commit(paths, cfg, run_id, first_batch, snapshot_id)
    batch_status = OK
    if prog is None and first_batch is None:
        batch_status = NOT_AVAILABLE
    elif prog is not None and first_batch is not None and prog.get("batch_id") is not None \
            and int(prog["batch_id"]) > int(first_batch):
        batch_status = MISMATCH
    stages.append(_stage("batch", batch_status, first_batch_id=first_batch, progress=prog, curated=curated,
                         note="first_batch_id is frozen at the first build; a duplicate delivery is processed in a "
                              "later batch"))
    if api_base:
        status, info = _api_snapshot(api_base, run_id, feed)
        if status == OK and pointer and info.get("snapshot_id") != pointer.get("snapshot_id"):
            status = MISMATCH
        stages.append(_stage("api", status, expected_current=(pointer or {}).get("snapshot_id"), **info))
    else:
        stages.append(_stage("api", NOT_AVAILABLE, reason="no API base URL given"))
    consistent = all(s["status"] not in (MISMATCH, ERROR) for s in stages)
    return {"ok": consistent, "consistent": consistent, "run_id": run_id, "feed": feed, "snapshot_id": snapshot_id,
            "traced_at": iso_utc(datetime.now(UTC)), "stages": stages}


def main(argv: list[str] | None = None) -> int:
    from transitpulse.config import load_config

    ap = argparse.ArgumentParser(prog="python -m transitpulse.obs.lineage", description="Trace one snapshot end to end")
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--feed", default="vehicle_positions", choices=FEEDS)
    ap.add_argument("--snapshot-id", default=None)
    ap.add_argument("--profile", default=None)
    ap.add_argument("--api-base", default=None)
    args = ap.parse_args(argv)
    paths = WorkPaths.from_workdir(args.workdir)
    try:
        cfg = load_config(args.profile, workdir=paths.root)
        api = args.api_base or cfg.ui.api_base_url
        result = trace(cfg, paths, args.run_id, args.feed, args.snapshot_id, api_base=api)
        result.update(action="lineage", error=None)
        code = 0 if result["ok"] else 1
    except Exception as exc:  # noqa: BLE001 - one JSON document with the error
        result = {"ok": False, "action": "lineage", "error": {"code": type(exc).__name__, "message": str(exc)[:2000],
                                                               "hint": "is Cassandra running (scripts/infra.py status)?",
                                                               "log_excerpt": None}}
        code = 1
    sys.stdout.write(json.dumps(result, default=str) + "\n")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
