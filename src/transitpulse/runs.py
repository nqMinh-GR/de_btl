"""Run records (interfaces §2, PDF p9/p30, spec §6.1).

A run is the isolation unit of the whole pipeline: Kafka keys, checkpoints, raw/outbox/curated
namespaces and every serving row are keyed by ``run_id``. A run pins exactly one GTFS version, one
scope and one policy at creation time; nothing changes them afterwards.

Creation order (each step only after the previous one succeeded):
  1. ``run_metadata`` via ``INSERT ... IF NOT EXISTS`` with ``run_state='CREATED'``, ``clock_sequence=0``
     (never null: LWT conditions such as ``IF clock_sequence < ?`` are false on null), and
     ``application_clock = initial_clock`` (null for wall-clock runs);
  2. ``runs_by_project`` (plain INSERT, listing index);
  3. ``W/runs/<run_id>/run.json`` written atomically (tmp + fsync + ``os.replace``).

Re-running :func:`create_run` with the same explicit ``run_id`` and the same identity (project, scope,
source type, GTFS version, routes, clock mode) is idempotent: the LWT is not applied, the existing
row's ``created_at`` and pinned policy are reused and the remaining steps are repeated. A different
identity raises :class:`RunConflictError`.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from transitpulse.config import ProjectConfig
from transitpulse.contracts.enums import ClockMode, RunState, SourceType
from transitpulse.contracts.envelope import iso_utc, parse_iso_utc, utcnow
from transitpulse.contracts.ids import new_run_id
from transitpulse.contracts.policy import Policy
from transitpulse.obs.logging import get_logger, log_event
from transitpulse.runtime.paths import WorkPaths, safe_component

RUN_JSON_SCHEMA_VERSION = 1

_log = get_logger("transitpulse.runs")
_CQL_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,47}$")

_RUN_METADATA_COLUMNS: tuple[str, ...] = (
    "run_id", "project_id", "scope_id", "source_type", "clock_mode", "gtfs_version", "agency_timezone",
    "route_ids", "pipeline_version", "policy_version", "policy_json", "fixture_kind", "scenario_id", "seed",
    "recording_id", "session_id", "speed", "label", "provenance_json", "created_at", "run_state",
    "application_clock", "clock_sequence",
)
_RUNS_BY_PROJECT_COLUMNS: tuple[str, ...] = (
    "project_id", "created_at", "run_id", "source_type", "scope_id", "gtfs_version", "scenario_id", "label",
)
# Identity columns compared when an INSERT IF NOT EXISTS finds an existing row.
_IDENTITY_FIELDS: tuple[str, ...] = (
    "project_id", "scope_id", "source_type", "clock_mode", "gtfs_version", "pipeline_version",
)


class RunConflictError(RuntimeError):
    """A run with this run_id already exists with a different identity (CLI exit code 4)."""

    def __init__(self, run_id: str, differences: dict[str, tuple[Any, Any]]) -> None:
        self.run_id = run_id
        self.differences = differences
        detail = ", ".join(f"{k}: existing={v[0]!r} requested={v[1]!r}" for k, v in sorted(differences.items()))
        super().__init__(f"run {run_id} already exists with a different identity ({detail})")


# --------------------------------------------------------------------------------------------------
# Record
# --------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class RunRecord:
    run_id: str
    project_id: str
    scope_id: str
    source_type: str
    clock_mode: str
    gtfs_version: str
    agency_timezone: str
    route_ids: tuple[str, ...]
    pipeline_version: str
    policy: Policy                       # pinned at creation
    created_at: datetime
    initial_clock: datetime | None
    static_zip_path: str                 # absolute path of the immutable ZIP copy
    static_scope_dir: str                # absolute path of <STATIC_ROOT>/<gtfs_version>/<scope_id>
    scenario_id: str | None = None
    fixture_kind: str | None = None
    seed: int | None = None
    recording_id: str | None = None
    session_id: str | None = None
    speed: float | None = None
    label: str | None = None
    provenance: dict = field(default_factory=dict)

    @property
    def is_source_clock(self) -> bool:
        return self.clock_mode == ClockMode.SOURCE.value

    def to_json(self) -> dict:
        return {
            "schema_version": RUN_JSON_SCHEMA_VERSION,
            "run_id": self.run_id,
            "project_id": self.project_id,
            "scope_id": self.scope_id,
            "source_type": self.source_type,
            "clock_mode": self.clock_mode,
            "gtfs_version": self.gtfs_version,
            "agency_timezone": self.agency_timezone,
            "route_ids": list(self.route_ids),
            "pipeline_version": self.pipeline_version,
            "policy": asdict(self.policy),
            "created_at": iso_utc(self.created_at),
            "initial_clock": iso_utc(self.initial_clock) if self.initial_clock is not None else None,
            "static_zip_path": self.static_zip_path,
            "static_scope_dir": self.static_scope_dir,
            "scenario_id": self.scenario_id,
            "fixture_kind": self.fixture_kind,
            "seed": self.seed,
            "recording_id": self.recording_id,
            "session_id": self.session_id,
            "speed": self.speed,
            "label": self.label,
            "provenance": dict(self.provenance),
        }

    @classmethod
    def from_json(cls, d: dict) -> RunRecord:
        if not isinstance(d, Mapping):
            raise ValueError("run record must be a JSON object")
        version = d.get("schema_version", RUN_JSON_SCHEMA_VERSION)
        if version != RUN_JSON_SCHEMA_VERSION:
            raise ValueError(f"unsupported run.json schema_version {version!r}")
        policy_raw = d["policy"]
        if isinstance(policy_raw, str):
            policy = Policy.from_json(policy_raw)
        elif isinstance(policy_raw, Mapping):
            policy = Policy(**dict(policy_raw))
        else:
            raise ValueError("run record 'policy' must be an object or a JSON string")
        provenance_raw = d.get("provenance") or {}
        if isinstance(provenance_raw, str):
            provenance_raw = json.loads(provenance_raw)
        initial_raw = d.get("initial_clock")
        seed_raw = d.get("seed")
        speed_raw = d.get("speed")
        return cls(
            run_id=str(d["run_id"]),
            project_id=str(d["project_id"]),
            scope_id=str(d["scope_id"]),
            source_type=str(d["source_type"]),
            clock_mode=str(d["clock_mode"]),
            gtfs_version=str(d["gtfs_version"]),
            agency_timezone=str(d["agency_timezone"]),
            route_ids=tuple(str(r) for r in d.get("route_ids") or ()),
            pipeline_version=str(d["pipeline_version"]),
            policy=policy,
            created_at=parse_iso_utc(d["created_at"]),
            initial_clock=parse_iso_utc(initial_raw) if initial_raw else None,
            static_zip_path=str(d["static_zip_path"]),
            static_scope_dir=str(d["static_scope_dir"]),
            scenario_id=d.get("scenario_id"),
            fixture_kind=d.get("fixture_kind"),
            seed=int(seed_raw) if seed_raw is not None else None,
            recording_id=d.get("recording_id"),
            session_id=d.get("session_id"),
            speed=float(speed_raw) if speed_raw is not None else None,
            label=d.get("label"),
            provenance=dict(provenance_raw),
        )


# --------------------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------------------

def _truncate_ms(dt: datetime) -> datetime:
    """Cassandra timestamps have millisecond precision; keep run.json identical to the stored row."""
    if dt.tzinfo is None:
        raise ValueError("naive datetime is not allowed; use timezone-aware UTC datetimes")
    dt = dt.astimezone(UTC)
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)


def _as_aware_utc(value: Any) -> datetime | None:
    """Cassandra returns naive UTC datetimes; normalize to aware UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, str):
        return parse_iso_utc(value)
    raise TypeError(f"unexpected timestamp value {value!r}")


def _atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    try:
        dfd = os.open(path.parent, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dfd)
    except OSError:
        pass
    finally:
        os.close(dfd)


def _row_to_dict(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    if isinstance(row, Mapping):
        return dict(row)
    if hasattr(row, "_asdict"):
        return dict(row._asdict())
    raise TypeError(f"unsupported Cassandra row type {type(row).__name__}; use dict_factory or named tuples")


def _lwt_outcome(result: Any) -> tuple[bool, dict[str, Any] | None]:
    """(applied, row) of an LWT result for dict_factory / named_tuple_factory sessions."""
    first = result.one() if hasattr(result, "one") else None
    row = _row_to_dict(first)
    applied: bool | None = None
    try:
        applied = bool(result.was_applied)
    except (AttributeError, RuntimeError):
        applied = None
    if applied is None:
        if row is None or "[applied]" not in row:
            raise RuntimeError("cannot determine the LWT outcome of the run_metadata insert")
        applied = bool(row["[applied]"])
    return applied, row


def _keyspace(cfg: ProjectConfig) -> str:
    ks = cfg.cassandra.keyspace
    if not _CQL_IDENT.match(ks):
        raise ValueError(f"invalid Cassandra keyspace name {ks!r}")
    return ks


def _insert_cql(keyspace: str, table: str, columns: Sequence[str], *, if_not_exists: bool) -> str:
    cols = ", ".join(columns)
    marks = ", ".join("?" for _ in columns)
    suffix = " IF NOT EXISTS" if if_not_exists else ""
    return f"INSERT INTO {keyspace}.{table} ({cols}) VALUES ({marks}){suffix}"


def _validate_route_ids(route_ids: Sequence[str]) -> tuple[str, ...]:
    if isinstance(route_ids, str):
        raise TypeError("route_ids must be a sequence of route ids, not a string")
    out: list[str] = []
    for r in route_ids:
        if not isinstance(r, str) or not r.strip():
            raise ValueError(f"invalid route id {r!r}")
        if r not in out:
            out.append(r)
    return tuple(out)


# --------------------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------------------

def create_run(
    cfg: ProjectConfig,
    session: Any,
    *,
    source_type: str,
    route_ids: Sequence[str],
    gtfs_version: str,
    scope_id: str,
    agency_timezone: str,
    static_zip_path: str,
    static_scope_dir: str,
    initial_clock: datetime | None = None,
    scenario_id: str | None = None,
    fixture_kind: str | None = None,
    seed: int | None = None,
    recording_id: str | None = None,
    session_id: str | None = None,
    speed: float | None = None,
    label: str | None = None,
    provenance: dict | None = None,
    run_id: str | None = None,
) -> RunRecord:
    """Create (or idempotently re-create) a run: run_metadata LWT -> runs_by_project -> run.json."""
    try:
        src = SourceType(source_type).value
    except ValueError as exc:
        raise ValueError(f"source_type must be one of {[s.value for s in SourceType]}, got {source_type!r}") from exc
    routes = _validate_route_ids(route_ids)
    if src == SourceType.LIVE.value and not routes:
        raise ValueError("a live run requires a non-empty list of verified route_ids (no random fallback)")
    clock_mode = ClockMode.WALL.value if src == SourceType.LIVE.value else ClockMode.SOURCE.value
    if clock_mode == ClockMode.SOURCE.value:
        if initial_clock is None:
            raise ValueError(f"a {src} run uses the source clock and requires initial_clock")
        initial = _truncate_ms(initial_clock)
    else:
        if initial_clock is not None:
            log_event(_log, "run_create", "skipped", "initial_clock ignored for a wall-clock (live) run",
                      level=logging.WARNING, initial_clock=iso_utc(initial_clock))
        initial = None
    if not gtfs_version or not scope_id:
        raise ValueError("gtfs_version and scope_id are required")
    if not agency_timezone:
        raise ValueError("agency_timezone is required (read from agency.txt by the static loader)")
    if speed is not None and not speed > 0:
        raise ValueError("speed must be > 0")
    rid = safe_component(run_id, "run_id") if run_id is not None else new_run_id()
    paths = WorkPaths.from_workdir(cfg.workdir)
    if session is None:
        from transitpulse.storage.cassandra import get_session  # WP4; imported lazily

        session = get_session(cfg)

    policy = Policy.from_config(cfg)
    created_at = _truncate_ms(utcnow())
    prov = dict(provenance or {})
    ks = _keyspace(cfg)

    values: dict[str, Any] = {
        "run_id": rid,
        "project_id": cfg.project.project_id,
        "scope_id": scope_id,
        "source_type": src,
        "clock_mode": clock_mode,
        "gtfs_version": gtfs_version,
        "agency_timezone": agency_timezone,
        "route_ids": list(routes),
        "pipeline_version": cfg.project.pipeline_version,
        "policy_version": policy.policy_version,
        "policy_json": policy.to_json(),
        "fixture_kind": fixture_kind,
        "scenario_id": scenario_id,
        "seed": int(seed) if seed is not None else None,
        "recording_id": recording_id,
        "session_id": session_id,
        "speed": float(speed) if speed is not None else None,
        "label": label,
        "provenance_json": json.dumps(prov, sort_keys=True, ensure_ascii=False) if prov else None,
        "created_at": created_at,
        "run_state": RunState.CREATED.value,
        "application_clock": initial,
        "clock_sequence": 0,
    }

    # 1. run_metadata (LWT bootstrap)
    stmt = session.prepare(_insert_cql(ks, "run_metadata", _RUN_METADATA_COLUMNS, if_not_exists=True))
    applied, existing = _lwt_outcome(session.execute(stmt, tuple(values[c] for c in _RUN_METADATA_COLUMNS)))
    if not applied:
        if existing is None:
            raise RuntimeError(f"run_metadata insert for {rid} was not applied and returned no row")
        differences: dict[str, tuple[Any, Any]] = {}
        for name in _IDENTITY_FIELDS:
            if existing.get(name) != values[name]:
                differences[name] = (existing.get(name), values[name])
        if list(existing.get("route_ids") or []) != list(routes):
            differences["route_ids"] = (list(existing.get("route_ids") or []), list(routes))
        if differences:
            log_event(_log, "run_create", "failed", "run_id already used by a different run",
                      level=logging.ERROR, run_id=rid, differences=list(differences))
            raise RunConflictError(rid, differences)
        created_at = _as_aware_utc(existing.get("created_at")) or created_at
        if existing.get("policy_json"):
            policy = Policy.from_json(existing["policy_json"])
        values["created_at"] = created_at
        log_event(_log, "run_create", "reused", "run_metadata already present with the same identity", run_id=rid)

    # 2. runs_by_project (listing index; idempotent upsert on the same key)
    stmt2 = session.prepare(_insert_cql(ks, "runs_by_project", _RUNS_BY_PROJECT_COLUMNS, if_not_exists=False))
    session.execute(stmt2, tuple(values[c] for c in _RUNS_BY_PROJECT_COLUMNS))

    record = RunRecord(
        run_id=rid,
        project_id=cfg.project.project_id,
        scope_id=scope_id,
        source_type=src,
        clock_mode=clock_mode,
        gtfs_version=gtfs_version,
        agency_timezone=agency_timezone,
        route_ids=routes,
        pipeline_version=cfg.project.pipeline_version,
        policy=policy,
        created_at=created_at,
        initial_clock=initial,
        static_zip_path=str(static_zip_path),
        static_scope_dir=str(static_scope_dir),
        scenario_id=scenario_id,
        fixture_kind=fixture_kind,
        seed=int(seed) if seed is not None else None,
        recording_id=recording_id,
        session_id=session_id,
        speed=float(speed) if speed is not None else None,
        label=label,
        provenance=prov,
    )
    # 3. run.json (atomic)
    _atomic_write_json(paths.run_json(rid), record.to_json())
    log_event(_log, "run_create", "ok", "run created", run_id=rid, source_type=src, clock_mode=clock_mode,
              scope_id=scope_id, gtfs_version=gtfs_version, route_ids=list(routes))
    return record


def load_run(paths: WorkPaths, run_id: str) -> RunRecord:
    """Read ``W/runs/<run_id>/run.json`` (FileNotFoundError when the run was never created locally)."""
    path = paths.run_json(run_id)
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    record = RunRecord.from_json(data)
    if record.run_id != run_id:
        raise ValueError(f"{path} contains run_id {record.run_id!r}, expected {run_id!r}")
    return record


def list_local_runs(paths: WorkPaths) -> list[RunRecord]:
    """All readable local run records, newest first (unreadable files are logged and skipped)."""
    runs_dir = paths.root / "runs"
    if not runs_dir.is_dir():
        return []
    out: list[RunRecord] = []
    for run_json in sorted(runs_dir.glob("*/run.json")):
        try:
            with open(run_json, encoding="utf-8") as fh:
                out.append(RunRecord.from_json(json.load(fh)))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            log_event(_log, "run_list", "skipped", f"unreadable run record {run_json}: {exc}",
                      level=logging.WARNING)
    out.sort(key=lambda r: (r.created_at, r.run_id), reverse=True)
    return out
