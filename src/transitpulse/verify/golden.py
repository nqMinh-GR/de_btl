"""Golden / scenario verification of ``tests/fixtures/golden/expected.json`` against the API and Cassandra.

``check_step(api_base, run_id, step_expect, labels)`` compares one step (``{"step", "clock", "inputs", "expect"}``
or only its ``expect`` mapping) with what ``/api/v1`` serves for ``run_id``; ``cassandra_check_step`` compares the
same step with the stored rows. Fixture labels (VP1, TU1, ALERTS2, ...) are mapped to run-specific snapshot ids
through ``labels`` (``{label: snapshot_id}``; values may also be ``{"snapshot_id": ...}``).

Conventions of expected.json (its ``conventions`` block): instants are ISO-8601 UTC with second precision and are
compared as parsed instants (the API uses milliseconds); numbers use absolute tolerance 1e-9; ``null`` is a value
compared strictly; ``vehicle_metrics`` is the serving view recomputed at the step clock while
``vehicle_generation_metrics`` is what Spark stored for the pointer generation; ``predictions`` and ``history`` are
complete per stop / vehicle; ``alerts_by_route`` lists exactly the served alerts.

Every expectation key has a handler in each leg or an explicit reason why that leg cannot see it (``skipped``);
an unknown key is ``unsupported`` and fails the step: an expectation that cannot be checked is never reported as
passed. ``deliveries``, ``lineage_totals`` (per-feed counters) and ``dlq`` are verified by the harness
(:mod:`transitpulse.verify.flows`), which observes state before and after the step and reads the DLQ topic.

Used by ``scripts/demo.py golden-smoke`` and the integration tests IT01-IT09.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

FLOAT_TOL = 1e-9
AGE_TOL = 1e-6
FEEDS = ("vehicle_positions", "trip_updates", "alerts")
LINEAGE_REASONS = frozenset({"DUPLICATE_SNAPSHOT_DELIVERY", "LATE_SNAPSHOT_NOT_PUBLISHED", "EXPIRED_NOT_PUBLISHED",
                             "PARENT_UNAVAILABLE"})
INFORMATIONAL_KEYS = frozenset({"note", "notes", "comment", "description", "inputs"})
HARNESS_KEYS = {
    "deliveries": "per-delivery lineage needs the state before the step; verified by verify.flows",
    "lineage_totals": "pipeline counters and generation totals are verified from Cassandra by verify.flows",
    "dlq": "DLQ is a Kafka topic (the API never reads Kafka); verified by verify.flows from mbta.dlq.v1",
}

COUNT_COLUMNS = {
    "fresh": "fresh_vehicle_count", "stale": "stale_vehicle_count", "unknown": "unknown_age_vehicle_count",
    "invalid": "invalid_vehicle_count", "membership": "snapshot_membership_count",
    "inherited": "inherited_vehicle_count",
}
PREDICTION_FIELDS = {
    "trip_instances": "trip_instance_count", "eligible": "eligible_trip_count", "late": "predicted_late_trip_count",
    "rate": "predicted_late_rate", "mean": "mean_predicted_delay_seconds", "excluded": "excluded_by_reason",
    "excluded_total": "excluded_trip_count", "unknown_uncertainty": "unknown_uncertainty_count", "stale": "stale",
}
STORED_PREDICTION_COLUMNS = {
    "trip_instances": "trip_instance_count", "eligible": "eligible_trip_count", "late": "predicted_late_trip_count",
    "rate": "predicted_late_rate", "mean": "mean_predicted_delay_seconds", "excluded": "excluded_trip_counts",
}
VEHICLE_FIELDS = {
    "route_id": "route_id", "trip_id": "trip_id", "class": "freshness_status", "age_s": "observation_age_seconds",
    "record_kind": "record_kind", "observation_time": "observation_time", "event_time": "event_time",
    "latitude": "latitude", "longitude": "longitude", "invalid_reason": "invalid_reason", "flags": "quality_flags",
}
STORED_VEHICLE_FIELDS = ("route_id", "trip_id", "record_kind", "observation_time", "event_time", "latitude",
                         "longitude", "invalid_reason")
PREDICTION_ROW_FIELDS = {
    "route_id": "route_id", "predicted_at": "predicted_time", "scheduled_at": "scheduled_time",
    "delay_s": "predicted_delay_seconds", "uncertainty": "uncertainty_seconds",
    "uncertainty_class": "prediction_quality", "schedule_resolution": "schedule_resolution",
}
STORED_PREDICTION_ROW_FIELDS = {
    "route_id": "route_id", "predicted_at": "predicted_at", "scheduled_at": "scheduled_at",
    "delay_s": "predicted_delay_seconds", "uncertainty": "uncertainty", "uncertainty_class": "prediction_quality",
    "schedule_resolution": "schedule_resolution",
}
TARGET_FIELDS = {
    "route_id": "route_id", "trip_relationship": "trip_schedule_relationship",
    "schedule_resolution": "schedule_resolution", "resolution_method": "resolution_method",
    "service_date": "service_date", "stop_sequence": "stop_sequence", "stop_id": "stop_id",
    "delay_s": "predicted_delay_seconds", "uncertainty_class": "prediction_quality",
}


# --------------------------------------------------------------------------------------------------
# Fixture files
# --------------------------------------------------------------------------------------------------

def default_golden_dir() -> Path:
    from transitpulse.config import repo_root

    return repo_root() / "tests" / "fixtures" / "golden"


def load_expected(golden_dir: Path | None = None) -> dict[str, Any]:
    """``expected.json`` of the golden fixture (hand-written, never generated from the implementation)."""
    path = Path(golden_dir or default_golden_dir()) / "expected.json"
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict) or not isinstance(data.get("runs"), dict):
        raise ValueError(f"{path}: expected an object with a 'runs' mapping")
    return data


def load_timeline(golden_dir: Path | None = None) -> dict[str, Any]:
    path = Path(golden_dir or default_golden_dir()) / "timeline.json"
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict) or not isinstance(data.get("runs"), dict):
        raise ValueError(f"{path}: expected an object with a 'runs' mapping")
    return data


def resolve_run_key(runs: Mapping[str, Any], wanted: str) -> str | None:
    """Exact, then case-insensitive, then prefix match of a run key."""
    if wanted in runs:
        return wanted
    low = wanted.lower()
    for key in runs:
        if key.lower() == low:
            return key
    for key in runs:
        if key.lower().startswith(low):
            return key
    return None


def expected_steps(expected: Mapping[str, Any], run_key: str) -> list[dict[str, Any]]:
    key = resolve_run_key(expected.get("runs", {}), run_key)
    if key is None:
        return []
    run = expected["runs"][key]
    steps = run.get("steps") if isinstance(run, dict) else None
    return [s for s in steps or [] if isinstance(s, dict)]


def find_step(expected: Mapping[str, Any], run_key: str, step_name: str) -> dict[str, Any] | None:
    for s in expected_steps(expected, run_key):
        if s.get("step") == step_name:
            return s
    return None


# --------------------------------------------------------------------------------------------------
# Labels
# --------------------------------------------------------------------------------------------------

def normalize_labels(labels: Mapping[str, Any] | None) -> dict[str, str]:
    """``{label: snapshot_id}`` from ``{label: sid}`` or ``{label: {"snapshot_id": sid, ...}}``."""
    out: dict[str, str] = {}
    for label, value in (labels or {}).items():
        if isinstance(value, str):
            out[label] = value
        elif isinstance(value, Mapping) and isinstance(value.get("snapshot_id"), str):
            out[label] = value["snapshot_id"]
    return out


def invert_labels(labels: Mapping[str, str]) -> dict[str, str]:
    return {sid: label for label, sid in labels.items()}


def labels_from_run_file(paths: Any, run_id: str) -> dict[str, str]:
    """``W/runs/<run_id>/labels.json`` written by the replay publisher for synthetic runs
    (``{"captures": {...}, "snapshots": {label: snapshot_id}}``); ``{}`` when absent."""
    path = Path(paths.run_dir(run_id)) / "labels.json"
    try:
        with open(path, encoding="utf-8") as fh:
            doc = json.load(fh)
    except (OSError, ValueError):
        return {}
    snaps = doc.get("snapshots") if isinstance(doc, dict) else None
    return normalize_labels(snaps if isinstance(snaps, Mapping) else {})


def labels_from_manifest_rows(rows: Iterable[Mapping[str, Any]], capture_labels: Mapping[str, Any]) -> dict[str, str]:
    """Map labels to snapshot ids through ``capture_id`` (``labels.json`` gives ``{label: {capture_id, ...}}``)."""
    by_capture: dict[str, str] = {}
    for row in rows:
        cid, sid = row.get("capture_id"), row.get("snapshot_id")
        if isinstance(cid, str) and isinstance(sid, str):
            by_capture[cid] = sid
    out: dict[str, str] = {}
    for label, info in capture_labels.items():
        cid = info.get("capture_id") if isinstance(info, Mapping) else info
        if isinstance(cid, str) and cid in by_capture:
            out[label] = by_capture[cid]
    return out


# --------------------------------------------------------------------------------------------------
# Values
# --------------------------------------------------------------------------------------------------

def parse_instant(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str) and _looks_like_instant(value):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    return None


def _looks_like_instant(value: Any) -> bool:
    return isinstance(value, str) and len(value) >= 19 and value[4:5] == "-" and value[10:11] == "T"


def values_equal(expected: Any, actual: Any, tol: float = FLOAT_TOL) -> bool:
    """Strict null; numbers within ``tol``; ISO instants as parsed instants; mappings/lists recursively."""
    if expected is None or actual is None:
        return expected is None and actual is None
    if isinstance(expected, bool) or isinstance(actual, bool):
        return isinstance(expected, bool) and isinstance(actual, bool) and expected == actual
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        if isinstance(expected, float) and math.isnan(expected):
            return isinstance(actual, float) and math.isnan(actual)
        return abs(float(expected) - float(actual)) <= tol
    if _looks_like_instant(expected) or isinstance(expected, datetime):
        a, b = parse_instant(expected), parse_instant(actual)
        if a is not None and b is not None:
            return abs((a - b).total_seconds()) < 0.0005
    if isinstance(expected, Mapping) and isinstance(actual, Mapping):
        return set(expected) == set(actual) and all(values_equal(expected[k], actual[k], tol) for k in expected)
    if isinstance(expected, (list, tuple)) and isinstance(actual, (list, tuple)):
        return len(expected) == len(actual) and all(
            values_equal(e, a, tol) for e, a in zip(expected, actual, strict=True))
    return expected == actual


def diff_paths(expected: Any, actual: Any, path: str = "", *, limit: int = 40) -> list[str]:
    """Human-readable list of differing paths (used for oracle == expected and evidence)."""
    out: list[str] = []

    def walk(e: Any, a: Any, p: str) -> None:
        if len(out) >= limit:
            return
        if isinstance(e, Mapping) and isinstance(a, Mapping):
            for k in sorted(set(e) | set(a), key=str):
                if k not in a:
                    out.append(f"{p}.{k}: missing in actual")
                elif k not in e:
                    out.append(f"{p}.{k}: unexpected in actual")
                else:
                    walk(e[k], a[k], f"{p}.{k}")
        elif isinstance(e, list) and isinstance(a, list) and len(e) == len(a):
            for i, (x, y) in enumerate(zip(e, a, strict=True)):
                walk(x, y, f"{p}[{i}]")
        elif not values_equal(e, a):
            out.append(f"{p}: expected {e!r}, got {a!r}")

    walk(expected, actual, path or "$")
    return out


def _count_map(value: Any) -> dict[str, int]:
    """Exclusion/quality count maps: missing/None -> {}, zero entries dropped."""
    if not isinstance(value, Mapping):
        return {}
    out: dict[str, int] = {}
    for k, v in value.items():
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v != 0:
            out[str(k)] = int(v)
    return out


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (set, frozenset)):
        return sorted(_jsonable(v) for v in value)
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _norm(value: Any) -> Any:
    """Comparable form: enums lower-cased for event types, datetimes aware, sets sorted lists."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    return value


class _Checks:
    def __init__(self) -> None:
        self.checks: list[dict[str, Any]] = []
        self.unsupported: list[dict[str, Any]] = []
        self.skipped: list[dict[str, Any]] = []

    def add(self, name: str, expected: Any, actual: Any, passed: bool, source: str, **extra: Any) -> None:
        item = {"name": name, "expected": expected, "actual": actual, "passed": bool(passed), "source": source}
        item.update(extra)
        self.checks.append(item)

    def cmp(self, name: str, expected: Any, actual: Any, source: str, *, tol: float = FLOAT_TOL) -> None:
        self.add(name, expected, actual, values_equal(expected, _norm(actual), tol), source)

    def skip(self, key: str, reason: str) -> None:
        self.skipped.append({"key": key, "reason": reason})

    def unsupported_key(self, key: str, reason: str) -> None:
        self.unsupported.append({"key": key, "reason": reason})

    def result(self, run_id: str, step: str | None, *, require_checks: bool) -> dict[str, Any]:
        passed = all(c["passed"] for c in self.checks) and not self.unsupported and (
            bool(self.checks) or not require_checks)
        return {
            "passed": passed, "step": step, "run_id": run_id,
            "checked_at": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "checks": [_jsonable(c) for c in self.checks], "unsupported": self.unsupported, "skipped": self.skipped,
        }


def _split_step(step_expect: Mapping[str, Any]) -> tuple[Mapping[str, Any], str | None, str | None]:
    if isinstance(step_expect.get("expect"), Mapping):
        return step_expect["expect"], step_expect.get("clock"), step_expect.get("step")
    return step_expect, step_expect.get("clock") if isinstance(step_expect.get("clock"), str) else None, None


# --------------------------------------------------------------------------------------------------
# API leg
# --------------------------------------------------------------------------------------------------

class _Resp:
    def __init__(self, status: int | None, body: Any, error: str | None = None) -> None:
        self.status = status
        self.body = body
        self.error = error

    @property
    def ok(self) -> bool:
        return self.status == 200 and isinstance(self.body, Mapping)

    @property
    def data(self) -> Any:
        return self.body.get("data") if isinstance(self.body, Mapping) else None

    @property
    def meta(self) -> Mapping[str, Any]:
        m = self.body.get("meta") if isinstance(self.body, Mapping) else None
        return m if isinstance(m, Mapping) else {}

    @property
    def error_code(self) -> str | None:
        if isinstance(self.body, Mapping) and isinstance(self.body.get("error"), Mapping):
            return self.body["error"].get("code")
        return None

    def describe(self) -> str:
        if self.status is None:
            return f"request failed: {self.error}"
        return f"HTTP {self.status}" + (f" {self.error_code}" if self.error_code else "")


class _Api:
    def __init__(self, base: str, run_id: str, client: httpx.Client | None, timeout: float) -> None:
        self.base = base.rstrip("/")
        self.run_id = run_id
        self._own = client is None
        self.client = client or httpx.Client(timeout=timeout, trust_env=False)
        self._cache: dict[tuple[str, tuple[tuple[str, str], ...]], _Resp] = {}

    def close(self) -> None:
        if self._own:
            self.client.close()

    def get(self, path: str, *, run_scoped: bool = True, **params: Any) -> _Resp:
        q = {k: v for k, v in params.items() if v is not None}
        if run_scoped:
            q.setdefault("run_id", self.run_id)
        key = (path, tuple(sorted((k, str(v)) for k, v in q.items())))
        if key in self._cache:
            return self._cache[key]
        try:
            r = self.client.get(f"{self.base}/{path.lstrip('/')}", params=q)
            try:
                body = r.json()
            except ValueError:
                body = None
            resp = _Resp(r.status_code, body)
        except httpx.HTTPError as exc:
            resp = _Resp(None, None, f"{type(exc).__name__}: {exc}")
        self._cache[key] = resp
        return resp

    def run(self) -> _Resp:
        return self.get(f"runs/{self.run_id}", run_scoped=False)

    def scope_routes(self) -> list[str]:
        r = self.run()
        if r.ok and isinstance(r.data, Mapping) and isinstance(r.data.get("route_ids"), list):
            return [x for x in r.data["route_ids"] if isinstance(x, str)]
        cov = r.meta.get("coverage") if r.ok else None
        if isinstance(cov, Mapping) and isinstance(cov.get("route_ids"), list):
            return [x for x in cov["route_ids"] if isinstance(x, str)]
        return []

    def generation(self, feed: str) -> tuple[Mapping[str, Any] | None, str]:
        """FeedMeta of the feed pointer: /runs/{id} data.generations, else meta.feeds of /runs or /quality."""
        r = self.run()
        if r.ok:
            gens = r.data.get("generations") if isinstance(r.data, Mapping) else None
            if isinstance(gens, Mapping) and feed in gens:
                block = gens.get(feed)
                return (block if isinstance(block, Mapping) else None), "runs/{run_id} data.generations"
            feeds = r.meta.get("feeds")
            if isinstance(feeds, Mapping) and isinstance(feeds.get(feed), Mapping):
                return feeds[feed], "runs/{run_id} meta.feeds"
        q = self.get("quality")
        feeds = q.meta.get("feeds") if q.body is not None else None
        if isinstance(feeds, Mapping) and isinstance(feeds.get(feed), Mapping):
            return feeds[feed], "quality meta.feeds"
        return None, f"no generation block ({r.describe()})"

    def quality_feed(self, feed: str) -> tuple[Mapping[str, Any] | None, _Resp]:
        q = self.get("quality")
        feeds = q.data.get("feeds") if q.ok and isinstance(q.data, Mapping) else None
        block = feeds.get(feed) if isinstance(feeds, Mapping) else None
        return (block if isinstance(block, Mapping) else None), q

    def route_metrics(self, route: str) -> tuple[_Resp, Mapping[str, Any]]:
        r = self.get(f"routes/{route}/metrics")
        return r, (r.data if r.ok and isinstance(r.data, Mapping) else {})


def _served_snapshot(block: Mapping[str, Any] | None) -> str | None:
    if not block:
        return None
    sid = block.get("snapshot_id")
    return sid if isinstance(sid, str) else None


def _reason_counts(block: Mapping[str, Any] | None) -> dict[str, int] | None:
    if block is None:
        return None
    reasons = block.get("reasons")
    out: dict[str, int] = {}
    if isinstance(reasons, list):
        for r in reasons:
            if isinstance(r, Mapping) and isinstance(r.get("reason_code"), str):
                cnt = r.get("count", r.get("record_count"))
                out[r["reason_code"]] = out.get(r["reason_code"], 0) + (int(cnt) if isinstance(cnt, int) else 0)
    return out


def _a_pointer(api: _Api, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    for feed, label in (exp or {}).items():
        block, src = api.generation(feed)
        sid = _served_snapshot(block)
        exp_sid = None if label is None else ctx["labels"].get(label, f"<unmapped label {label}>")
        out.add(f"pointer[{feed}]", label, ctx["inv"].get(sid, sid) if sid else None, sid == exp_sid, src)


def _a_clock(api: _Api, clock: str, out: _Checks) -> None:
    r = api.run()
    actual = r.meta.get("application_clock") if r.ok else r.describe()
    if r.ok and actual is None and isinstance(r.data, Mapping):
        actual = r.data.get("application_clock")
    out.cmp("application_clock", clock, actual, "runs/{run_id}")


def _a_feeds(api: _Api, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    for feed, spec in (exp or {}).items():
        block, src = api.generation(feed)
        if spec is None:
            sid = _served_snapshot(block)
            out.add(f"feeds[{feed}]", None, {"snapshot_id": sid, "state": (block or {}).get("state")},
                    sid is None, src)
            continue
        qblock, qresp = api.quality_feed(feed)
        for key, value in spec.items():
            name = f"feeds[{feed}].{key}"
            if key == "label":
                sid = _served_snapshot(block)
                out.add(name, value, ctx["inv"].get(sid, sid) if sid else None,
                        sid is not None and sid == ctx["labels"].get(value), src)
            elif key in ("feed_timestamp", "metrics_as_of", "is_partial", "is_empty", "stale"):
                out.cmp(name, value, (block or {}).get(key) if block else f"<{src}>", src)
            elif key == "age_s":
                out.cmp(name, value, (block or {}).get("source_age_seconds") if block else f"<{src}>", src, tol=AGE_TOL)
            elif key in ("source_entity_count", "rejected_entity_count", "selected_entity_count"):
                out.cmp(name, value, qblock.get(key) if qblock else qresp.describe(), "quality data.feeds")
            else:
                out.unsupported_key(name, "unknown feed field")


def _a_vehicle_metrics(api: _Api, exp: Any, ctx: dict[str, Any], out: _Checks, *, stored: bool) -> None:
    key = "vehicle_generation_metrics" if stored else "vehicle_metrics"
    routes = list(exp) if isinstance(exp, Mapping) else api.scope_routes()
    for route in routes:
        spec = exp.get(route) if isinstance(exp, Mapping) else None
        resp, data = api.route_metrics(route)
        vm = data.get("vehicle_metrics") if isinstance(data.get("vehicle_metrics"), Mapping) else None
        has_generation = vm is not None and vm.get("snapshot_id") is not None
        block = None
        if has_generation:
            raw = vm.get("stored_counts_at_as_of") if stored else vm.get("counts")
            block = raw if isinstance(raw, Mapping) else None
        src = f"routes/{route}/metrics vehicle_metrics" + (".stored_counts_at_as_of" if stored else ".counts")
        if spec is None:
            out.add(f"{key}[{route}]", None, dict(block) if block is not None else None, not has_generation, src)
            continue
        if not resp.ok:
            out.add(f"{key}[{route}]", spec, resp.describe(), False, src)
            continue
        for ck, cv in spec.items():
            col = COUNT_COLUMNS.get(ck)
            if col is None:
                out.unsupported_key(f"{key}[{route}].{ck}", "unknown count key")
                continue
            actual = block.get(col) if isinstance(block, Mapping) else "vehicle_metrics block is null"
            out.cmp(f"{key}[{route}].{ck}", cv, actual, src)


def _vehicle_rows(api: _Api) -> tuple[_Resp, dict[str, Mapping[str, Any]] | None]:
    resp = api.get("vehicles", limit=2000)
    rows = resp.data.get("vehicles") if resp.ok and isinstance(resp.data, Mapping) else None
    if not isinstance(rows, list):
        return resp, None
    return resp, {r["vehicle_id"]: r for r in rows if isinstance(r, Mapping) and isinstance(r.get("vehicle_id"), str)}


def _a_vehicles(api: _Api, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    resp, by_id = _vehicle_rows(api)
    if exp is None:
        none_served = by_id == {} or (by_id is None and resp.error_code == "generation_unavailable")
        out.add("vehicles", None, sorted(by_id) if by_id else resp.describe(), bool(none_served), "vehicles")
        return
    if by_id is None:
        out.add("vehicles", sorted(exp), resp.describe(), False, "vehicles")
        return
    out.add("vehicles ids", sorted(exp), sorted(by_id), set(exp) == set(by_id), "vehicles")
    for vid, spec in exp.items():
        row = by_id.get(vid)
        if row is None or not isinstance(spec, Mapping):
            continue
        for key, value in spec.items():
            field = VEHICLE_FIELDS.get(key)
            name = f"vehicles[{vid}].{key}"
            if field is None:
                out.unsupported_key(name, "unknown vehicle field")
            elif key == "flags":
                have = sorted(row.get(field) or [])
                out.add(name, sorted(value or []), have, sorted(value or []) == have, "vehicles")
            else:
                out.cmp(name, value, row.get(field), "vehicles", tol=AGE_TOL if key == "age_s" else FLOAT_TOL)


def _a_reason_map(api: _Api, feed: str, key: str, exp: Any, out: _Checks) -> None:
    block, q = api.quality_feed(feed)
    counts = _reason_counts(block)
    if exp is None:
        out.add(key, None, counts, block is None, "quality data.feeds")
        return
    if counts is None:
        out.add(key, exp, q.describe() if not q.ok else "feed quality block is null", False, "quality data.feeds")
        return
    for code, n in exp.items():
        out.cmp(f"{key}.{code}", n, counts.get(code, 0), "quality data.feeds")
    extra = sorted(c for c in counts if c not in exp and c not in LINEAGE_REASONS and counts[c])
    out.add(f"{key} (no unexpected reasons)", [], extra, not extra, "quality data.feeds")


def _history_window(clock: str | None) -> tuple[str | None, str | None]:
    c = parse_instant(clock)
    if c is None:
        return None, None
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return (c - timedelta(hours=2)).strftime(fmt), (c + timedelta(minutes=10)).strftime(fmt)


def _a_history(api: _Api, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    clock = ctx.get("clock")
    if clock is None:
        # only the ``expect`` mapping was given: use the run's application clock (the API default window is the
        # half-open last hour [clock - 1 h, clock), which would hide a state sample taken exactly at the clock)
        r = api.run()
        clock = r.meta.get("application_clock") if r.ok else None
    frm, to = _history_window(clock)
    for vid, spec in (exp or {}).items():
        rows: list[Mapping[str, Any]] = []
        token: str | None = None
        problem = None
        for _ in range(50):
            r = api.get(f"vehicles/{vid}/history", page_size=500, page_token=token, **{"from": frm, "to": to})
            if not r.ok:
                problem = r.describe()
                break
            rows.extend(x for x in (r.data.get("observations") or []) if isinstance(x, Mapping))
            token = r.data.get("next_page_token")
            if not token:
                break
        src = f"vehicles/{vid}/history"
        if problem:
            out.add(f"history[{vid}]", spec, problem, False, src)
            continue
        times = sorted(t for t in (parse_instant(x.get("event_time")) for x in rows) if t is not None)
        if isinstance(spec, Mapping):
            if "rows" in spec:
                out.cmp(f"history[{vid}].rows", spec["rows"], len(rows), src)
            if "event_times" in spec:
                want = sorted(t for t in (parse_instant(x) for x in spec["event_times"]) if t is not None)
                out.add(f"history[{vid}].event_times", [_jsonable(t) for t in want], [_jsonable(t) for t in times],
                        want == times, src)
        oids = [x.get("observation_id") for x in rows]
        out.add(f"history[{vid}] distinct observation_id", len(oids), len(set(oids)), len(oids) == len(set(oids)), src)


def _a_prediction_metrics(api: _Api, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    routes = list(exp) if isinstance(exp, Mapping) else api.scope_routes()
    for route in routes:
        spec = exp.get(route) if isinstance(exp, Mapping) else None
        resp, data = api.route_metrics(route)
        pm = data.get("prediction_metrics") if isinstance(data.get("prediction_metrics"), Mapping) else None
        src = f"routes/{route}/metrics prediction_metrics"
        if spec is None:
            out.add(f"prediction_metrics[{route}]", None, pm and pm.get("snapshot_id"),
                    pm is None or pm.get("snapshot_id") is None, src)
            continue
        if pm is None:
            out.add(f"prediction_metrics[{route}]", spec, resp.describe() if not resp.ok else None, False, src)
            continue
        for key, value in spec.items():
            field = PREDICTION_FIELDS.get(key)
            name = f"prediction_metrics[{route}].{key}"
            if field is None:
                out.unsupported_key(name, "unknown prediction metric")
            elif key == "excluded":
                out.add(name, _count_map(value), _count_map(pm.get(field)),
                        _count_map(value) == _count_map(pm.get(field)), src)
            else:
                out.cmp(name, value, pm.get(field), src)


def _stop_predictions(api: _Api, stop_id: str) -> tuple[_Resp, list[Mapping[str, Any]] | None, datetime | None]:
    r = api.get(f"stops/{stop_id}/predictions", include="all", limit=200)
    if not r.ok or not isinstance(r.data, Mapping):
        return r, None, None
    rows = [x for x in (r.data.get("predictions") or []) if isinstance(x, Mapping)]
    return r, rows, parse_instant(r.data.get("window_start"))


def _pkey(trip: Any, seq: Any, event: Any) -> tuple[Any, Any, str]:
    return trip, seq, str(event or "").lower()


def _a_predictions(api: _Api, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    if exp is None:
        block, src = api.generation("trip_updates")
        out.add("predictions", None, _served_snapshot(block), _served_snapshot(block) is None, src)
        return
    by_stop: dict[str, list[Mapping[str, Any]]] = {}
    for p in exp:
        by_stop.setdefault(str(p.get("stop_id")), []).append(p)
    for stop, items in sorted(by_stop.items()):
        resp, rows, window_start = _stop_predictions(api, stop)
        src = f"stops/{stop}/predictions"
        if rows is None:
            out.add(f"predictions[{stop}]", len(items), resp.describe(), False, src)
            continue
        visible = [p for p in items if window_start is None
                   or (parse_instant(p.get("predicted_at")) or window_start) >= window_start]
        hidden = len(items) - len(visible)
        if hidden:
            out.skip(f"predictions[{stop}]", f"{hidden} expected prediction(s) before the API window start "
                                             f"{_jsonable(window_start)} (served window = clock - 60 s)")
        actual = {_pkey(r.get("trip_id"), r.get("stop_sequence"), r.get("event_type")): r for r in rows}
        want = {_pkey(p.get("trip_id"), p.get("stop_sequence"), p.get("event_type")): p for p in visible}
        out.add(f"predictions[{stop}] keys", sorted(map(str, want)), sorted(map(str, actual)),
                set(want) == set(actual), src)
        for k, p in want.items():
            row = actual.get(k)
            if row is None:
                continue
            label = f"predictions[{stop}][{k[0]}/{k[1]}/{k[2]}]"
            for key, field in PREDICTION_ROW_FIELDS.items():
                if key in p:
                    out.cmp(f"{label}.{key}", p[key], row.get(field), src)


def _a_targets(api: _Api, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    if exp is None:
        block, src = api.generation("trip_updates")
        out.add("targets", None, _served_snapshot(block), _served_snapshot(block) is None, src)
        return
    late_s = ctx.get("late_threshold", 300)
    for trip, spec in exp.items():
        stop = spec.get("stop_id")
        if stop is None:
            out.skip(f"targets[{trip}]", f"no target stop ({spec.get('kpi_reason')}); covered by "
                                         "prediction_metrics.excluded")
            continue
        resp, rows, _ = _stop_predictions(api, stop)
        src = f"stops/{stop}/predictions"
        if rows is None:
            out.add(f"targets[{trip}]", spec, resp.describe(), False, src)
            continue
        cands = [r for r in rows if r.get("trip_id") == trip and r.get("stop_sequence") == spec.get("stop_sequence")
                 and str(r.get("event_type") or "").lower() == str(spec.get("event_type") or "").lower()]
        if not cands:
            out.add(f"targets[{trip}].present", True, False, False, src)
            continue
        row = cands[0]
        for key, value in spec.items():
            name = f"targets[{trip}].{key}"
            if key == "event_type":
                out.cmp(name, str(value).lower(), str(row.get("event_type") or "").lower(), src)
            elif key in TARGET_FIELDS:
                out.cmp(name, value, row.get(TARGET_FIELDS[key]), src)
            elif key == "kpi_reason":
                out.add(f"targets[{trip}].kpi_eligible", value is None, row.get("kpi_eligible"),
                        row.get("kpi_eligible") is (value is None), src)
                if value is None:
                    out.add(f"targets[{trip}].is_target", True, row.get("is_target"), row.get("is_target") is True,
                            src)
            elif key == "late":
                delay = row.get("predicted_delay_seconds")
                actual = (delay >= late_s) if (row.get("kpi_eligible") is True and isinstance(delay, int)) else None
                out.cmp(name, value, actual, src)
            else:
                out.unsupported_key(name, "unknown target field")


def _alerts(api: _Api, route: str | None) -> tuple[_Resp, dict[str, Mapping[str, Any]] | None]:
    r = api.get("alerts", route_id=route)
    rows = r.data.get("alerts") if r.ok and isinstance(r.data, Mapping) else None
    if not isinstance(rows, list):
        return r, None
    return r, {a["alert_id"]: a for a in rows if isinstance(a, Mapping) and isinstance(a.get("alert_id"), str)}


def _a_alerts_by_route(api: _Api, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    if exp is None:
        r, rows = _alerts(api, None)
        out.add("alerts_by_route", None, sorted(rows) if rows else r.describe(), not rows, "alerts")
        return
    for route, alerts in exp.items():
        r, rows = _alerts(api, route)
        src = f"alerts?route_id={route}"
        if rows is None:
            out.add(f"alerts_by_route[{route}]", sorted(alerts or {}), r.describe(), False, src)
            continue
        out.add(f"alerts_by_route[{route}] ids", sorted(alerts or {}), sorted(rows), set(alerts or {}) == set(rows), src)
        for aid, spec in (alerts or {}).items():
            row = rows.get(aid)
            if row is None or not isinstance(spec, Mapping):
                continue
            for key, value in spec.items():
                name = f"alerts_by_route[{route}][{aid}].{key}"
                if key == "state":
                    out.cmp(name, value, row.get("active_state_at_clock"), src)
                elif key == "confirmation":
                    out.cmp(name, value, row.get("confirmation_status"), src)
                elif key in ("inherited_from", "first_seen"):
                    sid = row.get("inherited_from_snapshot_id" if key == "inherited_from" else "first_seen_snapshot_id")
                    want = None if value is None else ctx["labels"].get(value, f"<unmapped label {value}>")
                    out.add(name, value, ctx["inv"].get(sid, sid) if sid else None, sid == want, src)
                else:
                    out.unsupported_key(name, "unknown alert field")


def _a_selector_resolution(api: _Api, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    if exp is None:
        return
    merged: dict[str, Mapping[str, Any]] = {}
    for route in api.scope_routes() or [None]:
        _, rows = _alerts(api, route)
        merged.update(rows or {})
    for aid, value in exp.items():
        row = merged.get(aid)
        if row is None:
            out.skip(f"alert_selector_resolution[{aid}]", "alert applies to no scope route; the API serves only "
                                                         "applied alerts (selector quality is checked via alert_quality)")
            continue
        out.cmp(f"alert_selector_resolution[{aid}]", value, row.get("selector_resolution"), "alerts")


def _a_alert_quality(api: _Api, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    block, q = api.quality_feed("alerts")
    counts = _reason_counts(block)
    if exp is None:
        out.add("alert_quality", None, counts, block is None, "quality data.feeds.alerts")
        return
    for aid, codes in exp.items():
        for code in codes if isinstance(codes, list) else [codes]:
            actual = (counts or {}).get(code, 0) if counts is not None else q.describe()
            out.add(f"alert_quality[{aid}].{code}", ">= 1", actual, isinstance(actual, int) and actual >= 1,
                    "quality data.feeds.alerts")


def _a_alerts_closed(api: _Api, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    if not exp:
        return
    served: set[str] = set()
    for route in api.scope_routes():
        _, rows = _alerts(api, route)
        served |= set(rows or {})
    for aid in exp:
        out.add(f"alerts_closed[{aid}] not served", False, aid in served, aid not in served, "alerts")
    out.skip("alerts_closed details", "closed_reason/closed_at are kept in curated alerts, not served by the API")


_API_HANDLERS = {
    "pointer": _a_pointer, "feeds": _a_feeds, "vehicles": _a_vehicles, "history": _a_history,
    "prediction_metrics": _a_prediction_metrics, "predictions": _a_predictions, "targets": _a_targets,
    "alerts_by_route": _a_alerts_by_route, "alert_selector_resolution": _a_selector_resolution,
    "alert_quality": _a_alert_quality, "alerts_closed": _a_alerts_closed,
}


def check_step(
    api_base: str,
    run_id: str,
    step_expect: dict,
    labels: dict,
    *,
    timeout: float = 10.0,
    client: httpx.Client | None = None,
    late_threshold_seconds: int = 300,
) -> dict:
    """Compare one expected step with the API; returns ``{passed, checks: [{name, expected, actual, passed}], ...}``."""
    exp, clock, step_name = _split_step(step_expect)
    lab = normalize_labels(labels)
    ctx: dict[str, Any] = {"labels": lab, "inv": invert_labels(lab), "clock": clock,
                           "late_threshold": late_threshold_seconds}
    out = _Checks()
    api = _Api(api_base, run_id, client, timeout)
    try:
        if clock is not None:
            _a_clock(api, clock, out)
        for key, value in exp.items():
            if key in INFORMATIONAL_KEYS:
                continue
            if key in HARNESS_KEYS:
                out.skip(key, HARNESS_KEYS[key])
            elif key == "vehicle_metrics":
                _a_vehicle_metrics(api, value, ctx, out, stored=False)
            elif key == "vehicle_generation_metrics":
                _a_vehicle_metrics(api, value, ctx, out, stored=True)
            elif key == "vehicle_quality":
                _a_reason_map(api, "vehicle_positions", "vehicle_quality", value, out)
            elif key in _API_HANDLERS:
                _API_HANDLERS[key](api, value, ctx, out)
            else:
                out.unsupported_key(key, "no API checker for this expectation key")
    finally:
        api.close()
    return out.result(run_id, step_name, require_checks=True)


# --------------------------------------------------------------------------------------------------
# Cassandra leg
# --------------------------------------------------------------------------------------------------

def _aware(dt: Any) -> Any:
    if isinstance(dt, datetime) and dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def _rows(session: Any, cql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    out = []
    for row in session.execute(cql, params):
        d = row if isinstance(row, dict) else row._asdict()
        out.append({k: _aware(v) for k, v in d.items()})
    return out


class _Store:
    def __init__(self, session: Any, run_id: str) -> None:
        self.s = session
        self.run_id = run_id
        self._ptr: dict[str, dict[str, Any] | None] = {}
        self._man: dict[tuple[str, str], dict[str, Any] | None] = {}

    def pointer(self, feed: str) -> dict[str, Any] | None:
        if feed not in self._ptr:
            rows = _rows(self.s, "SELECT * FROM feed_current WHERE run_id = %s AND feed_type = %s", (self.run_id, feed))
            self._ptr[feed] = rows[0] if rows else None
        return self._ptr[feed]

    def manifest(self, feed: str, sid: str) -> dict[str, Any] | None:
        if (feed, sid) not in self._man:
            rows = _rows(self.s, "SELECT * FROM snapshot_manifest WHERE run_id = %s AND feed_type = %s AND "
                                 "snapshot_id = %s", (self.run_id, feed, sid))
            self._man[(feed, sid)] = rows[0] if rows else None
        return self._man[(feed, sid)]

    def current_sid(self, feed: str) -> str | None:
        p = self.pointer(feed)
        return p.get("snapshot_id") if p else None

    def route_metrics(self, feed: str, sid: str, route: str) -> dict[str, Any] | None:
        rows = _rows(self.s, "SELECT * FROM route_metrics_by_snapshot WHERE run_id = %s AND feed_type = %s AND "
                             "snapshot_id = %s AND route_id = %s", (self.run_id, feed, sid, route))
        return rows[0] if rows else None

    def by_partition(self, table: str, sid: str, key_col: str, key: str) -> list[dict[str, Any]]:
        return _rows(self.s, f"SELECT * FROM {table} WHERE run_id = %s AND snapshot_id = %s AND {key_col} = %s",
                     (self.run_id, sid, key))

    def quality(self, feed: str, sid: str) -> dict[str, dict[str, Any]]:
        rows = _rows(self.s, "SELECT * FROM quality_by_snapshot WHERE run_id = %s AND feed_type = %s AND "
                             "snapshot_id = %s", (self.run_id, feed, sid))
        return {r["reason_code"]: r for r in rows}

    def manifests(self, feed: str) -> list[dict[str, Any]]:
        return _rows(self.s, "SELECT snapshot_id, status FROM snapshot_manifest WHERE run_id = %s AND feed_type = %s",
                     (self.run_id, feed))

    def run_routes(self) -> list[str]:
        rows = _rows(self.s, "SELECT route_ids FROM run_metadata WHERE run_id = %s", (self.run_id,))
        return list(rows[0].get("route_ids") or []) if rows else []


def _c_pointer(st: _Store, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    for feed, label in (exp or {}).items():
        sid = st.current_sid(feed)
        want = None if label is None else ctx["labels"].get(label, f"<unmapped label {label}>")
        out.add(f"feed_current[{feed}]", label, ctx["inv"].get(sid, sid) if sid else None, sid == want,
                "cassandra feed_current")
        if sid:
            man = st.manifest(feed, sid) or {}
            out.add(f"snapshot_manifest[{feed}].status", "READY", man.get("status"), man.get("status") == "READY",
                    "cassandra snapshot_manifest")


def _c_feeds(st: _Store, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    for feed, spec in (exp or {}).items():
        sid = st.current_sid(feed)
        if spec is None:
            out.add(f"feeds[{feed}]", None, sid, sid is None, "cassandra feed_current")
            continue
        man = st.manifest(feed, sid) if sid else None
        for key, value in spec.items():
            name = f"snapshot_manifest[{feed}].{key}"
            if key == "label":
                out.add(name, value, ctx["inv"].get(sid, sid) if sid else None, sid == ctx["labels"].get(value),
                        "cassandra feed_current")
            elif key in ("feed_timestamp", "metrics_as_of", "is_partial", "is_empty", "source_entity_count",
                         "rejected_entity_count"):
                out.cmp(name, value, (man or {}).get(key), "cassandra snapshot_manifest")
            elif key in ("age_s", "stale"):
                continue
            else:
                out.unsupported_key(name, "unknown feed field")
    out.skip("feeds.age_s/stale", "serving-time values (computed by the API at the step clock)")


def _c_generation_metrics(st: _Store, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    sid = st.current_sid("vehicle_positions")
    routes = list(exp) if isinstance(exp, Mapping) else st.run_routes()
    for route in routes:
        spec = exp.get(route) if isinstance(exp, Mapping) else None
        row = st.route_metrics("vehicle_positions", sid, route) if sid else None
        src = "cassandra route_metrics_by_snapshot"
        if spec is None:
            out.add(f"vehicle_generation_metrics[{route}]", None, row is not None, row is None, src)
            continue
        vrows = st.by_partition("vehicles_by_route_snapshot", sid, "route_id", route) if sid else []
        for ck, cv in spec.items():
            col = COUNT_COLUMNS.get(ck)
            if col is None:
                out.unsupported_key(f"vehicle_generation_metrics[{route}].{ck}", "unknown count key")
                continue
            out.cmp(f"route_metrics_by_snapshot[{route}].{col}", cv, (row or {}).get(col), src)
            if ck == "membership":
                ids = {r.get("vehicle_id") for r in vrows if r.get("vehicle_id")}
                out.cmp(f"vehicles_by_route_snapshot[{route}] distinct vehicle_id", cv, len(ids),
                        "cassandra vehicles_by_route_snapshot")


def _c_vehicles(st: _Store, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    sid = st.current_sid("vehicle_positions")
    if exp is None:
        out.add("vehicles", None, sid, sid is None, "cassandra feed_current")
        return
    if sid is None:
        out.add("vehicles", sorted(exp), None, False, "cassandra feed_current")
        return
    stored: dict[str, dict[str, Any]] = {}
    for route in st.run_routes():
        for r in st.by_partition("vehicles_by_route_snapshot", sid, "route_id", route):
            stored[r["vehicle_id"]] = r
    out.add("vehicles_by_route_snapshot ids", sorted(exp), sorted(stored), set(exp) == set(stored),
            "cassandra vehicles_by_route_snapshot")
    for vid, spec in exp.items():
        row = stored.get(vid)
        if row is None or not isinstance(spec, Mapping):
            continue
        for key in STORED_VEHICLE_FIELDS:
            if key in spec:
                out.cmp(f"vehicles_by_route_snapshot[{vid}].{key}", spec[key], row.get(key),
                        "cassandra vehicles_by_route_snapshot")
    out.skip("vehicles.class/age_s/flags", "freshness is recomputed by the API at the request clock")


def _c_vehicle_quality(st: _Store, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    sid = st.current_sid("vehicle_positions")
    if exp is None:
        out.add("vehicle_quality", None, sid, sid is None, "cassandra feed_current")
        return
    q = st.quality("vehicle_positions", sid) if sid else {}
    for code, n in exp.items():
        out.cmp(f"quality_by_snapshot[vehicle_positions].{code}", n, (q.get(code) or {}).get("record_count", 0),
                "cassandra quality_by_snapshot")


def _c_history(st: _Store, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    clock = parse_instant(ctx.get("clock"))
    for vid, spec in (exp or {}).items():
        if not isinstance(spec, Mapping):
            continue
        want = sorted(t for t in (parse_instant(x) for x in spec.get("event_times") or []) if t is not None)
        days = sorted({t.date() for t in want} | ({clock.date()} if clock else set()))
        rows: list[dict[str, Any]] = []
        for d in days:
            rows += _rows(st.s, "SELECT event_time, observation_id FROM vehicle_observations_by_day WHERE run_id = %s "
                                "AND vehicle_id = %s AND utc_date = %s", (st.run_id, vid, d))
        have = sorted(r["event_time"] for r in rows)
        src = "cassandra vehicle_observations_by_day"
        if "rows" in spec:
            out.cmp(f"vehicle_observations_by_day[{vid}].rows", spec["rows"], len(rows), src)
        if "event_times" in spec:
            out.add(f"vehicle_observations_by_day[{vid}].event_times", [_jsonable(t) for t in want],
                    [_jsonable(t) for t in have], want == have, src)
        oids = [r["observation_id"] for r in rows]
        out.add(f"vehicle_observations_by_day[{vid}] distinct observation_id", len(oids), len(set(oids)),
                len(oids) == len(set(oids)), src)


def _c_prediction_metrics(st: _Store, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    sid = st.current_sid("trip_updates")
    if exp is None:
        out.add("prediction_metrics", None, sid, sid is None, "cassandra feed_current")
        return
    for route, spec in exp.items():
        row = st.route_metrics("trip_updates", sid, route) if sid else None
        src = "cassandra route_metrics_by_snapshot"
        for key, value in (spec or {}).items():
            col = STORED_PREDICTION_COLUMNS.get(key)
            if key == "excluded_total":
                actual = sum(_count_map((row or {}).get("excluded_trip_counts")).values()) if row else None
                out.cmp(f"route_metrics_by_snapshot[{route}].sum(excluded_trip_counts)", value, actual, src)
            elif key == "unknown_uncertainty":
                out.cmp(f"route_metrics_by_snapshot[{route}].unknown_uncertainty_count", value,
                        (row or {}).get("unknown_uncertainty_count"), src)
            elif key == "stale":
                continue
            elif col is None:
                out.unsupported_key(f"prediction_metrics[{route}].{key}", "no stored column")
            elif key == "excluded":
                out.add(f"route_metrics_by_snapshot[{route}].{col}", _count_map(value),
                        _count_map((row or {}).get(col)), _count_map(value) == _count_map((row or {}).get(col)), src)
            else:
                out.cmp(f"route_metrics_by_snapshot[{route}].{col}", value, (row or {}).get(col), src)


def _c_predictions(st: _Store, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    sid = st.current_sid("trip_updates")
    if exp is None:
        out.add("predictions", None, sid, sid is None, "cassandra feed_current")
        return
    by_stop: dict[str, list[Mapping[str, Any]]] = {}
    for p in exp:
        by_stop.setdefault(str(p.get("stop_id")), []).append(p)
    for stop, items in sorted(by_stop.items()):
        rows = st.by_partition("predictions_by_stop_snapshot", sid, "stop_id", stop) if sid else []
        actual = {_pkey(r.get("trip_id"), r.get("stop_sequence"), r.get("event_type")): r for r in rows}
        want = {_pkey(p.get("trip_id"), p.get("stop_sequence"), p.get("event_type")): p for p in items}
        src = "cassandra predictions_by_stop_snapshot"
        out.add(f"predictions_by_stop_snapshot[{stop}] keys", sorted(map(str, want)), sorted(map(str, actual)),
                set(want) == set(actual), src)
        for k, p in want.items():
            row = actual.get(k)
            if row is None:
                continue
            for key, col in STORED_PREDICTION_ROW_FIELDS.items():
                if key in p:
                    out.cmp(f"predictions_by_stop_snapshot[{stop}][{k[0]}/{k[1]}/{k[2]}].{key}", p[key], row.get(col),
                            src)


def _c_alerts(st: _Store, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    sid = st.current_sid("alerts")
    if exp is None:
        out.add("alerts_by_route", None, sid, sid is None, "cassandra feed_current")
        return
    for route, alerts in exp.items():
        rows = {r["alert_id"]: r for r in st.by_partition("alerts_by_route_snapshot", sid, "route_id", route)} \
            if sid else {}
        src = "cassandra alerts_by_route_snapshot"
        out.add(f"alerts_by_route_snapshot[{route}] ids", sorted(alerts or {}), sorted(rows),
                set(alerts or {}) == set(rows), src)
        for aid, spec in (alerts or {}).items():
            row = rows.get(aid)
            if row is None or not isinstance(spec, Mapping):
                continue
            if "confirmation" in spec:
                out.cmp(f"alerts_by_route_snapshot[{route}][{aid}].confirmation", spec["confirmation"],
                        row.get("confirmation_status"), src)
            for key, col in (("inherited_from", "inherited_from_snapshot_id"), ("first_seen", "first_seen_snapshot_id")):
                if key in spec:
                    sid_val = row.get(col)
                    want = None if spec[key] is None else ctx["labels"].get(spec[key], f"<unmapped {spec[key]}>")
                    out.add(f"alerts_by_route_snapshot[{route}][{aid}].{key}", spec[key],
                            ctx["inv"].get(sid_val, sid_val) if sid_val else None, sid_val == want, src)
    out.skip("alerts_by_route.state", "active state is recomputed by the API at the request clock")


def _c_selector_resolution(st: _Store, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    sid = st.current_sid("alerts")
    if exp is None or sid is None:
        return
    merged: dict[str, dict[str, Any]] = {}
    for route in st.run_routes():
        for r in st.by_partition("alerts_by_route_snapshot", sid, "route_id", route):
            merged[r["alert_id"]] = r
    for aid, value in exp.items():
        if aid in merged:
            out.cmp(f"alerts_by_route_snapshot[{aid}].selector_resolution", value,
                    merged[aid].get("selector_resolution"), "cassandra alerts_by_route_snapshot")


def _c_alert_quality(st: _Store, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    sid = st.current_sid("alerts")
    if exp is None:
        return
    q = st.quality("alerts", sid) if sid else {}
    for aid, codes in exp.items():
        for code in codes if isinstance(codes, list) else [codes]:
            n = (q.get(code) or {}).get("record_count", 0)
            out.add(f"quality_by_snapshot[alerts].{code} ({aid})", ">= 1", n, isinstance(n, int) and n >= 1,
                    "cassandra quality_by_snapshot")


def _c_alerts_closed(st: _Store, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    sid = st.current_sid("alerts")
    if not exp or sid is None:
        return
    present: set[str] = set()
    for route in st.run_routes():
        present |= {r["alert_id"] for r in st.by_partition("alerts_by_route_snapshot", sid, "route_id", route)}
    for aid in exp:
        out.add(f"alerts_closed[{aid}] absent from current generation", False, aid in present, aid not in present,
                "cassandra alerts_by_route_snapshot")


def _c_lineage_totals(st: _Store, exp: Any, ctx: dict[str, Any], out: _Checks) -> None:
    if not isinstance(exp, Mapping):
        return
    if "generations_built" in exp:
        ready = sum(1 for f in FEEDS for m in st.manifests(f) if m.get("status") == "READY")
        out.cmp("snapshot_manifest READY generations", exp["generations_built"], ready, "cassandra snapshot_manifest")
    for label in exp.get("late_labels") or []:
        sid = ctx["labels"].get(label)
        found = None
        for f in FEEDS:
            if sid and "LATE_SNAPSHOT_NOT_PUBLISHED" in st.quality(f, sid):
                found = f
        out.add(f"late_labels[{label}] LATE_SNAPSHOT_NOT_PUBLISHED", True, found is not None, found is not None,
                "cassandra quality_by_snapshot")


_CASS_HANDLERS = {
    "pointer": _c_pointer, "feeds": _c_feeds, "vehicle_generation_metrics": _c_generation_metrics,
    "vehicles": _c_vehicles, "vehicle_quality": _c_vehicle_quality, "history": _c_history,
    "prediction_metrics": _c_prediction_metrics, "predictions": _c_predictions, "alerts_by_route": _c_alerts,
    "alert_selector_resolution": _c_selector_resolution, "alert_quality": _c_alert_quality,
    "alerts_closed": _c_alerts_closed, "lineage_totals": _c_lineage_totals,
}
_CASS_SKIPS = {
    "vehicle_metrics": "serving view recomputed at the step clock (stored values: vehicle_generation_metrics)",
    "targets": "per-trip targets are checked through the API leg and predictions_by_stop_snapshot rows",
    "deliveries": HARNESS_KEYS["deliveries"],
    "dlq": HARNESS_KEYS["dlq"],
}


def cassandra_check_step(session: Any, run_id: str, step_expect: dict, labels: dict) -> dict:
    """Compare the same step with the stored rows (``Cassandra == expected.json``)."""
    exp, clock, step_name = _split_step(step_expect)
    lab = normalize_labels(labels)
    ctx: dict[str, Any] = {"labels": lab, "inv": invert_labels(lab), "clock": clock}
    out = _Checks()
    st = _Store(session, run_id)
    if clock is not None:
        rows = _rows(session, "SELECT application_clock FROM run_metadata WHERE run_id = %s", (run_id,))
        out.cmp("run_metadata.application_clock", clock, rows[0].get("application_clock") if rows else None,
                "cassandra run_metadata")
    for key, value in exp.items():
        if key in INFORMATIONAL_KEYS:
            continue
        if key in _CASS_SKIPS:
            out.skip(key, _CASS_SKIPS[key])
        elif key in _CASS_HANDLERS:
            _CASS_HANDLERS[key](st, value, ctx, out)
        else:
            out.unsupported_key(key, "no Cassandra checker for this expectation key")
    return out.result(run_id, step_name, require_checks=False)


def summarize(result: Mapping[str, Any], *, limit: int = 12) -> str:
    """One-line human summary of failed checks (for assertion messages)."""
    failed = [c for c in result.get("checks", []) if not c.get("passed")]
    parts = [f"{c['name']}: expected {c['expected']!r}, got {c['actual']!r}" for c in failed[:limit]]
    parts += [f"unsupported {u['key']}: {u['reason']}" for u in result.get("unsupported", [])[:limit]]
    more = len(failed) - limit
    if more > 0:
        parts.append(f"... {more} more")
    return "; ".join(parts) if parts else "all checks passed"
