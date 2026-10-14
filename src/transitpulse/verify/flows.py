"""Fixture runs driven step by step through the real stack (golden run, IT06-IT09 scenarios, recovery flows).

A :class:`FixtureRun` takes one run of ``tests/fixtures/golden/timeline.json`` (``runs.<key>.steps[].items[]``),
materializes the matching recording with ``replay synth-generate``, loads its static scope, creates a synthetic
run, starts the processor and then applies the timeline items itself:

* ``tick`` -> ``clock_tick`` (ACKed, then waited until ``run_metadata.clock_sequence`` shows it applied);
* ``capture`` -> a clock tick first when the item is later than the clock, then the envelope rebuilt from the raw
  capture exactly like the replay publisher (same snapshot id);
* ``republish`` -> the earlier label's envelope again (new ``published_at``);
* ``malformed_envelope`` -> the raw bytes on the feed topic; ``arm_fault`` -> the documented arm file;
  ``barrier`` -> nothing.

After each step the harness waits until the processor's committed progress covers everything produced, then the
step's ``expected.json`` entry is compared with the API (``check_step``) and with Cassandra
(``cassandra_check_step``).
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from transitpulse.config import ProjectConfig
from transitpulse.contracts.envelope import kafka_key, parse_iso_utc
from transitpulse.contracts.topics import DLQ, TOPIC_BY_FEED
from transitpulse.runtime.paths import WorkPaths
from transitpulse.verify.fixture import (
    EnvelopeFactory,
    Recording,
    StaticScope,
    load_static_scope,
    materialize,
)
from transitpulse.verify.golden import (
    cassandra_check_step,
    check_step,
    expected_steps,
    find_step,
    load_expected,
    load_timeline,
    resolve_run_key,
)
from transitpulse.verify.stack import Delivery, HarnessError, StackHarness


@dataclass
class StackContext:
    """Everything needed to materialize fixtures and load static scopes (cached per process)."""

    cfg: ProjectConfig
    paths: WorkPaths
    runtime: Any
    profile: str
    repo_dir: Path
    golden_dir: Path
    env: dict[str, str]
    _recordings: dict[tuple[Any, ...], Recording] = field(default_factory=dict)
    _statics: dict[tuple[str, tuple[str, ...]], StaticScope] = field(default_factory=dict)
    _timeline: dict[str, Any] | None = None
    _expected: dict[str, Any] | None = None

    @property
    def timeline(self) -> dict[str, Any]:
        if self._timeline is None:
            self._timeline = load_timeline(self.golden_dir)
        return self._timeline

    @property
    def expected(self) -> dict[str, Any]:
        if self._expected is None:
            self._expected = load_expected(self.golden_dir)
        return self._expected

    def materialize(self, kind: str, scenario_name: str | None = None, *, seed: int | None = None,
                    **kw: Any) -> Recording:
        seed = self.cfg.synthetic.default_seed if seed is None else int(seed)
        key = (kind, scenario_name, seed, tuple(sorted(kw.items())))
        if key not in self._recordings:
            self._recordings[key] = materialize(
                core_python=self.runtime.core_python, env=self.env, repo_dir=self.repo_dir, workdir=self.paths.root,
                kind=kind, seed=seed, scenario_name=scenario_name, **kw)
        return self._recordings[key]

    def static_for(self, zip_path: Path, route_ids: tuple[str, ...]) -> StaticScope:
        digest = hashlib.sha256(Path(zip_path).read_bytes()).hexdigest()
        key = (digest, tuple(route_ids))
        if key not in self._statics:
            self._statics[key] = load_static_scope(
                core_python=self.runtime.core_python, env=self.env, repo_dir=self.repo_dir, workdir=self.paths.root,
                profile=self.profile, zip_path=Path(zip_path), route_ids=route_ids)
        return self._statics[key]

    def timeline_run(self, run_key: str) -> tuple[str, dict[str, Any]]:
        runs = self.timeline.get("runs", {})
        key = resolve_run_key(runs, run_key)
        if key is None:
            raise HarnessError(f"timeline.json has no run {run_key!r} (runs: {sorted(runs)})")
        return key, runs[key]


@dataclass
class StepOutcome:
    step: str
    deliveries: list[tuple[str, Delivery]]
    api: dict[str, Any] | None
    cassandra: dict[str, Any] | None
    expected: dict[str, Any] | None
    wall_seconds: float
    lineage: dict[str, Any] | None = None

    @property
    def passed(self) -> bool:
        if self.api is None:
            return False
        return (bool(self.api.get("passed")) and (self.cassandra is None or bool(self.cassandra.get("passed")))
                and (self.lineage is None or bool(self.lineage.get("passed"))))

    def as_dict(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "deliveries": [{"label": label, "topic": d.topic, "partition": d.partition, "offset": d.offset}
                           for label, d in self.deliveries],
            "api": self.api,
            "cassandra": self.cassandra,
            "lineage": self.lineage,
            "expected": self.expected,
            "wall_seconds": round(self.wall_seconds, 3),
            "passed": self.passed,
        }


def _check(name: str, expected: Any, actual: Any, passed: bool, source: str) -> dict[str, Any]:
    return {"name": name, "expected": expected, "actual": actual, "passed": bool(passed), "source": source}


class FixtureRun:
    """One timeline run applied through :class:`StackHarness`."""

    def __init__(self, harness: StackHarness, ctx: StackContext, run_key: str, *, recording: Recording,
                 static: StaticScope, run: Any, timeline_run: dict[str, Any]) -> None:
        self.h = harness
        self.ctx = ctx
        self.run_key = run_key
        self.recording = recording
        self.static = static
        self.run = run
        self.timeline_run = timeline_run
        self.factory = EnvelopeFactory(run, component="pytest-harness")
        self.labels: dict[str, str] = {}
        self.envelopes: dict[str, dict[str, Any]] = {}
        self.capture_sources: dict[str, str] = {}
        self.published: list[tuple[str, str, Delivery]] = []    # (step, label, delivery)
        self.outcomes: dict[str, StepOutcome] = {}
        self.dlq_start: dict[tuple[str, int], int] = {}
        self._items_by_step: dict[str, list[dict[str, Any]]] = {}
        for idx, step in enumerate(self.steps):
            self._items_by_step[str(step.get("step", f"#{idx}"))] = [
                i for i in step.get("items", []) if isinstance(i, dict)]

    # ------------------------------------------------------------------ construction
    @classmethod
    def prepare(cls, harness: StackHarness, ctx: StackContext, run_key: str, *, fault_injection: bool = False,
                overrides: Mapping[str, Any] | None = None, extra_env: Mapping[str, str] | None = None,
                start: bool = True) -> FixtureRun:
        key, trun = ctx.timeline_run(run_key)
        kind, scen = ("golden", None) if key == "golden" else ("scenario", key)
        recording = ctx.materialize(kind, scen)
        routes = tuple(trun.get("scope_route_ids") or ctx.timeline.get("scope_route_ids") or recording.route_ids)
        static = ctx.static_for(recording.static_zip_path, routes)
        start_clock = parse_iso_utc(str(trun.get("start_clock"))) if trun.get("start_clock") else recording.start_clock
        run = harness.create_run(
            static=static, source_type="synthetic", initial_clock=start_clock,
            scenario_id="golden_v1" if key == "golden" else key, fixture_kind="golden_handwritten",
            recording_id=recording.recording_id, label=f"{harness.session_id}:{key}", overrides=overrides,
            route_ids=routes,
        )
        fr = cls(harness, ctx, key, recording=recording, static=static, run=run, timeline_run=trun)
        fr.dlq_start = harness.high_watermarks([DLQ])
        if start:
            fr.start(fault_injection=fault_injection, extra_env=extra_env)
        return fr

    def start(self, *, fault_injection: bool = False, extra_env: Mapping[str, str] | None = None) -> None:
        self.h.start_processor(self.run.run_id, fault_injection=fault_injection, extra_env=extra_env)
        self.h.begin_run(self.run, self.run.initial_clock)

    # ------------------------------------------------------------------ steps
    @property
    def steps(self) -> list[dict[str, Any]]:
        return [s for s in self.timeline_run.get("steps", []) if isinstance(s, dict)]

    @property
    def step_names(self) -> list[str]:
        return list(self._items_by_step)

    def items(self, step: str) -> list[dict[str, Any]]:
        return list(self._items_by_step.get(step, []))

    def expected_step(self, step: str) -> dict[str, Any] | None:
        return find_step(self.ctx.expected, self.run_key, step)

    def has_expectations(self) -> bool:
        return bool(expected_steps(self.ctx.expected, self.run_key))

    def _recording_item(self, label: str, item: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str]:
        found = self.recording.item_for_label(label)
        if found:
            return found, "recording(labels.json)"
        file_rel = item.get("file")
        if isinstance(file_rel, str):
            path = self.ctx.golden_dir / file_rel
            if path.is_file():
                sha = hashlib.sha256(path.read_bytes()).hexdigest()
                found = self.recording.item_by_raw_sha256(sha)
                if found:
                    return found, "recording(raw_sha256)"
        return None, "missing"

    def envelope_for(self, label: str, item: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if label in self.envelopes:
            return self.envelopes[label]
        if item is None:
            raise HarnessError(f"label {label!r} is republished before it was captured")
        rec_item, source = self._recording_item(label, item)
        if rec_item is not None:
            env = self.factory.from_recording_item(self.recording, rec_item)
        else:
            file_rel = item.get("file")
            if not isinstance(file_rel, str):
                raise HarnessError(f"capture {label!r} has neither a recording item nor a fixture file")
            body = (self.ctx.golden_dir / file_rel).read_bytes()
            env = self.factory.archive_and_build(
                self.h.paths, feed_type=str(item["feed_type"]), body=body,
                fetched_at=parse_iso_utc(str(item["at"])), source_url=f"fixture://golden/{file_rel}")
            source = "archived fixture file"
        self.envelopes[label] = env
        self.labels[label] = str(env["snapshot_id"])
        self.capture_sources[label] = source
        return env

    def _tick_to(self, at: datetime) -> None:
        cur = self.h.current_clock(self.run.run_id)
        if cur is None or at > cur:
            self.h.publish_clock(self.run, at)

    def apply_item(self, step: str, item: Mapping[str, Any]) -> tuple[str, Delivery] | None:
        kind = item.get("kind")
        at = parse_iso_utc(str(item["at"])) if item.get("at") else None
        if kind == "tick":
            if at is None:
                raise HarnessError(f"tick without 'at' in step {step}")
            self._tick_to(at)
            return None
        if kind == "barrier":
            return None
        if kind == "capture":
            label = str(item.get("label") or item.get("file"))
            env = self.envelope_for(label, item)
            if at is not None:
                self._tick_to(at)
            d = self.h.publish_envelope(env)
            self.published.append((step, label, d))
            return label, d
        if kind == "republish":
            ref = item.get("label") or item.get("of_label") or item.get("of_step_item")
            if not isinstance(ref, str):
                raise HarnessError(f"republish item without a label in step {step}: {dict(item)}")
            env = self.envelope_for(ref)
            d = self.h.publish_envelope(env)
            self.published.append((step, ref, d))
            return ref, d
        if kind == "malformed_envelope":
            feed = str(item.get("feed_type") or (item.get("params") or {}).get("feed_type"))
            text = item.get("value_text") or (item.get("params") or {}).get("value_text") or ""
            d = self.h.publish_raw_bytes(TOPIC_BY_FEED[feed], kafka_key(self.run.run_id, feed), str(text).encode())
            label = str(item.get("label") or f"{step}:malformed")
            self.published.append((step, label, d))
            return label, d
        if kind == "arm_fault":
            params = item.get("params") or item
            match = dict(params.get("match") or {})
            target = match.pop("label", None)
            if isinstance(target, str):
                match["snapshot_id"] = self.labels.get(target) or str(self.envelope_for(target)["snapshot_id"])
            self.h.arm_fault(self.run.run_id, str(params["point"]), match)
            return None
        raise HarnessError(f"timeline item kind {kind!r} is not supported by the harness (step {step})")

    def apply_step(self, step: str, *, wait: bool = True, timeout: float = 240.0) -> list[tuple[str, Delivery]]:
        if step not in self._items_by_step:
            raise HarnessError(f"step {step!r} not in timeline run {self.run_key} ({self.step_names})")
        out = []
        for item in self._items_by_step[step]:
            res = self.apply_item(step, item)
            if res is not None:
                out.append(res)
        if wait:
            self.h.wait_caught_up(self.run.run_id, timeout=timeout)
        return out

    def check(self, step: str, *, api_base: str | None = None, retries: int = 1) -> tuple[dict | None, dict | None]:
        exp = self.expected_step(step)
        if exp is None:
            return None, None
        base = api_base or self.h.api_base
        if not base:
            raise HarnessError("API base URL is required for check_step")
        api = check_step(base, self.run.run_id, exp, self.labels)
        for _ in range(retries):
            if api["passed"]:
                break
            time.sleep(3.0)
            api = check_step(base, self.run.run_id, exp, self.labels)
        cass = cassandra_check_step(self.h.session, self.run.run_id, exp, self.labels)
        return api, cass

    # ------------------------------------------------------------------ lineage (needs before/after state)
    def lineage_state(self) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for feed in TOPIC_BY_FEED:
            ptr = self.h.pointer(self.run.run_id, feed)
            out[feed] = {"pointer": (ptr or {}).get("snapshot_id"),
                         "manifests": {m["snapshot_id"] for m in self.h.manifests(self.run.run_id, feed)}}
        return out

    def dlq_counts(self) -> dict[str, int]:
        """Distinct DLQ records (by ``error_id``) of this run per reason code, since the run was prepared."""
        seen: dict[str, set[str]] = {}
        for part in self.h.partitions(DLQ):
            start = self.dlq_start.get((DLQ, part), 0)
            end = self.h.watermarks(DLQ, part)[1]
            for m in self.h.consume_range(DLQ, part, start, end, key_prefix=f"{self.run.run_id}|".encode()):
                try:
                    rec = json.loads(m["value"])
                except ValueError:
                    continue
                if isinstance(rec, dict) and rec.get("run_id") in (self.run.run_id, None):
                    seen.setdefault(str(rec.get("reason_code")), set()).add(str(rec.get("error_id") or m["offset"]))
        return {k: len(v) for k, v in seen.items()}

    def _pipeline_counters(self) -> dict[str, dict[str, Any]]:
        rows = self.h.query("SELECT * FROM pipeline_status WHERE run_id = %s", (self.run.run_id,))
        return {str(r.get("component")): r for r in rows}

    def check_lineage(self, step: str, before: Mapping[str, Mapping[str, Any]]) -> dict[str, Any] | None:
        """deliveries / lineage_totals / dlq of the expected step (state before vs after the step)."""
        exp_step = self.expected_step(step)
        if exp_step is None:
            return None
        exp = exp_step.get("expect") or {}
        after = self.lineage_state()
        checks: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for d in exp.get("deliveries") or []:
            label, feed = str(d.get("label")), str(d.get("feed_type"))
            sid = self.labels.get(label)
            name = f"deliveries[{label}]"
            if sid is None:
                checks.append(_check(f"{name}.label", label, None, False, "harness labels"))
                continue
            if "new_generation" in d:
                new = sid not in before[feed]["manifests"] and sid in after[feed]["manifests"]
                checks.append(_check(f"{name}.new_generation", d["new_generation"], new, new == d["new_generation"],
                                     "snapshot_manifest before/after"))
            if "duplicate_delivery" in d:
                dup = sid in before[feed]["manifests"]
                checks.append(_check(f"{name}.duplicate_delivery", d["duplicate_delivery"], dup,
                                     dup == d["duplicate_delivery"], "snapshot_manifest before"))
            if "late" in d:
                late = "LATE_SNAPSHOT_NOT_PUBLISHED" in self.h.quality_rows(self.run.run_id, feed, sid)
                checks.append(_check(f"{name}.late", d["late"], late, late == d["late"], "quality_by_snapshot"))
            if "pointer_moved" in d:
                moved = after[feed]["pointer"] == sid and before[feed]["pointer"] != sid
                checks.append(_check(f"{name}.pointer_moved", d["pointer_moved"], moved, moved == d["pointer_moved"],
                                     "feed_current before/after"))
        totals = exp.get("lineage_totals") or {}
        counters = self._pipeline_counters() if totals else {}
        for kind, column in (("duplicate_deliveries", "duplicate_deliveries_total"),
                             ("late_deliveries", "late_deliveries_total")):
            want = totals.get(kind)
            if not isinstance(want, Mapping):
                continue
            per_feed = {f: counters.get(f"feed:{f}", {}).get(column) for f in want}
            if all(v is not None for v in per_feed.values()):
                for f, n in want.items():
                    checks.append(_check(f"lineage_totals.{kind}[{f}]", n, per_feed[f], per_feed[f] == n,
                                         f"pipeline_status feed:{f}.{column}"))
            elif counters.get("processor", {}).get(column) is not None:
                total = int(counters["processor"][column])
                checks.append(_check(f"lineage_totals.{kind} (all feeds)", sum(want.values()), total,
                                     total == sum(want.values()), f"pipeline_status processor.{column}"))
            else:
                skipped.append({"key": f"lineage_totals.{kind}", "reason": f"pipeline_status has no {column}"})
        if "dlq" in exp:
            want_dlq = {k: int(v) for k, v in (exp.get("dlq") or {}).items() if v}
            got = self.dlq_counts()
            checks.append(_check("dlq (distinct error_id per reason)", want_dlq, got, want_dlq == got, "mbta.dlq.v1"))
        return {"passed": all(c["passed"] for c in checks), "checks": checks, "skipped": skipped}

    def run_step(self, step: str, *, check: bool = True) -> StepOutcome:
        t0 = time.monotonic()
        before = self.lineage_state() if check else None
        deliveries = self.apply_step(step)
        api, cass = self.check(step) if check else (None, None)
        lineage = self.check_lineage(step, before) if check and before is not None else None
        outcome = StepOutcome(step, deliveries, api, cass, self.expected_step(step), time.monotonic() - t0, lineage)
        self.outcomes[step] = outcome
        self.h.write_evidence(f"{self.run_key}_{self.run.run_id[:8]}_{step}.json",
                              {"run_id": self.run.run_id, "labels": self.labels, **outcome.as_dict()})
        return outcome

    # ------------------------------------------------------------------ helpers for assertions
    def first_capture(self, step: str, feed: str) -> Mapping[str, Any] | None:
        for item in self.items(step):
            if item.get("kind") == "capture" and item.get("feed_type") == feed:
                return item
        return None

    def label_of(self, snapshot_id: str | None) -> str | None:
        for label, sid in self.labels.items():
            if sid == snapshot_id:
                return label
        return snapshot_id

    def header_ts(self, label: str) -> int | None:
        env = self.envelopes.get(label)
        if env is None:
            return None
        ts = env.get("feed_timestamp")
        return ts if isinstance(ts, int) else None
