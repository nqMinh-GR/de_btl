"""Scenario runner of the independent oracle (stdlib only).

``run_scenario(fixture_dir, run_name)`` replays one run of ``timeline.json`` (steps of tick / capture /
republish / malformed_envelope / barrier items) and returns the logical outputs in exactly the shape of
``expected.json["runs"][run_name]``: ``{start_clock, scope_route_ids, steps: [{step, clock, inputs, expect}]}``.

Replay semantics (design B §4-§5, design C §6-§8, interfaces §4/§10):
* every item with ``at`` first advances the source clock to ``max(clock, at)`` (late items never move the
  clock backwards); a generation's ``metrics_as_of`` is the clock when it is first built;
* a label delivered again is a duplicate delivery (same snapshot id -> same READY generation, no rebuild);
* source order = (header timestamp, capture time); a delivery older than the current pointer is late
  (LATE_SNAPSHOT_NOT_PUBLISHED) and never moves the pointer; a new late build has no parent;
* serving views (freshness, staleness, alert state) are recomputed at the step clock.

MUST NOT import ``transitpulse`` or ``pyspark``.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from .gtfs import GtfsFeed, Scope, build_scope, iso_epoch, parse_iso, static_summary, to_epoch
from .realtime import (
    DEFAULT_POLICY,
    FEED_STALE_POLICY_KEY,
    FEED_TYPES,
    build_alert_generation,
    build_trip_update_generation,
    build_vehicle_generation,
    route_vehicle_metrics,
    serve_alerts,
    serve_vehicles,
)

ITEM_KINDS = ("tick", "capture", "republish", "malformed_envelope", "barrier")


def load_timeline(fixture_dir: Path | str) -> dict[str, Any]:
    return json.loads((Path(fixture_dir) / "timeline.json").read_text(encoding="utf-8"))


class _RunState:
    def __init__(self, fixture_dir: Path, scope: Scope, policy: dict[str, Any]):
        self.fixture_dir = fixture_dir
        self.scope = scope
        self.policy = policy
        self.clock: int | None = None
        self.captures: dict[str, dict[str, Any]] = {}
        self.generations: dict[str, dict[str, Any]] = {}
        self.source_order: dict[str, tuple[int, int, str]] = {}
        self.pointer: dict[str, str | None] = {ft: None for ft in FEED_TYPES}
        self.duplicates: Counter[str] = Counter()
        self.lates: Counter[str] = Counter()
        self.late_labels: set[str] = set()
        self.dlq: Counter[str] = Counter()
        self.history: dict[str, dict[tuple, int]] = {}
        self.alerts_closed: dict[str, dict[str, Any]] = {}
        self.step_deliveries: list[dict[str, Any]] = []

    # ------------------------------------------------------------------ items
    def advance(self, at_text: str | None) -> None:
        if at_text is None:
            return
        t = to_epoch(parse_iso(at_text))
        self.clock = t if self.clock is None else max(self.clock, t)

    def capture(self, item: dict[str, Any]) -> None:
        label = item["label"]
        if label in self.captures:
            raise ValueError(f"capture label {label!r} used twice in one run")
        doc = json.loads((self.fixture_dir / item["file"]).read_text(encoding="utf-8"))
        self.captures[label] = {
            "feed_type": item["feed_type"],
            "at": to_epoch(parse_iso(item["at"])),
            "doc": doc,
        }
        self.deliver(label)

    def malformed(self, item: dict[str, Any]) -> None:
        text = item.get("value_text")
        try:
            json.loads(text if isinstance(text, str) else "")
        except ValueError:
            self.dlq["INVALID_JSON"] += 1
            return
        raise ValueError("malformed_envelope.value_text must not be valid JSON")

    def deliver(self, label: str) -> None:
        if label not in self.captures:
            raise KeyError(f"republish of unknown label {label!r}")
        cap = self.captures[label]
        feed_type = cap["feed_type"]
        order = (int(cap["doc"]["header"]["timestamp"]), cap["at"], label)
        new_generation = label not in self.generations
        if new_generation:
            current = self.pointer[feed_type]
            built_late = current is not None and self.source_order[current] > order
            parent = None if (built_late or current is None) else self.generations[current]
            generation = self._build(feed_type, label, cap["doc"], parent)
            self.generations[label] = generation
            self.source_order[label] = order
            self.dlq.update(generation["dlq"])
            for vehicle_id, identity, event_epoch in generation.get("history", []):
                self.history.setdefault(vehicle_id, {})[identity] = event_epoch
        else:
            self.duplicates[feed_type] += 1

        current = self.pointer[feed_type]
        late = False
        moved = False
        if current == label:
            pass                                   # idempotent success (pointer already at this snapshot)
        elif current is not None and self.source_order[current] > order:
            late = True
            self.lates[feed_type] += 1
            self.late_labels.add(label)
        else:
            self.pointer[feed_type] = label
            moved = True
            if feed_type == "alerts":
                self.alerts_closed.update(self.generations[label].get("closed", {}))
        self.step_deliveries.append({
            "label": label,
            "feed_type": feed_type,
            "new_generation": new_generation,
            "duplicate_delivery": not new_generation,
            "late": late,
            "pointer_moved": moved,
        })

    def _build(self, feed_type: str, label: str, doc: dict, parent: dict | None) -> dict[str, Any]:
        assert self.clock is not None
        if feed_type == "vehicle_positions":
            return build_vehicle_generation(doc, self.scope, self.clock, self.policy, parent, label)
        if feed_type == "trip_updates":
            return build_trip_update_generation(doc, self.scope, self.clock, self.policy, label)
        if feed_type == "alerts":
            return build_alert_generation(doc, self.scope, self.clock, self.policy, parent, label)
        raise ValueError(f"unknown feed_type {feed_type!r}")

    # ------------------------------------------------------------------ expectations
    def expect(self) -> dict[str, Any]:
        clock = self.clock
        assert clock is not None
        policy = self.policy
        feeds: dict[str, Any] = {}
        for feed_type in FEED_TYPES:
            label = self.pointer[feed_type]
            if label is None:
                feeds[feed_type] = None
                continue
            gen = self.generations[label]
            age = clock - gen["header_ts"]
            feeds[feed_type] = {
                "label": label,
                "feed_timestamp": iso_epoch(gen["header_ts"]),
                "metrics_as_of": iso_epoch(gen["metrics_as_of"]),
                "age_s": age,
                "stale": age > policy[FEED_STALE_POLICY_KEY[feed_type]],
                "is_partial": gen["is_partial"],
                "is_empty": gen["is_empty"],
                "source_entity_count": gen["source_entity_count"],
                "rejected_entity_count": gen["rejected_entity_count"],
            }

        out: dict[str, Any] = {
            "pointer": dict(self.pointer),
            "deliveries": list(self.step_deliveries),
            "lineage_totals": {
                "generations_built": len(self.generations),
                "duplicate_deliveries": {ft: self.duplicates[ft] for ft in FEED_TYPES},
                "late_deliveries": {ft: self.lates[ft] for ft in FEED_TYPES},
                "late_labels": sorted(self.late_labels),
            },
            "feeds": feeds,
        }

        vp = self.pointer["vehicle_positions"]
        if vp is not None:
            gen = self.generations[vp]
            out["vehicle_metrics"] = route_vehicle_metrics(gen, self.scope, clock, policy)
            out["vehicle_generation_metrics"] = route_vehicle_metrics(gen, self.scope, gen["metrics_as_of"], policy)
            out["vehicles"] = serve_vehicles(gen, clock, policy)
            out["vehicle_quality"] = dict(sorted(gen["quality"].items()))
        else:
            out["vehicle_metrics"] = None
            out["vehicle_generation_metrics"] = None
            out["vehicles"] = None
            out["vehicle_quality"] = None
        out["history"] = {
            vid: {"rows": len(rows), "event_times": sorted(str(iso_epoch(e)) for e in rows.values())}
            for vid, rows in sorted(self.history.items())
        }

        tu = self.pointer["trip_updates"]
        if tu is not None:
            gen = self.generations[tu]
            stale = (clock - gen["header_ts"]) > policy["prediction_stale_seconds"]
            out["prediction_metrics"] = {rid: dict(m, stale=stale) for rid, m in gen["route_metrics"].items()}
            out["targets"] = gen["targets"]
            out["predictions"] = gen["predictions"]
        else:
            out["prediction_metrics"] = None
            out["targets"] = None
            out["predictions"] = None

        al = self.pointer["alerts"]
        if al is not None:
            gen = self.generations[al]
            out["alerts_by_route"] = serve_alerts(gen, clock)
            out["alert_selector_resolution"] = dict(sorted(gen["selector_resolution"].items()))
            out["alert_quality"] = dict(sorted(gen["alert_quality"].items()))
        else:
            out["alerts_by_route"] = None
            out["alert_selector_resolution"] = None
            out["alert_quality"] = None
        out["alerts_closed"] = dict(sorted(self.alerts_closed.items()))
        out["dlq"] = {k: v for k, v in sorted(self.dlq.items()) if v}
        return out


def run_scenario(fixture_dir: Path | str, run_name: str) -> dict[str, Any]:
    """Replay ``timeline.json["runs"][run_name]`` and return the expected.json-shaped run object."""
    base = Path(fixture_dir)
    timeline = load_timeline(base)
    run = timeline["runs"][run_name]
    route_ids = list(run.get("scope_route_ids") or timeline["scope_route_ids"])
    scope = build_scope(GtfsFeed.load(base / "static"), route_ids)
    state = _RunState(base, scope, dict(DEFAULT_POLICY))
    state.advance(run["start_clock"])
    steps: list[dict[str, Any]] = []
    for step in run["steps"]:
        state.step_deliveries = []
        inputs: list[str] = []
        for item in step["items"]:
            kind = item["kind"]
            if kind not in ITEM_KINDS:
                raise ValueError(f"unknown timeline item kind {kind!r}")
            state.advance(item.get("at"))
            if kind == "capture":
                state.capture(item)
            elif kind == "republish":
                state.deliver(item["label"])
            elif kind == "malformed_envelope":
                state.malformed(item)
            if item.get("label") is not None:
                inputs.append(item["label"])
        steps.append({
            "step": step["step"],
            "clock": iso_epoch(state.clock),
            "inputs": inputs,
            "expect": state.expect(),
        })
    return {"start_clock": run["start_clock"], "scope_route_ids": route_ids, "steps": steps}


def run_all(fixture_dir: Path | str) -> dict[str, Any]:
    """All runs plus the static summary (convenience for debugging / the Colab golden smoke)."""
    base = Path(fixture_dir)
    timeline = load_timeline(base)
    return {
        "policy": dict(DEFAULT_POLICY),
        "static_summary": static_summary(base / "static", timeline["scope_route_ids"]),
        "runs": {name: run_scenario(base, name) for name in timeline["runs"]},
    }
