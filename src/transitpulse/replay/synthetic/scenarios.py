"""Synthetic fault/edge-case scenarios (design E §3.4, spec §15.2, PDF p23).

A scenario is ``apply(plan, rng, params) -> plan``: it mutates a :class:`SynthPlan` (the generated timeline
before it is written) deterministically from ``rng`` (``random.Random(f"{seed}:{scenario_id}")``). Each
injected or modified item is labelled ``<scenario_id>`` / ``<scenario_id>:<role>`` so that tests and the
notebook can find it through ``labels.json``; capture params carry ``{"scenario": <id>}``.

Scenarios are applied in registry order (independent of the order given on the command line) so the
same set always produces the same recording.
"""

from __future__ import annotations

import copy
import random
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from transitpulse.contracts.envelope import iso_utc
from transitpulse.replay.synthetic.feed_builder import FeedBuilder, header
from transitpulse.replay.synthetic.recording import PlannedItem, doc_bytes

VP, TU, AL = "vehicle_positions", "trip_updates", "alerts"


class ScenarioError(ValueError):
    """A scenario cannot be applied to this plan (e.g. no suitable capture)."""


@dataclass
class SynthPlan:
    builder: FeedBuilder
    start: datetime
    end: datetime
    items: list[PlannedItem] = field(default_factory=list)
    gaps: list[dict[str, Any]] = field(default_factory=list)
    applied: list[str] = field(default_factory=list)
    _counter: int = 0

    # ------------------------------------------------------------------ queries
    def captures(self, feed_type: str, *, untouched: bool = True) -> list[PlannedItem]:
        out = []
        for it in self.items:
            if it.kind != "capture" or it.feed_type != feed_type or it.doc is None:
                continue
            if untouched and ("scenario" in it.params or it.key is not None):
                continue  # modified by a scenario or referenced by a republish item
            out.append(it)
        return out

    def pick(self, rng: random.Random, feed_type: str, *, require: Callable[[PlannedItem], bool] | None = None
             ) -> PlannedItem:
        """A capture from the middle third of the feed's captures (never the first one)."""
        caps = [c for c in self.captures(feed_type) if require is None or require(c)]
        if not caps:
            raise ScenarioError(f"no unmodified {feed_type} capture available")
        lo = max(1, len(caps) // 3) if len(caps) > 1 else 0
        hi = max(lo + 1, (2 * len(caps)) // 3)
        window = caps[lo:hi] or caps[-1:]
        return window[rng.randrange(len(window))]

    def index(self, item: PlannedItem) -> int:
        for i, it in enumerate(self.items):
            if it is item:
                return i
        raise ScenarioError("item is not part of the plan")

    def previous_capture(self, item: PlannedItem) -> PlannedItem | None:
        for it in reversed(self.items[: self.index(item)]):
            if it.kind == "capture" and it.feed_type == item.feed_type and it.doc is not None:
                return it
        return None

    def new_key(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}#{self._counter}"

    # ------------------------------------------------------------------ mutations
    def insert_after(self, anchor: PlannedItem, new: PlannedItem) -> None:
        self.items.insert(self.index(anchor) + 1, new)

    def insert_before(self, anchor: PlannedItem, new: PlannedItem) -> None:
        self.items.insert(self.index(anchor), new)

    def replace(self, old: PlannedItem, new: PlannedItem) -> None:
        self.items[self.index(old)] = new

    def remove(self, item: PlannedItem) -> None:
        del self.items[self.index(item)]

    @staticmethod
    def mark(item: PlannedItem, scenario_id: str, role: str | None = None) -> None:
        item.params["scenario"] = scenario_id
        item.label = scenario_id if role is None else f"{scenario_id}:{role}"


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    description: str
    apply: Callable[[SynthPlan, random.Random, Mapping[str, Any]], SynthPlan]


# --------------------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------------------

def _vehicles(item: PlannedItem) -> list[dict[str, Any]]:
    return [e for e in (item.doc or {}).get("entity") or [] if isinstance(e.get("vehicle"), dict)]


def _trip_updates(item: PlannedItem) -> list[dict[str, Any]]:
    return [e for e in (item.doc or {}).get("entity") or [] if isinstance(e.get("trip_update"), dict)]


def _has_vehicles(n: int) -> Callable[[PlannedItem], bool]:
    return lambda it: len(_vehicles(it)) >= n


def _has_trip_updates(n: int) -> Callable[[PlannedItem], bool]:
    return lambda it: len(_trip_updates(it)) >= n


def _header_ts(item: PlannedItem) -> int:
    return int((item.doc or {})["header"]["timestamp"])


def _choose(rng: random.Random, seq: Sequence[Any]) -> Any:
    return seq[rng.randrange(len(seq))]


# --------------------------------------------------------------------------------------------------
# scenario implementations
# --------------------------------------------------------------------------------------------------

def _duplicate_snapshot(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, VP, require=_has_vehicles(1))
    target.key = target.key or plan.new_key("dup")
    plan.mark(target, "duplicate_snapshot", "original")
    plan.insert_after(target, PlannedItem(kind="republish", at=target.at, feed_type=VP, ref=target.key,
                                          params={"scenario": "duplicate_snapshot"},
                                          label="duplicate_snapshot:republish"))
    return plan


def _duplicate_observation(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, VP, require=_has_vehicles(1))
    plan.mark(target, "duplicate_observation", "original")
    shift = int(params.get("shift_seconds", 5))
    doc = copy.deepcopy(target.doc or {})
    doc["header"] = header(_header_ts(target) + shift)
    new = PlannedItem(kind="capture", at=target.at + timedelta(seconds=shift), feed_type=VP, doc=doc,
                      key=plan.new_key("dupobs"), params={"scenario": "duplicate_observation"},
                      label="duplicate_observation:new_header")
    plan.insert_after(target, new)
    return plan


def _late_snapshot(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, VP, require=lambda it: plan.previous_capture(it) is not None)
    plan.mark(target, "late_snapshot", "newer")
    lag = int(params.get("lag_seconds", 15))
    late_at = target.at - timedelta(seconds=lag)
    late_doc = plan.builder.vehicle_positions(late_at)
    unseen = PlannedItem(kind="capture", at=late_at, feed_type=VP, doc=late_doc, key=plan.new_key("late"),
                         params={"scenario": "late_snapshot", "variant": "new_unseen"},
                         label="late_snapshot:new_unseen")
    plan.insert_after(target, unseen)
    older = plan.previous_capture(target)
    if older is not None:
        older.key = older.key or plan.new_key("late_ref")
        plan.insert_after(unseen, PlannedItem(kind="republish", at=target.at, feed_type=VP, ref=older.key,
                                              params={"scenario": "late_snapshot", "variant": "redelivery"},
                                              label="late_snapshot:redelivery"))
    return plan


def _invalid_gps(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, VP, require=_has_vehicles(2))
    plan.mark(target, "invalid_gps")
    vehicles = _vehicles(target)
    vehicles[0]["vehicle"]["position"]["latitude"] = 95.0
    vehicles[1]["vehicle"]["position"]["longitude"] = -200.0
    return plan


def _future_timestamp(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, VP, require=_has_vehicles(1))
    plan.mark(target, "future_timestamp")
    ahead = int(params.get("ahead_seconds", 60))
    veh = _choose(rng, _vehicles(target))
    veh["vehicle"]["timestamp"] = _header_ts(target) + ahead
    return plan


def _missing_timestamp(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, VP, require=_has_vehicles(1))
    plan.mark(target, "missing_timestamp")
    _choose(rng, _vehicles(target))["vehicle"].pop("timestamp", None)
    return plan


def _missing_required_key(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, VP, require=_has_vehicles(1))
    plan.mark(target, "missing_required_key")
    _choose(rng, _vehicles(target))["vehicle"]["vehicle"].pop("id", None)
    return plan


def _cancelled_trip(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, TU, require=_has_trip_updates(1))
    plan.mark(target, "cancelled_trip")
    tu = _choose(rng, _trip_updates(target))["trip_update"]
    tu["trip"]["schedule_relationship"] = "CANCELED"
    tu.pop("stop_time_update", None)
    return plan


def _uncertainty_scenario(scenario_id: str, value: int | None) -> Callable[..., SynthPlan]:
    def apply(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
        target = plan.pick(rng, TU, require=_has_trip_updates(1))
        plan.mark(target, scenario_id)
        tu = _choose(rng, _trip_updates(target))["trip_update"]
        for stu in tu.get("stop_time_update") or []:
            for event in ("arrival", "departure"):
                ev = stu.get(event)
                if not isinstance(ev, dict):
                    continue
                if value is None:
                    ev.pop("uncertainty", None)
                else:
                    ev["uncertainty"] = value
        return plan

    return apply


def _invalid_prediction(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, TU, require=_has_trip_updates(2))
    plan.mark(target, "invalid_prediction")
    tus = _trip_updates(target)
    first, second = tus[0]["trip_update"], tus[1]["trip_update"]
    for stu in first.get("stop_time_update") or []:          # uncertainty above 301 -> invalid
        for event in ("arrival", "departure"):
            if isinstance(stu.get(event), dict):
                stu[event]["uncertainty"] = 360
    stus = second.get("stop_time_update") or []
    if stus:                                                  # an event without a time
        stu = stus[0]
        for event in ("arrival", "departure"):
            if isinstance(stu.get(event), dict):
                stu[event] = {"uncertainty": 60}
    return plan


def _ambiguous_service_date(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    night = next((i for i in plan.builder.instances if i.trip.trip_id.endswith("_NIGHT")), None)
    if night is None:
        raise ScenarioError("the static model has no night (DAILY, >24h) trip")
    target = plan.pick(rng, TU)
    plan.mark(target, "ambiguous_service_date")
    ts = _header_ts(target)
    ent = plan.builder.trip_update_entity(night, ts, with_start_date=False, min_time=0)
    if ent is None:
        raise ScenarioError("night trip produced no stop time updates")
    ent["id"] = f"TU-{night.trip.trip_id}-NOSTARTDATE"
    (target.doc or {}).setdefault("entity", []).append(ent)
    return plan


def _repeated_stop_without_sequence(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    loop = next((i for i in plan.builder.instances if i.trip.trip_id.endswith("_LOOP")), None)
    if loop is None:
        raise ScenarioError("the static model has no loop trip")
    target = plan.pick(rng, TU)
    plan.mark(target, "repeated_stop_without_sequence")
    ts = _header_ts(target)
    ent = plan.builder.trip_update_entity(loop, ts, min_time=0)
    if ent is None:
        raise ScenarioError("loop trip produced no stop time updates")
    for stu in ent["trip_update"]["stop_time_update"]:
        stu.pop("stop_sequence", None)
    doc = target.doc or {}
    doc["entity"] = [e for e in doc.get("entity") or [] if e.get("id") != ent["id"]] + [ent]
    return plan


def _valid_empty_feed(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, VP)
    plan.mark(target, "valid_empty_feed")
    (target.doc or {})["entity"] = []
    return plan


def _malformed_response(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, VP)
    body = doc_bytes(target.doc or {})
    truncated = body[: max(16, len(body) // 3)]
    bad = PlannedItem(kind="capture", at=target.at, feed_type=VP, body=truncated, parse_status="INVALID_JSON",
                      params={"scenario": "malformed_response"}, label="malformed_response:capture")
    plan.replace(target, bad)
    if params.get("with_envelope", True):
        plan.insert_after(bad, PlannedItem(
            kind="malformed_envelope", at=target.at, feed_type=VP,
            params={"scenario": "malformed_response", "feed_type": VP,
                    "value_text": '{"schema_version": 1, "message_type": "feed_snapshot", "entities": ["{\\"id'},
            label="malformed_response:envelope",
        ))
    return plan


def _timeout(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, TU)
    plan.replace(target, PlannedItem(
        kind="fetch_event", at=target.at, feed_type=TU,
        params={"scenario": "timeout", "outcome": "timeout", "http_status": None,
                "reason_code": "SOURCE_FETCH_FAILED"},
        label="timeout",
    ))
    return plan


def _paused_feed(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    minutes = float(params.get("minutes", 4))
    caps = plan.captures(VP)
    if len(caps) < 3:
        raise ScenarioError("paused_feed needs at least three vehicle_positions captures")
    first = plan.pick(rng, VP)
    window_end = first.at + timedelta(minutes=minutes)
    removed = [c for c in caps if first.at <= c.at < window_end and "scenario" not in c.params]
    if len(removed) >= len(caps) - 1:
        removed = removed[:-1] or removed
    for c in removed:
        plan.remove(c)
    plan.gaps.append({"feed_type": VP, "from": iso_utc(first.at), "to": iso_utc(window_end),
                      "reason": "paused_feed scenario (synthetic)", "removed_captures": len(removed)})
    nxt = next((c for c in plan.captures(VP, untouched=False) if c.at >= window_end), None)
    if nxt is not None and "scenario" not in nxt.params:
        plan.mark(nxt, "paused_feed", "resumed")
    return plan


def _partial_alerts(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, AL, require=lambda it: len((it.doc or {}).get("entity") or []) >= 3)
    plan.mark(target, "partial_alerts")
    entities = (target.doc or {})["entity"]
    entities.pop()                                             # one alert absent
    malformed = entities[0]["alert"]
    malformed["active_period"] = [{"start": "abc"}]            # wrong type -> entity rejected (is_partial)
    return plan


def _crash_before_pointer(plan: SynthPlan, rng: random.Random, params: Mapping[str, Any]) -> SynthPlan:
    target = plan.pick(rng, VP, require=_has_vehicles(1))
    plan.mark(target, "crash_before_pointer", "target")
    plan.insert_before(target, PlannedItem(
        kind="arm_fault", at=target.at, feed_type=VP,
        params={"scenario": "crash_before_pointer", "point": str(params.get("point", "after_ready_before_cas")),
                "match": {"feed_type": VP, "snapshot_ordinal": 1}},
        label="crash_before_pointer:arm",
    ))
    return plan


SCENARIOS: dict[str, Scenario] = {s.scenario_id: s for s in [
    Scenario("duplicate_snapshot", "republish the same VP capture", _duplicate_snapshot),
    Scenario("duplicate_observation", "new header time with identical vehicle observations", _duplicate_observation),
    Scenario("late_snapshot", "older snapshot after a newer one (new-unseen and re-delivery)", _late_snapshot),
    Scenario("invalid_gps", "latitude 95 and longitude -200", _invalid_gps),
    Scenario("future_timestamp", "vehicle timestamp 60 s ahead of the header", _future_timestamp),
    Scenario("missing_timestamp", "vehicle without timestamp", _missing_timestamp),
    Scenario("missing_required_key", "vehicle without vehicle.vehicle.id", _missing_required_key),
    Scenario("cancelled_trip", "trip schedule_relationship CANCELED without stop time updates", _cancelled_trip),
    Scenario("schedule_only_prediction", "uncertainty 300", _uncertainty_scenario("schedule_only_prediction", 300)),
    Scenario("degraded_prediction", "uncertainty 301", _uncertainty_scenario("degraded_prediction", 301)),
    Scenario("invalid_prediction", "uncertainty 360 and an event without time", _invalid_prediction),
    Scenario("unknown_uncertainty", "uncertainty absent", _uncertainty_scenario("unknown_uncertainty", None)),
    Scenario("ambiguous_service_date", "night trip without start_date", _ambiguous_service_date),
    Scenario("repeated_stop_without_sequence", "loop trip STUs without stop_sequence",
             _repeated_stop_without_sequence),
    Scenario("valid_empty_feed", "VP snapshot with entity []", _valid_empty_feed),
    Scenario("malformed_response", "invalid JSON capture plus a malformed envelope", _malformed_response),
    Scenario("timeout", "TU fetch timeout (fetch_event)", _timeout),
    Scenario("paused_feed", "VP omitted for N minutes while TU/alerts continue", _paused_feed),
    Scenario("partial_alerts", "one malformed alert, one alert absent", _partial_alerts),
    Scenario("crash_before_pointer", "arm a processor fault before a VP capture", _crash_before_pointer),
]}


def normalize_scenarios(ids: Sequence[str]) -> tuple[str, ...]:
    """Validate scenario ids and return them in registry order (deterministic)."""
    wanted = {s.strip() for s in ids if s and s.strip()}
    unknown = sorted(wanted - set(SCENARIOS))
    if unknown:
        raise ScenarioError(f"unknown scenario(s) {unknown}; available: {sorted(SCENARIOS)}")
    return tuple(sid for sid in SCENARIOS if sid in wanted)


def apply_scenarios(plan: SynthPlan, scenario_ids: Sequence[str], *, seed: int,
                    params: Mapping[str, Mapping[str, Any]] | None = None) -> SynthPlan:
    for sid in normalize_scenarios(scenario_ids):
        rng = random.Random(f"{seed}:{sid}")
        SCENARIOS[sid].apply(plan, rng, (params or {}).get(sid, {}))
        plan.applied.append(sid)
    return plan
