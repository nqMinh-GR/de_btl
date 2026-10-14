"""Independent realtime oracle (stdlib only).

Design C §4-§7 re-implemented from scratch:
* entity schema conformance mirroring ``processing/schemas.py`` field types (a LONG field holding a string,
  e.g. ``active_period.start = "abc"``, makes the whole entity INVALID_ENTITY);
* vehicle freshness classes (invalid / unknown / fresh / stale, CLOCK_SKEW, INVALID_TIMESTAMP_UNIT);
* ``mbta_bus_uncertainty_v1``; predicted delay; ``first_future_resolved_stop_v1`` target KPI with the fixed
  exclusion order;
* alert selector predicate (conjunction inside a selector, disjunction across selectors), active state, and
  full/partial snapshot semantics (close vs. inherit).

All instants are integer epoch seconds inside this module. MUST NOT import ``transitpulse`` or ``pyspark``.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from typing import Any

from .gtfs import Scope, from_epoch, iso_epoch, resolve_stop, resolve_trip_instance, scheduled_utc, to_epoch

FEED_TYPES: tuple[str, ...] = ("vehicle_positions", "trip_updates", "alerts")

# Project policy (config/base.yaml "policy" section), restated independently.
DEFAULT_POLICY: dict[str, Any] = {
    "vehicle_stale_seconds": 120,
    "prediction_stale_seconds": 90,
    "alert_stale_seconds": 300,
    "predicted_late_seconds": 300,
    "future_tolerance_seconds": 5,
    "service_date_inference_window_seconds": 43200,
    "timestamp_ms_threshold": 100000000000,
    "uncertainty_policy": "mbta_bus_uncertainty_v1",
    "target_policy": "first_future_resolved_stop_v1",
}
FEED_STALE_POLICY_KEY: dict[str, str] = {
    "vehicle_positions": "vehicle_stale_seconds",
    "trip_updates": "prediction_stale_seconds",
    "alerts": "alert_stale_seconds",
}
KPI_EXCLUSION_ORDER: tuple[str, ...] = (
    "CANCELED_TRIP", "ADDED_TRIP", "AMBIGUOUS_SERVICE_DATE", "UNRESOLVED_TRIP", "UNRESOLVED_STOP_SEQUENCE",
    "NO_TARGET_STOP", "SCHEDULE_ONLY_PREDICTION", "DEGRADED_PREDICTION", "INVALID_PREDICTION",
    "NO_SCHEDULED_EVENT",
)


# --------------------------------------------------------------------------------------------------
# Entity schema conformance (types of processing/schemas.py, restated)
# --------------------------------------------------------------------------------------------------

_S, _I, _L, _D, _B = "string", "int", "long", "double", "bool"
_TRIP = {"trip_id": _S, "route_id": _S, "direction_id": _I, "start_time": _S, "start_date": _S,
         "schedule_relationship": _S}
_VEH = {"id": _S, "label": _S, "license_plate": _S}
_STE = {"delay": _I, "time": _L, "uncertainty": _I}
_TS = {"translation": [{"text": _S, "language": _S}]}
VP_ENTITY_SCHEMA: dict[str, Any] = {
    "id": _S, "is_deleted": _B,
    "vehicle": {
        "trip": _TRIP, "vehicle": _VEH,
        "position": {"latitude": _D, "longitude": _D, "bearing": _D, "odometer": _D, "speed": _D},
        "current_stop_sequence": _I, "stop_id": _S, "current_status": _S, "timestamp": _L,
        "congestion_level": _S, "occupancy_status": _S, "occupancy_percentage": _I,
    },
}
TU_ENTITY_SCHEMA: dict[str, Any] = {
    "id": _S, "is_deleted": _B,
    "trip_update": {
        "trip": _TRIP, "vehicle": _VEH, "timestamp": _L, "delay": _I,
        "stop_time_update": [{"stop_sequence": _I, "stop_id": _S, "arrival": _STE, "departure": _STE,
                              "schedule_relationship": _S}],
    },
}
ALERT_ENTITY_SCHEMA: dict[str, Any] = {
    "id": _S, "is_deleted": _B,
    "alert": {
        "active_period": [{"start": _L, "end": _L}],
        "informed_entity": [{"agency_id": _S, "route_id": _S, "route_type": _I, "direction_id": _I,
                             "trip": _TRIP, "stop_id": _S}],
        "cause": _S, "effect": _S, "severity_level": _S,
        "url": _TS, "header_text": _TS, "description_text": _TS, "cause_detail": _TS, "effect_detail": _TS,
    },
}


def conforms(value: Any, spec: Any) -> bool:
    """True when ``value`` can be read with the Spark type ``spec`` without a corrupt record.

    null is always accepted; a string field accepts any JSON token (Spark renders it as text); integral
    fields need a JSON integer in range; doubles accept any JSON number; structs/arrays need objects/lists.
    """
    if value is None:
        return True
    if isinstance(spec, dict):
        return isinstance(value, dict) and all(conforms(value.get(k), sub) for k, sub in spec.items())
    if isinstance(spec, list):
        return isinstance(value, list) and all(conforms(v, spec[0]) for v in value)
    if spec == _S:
        return True
    if spec == _B:
        return isinstance(value, bool)
    if spec in (_I, _L):
        if isinstance(value, bool) or not isinstance(value, int):
            return False
        limit = 2 ** 31 if spec == _I else 2 ** 63
        return -limit <= value < limit
    if spec == _D:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    raise ValueError(f"unknown spec {spec!r}")


# --------------------------------------------------------------------------------------------------
# Small classifiers
# --------------------------------------------------------------------------------------------------

def coords_valid(lat: Any, lon: Any) -> bool:
    for v in (lat, lon):
        if isinstance(v, bool) or not isinstance(v, (int, float)) or v != v:
            return False
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return False
    return not (lat == 0 and lon == 0)


def classify_uncertainty(value: Any) -> str:
    """mbta_bus_uncertainty_v1: absent -> unknown (never 0); 0..299 realtime; 300 schedule_only;
    301 degraded; > 301, negative or non-integer -> invalid."""
    if value is None:
        return "unknown_uncertainty"
    if isinstance(value, bool) or not isinstance(value, int):
        return "invalid"
    if value < 0:
        return "invalid"
    if value < 300:
        return "realtime"
    if value == 300:
        return "schedule_only"
    if value == 301:
        return "degraded"
    return "invalid"


def vehicle_freshness(position_valid: bool, observation_epoch: int | None, clock_epoch: int,
                      policy: dict[str, Any]) -> tuple[str, int | None, list[str]]:
    """(class, displayed age, flags) at ``clock_epoch``; negative age within tolerance -> CLOCK_SKEW, age 0."""
    if not position_valid:
        return "invalid", None, []
    if observation_epoch is None:
        return "unknown", None, []
    age = clock_epoch - observation_epoch
    if age < -policy["future_tolerance_seconds"]:
        return "invalid", None, ["FUTURE_TIMESTAMP"]
    if age < 0:
        return "fresh", 0, ["CLOCK_SKEW"]
    if age <= policy["vehicle_stale_seconds"]:
        return "fresh", age, []
    return "stale", age, []


def alert_active_state(periods: list[dict[str, Any]] | None, clock_epoch: int) -> str:
    """ALWAYS without periods; ACTIVE if any start <= c < end (missing start = -inf, missing end = +inf);
    else UPCOMING if any start > c; else EXPIRED."""
    if not periods:
        return "ALWAYS"
    upcoming = False
    for period in periods:
        period = period or {}
        start = period.get("start")
        end = period.get("end")
        if (start is None or start <= clock_epoch) and (end is None or clock_epoch < end):
            return "ACTIVE"
        if start is not None and start > clock_epoch:
            upcoming = True
    return "UPCOMING" if upcoming else "EXPIRED"


# --------------------------------------------------------------------------------------------------
# Vehicle positions
# --------------------------------------------------------------------------------------------------

def _vehicle_identity(record_kind: str, vid: str, trip: dict, effective_ts: int, pos: dict,
                      veh: dict) -> tuple:
    """Source fields that make one observation / state sample (no run, snapshot or capture)."""
    return (
        record_kind, vid, trip.get("trip_id"), trip.get("route_id"), trip.get("direction_id"),
        trip.get("start_date"), trip.get("start_time"), trip.get("schedule_relationship"), effective_ts,
        pos.get("latitude"), pos.get("longitude"), pos.get("bearing"), pos.get("speed"),
        veh.get("stop_id"), veh.get("current_stop_sequence"), veh.get("current_status"),
    )


def build_vehicle_generation(doc: dict, scope: Scope, metrics_as_of: int, policy: dict[str, Any],
                             parent: dict | None, label: str) -> dict[str, Any]:
    header_ts = int(doc["header"]["timestamp"])
    entities = doc.get("entity") or []
    quality: Counter[str] = Counter()
    dlq: Counter[str] = Counter()
    rejected = 0
    selected = 0
    missing_id_by_route: Counter[str] = Counter()
    candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for ent in entities:
        if not isinstance(ent, dict) or not conforms(ent, VP_ENTITY_SCHEMA):
            selected += 1
            rejected += 1
            quality["INVALID_ENTITY"] += 1
            dlq["INVALID_ENTITY"] += 1
            continue
        veh = ent.get("vehicle") or {}
        trip = veh.get("trip") or {}
        route_id = trip.get("route_id") or scope.feed.trip_to_route.get(trip.get("trip_id"))
        if route_id is None:
            quality["UNKNOWN_ROUTE_OR_TRIP"] += 1
            continue
        if route_id not in scope.route_set:
            continue   # out of scope: never selected
        selected += 1
        vid = (veh.get("vehicle") or {}).get("id")
        if vid is None or vid == "":
            rejected += 1
            quality["MISSING_VEHICLE_ID"] += 1
            dlq["MISSING_VEHICLE_ID"] += 1
            missing_id_by_route[route_id] += 1
            continue
        pos = veh.get("position") or {}
        lat, lon = pos.get("latitude"), pos.get("longitude")
        ts = veh.get("timestamp")
        reason = None
        if not coords_valid(lat, lon):
            reason = "INVALID_COORDINATES"
        elif ts is not None and ts > policy["timestamp_ms_threshold"]:
            reason = "INVALID_TIMESTAMP_UNIT"
        elif ts is not None and ts - metrics_as_of > policy["future_tolerance_seconds"]:
            reason = "FUTURE_TIMESTAMP"
        flags: list[str] = []
        if reason is not None:
            flags.append(reason)
        if ts is None:
            flags.append("MISSING_OBSERVATION_TIMESTAMP")
        elif reason is None and ts > metrics_as_of:
            flags.append("CLOCK_SKEW")
        record_kind = "observation" if ts is not None else "snapshot_state_sample"
        effective = int(ts) if ts is not None else header_ts
        candidates[str(vid)].append({
            "vehicle_id": str(vid),
            "route_id": route_id,
            "trip_id": trip.get("trip_id"),
            "latitude": lat,
            "longitude": lon,
            "timestamp": ts,
            "obs_epoch": int(ts) if (ts is not None and reason is None) else None,
            "event_epoch": effective,
            "record_kind": record_kind,
            "invalid_reason": reason,
            "flags": sorted(flags),
            "identity": _vehicle_identity(record_kind, str(vid), trip, effective, pos, veh),
            "is_inherited": False,
            "inherited_from": None,
        })

    rows: dict[str, dict[str, Any]] = {}
    for vid, cands in candidates.items():
        cands.sort(key=lambda r: (r["timestamp"] is None, -(r["timestamp"] or 0), repr(r["identity"])))
        rows[vid] = cands[0]
        quality["DUPLICATE_OBSERVATION"] += len(cands) - 1
    for row in rows.values():
        for flag in row["flags"]:
            quality[flag] += 1

    # History: every valid observation/sample of this snapshot (set semantics == idempotent upsert).
    history = [(r["vehicle_id"], r["identity"], r["event_epoch"]) for r in rows.values() if r["invalid_reason"] is None]

    # OBSERVATION_TIME_REGRESSION against the frozen parent: serve the parent's newer observation.
    if parent is not None:
        for vid, row in list(rows.items()):
            prev = parent["rows"].get(vid)
            if prev is None or row["invalid_reason"] is not None or prev["invalid_reason"] is not None:
                continue
            if row["obs_epoch"] is not None and prev["obs_epoch"] is not None and row["obs_epoch"] < prev["obs_epoch"]:
                served = dict(prev)
                served["is_inherited"] = True
                served["inherited_from"] = prev["inherited_from"] or parent["label"]
                served["flags"] = sorted(set(prev["flags"]) | {"OBSERVATION_TIME_REGRESSION"})
                rows[vid] = served
                quality["OBSERVATION_TIME_REGRESSION"] += 1

    return {
        "label": label,
        "feed_type": "vehicle_positions",
        "header_ts": header_ts,
        "metrics_as_of": metrics_as_of,
        "source_entity_count": len(entities),
        "selected_entity_count": selected,
        "rejected_entity_count": rejected,
        "is_partial": rejected > 0,
        "is_empty": selected == 0,
        "rows": rows,
        "missing_id_by_route": dict(missing_id_by_route),
        "quality": {k: v for k, v in quality.items() if v},
        "dlq": dict(dlq),
        "history": history,
    }


def _vehicle_class(row: dict[str, Any], clock_epoch: int, policy: dict[str, Any]) -> tuple[str, int | None, list[str]]:
    if row["invalid_reason"] is not None:
        return "invalid", None, []
    return vehicle_freshness(True, row["obs_epoch"], clock_epoch, policy)


def route_vehicle_metrics(gen: dict, scope: Scope, clock_epoch: int, policy: dict[str, Any]) -> dict[str, dict[str, int]]:
    """Per scope route: fresh/stale/unknown (distinct IDs), invalid (entities incl. missing ID), membership
    (distinct recognised IDs incl. invalid ones that have an ID)."""
    out = {rid: {"fresh": 0, "stale": 0, "unknown": 0, "invalid": 0, "membership": 0} for rid in scope.route_ids}
    for row in gen["rows"].values():
        metrics = out[row["route_id"]]
        metrics["membership"] += 1
        metrics[_vehicle_class(row, clock_epoch, policy)[0]] += 1
    for rid, n in gen["missing_id_by_route"].items():
        out[rid]["invalid"] += n
    return out


def serve_vehicles(gen: dict, clock_epoch: int, policy: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for vid, row in sorted(gen["rows"].items()):
        cls, age, _ = _vehicle_class(row, clock_epoch, policy)
        if row["invalid_reason"] is not None:
            out[vid] = {
                "route_id": row["route_id"],
                "class": "invalid",
                "age_s": None,
                "invalid_reason": row["invalid_reason"],
                "flags": list(row["flags"]),
            }
            continue
        out[vid] = {
            "route_id": row["route_id"],
            "trip_id": row["trip_id"],
            "class": cls,
            "age_s": age,
            "record_kind": row["record_kind"],
            "observation_time": iso_epoch(row["obs_epoch"]),
            "event_time": iso_epoch(row["event_epoch"]),
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "invalid_reason": None,
            "flags": list(row["flags"]),
        }
    return out


# --------------------------------------------------------------------------------------------------
# Trip updates: predictions, targets, KPI
# --------------------------------------------------------------------------------------------------

def trip_instance_key(trip_id: str, res: dict[str, Any], trip: dict[str, Any]) -> str:
    if res["service_date"] is not None:
        return f"{trip_id}|{res['service_date']}"
    return f"{trip_id}|?{trip.get('start_date') or 'none'}"


def kpi_reason(relationship: str, resolution: str, any_unresolved_future: bool, target: dict | None) -> str | None:
    """First matching exclusion in KPI_EXCLUSION_ORDER, or None when the trip instance is eligible."""
    if relationship == "CANCELED":
        return "CANCELED_TRIP"
    if relationship == "ADDED":
        return "ADDED_TRIP"
    if resolution == "ambiguous":
        return "AMBIGUOUS_SERVICE_DATE"
    if resolution != "resolved":
        return "UNRESOLVED_TRIP"
    if any_unresolved_future:
        return "UNRESOLVED_STOP_SEQUENCE"
    if target is None:
        return "NO_TARGET_STOP"
    if target["uncertainty_class"] == "schedule_only":
        return "SCHEDULE_ONLY_PREDICTION"
    if target["uncertainty_class"] == "degraded":
        return "DEGRADED_PREDICTION"
    if target["uncertainty_class"] == "invalid":
        return "INVALID_PREDICTION"
    if target["delay_s"] is None:
        return "NO_SCHEDULED_EVENT"
    return None


def build_trip_update_generation(doc: dict, scope: Scope, metrics_as_of: int, policy: dict[str, Any],
                                 label: str) -> dict[str, Any]:
    header_ts = int(doc["header"]["timestamp"])
    entities = doc.get("entity") or []
    tz = scope.agency_timezone
    window = policy["service_date_inference_window_seconds"]
    late_threshold = policy["predicted_late_seconds"]
    clock_dt = from_epoch(metrics_as_of)
    quality: Counter[str] = Counter()
    dlq: Counter[str] = Counter()
    rejected = 0
    selected = 0
    chosen: dict[str, tuple[tuple, dict, dict]] = {}

    for ent in entities:
        if not isinstance(ent, dict) or not conforms(ent, TU_ENTITY_SCHEMA):
            selected += 1
            rejected += 1
            quality["INVALID_ENTITY"] += 1
            dlq["INVALID_ENTITY"] += 1
            continue
        tu = ent.get("trip_update") or {}
        trip = tu.get("trip") or {}
        trip_id = trip.get("trip_id")
        if not trip_id:
            if trip.get("route_id") is not None and trip.get("route_id") not in scope.route_set:
                continue
            selected += 1
            rejected += 1
            quality["INVALID_PREDICTION"] += 1
            dlq["INVALID_PREDICTION"] += 1
            continue
        res = resolve_trip_instance(scope, trip, clock_dt, window)
        if res["route_id"] is None:
            quality["UNKNOWN_ROUTE_OR_TRIP"] += 1
            continue
        if res["route_id"] not in scope.route_set:
            continue
        selected += 1
        key = trip_instance_key(str(trip_id), res, trip)
        rank = (-(tu.get("timestamp") or 0), str(ent.get("id") or ""))
        if key not in chosen or rank < chosen[key][0]:
            chosen[key] = (rank, ent, res)

    instances: list[dict[str, Any]] = []
    predictions: list[dict[str, Any]] = []
    for key in sorted(chosen):
        _, ent, res = chosen[key]
        tu = ent["trip_update"]
        trip = tu["trip"]
        trip_id = str(trip["trip_id"])
        relationship = res["trip_relationship"]
        info = None if relationship == "ADDED" else scope.trips.get(trip_id)
        resolved = res["schedule_resolution"] == "resolved"
        service_date = date.fromisoformat(res["service_date"]) if res["service_date"] else None
        stu_targets: list[tuple[dict[str, Any], str]] = []
        for stu in tu.get("stop_time_update") or []:
            sres = resolve_stop(info, stu.get("stop_sequence"), stu.get("stop_id"))
            stu_relationship = stu.get("schedule_relationship") or "SCHEDULED"
            if sres["reason"] is not None:
                quality[sres["reason"]] += 1
            static_st = info.by_sequence.get(sres["stop_sequence"]) if (info is not None and sres["resolved"]) else None
            events: list[dict[str, Any]] = []
            for event_type in ("arrival", "departure"):
                event = stu.get(event_type)
                if not isinstance(event, dict):
                    continue
                uncertainty = event.get("uncertainty")
                uclass = classify_uncertainty(uncertainty)
                scheduled = None
                if (resolved and relationship not in ("CANCELED", "ADDED") and static_st is not None
                        and service_date is not None):
                    secs = static_st.arrival_secs if event_type == "arrival" else static_st.departure_secs
                    if secs is not None:
                        scheduled = to_epoch(scheduled_utc(service_date, secs, tz))
                t = event.get("time")
                if t is None:
                    if event.get("delay") is not None and scheduled is not None:
                        t = scheduled + int(event["delay"])      # time_origin = derived_from_delay
                    else:
                        quality["NO_PREDICTION_DATA"] += 1
                        continue
                elif t > policy["timestamp_ms_threshold"]:
                    quality["INVALID_PREDICTION"] += 1         # INVALID_TIMESTAMP_UNIT: never converted
                    continue
                delay = None
                if (scheduled is not None and stu_relationship == "SCHEDULED"
                        and uclass not in ("schedule_only", "invalid")):
                    delay = int(t) - scheduled
                pred = {
                    "trip_id": trip_id,
                    "route_id": res["route_id"],
                    "stop_id": sres["stop_id"],
                    "stop_sequence": sres["stop_sequence"],
                    "stop_resolved": sres["resolved"],
                    "event_type": event_type,
                    "predicted_at": int(t),
                    "scheduled_at": scheduled,
                    "delay_s": delay,
                    "uncertainty": uncertainty,
                    "uncertainty_class": uclass,
                    "schedule_resolution": res["schedule_resolution"],
                }
                events.append(pred)
                predictions.append(pred)
            if events:
                # target event of this STU: ARRIVAL if present, else DEPARTURE (arrival is appended first)
                stu_targets.append((events[0], stu_relationship))

        future = [(ev, rel) for ev, rel in stu_targets
                  if ev["predicted_at"] > metrics_as_of and rel not in ("SKIPPED", "NO_DATA")]
        any_unresolved = any(not ev["stop_resolved"] for ev, _ in future)
        resolved_future = sorted((ev for ev, _ in future if ev["stop_resolved"]), key=lambda e: e["stop_sequence"])
        target = resolved_future[0] if resolved_future else None
        reason = kpi_reason(relationship, res["schedule_resolution"], any_unresolved, target)
        instances.append({"key": key, "trip_id": trip_id, "route_id": res["route_id"], "res": res,
                          "target": target, "reason": reason})

    route_metrics: dict[str, dict[str, Any]] = {}
    for rid in scope.route_ids:
        insts = [i for i in instances if i["route_id"] == rid]
        eligible = [i for i in insts if i["reason"] is None]
        n = len(eligible)
        late = sum(1 for i in eligible if i["target"]["delay_s"] >= late_threshold)
        excluded = Counter(i["reason"] for i in insts if i["reason"] is not None)
        route_metrics[rid] = {
            "trip_instances": len(insts),
            "eligible": n,
            "late": late,
            "rate": (late / n) if n else None,
            "mean": (sum(i["target"]["delay_s"] for i in eligible) / n) if n else None,
            "excluded": dict(sorted(excluded.items())),
            "excluded_total": sum(excluded.values()),
            "unknown_uncertainty": sum(1 for i in eligible if i["target"]["uncertainty_class"] == "unknown_uncertainty"),
        }

    targets: dict[str, dict[str, Any]] = {}
    for inst in instances:
        tgt = inst["target"]
        res = inst["res"]
        eligible = inst["reason"] is None
        entry = {
            "route_id": inst["route_id"],
            "trip_relationship": res["trip_relationship"],
            "schedule_resolution": res["schedule_resolution"],
            "resolution_method": res["resolution_method"],
            "service_date": res["service_date"],
            "stop_sequence": tgt["stop_sequence"] if tgt else None,
            "stop_id": tgt["stop_id"] if tgt else None,
            "event_type": tgt["event_type"] if tgt else None,
            "delay_s": tgt["delay_s"] if tgt else None,
            "uncertainty_class": tgt["uncertainty_class"] if tgt else None,
            "kpi_reason": inst["reason"],
            "late": (tgt["delay_s"] >= late_threshold) if (eligible and tgt is not None) else None,
        }
        name = inst["trip_id"] if inst["trip_id"] not in targets else inst["key"]
        targets[name] = entry

    predictions.sort(key=lambda p: (p["trip_id"], p["predicted_at"], 0 if p["event_type"] == "arrival" else 1,
                                    p["stop_id"] or ""))
    predictions_out = [
        dict(p, predicted_at=iso_epoch(p["predicted_at"]), scheduled_at=iso_epoch(p["scheduled_at"]))
        for p in predictions
    ]

    return {
        "label": label,
        "feed_type": "trip_updates",
        "header_ts": header_ts,
        "metrics_as_of": metrics_as_of,
        "source_entity_count": len(entities),
        "selected_entity_count": selected,
        "rejected_entity_count": rejected,
        "is_partial": rejected > 0,
        "is_empty": selected == 0,
        "route_metrics": route_metrics,
        "targets": dict(sorted(targets.items())),
        "predictions": predictions_out,
        "quality": {k: v for k, v in quality.items() if v},
        "dlq": dict(dlq),
    }


# --------------------------------------------------------------------------------------------------
# Alerts
# --------------------------------------------------------------------------------------------------

_SELECTOR_FIELDS = ("agency_id", "route_id", "route_type", "direction_id", "trip", "stop_id")


def evaluate_selector(selector: dict[str, Any], scope: Scope) -> tuple[str, set[str]]:
    """Return (status, matched scope routes); status in {"ok", "invalid", "unresolved"}.

    Conjunction of every present field; a parent station expands to its children; an unknown stop_id or
    trip_id without any route_id is unresolved (never assigned to a route at random)."""
    fields = {k: selector.get(k) for k in _SELECTOR_FIELDS if selector.get(k) is not None}
    trip = fields.get("trip")
    if isinstance(trip, dict):
        trip = {k: v for k, v in trip.items() if v is not None}
        if not trip:
            fields.pop("trip")
            trip = None
    if not fields:
        return "invalid", set()

    trip_route: str | None = None
    if trip:
        trip_id = trip.get("trip_id")
        if trip_id is not None and trip_id in scope.feed.trip_to_route:
            trip_route = scope.feed.trip_to_route[trip_id]
        elif trip.get("route_id") is not None:
            trip_route = trip["route_id"]
        else:
            return "unresolved", set()

    stop_set: set[str] | None = None
    stop_id = fields.get("stop_id")
    if stop_id is not None:
        if stop_id in scope.feed.stops:
            stop_set = {stop_id} | set(scope.feed.children.get(stop_id, set()))
        elif fields.get("route_id") is None and trip_route is None:
            return "unresolved", set()
        else:
            stop_set = {stop_id}   # unknown stop with a route: cannot be served by that route

    direction = fields.get("direction_id")
    if direction is None and trip:
        direction = trip.get("direction_id")

    matched: set[str] = set()
    for rid in scope.route_ids:
        if "agency_id" in fields and fields["agency_id"] != scope.agency_by_route[rid]:
            continue
        if "route_type" in fields and fields["route_type"] != scope.route_type_by_route[rid]:
            continue
        if "route_id" in fields and fields["route_id"] != rid:
            continue
        if trip and trip_route != rid:
            continue
        if direction is not None and direction not in scope.route_directions[rid]:
            continue
        if stop_set is not None:
            served = scope.route_stop_dirs[rid]
            if not any(s in served and (direction is None or direction in served[s]) for s in stop_set):
                continue
        matched.add(rid)
    return "ok", matched


def build_alert_generation(doc: dict, scope: Scope, metrics_as_of: int, policy: dict[str, Any],
                           parent: dict | None, label: str) -> dict[str, Any]:
    header_ts = int(doc["header"]["timestamp"])
    entities = doc.get("entity") or []
    valid: dict[str, dict[str, Any]] = {}
    alert_quality: dict[str, list[str]] = {}
    dlq: Counter[str] = Counter()
    rejected = 0
    for ent in entities:
        alert_id = ent.get("id") if isinstance(ent, dict) else None
        if (not isinstance(ent, dict) or not conforms(ent, ALERT_ENTITY_SCHEMA)
                or not isinstance(alert_id, str) or not alert_id):
            rejected += 1
            dlq["INVALID_ENTITY"] += 1
            if isinstance(alert_id, str) and alert_id:
                alert_quality[alert_id] = ["INVALID_ENTITY"]
            continue
        valid[alert_id] = ent.get("alert") or {}
    is_partial = rejected > 0

    parent_rows: dict[str, dict[str, dict[str, Any]]] = parent["route_rows"] if parent is not None else {}
    route_rows: dict[str, dict[str, dict[str, Any]]] = {rid: {} for rid in scope.route_ids}
    selector_resolution: dict[str, str] = {}
    for alert_id in sorted(valid):
        alert = valid[alert_id]
        reasons: set[str] = set()
        matched: set[str] = set()
        selectors = alert.get("informed_entity") or []
        bad = 0
        for selector in selectors:
            status, routes = evaluate_selector(selector or {}, scope)
            if status == "invalid":
                reasons.add("INVALID_SELECTOR")
                bad += 1
            elif status == "unresolved":
                reasons.add("UNRESOLVED_ALERT_SELECTOR")
                bad += 1
            elif not routes:
                reasons.add("NO_SCOPE_MATCH")
            matched |= routes
        if not selectors:
            reasons.add("INVALID_SELECTOR")
            selector_resolution[alert_id] = "unresolved"
        else:
            selector_resolution[alert_id] = "resolved" if bad == 0 else ("unresolved" if bad == len(selectors) else "partial")
        if reasons:
            alert_quality[alert_id] = sorted(reasons)
        for rid in matched:
            previous = parent_rows.get(rid, {}).get(alert_id)
            route_rows[rid][alert_id] = {
                "alert": alert,
                "confirmation": "CONFIRMED",
                "inherited_from": None,
                "first_seen": previous["first_seen"] if previous is not None else label,
            }

    closed: dict[str, dict[str, Any]] = {}
    if parent is not None:
        for rid in scope.route_ids:
            for alert_id, prow in parent_rows.get(rid, {}).items():
                if alert_id in valid:
                    continue
                if is_partial:
                    # partial: absent or rejected alerts are carried, unconfirmed; nothing is closed
                    route_rows[rid][alert_id] = {
                        "alert": prow["alert"],
                        "confirmation": "UNCONFIRMED_INHERITED",
                        "inherited_from": prow["inherited_from"] or parent["label"],
                        "first_seen": prow["first_seen"],
                    }
                else:
                    closed[alert_id] = {"closed_reason": "ABSENT_FROM_FULL_SNAPSHOT",
                                        "closed_at": iso_epoch(header_ts), "closed_by": label}

    return {
        "label": label,
        "feed_type": "alerts",
        "header_ts": header_ts,
        "metrics_as_of": metrics_as_of,
        "source_entity_count": len(entities),
        "selected_entity_count": len(entities),
        "rejected_entity_count": rejected,
        "is_partial": is_partial,
        "is_empty": len(entities) == 0,
        "route_rows": route_rows,
        "selector_resolution": selector_resolution,
        "alert_quality": alert_quality,
        "closed": closed,
        "dlq": dict(dlq),
    }


def serve_alerts(gen: dict, clock_epoch: int) -> dict[str, dict[str, dict[str, Any]]]:
    return {
        rid: {
            alert_id: {
                "state": alert_active_state(row["alert"].get("active_period"), clock_epoch),
                "confirmation": row["confirmation"],
                "inherited_from": row["inherited_from"],
                "first_seen": row["first_seen"],
            }
            for alert_id, row in sorted(rows.items())
        }
        for rid, rows in gen["route_rows"].items()
    }
