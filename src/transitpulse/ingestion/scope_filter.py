"""Coarse scope filter applied by the collector before building an envelope (design E §1.4, D-E09/D-E10).

The filter only decides which source entities are *copied* into the snapshot envelope. Exact route
resolution, schedule joins and alert selector resolution happen in Spark with the full static dims.
Selected entities are returned unchanged and in source order.

VehiclePositions / TripUpdates
    * descriptor ``trip.route_id`` present: selected iff the route is in scope (``by_route_id``),
      otherwise ``out_of_scope``;
    * else ``trip.trip_id`` mapped through ``trip_to_route`` (every trip of the GTFS version): selected
      iff the mapped route is in scope (``by_trip_lookup``), otherwise ``out_of_scope``;
    * no descriptor, or an unknown trip: ``unresolved`` (quality reason ``UNKNOWN_ROUTE_OR_TRIP``);
      such an entity is never assigned to a route.

Alerts (lead decision: drop an alert only when every selector certainly cannot apply)
    An alert is selected when ANY informed_entity selector *may apply*: every field the selector
    specifies must be compatible (fields are AND-ed, selectors are OR-ed):

    * ``route_id`` in scope; ``route_type`` equal to a scope route type (3 for buses);
    * ``agency_id`` among the scope agencies;
    * ``stop_id`` in the scope stops (parent stations included, children via ``parent_station``), or
      unknown to static: kept and flagged as an unresolved reference so that Spark can emit
      ``UNRESOLVED_ALERT_SELECTOR`` / ``NO_SCOPE_MATCH``. A stop is *known outside the scope* only when
      it appears in the GTFS ``parent_station`` map (as child or parent) and neither it nor its parent
      is a scope stop; only then does the stop make the selector fail;
    * ``trip``: ``trip.route_id`` in scope; ``trip.trip_id`` resolving to a scope route, or unknown to
      static (kept and flagged);
    * a malformed value (e.g. a non-string ``route_id``/``stop_id``) cannot be judged here: kept and
      flagged, Spark decides (``INVALID_SELECTOR``);
    * ``direction_id`` and other attributes never restrict.

    A selector with only ``agency_id`` or only ``route_type`` is broad and kept. A selector specifying
    no restricting field at all is invalid and selects nothing.

The accounting dict is flat ``{str: int}`` (the envelope schema reads it as ``map<string,string>``) and
satisfies ``source_entities == selected + out_of_scope + unresolved + invalid_entity`` and
``selected == sum(by_* buckets)``. The parser drops non-object ``entity`` entries before the filter
sees them; :func:`include_unparsed_entities` adds them to ``source_entities`` and ``invalid_entity`` so
that ``source_entities`` equals the envelope's ``source_entity_count`` (``ParsedFeed.source_entity_count``).
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from typing import Any, Protocol

TRIP_FEEDS: dict[str, str] = {"vehicle_positions": "vehicle", "trip_updates": "trip_update"}

TRIP_BUCKETS: tuple[str, ...] = ("by_route_id", "by_trip_lookup")
ALERT_BUCKETS: tuple[str, ...] = (
    "by_route_id", "by_trip_lookup", "by_stop_id", "by_unknown_reference", "by_broad_selector",
)
_ALERT_RANK = {name: i for i, name in enumerate(ALERT_BUCKETS)}


class ScopeIndexLike(Protocol):
    """The attributes of ``transitpulse.static.index.ScopeIndex`` used by the filter."""

    route_ids: Collection[str]
    route_type_by_route: Mapping[str, Any]
    agency_ids: Collection[str]
    trip_to_route: Mapping[str, str]
    scope_stop_ids: Collection[str]
    parent_station: Mapping[str, str]


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _nonempty_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value != "" else None


def empty_accounting(feed_type: str) -> dict[str, int]:
    base = {"source_entities": 0, "selected": 0, "out_of_scope": 0, "unresolved": 0, "invalid_entity": 0}
    if feed_type == "alerts":
        base.update({b: 0 for b in ALERT_BUCKETS})
        base.update(alerts_with_unresolved_selectors=0, selectors_total=0, selectors_may_apply=0,
                    selectors_invalid=0)
    elif feed_type in TRIP_FEEDS:
        base.update({b: 0 for b in TRIP_BUCKETS})
    else:
        raise ValueError(f"unknown feed_type {feed_type!r}")
    return base


class ScopeFilter:
    """Select the in-scope entities of one feed snapshot (pure, thread-safe after construction)."""

    def __init__(self, index: ScopeIndexLike) -> None:
        self.index = index
        self.route_ids: frozenset[str] = frozenset(str(r) for r in index.route_ids)
        types: set[int] = set()
        for route, rtype in dict(index.route_type_by_route).items():
            t = _as_int(rtype)
            if t is not None and str(route) in self.route_ids:
                types.add(t)
        self.route_types: frozenset[int] = frozenset(types) or frozenset({3})
        self.agency_ids: frozenset[str] = frozenset(str(a) for a in index.agency_ids)
        self.stop_ids: frozenset[str] = frozenset(str(s) for s in index.scope_stop_ids)
        self.parent_station: Mapping[str, str] = index.parent_station
        self.trip_to_route: Mapping[str, str] = index.trip_to_route
        # Stops provably present in the GTFS version (children and parents of the parent_station map).
        known: set[str] = set()
        for child, parent in dict(index.parent_station).items():
            known.add(str(child))
            if parent:
                known.add(str(parent))
        self.known_stop_ids: frozenset[str] = frozenset(known) | self.stop_ids

    # ------------------------------------------------------------------ public API
    def select(self, feed_type: str, entities: Iterable[Any]) -> tuple[list[dict[str, Any]], dict[str, int]]:
        if feed_type in TRIP_FEEDS:
            return self._select_trip_feed(feed_type, entities)
        if feed_type == "alerts":
            return self._select_alerts(entities)
        raise ValueError(f"unknown feed_type {feed_type!r}")

    def resolve_trip(self, trip: Any) -> tuple[str, str | None]:
        """Classify a TripDescriptor: (bucket|'out_of_scope'|'unresolved', route_id or None)."""
        if not isinstance(trip, dict):
            return "unresolved", None
        route_id = _nonempty_str(trip.get("route_id"))
        if route_id is not None:
            return ("by_route_id" if route_id in self.route_ids else "out_of_scope"), route_id
        trip_id = _nonempty_str(trip.get("trip_id"))
        if trip_id is not None:
            mapped = self.trip_to_route.get(trip_id)
            if mapped is None:
                return "unresolved", None
            return ("by_trip_lookup" if mapped in self.route_ids else "out_of_scope"), mapped
        return "unresolved", None

    def stop_in_scope(self, stop_id: str) -> bool:
        if stop_id in self.stop_ids:
            return True
        parent = self.parent_station.get(stop_id)
        return bool(parent) and parent in self.stop_ids

    def stop_status(self, stop_id: str) -> str:
        """``in_scope``, ``out_of_scope`` (known to static, certainly not a scope stop) or ``unknown``."""
        if self.stop_in_scope(stop_id):
            return "in_scope"
        return "out_of_scope" if stop_id in self.known_stop_ids else "unknown"

    def selector_verdict(self, selector: Any) -> tuple[str, str | None, bool]:
        """Return ``(status, bucket, unknown_reference)`` for one informed_entity.

        ``status`` is ``applies``, ``no_match`` (the selector certainly cannot apply) or ``invalid``
        (no restricting field). ``unknown_reference`` flags a kept selector with an unknown or
        malformed stop/trip/route reference.
        """
        if not isinstance(selector, dict):
            return "invalid", None, False
        restricting = False
        route_match = trip_match = stop_match = broad = unknown = False

        if "route_id" in selector and selector["route_id"] is not None:
            restricting = True
            rid = _nonempty_str(selector["route_id"])
            if rid is None:
                unknown = True                       # malformed value: Spark decides (INVALID_SELECTOR)
            elif rid not in self.route_ids:
                return "no_match", None, False
            else:
                route_match = True
        if "route_type" in selector and selector["route_type"] is not None:
            restricting = True
            rtype = _as_int(selector["route_type"])
            if rtype is None or rtype not in self.route_types:
                return "no_match", None, False
            broad = True
        if "agency_id" in selector and selector["agency_id"] is not None:
            restricting = True
            if str(selector["agency_id"]) not in self.agency_ids:
                return "no_match", None, False
            broad = True
        if "trip" in selector and selector["trip"] is not None:
            trip = selector["trip"]
            if not isinstance(trip, dict):
                return "invalid", None, False
            trip_route = _nonempty_str(trip.get("route_id"))
            trip_id = _nonempty_str(trip.get("trip_id"))
            if trip_route is not None:
                restricting = True
                if trip_route not in self.route_ids:
                    return "no_match", None, False
                route_match = True
            if trip_id is not None:
                restricting = True
                mapped = self.trip_to_route.get(trip_id)
                if mapped is None:
                    unknown = True
                elif mapped not in self.route_ids:
                    return "no_match", None, False
                else:
                    trip_match = True
        if "stop_id" in selector and selector["stop_id"] is not None:
            restricting = True
            sid = _nonempty_str(selector["stop_id"])
            status = self.stop_status(sid) if sid is not None else "unknown"
            if status == "out_of_scope":
                return "no_match", None, False
            if status == "in_scope":
                stop_match = True
            else:
                unknown = True                       # unknown/malformed stop: kept and flagged
        if not restricting:
            return "invalid", None, False
        if route_match:
            bucket = "by_route_id"
        elif trip_match:
            bucket = "by_trip_lookup"
        elif stop_match:
            bucket = "by_stop_id"
        elif unknown:
            bucket = "by_unknown_reference"
        else:
            bucket = "by_broad_selector" if broad else "by_unknown_reference"
        return "applies", bucket, unknown

    # ------------------------------------------------------------------ internals
    def _select_trip_feed(self, feed_type: str, entities: Iterable[Any]) -> tuple[list[dict[str, Any]], dict[str, int]]:
        key = TRIP_FEEDS[feed_type]
        acc = empty_accounting(feed_type)
        selected: list[dict[str, Any]] = []
        for ent in entities:
            acc["source_entities"] += 1
            if not isinstance(ent, dict):
                acc["invalid_entity"] += 1
                continue
            body = ent.get(key)
            trip = body.get("trip") if isinstance(body, dict) else None
            decision, _route = self.resolve_trip(trip)
            if decision in TRIP_BUCKETS:
                acc[decision] += 1
                acc["selected"] += 1
                selected.append(ent)
            else:
                acc[decision] += 1
        return selected, acc

    def _select_alerts(self, entities: Iterable[Any]) -> tuple[list[dict[str, Any]], dict[str, int]]:
        acc = empty_accounting("alerts")
        selected: list[dict[str, Any]] = []
        for ent in entities:
            acc["source_entities"] += 1
            alert = ent.get("alert") if isinstance(ent, dict) else None
            if not isinstance(alert, dict):
                acc["invalid_entity"] += 1
                continue
            selectors = alert.get("informed_entity")
            if not isinstance(selectors, list) or not selectors:
                acc["unresolved"] += 1
                continue
            best: str | None = None
            any_valid = False
            has_unknown = False
            for sel in selectors:
                acc["selectors_total"] += 1
                status, bucket, unknown = self.selector_verdict(sel)
                if status == "invalid":
                    acc["selectors_invalid"] += 1
                    continue
                any_valid = True
                if status == "applies" and bucket is not None:
                    acc["selectors_may_apply"] += 1
                    has_unknown = has_unknown or unknown
                    if best is None or _ALERT_RANK[bucket] < _ALERT_RANK[best]:
                        best = bucket
            if best is not None:
                acc[best] += 1
                acc["selected"] += 1
                if has_unknown:
                    acc["alerts_with_unresolved_selectors"] += 1
                selected.append(ent)
            elif any_valid:
                acc["out_of_scope"] += 1
            else:
                acc["unresolved"] += 1
        return selected, acc


def include_unparsed_entities(accounting: Mapping[str, Any], source_entity_count: int) -> dict[str, Any]:
    """Copy of a ScopeFilter accounting where source entries the filter never saw count as invalid.

    ``source_entity_count`` is ``ParsedFeed.source_entity_count`` (every entry of the source ``entity``
    list). The difference to ``accounting["source_entities"]`` is the number of non-object entries the
    parser could not keep; it is added to both ``source_entities`` and ``invalid_entity`` so the identity
    ``source_entities == selected + out_of_scope + unresolved + invalid_entity`` holds against the
    envelope. Idempotent (an already reconciled accounting is returned unchanged); a dict that is not a
    ScopeFilter accounting (no ``source_entities`` key) is copied as is.
    """
    out = dict(accounting)
    if "source_entities" not in out:
        return out
    missing = int(source_entity_count) - int(out["source_entities"])
    if missing > 0:
        out["source_entities"] = int(out["source_entities"]) + missing
        out["invalid_entity"] = int(out.get("invalid_entity", 0)) + missing
    return out


def quality_counts(feed_type: str, accounting: Mapping[str, int]) -> dict[str, int]:
    """Collector-side quality reason counts recorded in the capture sidecar."""
    out: dict[str, int] = {}
    if feed_type in TRIP_FEEDS and accounting.get("unresolved"):
        out["UNKNOWN_ROUTE_OR_TRIP"] = int(accounting["unresolved"])
    if feed_type == "alerts" and accounting.get("alerts_with_unresolved_selectors"):
        out["UNRESOLVED_ALERT_SELECTOR"] = int(accounting["alerts_with_unresolved_selectors"])
    return out
