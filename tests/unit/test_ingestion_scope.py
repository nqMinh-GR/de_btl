"""Unit tests for the collector's coarse scope filter (ingestion.scope_filter)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from transitpulse.ingestion.scope_filter import (
    ALERT_BUCKETS,
    ScopeFilter,
    empty_accounting,
    include_unparsed_entities,
    quality_counts,
)

INDEX = SimpleNamespace(
    gtfs_version="g" * 64,
    scope_id="scope0001",
    route_ids=frozenset({"R1", "R2"}),
    route_type_by_route={"R1": 3, "R2": 3},
    agency_by_route={"R1": "1", "R2": "1"},
    agency_ids=frozenset({"1"}),
    agency_timezone="America/New_York",
    trip_to_route={"T1": "R1", "T2": "R2", "T9": "R9"},
    scope_stop_ids=frozenset({"S1", "PARENT1"}),
    stops_by_route={"R1": ["S1"], "R2": ["S1"]},
    parent_station={"S1": "PARENT1", "C1": "PARENT1", "X1": "PX"},
)


def vp(eid: str, trip: Any) -> dict[str, Any]:
    body: dict[str, Any] = {"vehicle": {"id": eid}}
    if trip is not None:
        body["trip"] = trip
    return {"id": eid, "vehicle": body}


def tu(eid: str, trip: Any) -> dict[str, Any]:
    return {"id": eid, "trip_update": {"trip": trip, "stop_time_update": []}}


def alert(aid: str, selectors: Any) -> dict[str, Any]:
    body: dict[str, Any] = {"effect": "DETOUR"}
    if selectors is not ...:
        body["informed_entity"] = selectors
    return {"id": aid, "alert": body}


@pytest.fixture()
def scope() -> ScopeFilter:
    return ScopeFilter(INDEX)


def _identity(acc: dict[str, int], buckets: tuple[str, ...]) -> None:
    assert acc["source_entities"] == acc["selected"] + acc["out_of_scope"] + acc["unresolved"] + acc["invalid_entity"]
    assert acc["selected"] == sum(acc[b] for b in buckets)


def test_vehicle_positions_route_and_trip_resolution(scope: ScopeFilter) -> None:
    ents = [
        vp("a", {"route_id": "R1", "trip_id": "T1"}),   # descriptor route in scope
        vp("b", {"route_id": "R9"}),                    # descriptor route out of scope
        vp("c", {"trip_id": "T2"}),                     # trip lookup -> R2 in scope
        vp("d", {"trip_id": "T9"}),                     # trip lookup -> R9 out of scope
        vp("e", {"trip_id": "UNKNOWN"}),                # unknown trip -> unresolved
        vp("f", None),                                  # no descriptor -> unresolved
        "not-an-object",                                # invalid entity
        vp("g", {"route_id": "R9", "trip_id": "T1"}),   # descriptor wins over trip lookup
        vp("h", {"route_id": "", "trip_id": "T1"}),     # empty route_id treated as absent
    ]
    selected, acc = scope.select("vehicle_positions", ents)
    assert [e["id"] for e in selected] == ["a", "c", "h"]
    assert selected[0] is ents[0]                       # unchanged objects, source order
    assert acc["by_route_id"] == 1 and acc["by_trip_lookup"] == 2
    assert acc["out_of_scope"] == 3 and acc["unresolved"] == 2 and acc["invalid_entity"] == 1
    assert acc["source_entities"] == 9
    _identity(acc, ("by_route_id", "by_trip_lookup"))
    assert quality_counts("vehicle_positions", acc) == {"UNKNOWN_ROUTE_OR_TRIP": 2}
    assert all(isinstance(v, int) for v in acc.values())


def test_trip_updates_use_the_trip_update_descriptor(scope: ScopeFilter) -> None:
    ents = [tu("a", {"route_id": "R2"}), tu("b", {"trip_id": "T1"}), tu("c", {"trip_id": "nope"}),
            {"id": "d", "vehicle": {"trip": {"route_id": "R1"}}}]
    selected, acc = scope.select("trip_updates", ents)
    assert [e["id"] for e in selected] == ["a", "b"]
    assert acc["unresolved"] == 2
    _identity(acc, ("by_route_id", "by_trip_lookup"))


def test_alert_selector_predicates(scope: ScopeFilter) -> None:
    # Lead decision: drop an alert only when every selector certainly cannot apply; unknown stop/trip
    # references are kept and flagged so that Spark can emit UNRESOLVED_ALERT_SELECTOR / NO_SCOPE_MATCH.
    ents = [
        alert("route", [{"route_id": "R1"}]),
        alert("stop", [{"stop_id": "S1"}]),
        alert("child_stop", [{"stop_id": "C1"}]),
        alert("agency", [{"agency_id": "1"}]),
        alert("route_type", [{"route_type": 3}]),
        alert("other_route", [{"route_id": "R9"}]),
        alert("and_semantics", [{"route_id": "R1", "stop_id": "X1"}]),   # X1 known, outside the scope
        alert("other_agency", [{"agency_id": "2"}]),
        alert("subway", [{"route_type": 1}]),
        alert("or_semantics", [{"route_id": "R9"}, {"route_id": "R2", "direction_id": 1}]),
        alert("trip_known", [{"trip": {"trip_id": "T1"}}]),
        alert("trip_other", [{"trip": {"trip_id": "T9"}}]),
        alert("trip_unknown", [{"trip": {"trip_id": "T-NEW"}}]),
        alert("no_selectors", []),
        alert("missing_selectors", ...),
        alert("direction_only", [{"direction_id": 0}]),
        {"id": "not_alert", "vehicle": {}},
        alert("stop_outside", [{"stop_id": "X1"}]),
        alert("stop_unknown", [{"stop_id": "S_UNKNOWN"}]),                # golden A7 shape
        alert("route_with_unknown_stop", [{"route_id": "R1", "stop_id": "S_UNKNOWN"}]),
        alert("route_and_scope_stop", [{"route_id": "R1", "stop_id": "S1"}]),  # golden A6 shape
    ]
    selected, acc = scope.select("alerts", ents)
    assert [e["id"] for e in selected] == [
        "route", "stop", "child_stop", "agency", "route_type", "or_semantics", "trip_known", "trip_unknown",
        "stop_unknown", "route_with_unknown_stop", "route_and_scope_stop",
    ]
    assert acc["by_route_id"] == 4
    assert acc["by_stop_id"] == 2
    assert acc["by_broad_selector"] == 2
    assert acc["by_trip_lookup"] == 1
    assert acc["by_unknown_reference"] == 2
    assert acc["alerts_with_unresolved_selectors"] == 3
    assert acc["out_of_scope"] == 6
    assert acc["unresolved"] == 3
    assert acc["invalid_entity"] == 1
    assert acc["selectors_invalid"] == 1
    assert acc["source_entities"] == 21
    _identity(acc, ALERT_BUCKETS)
    assert quality_counts("alerts", acc) == {"UNRESOLVED_ALERT_SELECTOR": 3}


def test_selector_verdicts(scope: ScopeFilter) -> None:
    assert scope.selector_verdict({"route_id": "R1", "route_type": 3, "agency_id": "1"}) == ("applies", "by_route_id", False)
    assert scope.selector_verdict({"trip": {"route_id": "R2", "trip_id": "T-NEW"}}) == ("applies", "by_route_id", True)
    assert scope.selector_verdict({"stop_id": "S_UNKNOWN"}) == ("applies", "by_unknown_reference", True)
    assert scope.selector_verdict({"stop_id": 123}) == ("applies", "by_unknown_reference", True)
    assert scope.selector_verdict({"route_id": 7}) == ("applies", "by_unknown_reference", True)
    assert scope.selector_verdict({"agency_id": "1", "stop_id": "PX"})[0] == "no_match"   # known parent outside
    assert scope.selector_verdict({"route_id": "R1", "route_type": 1})[0] == "no_match"
    assert scope.selector_verdict({"trip": "T1"})[0] == "invalid"
    assert scope.selector_verdict("x")[0] == "invalid"
    assert scope.selector_verdict({"facility_id": "F1"})[0] == "invalid"
    assert scope.stop_status("S1") == "in_scope" and scope.stop_status("C1") == "in_scope"
    assert scope.stop_status("X1") == "out_of_scope" and scope.stop_status("NEW") == "unknown"


def test_route_types_default_to_bus_when_unknown() -> None:
    idx = SimpleNamespace(**{**vars(INDEX), "route_type_by_route": {}})
    assert ScopeFilter(idx).route_types == frozenset({3})


def test_include_unparsed_entities_restores_the_identity(scope: ScopeFilter) -> None:
    # The parser keeps only object entries; 3 non-objects were in the source list of 5 entries.
    _selected, acc = scope.select("alerts", [alert("a", [{"route_id": "R1"}]), alert("b", [{"route_id": "R9"}])])
    fixed = include_unparsed_entities(acc, 5)
    assert fixed["source_entities"] == 5 and fixed["invalid_entity"] == 3
    _identity(fixed, ALERT_BUCKETS)
    assert acc["source_entities"] == 2                                # input not mutated
    assert include_unparsed_entities(fixed, 5) == fixed               # idempotent
    assert include_unparsed_entities({"selected": 1}, 5) == {"selected": 1}   # not a filter accounting
    assert include_unparsed_entities({}, 3) == {}                    # rejected capture: no accounting


def test_empty_input_and_unknown_feed(scope: ScopeFilter) -> None:
    selected, acc = scope.select("alerts", [])
    assert selected == [] and acc == empty_accounting("alerts")
    with pytest.raises(ValueError):
        scope.select("bikes", [])
    with pytest.raises(ValueError):
        empty_accounting("bikes")
