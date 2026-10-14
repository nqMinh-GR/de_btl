"""Units of account (md §16.2). Kafka messages, source entities, normalized candidate records and
unique records are DIFFERENT units and are never summed together.

A *normalized candidate record* is one of:
  * one VehiclePositions entity;
  * one arrival OR departure event inside a TripUpdate stop_time_update;
  * one informed_entity selector of an alert.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def count_candidates(feed_type: str, entities: Iterable[dict[str, Any]]) -> int:
    n = 0
    for e in entities:
        if feed_type == "vehicle_positions":
            n += 1
        elif feed_type == "trip_updates":
            tu = e.get("trip_update") or {}
            for stu in tu.get("stop_time_update") or []:
                n += int(isinstance(stu.get("arrival"), dict)) + int(isinstance(stu.get("departure"), dict))
        elif feed_type == "alerts":
            al = e.get("alert") or {}
            n += len(al.get("informed_entity") or [])
        else:
            raise ValueError(f"unknown feed_type {feed_type!r}")
    return n


# Identities asserted before a generation becomes READY (processing) and re-checked by tests.
ACCOUNTING_IDENTITIES: tuple[str, ...] = (
    "selected_entity_count == fully_valid + partially_valid + rejected",
    "valid_candidate_observations == unique_observations + duplicate_observations",
    "per route: snapshot_membership_count == fresh + stale + unknown_age + invalid_with_vehicle_id",
    "candidate_prediction_records == retained + rejected + not_applicable",
    "trip_instance_count == eligible_trip_count + sum(excluded_trip_counts.values())",
    "served_alerts == confirmed + inherited",
)
