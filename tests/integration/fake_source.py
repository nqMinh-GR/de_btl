"""Fake MBTA realtime source for SIMULATED source faults (design E §7.2, D-E28).

The implementation lives in :mod:`transitpulse.verify.fake_source` (shared with the benchmark recovery scenario
``recovery_collector_outbox``); this module adds the body builders used by IT05, IT13 and the outbox-resume test.
Runs that use it are synthetic runs with ``scenario_id="simulated_source_fault:<case>"``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from transitpulse.verify.fake_source import (
    FEED_PATHS,
    FakeMbtaSource,
    FakeResponse,
    RequestLog,
    body_bytes,
    gtfs_rt_doc,
    vp_entity,
)

__all__ = [
    "FEED_PATHS", "FakeMbtaSource", "FakeResponse", "RequestLog", "body_bytes", "gtfs_rt_doc", "vp_entity",
    "now_epoch", "golden_vehicles_doc", "empty_doc", "padded_vehicles_doc",
]


def now_epoch() -> int:
    return int(datetime.now(UTC).timestamp())


def golden_vehicles_doc(header_ts: int, *, ages: tuple[int, int] = (10, 20)) -> dict[str, Any]:
    """Two valid vehicles on golden route R_A (trips T1/T2 of the golden static fixture)."""
    return gtfs_rt_doc(header_ts, [
        vp_entity("V1", route_id="R_A", trip_id="T1", start_date=None, lat=42.3515, lon=-71.0585,
                  timestamp=header_ts - ages[0]),
        vp_entity("V2", route_id="R_A", trip_id="T2", start_date=None, lat=42.3525, lon=-71.0575,
                  timestamp=header_ts - ages[1]),
    ])


def empty_doc(header_ts: int) -> dict[str, Any]:
    return gtfs_rt_doc(header_ts, [])


def padded_vehicles_doc(header_ts: int, padding: int) -> dict[str, Any]:
    """Valid two-vehicle snapshot whose V1 entity carries an ignored ``tp_padding`` field of ``padding`` chars."""
    doc = golden_vehicles_doc(header_ts)
    doc["entity"][0]["tp_padding"] = "x" * max(0, int(padding))
    return doc
