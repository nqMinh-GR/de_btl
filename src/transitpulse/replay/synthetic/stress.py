"""``synth-generate --kind stress``: target normalized-candidate rates (design E §3.5, D-E17, PDF p23).

A *normalized candidate record* is counted by :func:`contracts.accounting.count_candidates` (one VP entity;
one arrival OR departure event of a stop_time_update; one alert informed_entity selector).

Every ``interval_seconds`` of source time the recording holds one VehiclePositions and one TripUpdates
snapshot whose candidate counts add up EXACTLY to ``rate × interval_seconds`` with an 80/20 TU/VP split.
Played at speed 1 this is ``rate`` candidates per second. The static GTFS contains replicated trips so
every realtime trip/stop reference exists in static; every snapshot has a new header time and moved
vehicles, so no snapshot is a duplicate. Transformation and labels are recorded in the manifest: this is
synthetic load, NOT MBTA vehicle counts.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any

from transitpulse.config import ProjectConfig
from transitpulse.contracts.accounting import count_candidates
from transitpulse.contracts.envelope import iso_utc, parse_iso_utc
from transitpulse.replay.synthetic.feed_builder import header, scheduled_epoch
from transitpulse.replay.synthetic.gtfs_builder import StaticModel, SynTrip, bearing_deg, build_stress_static_model, unit
from transitpulse.replay.synthetic.recording import (
    GENERATOR_VERSION,
    PlannedItem,
    RecordingResult,
    RecordingSpec,
    find_reusable,
    inputs_sha256,
    write_recording,
)
from transitpulse.runtime.paths import WorkPaths

STRESS_RATES: tuple[int, ...] = (100, 500, 1000)
TU_SHARE = 0.8
STOPS_PER_TRIP = 10
DEFAULT_INTERVAL_SECONDS = 5
DEFAULT_STRESS_DURATION_SECONDS = 300
MAX_STRESS_DURATION_SECONDS = 3600
FIXTURE_KIND_STRESS = "stress"
TRANSFORMATION = (
    "Synthetic stress load: replicated static trips (5 synthetic routes x N trips, 10 stops each) and generated "
    "vehicles/predictions sized so that each interval carries exactly rate x interval normalized candidate "
    "records (80% trip_update arrival/departure events, 20% vehicle entities). Not MBTA data; not real vehicle "
    "counts."
)


def split_counts(rate: int, interval_seconds: int) -> tuple[int, int]:
    """(vehicle_positions candidates, trip_updates candidates) per interval; sums to rate*interval exactly."""
    total = int(rate) * int(interval_seconds)
    vp = int(round(total * (1.0 - TU_SHARE)))
    return vp, total - vp


def tu_layout(tu_candidates: int, stops_per_trip: int = STOPS_PER_TRIP) -> list[tuple[int, int]]:
    """Per trip: (stop_time_updates with arrival+departure, extra STU with arrival only 0/1)."""
    per_trip = 2 * stops_per_trip
    full, rest = divmod(int(tu_candidates), per_trip)
    layout = [(stops_per_trip, 0)] * full
    if rest:
        layout.append((rest // 2, rest % 2))
    return layout


class StressFeeds:
    def __init__(self, model: StaticModel, *, seed: int, service_date: date, vp_count: int,
                 tu_layout_: list[tuple[int, int]]) -> None:
        self.model = model
        self.seed = seed
        self.service_date = service_date
        # interleave routes so every route carries load: (k, route) order
        trips = sorted(model.trips.values(), key=lambda t: (int(t.trip_id.rsplit("ST", 1)[1]), t.route_id))
        if len(trips) < max(vp_count, len(tu_layout_)):
            raise ValueError("stress static model has too few trips")
        self.vp_trips = trips[:vp_count]
        self.tu_trips = list(reversed(trips))[: len(tu_layout_)]
        self.layout = tu_layout_
        self.sched = {t.trip_id: tuple(scheduled_epoch(service_date, s, model.agency_timezone) for s in t.stop_secs)
                      for t in trips}

    def _descriptor(self, trip: SynTrip) -> dict[str, Any]:
        return {"trip_id": trip.trip_id, "route_id": trip.route_id, "direction_id": trip.direction_id,
                "start_date": self.service_date.strftime("%Y%m%d"), "start_time": trip.start_time,
                "schedule_relationship": "SCHEDULED"}

    def vehicle_positions(self, at: datetime, snapshot_index: int) -> dict[str, Any]:
        ts = int(at.timestamp())
        entities = []
        for i, trip in enumerate(self.vp_trips):
            stops = trip.stop_ids
            phase = (snapshot_index * 0.01 + unit(self.seed, "phase", trip.trip_id)) % 1.0
            pos = phase * (len(stops) - 1)
            k = min(int(math.floor(pos)), len(stops) - 2)
            frac = pos - k
            a, b = self.model.stops[stops[k]], self.model.stops[stops[k + 1]]
            vid = f"SYN-STR-{i:05d}"
            entities.append({
                "id": vid,
                "vehicle": {
                    "trip": self._descriptor(trip),
                    "vehicle": {"id": vid, "label": f"STR{i:05d}"},
                    "position": {
                        "latitude": round(a.lat + (b.lat - a.lat) * frac, 6),
                        "longitude": round(a.lon + (b.lon - a.lon) * frac, 6),
                        "bearing": round(bearing_deg(a.lat, a.lon, b.lat, b.lon), 1),
                        "speed": round(5.0 + 5.0 * unit(self.seed, "speed", trip.trip_id), 2),
                    },
                    "current_stop_sequence": k + 2,
                    "stop_id": stops[k + 1],
                    "current_status": "IN_TRANSIT_TO",
                    "timestamp": ts - (i % 7),
                },
            })
        return {"header": header(ts), "entity": entities}

    def trip_updates(self, at: datetime, snapshot_index: int) -> dict[str, Any]:
        ts = int(at.timestamp())
        entities = []
        for j, (trip, (full_stus, arrival_only)) in enumerate(zip(self.tu_trips, self.layout, strict=True)):
            sched = self.sched[trip.trip_id]
            delay = int(unit(self.seed, "delay", trip.trip_id) * 420) - 60 + (snapshot_index % 5) * 3
            stus: list[dict[str, Any]] = []
            for k in range(full_stus):
                t = sched[k] + delay
                stus.append({"stop_sequence": k + 1, "stop_id": trip.stop_ids[k],
                             "arrival": {"time": t}, "departure": {"time": t},
                             "schedule_relationship": "SCHEDULED"})
            if arrival_only:
                k = full_stus
                stus.append({"stop_sequence": k + 1, "stop_id": trip.stop_ids[k],
                             "arrival": {"time": sched[k] + delay}, "schedule_relationship": "SCHEDULED"})
            entities.append({
                "id": f"TU-{trip.trip_id}",
                "trip_update": {"trip": self._descriptor(trip), "timestamp": ts, "stop_time_update": stus},
            })
        return {"header": header(ts), "entity": entities}


def generate_stress(
    cfg: ProjectConfig,
    paths: WorkPaths,
    *,
    rate: int,
    seed: int,
    duration_seconds: int = DEFAULT_STRESS_DURATION_SECONDS,
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
) -> RecordingResult:
    if int(rate) not in STRESS_RATES:
        raise ValueError(f"stress rate must be one of {list(STRESS_RATES)} candidates/s, got {rate}")
    if not interval_seconds <= int(duration_seconds) <= MAX_STRESS_DURATION_SECONDS:
        raise ValueError(f"stress duration must be within {interval_seconds}..{MAX_STRESS_DURATION_SECONDS} s")
    rate, duration, interval = int(rate), int(duration_seconds), int(interval_seconds)
    route_ids = tuple(cfg.synthetic.route_ids)
    service_date = date.fromisoformat(cfg.synthetic.service_date)
    start = parse_iso_utc(cfg.synthetic.start_clock)
    vp_count, tu_count = split_counts(rate, interval)
    layout = tu_layout(tu_count)
    inputs = {
        "kind": "stress", "rate": rate, "seed": int(seed), "duration_seconds": duration,
        "interval_seconds": interval, "route_ids": list(route_ids), "service_date": service_date.isoformat(),
        "start_clock": iso_utc(start), "scope_policy_version": cfg.scope.scope_policy_version,
        "generator_version": GENERATOR_VERSION,
    }
    in_hash = inputs_sha256(inputs)
    recording_id = f"syn-stress-r{rate}-s{int(seed)}-{in_hash[:12]}"
    existing = find_reusable(paths.recordings_root, recording_id, in_hash)
    if existing is not None:
        return existing

    trips_needed = max(vp_count, len(layout))
    trips_per_route = max(1, math.ceil(trips_needed / len(route_ids)))
    model = build_stress_static_model(route_ids, seed=int(seed), service_date=service_date,
                                      trips_per_route=trips_per_route, stops_per_trip=STOPS_PER_TRIP)
    feeds = StressFeeds(model, seed=int(seed), service_date=service_date, vp_count=vp_count, tu_layout_=layout)
    items: list[PlannedItem] = []
    for n in range(duration // interval):
        at = start + timedelta(seconds=n * interval)
        vp_doc = feeds.vehicle_positions(at, n)
        tu_doc = feeds.trip_updates(at, n)
        vp_n = count_candidates("vehicle_positions", vp_doc["entity"])
        tu_n = count_candidates("trip_updates", tu_doc["entity"])
        if vp_n != vp_count or tu_n != tu_count:
            raise RuntimeError(f"stress generator invariant violated: vp {vp_n}/{vp_count}, tu {tu_n}/{tu_count}")
        items.append(PlannedItem(kind="capture", at=at, feed_type="vehicle_positions", doc=vp_doc,
                                 params={"candidate_count": vp_n, "target_rate": rate}))
        items.append(PlannedItem(kind="capture", at=at, feed_type="trip_updates", doc=tu_doc,
                                 params={"candidate_count": tu_n, "target_rate": rate}))
    spec = RecordingSpec(
        recording_id=recording_id,
        fixture_kind=FIXTURE_KIND_STRESS,
        seed=int(seed),
        scenario_ids=(f"stress_{rate}",),
        provenance=(f"synthetic stress load ({GENERATOR_VERSION}, seed {int(seed)}, {rate} normalized candidates/s); "
                    "NOT MBTA responses"),
        attribution="Synthetic stress data generated by TransitPulse. Not MBTA data; not real vehicle counts.",
        static_zip=model.zip_bytes(),
        static_feed_version=model.feed_version,
        static_source_url="synthetic://transitpulse/static-stress",
        route_ids=route_ids,
        route_type=cfg.scope.route_type,
        scope_policy_version=cfg.scope.scope_policy_version,
        poll_interval_seconds=float(interval),
        feed_source_urls={f: f"synthetic://transitpulse/stress/{f}" for f in ("vehicle_positions", "trip_updates",
                                                                               "alerts")},
        start_clock=start,
        generator={"kind": "stress", "inputs_sha256": in_hash, "inputs": inputs},
        extra={"stress": {
            "rate_candidates_per_second": rate,
            "interval_seconds": interval,
            "duration_seconds": duration,
            "snapshots_per_feed": duration // interval,
            "split": {"trip_updates": TU_SHARE, "vehicle_positions": round(1.0 - TU_SHARE, 6)},
            "per_snapshot_candidates": {"vehicle_positions": vp_count, "trip_updates": tu_count},
            "trips_per_route": trips_per_route,
            "unit": "normalized candidate records (contracts.accounting.count_candidates)",
            "transformation": TRANSFORMATION,
        }},
    )
    return write_recording(paths.recordings_root, spec, items)
