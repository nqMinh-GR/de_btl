"""Deterministic synthetic GTFS-Realtime documents in the MBTA JSON shape (design E §3.4).

* Header ``{"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET", "timestamp": <int s>}``.
* String enums, integer epoch seconds, integer ``direction_id`` (``processing/schemas.py``).
* Vehicles move along their trip's shape: the position is interpolated between consecutive stops at the
  vehicle's observation time using schedule + a seeded per-trip delay.
* Trip updates are the schedule plus the same seeded delay (plus a small seeded per-stop drift);
  ``uncertainty`` is mostly absent (MBTA bus behaviour) and 60 for a seeded ~20% of trips.
* Alerts are a fixed set covering route/stop/agency/trip/route_type selectors and the
  ACTIVE/ALWAYS/UPCOMING/EXPIRED states.

All draws use :func:`gtfs_builder.unit` so a document depends only on (seed, static model, time).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from transitpulse.replay.synthetic.gtfs_builder import (
    AGENCY_ID,
    StaticModel,
    SynTrip,
    bearing_deg,
    haversine_m,
    unit,
)

GTFS_RT_VERSION = "2.0"
FULL_DATASET = "FULL_DATASET"
OCCUPANCY = ("MANY_SEATS_AVAILABLE", "FEW_SEATS_AVAILABLE", "STANDING_ROOM_ONLY")
STOPPED_WINDOW_SECONDS = 20


def service_anchor_utc(service_date: date, tz: str) -> datetime:
    """GTFS service-day anchor: UTC(local noon) - 12 h (correct across DST changes)."""
    noon = datetime(service_date.year, service_date.month, service_date.day, 12, 0, tzinfo=ZoneInfo(tz))
    return noon.astimezone(UTC) - timedelta(hours=12)


def scheduled_epoch(service_date: date, gtfs_seconds: int, tz: str) -> int:
    return int((service_anchor_utc(service_date, tz) + timedelta(seconds=gtfs_seconds)).timestamp())


def header(timestamp: int) -> dict[str, Any]:
    return {"gtfs_realtime_version": GTFS_RT_VERSION, "incrementality": FULL_DATASET, "timestamp": int(timestamp)}


@dataclass(frozen=True)
class TripInstance:
    trip: SynTrip
    service_date: date
    sched: tuple[int, ...]           # scheduled epoch seconds per stop
    delay: int                        # seconds (negative = early)
    vehicle_id: str
    vehicle_label: str
    uncertainty: int | None

    def actual(self, k: int) -> int:
        return self.sched[k] + self.delay


class FeedBuilder:
    """Builds the three realtime documents for a fetch instant of the synthetic demo network."""

    def __init__(self, model: StaticModel, *, seed: int, service_date: date,
                 lookahead_seconds: int = 1800, delay_min: int = -60, delay_max: int = 480) -> None:
        self.model = model
        self.seed = int(seed)
        self.service_date = service_date
        self.tz = model.agency_timezone
        self.lookahead = int(lookahead_seconds)
        self.anchor = service_anchor_utc(service_date, self.tz)
        counters: dict[str, int] = {}
        instances: list[TripInstance] = []
        for trip in sorted(model.trips.values(), key=lambda t: (t.route_id, t.start_secs, t.trip_id)):
            if not model.service_active(trip.service_id, service_date):
                continue
            n = counters.get(trip.route_id, 0) + 1
            counters[trip.route_id] = n
            short = model.routes[trip.route_id].short_name
            delay = delay_min + int(unit(self.seed, "delay", trip.trip_id) * (delay_max - delay_min + 1))
            uncertainty = 60 if unit(self.seed, "uncertainty", trip.trip_id) < 0.2 else None
            instances.append(TripInstance(
                trip=trip,
                service_date=service_date,
                sched=tuple(scheduled_epoch(service_date, s, self.tz) for s in trip.stop_secs),
                delay=delay,
                vehicle_id=f"SYN-{short}{n:03d}",
                vehicle_label=f"{short}{n:03d}",
                uncertainty=uncertainty,
            ))
        self.instances: tuple[TripInstance, ...] = tuple(instances)
        self._by_trip = {inst.trip.trip_id: inst for inst in instances}

    def instance(self, trip_id: str) -> TripInstance | None:
        return self._by_trip.get(trip_id)

    # ------------------------------------------------------------------ helpers
    def header_timestamp(self, feed_type: str, fetched_at: datetime) -> int:
        epoch = int(fetched_at.timestamp())
        return epoch - int(unit(self.seed, "header_lag", feed_type, epoch) * 5)

    def trip_descriptor(self, inst: TripInstance, *, with_start_date: bool = True) -> dict[str, Any]:
        d: dict[str, Any] = {
            "trip_id": inst.trip.trip_id,
            "route_id": inst.trip.route_id,
            "direction_id": inst.trip.direction_id,
        }
        if with_start_date:
            d["start_date"] = inst.service_date.strftime("%Y%m%d")
        d["start_time"] = inst.trip.start_time
        d["schedule_relationship"] = "SCHEDULED"
        return d

    def vehicle_entity(self, inst: TripInstance, observed: int) -> dict[str, Any] | None:
        """Vehicle of ``inst`` at epoch ``observed`` (None when the trip is not running then)."""
        last = len(inst.sched) - 1
        if observed < inst.actual(0) or observed > inst.actual(last):
            return None
        k = 0
        while k < last and inst.actual(k + 1) <= observed:
            k += 1
        stops = inst.trip.stop_ids
        a = self.model.stops[stops[k]]
        if k == last:
            lat, lon, bearing, speed = a.lat, a.lon, None, 0.0
            status, stop_k = "STOPPED_AT", k
        else:
            b = self.model.stops[stops[k + 1]]
            t0, t1 = inst.actual(k), inst.actual(k + 1)
            frac = (observed - t0) / (t1 - t0) if t1 > t0 else 0.0
            lat = a.lat + (b.lat - a.lat) * frac
            lon = a.lon + (b.lon - a.lon) * frac
            bearing = bearing_deg(a.lat, a.lon, b.lat, b.lon)
            speed = haversine_m(a.lat, a.lon, b.lat, b.lon) / (t1 - t0) if t1 > t0 else 0.0
            if observed - t0 < STOPPED_WINDOW_SECONDS:
                status, stop_k = "STOPPED_AT", k
            else:
                status, stop_k = "IN_TRANSIT_TO", k + 1
        position: dict[str, Any] = {"latitude": round(lat, 6), "longitude": round(lon, 6)}
        if bearing is not None:
            position["bearing"] = round(bearing, 1)
        position["speed"] = round(speed, 2)
        occupancy = OCCUPANCY[int(unit(self.seed, "occupancy", inst.trip.trip_id) * len(OCCUPANCY))]
        return {
            "id": inst.vehicle_id,
            "vehicle": {
                "trip": self.trip_descriptor(inst),
                "vehicle": {"id": inst.vehicle_id, "label": inst.vehicle_label},
                "position": position,
                "current_stop_sequence": stop_k + 1,
                "stop_id": stops[stop_k],
                "current_status": status,
                "timestamp": int(observed),
                "occupancy_status": occupancy,
            },
        }

    def trip_update_entity(self, inst: TripInstance, header_ts: int, *, with_start_date: bool = True,
                           min_time: int | None = None) -> dict[str, Any] | None:
        last = len(inst.sched) - 1
        floor = header_ts - 30 if min_time is None else min_time
        stus: list[dict[str, Any]] = []
        for k, stop_id in enumerate(inst.trip.stop_ids):
            drift = int(unit(self.seed, "drift", inst.trip.trip_id, k) * 61) - 30
            t = inst.actual(k) + drift
            if t < floor:
                continue
            stu: dict[str, Any] = {"stop_sequence": k + 1, "stop_id": stop_id}
            if k > 0:
                stu["arrival"] = self._event(t, inst.uncertainty)
            if k < last:
                stu["departure"] = self._event(t, inst.uncertainty)
            stu["schedule_relationship"] = "SCHEDULED"
            stus.append(stu)
        if not stus:
            return None
        return {
            "id": f"TU-{inst.trip.trip_id}",
            "trip_update": {
                "trip": self.trip_descriptor(inst, with_start_date=with_start_date),
                "vehicle": {"id": inst.vehicle_id, "label": inst.vehicle_label},
                "timestamp": int(header_ts),
                "stop_time_update": stus,
            },
        }

    @staticmethod
    def _event(t: int, uncertainty: int | None) -> dict[str, Any]:
        ev: dict[str, Any] = {"time": int(t)}
        if uncertainty is not None:
            ev["uncertainty"] = int(uncertainty)
        return ev

    # ------------------------------------------------------------------ documents
    def vehicle_positions(self, fetched_at: datetime) -> dict[str, Any]:
        ts = self.header_timestamp("vehicle_positions", fetched_at)
        entities: list[dict[str, Any]] = []
        for inst in self.instances:
            lag = int(unit(self.seed, "vehicle_lag", inst.trip.trip_id, ts) * 20)
            ent = self.vehicle_entity(inst, ts - lag)
            if ent is not None:
                entities.append(ent)
        return {"header": header(ts), "entity": entities}

    def trip_updates(self, fetched_at: datetime) -> dict[str, Any]:
        ts = self.header_timestamp("trip_updates", fetched_at)
        entities: list[dict[str, Any]] = []
        for inst in self.instances:
            last = len(inst.sched) - 1
            if inst.actual(last) < ts - 60 or inst.actual(0) > ts + self.lookahead:
                continue
            ent = self.trip_update_entity(inst, ts)
            if ent is not None:
                entities.append(ent)
        return {"header": header(ts), "entity": entities}

    def alerts(self, fetched_at: datetime) -> dict[str, Any]:
        ts = self.header_timestamp("alerts", fetched_at)
        routes = self.model.route_ids
        r0 = routes[0]
        r1 = routes[1] if len(routes) > 1 else r0
        r2 = routes[2] if len(routes) > 2 else r0
        stop_r1 = self.model.routes[r1].stop_ids[min(2, len(self.model.routes[r1].stop_ids) - 1)]
        trip_r2 = next((i.trip.trip_id for i in self.instances if i.trip.route_id == r2), None)
        a = int(self.anchor.timestamp())
        h = 3600

        def text(s: str) -> dict[str, Any]:
            return {"translation": [{"text": s, "language": "en"}]}

        def alert(aid: str, periods: list[dict[str, int]], selectors: list[dict[str, Any]], cause: str,
                  effect: str, severity: str, title: str) -> dict[str, Any]:
            body: dict[str, Any] = {}
            if periods:
                body["active_period"] = periods
            body["informed_entity"] = selectors
            body["cause"] = cause
            body["effect"] = effect
            body["severity_level"] = severity
            body["header_text"] = text(f"{title} (synthetic test alert, not MBTA)")
            body["description_text"] = text("Generated by the TransitPulse synthetic generator for testing.")
            return {"id": aid, "alert": body}

        trip_selector = {"trip": {"trip_id": trip_r2}} if trip_r2 else {"route_id": r2, "direction_id": 0}
        entities = [
            alert("SYN-A1", [{"start": a + 6 * h, "end": a + 22 * h}],
                  [{"agency_id": AGENCY_ID, "route_id": r0, "route_type": 3}],
                  "CONSTRUCTION", "DETOUR", "WARNING", f"Route {r0} detour"),
            alert("SYN-A2", [], [{"stop_id": stop_r1}], "MAINTENANCE", "STOP_MOVED", "INFO",
                  f"Stop {stop_r1} relocated"),
            alert("SYN-A3", [{"start": a + 20 * h, "end": a + 23 * h}], [{"agency_id": AGENCY_ID}],
                  "HOLIDAY", "REDUCED_SERVICE", "INFO", "Evening reduced service"),
            alert("SYN-A4", [{"start": a + 5 * h, "end": a + 12 * h}], [trip_selector],
                  "TECHNICAL_PROBLEM", "SIGNIFICANT_DELAYS", "WARNING", f"Trip delay on {r2}"),
            alert("SYN-A5", [{"start": a - 18 * h, "end": a - 14 * h}], [{"route_type": 3}],
                  "WEATHER", "OTHER_EFFECT", "SEVERE", "Expired storm notice"),
        ]
        return {"header": header(ts), "entity": entities}

    def document(self, feed_type: str, fetched_at: datetime) -> dict[str, Any]:
        if feed_type == "vehicle_positions":
            return self.vehicle_positions(fetched_at)
        if feed_type == "trip_updates":
            return self.trip_updates(fetched_at)
        if feed_type == "alerts":
            return self.alerts(fetched_at)
        raise ValueError(f"unknown feed_type {feed_type!r}")
