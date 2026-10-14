"""Deterministic synthetic static GTFS (design E §3.4; spec §15.2, §25.6).

Everything is derived from the seed through sha256-based draws (:func:`unit`), never from wall time, so
the same seed gives byte-identical files and ZIP. The ZIP is written with sorted entries, a fixed
``ZipInfo`` date of 1980-01-01, fixed permissions and ``ZIP_STORED`` (no compressor-version dependence).

Content (agency ``SYN`` — NOT MBTA):
* routes from ``synthetic.route_ids`` (default ``R_A..R_E``), ``route_type=3``, 8–12 stops each on a
  labelled grid (lat 42.30–42.40, lon −71.15..−71.00), two directions with shapes;
* calendar ``WKDY`` (Mon–Fri) and ``DAILY`` over the service month, one ``calendar_dates`` removal
  (2026-10-12) — the service date 2026-10-05 is an active Monday;
* one late-night ``DAILY`` trip whose last stop is at ``25:10:00`` and one loop trip visiting its first
  stop twice (for schedule-resolution edge cases).
"""

from __future__ import annotations

import calendar as _calendar
import csv
import hashlib
import io
import math
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta

AGENCY_ID = "SYN"
AGENCY_NAME = "TransitPulse Synthetic (NOT MBTA)"
AGENCY_URL = "https://example.invalid"
AGENCY_TIMEZONE = "America/New_York"
FEED_PUBLISHER = "TransitPulse synthetic generator (NOT MBTA data)"
ZIP_DATE_TIME = (1980, 1, 1, 0, 0, 0)
LAT_MIN, LAT_MAX = 42.30, 42.40
LON_MIN, LON_MAX = -71.15, -71.00
ROUTE_COLORS: tuple[tuple[str, str], ...] = (
    ("1F77B4", "FFFFFF"), ("FF7F0E", "000000"), ("2CA02C", "FFFFFF"), ("D62728", "FFFFFF"),
    ("9467BD", "FFFFFF"), ("8C564B", "FFFFFF"), ("E377C2", "000000"), ("7F7F7F", "FFFFFF"),
)
NIGHT_FIRST_SECONDS = 19 * 3600 + 50 * 60     # 19:50:00
NIGHT_LAST_SECONDS = 25 * 3600 + 10 * 60      # 25:10:00
LOOP_START_SECONDS = 8 * 3600 + 30 * 60       # 08:30:00
REMOVED_SERVICE_DATE = date(2026, 10, 12)


def unit(seed: int, *parts: object) -> float:
    """Deterministic draw in [0, 1) from (seed, parts) (independent of call order and Python version)."""
    key = "\x1f".join([str(int(seed)), *(str(p) for p in parts)]).encode("utf-8")
    return int.from_bytes(hashlib.sha256(key).digest()[:8], "big") / float(1 << 64)


def format_gtfs_time(seconds: int) -> str:
    if seconds < 0:
        raise ValueError("GTFS times are non-negative")
    return f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371008.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


@dataclass(frozen=True)
class SynStop:
    stop_id: str
    stop_name: str
    lat: float
    lon: float


@dataclass(frozen=True)
class SynRoute:
    route_id: str
    short_name: str
    long_name: str
    color: str
    text_color: str
    sort_order: int
    stop_ids: tuple[str, ...]          # direction 0 order


@dataclass(frozen=True)
class SynTrip:
    trip_id: str
    route_id: str
    service_id: str
    direction_id: int
    shape_id: str
    headsign: str
    stop_ids: tuple[str, ...]
    stop_secs: tuple[int, ...]         # arrival == departure, GTFS seconds after the service anchor

    @property
    def start_secs(self) -> int:
        return self.stop_secs[0]

    @property
    def end_secs(self) -> int:
        return self.stop_secs[-1]

    @property
    def start_time(self) -> str:
        return format_gtfs_time(self.stop_secs[0])


@dataclass
class StaticModel:
    seed: int
    agency_timezone: str
    feed_version: str
    service_start: date
    service_end: date
    routes: dict[str, SynRoute] = field(default_factory=dict)
    stops: dict[str, SynStop] = field(default_factory=dict)
    trips: dict[str, SynTrip] = field(default_factory=dict)
    shapes: dict[str, tuple[tuple[float, float], ...]] = field(default_factory=dict)
    calendar: dict[str, tuple[int, ...]] = field(default_factory=dict)       # service -> mon..sun flags
    calendar_dates: list[tuple[str, date, int]] = field(default_factory=list)

    @property
    def route_ids(self) -> tuple[str, ...]:
        return tuple(self.routes)

    def service_active(self, service_id: str, d: date) -> bool:
        for sid, day, exception in self.calendar_dates:
            if sid == service_id and day == d:
                return exception == 1
        flags = self.calendar.get(service_id)
        if flags is None or not (self.service_start <= d <= self.service_end):
            return False
        return bool(flags[d.weekday()])

    def trips_of_route(self, route_id: str) -> list[SynTrip]:
        return [t for t in self.trips.values() if t.route_id == route_id]

    # ------------------------------------------------------------------ serialization
    def files(self) -> dict[str, bytes]:
        def ymd(d: date) -> str:
            return d.strftime("%Y%m%d")

        out: dict[str, bytes] = {}
        out["agency.txt"] = csv_bytes(
            ("agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang", "agency_phone"),
            [(AGENCY_ID, AGENCY_NAME, AGENCY_URL, self.agency_timezone, "en", None)],
        )
        out["routes.txt"] = csv_bytes(
            ("route_id", "agency_id", "route_short_name", "route_long_name", "route_desc", "route_type",
             "route_url", "route_color", "route_text_color", "route_sort_order"),
            [(r.route_id, AGENCY_ID, r.short_name, r.long_name, "Synthetic test route (not MBTA)", 3, None,
              r.color, r.text_color, r.sort_order) for r in self.routes.values()],
        )
        out["stops.txt"] = csv_bytes(
            ("stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon", "zone_id", "stop_url",
             "location_type", "parent_station", "wheelchair_boarding", "platform_code"),
            [(s.stop_id, None, s.stop_name, None, f"{s.lat:.6f}", f"{s.lon:.6f}", None, None, 0, None, None, None)
             for s in sorted(self.stops.values(), key=lambda s: s.stop_id)],
        )
        out["trips.txt"] = csv_bytes(
            ("route_id", "service_id", "trip_id", "trip_headsign", "trip_short_name", "direction_id", "block_id",
             "shape_id", "wheelchair_accessible"),
            [(t.route_id, t.service_id, t.trip_id, t.headsign, None, t.direction_id, None, t.shape_id, None)
             for t in self.trips.values()],
        )
        st_rows: list[Sequence[object]] = []
        for t in self.trips.values():
            for i, (sid, secs) in enumerate(zip(t.stop_ids, t.stop_secs, strict=True), start=1):
                gt = format_gtfs_time(secs)
                st_rows.append((t.trip_id, gt, gt, sid, i, None, 0, 0, 1))
        out["stop_times.txt"] = csv_bytes(
            ("trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "stop_headsign",
             "pickup_type", "drop_off_type", "timepoint"),
            st_rows,
        )
        out["calendar.txt"] = csv_bytes(
            ("service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
             "start_date", "end_date"),
            [(sid, *flags, ymd(self.service_start), ymd(self.service_end)) for sid, flags in self.calendar.items()],
        )
        out["calendar_dates.txt"] = csv_bytes(
            ("service_id", "date", "exception_type"),
            [(sid, ymd(d), exc) for sid, d, exc in self.calendar_dates],
        )
        shape_rows: list[Sequence[object]] = []
        for shape_id, pts in self.shapes.items():
            dist = 0.0
            prev: tuple[float, float] | None = None
            for i, (lat, lon) in enumerate(pts, start=1):
                if prev is not None:
                    dist += haversine_m(prev[0], prev[1], lat, lon)
                shape_rows.append((shape_id, f"{lat:.6f}", f"{lon:.6f}", i, f"{dist:.1f}"))
                prev = (lat, lon)
        out["shapes.txt"] = csv_bytes(
            ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", "shape_dist_traveled"), shape_rows,
        )
        out["feed_info.txt"] = csv_bytes(
            ("feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_start_date", "feed_end_date",
             "feed_version", "feed_contact_email"),
            [(FEED_PUBLISHER, AGENCY_URL, "en", ymd(self.service_start), ymd(self.service_end), self.feed_version,
              None)],
        )
        return out

    def zip_bytes(self) -> bytes:
        return deterministic_zip(self.files())


def csv_bytes(header: Sequence[str], rows: Sequence[Sequence[object]]) -> bytes:
    """GTFS CSV, UTF-8 without BOM, '\\n' line endings; None -> empty field (missing, never '0')."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    for row in rows:
        if len(row) != len(header):
            raise ValueError(f"row has {len(row)} fields, header has {len(header)}")
        writer.writerow(["" if v is None else v for v in row])
    return buf.getvalue().encode("utf-8")


def deterministic_zip(files: Mapping[str, bytes]) -> bytes:
    """ZIP with sorted entries, fixed 1980-01-01 timestamps, fixed attributes, no compression."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED, allowZip64=False) as zf:
        for name in sorted(files):
            info = zipfile.ZipInfo(name, date_time=ZIP_DATE_TIME)
            info.compress_type = zipfile.ZIP_STORED
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            zf.writestr(info, files[name])
    return buf.getvalue()


def month_bounds(d: date) -> tuple[date, date]:
    last = _calendar.monthrange(d.year, d.month)[1]
    return date(d.year, d.month, 1), date(d.year, d.month, last)


def _short_name(route_id: str) -> str:
    return route_id[2:] if route_id.startswith("R_") and len(route_id) > 2 else route_id


def _route_line(seed: int, route_id: str, index: int, n_routes: int, n_stops: int) -> list[tuple[float, float]]:
    """Stop coordinates of one route: a gently sloped line across the synthetic grid."""
    span = LAT_MAX - LAT_MIN - 0.02
    base = LAT_MIN + 0.01 + (span * index / (n_routes - 1) if n_routes > 1 else span / 2)
    slope = (unit(seed, "slope", route_id) - 0.5) * 0.004
    lon0, lon1 = LON_MIN + 0.005, LON_MAX - 0.005
    pts: list[tuple[float, float]] = []
    for k in range(n_stops):
        frac = k / (n_stops - 1)
        jitter = (unit(seed, "jitter", route_id, k) - 0.5) * 0.004
        lat = min(LAT_MAX - 0.001, max(LAT_MIN + 0.001, base + slope * k + jitter))
        lon = lon0 + (lon1 - lon0) * frac
        pts.append((round(lat, 6), round(lon, 6)))
    return pts


def _with_midpoints(points: Sequence[tuple[float, float]]) -> tuple[tuple[float, float], ...]:
    out: list[tuple[float, float]] = []
    for i, p in enumerate(points):
        if i > 0:
            q = points[i - 1]
            out.append((round((p[0] + q[0]) / 2, 6), round((p[1] + q[1]) / 2, 6)))
        out.append(p)
    return tuple(out)


def _add_route_geometry(model: StaticModel, route_ids: Sequence[str], seed: int,
                        stops_for: Mapping[str, int]) -> None:
    for idx, rid in enumerate(route_ids):
        n_stops = stops_for[rid]
        coords = _route_line(seed, rid, idx, len(route_ids), n_stops)
        short = _short_name(rid)
        stop_ids: list[str] = []
        for k, (lat, lon) in enumerate(coords, start=1):
            sid = f"{rid}_S{k:02d}"
            model.stops[sid] = SynStop(sid, f"Synthetic {short} stop {k}", lat, lon)
            stop_ids.append(sid)
        color, text = ROUTE_COLORS[idx % len(ROUTE_COLORS)]
        model.routes[rid] = SynRoute(rid, short, f"Synthetic route {short} (not MBTA)", color, text, idx + 1,
                                     tuple(stop_ids))
        model.shapes[f"{rid}_SH0"] = _with_midpoints(coords)
        model.shapes[f"{rid}_SH1"] = _with_midpoints(list(reversed(coords)))


def _validate_routes(route_ids: Sequence[str]) -> tuple[str, ...]:
    routes = tuple(dict.fromkeys(route_ids))
    if not routes:
        raise ValueError("at least one synthetic route id is required")
    for r in routes:
        if not r or any(c in r for c in ",\"\n\r"):
            raise ValueError(f"invalid synthetic route id {r!r}")
    return routes


def build_static_model(
    route_ids: Sequence[str],
    *,
    seed: int,
    service_date: date,
    timezone: str = AGENCY_TIMEZONE,
    headway_seconds: int = 1200,
    first_departure_seconds: int = 5 * 3600,
    last_departure_seconds: int = 23 * 3600,
    stop_spacing_seconds: int = 180,
) -> StaticModel:
    """Demo network: regular trips both directions + one 25:10 night trip + one loop trip."""
    routes = _validate_routes(route_ids)
    start, end = month_bounds(service_date)
    model = StaticModel(seed=seed, agency_timezone=timezone, feed_version=f"synthetic-v1-seed{seed}",
                        service_start=start, service_end=end)
    stops_for = {rid: 8 + int(unit(seed, "nstops", rid) * 5) for rid in routes}
    _add_route_geometry(model, routes, seed, stops_for)
    model.calendar["WKDY"] = (1, 1, 1, 1, 1, 0, 0)
    model.calendar["DAILY"] = (1, 1, 1, 1, 1, 1, 1)
    if start <= REMOVED_SERVICE_DATE <= end:
        model.calendar_dates.append(("WKDY", REMOVED_SERVICE_DATE, 2))
    else:
        removed = end - timedelta(days=(end.weekday() - 0) % 7)   # last Monday of the month
        model.calendar_dates.append(("WKDY", removed, 2))

    for rid in routes:
        route = model.routes[rid]
        n = len(route.stop_ids)
        segs = [stop_spacing_seconds + 30 * (int(unit(seed, "seg", rid, k) * 3) - 1) for k in range(n - 1)]
        for direction in (0, 1):
            order = route.stop_ids if direction == 0 else tuple(reversed(route.stop_ids))
            dsegs = segs if direction == 0 else list(reversed(segs))
            dep = first_departure_seconds + (headway_seconds // 2 if direction == 1 else 0)
            headsign = model.stops[order[-1]].stop_name
            while dep <= last_departure_seconds:
                secs = [dep]
                for s in dsegs:
                    secs.append(secs[-1] + s)
                trip_id = f"{rid}_{direction}_{format_gtfs_time(dep)[:5].replace(':', '')}"
                model.trips[trip_id] = SynTrip(trip_id, rid, "WKDY", direction, f"{rid}_SH{direction}", headsign,
                                               tuple(order), tuple(secs))
                dep += headway_seconds

    # Late-night trip past midnight (GTFS time 25:10:00) on the first route, every day.
    first = model.routes[routes[0]]
    n = len(first.stop_ids)
    night: list[int] = []
    for k in range(n):
        raw = NIGHT_FIRST_SECONDS + (NIGHT_LAST_SECONDS - NIGHT_FIRST_SECONDS) * k / (n - 1)
        night.append(int(round(raw / 60.0)) * 60)
    night[-1] = NIGHT_LAST_SECONDS
    model.trips[f"{first.route_id}_NIGHT"] = SynTrip(
        f"{first.route_id}_NIGHT", first.route_id, "DAILY", 0, f"{first.route_id}_SH0",
        model.stops[first.stop_ids[-1]].stop_name, first.stop_ids, tuple(night),
    )

    # Loop trip that visits its first stop twice (repeated stop) on the second route.
    loop_route = model.routes[routes[1] if len(routes) > 1 else routes[0]]
    loop_stops = (*loop_route.stop_ids, loop_route.stop_ids[0])
    coords = [(model.stops[s].lat, model.stops[s].lon) for s in loop_stops]
    model.shapes[f"{loop_route.route_id}_SHL"] = _with_midpoints(coords)
    loop_secs = [LOOP_START_SECONDS]
    for _ in loop_stops[1:]:
        loop_secs.append(loop_secs[-1] + stop_spacing_seconds)
    model.trips[f"{loop_route.route_id}_LOOP"] = SynTrip(
        f"{loop_route.route_id}_LOOP", loop_route.route_id, "WKDY", 0, f"{loop_route.route_id}_SHL",
        model.stops[loop_stops[0]].stop_name, tuple(loop_stops), tuple(loop_secs),
    )
    return model


def build_stress_static_model(
    route_ids: Sequence[str],
    *,
    seed: int,
    service_date: date,
    trips_per_route: int,
    stops_per_trip: int = 10,
    timezone: str = AGENCY_TIMEZONE,
    first_departure_seconds: int = 7 * 3600 + 30 * 60,
    stop_spacing_seconds: int = 120,
) -> StaticModel:
    """Stress network: replicated trips so every realtime reference of the stress feed exists in static."""
    routes = _validate_routes(route_ids)
    if trips_per_route < 1 or stops_per_trip < 2:
        raise ValueError("trips_per_route >= 1 and stops_per_trip >= 2 are required")
    start, end = month_bounds(service_date)
    model = StaticModel(seed=seed, agency_timezone=timezone, feed_version=f"synthetic-stress-v1-seed{seed}",
                        service_start=start, service_end=end)
    _add_route_geometry(model, routes, seed, {rid: stops_per_trip for rid in routes})
    model.calendar["STRESS"] = (1, 1, 1, 1, 1, 1, 1)
    for rid in routes:
        route = model.routes[rid]
        for k in range(trips_per_route):
            direction = k % 2
            order = route.stop_ids if direction == 0 else tuple(reversed(route.stop_ids))
            dep = first_departure_seconds + (k % 120) * 30
            secs = tuple(dep + i * stop_spacing_seconds for i in range(len(order)))
            trip_id = f"{rid}_ST{k:04d}"
            model.trips[trip_id] = SynTrip(trip_id, rid, "STRESS", direction, f"{rid}_SH{direction}",
                                           model.stops[order[-1]].stop_name, tuple(order), secs)
    return model
