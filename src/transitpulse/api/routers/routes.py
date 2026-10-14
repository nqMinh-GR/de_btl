"""Route endpoints: catalogue, geometry, stop set and route metrics/KPI.

``/routes/{route_id}/metrics`` combines two feeds with separate as-of times: vehicle counts are
recomputed at the application clock from the bounded current vehicle rows (stored Spark counts are
returned only as ``stored_counts_at_as_of``), and the predicted-delay KPI comes from the trip-updates
``route_metrics_by_snapshot`` row (one target stop per trip instance). Rates and means are ``null`` when
the denominator is 0 or unknown, never 0 %. A feed that is expired/unavailable/inconsistent nulls its
block (200 + warning); the endpoint answers 503 only when both blocks are unusable.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends

from transitpulse.api.builders import as_list, parse_json_text
from transitpulse.api.context import INFO, PIPELINE, ROUTE_METRICS_LIMIT, FeedView, RunContext
from transitpulse.api.deps import RequestEnv, get_env
from transitpulse.api.freshness import count_vehicles, evaluate_vehicle, invalid_without_vehicle_id
from transitpulse.api.models import (
    Envelope,
    GeometryData,
    PredictionMetrics,
    RouteItem,
    RouteMetricsData,
    RoutesData,
    RouteStopItem,
    RouteStopsData,
    ShapeItem,
    StoredVehicleCounts,
    VehicleCounts,
    VehicleMetrics,
)
from transitpulse.api.params import ERROR_RESPONSES, RouteIdPath, RunIdQuery
from transitpulse.api.settings import ApiSettings
from transitpulse.contracts.enums import KPI_EXCLUSION_ORDER, FeedType

GEOMETRY_SHAPE_LIMIT = 50
VP = FeedType.VEHICLE_POSITIONS.value
TU = FeedType.TRIP_UPDATES.value


# ------------------------------------------------------------------------------------------ geometry
def _path_of(points_json: Any) -> list[list[float]] | None:
    """``points_json`` (``[[lon, lat], ...]``) -> validated path; ``None`` when unreadable."""
    value = parse_json_text(points_json)
    if not isinstance(value, list):
        return None
    path: list[list[float]] = []
    for point in value:
        if (
            not isinstance(point, (list, tuple))
            or len(point) < 2
            or not all(isinstance(c, (int, float)) and not isinstance(c, bool) for c in point[:2])
        ):
            return None
        path.append([float(point[0]), float(point[1])])
    return path


def _bbox(path: list[list[float]] | None) -> list[float] | None:
    if not path:
        return None
    lons = [p[0] for p in path]
    lats = [p[1] for p in path]
    return [min(lons), min(lats), max(lons), max(lats)]


def _shape(row: Mapping[str, Any]) -> ShapeItem:
    path = _path_of(row.get("points_json"))
    return ShapeItem(
        shape_id=str(row["shape_id"]),
        direction_ids=sorted(int(d) for d in as_list(row.get("direction_ids"))),
        trip_count=row.get("trip_count"),
        point_count=row.get("point_count") if row.get("point_count") is not None else (len(path) if path else None),
        simplified=row.get("simplified"),
        is_representative=row.get("is_representative"),
        bbox=_bbox(path),
        path=path,
    )


# ------------------------------------------------------------------------------------------ metrics
def _ordered_exclusions(raw: Mapping[str, Any]) -> dict[str, int]:
    """Exclusion counts in the fixed KPI precedence order, then any other reason alphabetically."""
    counts = {str(k): int(v) for k, v in raw.items() if v is not None}
    ordered = {code: counts[code] for code in KPI_EXCLUSION_ORDER if code in counts}
    for code in sorted(set(counts) - set(ordered)):
        ordered[code] = counts[code]
    return ordered


def _vehicle_metrics(
    ctx: RunContext, env: RequestEnv, vp: FeedView, route_id: str, clock: datetime
) -> VehicleMetrics | None:
    limit = env.api.max_total_rows
    snapshot_id = vp.snapshot_id or ""
    reads = ctx.read_partitions(
        vp,
        "vehicles_by_route_snapshot",
        [route_id],
        limit=limit,
        fetch=lambda keys, n: env.repo.vehicles(
            ctx.run_id, snapshot_id, [str(k) for k in keys], limit_per_route=n, deadline=env.deadline
        ),
        required=False,
    )
    if reads is None:
        return None
    stored_reads = ctx.read_partitions(
        vp,
        "route_metrics_by_snapshot",
        [None],
        limit=ROUTE_METRICS_LIMIT,
        fetch=lambda keys, n: {
            None: env.repo.route_metrics(ctx.run_id, VP, snapshot_id, limit=n, deadline=env.deadline)
        },
        required=False,
    )
    if stored_reads is None:
        return None
    read = reads[route_id]
    stored_row = next((r for r in stored_reads[None].rows if r.get("route_id") == route_id), None)
    # invalid entities without a vehicle id have no vehicle row; the same generation's stored count has them
    rowless_invalid = (
        invalid_without_vehicle_id(stored_row.get("invalid_vehicle_count"), read.rows)
        if stored_row is not None and not read.truncated
        else 0
    )
    counts = count_vehicles(
        ((row, evaluate_vehicle(row, clock, ctx.policy)) for row in read.rows), invalid_without_id=rowless_invalid
    )
    if counts.clock_skew_vehicle_count:
        ctx.warn(
            "CLOCK_SKEW",
            f"{counts.clock_skew_vehicle_count} observation(s) are slightly ahead of the application clock; "
            "their age is shown as 0",
            PIPELINE,
            feed_type=VP,
            count=counts.clock_skew_vehicle_count,
        )
    stored = None
    if stored_row is not None:
        stored = StoredVehicleCounts(
            metrics_as_of=stored_row.get("metrics_as_of"),
            fresh_vehicle_count=stored_row.get("fresh_vehicle_count"),
            stale_vehicle_count=stored_row.get("stale_vehicle_count"),
            unknown_age_vehicle_count=stored_row.get("unknown_age_vehicle_count"),
            invalid_vehicle_count=stored_row.get("invalid_vehicle_count"),
            snapshot_membership_count=stored_row.get("snapshot_membership_count"),
            inherited_vehicle_count=stored_row.get("inherited_vehicle_count"),
            stale_threshold_seconds=stored_row.get("stale_threshold_seconds"),
        )
    view = ctx.feed(VP)
    return VehicleMetrics(
        snapshot_id=view.snapshot_id,
        evaluated_at=clock,
        metrics_as_of=view.metrics_as_of,
        stale=view.stale,
        stale_threshold_seconds=ctx.policy.vehicle_stale_seconds,
        counts=VehicleCounts(**counts.as_dict()),
        counts_complete=not read.truncated,
        stored_counts_at_as_of=stored,
    )


def _prediction_metrics(ctx: RunContext, env: RequestEnv, tu: FeedView, route_id: str) -> PredictionMetrics | None:
    snapshot_id = tu.snapshot_id or ""
    reads = ctx.read_partitions(
        tu,
        "route_metrics_by_snapshot",
        [None],
        limit=ROUTE_METRICS_LIMIT,
        fetch=lambda keys, n: {
            None: env.repo.route_metrics(ctx.run_id, TU, snapshot_id, limit=n, deadline=env.deadline)
        },
        required=False,
    )
    if reads is None:
        return None
    row = next((r for r in reads[None].rows if r.get("route_id") == route_id), None)
    if row is None:
        # every scope route has one trip_updates metrics row (interfaces §7); a missing row is data loss
        ctx.mark_inconsistent(TU, f"route_metrics_by_snapshot has no trip_updates row for route {route_id}")
        return None
    eligible = row.get("eligible_trip_count")
    trips = row.get("trip_instance_count")
    late = row.get("predicted_late_trip_count") if eligible is not None else None
    has_denominator = isinstance(eligible, int) and eligible > 0
    rate: float | None = None
    mean: float | None = None
    if has_denominator:
        stored_rate = row.get("predicted_late_rate")
        rate = float(stored_rate) if stored_rate is not None else (late / eligible if late is not None else None)
        stored_mean = row.get("mean_predicted_delay_seconds")
        mean = float(stored_mean) if stored_mean is not None else None
    raw_excluded = row.get("excluded_trip_counts")
    excluded_by_reason: dict[str, int] | None
    excluded_count: int | None
    if raw_excluded is not None:
        excluded_by_reason = _ordered_exclusions(raw_excluded)
        excluded_count = sum(excluded_by_reason.values())
    elif trips is not None and eligible is not None:
        # Cassandra stores an empty map as null; the accounting identity gives the excluded total
        excluded_count = trips - eligible
        excluded_by_reason = {} if excluded_count == 0 else None
    else:
        excluded_by_reason, excluded_count = None, None
    policy = ctx.policy
    view = ctx.feed(TU)
    return PredictionMetrics(
        snapshot_id=view.snapshot_id,
        metrics_as_of=row.get("metrics_as_of") or view.metrics_as_of,
        stale=view.stale,
        feed_age_seconds=view.source_age_seconds,
        trip_instance_count=trips,
        eligible_trip_count=eligible,
        predicted_late_trip_count=late,
        predicted_late_rate=rate,
        mean_predicted_delay_seconds=mean,
        excluded_trip_count=excluded_count,
        excluded_by_reason=excluded_by_reason,
        unknown_uncertainty_count=row.get("unknown_uncertainty_count"),
        late_threshold_seconds=row.get("late_threshold_seconds") or policy.predicted_late_seconds,
        target_policy=row.get("target_policy") or policy.target_policy,
        policy_version=row.get("policy_version") or policy.policy_version,
        quality_flags=sorted(as_list(row.get("quality_flags"))),
    )


# ------------------------------------------------------------------------------------------ router
def build_router(settings: ApiSettings) -> APIRouter:
    router = APIRouter(tags=["routes"], responses=ERROR_RESPONSES)
    max_rows = settings.api.max_total_rows

    @router.get("/routes", response_model=Envelope[RoutesData], summary="Route catalogue of the run scope")
    def list_routes(run_id: RunIdQuery, env: RequestEnv = Depends(get_env)) -> Envelope[RoutesData]:
        ctx = RunContext.load(env, run_id, source_health=False)
        version = ctx.require_gtfs_version()
        scope = list(ctx.scope_route_ids)
        rows = ctx.static_rows(
            "routes_by_version", tuple(scope), lambda: env.repo.routes(version, scope, deadline=env.deadline)
        )
        by_id = {str(r["route_id"]): r for r in rows}
        routes = [
            RouteItem(
                route_id=rid,
                short_name=by_id[rid].get("route_short_name"),
                long_name=by_id[rid].get("route_long_name"),
                route_type=by_id[rid].get("route_type"),
                color=by_id[rid].get("route_color"),
                text_color=by_id[rid].get("route_text_color"),
                sort_order=by_id[rid].get("route_sort_order"),
                agency_id=by_id[rid].get("agency_id"),
            )
            for rid in scope
            if rid in by_id
        ]
        missing = [rid for rid in scope if rid not in by_id]
        if missing:
            ctx.warn(
                "STATIC_ROWS_MISSING",
                f"routes {missing} of the run scope are absent from routes_by_version {version}",
                PIPELINE,
                count=len(missing),
            )
        return Envelope[RoutesData](
            data=RoutesData(routes=routes, missing_route_ids=missing),
            meta=ctx.meta(coverage=ctx.coverage(scope, returned_rows=len(routes))),
            warnings=ctx.warnings,
        )

    @router.get(
        "/routes/{route_id}/geometry",
        response_model=Envelope[GeometryData],
        summary="Route shapes (one entry per shape/pattern, [lon, lat] paths)",
    )
    def route_geometry(
        route_id: RouteIdPath, run_id: RunIdQuery, env: RequestEnv = Depends(get_env)
    ) -> Envelope[GeometryData]:
        ctx = RunContext.load(env, run_id, source_health=False)
        ctx.require_route_in_scope(route_id)
        version = ctx.require_gtfs_version()
        rows = ctx.static_rows(
            "geometries_by_route",
            (route_id, GEOMETRY_SHAPE_LIMIT),
            lambda: env.repo.geometries(version, route_id, limit=GEOMETRY_SHAPE_LIMIT + 1, deadline=env.deadline),
        )
        truncated = len(rows) > GEOMETRY_SHAPE_LIMIT
        shapes = [_shape(r) for r in rows[:GEOMETRY_SHAPE_LIMIT]]
        unreadable = [s.shape_id for s in shapes if s.path is None]
        if unreadable:
            ctx.warn(
                "GEOMETRY_UNREADABLE",
                f"points_json of shapes {unreadable} could not be read; their path is null",
                PIPELINE,
                count=len(unreadable),
            )
        if truncated:
            ctx.warn("RESULT_TRUNCATED", f"only the first {GEOMETRY_SHAPE_LIMIT} shapes are returned", INFO)
        return Envelope[GeometryData](
            data=GeometryData(route_id=route_id, shapes=shapes, shape_count=len(shapes), truncated=truncated),
            meta=ctx.meta(
                coverage=ctx.coverage(
                    [route_id], returned_rows=len(shapes), max_rows=GEOMETRY_SHAPE_LIMIT, truncated=truncated
                )
            ),
            warnings=ctx.warnings,
        )

    @router.get(
        "/routes/{route_id}/stops",
        response_model=Envelope[RouteStopsData],
        summary="Set of stops served by the route (no route order is implied; use /trips/{trip_id}/stops)",
    )
    def route_stops(
        route_id: RouteIdPath, run_id: RunIdQuery, env: RequestEnv = Depends(get_env)
    ) -> Envelope[RouteStopsData]:
        ctx = RunContext.load(env, run_id, source_health=False)
        ctx.require_route_in_scope(route_id)
        version = ctx.require_gtfs_version()
        rows = ctx.static_rows(
            "stops_by_route",
            (route_id, max_rows),
            lambda: env.repo.route_stops(version, route_id, limit=max_rows + 1, deadline=env.deadline),
        )
        truncated = len(rows) > max_rows
        stops = [
            RouteStopItem(
                stop_id=str(r["stop_id"]),
                stop_name=r.get("stop_name"),
                latitude=r.get("stop_lat"),
                longitude=r.get("stop_lon"),
                direction_ids=sorted(int(d) for d in as_list(r.get("direction_ids"))),
                pattern_count=r.get("pattern_count"),
            )
            for r in rows[:max_rows]
        ]
        if truncated:
            ctx.warn("RESULT_TRUNCATED", f"only the first {max_rows} stops are returned", INFO)
        return Envelope[RouteStopsData](
            data=RouteStopsData(
                route_id=route_id,
                ordering="none_use_trip_stops",
                stops=stops,
                stop_count=len(stops),
                truncated=truncated,
            ),
            meta=ctx.meta(
                coverage=ctx.coverage([route_id], returned_rows=len(stops), max_rows=max_rows, truncated=truncated)
            ),
            warnings=ctx.warnings,
        )

    @router.get(
        "/routes/{route_id}/metrics",
        response_model=Envelope[RouteMetricsData],
        summary="Vehicle freshness counts (at the application clock) and predicted-delay KPI per route",
    )
    def route_metrics(
        route_id: RouteIdPath, run_id: RunIdQuery, env: RequestEnv = Depends(get_env)
    ) -> Envelope[RouteMetricsData]:
        ctx = RunContext.load(env, run_id)
        ctx.require_route_in_scope(route_id)
        clock = ctx.require_clock()
        vp = ctx.feed(VP)
        tu = ctx.feed(TU)
        if not vp.usable and not tu.usable:
            raise ctx.feed_error(vp)
        vehicle_metrics = _vehicle_metrics(ctx, env, vp, route_id, clock) if vp.usable else None
        prediction_metrics = _prediction_metrics(ctx, env, tu, route_id) if tu.usable else None
        final_views = [ctx.feed(VP), ctx.feed(TU)]
        for view in final_views:
            ctx.warn_unusable(view)
        if vehicle_metrics is None and prediction_metrics is None:
            raise ctx.feed_error(next(v for v in final_views if not v.usable))
        return Envelope[RouteMetricsData](
            data=RouteMetricsData(
                route_id=route_id,
                evaluated_at=clock,
                vehicle_metrics=vehicle_metrics,
                prediction_metrics=prediction_metrics,
            ),
            meta=ctx.meta(coverage=ctx.coverage([route_id], denominator_unit="trip_instance")),
            warnings=ctx.warnings,
        )

    return router
