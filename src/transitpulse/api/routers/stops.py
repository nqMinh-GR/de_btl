"""``GET /stops/{stop_id}/predictions``: upcoming predictions of the current trip-updates generation.

The stop must be served by a scope route (``stops_by_route`` point reads, at most 5 concurrent). The
stop's whole generation partition is read with a bound (so the expected-count check is exact), then
predictions earlier than ``application_clock - 60 s`` are dropped and the filters are applied.
``include=displayable`` hides ``prediction_quality = invalid`` (MBTA bus uncertainty > 301 must not be
shown as a valid ETA); ``include=all`` returns everything for audit. An empty result is ``[]`` with
``reason = NO_PREDICTION_DATA`` — arrival times are never invented.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query

from transitpulse.api.builders import as_list
from transitpulse.api.context import INFO, RunContext
from transitpulse.api.deps import RequestEnv, get_env
from transitpulse.api.models import Envelope, PredictionItem, PredictionsData
from transitpulse.api.params import ERROR_RESPONSES, OptionalRouteIdQuery, RunIdQuery, StopIdPath
from transitpulse.api.settings import ApiSettings
from transitpulse.contracts.enums import FeedType, UncertaintyClass

TU = FeedType.TRIP_UPDATES.value
PREDICTION_LOOKBACK_SECONDS = 60
NO_PREDICTION_DATA = "NO_PREDICTION_DATA"


def prediction_item(row: Mapping[str, Any]) -> PredictionItem:
    return PredictionItem(
        prediction_id=str(row["prediction_id"]),
        stop_id=row.get("stop_id"),
        trip_id=row.get("trip_id"),
        route_id=row.get("route_id"),
        direction_id=row.get("direction_id"),
        start_date=row.get("start_date"),
        start_time=row.get("start_time"),
        service_date=row.get("service_date"),
        stop_sequence=row.get("stop_sequence"),
        event_type=row.get("event_type"),
        predicted_time=row["predicted_at"],
        scheduled_time=row.get("scheduled_at"),
        predicted_delay_seconds=row.get("predicted_delay_seconds"),
        uncertainty_seconds=row.get("uncertainty"),
        prediction_quality=row.get("prediction_quality"),
        schedule_resolution=row.get("schedule_resolution"),
        resolution_method=row.get("resolution_method"),
        trip_schedule_relationship=row.get("trip_schedule_relationship"),
        stop_schedule_relationship=row.get("stop_schedule_relationship"),
        kpi_eligible=row.get("kpi_eligible"),
        is_target=row.get("is_target"),
        vehicle_id=row.get("vehicle_id"),
        quality_flags=sorted(str(f) for f in as_list(row.get("quality_flags"))),
    )


def build_router(settings: ApiSettings) -> APIRouter:
    router = APIRouter(tags=["stops"], responses=ERROR_RESPONSES)
    api = settings.api

    @router.get(
        "/stops/{stop_id}/predictions",
        response_model=Envelope[PredictionsData],
        summary="Upcoming predictions at a scope stop, with quality class and predicted delay",
    )
    def stop_predictions(
        stop_id: StopIdPath,
        run_id: RunIdQuery,
        route_id: OptionalRouteIdQuery = None,
        direction_id: Annotated[int | None, Query(ge=0, le=1)] = None,
        include: Annotated[
            Literal["displayable", "all"], Query(description="displayable hides prediction_quality=invalid")
        ] = "displayable",
        # config-dependent bound in the default: string annotations cannot see build_router locals
        limit: int = Query(api.predictions_limit_default, ge=1, le=api.predictions_limit_max),
        env: RequestEnv = Depends(get_env),
    ) -> Envelope[PredictionsData]:
        ctx = RunContext.load(env, run_id)
        if route_id is not None:
            ctx.require_route_in_scope(route_id)
        version = ctx.require_gtfs_version()
        scope = list(ctx.scope_route_ids)
        serving = ctx.static_rows(
            "stops_by_route/stop",
            (stop_id, *scope),
            lambda: env.repo.stop_routes(version, scope, stop_id, deadline=env.deadline),
        )
        if not serving:
            raise ctx.error(
                404,
                "stop_not_in_scope",
                f"stop {stop_id!r} is not served by any route of the scope of run {ctx.run_id}",
                details=[{"stop_id": stop_id, "scope_route_ids": scope}],
                hint="pick a stop from GET /routes/{route_id}/stops",
            )
        stop_routes = [r for r in scope if r in serving]
        stop_name = next((serving[r].get("stop_name") for r in stop_routes if serving[r].get("stop_name")), None)
        clock = ctx.require_clock()
        view = ctx.require_feed(TU)
        cap = env.api.max_total_rows
        snapshot_id = view.snapshot_id or ""
        reads = ctx.read_partitions(
            view,
            "predictions_by_stop_snapshot",
            [stop_id],
            limit=cap,
            fetch=lambda keys, n: {
                k: env.repo.predictions(ctx.run_id, snapshot_id, str(k), limit=n, deadline=env.deadline) for k in keys
            },
        )
        assert reads is not None  # required=True raises instead of returning None
        read = reads[stop_id]
        window_start = clock - timedelta(seconds=PREDICTION_LOOKBACK_SECONDS)
        in_window = [r for r in read.rows if r["predicted_at"] >= window_start]
        selected = [
            r
            for r in in_window
            if (route_id is None or r.get("route_id") == route_id)
            and (direction_id is None or r.get("direction_id") == direction_id)
            and (include == "all" or r.get("prediction_quality") != UncertaintyClass.INVALID.value)
        ]
        selected.sort(key=lambda r: (r["predicted_at"], str(r["prediction_id"])))
        truncated = read.truncated or len(selected) > limit
        items = [prediction_item(r) for r in selected[:limit]]
        reason = reason_detail = None
        if not items:
            reason = NO_PREDICTION_DATA
            if not read.rows:
                reason_detail = (
                    "the current trip_updates snapshot is empty"
                    if view.state == "empty"
                    else "the current trip_updates generation has no predictions for this stop"
                )
            elif not in_window:
                reason_detail = "every prediction for this stop is earlier than the window start"
            else:
                reason_detail = "no prediction matches route_id/direction_id/include"
            ctx.warn(NO_PREDICTION_DATA, f"no prediction to show at stop {stop_id}: {reason_detail}", INFO, feed_type=TU)
        if truncated:
            ctx.warn("RESULT_TRUNCATED", f"at most {limit} predictions are returned", INFO)
        return Envelope[PredictionsData](
            data=PredictionsData(
                stop_id=stop_id,
                stop_name=stop_name,
                route_ids=stop_routes,
                snapshot_id=view.snapshot_id,
                include=include,
                evaluated_at=clock,
                window_start=window_start,
                predictions=items,
                returned_count=len(items),
                truncated=truncated,
                reason=reason,
                reason_detail=reason_detail,
            ),
            meta=ctx.meta(
                coverage=ctx.coverage(
                    stop_routes,
                    denominator_unit="prediction",
                    returned_rows=len(items),
                    max_rows=limit,
                    truncated=truncated,
                )
            ),
            warnings=ctx.warnings,
        )

    return router
