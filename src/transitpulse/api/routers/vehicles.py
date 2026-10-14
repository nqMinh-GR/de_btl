"""``GET /vehicles`` (current generation, freshness at the application clock) and
``GET /vehicles/{vehicle_id}/history`` (observation history, paged over UTC day buckets)."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from transitpulse.api.builders import as_list
from transitpulse.api.context import INFO, PIPELINE, ROUTE_METRICS_LIMIT, RunContext
from transitpulse.api.deps import RequestEnv, get_env
from transitpulse.api.freshness import (
    VehicleFreshness,
    count_vehicles,
    evaluate_vehicle,
    invalid_without_vehicle_id,
    merged_flags,
)
from transitpulse.api.models import (
    Envelope,
    HistoryData,
    HistoryObservation,
    HistoryRange,
    VehicleCounts,
    VehicleItem,
    VehiclesData,
)
from transitpulse.api.paging import (
    MAX_TOKEN_CHARS,
    InvalidPageToken,
    collect_history_page,
    decode_history_token,
    encode_history_token,
    history_buckets,
)
from transitpulse.api.params import ERROR_RESPONSES, OptionalRouteIdQuery, RunIdQuery, VehicleIdPath
from transitpulse.api.settings import ApiSettings
from transitpulse.contracts.enums import FeedType, FreshnessClass
from transitpulse.contracts.envelope import iso_utc

VP = FeedType.VEHICLE_POSITIONS.value


def vehicle_item(row: Mapping[str, Any], state: VehicleFreshness) -> VehicleItem:
    return VehicleItem(
        vehicle_id=str(row["vehicle_id"]),
        label=row.get("vehicle_label"),
        route_id=str(row["route_id"]),
        trip_id=row.get("trip_id"),
        direction_id=row.get("direction_id"),
        start_date=row.get("start_date"),
        start_time=row.get("start_time"),
        trip_schedule_relationship=row.get("trip_schedule_relationship"),
        latitude=row.get("latitude"),
        longitude=row.get("longitude"),
        bearing=row.get("bearing"),
        speed_mps=row.get("speed_mps"),
        observation_time=row.get("observation_time"),
        event_time=row.get("event_time"),
        event_time_origin=row.get("event_time_origin"),
        record_kind=row.get("record_kind"),
        observation_age_seconds=state.age_seconds,
        freshness_status=state.status.value,
        invalid_reason=state.invalid_reason,
        current_status=row.get("current_status"),
        stop_id=row.get("stop_id"),
        current_stop_sequence=row.get("current_stop_sequence"),
        occupancy_status=row.get("occupancy_status"),
        quality_flags=merged_flags(row.get("quality_flags"), state.flags),
        observation_id=row.get("observation_id"),
        inherited=row.get("is_inherited") is True,
        inherited_from_snapshot_id=row.get("inherited_from_snapshot_id"),
        feed_timestamp=row.get("feed_timestamp"),
    )


def history_observation(row: Mapping[str, Any]) -> HistoryObservation:
    return HistoryObservation(
        event_time=row["event_time"],
        observation_id=str(row["observation_id"]),
        record_kind=row.get("record_kind"),
        event_time_origin=row.get("event_time_origin"),
        route_id=row.get("route_id"),
        trip_id=row.get("trip_id"),
        direction_id=row.get("direction_id"),
        start_date=row.get("start_date"),
        latitude=row.get("latitude"),
        longitude=row.get("longitude"),
        bearing=row.get("bearing"),
        speed_mps=row.get("speed_mps"),
        stop_id=row.get("stop_id"),
        current_stop_sequence=row.get("current_stop_sequence"),
        current_status=row.get("current_status"),
        quality_flags=sorted(str(f) for f in as_list(row.get("quality_flags"))),
    )


def build_router(settings: ApiSettings) -> APIRouter:
    router = APIRouter(tags=["vehicles"], responses=ERROR_RESPONSES)
    api = settings.api

    @router.get(
        "/vehicles",
        response_model=Envelope[VehiclesData],
        summary="Vehicles of the current generation with freshness recomputed at the application clock",
    )
    def list_vehicles(
        run_id: RunIdQuery,
        route_id: OptionalRouteIdQuery = None,
        status: Annotated[FreshnessClass | None, Query(description="filter by freshness_status")] = None,
        # Config-dependent bounds live in the default (evaluated now); string annotations are resolved
        # against module globals only, so they must not reference locals of build_router.
        limit: int = Query(api.max_total_rows, ge=1, le=api.max_total_rows, description="maximum vehicles returned"),
        env: RequestEnv = Depends(get_env),
    ) -> Envelope[VehiclesData]:
        ctx = RunContext.load(env, run_id)
        routes = ctx.fanout_routes(route_id)
        clock = ctx.require_clock()
        view = ctx.require_feed(VP)
        per_route = env.api.max_total_rows
        snapshot_id = view.snapshot_id or ""
        reads = ctx.read_partitions(
            view,
            "vehicles_by_route_snapshot",
            routes,
            limit=per_route,
            fetch=lambda keys, n: env.repo.vehicles(
                ctx.run_id, snapshot_id, [str(k) for k in keys], limit_per_route=n, deadline=env.deadline
            ),
        )
        assert reads is not None  # required=True raises instead of returning None
        # Same generation's VP route metrics: invalid entities without a vehicle id have no row above.
        metrics_reads = ctx.read_partitions(
            view,
            "route_metrics_by_snapshot",
            [None],
            limit=ROUTE_METRICS_LIMIT,
            fetch=lambda keys, n: {
                None: env.repo.route_metrics(ctx.run_id, VP, snapshot_id, limit=n, deadline=env.deadline)
            },
        )
        assert metrics_reads is not None
        stored_invalid = {str(m.get("route_id")): m.get("invalid_vehicle_count") for m in metrics_reads[None].rows}
        rowless_invalid = sum(
            invalid_without_vehicle_id(stored_invalid.get(r), reads[r].rows) for r in routes if not reads[r].truncated
        )
        evaluated = [(row, evaluate_vehicle(row, clock, ctx.policy)) for r in routes for row in reads[r].rows]
        counts = count_vehicles(evaluated, invalid_without_id=rowless_invalid)
        items = [vehicle_item(row, state) for row, state in evaluated if status is None or state.status == status]
        items.sort(key=lambda v: (v.route_id, v.vehicle_id))
        partition_truncated = any(reads[r].truncated for r in routes)
        truncated = partition_truncated or len(items) > limit
        items = items[:limit]
        reason = None
        if not items:
            reason = "EMPTY_SNAPSHOT" if view.state == "empty" else "NO_MATCHING_VEHICLES"
        if counts.clock_skew_vehicle_count:
            ctx.warn(
                "CLOCK_SKEW",
                f"{counts.clock_skew_vehicle_count} observation(s) are slightly ahead of the application clock; "
                "their age is shown as 0",
                PIPELINE,
                feed_type=VP,
                count=counts.clock_skew_vehicle_count,
            )
        if truncated:
            ctx.warn("RESULT_TRUNCATED", f"at most {limit} vehicles are returned; narrow with route_id/status", INFO)
        return Envelope[VehiclesData](
            data=VehiclesData(
                route_ids=routes,
                snapshot_id=view.snapshot_id,
                evaluated_at=clock,
                counts=VehicleCounts(**counts.as_dict()),
                counts_complete=not partition_truncated,
                vehicles=items,
                returned_count=len(items),
                truncated=truncated,
                reason=reason,
            ),
            meta=ctx.meta(
                coverage=ctx.coverage(
                    routes, denominator_unit="vehicle_id", returned_rows=len(items), max_rows=limit, truncated=truncated
                )
            ),
            warnings=ctx.warnings,
        )

    @router.get(
        "/vehicles/{vehicle_id}/history",
        response_model=Envelope[HistoryData],
        summary="Observation history of one vehicle over [from, to) (default: last hour of the application clock)",
    )
    def vehicle_history(
        vehicle_id: VehicleIdPath,
        run_id: RunIdQuery,
        from_: Annotated[
            datetime | None, Query(alias="from", description="inclusive start, ISO-8601 with timezone")
        ] = None,
        to: Annotated[datetime | None, Query(description="exclusive end, ISO-8601 with timezone")] = None,
        page_size: int = Query(api.page_size_default, ge=1, le=api.page_size_max, description="rows per page"),
        page_token: Annotated[str | None, Query(min_length=1, max_length=MAX_TOKEN_CHARS)] = None,
        env: RequestEnv = Depends(get_env),
    ) -> Envelope[HistoryData]:
        ctx = RunContext.load(env, run_id, source_health=False)
        for name, value in (("from", from_), ("to", to)):
            if value is not None and value.tzinfo is None:
                raise ctx.error(
                    422,
                    "naive_datetime",
                    f"'{name}' has no timezone; instants must be explicit",
                    details=[{"loc": ["query", name], "msg": "timezone required", "type": "naive_datetime"}],
                    hint="append 'Z' or an offset, e.g. 2026-10-05T12:00:00Z",
                )
        req_from = from_.astimezone(UTC) if from_ is not None else None
        req_to = to.astimezone(UTC) if to is not None else None
        cursor = None
        if page_token is not None:
            try:
                token = decode_history_token(env.services.signer, page_token, run_id=ctx.run_id, vehicle_id=vehicle_id)
            except InvalidPageToken as exc:
                raise ctx.error(
                    422,
                    "invalid_page_token",
                    str(exc),
                    details=[{"loc": ["query", "page_token"]}],
                    hint="restart from the first page (omit page_token)",
                ) from exc
            if (req_from is not None and req_from != token.start) or (req_to is not None and req_to != token.end):
                raise ctx.error(
                    422,
                    "invalid_page_token",
                    "page token was issued for another time range",
                    details=[{"loc": ["query", "page_token"]}],
                    hint="repeat the original from/to (or omit them) with this page_token",
                )
            start, end, cursor = token.start, token.end, token.cursor
        else:
            end = req_to if req_to is not None else ctx.require_clock()
            start = req_from if req_from is not None else end - timedelta(hours=api.history_default_hours)
        if not start < end:
            raise ctx.error(
                422,
                "validation_error",
                "'from' must be earlier than 'to'",
                details=[{"loc": ["query", "from"], "msg": "from >= to", "type": "value_error"}],
            )
        max_range = timedelta(hours=api.history_max_hours)
        if end - start > max_range:
            hours = (end - start).total_seconds() / 3600.0
            raise ctx.error(
                422,
                "history_range_too_large",
                f"the requested range is {hours:g} h; at most {api.history_max_hours} h are allowed",
                details=[{"from": iso_utc(start), "to": iso_utc(end), "max_hours": api.history_max_hours}],
                hint=f"narrow from/to to at most {api.history_max_hours} h and page with page_token",
            )
        buckets = history_buckets(start, end)

        def fetch(
            index: int, day: date, lo: datetime, hi: datetime, after: tuple[datetime, str] | None, limit: int
        ) -> list[dict[str, Any]]:
            return env.repo.vehicle_history(
                ctx.run_id, vehicle_id, day, start=lo, end=hi, after=after, limit=limit, deadline=env.deadline
            )

        try:
            rows, next_cursor = collect_history_page(
                buckets=buckets, start=start, end=end, cursor=cursor, page_size=page_size, fetch=fetch
            )
        except InvalidPageToken as exc:
            raise ctx.error(422, "invalid_page_token", str(exc), hint="restart from the first page") from exc
        next_token = (
            encode_history_token(
                env.services.signer, run_id=ctx.run_id, vehicle_id=vehicle_id, start=start, end=end, cursor=next_cursor
            )
            if next_cursor is not None
            else None
        )
        observations = [history_observation(r) for r in rows]
        return Envelope[HistoryData](
            data=HistoryData(
                vehicle_id=vehicle_id,
                range=HistoryRange(start=start, end=end),
                buckets=buckets,
                order="event_time_asc",
                page_size=page_size,
                returned_count=len(observations),
                observations=observations,
                next_page_token=next_token,
                reason=None if observations or cursor is not None else "NO_OBSERVATIONS_IN_RANGE",
            ),
            meta=ctx.meta(),
            warnings=ctx.warnings,
        )

    return router
