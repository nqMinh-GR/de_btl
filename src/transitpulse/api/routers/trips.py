"""``GET /trips/{trip_id}/stops``: ordered stop times of one static trip of the run's GTFS version.

GTFS times are service-day offsets (they may exceed 24:00:00). ``scheduled_*_utc`` are produced only
when ``service_date`` is given, with the shared static helper :func:`static.gtfs_time.scheduled_utc`
(the same anchor rule as Spark: UTC of local noon minus 12 h), in the run's agency time zone.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from transitpulse.api.context import PIPELINE, RunContext
from transitpulse.api.deps import RequestEnv, get_env
from transitpulse.api.models import Envelope, TripStopItem, TripStopsData
from transitpulse.api.params import ERROR_RESPONSES, RunIdQuery, TripIdPath
from transitpulse.api.settings import ApiSettings
from transitpulse.static.gtfs_time import format_gtfs_seconds, parse_service_date, scheduled_utc

TRIP_STOP_TIMES_LIMIT = 1000
SERVICE_DATE_PATTERN = r"^\d{4}-?\d{2}-?\d{2}$"


def _gtfs_time(raw: Any, seconds: Any) -> str | None:
    if isinstance(raw, str) and raw:
        return raw
    if isinstance(seconds, int) and not isinstance(seconds, bool):
        return format_gtfs_seconds(seconds)
    return None


def _scheduled(service_day: date | None, seconds: Any, tz: str | None) -> datetime | None:
    if service_day is None or tz is None or not isinstance(seconds, int) or isinstance(seconds, bool):
        return None
    value = scheduled_utc(service_day, seconds, tz)
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def build_router(settings: ApiSettings) -> APIRouter:
    router = APIRouter(tags=["trips"], responses=ERROR_RESPONSES)

    @router.get(
        "/trips/{trip_id}/stops",
        response_model=Envelope[TripStopsData],
        summary="Stop order and schedule of one trip (scheduled UTC times when service_date is given)",
    )
    def trip_stops(
        trip_id: TripIdPath,
        run_id: RunIdQuery,
        service_date: Annotated[
            str | None,
            Query(pattern=SERVICE_DATE_PATTERN, description="service date YYYYMMDD or YYYY-MM-DD (agency local)"),
        ] = None,
        env: RequestEnv = Depends(get_env),
    ) -> Envelope[TripStopsData]:
        ctx = RunContext.load(env, run_id, source_health=False)
        version = ctx.require_gtfs_version()
        service_day: date | None = None
        if service_date is not None:
            try:
                service_day = parse_service_date(service_date.replace("-", ""))
            except ValueError as exc:
                raise ctx.error(
                    422,
                    "validation_error",
                    f"service_date {service_date!r} is not a valid calendar date",
                    details=[{"loc": ["query", "service_date"], "msg": str(exc), "type": "value_error"}],
                    hint="use YYYYMMDD or YYYY-MM-DD",
                ) from exc
        rows = ctx.static_rows(
            "stop_times_by_trip",
            (trip_id, TRIP_STOP_TIMES_LIMIT),
            lambda: env.repo.trip_stop_times(version, trip_id, limit=TRIP_STOP_TIMES_LIMIT, deadline=env.deadline),
        )
        if not rows:
            raise ctx.error(
                404,
                "trip_not_found",
                f"trip {trip_id!r} does not exist in GTFS version {version}",
                details=[{"trip_id": trip_id, "gtfs_version": version}],
            )
        first = rows[0]
        route_id = first.get("route_id")
        if route_id not in ctx.scope_route_ids:
            raise ctx.error(
                404,
                "trip_not_found",
                f"trip {trip_id!r} belongs to route {route_id!r}, which is outside the scope of run {ctx.run_id}",
                details=[{"trip_id": trip_id, "route_id": route_id, "scope_route_ids": list(ctx.scope_route_ids)}],
            )
        tz = ctx.agency_timezone
        if service_day is not None and not tz:
            ctx.warn(
                "AGENCY_TIMEZONE_UNKNOWN",
                "the run has no agency_timezone; scheduled UTC times cannot be computed",
                PIPELINE,
            )
        stops = [
            TripStopItem(
                stop_sequence=int(r["stop_sequence"]),
                stop_id=r.get("stop_id"),
                arrival_time=_gtfs_time(r.get("arrival_time_raw"), r.get("arrival_seconds")),
                departure_time=_gtfs_time(r.get("departure_time_raw"), r.get("departure_seconds")),
                arrival_seconds=r.get("arrival_seconds"),
                departure_seconds=r.get("departure_seconds"),
                stop_headsign=r.get("stop_headsign"),
                pickup_type=r.get("pickup_type"),
                drop_off_type=r.get("drop_off_type"),
                timepoint=r.get("timepoint"),
                scheduled_arrival_utc=_scheduled(service_day, r.get("arrival_seconds"), tz),
                scheduled_departure_utc=_scheduled(service_day, r.get("departure_seconds"), tz),
            )
            for r in rows
        ]
        return Envelope[TripStopsData](
            data=TripStopsData(
                trip_id=trip_id,
                route_id=route_id,
                direction_id=first.get("direction_id"),
                service_id=first.get("service_id"),
                shape_id=first.get("shape_id"),
                trip_headsign=first.get("trip_headsign"),
                service_date=service_day,
                agency_timezone=tz,
                stops=stops,
                stop_count=len(stops),
            ),
            meta=ctx.meta(coverage=ctx.coverage([str(route_id)], returned_rows=len(stops))),
            warnings=ctx.warnings,
        )

    return router
