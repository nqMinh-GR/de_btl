"""Shared request-parameter declarations and OpenAPI error responses."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import Path, Query

from transitpulse.api.models import ErrorEnvelope

RUN_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"
"""Run ids are path components (``runtime.paths.safe_component``); uuid4 strings match."""

ENTITY_ID_PATTERN = r"^[^\x00-\x1f\x7f/]{1,256}$"
"""GTFS ids (route, stop, trip, vehicle) may contain most characters; control characters and '/' are refused."""

RunIdQuery = Annotated[
    str,
    Query(description="Run identifier (from GET /runs)", min_length=1, max_length=128, pattern=RUN_ID_PATTERN),
]
RunIdPath = Annotated[
    str,
    Path(description="Run identifier (from GET /runs)", min_length=1, max_length=128, pattern=RUN_ID_PATTERN),
]
RouteIdPath = Annotated[str, Path(description="GTFS route_id of the run scope", pattern=ENTITY_ID_PATTERN)]
StopIdPath = Annotated[str, Path(description="GTFS stop_id", pattern=ENTITY_ID_PATTERN)]
TripIdPath = Annotated[str, Path(description="GTFS trip_id", pattern=ENTITY_ID_PATTERN)]
VehicleIdPath = Annotated[str, Path(description="GTFS-RT vehicle id", pattern=ENTITY_ID_PATTERN)]
OptionalRouteIdQuery = Annotated[
    str | None, Query(description="Restrict to one scope route", pattern=ENTITY_ID_PATTERN)
]

ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    404: {"model": ErrorEnvelope, "description": "run, route, stop or trip outside the run scope / not found"},
    422: {"model": ErrorEnvelope, "description": "invalid parameters (validation_error and specific codes)"},
    503: {
        "model": ErrorEnvelope,
        "description": "database unavailable/timeout, generation expired/unavailable/inconsistent, run clock unavailable",
    },
}
