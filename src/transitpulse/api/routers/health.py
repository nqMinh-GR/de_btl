"""``/health/live``, ``/health/ready`` and ``/metrics``.

Liveness never touches dependencies. Readiness checks only the serving store (Cassandra reachable and
the schema present, 1 s per statement); it never checks Kafka, the MBTA source or feed freshness —
"ready" does not mean "data is fresh".
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST

from transitpulse.api.deps import RequestEnv, get_env
from transitpulse.api.errors import ApiError
from transitpulse.api.models import Envelope, LiveData, ReadyChecks, ReadyData
from transitpulse.api.params import ERROR_RESPONSES
from transitpulse.api.settings import ApiSettings


def build_router(settings: ApiSettings) -> APIRouter:
    router = APIRouter(tags=["health"])

    @router.get("/health/live", response_model=Envelope[LiveData], summary="API process is running")
    def health_live(env: RequestEnv = Depends(get_env)) -> Envelope[LiveData]:
        return Envelope[LiveData](data=LiveData(status="live"), meta=env.base_meta(), warnings=[])

    @router.get(
        "/health/ready",
        response_model=Envelope[ReadyData],
        responses={503: ERROR_RESPONSES[503]},
        summary="Serving dependencies (Cassandra + schema) are ready; not a data-freshness signal",
    )
    def health_ready(env: RequestEnv = Depends(get_env)) -> Envelope[ReadyData]:
        checks = env.repo.ping(deadline=env.deadline)
        if checks.get("schema_tables") != "ok":
            raise ApiError(
                503,
                "schema_not_ready",
                "Cassandra is reachable but the serving schema is missing",
                retryable=True,
                details=[dict(checks)],
                hint="apply the CQL migrations with `scripts/infra.py init`",
                meta=env.base_meta(),
            )
        return Envelope[ReadyData](
            data=ReadyData(status="ready", checks=ReadyChecks(**checks)), meta=env.base_meta(), warnings=[]
        )

    @router.get(
        "/metrics",
        response_class=PlainTextResponse,
        summary="Prometheus text: API registry plus exported component textfiles",
    )
    def metrics(env: RequestEnv = Depends(get_env)) -> PlainTextResponse:
        text = env.services.metrics.render(settings.metrics_dir)
        return PlainTextResponse(text, media_type=CONTENT_TYPE_LATEST)

    return router
