"""FastAPI application factory.

Run with (interfaces §8)::

    uvicorn transitpulse.api.app:create_app --factory --host 127.0.0.1 --port 8000 --workers 1

Environment: ``TP_PROFILE``, ``TP_WORKDIR`` (and optionally ``TP_API_TOKEN_KEY``). Endpoints are sync
functions executed in the threadpool (D-E18). The Cassandra session is created lazily: startup only
launches a background connection attempt, so the app starts and ``/health/live`` answers 200 while
Cassandra is down; ``/health/ready`` and data endpoints answer 503 until it is reachable.
"""

from __future__ import annotations

import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib import metadata

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.concurrency import run_in_threadpool

from transitpulse.api.cache import ImmutableCache
from transitpulse.api.deps import ApiServices
from transitpulse.api.errors import REQUEST_ID_HEADER, install_error_handlers
from transitpulse.api.metrics import ApiMetrics
from transitpulse.api.middleware import RequestContextMiddleware
from transitpulse.api.paging import TokenSigner
from transitpulse.api.repository import CassandraRepository, ServingRepository
from transitpulse.api.routers import alerts, health, pipeline, quality, routes, runs, stops, trips, vehicles
from transitpulse.api.settings import ApiSettings
from transitpulse.obs.logging import configure, get_logger, log_event

_LOG = get_logger("transitpulse.api.app")

API_TITLE = "TransitPulse MBTA API"
API_DESCRIPTION = (
    "Serving API over the published Cassandra generations of a TransitPulse run. Every data response is "
    "{data, meta, warnings}; every error is {error, meta}. Instants are ISO-8601 UTC with 'Z'. MBTA service "
    "alerts are data; pipeline data-quality signals appear only in `warnings`. Local demo API: no "
    "authentication, CORS allowlist, bind to 127.0.0.1."
)


def _version() -> str:
    try:
        return metadata.version("transitpulse")
    except metadata.PackageNotFoundError:
        return "0.0.0+unknown"


def _warmup(services: ApiServices) -> None:
    try:
        services.repo.warmup()
    except Exception as exc:  # warmup is best effort; requests reconnect lazily
        log_event(_LOG, "startup", "failed", f"repository warmup failed: {type(exc).__name__}: {exc}")


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    services: ApiServices = app.state.services
    settings = services.settings
    metrics_dir = settings.metrics_dir
    if metrics_dir is not None:
        services.metrics.start_exporter(metrics_dir, settings.cfg.observability.metrics_export_seconds)
    threading.Thread(target=_warmup, args=(services,), name="api-repository-warmup", daemon=True).start()
    log_event(_LOG, "startup", "ok", f"API ready to accept requests under {settings.base_path or '/'}")
    try:
        yield
    finally:
        services.metrics.stop(metrics_dir)
        await run_in_threadpool(services.repo.close)
        log_event(_LOG, "shutdown", "ok", "API stopped")


def create_app(settings: ApiSettings | None = None, repo: ServingRepository | None = None) -> FastAPI:
    """Build the API. ``settings`` defaults to :meth:`ApiSettings.from_env`; ``repo`` to Cassandra."""
    settings = settings if settings is not None else ApiSettings.from_env()
    if settings.configure_logging:
        configure("api")
    metrics = ApiMetrics()
    repository: ServingRepository = (
        repo
        if repo is not None
        else CassandraRepository(settings.cfg, reconnect_interval_seconds=settings.reconnect_interval_seconds)
    )
    cache = ImmutableCache(
        settings.api.cache_entries,
        settings.api.cache_max_ttl_seconds,
        on_hit=metrics.cache_hits.inc,
        on_miss=metrics.cache_misses.inc,
    )
    services = ApiServices(
        settings=settings, repo=repository, cache=cache, metrics=metrics, signer=TokenSigner(settings.token_key)
    )

    base = settings.base_path
    app = FastAPI(
        title=API_TITLE,
        version=_version(),
        description=API_DESCRIPTION,
        lifespan=_lifespan,
        openapi_url=f"{base}/openapi.json",
        docs_url=f"{base}/docs",
        redoc_url=None,
    )
    app.state.services = services
    install_error_handlers(app)
    # add_middleware prepends: the request-context middleware ends up outermost, so CORS preflight
    # responses also carry X-Request-ID and are counted.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(settings.api.cors_allow_origins),
        allow_methods=["GET"],
        allow_headers=[REQUEST_ID_HEADER, "Accept", "Content-Type"],
        expose_headers=[REQUEST_ID_HEADER],
        allow_credentials=False,
        max_age=600,
    )
    app.add_middleware(RequestContextMiddleware, metrics=metrics, base_path=base)
    for module in (health, runs, routes, trips, vehicles, stops, alerts, quality, pipeline):
        app.include_router(module.build_router(settings), prefix=base)
    return app
