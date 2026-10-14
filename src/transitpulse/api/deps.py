"""Application services and per-request dependencies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from fastapi import Request

from transitpulse.api.cache import ImmutableCache
from transitpulse.api.errors import request_id_of
from transitpulse.api.metrics import ApiMetrics, route_label
from transitpulse.api.models import Meta
from transitpulse.api.paging import TokenSigner
from transitpulse.api.repository import Deadline, ServingRepository
from transitpulse.api.settings import ApiSettings
from transitpulse.config import ApiSection, ProjectConfig


@dataclass
class ApiServices:
    """Process-wide singletons attached to ``app.state.services``."""

    settings: ApiSettings
    repo: ServingRepository
    cache: ImmutableCache
    metrics: ApiMetrics
    signer: TokenSigner

    @property
    def cfg(self) -> ProjectConfig:
        return self.settings.cfg

    def wall_now(self) -> datetime:
        return self.settings.wall_clock()


@dataclass(frozen=True)
class RequestEnv:
    """Everything one request needs: services, request id, wall time at start, time budget."""

    services: ApiServices
    request_id: str
    served_at: datetime
    deadline: Deadline
    endpoint: str

    @property
    def cfg(self) -> ProjectConfig:
        return self.services.settings.cfg

    @property
    def api(self) -> ApiSection:
        return self.services.settings.cfg.api

    @property
    def repo(self) -> ServingRepository:
        return self.services.repo

    def base_meta(self) -> Meta:
        """Meta of responses that are not about a run (health, run list)."""
        return Meta(request_id=self.request_id, served_at=self.served_at)


def get_services(request: Request) -> ApiServices:
    return request.app.state.services


async def get_env(request: Request) -> RequestEnv:
    """FastAPI dependency: per-request environment (no I/O, so it runs on the event loop)."""
    services: ApiServices = request.app.state.services
    endpoint = route_label(request.scope.get("route"), services.settings.base_path)
    api = services.settings.cfg.api
    return RequestEnv(
        services=services,
        request_id=request_id_of(request),
        served_at=services.wall_now(),
        deadline=Deadline.start(api.request_timeout_seconds, api.statement_timeout_seconds),
        endpoint=endpoint,
    )
