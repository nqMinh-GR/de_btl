"""``GET /pipeline/status``: run clock and state, per-component heartbeats/progress, generation states.

Heartbeat ages are wall-clock ages (``served_at - last_heartbeat_at``), independent of the replay
clock. ``kafka_backlog`` and ``latency`` are returned only when a component has measured them (never
fabricated as 0).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from transitpulse.api.builders import component_statuses
from transitpulse.api.context import FEED_TYPES, RunContext
from transitpulse.api.deps import RequestEnv, get_env
from transitpulse.api.models import ComponentStatus, Envelope, PipelineStatusData
from transitpulse.api.params import ERROR_RESPONSES, RunIdQuery
from transitpulse.api.settings import ApiSettings


HEARTBEAT_COMPONENT = "heartbeat"
BACKLOG_KEYS = ("topic", "partition", "end_offset", "latest_offset", "backlog")


def _kafka_backlog(components: list[ComponentStatus]) -> list[dict[str, Any]] | None:
    """Kafka backlog measured by the processor's query listener.

    The listener (``processing.stream_job.backlog_entries``) stores
    ``[{topic, partition, end_offset, latest_offset, backlog}]`` under ``kafka_backlog`` in the
    ``pipeline_status('heartbeat')`` row's ``progress_json`` on progress events (``backlog`` = messages,
    null when an offset is unknown). Idle heartbeats carry no backlog, so ``None`` means "not measured".
    """
    heartbeat = next((c for c in components if c.component == HEARTBEAT_COMPONENT), None)
    if heartbeat is None or not heartbeat.details:
        return None
    backlog = heartbeat.details.get("kafka_backlog")
    if not isinstance(backlog, list) or not backlog:
        return None
    entries = [{k: item.get(k) for k in BACKLOG_KEYS} for item in backlog if isinstance(item, dict)]
    return entries or None


def _latency(components: list[ComponentStatus]) -> dict[str, float] | None:
    """Last measured visibility latency per feed (``feed:<feed_type>`` rows)."""
    out: dict[str, float] = {}
    for component in components:
        if component.component.startswith("feed:") and component.last_visibility_latency_seconds is not None:
            out[component.component.split(":", 1)[1]] = component.last_visibility_latency_seconds
    return out or None


def build_router(settings: ApiSettings) -> APIRouter:
    router = APIRouter(tags=["pipeline"], responses=ERROR_RESPONSES)

    @router.get(
        "/pipeline/status",
        response_model=Envelope[PipelineStatusData],
        summary="Run clock/state, component heartbeats and progress, generation states",
    )
    def pipeline_status(run_id: RunIdQuery, env: RequestEnv = Depends(get_env)) -> Envelope[PipelineStatusData]:
        ctx = RunContext.load(env, run_id, source_health=True)
        components = component_statuses(ctx.pipeline_rows, env.served_at)
        generations = {ft: ctx.feed(ft).to_meta() for ft in FEED_TYPES}
        run = ctx.run
        return Envelope[PipelineStatusData](
            data=PipelineStatusData(
                run_id=ctx.run_id,
                run_state=run.get("run_state"),
                state_reason=run.get("state_reason"),
                clock_mode=ctx.clock_mode,
                application_clock=ctx.application_clock,
                clock_sequence=run.get("clock_sequence"),
                clock_updated_at=run.get("clock_updated_at"),
                components=components,
                generations=generations,
                kafka_backlog=_kafka_backlog(components),
                latency=_latency(components),
            ),
            meta=ctx.meta(),
            warnings=ctx.warnings,
        )

    return router
