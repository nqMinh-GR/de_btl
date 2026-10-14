"""``GET /runs`` (paged run list) and ``GET /runs/{run_id}`` (scope, clock, versions, generations, source health)."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from transitpulse.api.builders import component_statuses, parse_json_object
from transitpulse.api.context import FEED_TYPES, RunContext
from transitpulse.api.deps import RequestEnv, get_env
from transitpulse.api.errors import ApiError
from transitpulse.api.models import Envelope, RunDetail, RunsData, RunSummary
from transitpulse.api.paging import MAX_TOKEN_CHARS, InvalidPageToken, RunsCursor, decode_runs_token, encode_runs_token
from transitpulse.api.params import ERROR_RESPONSES, RunIdPath
from transitpulse.api.settings import ApiSettings
from transitpulse.contracts.enums import ClockMode

RUNS_PAGE_DEFAULT = 20
RUNS_PAGE_MAX = 100


def _summary(index_row: Mapping[str, Any], run: Mapping[str, Any] | None, served_at: datetime) -> RunSummary:
    """Merge the ``runs_by_project`` row with ``run_metadata`` (which may be missing for a broken run)."""
    run = run or {}
    clock_mode = run.get("clock_mode")
    clock = served_at if clock_mode == ClockMode.WALL.value else run.get("application_clock")
    return RunSummary(
        run_id=str(index_row["run_id"]),
        created_at=index_row.get("created_at") or run.get("created_at"),
        source_type=run.get("source_type") or index_row.get("source_type"),
        clock_mode=clock_mode,
        scenario_id=run.get("scenario_id") or index_row.get("scenario_id"),
        label=run.get("label") or index_row.get("label"),
        run_state=run.get("run_state"),
        gtfs_version=run.get("gtfs_version") or index_row.get("gtfs_version"),
        scope_id=run.get("scope_id") or index_row.get("scope_id"),
        route_ids=list(run.get("route_ids") or []),
        application_clock=clock,
        clock_sequence=run.get("clock_sequence"),
    )


def build_router(settings: ApiSettings) -> APIRouter:
    router = APIRouter(tags=["runs"], responses=ERROR_RESPONSES)

    @router.get("/runs", response_model=Envelope[RunsData], summary="Runs of this project, newest first")
    def list_runs(
        limit: Annotated[int, Query(ge=1, le=RUNS_PAGE_MAX, description="page size")] = RUNS_PAGE_DEFAULT,
        page_token: Annotated[str | None, Query(min_length=1, max_length=MAX_TOKEN_CHARS)] = None,
        env: RequestEnv = Depends(get_env),
    ) -> Envelope[RunsData]:
        project_id = env.cfg.project.project_id
        signer = env.services.signer
        after: tuple[datetime, str] | None = None
        if page_token is not None:
            try:
                cursor = decode_runs_token(signer, page_token, project_id=project_id)
            except InvalidPageToken as exc:
                raise ApiError(
                    422,
                    "invalid_page_token",
                    str(exc),
                    details=[{"loc": ["query", "page_token"]}],
                    hint="restart from the first page (omit page_token)",
                    meta=env.base_meta(),
                ) from exc
            after = (cursor.created_at, cursor.run_id)
        rows = env.repo.runs_page(project_id, limit=limit + 1, after=after, deadline=env.deadline)
        page = rows[:limit]
        next_token = None
        if len(rows) > limit and page:
            last = page[-1]
            next_token = encode_runs_token(
                signer, project_id=project_id, cursor=RunsCursor(last["created_at"], str(last["run_id"]))
            )
        metadata = env.repo.run_metadata_many([str(r["run_id"]) for r in page], deadline=env.deadline)
        runs = [_summary(r, metadata.get(str(r["run_id"])), env.served_at) for r in page]
        return Envelope[RunsData](
            data=RunsData(runs=runs, returned_count=len(runs), next_page_token=next_token),
            meta=env.base_meta(),
            warnings=[],
        )

    @router.get(
        "/runs/{run_id}",
        response_model=Envelope[RunDetail],
        summary="Run scope, policy, clock, versions, per-feed generation summary and source health",
    )
    def get_run(run_id: RunIdPath, env: RequestEnv = Depends(get_env)) -> Envelope[RunDetail]:
        ctx = RunContext.load(env, run_id, source_health=True)
        generations = {ft: ctx.feed(ft).to_meta() for ft in FEED_TYPES}
        run = ctx.run
        data = RunDetail(
            run_id=ctx.run_id,
            project_id=run.get("project_id"),
            scope_id=run.get("scope_id"),
            source_type=run.get("source_type"),
            clock_mode=ctx.clock_mode,
            gtfs_version=run.get("gtfs_version"),
            agency_timezone=run.get("agency_timezone"),
            route_ids=list(ctx.scope_route_ids),
            pipeline_version=run.get("pipeline_version"),
            policy_version=ctx.policy.policy_version,
            policy=dataclasses.asdict(ctx.policy),
            policy_pinned=ctx.policy_pinned,
            fixture_kind=run.get("fixture_kind"),
            scenario_id=run.get("scenario_id"),
            seed=run.get("seed"),
            recording_id=run.get("recording_id"),
            session_id=run.get("session_id"),
            speed=run.get("speed"),
            label=run.get("label"),
            provenance=parse_json_object(run.get("provenance_json")),
            created_at=run.get("created_at"),
            run_state=run.get("run_state"),
            state_reason=run.get("state_reason"),
            application_clock=ctx.application_clock,
            clock_sequence=run.get("clock_sequence"),
            clock_updated_at=run.get("clock_updated_at"),
            generations=generations,
            source_health=component_statuses(ctx.pipeline_rows, env.served_at, source_only=True),
        )
        return Envelope[RunDetail](
            data=data, meta=ctx.meta(coverage=ctx.coverage(ctx.scope_route_ids)), warnings=ctx.warnings
        )

    return router
