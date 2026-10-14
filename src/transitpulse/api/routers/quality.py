"""``GET /quality``: per-feed quality reasons and accounting of the current generations, plus source health.

Counts carry their unit (``message``, ``snapshot``, ``entity``, ``prediction``, ``trip_instance``,
``selector``); warning counts are never used as the denominator of an error rate. A feed whose
generation is expired/unavailable/inconsistent yields ``null`` plus a warning; 503 only when all three
feeds are unusable.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import APIRouter, Depends

from transitpulse.api.builders import as_list, component_statuses, parse_json_object
from transitpulse.api.context import FEED_TYPES, FeedView, PartitionRead, RunContext
from transitpulse.api.deps import RequestEnv, get_env
from transitpulse.api.models import Envelope, FeedQuality, QualityData, QualityReasonItem
from transitpulse.api.params import ERROR_RESPONSES, RunIdQuery
from transitpulse.api.settings import ApiSettings
from transitpulse.contracts.enums import QUALITY_REASONS

QUALITY_LIMIT = 256
SAMPLE_REFS_LIMIT = 10
_SEVERITY_ORDER = {"error": 0, "warning": 1, "info": 2}


def _reason_item(row: Mapping[str, Any]) -> QualityReasonItem:
    code = str(row["reason_code"])
    info = QUALITY_REASONS.get(code)
    disposition_counts = row.get("disposition_counts")
    return QualityReasonItem(
        reason_code=code,
        severity=row.get("severity") or (info.severity.value if info else None),
        disposition=info.disposition.value if info else None,
        unit=row.get("unit") or (info.unit if info else None),
        category=info.category if info else None,
        count=row.get("record_count"),
        disposition_counts={str(k): int(v) for k, v in disposition_counts.items()} if disposition_counts else None,
        sample_refs=[str(s) for s in as_list(row.get("sample_refs"))[:SAMPLE_REFS_LIMIT]],
    )


def _feed_quality(view: FeedView, read: PartitionRead) -> FeedQuality:
    manifest: Mapping[str, Any] = view.manifest or {}
    reasons = sorted(
        (_reason_item(r) for r in read.rows),
        key=lambda item: (_SEVERITY_ORDER.get(str(item.severity), 3), item.reason_code),
    )
    return FeedQuality(
        feed_type=view.feed_type,
        snapshot_id=view.snapshot_id,
        is_partial=view.is_partial,
        is_empty=view.is_empty,
        source_entity_count=manifest.get("source_entity_count"),
        selected_entity_count=manifest.get("selected_entity_count"),
        fully_valid_entity_count=manifest.get("fully_valid_entity_count"),
        partially_valid_entity_count=manifest.get("partially_valid_entity_count"),
        rejected_entity_count=manifest.get("rejected_entity_count"),
        accounting=parse_json_object(manifest.get("accounting_json")),
        reasons=reasons,
        truncated=read.truncated,
    )


def build_router(settings: ApiSettings) -> APIRouter:
    router = APIRouter(tags=["quality"], responses=ERROR_RESPONSES)

    @router.get(
        "/quality",
        response_model=Envelope[QualityData],
        summary="Quality reasons, dispositions and accounting per feed generation; source health",
    )
    def quality(run_id: RunIdQuery, env: RequestEnv = Depends(get_env)) -> Envelope[QualityData]:
        ctx = RunContext.load(env, run_id)
        feeds: dict[str, FeedQuality | None] = {}
        for ft in FEED_TYPES:
            view = ctx.feed(ft)
            if not view.usable:
                feeds[ft] = None
                continue
            snapshot_id = view.snapshot_id or ""
            reads = ctx.read_partitions(
                view,
                "quality_by_snapshot",
                [None],
                limit=QUALITY_LIMIT,
                fetch=lambda keys, n, f=ft, s=snapshot_id: {
                    None: env.repo.quality(ctx.run_id, f, s, limit=n, deadline=env.deadline)
                },
                required=False,
            )
            feeds[ft] = _feed_quality(ctx.feed(ft), reads[None]) if reads is not None else None
        final_views = [ctx.feed(ft) for ft in FEED_TYPES]
        for view in final_views:
            ctx.warn_unusable(view)
        if all(feeds[ft] is None for ft in FEED_TYPES):
            raise ctx.feed_error(final_views[0])
        return Envelope[QualityData](
            data=QualityData(
                feeds=feeds,
                source_health=component_statuses(ctx.pipeline_rows, env.served_at, source_only=True),
            ),
            meta=ctx.meta(coverage=ctx.coverage(ctx.scope_route_ids)),
            warnings=ctx.warnings,
        )

    return router
