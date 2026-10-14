"""``GET /alerts``: MBTA service alerts of the current alerts generation (category ``MBTA_SERVICE_ALERT``).

Alerts applying to several scope routes are stored once per route partition; they are merged by
``alert_id`` (``route_ids`` lists every matching route). ``active_state_at_clock`` is re-evaluated at the
application clock with :func:`contracts.policy.alert_active_state`. Pipeline data-quality signals are
never mixed into ``data``: they appear only in ``warnings``.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from transitpulse.api.builders import as_list, parse_json_text
from transitpulse.api.context import INFO, RunContext
from transitpulse.api.deps import RequestEnv, get_env
from transitpulse.api.models import ActivePeriod, AlertItem, AlertsData, Envelope
from transitpulse.api.params import ERROR_RESPONSES, OptionalRouteIdQuery, RunIdQuery
from transitpulse.api.settings import ApiSettings
from transitpulse.contracts.enums import AlertActiveState, AlertConfirmation, FeedType
from transitpulse.contracts.policy import alert_active_state

AL = FeedType.ALERTS.value
ALERTS_LIMIT = 500
UNKNOWN_ACTIVE_STATE = "UNKNOWN"
UNREADABLE_ACTIVE_PERIODS = "UNREADABLE_ACTIVE_PERIODS"


def _epoch(value: Any) -> int | None:
    """Period bound -> epoch seconds (GTFS-RT ``start``/``end`` are epoch seconds; ISO strings tolerated)."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            return int(text)
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return None
        return int(parsed.timestamp())
    return None


def _periods(raw: Any) -> tuple[list[dict[str, int | None]], list[ActivePeriod]] | None:
    """Parsed ``active_periods_json``; ``None`` when the stored text is unreadable."""
    if raw is None:
        return [], []
    value = parse_json_text(raw)
    if not isinstance(value, list):
        return None
    bounds: list[dict[str, int | None]] = []
    periods: list[ActivePeriod] = []
    for item in value:
        if not isinstance(item, Mapping):
            return None
        start, end = _epoch(item.get("start")), _epoch(item.get("end"))
        bounds.append({"start": start, "end": end})
        periods.append(
            ActivePeriod(
                start=datetime.fromtimestamp(start, UTC) if start is not None else None,
                end=datetime.fromtimestamp(end, UTC) if end is not None else None,
            )
        )
    return bounds, periods


def alert_item(row: Mapping[str, Any], route_ids: list[str], clock: datetime) -> AlertItem:
    parsed = _periods(row.get("active_periods_json"))
    flags = {str(f) for f in as_list(row.get("quality_flags"))}
    if parsed is None:
        periods: list[ActivePeriod] = []
        state = UNKNOWN_ACTIVE_STATE
        flags.add(UNREADABLE_ACTIVE_PERIODS)
    else:
        bounds, periods = parsed
        state = alert_active_state(bounds, int(clock.timestamp())).value
    confirmation = row.get("confirmation_status")
    inherited_from = row.get("inherited_from_snapshot_id")
    return AlertItem(
        alert_id=str(row["alert_id"]),
        route_ids=route_ids,
        cause=row.get("cause"),
        effect=row.get("effect"),
        severity_level=row.get("severity_level"),
        header_text=row.get("header_text"),
        description_text=row.get("description_text"),
        url=row.get("url"),
        translations=parse_json_text(row.get("translations_json")),
        active_periods=periods,
        active_state_at_clock=state,
        active_state_at_build=row.get("active_state_at_build"),
        informed_entities=parse_json_text(row.get("informed_entities_json")),
        match_kinds=sorted(str(k) for k in as_list(row.get("match_kinds"))),
        selector_resolution=row.get("selector_resolution"),
        confirmation_status=confirmation,
        inherited=confirmation == AlertConfirmation.UNCONFIRMED_INHERITED.value or inherited_from is not None,
        inherited_from_snapshot_id=inherited_from,
        first_seen_snapshot_id=row.get("first_seen_snapshot_id"),
        feed_timestamp=row.get("feed_timestamp"),
        quality_flags=sorted(flags),
    )


def build_router(settings: ApiSettings) -> APIRouter:
    router = APIRouter(tags=["alerts"], responses=ERROR_RESPONSES)

    @router.get(
        "/alerts",
        response_model=Envelope[AlertsData],
        summary="MBTA service alerts applying to scope routes, with state at the application clock",
    )
    def list_alerts(
        run_id: RunIdQuery,
        route_id: OptionalRouteIdQuery = None,
        active_state: Annotated[
            AlertActiveState | None, Query(description="filter by active_state_at_clock")
        ] = None,
        env: RequestEnv = Depends(get_env),
    ) -> Envelope[AlertsData]:
        ctx = RunContext.load(env, run_id)
        routes = ctx.fanout_routes(route_id)
        clock = ctx.require_clock()
        view = ctx.require_feed(AL)
        snapshot_id = view.snapshot_id or ""
        reads = ctx.read_partitions(
            view,
            "alerts_by_route_snapshot",
            routes,
            limit=ALERTS_LIMIT,
            fetch=lambda keys, n: env.repo.alerts(
                ctx.run_id, snapshot_id, [str(k) for k in keys], limit_per_route=n, deadline=env.deadline
            ),
        )
        assert reads is not None  # required=True raises instead of returning None
        first_row: dict[str, Mapping[str, Any]] = {}
        alert_routes: dict[str, list[str]] = {}
        for r in routes:
            for row in reads[r].rows:
                alert_id = str(row["alert_id"])
                first_row.setdefault(alert_id, row)
                alert_routes.setdefault(alert_id, []).append(r)
        items = [alert_item(first_row[a], alert_routes[a], clock) for a in sorted(first_row)]
        if active_state is not None:
            items = [a for a in items if a.active_state_at_clock == active_state.value]
        truncated = any(reads[r].truncated for r in routes) or len(items) > ALERTS_LIMIT
        items = items[:ALERTS_LIMIT]
        reason = None
        if not items:
            reason = "EMPTY_SNAPSHOT" if view.state == "empty" else "NO_MATCHING_ALERTS"
        if truncated:
            ctx.warn("RESULT_TRUNCATED", f"at most {ALERTS_LIMIT} alerts are returned", INFO)
        return Envelope[AlertsData](
            data=AlertsData(
                route_ids=routes,
                snapshot_id=view.snapshot_id,
                evaluated_at=clock,
                alerts=items,
                returned_count=len(items),
                truncated=truncated,
                reason=reason,
            ),
            meta=ctx.meta(
                coverage=ctx.coverage(
                    routes, denominator_unit="alert_id", returned_rows=len(items), max_rows=ALERTS_LIMIT,
                    truncated=truncated,
                )
            ),
            warnings=ctx.warnings,
        )

    return router
