"""Row -> response-model conversions shared by several routers."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

from transitpulse.api.models import ComponentStatus

SOURCE_COMPONENT_PREFIXES = ("collector", "feed:", "publisher")


def parse_json_text(raw: Any) -> Any:
    """Parse a stored ``*_json`` text column; ``None`` when absent or unparsable (never ``{}``)."""
    if raw is None:
        return None
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except ValueError:
        return None


def parse_json_object(raw: Any) -> dict[str, Any] | None:
    value = parse_json_text(raw)
    return value if isinstance(value, dict) else None


def ms_to_seconds(value: Any) -> float | None:
    """Stored milliseconds (``*_ms`` columns) -> seconds (API unit)."""
    if value is None or isinstance(value, bool):
        return None
    return round(float(value) / 1000.0, 3)


def as_list(value: Any) -> list[Any]:
    """Set/list column -> list (``None`` -> ``[]``: an absent set means no members)."""
    if value is None:
        return []
    return list(value)


def component_status(row: Mapping[str, Any], wall_now: datetime) -> ComponentStatus:
    heartbeat = row.get("last_heartbeat_at")
    age = round((wall_now - heartbeat).total_seconds(), 3) if isinstance(heartbeat, datetime) else None
    return ComponentStatus(
        component=str(row.get("component")),
        status=row.get("status"),
        last_heartbeat_at=heartbeat,
        heartbeat_age_seconds=age,
        last_success_at=row.get("last_success_at"),
        last_batch_id=row.get("last_batch_id"),
        last_batch_duration_seconds=ms_to_seconds(row.get("last_batch_duration_ms")),
        input_messages_total=row.get("input_messages_total"),
        rejected_messages_total=row.get("rejected_messages_total"),
        snapshots_built_total=row.get("snapshots_built_total"),
        snapshots_published_total=row.get("snapshots_published_total"),
        late_deliveries_total=row.get("late_deliveries_total"),
        duplicate_deliveries_total=row.get("duplicate_deliveries_total"),
        last_snapshot_id=row.get("last_snapshot_id"),
        last_published_snapshot_id=row.get("last_published_snapshot_id"),
        last_visibility_latency_seconds=ms_to_seconds(row.get("last_visibility_latency_ms")),
        last_rejected_at=row.get("last_rejected_at"),
        last_rejected_reason=row.get("last_rejected_reason"),
        last_rejected_error_id=row.get("last_rejected_error_id"),
        last_error=row.get("last_error"),
        details=parse_json_object(row.get("progress_json")),
    )


def component_statuses(
    rows: Iterable[Mapping[str, Any]] | None, wall_now: datetime, *, source_only: bool = False
) -> list[ComponentStatus]:
    out = []
    for row in rows or ():
        name = str(row.get("component") or "")
        if source_only and not name.startswith(SOURCE_COMPONENT_PREFIXES):
            continue
        out.append(component_status(row, wall_now))
    return sorted(out, key=lambda c: c.component)
