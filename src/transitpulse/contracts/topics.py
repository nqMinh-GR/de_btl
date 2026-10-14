"""Kafka topic names and their desired configurations (created idempotently by ``infra.py init``).

One partition per topic (ordering per run/feed); RF1 single broker. Partitions are never changed and
topics are never deleted by init; a config drift is reported as a diff.
"""

from __future__ import annotations

from transitpulse.config import ProjectConfig

VEHICLE_POSITIONS = "mbta.vehicle_positions.snapshots.v1"
TRIP_UPDATES = "mbta.trip_updates.snapshots.v1"
ALERTS = "mbta.alerts.snapshots.v1"
CONTROL = "mbta.control.v1"
DLQ = "mbta.dlq.v1"

TOPIC_BY_FEED: dict[str, str] = {
    "vehicle_positions": VEHICLE_POSITIONS,
    "trip_updates": TRIP_UPDATES,
    "alerts": ALERTS,
}
FEED_BY_TOPIC: dict[str, str] = {v: k for k, v in TOPIC_BY_FEED.items()}
SNAPSHOT_TOPICS: tuple[str, ...] = (VEHICLE_POSITIONS, TRIP_UPDATES, ALERTS)
ALL_TOPICS: tuple[str, ...] = (*SNAPSHOT_TOPICS, CONTROL, DLQ)


def desired_topic_configs(cfg: ProjectConfig) -> dict[str, dict[str, str]]:
    """Topic -> Kafka topic config (string values, as AdminClient expects)."""
    retention_ms = str(int(cfg.kafka.retention_hours * 3600 * 1000))
    common = {
        "cleanup.policy": "delete",
        "retention.ms": retention_ms,
        "segment.ms": "900000",
        "min.insync.replicas": "1",
    }
    snap = {
        **common,
        "max.message.bytes": str(cfg.kafka.topic_max_message_bytes),
        "retention.bytes": str(cfg.kafka.snapshot_retention_bytes),
        "segment.bytes": "67108864",
    }
    small = {
        **common,
        "max.message.bytes": str(cfg.kafka.control_max_message_bytes),
        "segment.bytes": "16777216",
    }
    return {
        VEHICLE_POSITIONS: dict(snap),
        TRIP_UPDATES: dict(snap),
        ALERTS: dict(snap),
        CONTROL: {**small, "retention.bytes": "134217728"},
        DLQ: {**small, "retention.bytes": "268435456"},
    }
