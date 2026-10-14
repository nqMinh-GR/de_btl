"""Frozen enumerations shared by every component.

String values are the wire/storage representation (Kafka JSON, Cassandra text columns, API JSON).
Never rename a value without a schema_version bump and a decisions.md entry.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class FeedType(StrEnum):
    VEHICLE_POSITIONS = "vehicle_positions"
    TRIP_UPDATES = "trip_updates"
    ALERTS = "alerts"


FEED_ABBR: dict[str, str] = {"vehicle_positions": "vp", "trip_updates": "tu", "alerts": "al"}
FEED_ORDER: dict[str, int] = {"vehicle_positions": 0, "trip_updates": 1, "alerts": 2}


class SourceType(StrEnum):
    LIVE = "live"
    REPLAY = "replay"
    SYNTHETIC = "synthetic"


class ClockMode(StrEnum):
    WALL = "wall"      # live: application_clock = wall UTC now
    SOURCE = "source"  # replay/synthetic: application_clock advanced only by control clock ticks


class RunState(StrEnum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class GenerationStatus(StrEnum):
    BUILDING = "BUILDING"
    READY = "READY"


class PublishOutcome(StrEnum):
    PUBLISHED = "PUBLISHED"
    LATE_NOT_PUBLISHED = "LATE_NOT_PUBLISHED"
    EXPIRED_NOT_PUBLISHED = "EXPIRED_NOT_PUBLISHED"


class ScheduleResolution(StrEnum):
    RESOLVED = "resolved"
    AMBIGUOUS = "ambiguous"
    UNRESOLVED = "unresolved"
    NOT_APPLICABLE = "not_applicable"   # ADDED trips, no static schedule exists


class ResolutionMethod(StrEnum):
    DESCRIPTOR = "descriptor"           # start_date from TripDescriptor
    INFERRED = "inferred"               # single candidate service date


class UncertaintyClass(StrEnum):
    """mbta_bus_uncertainty_v1 classes (stored lower-case as prediction_quality)."""

    REALTIME = "realtime"                         # 0 <= u < 300
    UNKNOWN_UNCERTAINTY = "unknown_uncertainty"   # field absent (never treated as 0)
    SCHEDULE_ONLY = "schedule_only"               # u == 300
    DEGRADED = "degraded"                         # u == 301
    INVALID = "invalid"                           # u > 301, negative, non-integer, or invalid time


class EventType(StrEnum):
    ARRIVAL = "arrival"
    DEPARTURE = "departure"


class FreshnessClass(StrEnum):
    FRESH = "fresh"
    STALE = "stale"
    UNKNOWN = "unknown"
    INVALID = "invalid"


class RecordKind(StrEnum):
    OBSERVATION = "observation"
    SNAPSHOT_STATE_SAMPLE = "snapshot_state_sample"


class EventTimeOrigin(StrEnum):
    VEHICLE_TIMESTAMP = "vehicle_timestamp"
    FEED_HEADER = "feed_header"


class EntityStatus(StrEnum):
    FULLY_VALID = "fully_valid"
    PARTIALLY_VALID = "partially_valid"   # retained with warnings
    REJECTED = "rejected"


class AlertActiveState(StrEnum):
    ALWAYS = "ALWAYS"       # no active_period
    ACTIVE = "ACTIVE"
    UPCOMING = "UPCOMING"
    EXPIRED = "EXPIRED"


class AlertConfirmation(StrEnum):
    CONFIRMED = "CONFIRMED"
    UNCONFIRMED_INHERITED = "UNCONFIRMED_INHERITED"


class Severity(StrEnum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class Disposition(StrEnum):
    RETAINED = "retained"
    EXCLUDED_FROM_METRIC = "excluded_from_metric"
    QUARANTINED = "quarantined"
    DUPLICATE = "duplicate"
    OUT_OF_SCOPE = "out_of_scope"
    REJECTED = "rejected"


class AlertCategory(StrEnum):
    MBTA_SERVICE_ALERT = "MBTA_SERVICE_ALERT"
    PIPELINE_DATA_QUALITY_ALERT = "PIPELINE_DATA_QUALITY_ALERT"


@dataclass(frozen=True)
class ReasonInfo:
    code: str
    severity: Severity
    disposition: Disposition
    unit: str            # what is counted: message | snapshot | entity | prediction | trip_instance | selector | vehicle
    category: str        # contract | vehicle | time | prediction | lineage | projection | alert | kpi | source


def _r(code: str, sev: Severity, disp: Disposition, unit: str, cat: str) -> ReasonInfo:
    return ReasonInfo(code, sev, disp, unit, cat)


SE, SW, SI = Severity.ERROR, Severity.WARNING, Severity.INFO
D = Disposition

# Default severity/disposition per reason. A record may carry several warnings; warning counts are
# never used as a denominator of an error rate (spec md §16.1-16.2, PDF p22).
QUALITY_REASONS: dict[str, ReasonInfo] = {r.code: r for r in [
    # contract / transport (message level)
    _r("INVALID_JSON", SE, D.QUARANTINED, "message", "contract"),
    _r("UNSUPPORTED_SCHEMA", SE, D.QUARANTINED, "message", "contract"),
    _r("INVALID_HEADER", SE, D.QUARANTINED, "message", "contract"),
    _r("UNSUPPORTED_INCREMENTALITY", SE, D.QUARANTINED, "message", "contract"),
    _r("PAYLOAD_TOO_LARGE", SE, D.QUARANTINED, "message", "contract"),
    _r("STATIC_VERSION_MISMATCH", SE, D.QUARANTINED, "message", "lineage"),
    _r("INVALID_ENTITY", SE, D.REJECTED, "entity", "contract"),
    # vehicle
    _r("MISSING_VEHICLE_ID", SE, D.REJECTED, "entity", "vehicle"),
    _r("INVALID_COORDINATES", SE, D.EXCLUDED_FROM_METRIC, "entity", "vehicle"),
    _r("FUTURE_TIMESTAMP", SE, D.EXCLUDED_FROM_METRIC, "entity", "vehicle"),
    _r("INVALID_TIMESTAMP_UNIT", SE, D.EXCLUDED_FROM_METRIC, "entity", "time"),
    _r("CLOCK_SKEW", SW, D.RETAINED, "entity", "time"),
    _r("MISSING_OBSERVATION_TIMESTAMP", SW, D.RETAINED, "entity", "time"),
    _r("UNKNOWN_ROUTE_OR_TRIP", SW, D.OUT_OF_SCOPE, "entity", "projection"),
    _r("DUPLICATE_OBSERVATION", SI, D.DUPLICATE, "entity", "lineage"),
    _r("OBSERVATION_TIME_REGRESSION", SW, D.RETAINED, "entity", "projection"),
    # time / schedule
    _r("AMBIGUOUS_SERVICE_DATE", SW, D.EXCLUDED_FROM_METRIC, "trip_instance", "time"),
    _r("UNRESOLVED_STOP_SEQUENCE", SW, D.EXCLUDED_FROM_METRIC, "prediction", "time"),
    _r("SERVICE_DATE_INFERRED", SI, D.RETAINED, "trip_instance", "time"),
    _r("SERVICE_NOT_ACTIVE_ON_START_DATE", SW, D.EXCLUDED_FROM_METRIC, "trip_instance", "time"),
    _r("START_TIME_MISMATCH", SW, D.EXCLUDED_FROM_METRIC, "trip_instance", "time"),
    _r("DESCRIPTOR_STATIC_MISMATCH", SW, D.EXCLUDED_FROM_METRIC, "trip_instance", "time"),
    _r("STOP_ID_SEQUENCE_MISMATCH", SW, D.EXCLUDED_FROM_METRIC, "prediction", "time"),
    _r("UNKNOWN_STOP_FOR_TRIP", SW, D.EXCLUDED_FROM_METRIC, "prediction", "time"),
    # prediction
    _r("NO_PREDICTION_DATA", SI, D.EXCLUDED_FROM_METRIC, "prediction", "prediction"),
    _r("SCHEDULE_ONLY_PREDICTION", SI, D.EXCLUDED_FROM_METRIC, "prediction", "prediction"),
    _r("DEGRADED_PREDICTION", SW, D.EXCLUDED_FROM_METRIC, "prediction", "prediction"),
    _r("INVALID_PREDICTION", SW, D.EXCLUDED_FROM_METRIC, "prediction", "prediction"),
    _r("UNKNOWN_UNCERTAINTY", SI, D.RETAINED, "prediction", "prediction"),
    # kpi exclusions (per trip instance)
    _r("CANCELED_TRIP", SI, D.EXCLUDED_FROM_METRIC, "trip_instance", "kpi"),
    _r("ADDED_TRIP", SI, D.EXCLUDED_FROM_METRIC, "trip_instance", "kpi"),
    _r("UNRESOLVED_TRIP", SW, D.EXCLUDED_FROM_METRIC, "trip_instance", "kpi"),
    _r("NO_TARGET_STOP", SI, D.EXCLUDED_FROM_METRIC, "trip_instance", "kpi"),
    _r("NO_SCHEDULED_EVENT", SW, D.EXCLUDED_FROM_METRIC, "trip_instance", "kpi"),
    # lineage / ordering
    _r("DUPLICATE_SNAPSHOT_DELIVERY", SI, D.DUPLICATE, "snapshot", "lineage"),
    _r("LATE_SNAPSHOT_NOT_PUBLISHED", SW, D.RETAINED, "snapshot", "lineage"),
    _r("EXPIRED_NOT_PUBLISHED", SW, D.RETAINED, "snapshot", "lineage"),
    _r("PARENT_UNAVAILABLE", SW, D.RETAINED, "snapshot", "projection"),
    # alerts
    _r("UNRESOLVED_ALERT_SELECTOR", SW, D.EXCLUDED_FROM_METRIC, "selector", "alert"),
    _r("INVALID_SELECTOR", SW, D.REJECTED, "selector", "alert"),
    _r("NO_SCOPE_MATCH", SI, D.OUT_OF_SCOPE, "selector", "alert"),
    # source health (collector status / pipeline warnings)
    _r("SOURCE_FETCH_FAILED", SW, D.RETAINED, "message", "source"),
    _r("DISK_BUDGET_EXCEEDED", SE, D.QUARANTINED, "message", "source"),
]}

# The 20 reasons the spec requires (md §16.1) are all present above.
SPEC_REQUIRED_REASONS: tuple[str, ...] = (
    "INVALID_JSON", "UNSUPPORTED_SCHEMA", "INVALID_HEADER", "UNSUPPORTED_INCREMENTALITY", "PAYLOAD_TOO_LARGE",
    "MISSING_VEHICLE_ID", "INVALID_COORDINATES", "MISSING_OBSERVATION_TIMESTAMP", "FUTURE_TIMESTAMP",
    "OBSERVATION_TIME_REGRESSION", "UNKNOWN_ROUTE_OR_TRIP", "AMBIGUOUS_SERVICE_DATE", "UNRESOLVED_STOP_SEQUENCE",
    "NO_PREDICTION_DATA", "SCHEDULE_ONLY_PREDICTION", "DEGRADED_PREDICTION", "INVALID_PREDICTION",
    "DUPLICATE_OBSERVATION", "LATE_SNAPSHOT_NOT_PUBLISHED", "STATIC_VERSION_MISMATCH",
)

# KPI exclusion reasons in their fixed precedence order (first match wins), design C §5.
KPI_EXCLUSION_ORDER: tuple[str, ...] = (
    "CANCELED_TRIP",
    "ADDED_TRIP",
    "AMBIGUOUS_SERVICE_DATE",
    "UNRESOLVED_TRIP",
    "UNRESOLVED_STOP_SEQUENCE",
    "NO_TARGET_STOP",
    "SCHEDULE_ONLY_PREDICTION",
    "DEGRADED_PREDICTION",
    "INVALID_PREDICTION",
    "NO_SCHEDULED_EVENT",
)


def reason(code: str) -> ReasonInfo:
    """Return the registered ReasonInfo; unknown codes are a programming error."""
    try:
        return QUALITY_REASONS[code]
    except KeyError as exc:  # pragma: no cover - guarded by unit tests
        raise KeyError(f"unregistered quality reason {code!r}") from exc
