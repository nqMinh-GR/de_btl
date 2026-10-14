"""Pydantic response contracts of the serving API (design E §4.2).

Conventions (spec md §13, PDF p20):
* every success response is ``{data, meta, warnings}``; every error is ``{error, meta}``;
* instants are ISO-8601 UTC strings with milliseconds and ``Z`` (:data:`UtcDatetime`);
* durations end in ``_seconds``, counts in ``_count``, rates are fractions in [0, 1] ending in ``_rate``;
* epoch values are never returned; missing values are ``null``, never zero or ``""``;
* MBTA service alerts are data (``category = MBTA_SERVICE_ALERT``); pipeline data-quality signals are
  only ever reported in ``warnings`` (``category = PIPELINE_DATA_QUALITY_ALERT``).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer

from transitpulse.contracts.envelope import iso_utc

T = TypeVar("T")


def _serialize_instant(value: datetime) -> str:
    return iso_utc(value)


UtcDatetime = Annotated[datetime, PlainSerializer(_serialize_instant, return_type=str, when_used="json")]
"""Timezone-aware instant serialised as ``YYYY-MM-DDTHH:MM:SS.mmmZ``."""

FeedState = Literal["available", "empty", "stale", "expired", "unavailable", "inconsistent", "not_applicable"]
WarningCategory = Literal["PIPELINE_DATA_QUALITY_ALERT", "INFO"]


class ApiModel(BaseModel):
    """Base of every response model: unknown fields are a programming error."""

    model_config = ConfigDict(extra="forbid")


# ================================================================================================
# Envelope and metadata
# ================================================================================================
class FeedMeta(ApiModel):
    """State of one feed's current generation as seen by this request (pointer read once)."""

    state: FeedState
    snapshot_id: str | None = None
    generation_id: str | None = None
    feed_timestamp: UtcDatetime | None = None
    metrics_as_of: UtcDatetime | None = None
    view_published_at: UtcDatetime | None = None
    expires_at: UtcDatetime | None = None
    is_partial: bool | None = None
    is_empty: bool | None = None
    source_age_seconds: float | None = None
    stale: bool | None = None
    stale_threshold_seconds: int | None = None
    detail: str | None = None


class Coverage(ApiModel):
    """What the response covers and how it is bounded."""

    scope_route_count: int
    route_ids: list[str]
    denominator_unit: str | None = None
    returned_rows: int | None = None
    max_rows: int | None = None
    truncated: bool = False


class FreshnessMeta(ApiModel):
    """Run-pinned freshness policy applied at ``evaluated_at`` (the application clock)."""

    clock_mode: str
    evaluated_at: UtcDatetime | None = None
    policy_version: str
    vehicle_stale_threshold_seconds: int
    prediction_stale_threshold_seconds: int
    alert_stale_threshold_seconds: int
    future_tolerance_seconds: int


class Meta(ApiModel):
    request_id: str
    served_at: UtcDatetime
    run_id: str | None = None
    source_type: str | None = None
    scenario_id: str | None = None
    run_state: str | None = None
    gtfs_version: str | None = None
    scope_id: str | None = None
    clock_mode: str | None = None
    application_clock: UtcDatetime | None = None
    clock_sequence: int | None = None
    policy_version: str | None = None
    feeds: dict[str, FeedMeta] = Field(default_factory=dict)
    coverage: Coverage | None = None
    freshness: FreshnessMeta | None = None


class ApiWarning(ApiModel):
    """A pipeline/serving condition the client must surface (never an MBTA service alert)."""

    code: str
    message: str
    category: WarningCategory
    feed_type: str | None = None
    reason_code: str | None = None
    count: int | None = None


class Envelope(ApiModel, Generic[T]):
    data: T
    meta: Meta
    warnings: list[ApiWarning] = Field(default_factory=list)


class ErrorBody(ApiModel):
    code: str
    message: str
    request_id: str
    retryable: bool
    details: list[dict[str, Any]] | None = None
    hint: str | None = None


class ErrorEnvelope(ApiModel):
    error: ErrorBody
    meta: Meta | None = None


# ================================================================================================
# Health
# ================================================================================================
class LiveData(ApiModel):
    status: Literal["live"]


class ReadyChecks(ApiModel):
    cassandra: Literal["ok"]
    release_version: str | None = None
    schema_tables: Literal["ok", "missing"]
    schema_version: str | None = None
    migrations_recorded: list[str] = Field(default_factory=list)


class ReadyData(ApiModel):
    status: Literal["ready"]
    checks: ReadyChecks


# ================================================================================================
# Runs and pipeline
# ================================================================================================
class RunSummary(ApiModel):
    run_id: str
    created_at: UtcDatetime | None = None
    source_type: str | None = None
    clock_mode: str | None = None
    scenario_id: str | None = None
    label: str | None = None
    run_state: str | None = None
    gtfs_version: str | None = None
    scope_id: str | None = None
    route_ids: list[str] = Field(default_factory=list)
    application_clock: UtcDatetime | None = None
    clock_sequence: int | None = None


class RunsData(ApiModel):
    runs: list[RunSummary]
    returned_count: int
    next_page_token: str | None = None


class ComponentStatus(ApiModel):
    """One ``pipeline_status`` row; ``heartbeat_age_seconds`` is measured on the wall clock."""

    component: str
    status: str | None = None
    last_heartbeat_at: UtcDatetime | None = None
    heartbeat_age_seconds: float | None = None
    last_success_at: UtcDatetime | None = None
    last_batch_id: int | None = None
    last_batch_duration_seconds: float | None = None
    input_messages_total: int | None = None
    rejected_messages_total: int | None = None
    snapshots_built_total: int | None = None
    snapshots_published_total: int | None = None
    late_deliveries_total: int | None = None
    duplicate_deliveries_total: int | None = None
    last_snapshot_id: str | None = None
    last_published_snapshot_id: str | None = None
    last_visibility_latency_seconds: float | None = None
    last_rejected_at: UtcDatetime | None = None
    last_rejected_reason: str | None = None
    last_rejected_error_id: str | None = None
    last_error: str | None = None
    details: dict[str, Any] | None = None


class RunDetail(ApiModel):
    run_id: str
    project_id: str | None = None
    scope_id: str | None = None
    source_type: str | None = None
    clock_mode: str | None = None
    gtfs_version: str | None = None
    agency_timezone: str | None = None
    route_ids: list[str] = Field(default_factory=list)
    pipeline_version: str | None = None
    policy_version: str | None = None
    policy: dict[str, Any] | None = None
    policy_pinned: bool
    fixture_kind: str | None = None
    scenario_id: str | None = None
    seed: int | None = None
    recording_id: str | None = None
    session_id: str | None = None
    speed: float | None = None
    label: str | None = None
    provenance: dict[str, Any] | None = None
    created_at: UtcDatetime | None = None
    run_state: str | None = None
    state_reason: str | None = None
    application_clock: UtcDatetime | None = None
    clock_sequence: int | None = None
    clock_updated_at: UtcDatetime | None = None
    generations: dict[str, FeedMeta]
    source_health: list[ComponentStatus]


class PipelineStatusData(ApiModel):
    run_id: str
    run_state: str | None = None
    state_reason: str | None = None
    clock_mode: str | None = None
    application_clock: UtcDatetime | None = None
    clock_sequence: int | None = None
    clock_updated_at: UtcDatetime | None = None
    components: list[ComponentStatus]
    generations: dict[str, FeedMeta]
    kafka_backlog: list[dict[str, Any]] | None = None
    latency: dict[str, float] | None = None


# ================================================================================================
# Static catalogue
# ================================================================================================
class RouteItem(ApiModel):
    route_id: str
    short_name: str | None = None
    long_name: str | None = None
    route_type: int | None = None
    color: str | None = None
    text_color: str | None = None
    sort_order: int | None = None
    agency_id: str | None = None


class RoutesData(ApiModel):
    routes: list[RouteItem]
    missing_route_ids: list[str]


class ShapeItem(ApiModel):
    shape_id: str
    direction_ids: list[int]
    trip_count: int | None = None
    point_count: int | None = None
    simplified: bool | None = None
    is_representative: bool | None = None
    bbox: list[float] | None = Field(default=None, description="[min_lon, min_lat, max_lon, max_lat]")
    path: list[list[float]] | None = Field(default=None, description="[[lon, lat], ...]")


class GeometryData(ApiModel):
    route_id: str
    shapes: list[ShapeItem]
    shape_count: int
    truncated: bool


class RouteStopItem(ApiModel):
    stop_id: str
    stop_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    direction_ids: list[int]
    pattern_count: int | None = None


class RouteStopsData(ApiModel):
    route_id: str
    ordering: Literal["none_use_trip_stops"]
    stops: list[RouteStopItem]
    stop_count: int
    truncated: bool


class TripStopItem(ApiModel):
    stop_sequence: int
    stop_id: str | None = None
    arrival_time: str | None = Field(default=None, description="GTFS time string, may exceed 24:00:00")
    departure_time: str | None = None
    arrival_seconds: int | None = Field(default=None, description="seconds after the service-day anchor")
    departure_seconds: int | None = None
    stop_headsign: str | None = None
    pickup_type: int | None = None
    drop_off_type: int | None = None
    timepoint: int | None = None
    scheduled_arrival_utc: UtcDatetime | None = None
    scheduled_departure_utc: UtcDatetime | None = None


class TripStopsData(ApiModel):
    trip_id: str
    route_id: str | None = None
    direction_id: int | None = None
    service_id: str | None = None
    shape_id: str | None = None
    trip_headsign: str | None = None
    service_date: date | None = None
    agency_timezone: str | None = None
    stops: list[TripStopItem]
    stop_count: int


# ================================================================================================
# Vehicles
# ================================================================================================
class VehicleCounts(ApiModel):
    """Counts recomputed at the application clock. ``invalid_vehicle_count`` counts entities, including
    ``invalid_without_vehicle_id_count`` entities that have no row (e.g. MISSING_VEHICLE_ID, taken from the
    generation's stored route metrics); ``snapshot_membership_count`` counts distinct vehicle IDs (not a
    fleet count), so the classes need not sum to membership (D-BIZ-07)."""

    fresh_vehicle_count: int
    stale_vehicle_count: int
    unknown_age_vehicle_count: int
    invalid_vehicle_count: int
    invalid_without_vehicle_id_count: int
    snapshot_membership_count: int
    inherited_vehicle_count: int
    clock_skew_vehicle_count: int


class StoredVehicleCounts(ApiModel):
    """Counts written by Spark at ``metrics_as_of`` (audit only; they age with the clock)."""

    metrics_as_of: UtcDatetime | None = None
    fresh_vehicle_count: int | None = None
    stale_vehicle_count: int | None = None
    unknown_age_vehicle_count: int | None = None
    invalid_vehicle_count: int | None = None
    snapshot_membership_count: int | None = None
    inherited_vehicle_count: int | None = None
    stale_threshold_seconds: int | None = None


class VehicleItem(ApiModel):
    vehicle_id: str
    label: str | None = None
    route_id: str
    trip_id: str | None = None
    direction_id: int | None = None
    start_date: str | None = None
    start_time: str | None = None
    trip_schedule_relationship: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    bearing: float | None = None
    speed_mps: float | None = None
    observation_time: UtcDatetime | None = None
    event_time: UtcDatetime | None = None
    event_time_origin: str | None = None
    record_kind: str | None = None
    observation_age_seconds: float | None = None
    freshness_status: Literal["fresh", "stale", "unknown", "invalid"]
    invalid_reason: str | None = None
    current_status: str | None = None
    stop_id: str | None = None
    current_stop_sequence: int | None = None
    occupancy_status: str | None = None
    quality_flags: list[str]
    observation_id: str | None = None
    inherited: bool
    inherited_from_snapshot_id: str | None = None
    feed_timestamp: UtcDatetime | None = None


class VehiclesData(ApiModel):
    route_ids: list[str]
    snapshot_id: str | None = None
    evaluated_at: UtcDatetime
    counts: VehicleCounts
    counts_complete: bool
    vehicles: list[VehicleItem]
    returned_count: int
    truncated: bool
    reason: str | None = None


class HistoryRange(ApiModel):
    """Half-open interval ``[start, end)``."""

    start: UtcDatetime
    end: UtcDatetime


class HistoryObservation(ApiModel):
    event_time: UtcDatetime
    observation_id: str
    record_kind: str | None = None
    event_time_origin: str | None = None
    route_id: str | None = None
    trip_id: str | None = None
    direction_id: int | None = None
    start_date: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    bearing: float | None = None
    speed_mps: float | None = None
    stop_id: str | None = None
    current_stop_sequence: int | None = None
    current_status: str | None = None
    quality_flags: list[str]


class HistoryData(ApiModel):
    vehicle_id: str
    range: HistoryRange
    buckets: list[date]
    order: Literal["event_time_asc"]
    page_size: int
    returned_count: int
    observations: list[HistoryObservation]
    next_page_token: str | None = None
    reason: str | None = None


# ================================================================================================
# Predictions and KPI
# ================================================================================================
class PredictionItem(ApiModel):
    prediction_id: str
    stop_id: str | None = None
    trip_id: str | None = None
    route_id: str | None = None
    direction_id: int | None = None
    start_date: str | None = None
    start_time: str | None = None
    service_date: str | None = None
    stop_sequence: int | None = None
    event_type: str | None = None
    predicted_time: UtcDatetime
    scheduled_time: UtcDatetime | None = None
    predicted_delay_seconds: int | None = None
    uncertainty_seconds: int | None = Field(
        default=None, description="source uncertainty; MBTA bus codes 300/301 are classified in prediction_quality"
    )
    prediction_quality: str | None = None
    schedule_resolution: str | None = None
    resolution_method: str | None = None
    trip_schedule_relationship: str | None = None
    stop_schedule_relationship: str | None = None
    kpi_eligible: bool | None = None
    is_target: bool | None = None
    vehicle_id: str | None = None
    quality_flags: list[str]


class PredictionsData(ApiModel):
    stop_id: str
    stop_name: str | None = None
    route_ids: list[str]
    snapshot_id: str | None = None
    include: Literal["displayable", "all"]
    evaluated_at: UtcDatetime
    window_start: UtcDatetime
    predictions: list[PredictionItem]
    returned_count: int
    truncated: bool
    reason: str | None = None
    reason_detail: str | None = None


class VehicleMetrics(ApiModel):
    snapshot_id: str | None = None
    evaluated_at: UtcDatetime
    metrics_as_of: UtcDatetime | None = None
    stale: bool | None = None
    stale_threshold_seconds: int
    counts: VehicleCounts
    counts_complete: bool
    stored_counts_at_as_of: StoredVehicleCounts | None = None


class PredictionMetrics(ApiModel):
    """Route predicted-delay KPI (one target stop per trip instance). Rates and means are null when
    the denominator is 0 or unknown — never 0 %."""

    snapshot_id: str | None = None
    metrics_as_of: UtcDatetime | None = None
    stale: bool | None = None
    feed_age_seconds: float | None = None
    denominator_unit: Literal["trip_instance"] = "trip_instance"
    trip_instance_count: int | None = None
    eligible_trip_count: int | None = None
    predicted_late_trip_count: int | None = None
    predicted_late_rate: float | None = None
    mean_predicted_delay_seconds: float | None = None
    excluded_trip_count: int | None = None
    excluded_by_reason: dict[str, int] | None = None
    unknown_uncertainty_count: int | None = None
    late_threshold_seconds: int | None = None
    target_policy: str | None = None
    policy_version: str | None = None
    quality_flags: list[str] = Field(default_factory=list)


class RouteMetricsData(ApiModel):
    route_id: str
    evaluated_at: UtcDatetime
    vehicle_metrics: VehicleMetrics | None = None
    prediction_metrics: PredictionMetrics | None = None


# ================================================================================================
# Alerts and quality
# ================================================================================================
class ActivePeriod(ApiModel):
    start: UtcDatetime | None = None
    end: UtcDatetime | None = None


class AlertItem(ApiModel):
    alert_id: str
    category: Literal["MBTA_SERVICE_ALERT"] = "MBTA_SERVICE_ALERT"
    route_ids: list[str]
    cause: str | None = None
    effect: str | None = None
    severity_level: str | None = None
    header_text: str | None = None
    description_text: str | None = None
    url: str | None = None
    translations: Any | None = None
    active_periods: list[ActivePeriod]
    active_state_at_clock: str
    active_state_at_build: str | None = None
    informed_entities: Any | None = None
    match_kinds: list[str]
    selector_resolution: str | None = None
    confirmation_status: str | None = None
    inherited: bool
    inherited_from_snapshot_id: str | None = None
    first_seen_snapshot_id: str | None = None
    feed_timestamp: UtcDatetime | None = None
    quality_flags: list[str]


class AlertsData(ApiModel):
    route_ids: list[str]
    snapshot_id: str | None = None
    evaluated_at: UtcDatetime
    alerts: list[AlertItem]
    returned_count: int
    truncated: bool
    reason: str | None = None


class QualityReasonItem(ApiModel):
    reason_code: str
    severity: str | None = None
    disposition: str | None = None
    unit: str | None = None
    category: str | None = None
    count: int | None = None
    disposition_counts: dict[str, int] | None = None
    sample_refs: list[str]


class FeedQuality(ApiModel):
    feed_type: str
    snapshot_id: str | None = None
    is_partial: bool | None = None
    is_empty: bool | None = None
    source_entity_count: int | None = None
    selected_entity_count: int | None = None
    fully_valid_entity_count: int | None = None
    partially_valid_entity_count: int | None = None
    rejected_entity_count: int | None = None
    accounting: dict[str, Any] | None = None
    reasons: list[QualityReasonItem]
    truncated: bool


class QualityData(ApiModel):
    feeds: dict[str, FeedQuality | None]
    source_health: list[ComponentStatus]
