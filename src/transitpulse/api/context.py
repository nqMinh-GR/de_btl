"""Run context and feed views — the serving algorithm (design E §4.3, interfaces §8).

Per request::

    ctx = RunContext.load(env, run_id)          # 1 read of run_metadata (+1 pipeline_status partition)
      404 run_not_found when missing
      application_clock = wall now (clock_mode=wall) | run_metadata.application_clock (source)
      source clock null -> 503 run_clock_unavailable (only for endpoints that need the clock)
    view = ctx.feed(feed_type)                  # feed_current read ONCE per feed per request
      no pointer                      -> unavailable
      pointer.expires_at <= wall now  -> expired          (TTL is wall-clock retention)
      manifest missing / not READY    -> inconsistent
      manifest.is_empty               -> empty            (200, [] + reason)
      header age > feed threshold     -> stale            (200 + FEED_STALE warning)
      else                            -> available
    rows = ctx.read_partitions(view, table, keys, ...)   # bounded single-partition reads
      len(rows) < min(expected_counts[...], limit) -> inconsistent (503 generation_inconsistent);
      lost rows are never served as "zero vehicles".

Single-feed endpoints raise 503 when their feed is expired/unavailable/inconsistent; multi-feed endpoints
(metrics, quality) null the affected block, add a warning and fail only when every feed is unusable.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any

from transitpulse.api.cache import MISSING
from transitpulse.api.deps import RequestEnv
from transitpulse.api.errors import ApiError
from transitpulse.api.models import ApiWarning, Coverage, FeedMeta, FreshnessMeta, Meta, WarningCategory
from transitpulse.contracts.enums import FEED_ORDER, ClockMode, GenerationStatus, RunState, SourceType
from transitpulse.contracts.envelope import iso_utc
from transitpulse.contracts.policy import Policy, feed_is_stale
from transitpulse.processing.context import TABLE_TTL_CLASS

FEED_TYPES: tuple[str, ...] = tuple(sorted(FEED_ORDER, key=FEED_ORDER.__getitem__))
UNUSABLE_STATES = frozenset({"expired", "unavailable", "inconsistent"})
STATE_ERROR_CODES = {
    "expired": "generation_expired",
    "unavailable": "generation_unavailable",
    "inconsistent": "generation_inconsistent",
}
STATE_WARNING_CODES = {
    "expired": "GENERATION_EXPIRED",
    "unavailable": "GENERATION_UNAVAILABLE",
    "inconsistent": "GENERATION_INCONSISTENT",
}
_STATE_HINTS = {
    "expired": "the generation outlived its storage TTL; a newer snapshot replaces it once the processor "
    "publishes one (check GET /pipeline/status)",
    "unavailable": "no generation has been published yet; start or resume the run and wait for the processor "
    "(check GET /pipeline/status)",
    "inconsistent": "rows of the current generation are missing or its manifest is not READY; retry, then "
    "check the processor and Cassandra",
}
PIPELINE_STATUS_LIMIT = 100
ROUTE_METRICS_LIMIT = 256
# Serving tables are written once per generation before READY: any row count different from the manifest
# is data loss or leftovers. History/quality tables may legitimately gain audit rows after READY
# (LATE / EXPIRED / DUPLICATE quality rows), so only missing rows are an inconsistency there.
EXACT_COUNT_TABLES = frozenset(t for t, ttl_class in TABLE_TTL_CLASS.items() if ttl_class == "serving")
SOURCE_PROBLEM_STATES = frozenset({"degraded", "backoff", "failed"})
PIPELINE: WarningCategory = "PIPELINE_DATA_QUALITY_ALERT"
INFO: WarningCategory = "INFO"


def stale_threshold_seconds(feed_type: str, policy: Policy) -> int:
    """Feed-level stale threshold of the run-pinned policy (VP 120 s, TU 90 s, alerts 300 s)."""
    return {
        "vehicle_positions": policy.vehicle_stale_seconds,
        "trip_updates": policy.prediction_stale_seconds,
        "alerts": policy.alert_stale_seconds,
    }[feed_type]


def freeze(value: Any) -> Any:
    """Read-only view of a row, a row list, or a mapping of rows (values shared by the cache)."""
    if isinstance(value, Mapping):
        return MappingProxyType({k: freeze(v) if isinstance(v, Mapping) else v for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(MappingProxyType(dict(v)) if isinstance(v, Mapping) else v for v in value)
    return value


# ================================================================================================
# Feed view
# ================================================================================================
@dataclass(frozen=True)
class FeedView:
    """What one request knows about one feed's current generation."""

    feed_type: str
    state: str
    stale_threshold_seconds: int
    pointer: Mapping[str, Any] | None = None
    manifest: Mapping[str, Any] | None = None
    source_age_seconds: float | None = None
    stale: bool | None = None
    detail: str | None = None

    @property
    def usable(self) -> bool:
        return self.state not in UNUSABLE_STATES

    @property
    def snapshot_id(self) -> str | None:
        return self.pointer.get("snapshot_id") if self.pointer else None

    def _field(self, name: str) -> Any:
        if self.pointer is not None and self.pointer.get(name) is not None:
            return self.pointer.get(name)
        if self.manifest is not None:
            return self.manifest.get(name)
        return None

    @property
    def feed_timestamp(self) -> datetime | None:
        return self._field("feed_timestamp")

    @property
    def metrics_as_of(self) -> datetime | None:
        return self._field("metrics_as_of")

    @property
    def expires_at(self) -> datetime | None:
        return self._field("expires_at")

    @property
    def is_partial(self) -> bool | None:
        return self._field("is_partial")

    @property
    def is_empty(self) -> bool | None:
        return self._field("is_empty")

    def expected_rows(self, table: str, key: str | None = None) -> int | None:
        """Rows the READY manifest promises for ``table`` (``key=None``) or ``"<table>/<key>"``.

        ``None`` means "not recorded" (no check possible). A per-partition key absent from a manifest that
        records the table total means the partition is empty (the processor records non-empty ones).
        """
        counts = self.manifest.get("expected_counts") if self.manifest is not None else None
        if not counts:
            return None
        if key is None:
            total = counts.get(table)
            return int(total) if total is not None else None
        value = counts.get(f"{table}/{key}")
        if value is not None:
            return int(value)
        return 0 if table in counts else None

    def with_state(self, state: str, detail: str | None) -> FeedView:
        return dataclasses.replace(self, state=state, detail=detail)

    def to_meta(self) -> FeedMeta:
        return FeedMeta(
            state=self.state,  # type: ignore[arg-type]
            snapshot_id=self.snapshot_id,
            generation_id=self._field("generation_id"),
            feed_timestamp=self.feed_timestamp,
            metrics_as_of=self.metrics_as_of,
            view_published_at=self._field("view_published_at") or self._field("ready_at"),
            expires_at=self.expires_at,
            is_partial=self.is_partial,
            is_empty=self.is_empty,
            source_age_seconds=self.source_age_seconds,
            stale=self.stale,
            stale_threshold_seconds=self.stale_threshold_seconds,
            detail=self.detail,
        )


@dataclass(frozen=True)
class PartitionRead:
    """Rows of one generation partition after the consistency check."""

    key: str | None
    rows: tuple[Mapping[str, Any], ...]
    expected: int | None
    truncated: bool


# ================================================================================================
# Run context
# ================================================================================================
def _run_policy(run: Mapping[str, Any], env: RequestEnv) -> tuple[Policy, bool]:
    raw = run.get("policy_json")
    if raw:
        try:
            return Policy.from_json(raw), True
        except (TypeError, ValueError, KeyError):
            pass
    return Policy.from_config(env.cfg), False


def _clock_mode(run: Mapping[str, Any]) -> str:
    mode = run.get("clock_mode")
    if mode in (ClockMode.WALL.value, ClockMode.SOURCE.value):
        return str(mode)
    return ClockMode.WALL.value if run.get("source_type") == SourceType.LIVE.value else ClockMode.SOURCE.value


class RunContext:
    """Run metadata, clock, policy, feed views and warnings of one request (never cached)."""

    def __init__(
        self, env: RequestEnv, run: Mapping[str, Any], pipeline_rows: Sequence[Mapping[str, Any]] | None
    ) -> None:
        self.env = env
        self.run = run
        self.run_id: str = str(run["run_id"])
        self.pipeline_rows: list[Mapping[str, Any]] | None = list(pipeline_rows) if pipeline_rows is not None else None
        self.policy, self.policy_pinned = _run_policy(run, env)
        self.clock_mode = _clock_mode(run)
        self.served_at = env.served_at
        self.application_clock: datetime | None = (
            self.served_at if self.clock_mode == ClockMode.WALL.value else run.get("application_clock")
        )
        self.scope_route_ids: tuple[str, ...] = tuple(run.get("route_ids") or ())
        self._feeds: dict[str, FeedView] = {}
        self._warnings: list[ApiWarning] = []
        self._warning_keys: set[tuple[str, str | None, str | None]] = set()
        self._run_warnings()

    # ------------------------------------------------------------------ construction
    @classmethod
    def load(cls, env: RequestEnv, run_id: str, *, source_health: bool = True) -> RunContext:
        run = env.repo.run_metadata(run_id, deadline=env.deadline)
        if run is None:
            raise ApiError(
                404,
                "run_not_found",
                f"run {run_id!r} does not exist",
                details=[{"run_id": run_id}],
                hint="list available runs with GET /runs",
                meta=Meta(request_id=env.request_id, served_at=env.served_at, run_id=run_id),
            )
        pipeline_rows = (
            env.repo.pipeline_status(run_id, limit=PIPELINE_STATUS_LIMIT, deadline=env.deadline)
            if source_health
            else None
        )
        return cls(env, run, pipeline_rows)

    def _run_warnings(self) -> None:
        if not self.policy_pinned:
            self.warn(
                "POLICY_NOT_PINNED",
                "run_metadata.policy_json is missing or unreadable; the current configuration policy is used",
                PIPELINE,
            )
        state = self.run.get("run_state")
        if state == RunState.PAUSED.value:
            clock = self.application_clock
            at = f" at {iso_utc(clock)}" if clock is not None else ""
            self.warn("RUN_PAUSED", f"the run is paused; the application clock is frozen{at}", INFO)
        elif state == RunState.FINISHED.value:
            self.warn("RUN_FINISHED", "the run has finished; its data no longer advances", INFO)
        elif state == RunState.FAILED.value:
            reason = self.run.get("state_reason")
            self.warn("RUN_FAILED", f"the run failed{': ' + str(reason) if reason else ''}", PIPELINE)

    # ------------------------------------------------------------------ properties
    @property
    def gtfs_version(self) -> str | None:
        return self.run.get("gtfs_version")

    @property
    def agency_timezone(self) -> str | None:
        return self.run.get("agency_timezone")

    @property
    def warnings(self) -> list[ApiWarning]:
        return list(self._warnings)

    # ------------------------------------------------------------------ errors and warnings
    def warn(
        self,
        code: str,
        message: str,
        category: WarningCategory,
        *,
        feed_type: str | None = None,
        reason_code: str | None = None,
        count: int | None = None,
    ) -> None:
        key = (code, feed_type, reason_code)
        if key in self._warning_keys:
            return
        self._warning_keys.add(key)
        self._warnings.append(
            ApiWarning(
                code=code, message=message, category=category, feed_type=feed_type, reason_code=reason_code, count=count
            )
        )

    def error(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        details: list[dict[str, Any]] | None = None,
        hint: str | None = None,
    ) -> ApiError:
        return ApiError(
            status_code, code, message, retryable=retryable, details=details, hint=hint, meta=self.meta()
        )

    # ------------------------------------------------------------------ clock and scope
    def require_clock(self) -> datetime:
        if self.application_clock is None:
            raise self.error(
                503,
                "run_clock_unavailable",
                f"run {self.run_id} has no application clock yet",
                retryable=True,
                hint="the replay/synthetic clock starts with the publisher's first clock tick; wait for it to "
                "be processed (GET /pipeline/status)",
            )
        return self.application_clock

    def require_route_in_scope(self, route_id: str) -> None:
        if route_id not in self.scope_route_ids:
            raise self.error(
                404,
                "route_not_in_scope",
                f"route {route_id!r} is not in the scope of run {self.run_id}",
                details=[{"route_id": route_id, "scope_route_ids": list(self.scope_route_ids)}],
                hint="use one of meta.coverage.route_ids / GET /routes?run_id=...",
            )

    def fanout_routes(self, route_id: str | None) -> list[str]:
        """Routes of a per-route query: the given one (in scope) or the whole (small) scope."""
        if route_id is not None:
            self.require_route_in_scope(route_id)
            return [route_id]
        limit = self.env.api.max_fanout_routes
        if len(self.scope_route_ids) > limit:
            raise self.error(
                422,
                "route_id_required",
                f"the run scope has {len(self.scope_route_ids)} routes; at most {limit} are combined without route_id",
                details=[{"scope_route_count": len(self.scope_route_ids), "max_fanout_routes": limit}],
                hint="pass route_id=<one of the scope routes>",
            )
        return list(self.scope_route_ids)

    def require_gtfs_version(self) -> str:
        version = self.gtfs_version
        if not version:
            raise self.error(
                503,
                "run_metadata_incomplete",
                f"run {self.run_id} has no gtfs_version; its static catalogue cannot be served",
                retryable=False,
                hint="recreate the run with a loaded static version",
            )
        return str(version)

    # ------------------------------------------------------------------ feeds
    def feed(self, feed_type: str) -> FeedView:
        """Feed view; the pointer is read at most once per feed per request."""
        view = self._feeds.get(feed_type)
        if view is not None:
            return view
        pointer = self.env.repo.feed_pointer(self.run_id, feed_type, deadline=self.env.deadline)
        view = self._build_view(feed_type, pointer)
        self._feeds[feed_type] = view
        self.env.services.metrics.observe_feed_state(self.env.endpoint, feed_type, view.state)
        self._feed_warnings(view)
        return view

    def require_feed(self, feed_type: str) -> FeedView:
        view = self.feed(feed_type)
        if not view.usable:
            raise self.feed_error(view)
        return view

    def feed_error(self, view: FeedView) -> ApiError:
        return self.error(
            503,
            STATE_ERROR_CODES[view.state],
            f"{view.feed_type} generation is {view.state}" + (f": {view.detail}" if view.detail else ""),
            retryable=True,
            details=[{"feed_type": view.feed_type, "state": view.state, "snapshot_id": view.snapshot_id}],
            hint=_STATE_HINTS[view.state],
        )

    def warn_unusable(self, view: FeedView) -> None:
        """Warning for a non-primary feed that a multi-feed endpoint could not use."""
        if view.usable:
            return
        self.warn(
            STATE_WARNING_CODES[view.state],
            f"{view.feed_type} block omitted: generation is {view.state}" + (f" ({view.detail})" if view.detail else ""),
            PIPELINE,
            feed_type=view.feed_type,
        )

    def mark_inconsistent(self, feed_type: str, detail: str) -> FeedView:
        view = self.feed(feed_type).with_state("inconsistent", detail)
        self._feeds[feed_type] = view
        self.env.services.metrics.observe_feed_state(self.env.endpoint, feed_type, view.state)
        return view

    def _age(self, feed_type: str, feed_timestamp: datetime | None) -> tuple[float | None, bool | None]:
        clock = self.application_clock
        if clock is None:
            return None, None
        stale = feed_is_stale(feed_type, feed_timestamp, clock, self.policy)
        age = round((clock - feed_timestamp).total_seconds(), 3) if feed_timestamp is not None else None
        return age, stale

    def _build_view(self, feed_type: str, pointer: Mapping[str, Any] | None) -> FeedView:
        threshold = stale_threshold_seconds(feed_type, self.policy)
        if pointer is None or not pointer.get("snapshot_id"):
            return FeedView(feed_type, "unavailable", threshold, detail="no generation has been published yet")
        snapshot_id = str(pointer["snapshot_id"])
        age, stale = self._age(feed_type, pointer.get("feed_timestamp"))
        view = FeedView(feed_type, "available", threshold, pointer=pointer, source_age_seconds=age, stale=stale)
        expires_at = pointer.get("expires_at")
        if expires_at is None:
            return view.with_state("inconsistent", "the feed pointer has no expires_at")
        if expires_at <= self.served_at:
            return view.with_state("expired", f"generation {snapshot_id} expired at {iso_utc(expires_at)}")
        manifest = self._manifest(feed_type, snapshot_id, expires_at)
        view = dataclasses.replace(view, manifest=manifest)
        if manifest is None:
            return view.with_state("inconsistent", f"no manifest for snapshot {snapshot_id}")
        if manifest.get("status") != GenerationStatus.READY.value:
            return view.with_state("inconsistent", f"manifest of snapshot {snapshot_id} is {manifest.get('status')}")
        if view.is_empty:
            return view.with_state("empty", "the current snapshot is a valid empty snapshot")
        if stale:
            return view.with_state("stale", None)
        return view

    def _ttl_until(self, expires_at: datetime) -> float:
        return (expires_at - self.served_at).total_seconds()

    def _manifest(self, feed_type: str, snapshot_id: str, expires_at: datetime) -> Mapping[str, Any] | None:
        cache = self.env.services.cache
        key = ("manifest", self.run_id, feed_type, snapshot_id)
        hit = cache.get(key)
        if hit is not MISSING:
            return hit
        row = self.env.repo.manifest(self.run_id, feed_type, snapshot_id, deadline=self.env.deadline)
        if row is None:
            return None
        frozen = freeze(row)
        if row.get("status") == GenerationStatus.READY.value:  # READY manifests are immutable
            cache.put(key, frozen, self._ttl_until(expires_at))
        return frozen

    def _feed_warnings(self, view: FeedView) -> None:
        ft = view.feed_type
        if view.usable:
            if view.stale:
                age = f"{view.source_age_seconds:g} s" if view.source_age_seconds is not None else "unknown age"
                self.warn(
                    "FEED_STALE",
                    f"{ft} header is {age} old at the application clock (threshold "
                    f"{view.stale_threshold_seconds} s); values may not reflect the current situation",
                    PIPELINE,
                    feed_type=ft,
                )
            if view.is_partial:
                self.warn(
                    "PARTIAL_SNAPSHOT",
                    f"the current {ft} snapshot is partial: some entities were rejected (see GET /quality)",
                    PIPELINE,
                    feed_type=ft,
                )
            if view.state == "empty":
                self.warn("EMPTY_SNAPSHOT", f"the current {ft} snapshot contains no entities in scope", INFO, feed_type=ft)
        for row in self.pipeline_rows or ():
            component = str(row.get("component") or "")
            status = str(row.get("status") or "").lower()
            if component == f"collector:{ft}" and status in SOURCE_PROBLEM_STATES:
                last_error = row.get("last_error")
                self.warn(
                    "SOURCE_FETCH_FAILED",
                    f"the {ft} source is {status}" + (f": {last_error}" if last_error else ""),
                    PIPELINE,
                    feed_type=ft,
                    reason_code=row.get("last_rejected_reason"),
                )
            if component == f"feed:{ft}":
                rejected_at = row.get("last_rejected_at")
                published = view.pointer.get("view_published_at") if view.pointer is not None else None
                if rejected_at is not None and (published is None or rejected_at > published):
                    reason = row.get("last_rejected_reason")
                    self.warn(
                        "SOURCE_MESSAGE_REJECTED",
                        f"the latest {ft} message was rejected ({reason or 'unknown reason'}) at "
                        f"{iso_utc(rejected_at)}; the previous generation is still served",
                        PIPELINE,
                        feed_type=ft,
                        reason_code=reason,
                    )

    # ------------------------------------------------------------------ generation rows
    def read_partitions(
        self,
        view: FeedView,
        table: str,
        keys: Sequence[str | None],
        *,
        limit: int,
        fetch: Callable[[list[str | None], int], Mapping[str | None, Sequence[Mapping[str, Any]]]],
        required: bool = True,
    ) -> dict[str | None, PartitionRead] | None:
        """Bounded partition reads of ``view``'s generation with the expected-count check.

        ``fetch(keys, fetch_limit)`` must read each partition with ``LIMIT fetch_limit``. ``key=None`` reads a
        whole-generation partition checked against the ``"<table>"`` total.
        Fewer rows than expected is always inconsistent (lost rows are never "zero vehicles"); for serving
        tables (:data:`EXACT_COUNT_TABLES`) more rows than expected (when ``expected <= limit``) is
        inconsistent too, because leftovers of an aborted build must never be served. Those tables are read
        with ``limit + 1`` so a leftover row is detected even when ``expected == limit``; the extra row is
        used only for the check and never returned (at most ``limit`` rows per partition).
        On inconsistency the feed is marked ``inconsistent``; with ``required`` a 503 is raised,
        otherwise ``None`` is returned so a multi-feed endpoint can omit the block.
        """
        cache = self.env.services.cache
        snapshot_id = view.snapshot_id
        out: dict[str | None, PartitionRead] = {}
        missing: list[str | None] = []
        for key in keys:
            hit = cache.get(("rows", table, self.run_id, view.feed_type, snapshot_id, key, limit))
            if hit is MISSING:
                missing.append(key)
            else:
                out[key] = hit
        if missing:
            exact = table in EXACT_COUNT_TABLES
            fetch_limit = limit + 1 if exact else limit
            fetched = fetch(missing, fetch_limit)
            expires_at = view.expires_at
            for key in missing:
                rows = list(fetched.get(key) or ())
                expected = view.expected_rows(table, key)
                missing_rows = expected is not None and len(rows) < min(expected, limit)
                extra_rows = exact and expected is not None and expected <= limit and len(rows) > expected
                if missing_rows or extra_rows:
                    where = table if key is None else f"{table}/{key}"
                    detail = f"{where}: {len(rows)} rows readable, {expected} expected"
                    marked = self.mark_inconsistent(view.feed_type, detail)
                    if required:
                        raise self.feed_error(marked)
                    self.warn_unusable(marked)
                    return None
                # without a recorded count: exact tables know (an extra row came back), others assume the worst
                truncated = (expected > limit) if expected is not None else len(rows) >= fetch_limit
                read = PartitionRead(key, freeze(rows[:limit]), expected, truncated)
                if expires_at is not None:
                    cache.put(
                        ("rows", table, self.run_id, view.feed_type, snapshot_id, key, limit),
                        read,
                        self._ttl_until(expires_at),
                    )
                out[key] = read
        return out

    # ------------------------------------------------------------------ static rows
    def static_rows(self, table: str, key: tuple[Any, ...], loader: Callable[[], Any]) -> Any:
        """Static catalogue rows (immutable per ``gtfs_version``); empty results are never cached."""
        cache = self.env.services.cache
        cache_key = ("static", table, self.gtfs_version, *key)
        hit = cache.get(cache_key)
        if hit is not MISSING:
            return hit
        frozen = freeze(loader())
        if frozen:
            cache.put(cache_key, frozen, self.env.api.cache_max_ttl_seconds)
        return frozen

    # ------------------------------------------------------------------ metadata
    def coverage(
        self,
        route_ids: Sequence[str],
        *,
        denominator_unit: str | None = None,
        returned_rows: int | None = None,
        max_rows: int | None = None,
        truncated: bool = False,
    ) -> Coverage:
        return Coverage(
            scope_route_count=len(self.scope_route_ids),
            route_ids=list(route_ids),
            denominator_unit=denominator_unit,
            returned_rows=returned_rows,
            max_rows=max_rows,
            truncated=truncated,
        )

    def freshness_meta(self) -> FreshnessMeta:
        p = self.policy
        return FreshnessMeta(
            clock_mode=self.clock_mode,
            evaluated_at=self.application_clock,
            policy_version=p.policy_version,
            vehicle_stale_threshold_seconds=p.vehicle_stale_seconds,
            prediction_stale_threshold_seconds=p.prediction_stale_seconds,
            alert_stale_threshold_seconds=p.alert_stale_seconds,
            future_tolerance_seconds=p.future_tolerance_seconds,
        )

    def meta(self, coverage: Coverage | None = None) -> Meta:
        run = self.run
        return Meta(
            request_id=self.env.request_id,
            served_at=self.served_at,
            run_id=self.run_id,
            source_type=run.get("source_type"),
            scenario_id=run.get("scenario_id"),
            run_state=run.get("run_state"),
            gtfs_version=run.get("gtfs_version"),
            scope_id=run.get("scope_id"),
            clock_mode=self.clock_mode,
            application_clock=self.application_clock,
            clock_sequence=run.get("clock_sequence"),
            policy_version=self.policy.policy_version,
            feeds={ft: self._feeds[ft].to_meta() for ft in FEED_TYPES if ft in self._feeds},
            coverage=coverage,
            freshness=self.freshness_meta(),
        )
