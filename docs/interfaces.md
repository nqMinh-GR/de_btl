# Frozen interfaces (contract between work packages)

This file is the single source of truth for cross-module names, signatures, file formats and CLI
contracts. Code in one package may rely ONLY on what is written here (plus the already-written
foundation modules). Changing anything here requires updating every consumer and `docs/decisions.md`.

Foundation modules already implemented (read them, do not rewrite them):

| Module | Content |
|---|---|
| `src/transitpulse/config.py` | `load_config(profile=None, *, workdir=None, overrides=None, environ=None) -> ProjectConfig`, `repo_root()`, `PROFILES` (`local-native`, `colab-native`, `local-docker`) |
| `config/{base,local,colab,docker}.yaml` | every configurable value (sections: project, sources, scope, collector, kafka, spark, cassandra, policy, storage, api, ui, replay, synthetic, paths, runtime, observability) |
| `src/transitpulse/contracts/enums.py` | `FeedType`, `SourceType`, `ClockMode`, `RunState`, `GenerationStatus`, `PublishOutcome`, `ScheduleResolution`, `ResolutionMethod`, `UncertaintyClass`, `EventType`, `FreshnessClass`, `RecordKind`, `EventTimeOrigin`, `EntityStatus`, `AlertActiveState`, `AlertConfirmation`, `Severity`, `Disposition`, `AlertCategory`, `QUALITY_REASONS` (code -> severity/disposition/unit/category), `KPI_EXCLUSION_ORDER`, `FEED_ABBR`, `FEED_ORDER`, `reason(code)` |
| `src/transitpulse/contracts/ids.py` | `canonical_json`, `sha256_hex`, `H`, `K`, `e7/e2/e3`, `new_run_id`, `gtfs_version_of_bytes`, `scope_id`, `capture_id`, `payload_sha`, `snapshot_id`, `generation_id`, `source_order`, `observation_id`, `state_sample_id`, `prediction_id`, `error_id_message`, `error_id_entity`, `entity_ref`, `event_id` |
| `src/transitpulse/contracts/envelope.py` | `Envelope` TypedDict, `build_envelope`, `serialize_envelope`, `envelope_size`, `kafka_key`, `control_key`, `kafka_headers`, `iso_utc`, `parse_iso_utc`, `utcnow` |
| `src/transitpulse/contracts/control.py` | event type constants, `clock_event(...)`, `status_event(...)`, `CLOCK_EVENTS` |
| `src/transitpulse/contracts/dlq.py` | `DLQ_FIELDS`, `dlq_record(...)`, `bounded_excerpt` |
| `src/transitpulse/contracts/topics.py` | topic names, `TOPIC_BY_FEED`, `FEED_BY_TOPIC`, `SNAPSHOT_TOPICS`, `ALL_TOPICS`, `desired_topic_configs(cfg)` |
| `src/transitpulse/contracts/accounting.py` | `count_candidates(feed_type, entities)`, `ACCOUNTING_IDENTITIES` |
| `src/transitpulse/contracts/policy.py` | `Policy` (`from_config`, `to_json`, `from_json`), `classify_uncertainty`, `vehicle_freshness -> FreshnessResult`, `feed_is_stale`, `alert_active_state` |
| `src/transitpulse/storage/migrations/0001_initial.cql` | keyspace + all tables (column names are THE contract for every writer/reader) |
| `src/transitpulse/processing/schemas.py` | `ENVELOPE_SCHEMA`, `VP_ENTITY_SCHEMA`, `TU_ENTITY_SCHEMA`, `ALERT_ENTITY_SCHEMA`, `CONTROL_SCHEMA`, `ENTITY_SCHEMA_BY_FEED`, `JSON_OPTIONS`, `CORRUPT_COL` |
| `src/transitpulse/processing/context.py` | `RunInfo`, `BuildParams`, `GenerationOutput`, `PARSED_SNAPSHOT_COLUMNS`, `STATS_COLUMNS`, `PER_PARTITION_COUNT_KEYS`, `TABLE_TTL_CLASS` |
| `src/transitpulse/static/dims.py` | `Dims`, `DIM_NAMES` (with column lists), `load_dims(spark, scope_dir)`, `scope_dir_is_complete` |
| `src/transitpulse/runtime/paths.py` | `WorkPaths` (every runtime path), `safe_component`, `resolve_under` |
| `src/transitpulse/obs/logging.py` | `configure(component, run_id=, session_id=)`, `get_logger`, `log_event(logger, stage, status, msg, **fields)`, `timed`, `excerpt` |
| `src/transitpulse/obs/metrics.py` | `ComponentMetrics(component)` with `.counter/.gauge/.histogram/.start_exporter(dir, interval)/.export_once/.stop`, `read_textfiles(dir)` |
| `tests/conftest.py` | directory -> marker mapping; fixtures `repo_dir`, `golden_dir` |

Global rules for every package:
* Python 3.11, `from __future__ import annotations`, type hints, no `print` for logs (use `obs.logging`).
* All instants are timezone-aware UTC `datetime`; ISO strings via `iso_utc` (ms + `Z`). Epoch seconds only where a
  field name says so (`feed_timestamp`, GTFS-RT `time`). Never mix seconds and milliseconds.
* IDs stay strings. Missing optional fields stay `None`/null — never zero or empty string.
* Every CLI prints exactly ONE JSON document on stdout (`{"ok": bool, "action": str, ..., "error": {code, message,
  hint, log_excerpt} | null}`); progress goes to stderr. Exit codes: 0 ok, 2 usage, 3 preflight, 4 conflict,
  5 readiness/exit, 6 unsupported mode, 7 artifact/checksum, 1 other failure.
* Paths come from `WorkPaths`; never hard-code `/content`. Nothing deletes data in normal start/stop.
* No `ALLOW FILTERING`, no Cassandra counters, no `failOnDataLoss=false`, no `latest` tags, no `pkill/killall`.

---------------------------------------------------------------------------------------------------

## 1. Package ownership (who writes which files)

| WP | Owner of |
|---|---|
| WP0b Fixture+oracle | `tests/fixtures/golden/**` (IT06–IT09 runs live in the golden fixture), `tests/oracle/**`, `tests/unit/test_oracle_*.py`, `tests/unit/test_golden_fixture_*.py` |
| WP1 Static | `src/transitpulse/static/**` (except `dims.py`), `src/transitpulse/storage/static_writer.py`, `tests/unit/test_gtfs_time.py`, `test_calendar.py`, `test_schedule_resolution.py`, `test_static_validation.py`, `tests/spark/test_static_loader.py` |
| WP2 Ingestion | `src/transitpulse/ingestion/**`, `tests/unit/test_ingestion_*.py`, `tests/unit/test_envelope.py`, `tests/unit/test_ids.py` |
| WP3 Replay+Synthetic+Runs | `src/transitpulse/replay/**`, `src/transitpulse/runs.py`, `tests/unit/test_replay_*.py`, `tests/unit/test_synthetic_*.py`, `tests/unit/test_runs.py` |
| WP4 Spark core + storage | `src/transitpulse/processing/{spark_session,parse,control_clock,batch,sink,curated,dlq,faults,stream_job,report_job}.py`, `src/transitpulse/storage/{cassandra,migrate,pointer,manifest}.py`, `tests/unit/test_storage_*.py`, `tests/unit/test_faults.py`, `tests/spark/test_parse.py`, `tests/spark/test_batch_*.py` |
| WP5 Spark domain | `src/transitpulse/processing/{spark_ids,vehicles,predictions,schedule,kpi,alerts,quality}.py`, `tests/spark/test_vehicles.py`, `test_predictions.py`, `test_kpi.py`, `test_alerts.py`, `test_spark_ids_parity.py`, `tests/unit/test_policy.py` |
| WP6 API | `src/transitpulse/api/**`, `tests/unit/test_api_*.py` |
| WP7 Native runtime + scripts | `src/transitpulse/runtime/**` (except `paths.py`), `scripts/**`, `deploy/**`, `tests/runtime/**` |
| WP8 Frontends | `app/**`, `notebooks/**` |
| WP9 Packaging + integration | `docker-compose.yml`, `docker/**`, `Makefile`, `.github/**`, `tests/integration/**`, `tests/plugins/**`, `benchmarks/**`, `src/transitpulse/obs/lineage.py`, `src/transitpulse/verify/**`, `src/transitpulse/cli.py` |

`__init__.py` files already exist. Docs (`README.md`, `docs/*` other than this file) are written last by the lead.

---------------------------------------------------------------------------------------------------

## 2. Run record — `src/transitpulse/runs.py` (WP3)

```python
@dataclass(frozen=True)
class RunRecord:
    run_id: str; project_id: str; scope_id: str; source_type: str; clock_mode: str
    gtfs_version: str; agency_timezone: str; route_ids: tuple[str, ...]; pipeline_version: str
    policy: Policy                       # pinned at creation
    created_at: datetime; initial_clock: datetime | None
    static_zip_path: str                 # absolute path of the immutable ZIP copy
    static_scope_dir: str                # absolute path of <STATIC_ROOT>/<gtfs_version>/<scope_id>
    scenario_id: str | None = None; fixture_kind: str | None = None; seed: int | None = None
    recording_id: str | None = None; session_id: str | None = None; speed: float | None = None
    label: str | None = None; provenance: dict = field(default_factory=dict)
    def to_json(self) -> dict: ...
    @classmethod
    def from_json(cls, d: dict) -> "RunRecord": ...

def create_run(cfg: ProjectConfig, session, *, source_type: str, route_ids: Sequence[str], gtfs_version: str,
               scope_id: str, agency_timezone: str, static_zip_path: str, static_scope_dir: str,
               initial_clock: datetime | None = None, scenario_id=None, fixture_kind=None, seed=None,
               recording_id=None, session_id=None, speed=None, label=None, provenance=None,
               run_id: str | None = None) -> RunRecord
def load_run(paths: WorkPaths, run_id: str) -> RunRecord          # reads W/runs/<run_id>/run.json
def list_local_runs(paths: WorkPaths) -> list[RunRecord]
```
Rules: `clock_mode = "wall"` iff `source_type == "live"`, else `"source"` (and `initial_clock` is required).
Live requires non-empty `route_ids`. Writes, in order: `run_metadata` via
`INSERT ... IF NOT EXISTS` with `run_state='CREATED'`, `clock_sequence=0`, `application_clock=initial_clock`
(null for wall), `policy_json=Policy.to_json()`; then `runs_by_project`; then `run.json` atomically
(tmp + `os.replace`). The checkpoint, raw, outbox and curated namespaces are all keyed by `run_id`.

## 3. Static — `src/transitpulse/static/` (WP1)

CLI `python -m transitpulse.static`:
* `load --workdir W [--profile P] (--zip PATH | --download) --route-ids A,B,...` -> JSON
  `{ok, action:"static-load", gtfs_version, scope_id, status: PASSED|PASSED_WITH_WARNINGS|FAILED, agency_timezone,
  verified_route_ids, rejected_route_ids, static_zip_path, scope_dir, report_path}`. Exit 1 when FAILED.
* `list-routes --workdir W (--zip PATH | --download) [--check-realtime]` -> JSON `{ok, gtfs_version, bus_routes:
  [{route_id, short_name, long_name, trip_count, realtime_vehicle_count|null}], checked_at}`.
* `inspect --zip PATH` -> files present/empty/row counts, feed_version, date range (used by source-check).

Python API:
```python
def ingest_zip(paths: WorkPaths, zip_path: Path | None, *, download_url: str | None, user_agent: str) -> StaticVersion
    # copies to data/static/<gtfs_version>/MBTA_GTFS.zip (immutable) + version.json; gtfs_version = sha256(zip bytes)
def load_static(cfg, paths, gtfs_version: str, route_ids: Sequence[str], *, spark=None) -> StaticLoadResult
    # Spark batch: validate, scope-filter, write dim_* Parquet to <scope_dir> via temp dir + rename, then
    # scope_index.json, static_quality_report.json, manifest.json, and _SUCCESS LAST; then Cassandra static tables.
@dataclass(frozen=True) class StaticVersion: gtfs_version, zip_path, zip_sha256, feed_version, source_url, downloaded_at
@dataclass(frozen=True) class StaticLoadResult: gtfs_version, scope_id, status, agency_timezone,
    verified_route_ids: tuple, rejected: tuple[dict, ...], scope_dir: Path, report_path: Path
```
`manifest.json` keys: `gtfs_version, scope_id, agency_timezone, verified_route_ids, route_type, scope_policy_version,
status, row_counts, created_at, parser_version`.

`scope_index.json` (read by the collector/replay WITHOUT Spark):
```json
{"schema_version": 1, "gtfs_version": "...", "scope_id": "...", "route_ids": ["..."], "route_type_by_route": {"R": 3},
 "agency_by_route": {"R": "1"}, "agency_ids": ["1"], "agency_timezone": "America/New_York",
 "trip_to_route": {"<every trip_id of the GTFS version>": "<route_id>"},
 "scope_stop_ids": ["...incl. parent stations"], "stops_by_route": {"R": ["..."]}, "parent_station": {"stop": "parent"}}
```
```python
# static/index.py
@dataclass(frozen=True) class ScopeIndex: gtfs_version, scope_id, route_ids: frozenset, route_type_by_route: dict,
    agency_by_route: dict, agency_ids: frozenset, agency_timezone: str, trip_to_route: dict,
    scope_stop_ids: frozenset, stops_by_route: dict, parent_station: dict
def load_scope_index(scope_dir: Path) -> ScopeIndex
```
Pure-Python time helpers (used by API and replay, mirror Spark):
```python
# static/gtfs_time.py
def parse_gtfs_time(s: str | None) -> int | None        # "25:10:00" -> 90600; None/"" -> None; invalid -> ValueError
def format_gtfs_seconds(sec: int) -> str                 # 90600 -> "25:10:00"
def service_anchor_utc(service_date: date, tz: str) -> datetime   # UTC(local noon) - 12h
def scheduled_utc(service_date: date, gtfs_seconds: int, tz: str) -> datetime
def parse_service_date(s: str) -> date                   # "20261005" -> date
# static/calendar.py
def active_services(calendar_rows, calendar_dates_rows, d: date) -> set[str]
```
Cassandra static tables (`storage/static_writer.py`): `routes_by_version`, `stops_by_version`, `stops_by_route`,
`stop_times_by_trip`, `geometries_by_route` (points_json = JSON `[[lon, lat], ...]`, ≤ `storage.geometry_max_points`,
`simplified` flag), idempotent prepared upserts, no TTL.

## 4. Ingestion — `src/transitpulse/ingestion/` (WP2)

CLI `python -m transitpulse.ingestion`:
* `collect --workdir W --run-id R [--profile P] [--duration-seconds N] [--no-publish]` — long-running; reads
  `W/runs/R/run.json` and the run's `scope_index.json`; writes raw + outbox; publishes to Kafka unless `--no-publish`.
  On exit (SIGTERM, duration, disk budget) finalizes `W/data/raw/R/recording_manifest.json`.
* `record` = `collect --no-publish`.
* `source-check --workdir W [--route-ids A,B] [--offline] [--out PATH]` -> `W/reports/source_probe_report.json`.
* `finalize-recording --workdir W --run-id R`, `outbox-status --workdir W --run-id R`, `disk-check --workdir W`.

Python API used by other packages:
```python
# ingestion/kafka_producer.py
class PayloadTooLarge(Exception): size: int; limit: int
class EnvelopeProducer:
    def __init__(self, cfg: ProjectConfig, component: str, run_id: str, *, producer_factory=None): ...
    generation: int                                        # bumped when the client is recreated after a fatal error
    fatal_error: KafkaError | None
    def send_envelope(self, env: Envelope, *, on_ack: Callable[[str, int, int, datetime], None] | None = None,
                      on_error: Callable[[Any], None] | None = None) -> None
        # sets published_at=now, re-checks size (PayloadTooLarge), key run_id|feed_type, headers, async
        # on_ack(topic, partition, offset, published_at) called only on broker ACK (err is None); on_error(err) otherwise
    def send_control(self, event: dict, *, on_ack=None, on_error=None) -> None   # key run_id|control, topic mbta.control.v1
        # clock events (run_started/clock_tick/run_paused/run_resumed/run_finished) MUST be sent with on_ack and confirmed
        # before the next snapshot is produced (D-E13); heartbeats/status events are fire-and-forget
    def send_dlq(self, record: dict, *, on_ack=None, on_error=None) -> None       # key run_id|feed_type, topic mbta.dlq.v1
    def send_raw(self, topic: str, key: bytes, value: bytes, *, on_ack=None, on_error=None) -> None  # tests/fault scenarios only
    def poll(self, timeout: float = 0.0) -> int
    def pending(self) -> int                               # messages still queued in the client
    def flush(self, timeout: float = 30.0) -> int          # returns messages still in queue
    def close(self) -> None
# ingestion/feed_parse.py
@dataclass class ParsedFeed: header: dict; entities: list[dict]; feed_timestamp: int
@dataclass class Rejection: reason_code: str; detail: str
def parse_feed(body: bytes, fetched_at: datetime) -> ParsedFeed | Rejection
# ingestion/scope_filter.py
class ScopeFilter:
    def __init__(self, index: ScopeIndex): ...
    def select(self, feed_type: str, entities: list[dict]) -> tuple[list[dict], dict]   # (selected, accounting)
# ingestion/archive.py
@dataclass(frozen=True) class RawRef: raw_path: str; raw_sha256: str; raw_bytes: int; stored_sha256: str; stored_bytes: int
class RawArchive:
    def __init__(self, raw_root: Path): ...
    def write_capture(self, run_id: str, feed_type: str, capture_id: str, fetched_at: datetime, body: bytes,
                      meta: dict) -> RawRef          # raw/<run>/<feed>/<YYYY-MM-DD>/<capture_id>.json.gz + .meta.json
    def read_capture(self, raw_path: str, *, expected_sha256: str | None = None) -> bytes   # verifies sha256
# ingestion/envelope_builder.py
def envelope_from_capture(*, run: RunRecord, feed_type: str, parsed: ParsedFeed, selected: list[dict],
        accounting: dict, raw_ref: RawRef, capture_id: str, fetched_at: datetime, source_url: str,
        http: dict | None, scenario_id: str | None, component: str) -> Envelope
    # computes payload_sha + snapshot_id (run-specific) and calls contracts.envelope.build_envelope
```

### Recording manifest (shared by collector, replay, synthetic, golden) — `recording_manifest.json`
```json
{"manifest_version": 1, "kind": "transitpulse.recording", "recording_id": "...", "source_type": "live|synthetic",
 "fixture_kind": null, "seed": null, "scenario_ids": [], "provenance": "...", "created_at": "...Z", "finalized_at": "...Z",
 "complete": true,
 "static": {"gtfs_version": "...", "zip_sha256": "...", "zip_relpath": "static/MBTA_GTFS.zip", "feed_version": null,
            "fetched_at": "...Z", "source_url": "..."},
 "scope": {"scope_id": "...", "route_ids": ["..."], "route_type": 3, "policy_version": "scope-v1"},
 "collector": {"poll_interval_seconds": 30, "user_agent": "...", "version": "..."},
 "time_range": {"first_fetched_at": "...Z", "last_fetched_at": "...Z", "source_duration_seconds": 0},
 "feeds": {"vehicle_positions": {"source_url": "...", "capture_count": 0, "rejected_count": 0, "not_modified_count": 0,
           "failed_fetch_count": 0}},
 "items": [{"seq": 1, "kind": "capture", "feed_type": "vehicle_positions", "at": "...Z", "capture_id": "...",
            "raw_path": "<relative to the recording dir>", "raw_sha256": "...", "raw_bytes": 0, "stored_sha256": "...",
            "parse_status": "ok|INVALID_JSON|...", "http_status": 200, "feed_timestamp": 0, "params": {}}],
 "gaps": [], "attribution": "Massachusetts Bay Transportation Authority (MBTA) GTFS/GTFS-Realtime ...",
 "manifest_sha256": "<sha256 of canonical manifest without this field>"}
```
Item kinds: `capture`, `fetch_event` (failed/304 fetch; params `{outcome, http_status, reason_code}`) for all
recordings; synthetic-only kinds: `tick` (params `{}`; `at` = new source clock), `republish` (params
`{of_seq: <seq of an earlier capture>}`), `arm_fault` (params `{point, match: {feed_type, snapshot_ordinal}}`),
`malformed_envelope` (params `{feed_type, value_text}`), `barrier` (params `{label: "S1"}`; step mode stops here).
For a live recording `raw_path` is relative to `W/data/raw/<run_id>/`; for generated recordings relative to
the recording directory. The static ZIP of a generated recording is at `<recording_dir>/static/MBTA_GTFS.zip`.

## 5. Replay, synthetic, golden — `src/transitpulse/replay/` (WP3)

CLI `python -m transitpulse.replay`:
* `verify --workdir W --manifest P [--static-zip Z] [--route-ids ...]` -> JSON `{ok, recording_id, gtfs_version,
  route_ids, start_clock, item_count}`; exit 2 with guidance when static/checksums mismatch (never downloads a new ZIP).
* `publish --workdir W --run-id R --manifest P --session-id S [--speed X] [--step-mode] [--duration-seconds N]`
  long-running publisher. Emits `run_started`, `clock_tick` (before each item at the item's time, plus every
  `replay.tick_interval_seconds` of wall time while waiting), `run_paused/run_resumed/run_finished`, heartbeats;
  for each capture re-reads raw, verifies sha256, re-parses, re-applies the run's scope filter, recomputes
  `snapshot_id` with the NEW run_id, keeps `capture_id`/`raw_path`/`original_fetched_at`, sets `published_at=now`.
  A clock tick is flushed (broker ACK) before the snapshot that follows it is produced.
* `synth-generate --workdir W --kind demo|golden|stress|scenario --seed N [--scenarios a,b] [--rate R]
  [--duration-seconds N] [--scenario-name IT06]` -> JSON `{ok, recording_id, recording_dir, manifest_path,
  static_zip_path, route_ids, start_clock, source_type:"synthetic", fixture_kind}`. Idempotent (same inputs ->
  same directory, skipped when it already exists with the same manifest hash).
  `--kind golden` materializes `tests/fixtures/golden/` (static/*.txt -> deterministic ZIP; rt/*.json -> raw gz;
  `timeline.json` steps -> items with `barrier` between steps). `--kind scenario --scenario-name IT06|IT07|IT08|IT09`
  materializes that fixture run.
* `control --workdir W --session-id S --command pause|resume|finish|step|advance-clock|inject [--to ISO]
  [--scenario duplicate_snapshot|late_snapshot|invalid_gps]` -> JSON ACK.

```python
# replay/control.py
def send_command(session_dir: Path, command: str, args: dict | None = None, timeout: float = 5.0) -> dict
    # atomically writes session_dir/publisher_control.json {request_id, command, args, requested_at};
    # waits until session_dir/publisher_state.json has applied_request_id == request_id; returns that state.
def read_publisher_state(session_dir: Path) -> dict | None
# publisher_state.json: {state: RUNNING|PAUSED|STEP_WAIT|FINISHED|FAILED, applied_request_id, clock_sequence,
#   source_clock, wall_heartbeat_at, items_published, items_total, next_index, last_barrier}
# replay/recording_verify.py
def verify_recording(manifest_path: Path, *, static_zip: Path | None = None,
                     route_ids: Sequence[str] | None = None) -> VerifiedRecording
```
Pause/resume/step/advance-clock are only valid for `source_type in {replay, synthetic}`; live returns exit 6.

## 6. Storage — `src/transitpulse/storage/` (WP4 except static_writer)

```python
# storage/cassandra.py
def get_session(cfg: ProjectConfig, *, keyspace: bool = True):       # process-cached Session
    # Cluster(cfg.cassandra.hosts, port, connect_timeout, DCAwareRoundRobinPolicy(local_dc), protocol default),
    # session.row_factory = dict_factory, default consistency from cfg (LOCAL_ONE), serial = SERIAL,
    # default_timeout = cassandra.request_timeout_seconds
def prepare(session, cql: str)                                         # cached PreparedStatement
def shutdown() -> None
# storage/migrate.py
def apply_migrations(cfg: ProjectConfig, *, migrations_dir: Path | None = None) -> list[dict]
    # [{migration_id, status: APPLIED|SKIPPED, checksum_sha256}], raises MigrationChecksumMismatch; never TRUNCATE/DROP
# storage/pointer.py
def read_pointer(session, run_id: str, feed_type: str, *, serial: bool = False) -> dict | None   # feed_current row
def bootstrap_pointer(session, row: dict) -> tuple[bool, dict | None]      # INSERT IF NOT EXISTS
def cas_pointer(session, row: dict, expected_snapshot_id: str) -> bool    # UPDATE ... IF snapshot_id = ?
# storage/manifest.py
def read_manifest(session, run_id, feed_type, snapshot_id, *, serial: bool = False) -> dict | None
def insert_building(session, row: dict) -> tuple[bool, dict]               # IF NOT EXISTS; returns (applied, row)
def mark_ready(session, key: tuple[str, str, str], *, ready_at, expected_counts, content_checksum, accounting_json,
               counts: dict) -> bool                                        # IF status = 'BUILDING'
def set_outcome_once(session, key, outcome: str, first_visible_at=None, visibility_latency_ms=None) -> bool
                                                                            # IF publish_outcome = null
```

## 7. Processing — `src/transitpulse/processing/` (WP4 orchestration, WP5 domain)

Entry: `python -m transitpulse.processing.stream_job --workdir W --run-id R [--profile P] [--enable-fault-injection]`.
It reads `runtime.json` (via `runtime.manifest.load_runtime`) for `spark.jars` and creates the SparkSession in-process
(`processing/spark_session.build_session(cfg, runtime=None, app_name="transitpulse", *, with_kafka=True, extra_conf=None) -> SparkSession`;
`runtime=None` or `with_kafka=False` means no connector jars — used by the static loader and Spark unit tests): `master=cfg.spark.master`,
`spark.sql.session.timeZone=UTC`, `spark.sql.shuffle.partitions`, `spark.ui.enabled`, `spark.driver.host=127.0.0.1`,
`spark.local.dir=W/spark-local`, `spark.jars=<connector jars>`, `spark.python.worker.reuse=true`; env `TZ=UTC`,
`PYSPARK_PYTHON` = core venv python (set by the launcher).
It writes `W/logs/runs/<run_id>/query_progress.jsonl` (one JSON per `onQueryProgress`: batchId, numInputRows,
durationMs, endOffsets per topic/partition, timestamp) and `W/runs/<run_id>/processor_ready.json`
(`{query_id, started_at, checkpoint}`) once the query is active. Readiness = that file exists AND the process is alive.

### WP4 -> WP5 builder contract
```python
# processing/vehicles.py (WP5)
def build_vehicle_generation(spark, snap: DataFrame, params: BuildParams, run: RunInfo, dims: Dims,
                             parent: DataFrame | None) -> GenerationOutput
    # tables: vehicles_by_route_snapshot, vehicle_observations_by_day, route_metrics_by_snapshot (feed_type
    #         'vehicle_positions', one row per run.route_ids entry), quality_by_snapshot
    # curated: observations, entity_quality ; dlq: rejected entities (MISSING_VEHICLE_ID, INVALID_ENTITY)
# processing/predictions.py (WP5)
def build_trip_update_generation(spark, snap, params, run, dims) -> GenerationOutput
    # tables: predictions_by_stop_snapshot, route_metrics_by_snapshot (feed_type 'trip_updates', one row per
    #         scope route incl. routes with zero trips), quality_by_snapshot
    # curated: predictions, entity_quality ; dlq: INVALID_ENTITY / INVALID_PREDICTION (missing trip_id)
# processing/alerts.py (WP5)
def build_alert_generation(spark, snap, params, run, dims, parent: DataFrame | None) -> GenerationOutput
    # tables: alerts_by_route_snapshot, quality_by_snapshot ; curated: alerts, entity_quality
```
* `snap`: one row, columns `PARSED_SNAPSHOT_COLUMNS` (`processing/context.py`).
* `parent`: for vehicles the parent generation's `vehicles_by_route_snapshot` rows (CQL column names, timestamps as
  TimestampType); for alerts the parent's `alerts_by_route_snapshot` rows; `None` when there is no parent. When
  `params.parent_snapshot_id` is set but `params.parent_available` is False the builder emits a
  `PARENT_UNAVAILABLE` quality row and performs no inheritance.
* Table DataFrames: column names == CQL columns of that table (subset allowed). Types: text->StringType,
  int->IntegerType, bigint->LongType, double->DoubleType, boolean->BooleanType, timestamp->TimestampType (UTC),
  date->DateType, set<text>/list<text>->ArrayType(StringType), set<int>->ArrayType(IntegerType),
  map<text,int>->MapType(StringType, IntegerType). Every row includes the full primary key. Rows are unique per PK.
* All freshness/KPI evaluation uses `params.metrics_as_of` (never `current_timestamp()`).
* `stats`: one row with `STATS_COLUMNS`. `accounting_ok=false` makes WP4 raise `AccountingMismatch` (batch fails).
* DLQ rows: columns `contracts.dlq.DLQ_FIELDS`, `error_id` from the Spark mirror of `ids.error_id_entity`,
  excerpt ≤ `kafka.dlq_excerpt_max_chars`.
* Curated DataFrames: free-form but must include `run_id, snapshot_id, feed_type` and the record id
  (`observation_id`/`prediction_id`/`alert_id`), `event_time` or `predicted_at`, `route_id`, and quality flags.
* `tests/spark/conftest.py` (WP5) provides the session-scoped pytest fixture `spark` (local[1], session timezone UTC,
  shuffle partitions 1, ui disabled, no Kafka jars) used by every test under `tests/spark/` (WP1, WP4, WP5).
* `processing/spark_ids.py` (WP5) provides Spark Column mirrors: `k_cols(*cols)`, `h_col(col)`,
  `observation_id_col(...)`, `state_sample_id_col(...)`, `prediction_id_col(...)`, `error_id_entity_col(...)`,
  `e7_col/e2_col/e3_col`. `tests/spark/test_spark_ids_parity.py` compares them with `contracts.ids`.

### WP4 orchestration responsibilities (design B §3-§6)
`processing/parse.py`: Kafka rows -> parsed envelopes + control events + message-level rejects (DLQ + counts).
`processing/control_clock.py`: fold control events, LWT clock update (`IF clock_sequence < ? AND application_clock <= ?`
for source mode; `IF clock_sequence < ?` for wall), run_state transitions, collector status to pipeline_status.
`processing/batch.py`: `process_batch(df, batch_id)`; manifests (BUILDING via LWT, frozen params, parent selection =
max(pointer, previous non-late in-batch snapshot of the same feed)), parent row reads (bounded), builder calls,
`expected_counts` (`"<table>"` totals + `"<table>/<key>"` per `PER_PARTITION_COUNT_KEYS`), sink writes + count
verification, READY, pointer CAS loop, outcomes, `LATE_SNAPSHOT_NOT_PUBLISHED` / `DUPLICATE_SNAPSHOT_DELIVERY` /
`EXPIRED_NOT_PUBLISHED` quality rows, pipeline_status (batch-guarded LWT), metrics.
`processing/sink.py`: per-worker cached session, prepared INSERT per (table, column tuple) with `USING TTL ?`,
`execute_concurrent_with_args(concurrency=cfg.cassandra.write_concurrency, raise_on_first_error=True)`, `None ->
UNSET_VALUE`, epoch-ms -> aware datetime, TTL < 1 raises. Returns counts per table.
`processing/curated.py`: batch commit (staging -> rename -> `_COMMITTED`). `processing/dlq.py`: Kafka DLQ writes.
`processing/faults.py`: arm-file fault points (`after_partial_write`, `after_ready_before_cas`,
`after_cas_before_commit`, `executor_write_error`), `os._exit(86)`, honoured only with `--enable-fault-injection`.
`processing/report_job.py`: `python -m transitpulse.processing.report_job --workdir W --run-id R` (FR09 batch report).

## 8. API — `src/transitpulse/api/` (WP6)

`uvicorn transitpulse.api.app:create_app --factory --host <cfg.api.host> --port <cfg.api.port> --workers 1`
(env `TP_WORKDIR`, `TP_PROFILE`). Endpoints under `cfg.api.base_path` (`/api/v1`): `/health/live`, `/health/ready`,
`/runs`, `/runs/{run_id}`, `/routes`, `/routes/{route_id}/geometry`, `/routes/{route_id}/stops`,
`/trips/{trip_id}/stops`, `/vehicles`, `/vehicles/{vehicle_id}/history`, `/stops/{stop_id}/predictions`,
`/routes/{route_id}/metrics`, `/alerts`, `/quality`, `/pipeline/status`, `/metrics`.
Response `{data, meta, warnings}`; error `{error: {code, message, request_id, retryable, details}, meta}`.
Serving rules: read each feed pointer once per request; READY manifest; `expires_at <= now` -> 503
`generation_expired`; fewer rows than `expected_counts["<table>/<key>"]` -> 503 `generation_inconsistent`;
vehicle freshness recomputed with `contracts.policy.vehicle_freshness` at the run's application clock (wall now for
`clock_mode=wall`); rows with non-null `invalid_reason` stay invalid (frozen at processing).
The API never reads raw files, Kafka or MBTA.

## 9. Native runtime and CLIs — `src/transitpulse/runtime/`, `scripts/` (WP7)

`runtime.json` (written by `prepare_runtime.py`):
```json
{"schema_version": 1, "profile": "colab-native", "repo_dir": "...", "repo_commit": "...|null", "workdir": "...",
 "python": {"core_python": ".../.venv/bin/python", "version": "3.11.17", "uv": ".../uv", "lock_sha256": "...", "lock_origin": "committed|generated_on_colab"},
 "java": {"java_home": "...", "version": "17.0.20.1", "vendor": "Eclipse Temurin"},
 "kafka": {"home": "...", "config": ".../conf/kafka/server.properties", "log_dir": "...", "bootstrap": "127.0.0.1:9092"},
 "cassandra": {"home": "...", "conf_dir": "...", "host": "127.0.0.1", "port": 9042},
 "spark": {"spark_home": "...", "version": "3.5.7", "scala": "2.12", "connector_jars": ["..."]},
 "api": {"base_url": "http://127.0.0.1:8000/api/v1"},
 "paths": {"...": "..."}, "hashes": {"manifest": "...", "config": "...", "lock": "...", "rendered_conf": "..."},
 "env": {"JAVA_HOME": "...", "PATH": "...", "PYSPARK_PYTHON": "...", "PYSPARK_DRIVER_PYTHON": "...", "TZ": "UTC",
         "TP_WORKDIR": "...", "TP_REPO_DIR": "...", "TP_PROFILE": "..."}}
```
```python
# runtime/manifest.py
@dataclass(frozen=True) class RuntimeInfo: raw: dict; core_python: Path; java_home: Path; spark_home: Path;
    connector_jars: tuple[str, ...]; kafka_home: Path; cassandra_home: Path; env: dict[str, str]
def load_runtime(paths: WorkPaths) -> RuntimeInfo            # FileNotFoundError with a hint to run bootstrap
def subprocess_env(runtime: RuntimeInfo, extra: dict | None = None) -> dict[str, str]
    # starts from a CLEAN env (PATH, HOME, LANG) + runtime.env; strips PYTHONPATH/PYTHONHOME/SPARK_HOME/PYSPARK_*/CLASSPATH/VIRTUAL_ENV
# runtime/supervisor.py
class Supervisor:
    def __init__(self, paths: WorkPaths): ...
    def start(self, component: str, argv: list[str], *, env: dict, scope: str, session_id: str | None = None,
              run_id: str | None = None, ports: tuple[int, ...] = (), ready: Callable[[], bool] | None = None,
              ready_timeout: float = 240, fingerprint_extra: str = "", min_mem_available_bytes: int = 0,
              deadline_epoch: float | None = None, graceful_timeout: float | None = None, cwd: str | None = None) -> dict
              # -> registry entry incl. {"state": READY|..., "reused": bool}; raises SupervisorError(code, message, hint, log_excerpt)
    def stop(self, component: str, *, timeout: float | None = None) -> dict
    def status(self) -> dict
    def is_alive(self, component: str) -> bool
    def wait_exit(self, component: str, timeout: float) -> int | None
    def entry(self, component: str) -> dict | None
# runtime/spark_launch.py
def processor_argv(runtime: RuntimeInfo, paths: WorkPaths, run_id: str, *, profile: str,
                   fault_injection: bool = False) -> list[str]   # [core_python, -m, transitpulse.processing.stream_job, ...]
def processor_env(runtime: RuntimeInfo) -> dict[str, str]
```
Component names: `kafka`, `cassandra`, `processor`, `publisher`, `collector`, `api`, `dashboard`, `tunnel`, `tunnel-api`.
`dashboard` (Streamlit on 127.0.0.1:8501) and `tunnel`/`tunnel-api` (cloudflared quick tunnels) have scope `access`: they survive
demo sessions, are started by `demo.py expose start [--target dashboard|api|both]`, and are stopped by `demo.py expose stop`
or `demo.py stop --all`. Public URLs are written to `W/runtime/public_urls.json`.
`scripts/infra.py` (`start|init|status|logs|stop|adopt`) and `scripts/demo.py`
(`prepare|start|wait-ready|status|logs|pause|resume|restart-processor|stop|reset|golden-smoke|inject|step|
advance-clock|lineage|benchmark`) are thin wrappers over `runtime/cli_infra.py` and `runtime/cli_demo.py`.
`demo.py start` JSON: `{ok, action:"start", reused, session_id, run_id, source_type, mode, scope:{route_ids,
verified_mbta, gtfs_version, scope_id}, api:{base_url}, checkpoint_dir, deadline_at, components:{...}, repo_commit,
commands:{status, logs, stop}}`. The notebook reads this JSON; it never hard-codes a run_id.
Session record `W/sessions/<sid>/session.json`: `{session_id, run_id, mode, source_type, started_at, deadline_at,
duration_seconds, recording_manifest, speed, route_ids, state}`; `registry.json` key `current_session`.

## 10. Golden fixture, oracle, verification (WP0b, WP9)

```
tests/fixtures/golden/
  PROVENANCE.md                 synthetic, hand-written, NOT MBTA data
  static/{agency,routes,stops,trips,stop_times,calendar,calendar_dates,shapes,feed_info}.txt
  rt/VP1.json VP2.json TU1.json ALERTS1.json ALERTS2.json ALERTS3.json            (MBTA JSON shape)
  rt/IT06_VP.json IT07_TU.json IT08_TU.json                                         (scenario snapshots)
  timeline.json                 runs -> steps -> items (kinds as in §4: tick, capture{file}, republish{of_step_item}, barrier)
  expected.json                 hand-written expected values (design C §8-9 format)
```
`timeline.json`:
```json
{"fixture_id": "golden-v1", "agency_timezone": "America/New_York", "scope_route_ids": ["R_A", "R_B"],
 "runs": {"golden": {"start_clock": "2026-10-05T12:00:30Z", "steps": [
   {"step": "S1", "items": [{"kind": "tick", "at": "2026-10-05T12:00:30Z"},
                            {"kind": "capture", "feed_type": "vehicle_positions", "file": "rt/VP1.json", "at": "2026-10-05T12:00:30Z", "label": "VP1"},
                            {"kind": "capture", "feed_type": "trip_updates", "file": "rt/TU1.json", "at": "2026-10-05T12:00:30Z", "label": "TU1"},
                            {"kind": "capture", "feed_type": "alerts", "file": "rt/ALERTS1.json", "at": "2026-10-05T12:00:30Z", "label": "ALERTS1"}]},
   {"step": "S3", "items": [{"kind": "republish", "label": "VP2"}]}]}}}
```
(`republish` refers to an earlier capture `label`.) `expected.json` uses fixture labels (VP1, TU1, ...) instead of
run-specific ids; comparisons normalize ids through the label map returned by materialization
(`<recording_dir>/labels.json`: `{label: {capture_id, raw_sha256}}`, and per run `{label: snapshot_id}` computed
with `contracts.ids` after the run_id is known).

`tests/oracle/` is stdlib-only and must not import `transitpulse` or `pyspark`.
`src/transitpulse/verify/golden.py` (WP9): `check_step(api_base: str, run_id: str, step: dict, labels: dict, *, timeout=..., client=None, late_threshold_seconds=...)` — `step` is the whole expected.json step dict (`step`, `clock`, `expect`)
-> dict{passed: bool, checks: [{name, expected, actual, passed}]}` used by `demo.py golden-smoke` and IT01-IT04.
