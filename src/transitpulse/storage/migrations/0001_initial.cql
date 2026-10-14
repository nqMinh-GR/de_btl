-- TransitPulse MBTA — initial schema (query-first, denormalized; PDF p15-16, p30; design B §1).
-- Notation in docs: ((a,b),c) = partition key (a,b), clustering column c.
-- Rules: no ALLOW FILTERING anywhere; no counters; no multi-partition logged batches.
-- LWT-only tables/cells (never written with plain INSERT/UPDATE after creation):
--   feed_current, snapshot_manifest, run_metadata.{run_state,application_clock,clock_sequence,clock_updated_at,state_reason},
--   pipeline_status rows of component 'processor' and 'feed:<feed_type>'.
-- Serving generation tables are written USING TTL <seconds remaining until snapshot_manifest.expires_at>.
-- History/quality tables are written USING TTL <seconds remaining until snapshot_manifest.history_expires_at>.
-- Splitting rule used by storage/migrate.py: drop lines starting with '--', then split on ';' that ends a line.
-- (No ';' may appear inside string literals in migration files.)

CREATE KEYSPACE IF NOT EXISTS transitpulse WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} AND durable_writes = true;

CREATE TABLE IF NOT EXISTS transitpulse.schema_migrations (
  migration_id text PRIMARY KEY,
  checksum_sha256 text,
  description text,
  applied_at timestamp,
  applied_by text
);

-- ---------------------------------------------------------------- run metadata
CREATE TABLE IF NOT EXISTS transitpulse.run_metadata (
  run_id text PRIMARY KEY,
  project_id text,
  scope_id text,
  source_type text,
  clock_mode text,
  gtfs_version text,
  agency_timezone text,
  route_ids list<text>,
  pipeline_version text,
  policy_version text,
  policy_json text,
  fixture_kind text,
  scenario_id text,
  seed bigint,
  recording_id text,
  session_id text,
  speed double,
  label text,
  provenance_json text,
  created_at timestamp,
  run_state text,
  application_clock timestamp,
  clock_sequence bigint,
  clock_updated_at timestamp,
  state_reason text
);

CREATE TABLE IF NOT EXISTS transitpulse.runs_by_project (
  project_id text,
  created_at timestamp,
  run_id text,
  source_type text,
  scope_id text,
  gtfs_version text,
  scenario_id text,
  label text,
  PRIMARY KEY ((project_id), created_at, run_id)
) WITH CLUSTERING ORDER BY (created_at DESC, run_id ASC);

-- ---------------------------------------------------------------- publish protocol
CREATE TABLE IF NOT EXISTS transitpulse.feed_current (
  run_id text,
  feed_type text,
  snapshot_id text,
  generation_id text,
  source_order text,
  feed_timestamp timestamp,
  metrics_as_of timestamp,
  view_published_at timestamp,
  expires_at timestamp,
  is_partial boolean,
  is_empty boolean,
  PRIMARY KEY ((run_id, feed_type))
);

CREATE TABLE IF NOT EXISTS transitpulse.snapshot_manifest (
  run_id text,
  feed_type text,
  snapshot_id text,
  generation_id text,
  status text,
  source_order text,
  feed_timestamp timestamp,
  original_fetched_at timestamp,
  published_at timestamp,
  capture_id text,
  kafka_topic text,
  kafka_partition int,
  kafka_offset bigint,
  first_batch_id bigint,
  raw_path text,
  raw_sha256 text,
  gtfs_version text,
  scope_id text,
  scenario_id text,
  parent_snapshot_id text,
  parent_generation_id text,
  parent_available boolean,
  built_as_late boolean,
  metrics_as_of timestamp,
  clock_sequence_at_build bigint,
  first_materialized_at timestamp,
  expires_at timestamp,
  history_expires_at timestamp,
  source_entity_count int,
  selected_entity_count int,
  fully_valid_entity_count int,
  partially_valid_entity_count int,
  rejected_entity_count int,
  is_partial boolean,
  is_empty boolean,
  ready_at timestamp,
  expected_counts map<text, int>,
  content_checksum text,
  accounting_json text,
  publish_outcome text,
  first_visible_at timestamp,
  visibility_latency_ms bigint,
  PRIMARY KEY ((run_id, feed_type), snapshot_id)
);

-- ---------------------------------------------------------------- static catalog (per GTFS version)
CREATE TABLE IF NOT EXISTS transitpulse.routes_by_version (
  gtfs_version text,
  route_id text,
  agency_id text,
  route_short_name text,
  route_long_name text,
  route_type int,
  route_color text,
  route_text_color text,
  route_sort_order int,
  PRIMARY KEY ((gtfs_version), route_id)
);

CREATE TABLE IF NOT EXISTS transitpulse.stops_by_version (
  gtfs_version text,
  stop_id text,
  stop_code text,
  stop_name text,
  stop_lat double,
  stop_lon double,
  location_type int,
  parent_station text,
  platform_code text,
  wheelchair_boarding int,
  PRIMARY KEY ((gtfs_version), stop_id)
);

CREATE TABLE IF NOT EXISTS transitpulse.stops_by_route (
  gtfs_version text,
  route_id text,
  stop_id text,
  stop_name text,
  stop_lat double,
  stop_lon double,
  direction_ids set<int>,
  pattern_count int,
  PRIMARY KEY ((gtfs_version, route_id), stop_id)
);

CREATE TABLE IF NOT EXISTS transitpulse.stop_times_by_trip (
  gtfs_version text,
  trip_id text,
  stop_sequence int,
  stop_id text,
  arrival_time_raw text,
  departure_time_raw text,
  arrival_seconds int,
  departure_seconds int,
  route_id text,
  direction_id int,
  service_id text,
  shape_id text,
  trip_headsign text,
  stop_headsign text,
  pickup_type int,
  drop_off_type int,
  timepoint int,
  PRIMARY KEY ((gtfs_version, trip_id), stop_sequence)
);

CREATE TABLE IF NOT EXISTS transitpulse.geometries_by_route (
  gtfs_version text,
  route_id text,
  shape_id text,
  direction_ids set<int>,
  trip_count int,
  point_count int,
  points_json text,
  simplified boolean,
  is_representative boolean,
  PRIMARY KEY ((gtfs_version, route_id), shape_id)
);

-- ---------------------------------------------------------------- serving generations (TTL = remaining to expires_at)
CREATE TABLE IF NOT EXISTS transitpulse.vehicles_by_route_snapshot (
  run_id text,
  snapshot_id text,
  route_id text,
  vehicle_id text,
  observation_id text,
  record_kind text,
  observation_time timestamp,
  event_time timestamp,
  event_time_origin text,
  freshness_known boolean,
  position_valid boolean,
  invalid_reason text,
  latitude double,
  longitude double,
  bearing double,
  speed_mps double,
  vehicle_label text,
  trip_id text,
  direction_id int,
  start_date text,
  start_time text,
  trip_schedule_relationship text,
  stop_id text,
  current_stop_sequence int,
  current_status text,
  occupancy_status text,
  feed_timestamp timestamp,
  is_inherited boolean,
  inherited_from_snapshot_id text,
  quality_flags set<text>,
  PRIMARY KEY ((run_id, snapshot_id, route_id), vehicle_id)
);

CREATE TABLE IF NOT EXISTS transitpulse.predictions_by_stop_snapshot (
  run_id text,
  snapshot_id text,
  stop_id text,
  predicted_at timestamp,
  prediction_id text,
  event_type text,
  trip_id text,
  route_id text,
  direction_id int,
  start_date text,
  start_time text,
  service_date text,
  trip_instance_key text,
  stop_sequence int,
  trip_schedule_relationship text,
  stop_schedule_relationship text,
  vehicle_id text,
  scheduled_at timestamp,
  predicted_delay_seconds int,
  schedule_resolution text,
  resolution_method text,
  uncertainty int,
  prediction_quality text,
  kpi_eligible boolean,
  is_target boolean,
  feed_timestamp timestamp,
  quality_flags set<text>,
  PRIMARY KEY ((run_id, snapshot_id, stop_id), predicted_at, prediction_id)
);

CREATE TABLE IF NOT EXISTS transitpulse.route_metrics_by_snapshot (
  run_id text,
  feed_type text,
  snapshot_id text,
  route_id text,
  metrics_as_of timestamp,
  feed_timestamp timestamp,
  is_partial boolean,
  policy_version text,
  snapshot_membership_count int,
  fresh_vehicle_count int,
  stale_vehicle_count int,
  unknown_age_vehicle_count int,
  invalid_vehicle_count int,
  inherited_vehicle_count int,
  stale_threshold_seconds int,
  trip_instance_count int,
  eligible_trip_count int,
  predicted_late_trip_count int,
  predicted_late_rate double,
  mean_predicted_delay_seconds double,
  excluded_trip_counts map<text, int>,
  unknown_uncertainty_count int,
  late_threshold_seconds int,
  target_policy text,
  quality_flags set<text>,
  PRIMARY KEY ((run_id, feed_type, snapshot_id), route_id)
);

CREATE TABLE IF NOT EXISTS transitpulse.alerts_by_route_snapshot (
  run_id text,
  snapshot_id text,
  route_id text,
  alert_id text,
  cause text,
  effect text,
  severity_level text,
  header_text text,
  description_text text,
  url text,
  translations_json text,
  active_periods_json text,
  informed_entities_json text,
  match_kinds set<text>,
  selector_resolution text,
  active_state_at_build text,
  confirmation_status text,
  inherited_from_snapshot_id text,
  first_seen_snapshot_id text,
  feed_timestamp timestamp,
  quality_flags set<text>,
  PRIMARY KEY ((run_id, snapshot_id, route_id), alert_id)
);

-- ---------------------------------------------------------------- history / quality (TTL = remaining to history_expires_at)
CREATE TABLE IF NOT EXISTS transitpulse.vehicle_observations_by_day (
  run_id text,
  vehicle_id text,
  utc_date date,
  event_time timestamp,
  observation_id text,
  record_kind text,
  event_time_origin text,
  route_id text,
  trip_id text,
  direction_id int,
  start_date text,
  latitude double,
  longitude double,
  bearing double,
  speed_mps double,
  stop_id text,
  current_stop_sequence int,
  current_status text,
  quality_flags set<text>,
  PRIMARY KEY ((run_id, vehicle_id, utc_date), event_time, observation_id)
) WITH CLUSTERING ORDER BY (event_time DESC, observation_id ASC);

CREATE TABLE IF NOT EXISTS transitpulse.quality_by_snapshot (
  run_id text,
  feed_type text,
  snapshot_id text,
  reason_code text,
  severity text,
  unit text,
  record_count int,
  disposition_counts map<text, int>,
  sample_refs list<text>,
  details_json text,
  PRIMARY KEY ((run_id, feed_type, snapshot_id), reason_code)
);

-- ---------------------------------------------------------------- operations
CREATE TABLE IF NOT EXISTS transitpulse.pipeline_status (
  run_id text,
  component text,
  status text,
  last_batch_id bigint,
  last_success_at timestamp,
  last_heartbeat_at timestamp,
  last_batch_duration_ms bigint,
  input_messages_total bigint,
  rejected_messages_total bigint,
  snapshots_built_total bigint,
  snapshots_published_total bigint,
  late_deliveries_total bigint,
  duplicate_deliveries_total bigint,
  last_snapshot_id text,
  last_published_snapshot_id text,
  last_visibility_latency_ms bigint,
  last_rejected_at timestamp,
  last_rejected_reason text,
  last_rejected_error_id text,
  progress_json text,
  last_error text,
  PRIMARY KEY ((run_id), component)
);
