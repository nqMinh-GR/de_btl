# Ánh xạ truy vấn → bảng Cassandra (query-first)

Ký hiệu `((a,b),c)`: partition key gồm `a,b`, clustering column `c`. Không truy vấn nào dùng
`ALLOW FILTERING` hay full scan; mọi đọc đều theo partition và có `LIMIT`. CQL đầy đủ:
`src/transitpulse/storage/migrations/0001_initial.cql`.

| Bảng | Khóa | Ghi bởi | Đọc bởi (query) | TTL |
|---|---|---|---|---|
| `run_metadata` | `((run_id))` | `runs.create_run` (INSERT IF NOT EXISTS), processor (LWT clock/state) | API `/runs/{id}` và mọi endpoint (clock, scope, gtfs_version, policy) | không |
| `runs_by_project` | `((project_id), created_at DESC, run_id)` | `runs.create_run` | `/runs` (LIMIT + paging) | không |
| `feed_current` | `((run_id, feed_type))` | processor (CAS) | mọi endpoint dữ liệu: đọc **một lần** mỗi feed mỗi request | không |
| `snapshot_manifest` | `((run_id, feed_type), snapshot_id)` | processor (LWT) | API (READY, `expires_at`, `expected_counts`), lineage, benchmark latency | không |
| `routes_by_version` | `((gtfs_version), route_id)` | static loader | `/routes` (lọc theo scope của run) | không |
| `stops_by_version` | `((gtfs_version), stop_id)` | static loader | chi tiết trạm | không |
| `stops_by_route` | `((gtfs_version, route_id), stop_id)` | static loader | `/routes/{id}/stops` (tập trạm, không giả thứ tự) | không |
| `stop_times_by_trip` | `((gtfs_version, trip_id), stop_sequence)` | static loader | `/trips/{id}/stops` | không |
| `geometries_by_route` | `((gtfs_version, route_id), shape_id)` | static loader | `/routes/{id}/geometry` (từng shape riêng) | không |
| `vehicles_by_route_snapshot` | `((run_id, snapshot_id, route_id), vehicle_id)` | processor | `/vehicles`, `/routes/{id}/metrics` (tính lại freshness) | `expires_at` |
| `vehicle_observations_by_day` | `((run_id, vehicle_id, utc_date), event_time DESC, observation_id)` | processor | `/vehicles/{id}/history` (≤ 24 h, ≤ 2 bucket ngày, paging token) | 3 ngày |
| `predictions_by_stop_snapshot` | `((run_id, snapshot_id, stop_id), predicted_at, prediction_id)` | processor | `/stops/{id}/predictions` (`predicted_at >= clock − 60 s`) | `expires_at` |
| `route_metrics_by_snapshot` | `((run_id, feed_type, snapshot_id), route_id)` | processor | `/routes/{id}/metrics` (KPI as-of generation) | `expires_at` |
| `alerts_by_route_snapshot` | `((run_id, snapshot_id, route_id), alert_id)` | processor | `/alerts` (fan-out ≤ 5 route, dedup alert_id) | `expires_at` |
| `quality_by_snapshot` | `((run_id, feed_type, snapshot_id), reason_code)` | processor | `/quality` | 3 ngày |
| `pipeline_status` | `((run_id), component)` | processor, collector/publisher status | `/pipeline/status` | không |
| `schema_migrations` | `((migration_id))` | `infra.py init` | `/health/ready` | không |

## Endpoint → đọc

| Endpoint | Chuỗi đọc |
|---|---|
| `GET /health/live` | không |
| `GET /health/ready` | `system.local` + `schema_migrations` |
| `GET /runs` | `runs_by_project` (LIMIT) → `run_metadata` từng run |
| `GET /runs/{run_id}` | `run_metadata`, 3 × `feed_current`, `pipeline_status` |
| `GET /routes` | `run_metadata` → `routes_by_version` (gtfs_version của run) lọc scope |
| `GET /routes/{route_id}/geometry` | `run_metadata` → `geometries_by_route` |
| `GET /routes/{route_id}/stops` | `run_metadata` → `stops_by_route` |
| `GET /trips/{trip_id}/stops` | `run_metadata` → `stop_times_by_trip` |
| `GET /vehicles` | `run_metadata` → `feed_current(VP)` → `snapshot_manifest` → `vehicles_by_route_snapshot` (từng route trong scope, tổng ≤ 2000 hàng) |
| `GET /vehicles/{vehicle_id}/history` | `vehicle_observations_by_day` theo từng bucket ngày UTC |
| `GET /stops/{stop_id}/predictions` | `feed_current(TU)` → `snapshot_manifest` → `predictions_by_stop_snapshot` |
| `GET /routes/{route_id}/metrics` | `feed_current(VP, TU)` → `route_metrics_by_snapshot` + `vehicles_by_route_snapshot` (tính lại tại clock) |
| `GET /alerts` | `feed_current(AL)` → `alerts_by_route_snapshot` |
| `GET /quality` | 3 × `feed_current` → `quality_by_snapshot` + `pipeline_status` |
| `GET /pipeline/status` | `pipeline_status`, `run_metadata`, pointers |

Quy tắc nhất quán: mỗi request đọc pointer **một lần** cho mỗi feed rồi dùng đúng `snapshot_id` đó; nếu số
hàng đọc được ít hơn `expected_counts["<bảng>/<khóa>"]` trước khi hết hạn → 503 `generation_inconsistent`
(không biến mất thành “0 xe”). Generation hết hạn → 503 `generation_expired`; snapshot rỗng hợp lệ → 200 `[]`.
