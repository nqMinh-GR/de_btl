# Từ điển dữ liệu

Quy ước: ID luôn là chuỗi (giữ số 0 đầu); instant là UTC (`timestamp` trong Cassandra, ISO-8601 có `Z` trong
API); duration có hậu tố `_seconds`/`_ms`; count có hậu tố `_count`; rate là phân số [0,1] có hậu tố `_rate`.
Giá trị tùy chọn thiếu là `null`, không bao giờ là 0 hay chuỗi rỗng. Danh sách cột đầy đủ:
`src/transitpulse/storage/migrations/0001_initial.cql`.

## 1. Định danh

| Định danh | Cách tạo | Mục đích |
|---|---|---|
| `run_id` | uuid4, một lần mỗi run | Namespace Kafka key, checkpoint, raw, outbox, curated, serving |
| `scope_id` | sha256(route IDs đã sắp, route_type, scope policy, gtfs_version)[:16] | Phân biệt phạm vi xử lý |
| `gtfs_version` | sha256 của bytes ZIP | Pin một version lịch cho mỗi run |
| `capture_id` | `<vp|tu|al>-<UTC µs>Z-<sha256[:12]>` | Một lần HTTP capture (audit dù payload lặp) |
| `snapshot_id` | `sn_` + H(run, scope, feed, header.ts, sha256 nội dung đã lọc) | Snapshot logic; retry không đổi ID |
| `generation_id` | `gn_` + H(run, feed, snapshot, pipeline_version) | Bộ hàng phục vụ bất biến |
| `observation_id` | `ob_` + H(xe, descriptor, timestamp, vị trí lượng tử hóa, stop/status) | Một quan sát; poll lại không sinh lịch sử |
| state sample | `ss_` + H(…, header timestamp, …) | Xe thiếu timestamp; `event_time_origin=feed_header`, `freshness_known=false` |
| `prediction_id` | `pr_` + H(trip, start_date, start_time, route, direction, stop_sequence, stop_id, arrival/departure) | Một dự báo của một lần ghé trạm |
| `error_id` | `er_` + H(reference, reason) | Đối chiếu DLQ khi delivery lặp |
| `source_order` | `%012d.%017d.%s` (feed ts, capture µs, snapshot_id) | Thứ tự nguồn để CAS pointer |

`H(s) = sha256(s)[:32]`; các phần nối bằng `\x1f`, null → `\x00`; lat/lon × 1e7, bearing × 100, speed × 1000 làm
tròn trước khi hash để Python và Spark cho cùng kết quả.

## 2. Envelope Kafka (schema_version 1)

| Nhóm | Trường |
|---|---|
| Version & namespace | `schema_version`, `message_type="feed_snapshot"`, `run_id`, `scope_id`, `source_type` (live/replay/synthetic), `feed_type`, `gtfs_version`, `scenario_id` |
| Định danh & lineage | `snapshot_id`, `capture_id`, `source_url`, `raw_path` (tương đối RAW_ROOT), `raw_sha256`, `raw_bytes` |
| Thời gian | `feed_timestamp` (epoch giây), `original_fetched_at`, `published_at` (ISO UTC) |
| Accounting | `source_entity_count`, `selected_entity_count`, `scope_accounting` |
| Payload | `header`, `entities` (entity gốc trong scope, giữ nguyên) |
| Khác | `http{status, etag, last_modified, content_type}`, `producer{component, version}` |

Kích thước serialize ≤ 4 MiB; lớn hơn → giữ raw, `PAYLOAD_TOO_LARGE`, không publish, không cắt bớt entity.

## 3. Bảng phục vụ chính

### `vehicles_by_route_snapshot` — `((run_id, snapshot_id, route_id), vehicle_id)`
| Cột | Ý nghĩa |
|---|---|
| `observation_id`, `record_kind` | ID quan sát; `observation` hoặc `snapshot_state_sample` |
| `observation_time` | Thời điểm nguồn đo (null nếu thiếu) |
| `event_time`, `event_time_origin` | Thời điểm hiệu dụng; `vehicle_timestamp` hoặc `feed_header` |
| `freshness_known` | false khi thiếu timestamp nguồn |
| `position_valid`, `invalid_reason` | Quyết định invalid đóng băng lúc xử lý (`INVALID_COORDINATES`, `FUTURE_TIMESTAMP`, `INVALID_TIMESTAMP_UNIT`) |
| `latitude`, `longitude`, `bearing`, `speed_mps` | Vị trí nguồn; speed null nếu nguồn không có |
| `trip_id`, `direction_id`, `start_date`, `start_time`, `trip_schedule_relationship` | Descriptor chuyến |
| `is_inherited`, `inherited_from_snapshot_id` | Quan sát kế thừa từ parent khi `OBSERVATION_TIME_REGRESSION` |
| `quality_flags` | Cảnh báo (`CLOCK_SKEW`, `MISSING_OBSERVATION_TIMESTAMP`, …) |

Freshness **không** lưu cố định: API tính lại tại application clock của request (fresh/stale/unknown);
dòng có `invalid_reason` giữ trạng thái invalid.

### `predictions_by_stop_snapshot` — `((run_id, snapshot_id, stop_id), predicted_at, prediction_id)`
| Cột | Ý nghĩa |
|---|---|
| `event_type` | `arrival` / `departure` (không trộn) |
| `predicted_at`, `scheduled_at` | Giờ dự báo của nguồn; giờ lịch cùng loại sự kiện (null nếu lịch không xác định) |
| `predicted_delay_seconds` | `predicted − scheduled`; có thể âm; null khi lịch ambiguous/unresolved hoặc không áp dụng |
| `schedule_resolution`, `resolution_method` | resolved/ambiguous/unresolved/not_applicable; descriptor/inferred |
| `uncertainty`, `prediction_quality` | Giá trị gốc (null nếu thiếu); realtime/unknown_uncertainty/schedule_only/degraded/invalid |
| `kpi_eligible`, `is_target` | Tham gia KPI; là target stop của trip instance |
| `trip_instance_key`, `service_date` | Trip instance = trip_id + ngày dịch vụ |

### `route_metrics_by_snapshot` — `((run_id, feed_type, snapshot_id), route_id)`
Vehicle metrics (`feed_type=vehicle_positions`): `snapshot_membership_count`, `fresh/stale/unknown_age/invalid_vehicle_count`,
`inherited_vehicle_count`, `stale_threshold_seconds`, `metrics_as_of`.
Prediction KPI (`feed_type=trip_updates`): `trip_instance_count`, `eligible_trip_count`, `predicted_late_trip_count`,
`predicted_late_rate` (null khi mẫu số 0), `mean_predicted_delay_seconds` (null khi mẫu số 0),
`excluded_trip_counts` (map lý do → số chuyến), `unknown_uncertainty_count`, `late_threshold_seconds`,
`target_policy`, `policy_version`, `metrics_as_of`, `feed_timestamp`.

### `alerts_by_route_snapshot` — `((run_id, snapshot_id, route_id), alert_id)`
`cause`, `effect`, `severity_level`, `header_text`, `description_text`, `active_periods_json`,
`informed_entities_json`, `match_kinds` (route/stop/trip/agency/route_type), `selector_resolution`,
`active_state_at_build` (ALWAYS/ACTIVE/UPCOMING/EXPIRED — API tính lại tại clock), `confirmation_status`
(CONFIRMED / UNCONFIRMED_INHERITED), `inherited_from_snapshot_id`, `first_seen_snapshot_id`.

### `vehicle_observations_by_day` — `((run_id, vehicle_id, utc_date), event_time DESC, observation_id)`
Lịch sử quan sát duy nhất (đã khử trùng theo `observation_id`); TTL 3 ngày theo wall clock.

### `quality_by_snapshot` — `((run_id, feed_type, snapshot_id), reason_code)`
`severity` (error/warning/info), `unit` (message/snapshot/entity/prediction/trip_instance/selector),
`record_count`, `disposition_counts` (retained/excluded_from_metric/quarantined/duplicate/out_of_scope/rejected),
`sample_refs` (≤ 10), `details_json`. Một record có thể có nhiều warning; tổng warning không phải mẫu số tỷ lệ lỗi.

## 4. Bảng metadata

* `run_metadata`: scope, `source_type`, `clock_mode`, `gtfs_version`, `agency_timezone`, `route_ids`,
  `policy_json` (policy đóng băng khi tạo run), `application_clock` + `clock_sequence` (LWT), `run_state`.
* `feed_current`: pointer tới generation đang phục vụ + `source_order`, `feed_timestamp`, `metrics_as_of`,
  `view_published_at`, `expires_at`, `is_partial`, `is_empty`.
* `snapshot_manifest`: `status` BUILDING/READY, tham số đóng băng, counts theo entity, `expected_counts`
  (`"<bảng>"` và `"<bảng>/<khóa partition>"`), `accounting_json`, lineage raw/Kafka, `publish_outcome`,
  `visibility_latency_ms`.
* `pipeline_status`: heartbeat, batch cuối, tổng messages/snapshots, lỗi cuối theo component.

## 5. Lý do chất lượng (reason codes)

Danh sách và severity/disposition mặc định: `src/transitpulse/contracts/enums.py` (`QUALITY_REASONS`). Bao gồm
đủ 20 mã bắt buộc của đặc tả (`INVALID_JSON`, `UNSUPPORTED_SCHEMA`, `INVALID_HEADER`,
`UNSUPPORTED_INCREMENTALITY`, `PAYLOAD_TOO_LARGE`, `MISSING_VEHICLE_ID`, `INVALID_COORDINATES`,
`MISSING_OBSERVATION_TIMESTAMP`, `FUTURE_TIMESTAMP`, `OBSERVATION_TIME_REGRESSION`, `UNKNOWN_ROUTE_OR_TRIP`,
`AMBIGUOUS_SERVICE_DATE`, `UNRESOLVED_STOP_SEQUENCE`, `NO_PREDICTION_DATA`, `SCHEDULE_ONLY_PREDICTION`,
`DEGRADED_PREDICTION`, `INVALID_PREDICTION`, `DUPLICATE_OBSERVATION`, `LATE_SNAPSHOT_NOT_PUBLISHED`,
`STATIC_VERSION_MISMATCH`) và các mã bổ sung (KPI exclusions, alert selectors, clock skew…).

Hạch toán đơn vị (kiểm tra trước READY):
```text
selected_entities            = fully_valid + partially_valid + rejected
valid_candidate_observations = unique_observations + duplicate_observations
membership (theo route, ID)  = fresh + stale + unknown_age + invalid_có_ID
candidate_predictions        = retained + rejected + not_applicable
trip_instances               = eligible + Σ excluded
```
