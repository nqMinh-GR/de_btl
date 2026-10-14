# Thời gian, đồng hồ và lịch GTFS

## 1. Các mốc thời gian (không được trộn lẫn)

| Trường | Ý nghĩa | Đồng hồ |
|---|---|---|
| `observation_time` | Thời điểm nguồn đo vị trí xe (`vehicle.timestamp`), có thể thiếu | nguồn |
| `feed_timestamp` | Thời điểm header của feed (epoch giây); không chứng minh xe vừa được đo | nguồn |
| `original_fetched_at` | Wall UTC lúc capture gốc; giữ nguyên khi replay | wall (lịch sử) |
| `published_at` | Wall UTC lúc enqueue trong run hiện tại; dùng đo transport/visibility latency | wall |
| `processed_at`, `view_published_at` | Wall UTC khi xử lý và lần đầu visible | wall |
| `application_clock` | Đồng hồ nghiệp vụ: wall UTC cho live; clock lịch sử theo `clock_tick` cho replay/synthetic | nghiệp vụ |
| `served_at` | Wall UTC lúc API trả response; không phải thời điểm visible đầu tiên | wall |

```text
pipeline_visibility_latency = view_published_at − published_at        (đo bằng wall clock của run hiện tại)
source_observation_age      = application_clock − observation_time    (không phải latency xử lý)
```
Duration nội bộ đo bằng `time.monotonic()`. Replay không so timestamp lịch sử với giờ máy để kết luận stale.

## 2. Application clock theo chế độ

* **live** (`clock_mode=wall`): application clock = giờ UTC hiện tại tại processor (khi build) và tại API (khi
  phục vụ). Feed ngừng cập nhật → tuổi tiếp tục tăng → xe chuyển stale dù không có message mới.
* **replay/synthetic** (`clock_mode=source`): publisher gửi `clock_tick` (có `clock_sequence` tăng dần) trước
  mỗi item tại đúng thời điểm của item, và mỗi 1 giây wall khi chờ. Processor chỉ nhận tick có
  `clock_sequence` lớn hơn **và** `source_clock` không lùi (LWT trên `run_metadata`). Tick tiếp tục qua đoạn mất
  dữ liệu để kiểm tra stale. Pause đóng băng clock nghiệp vụ, heartbeat wall vẫn chạy. Finish giữ clock cuối và
  không tự chuyển sang live. Không bao giờ lấy giờ dự báo làm clock.
* TTL Cassandra là retention theo **wall clock**, vẫn chạy khi replay đang pause (giới hạn đã công bố).

## 3. Chuyển giờ GTFS sang UTC

GTFS cho phép `HH:MM:SS` ≥ 24:00 (chuyến qua nửa đêm). Mốc ngày dịch vụ theo chuẩn GTFS là “trưa giờ địa
phương trừ 12 giờ”, đúng cả trong ngày đổi giờ mùa hè/mùa đông:

```text
gtfs_seconds       = HH*3600 + MM*60 + SS           (MM, SS trong 0..59; HH có thể ≥ 24)
service_anchor_utc = UTC(local_noon(service_date, agency_timezone)) − 12 h
scheduled_utc      = service_anchor_utc + gtfs_seconds
```
Spark: `to_utc_timestamp('<date> 12:00:00', tz)` trừ 43 200 s, **bắt buộc** `spark.sql.session.timeZone=UTC`
(và `TZ=UTC`), nếu không mọi giờ lịch bị lệch âm thầm. Python: `datetime.combine(d, time(12), ZoneInfo(tz))`.

Vector kiểm thử (America/New_York):

| Ngày dịch vụ | GTFS | UTC kỳ vọng | Ghi chú |
|---|---|---|---|
| 2026-10-05 | 08:05:00 | 2026-10-05T12:05:00Z | EDT (UTC−4) |
| 2026-10-05 | 25:10:00 | 2026-10-06T05:10:00Z | qua nửa đêm |
| 2026-03-08 | 01:30:00 | 2026-03-08T05:30:00Z | ngày đổi sang EDT; cộng vào nửa đêm “ngây thơ” sẽ sai 1 giờ |
| 2026-03-08 | 08:05:00 | 2026-03-08T12:05:00Z | |
| 2026-11-01 | 00:00:00 | 2026-11-01T05:00:00Z | ngày đổi về EST |
| 2026-11-01 | 08:05:00 | 2026-11-01T13:05:00Z | |

## 4. Trip instance và ngày dịch vụ

* Ưu tiên `start_date` của TripDescriptor; chỉ resolve nếu service của trip active ngày đó (calendar +
  calendar_dates). Không active → `unresolved`, không đoán.
* Thiếu `start_date`: xét ngày ứng viên L−1/L/L+1 (L = ngày địa phương của `metrics_as_of`) mà service active và
  clock nằm trong cửa sổ chuyến ± 12 giờ; đúng một ứng viên → `resolved (inferred)`, nhiều → `ambiguous`.
* Ambiguous/unresolved vẫn hiển thị giờ dự báo của nguồn, nhưng `predicted_delay_seconds = null` và bị loại khỏi KPI.
* Nối stop theo `stop_sequence`; chỉ dùng `stop_id` khi nó xuất hiện duy nhất trong trip (chuyến vòng qua cùng
  trạm hai lần → `UNRESOLVED_STOP_SEQUENCE`).

## 5. Sai lệch đồng hồ

Tolerance tương lai 5 giây: quan sát tương lai ≤ 5 s → fresh + cờ `CLOCK_SKEW`, tuổi hiển thị 0; > 5 s →
invalid `FUTURE_TIMESTAMP`. Timestamp > 1e11 → `INVALID_TIMESTAMP_UNIT` (nghi milli giây, không tự quy đổi).
