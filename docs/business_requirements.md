# Nghiệp vụ, người dùng và yêu cầu

Tài liệu này tóm tắt phần **Business and related data** của dự án TransitPulse MBTA và ánh xạ tới các
yêu cầu chức năng (FR) / phi chức năng (NFR) trong `system design full.pdf` (trang 3–5).

## 1. Bài toán

Feed giao thông công cộng thay đổi liên tục, có thể thiếu trường, trễ hoặc ngừng cập nhật. Việc chỉ hiển thị
response mới nhất không đủ để trả lời: *tọa độ này có mới không? dự báo này có đáng dùng không? dữ liệu đã
được xử lý đủ chưa?* TransitPulse giải quyết bằng dữ liệu có **phiên bản** (run, snapshot, generation),
**thời gian nguồn rõ ràng**, **kiểm tra chất lượng** và **truy vết** từ raw tới API.

## 2. Người dùng

| Vai trò | Nhu cầu | Thấy gì trong sản phẩm |
|---|---|---|
| Hành khách | Xe ở đâu, khi nào tới trạm, tuyến có gián đoạn không | Bản đồ xe theo tuyến, ETA theo trạm có nhãn chất lượng, service alerts |
| Người theo dõi vận hành | Tuyến nào có dự báo chậm, bao nhiêu xe có vị trí mới | Thẻ KPI “độ trễ dự kiến” kèm tử số/mẫu số và thời điểm tính |
| Data engineer | Nguồn còn hoạt động không, lỗi ở đâu, có backlog không | Trang chất lượng & pipeline, DLQ, raw reference, lineage |
| Giảng viên / người đánh giá | Dữ liệu có thật sự đi qua Kafka→Spark→Cassandra không, kết quả có đúng không | Golden smoke, test report, fault scenarios, benchmark có số đo thật |

## 3. Nguồn dữ liệu (chỉ MBTA)

| Nguồn | URL | Vai trò |
|---|---|---|
| GTFS Static | https://cdn.mbta.com/MBTA_GTFS.zip | Tuyến, trạm, chuyến, lịch, shape, timezone |
| Vehicle Positions | https://cdn.mbta.com/realtime/VehiclePositions.json | Vị trí xe, descriptor chuyến, thời điểm quan sát |
| Trip Updates | https://cdn.mbta.com/realtime/TripUpdates.json | Giờ dự báo đến/rời trạm, uncertainty |
| Service Alerts | https://cdn.mbta.com/realtime/Alerts.json | Cảnh báo dịch vụ, selector, thời hạn |

Quan sát thực tế (06/10/2026): TripUpdates **không có trường `delay`**, nên độ trễ dự kiến được tính bằng
giờ dự báo trừ giờ lịch GTFS của **cùng loại sự kiện** (arrival với arrival, departure với departure).
Không dùng MBTA V3 API, LAMP hay nguồn khác làm dependency; không thay MBTA bằng dữ liệu khác khi lỗi mạng.

## 4. Phạm vi MVP

* Năm tuyến bus (`route_type=3`) được **xác minh** từ GTFS + một mẫu realtime (`make list-routes`); không gán
  sẵn route ID trong tài liệu. Có thể mở rộng lên 10 tuyến sau khi đo tài nguyên.
* Ba feed realtime, một GTFS version bất biến cho mỗi run, recording mục tiêu 2 giờ, replay 1x/10x/50x.
* Dashboard năm trang (Streamlit local) và notebook UI năm tab (Colab), cùng gọi FastAPI.
* Ngoài phạm vi: mô hình ETA riêng, tối ưu hành trình, số hành khách, đánh giá độ chính xác ETA (chưa có ground
  truth), high availability (Kafka/Cassandra một node, RF1), triển khai nhiều vùng.

## 5. Các chỉ số và cách diễn giải

| Chỉ số | Định nghĩa | Không được hiểu là |
|---|---|---|
| `fresh_vehicle_count` | Số vehicle ID duy nhất có vị trí hợp lệ và tuổi quan sát ≤ 120 s tại application clock | Toàn bộ số xe đang vận hành của MBTA |
| `stale_vehicle_count` | Vehicle ID hợp lệ nhưng tuổi > 120 s | Xe đã ngừng chạy |
| `unknown_age_vehicle_count` | Vị trí hợp lệ nhưng thiếu timestamp quan sát | Xe “fresh” |
| `invalid_vehicle_count` | Số **entity** bị loại theo policy (đơn vị: entity) | Số xe hỏng |
| `predicted_late_rate` | `predicted_late_trip_count / eligible_trip_count`, mỗi trip instance một target stop, ngưỡng 300 s | Độ trễ thực tế / on-time performance chính thức của MBTA |
| `mean_predicted_delay_seconds` | Trung bình delay dự kiến của các chuyến đủ điều kiện (có thể âm = dự báo sớm) | Thời gian chờ của hành khách |

Mẫu số bằng 0 → tỷ lệ và trung bình là `null`, UI ghi “Chưa có mẫu đủ điều kiện”, không bao giờ hiển thị 0%.
Mọi KPI có `metrics_as_of`, tử số, mẫu số, policy version và trạng thái stale.

## 6. Yêu cầu chức năng (tóm tắt) và nơi hiện thực

| Mã | Yêu cầu | Thành phần |
|---|---|---|
| FR01 | Nhập GTFS ZIP, version = SHA-256, dimensions Parquet + metadata bất biến | `static/` |
| FR02 | Thu thập độc lập 3 feed, lưu raw + metadata trước khi publish; timeout không tạo empty snapshot | `ingestion/collector.py`, `archive.py` |
| FR03 | Snapshot qua Kafka có schema, checksum, ID ổn định; outbox bền vững | `ingestion/outbox.py`, `kafka_producer.py` |
| FR04 | Spark parse/validate/explode/join GTFS/dedup + quality reason + raw reference | `processing/` |
| FR05 | Vị trí theo current generation; fresh/stale/unknown/invalid; không cộng số xe qua nhiều snapshot | `processing/vehicles.py`, `api/freshness.py` |
| FR06 | Dự báo đến/rời trạm, uncertainty MBTA, predicted delay khi xác định được lịch | `processing/predictions.py`, `schedule.py` |
| FR07 | KPI tuyến: tối đa một target stop / trip instance, công bố numerator/denominator/exclusions/as-of | `processing/kpi.py` |
| FR08 | Alert selectors + thời hạn; tách MBTA alert với cảnh báo chất lượng pipeline | `processing/alerts.py`, API `/alerts` |
| FR09 | Lịch sử xe có paging; curated Parquet; báo cáo lịch sử bằng Spark batch | `vehicle_observations_by_day`, `processing/report_job.py` |
| FR10 | Generation bất biến; API chỉ đọc READY; duplicate/late/retry không làm tăng KPI hoặc rollback pointer | `processing/batch.py`, `storage/pointer.py` |
| FR11 | Replay đúng static version; 1x/10x/50x, pause/resume; source clock tách wall clock | `replay/` |
| FR12 | API + dashboard + quality/progress + golden/fault tests + benchmark có bằng chứng | `api/`, `app/`, `notebooks/`, `tests/`, `benchmarks/` |

Truy vết đầy đủ FR/NFR/IT/CT: `docs/traceability.md`.
