# Tổng hợp dự án TransitPulse MBTA và Data Business Requirements

Phiên bản 1.0 • Ngày 14 tháng 10 năm 2026 • Phạm vi 10 tuyến bus

Tài liệu này mô tả mục tiêu, dữ liệu, kiến trúc và tám yêu cầu nghiệp vụ dữ liệu của TransitPulse MBTA. Nội dung phục vụ viết báo cáo đồ án, trao đổi trong nhóm và xây dựng bằng chứng nghiệm thu. Trọng tâm là cách dữ liệu được hiểu, kiểm tra và đưa tới người dùng qua pipeline.

Phạm vi nghiệp vụ được chốt là 10 tuyến theo notebook hiện tại. Các BR dưới đây tổng hợp hành vi được mô tả trong thiết kế và hiện thực trong mã nguồn. Tiêu chí nghiệm thu là điều kiện cần kiểm chứng; sự tồn tại của code và test chưa chứng minh hệ thống đã đạt điều kiện đó.

## 1 Tổng quan dự án

TransitPulse thu thập dữ liệu xe buýt MBTA tại Boston, nối dữ liệu thời gian thực với lịch GTFS, rồi phục vụ vị trí xe, dự báo đến trạm, cảnh báo dịch vụ và KPI tuyến. Nhóm vận hành dữ liệu có thể xem chất lượng, trạng thái pipeline và truy vết kết quả về nguồn. [E01, E02]

| Người dùng | Câu hỏi cần trả lời | Kết quả sử dụng |
|---|---|---|
| Hành khách | Xe ở đâu, dự kiến khi nào đến, tuyến có gián đoạn không | Vị trí, ETA có nhãn chất lượng, service alerts |
| Người theo dõi vận hành | Bao nhiêu xe có quan sát mới, chuyến nào được dự báo chậm | KPI kèm mẫu số, thời điểm tính và phạm vi |
| Data engineer | Nguồn hay xử lý đang lỗi, backlog ở đâu | Quality reasons, heartbeat, offsets và lineage |
| Người đánh giá đồ án | Dữ liệu có đi qua công nghệ yêu cầu và phục hồi đúng không | API, golden fixture, fault tests và báo cáo đo |

ETA là dự báo do MBTA cung cấp, không phải mô hình dự báo riêng của dự án. Delay KPI đo chênh lệch giữa giờ dự báo và lịch GTFS; chưa phản ánh thời điểm xe thực tế đến trạm. Nguồn hiện có chưa đủ để nghiệm thu độ chính xác ETA bằng ground truth. [E01, E02, E09]

Repo được khảo sát tại commit `fe50c680569b5b4635ebbb0a8032ec6fc8367be8`. Báo cáo kiểm chứng đã lưu, ngày 07/10/2026, còn ghi NOT_RUN hoặc NOT_VERIFIED; chưa có báo cáo mới được xác minh trong lần lập tài liệu này. Cache pytest không được dùng làm bằng chứng PASS. [E16]

<!-- pagebreak -->

## 2 Phạm vi và luồng dữ liệu

Notebook cấu hình các route ID dạng chuỗi: `1`, `15`, `22`, `23`, `28`, `32`, `39`, `57`, `66`, `77`, với chế độ mặc định `live`. Đây là danh sách cấu hình. [TODO] Lưu kết quả xác minh cả 10 tuyến là bus trong GTFS version được chọn và có dữ liệu realtime phù hợp. Các ID `R_A` đến `R_E` thuộc dữ liệu synthetic, không đại diện tuyến MBTA thật. [E03, E04]

Một run giữ cố định scope, GTFS version và policy. Ba chế độ `live`, `replay`, `synthetic` dùng cùng đường xử lý và công bố nhãn nguồn. Phạm vi hiện tại không bao gồm tối ưu hành trình, đếm hành khách, đánh giá sai số ETA, high availability hay triển khai nhiều vùng. [E02, E05]

### Nguồn dữ liệu

| Nguồn MBTA | Dữ liệu nghiệp vụ | Địa chỉ |
|---|---|---|
| GTFS Static ZIP | Tuyến, trạm, chuyến, stop times, lịch, timezone và shape | https://cdn.mbta.com/MBTA_GTFS.zip |
| Vehicle Positions JSON | Vehicle ID, tọa độ, descriptor chuyến, timestamp quan sát | https://cdn.mbta.com/realtime/VehiclePositions.json |
| Trip Updates JSON | Sự kiện arrival hoặc departure, giờ dự báo, uncertainty | https://cdn.mbta.com/realtime/TripUpdates.json |
| Alerts JSON | Nội dung, nguyên nhân, ảnh hưởng, selector và khoảng hiệu lực | https://cdn.mbta.com/realtime/Alerts.json |

Đây là endpoint được cấu hình trong repo, không phải kết quả probe live ngày lập tài liệu. GTFS version là SHA-256 của ZIP; replay phải dùng đúng version gắn với recording. [E04, E05]

### Kiến trúc

GTFS ZIP → Spark batch → dimensions Parquet và static catalog Cassandra.

MBTA JSON → Collector → raw archive và durable outbox → Kafka → Spark Structured Streaming → Cassandra serving generations và curated Parquet → FastAPI → React, Streamlit hoặc notebook UI.

Recording → replay publisher → cùng Kafka, Spark, Cassandra và API. Lỗi dữ liệu được ghi bằng quality reasons và DLQ; lịch sử curated phục vụ Spark batch report. API đọc dữ liệu đã xử lý, không gọi MBTA để bỏ qua pipeline. [E06]

React hiện có sáu màn: tổng quan, bản đồ, trạm, cảnh báo, chất lượng và lịch sử xe. MapLibre sử dụng nền CARTO trên dữ liệu OpenStreetMap; khả năng xem nền bản đồ phụ thuộc mạng phía trình duyệt. [E17]

<!-- pagebreak -->

## 3 Hợp đồng dữ liệu và thời gian

| Đối tượng | Vai trò và khóa cần giữ |
|---|---|
| Run | Namespace xử lý; `run_id`, scope, mode, GTFS version, policy và clock |
| Snapshot | Trạng thái một feed; `snapshot_id`, checksum, raw reference và thứ tự nguồn |
| Generation | Bộ dữ liệu serving bất biến; `generation_id`, manifest BUILDING hoặc READY |
| Vehicle observation | Một quan sát xe; `vehicle_id`, `observation_id`, thời gian và tọa độ |
| Prediction | Sự kiện tại điểm dừng; trip descriptor, stop sequence và arrival hoặc departure |
| Alert | Cảnh báo dịch vụ; `alert_id`, selectors, active periods và confirmation status |

ID giữ dạng chuỗi. Instant lưu UTC và trả bằng ISO 8601; giá trị tùy chọn thiếu dùng `null`. `feed_timestamp` là lúc tạo feed; `observation_time` là lúc đo vị trí. Header mới không chứng minh GPS mới. [E05, E07; GTFS Realtime Reference]

Tuổi quan sát = `application_clock − observation_time`. Visibility latency = `view_published_at − published_at` của lần visible đầu tiên. Hai phép đo có mốc thời gian khác nhau; polling không nằm trong khoảng đo visibility latency. Live dùng wall clock; replay và synthetic dùng source clock do control tick điều khiển. Pause đóng băng source clock, nhưng retention Cassandra vẫn chạy theo wall clock. [E07, E15]

### Policy hiện có

| Quy tắc | Giá trị trong cấu hình cơ sở | Cách diễn giải |
|---|---|---|
| Chu kỳ poll mỗi feed | 30 giây | Lịch thu thập của dự án |
| Tuổi vị trí xe còn fresh | ≤ 120 giây | Dựa vào timestamp quan sát |
| Feed Trip Updates stale | > 90 giây | Dựa vào tuổi header |
| Feed Alerts stale | > 300 giây | Khác với thời hạn hiệu lực alert |
| Chuyến trễ dự kiến | Delay ≥ 300 giây | Policy KPI, không phải on time chính thức MBTA |
| Tolerance timestamp tương lai | 5 giây | Trong tolerance có CLOCK_SKEW |
| Retention serving và history | 1 giờ và 3 ngày | Theo wall clock |
| Mục tiêu visibility latency | p95 < 10 giây | Mục tiêu thiết kế chưa đo |

Giá trị hiệu lực phải đọc từ policy đã pin trong run, sau khi hợp nhất profile. GTFS Best Practices khuyến nghị tuổi VP/TU tối đa 90 giây và Alerts tối đa 10 phút; các ngưỡng riêng trong repo không phải SLA của MBTA. [E04, E08, E18; GTFS Realtime Best Practices]

<!-- pagebreak -->

## 4 BR01 Vị trí phương tiện

### Mục tiêu nghiệp vụ

Hành khách và người theo dõi vận hành cần biết xe nào có vị trí trong phạm vi 10 tuyến, tọa độ đó được đo lúc nào và có đủ chất lượng để sử dụng hay không. Số xe quan sát được chỉ mô tả dữ liệu trong snapshot đang phục vụ. [E02, E10]

### Yêu cầu dữ liệu và kết quả

Đầu vào là Vehicle Positions, cùng static scope để xác định tuyến và chuyến. Giữ `vehicle_id`, `route_id`, descriptor chuyến, latitude, longitude, `observation_time`, bearing và speed nếu nguồn có. Đầu ra là `vehicles_by_route_snapshot`, lịch sử `vehicle_observations_by_day` và `GET /api/v1/vehicles`. Mọi kết quả gắn run và snapshot. [E05, E10, E11]

### Quy tắc nghiệp vụ

- Đọc vị trí từ current generation READY; không cộng xe qua các snapshot để tạo số xe hiện tại.
- Tọa độ hoặc timestamp không hợp lệ có reason cụ thể. Thiếu timestamp quan sát giữ nhãn `unknown`, tuổi là `null`.
- Xe vắng trong snapshot mới không được tự mang sang danh sách fresh. Quan sát có timestamp lùi chỉ kế thừa theo policy và công bố `is_inherited`.
- Lịch sử khử trùng bằng `observation_id`. `invalid_vehicle_count` gồm hàng invalid có ID được phục vụ sau dedup, cộng entity invalid thiếu vehicle ID; không diễn giải thành toàn bộ raw entity lỗi hoặc số xe hỏng. [E10, E11, E15]

### Tiêu chí nghiệm thu

1. **AC01.1** Với hàng vị trí xe hợp lệ được chọn vào current generation, API trả đúng tọa độ và provenance; nếu quan sát bị lùi và kế thừa parent, provenance ghi rõ nguồn kế thừa.
2. **AC01.2** Khi snapshot mới không chứa xe đã thấy trước đó, xe không còn được tính fresh chỉ vì tồn tại trong history.
3. **AC01.3** Khi phát lại cùng quan sát, số hàng history có cùng `observation_id` không tăng.
4. **AC01.4** Golden S1 tại tuyến synthetic R_A phải có fresh 1, stale 1, unknown 1 và membership 3; đây là giá trị fixture, không phải số liệu MBTA. [E19]

### Truy vết và mức độ đáp ứng

Ánh xạ FR05, FR09, FR10. Code xử lý và API đã có. Test liên quan: `tests/spark/test_vehicles.py`, `tests/unit/test_api_history_paging.py`, `tests/integration/test_golden_flow.py`. [TODO] Thu bằng chứng chạy qua Kafka, Spark, Cassandra và API; chưa ghi nhận nghiệm thu PASS. [E10, E11, E19, E20]

<!-- pagebreak -->

## 5 BR02 ETA và dự báo tại trạm

### Mục tiêu nghiệp vụ

Hành khách cần xem giờ dự kiến đến hoặc rời trạm cùng tuyến, chuyến và chất lượng dự báo. Nhóm đánh giá cần xác nhận dự báo được gắn đúng ngày dịch vụ và sự kiện lịch để tránh báo chậm sai. [E02, E09]

### Yêu cầu dữ liệu và kết quả

Đầu vào gồm Trip Updates và GTFS stop times, calendar, calendar dates, timezone. Giữ trip descriptor, `stop_id`, `stop_sequence`, `event_type`, `predicted_at`, `scheduled_at`, uncertainty và `schedule_resolution`. Đầu ra là `predictions_by_stop_snapshot` và `GET /api/v1/stops/{stop_id}/predictions`. ETA hiển thị lấy từ nguồn MBTA. [E05, E09, E11]

### Quy tắc nghiệp vụ

- So arrival với lịch arrival, departure với lịch departure. Delay = predicted time trừ scheduled time khi trip và stop resolve được, STU là SCHEDULED, chuyến không canceled hoặc added và quality không schedule only hoặc invalid; giữ giá trị âm khi dự báo sớm. Degraded vẫn có thể có delay nhưng không eligible KPI.
- Lịch ambiguous hoặc unresolved có thể giữ giờ dự báo của nguồn, nhưng delay là `null` và loại khỏi KPI.
- Dùng stop sequence để phân biệt các lần ghé cùng trạm; giữ ngày dịch vụ, timezone và giờ GTFS lớn hơn 24 giờ khi resolve lịch.
- Adapter của dự án phân loại uncertainty: thiếu → unknown; 0 đến 299 → realtime; 300 → schedule only; 301 → degraded; âm hoặc >301 → invalid. Đây không phải cách định nghĩa thống kê chung của GTFS.
- API mặc định chỉ ẩn prediction quality invalid; một số dự báo bị loại khỏi KPI vẫn có thể được trả kèm nhãn. Cửa sổ hiển thị giữ lookback 60 giây. [E08, E09, E11]

### Tiêu chí nghiệm thu

1. **AC02.1** Khi arrival dự báo 12:11 và arrival lịch 12:05, delay trả về là 360 giây.
2. **AC02.2** Khi ngày dịch vụ ambiguous, giờ dự báo nguồn vẫn có thể hiển thị, còn scheduled time và delay là `null`.
3. **AC02.3** Khi uncertainty thiếu, API trả unknown và `null`, không tự đổi thành 0.
4. **AC02.4** Khi cùng stop ID lặp trong chuyến, hệ thống dùng stop sequence hoặc ghi reason unresolved, không chọn ngẫu nhiên. [E09, E19]

### Truy vết và mức độ đáp ứng

Ánh xạ FR01, FR06, FR07; test hiện có `tests/spark/test_predictions.py`, `tests/unit/test_schedule_resolution.py`, `tests/integration/test_scenarios.py`. [TODO] Kiểm chứng xử lý, chưa nghiệm thu sai số ETA vì ground truth và mô hình ETA riêng nằm ngoài phạm vi. [E09, E20]

<!-- pagebreak -->

## 6 BR03 Cảnh báo dịch vụ

### Mục tiêu nghiệp vụ

Hành khách cần nhận biết gián đoạn áp dụng cho tuyến, chuyến hoặc trạm đang xem. Nhóm vận hành cần phân biệt cảnh báo dịch vụ do MBTA phát hành với lỗi nguồn và lỗi pipeline. [E02, E12]

### Yêu cầu dữ liệu và kết quả

Đầu vào là Alerts và static scope. Giữ alert ID, cause, effect, severity, nội dung, `informed_entity`, `active_period`, first seen và confirmation status. Đầu ra là `alerts_by_route_snapshot` và `GET /api/v1/alerts`. API gộp theo alert ID khi cảnh báo khớp nhiều tuyến. [E05, E11, E12]

### Quy tắc nghiệp vụ

- Các trường trong cùng selector phải cùng khớp; chỉ cần một trong các selector của alert khớp để áp dụng. Selector không resolve được có reason, không gán ngẫu nhiên sang tuyến khác.
- Khoảng hiệu lực theo điều kiện `start ≤ clock < end`. API tính lại ALWAYS, ACTIVE, UPCOMING hoặc EXPIRED tại thời điểm phục vụ.
- Alert vắng trong snapshot đầy đủ có thể được đóng. Nếu snapshot partial do entity lỗi và parent còn available, đọc được trong giới hạn, kế thừa alert parent vắng mặt với nhãn `UNCONFIRMED_INHERITED`.
- Freshness của feed không thay thế active period. Cảnh báo chất lượng pipeline có category riêng, không được trình bày như thông báo dịch vụ MBTA. [E08, E12]

### Tiêu chí nghiệm thu

1. **AC03.1** Với selector gồm route A và stop X, khi tuyến khác đi qua X, alert không tự áp dụng cho tuyến đó nếu route A không khớp.
2. **AC03.2** Với khoảng [12:00, 13:00), trạng thái ACTIVE tại 12:00 và không còn ACTIVE tại 13:00, nếu không có khoảng khác còn hiệu lực.
3. **AC03.3** Khi snapshot partial bỏ sót alert parent còn available và đọc được, alert được giữ với nhãn chưa xác nhận; snapshot đầy đủ vắng alert thì không phục vụ alert đó như current.
4. **AC03.4** Khi một alert khớp nhiều tuyến, API tổng hợp không lặp cùng alert ID; warning pipeline giữ category riêng. [E12, E19]

### Truy vết và mức độ đáp ứng

Ánh xạ FR08 và FR10; test `tests/spark/test_alerts.py`, `tests/unit/test_ingestion_scope.py`, `tests/integration/test_scenarios.py`. Code selector, active state và partial inheritance đã có. [TODO] Lưu kết quả chạy selector, biên thời gian và kế thừa; không suy ra PASS từ mã nguồn. [E12, E20]

<!-- pagebreak -->

## 7 BR04 Độ mới của dữ liệu

### Mục tiêu nghiệp vụ

Người dùng cần biết dữ liệu còn đủ mới để hỗ trợ quyết định. Khi nguồn ngừng cập nhật, vị trí cũ phải chuyển stale; replay lịch sử phải được đánh giá bằng clock lịch sử thay vì giờ máy hiện tại. [E07, E08]

### Yêu cầu dữ liệu và kết quả

Giữ timestamp quan sát, header timestamp, application clock, clock mode, policy và thời điểm served. API công bố tuổi, trạng thái fresh, stale, unknown hoặc invalid; feed metadata công bố staleness và threshold. Generation vẫn có thể tồn tại trong retention dù nội dung đã stale. [E05, E07, E11]

### Quy tắc nghiệp vụ

- Vị trí hợp lệ có tuổi ≤120 giây là fresh; >120 giây là stale. Thiếu observation timestamp là unknown.
- Quan sát tương lai ≤5 giây mang CLOCK_SKEW và tuổi hiển thị 0; vượt tolerance là invalid. Không dùng header làm timestamp GPS thay thế.
- API tính lại freshness tại mỗi request. Feed TU stale khi tuổi header >90 giây; Alerts >300 giây.
- Live dùng wall clock. Replay pause giữ source clock, trong khi tuổi heartbeat vận hành và TTL vẫn dựa wall clock. [E07, E08, E11]

### Tiêu chí nghiệm thu

1. **AC04.1** Với quan sát lúc 12:00:00, API đánh giá fresh tại 12:02:00 và stale tại 12:02:01 dù không nhận snapshot mới.
2. **AC04.2** Khi header mới nhưng observation timestamp thiếu, freshness là unknown và age là `null`.
3. **AC04.3** Với quan sát tương lai 4 giây, trạng thái fresh có CLOCK_SKEW; tương lai 6 giây là invalid theo policy 5 giây.
4. **AC04.4** Khi pause replay, tuổi nghiệp vụ giữ nguyên; khi tiến source clock qua ngưỡng, trạng thái chuyển stale. Hết TTL, endpoint yêu cầu feed đó trả `generation_expired`. [E08, E15, E19]

### Truy vết và mức độ đáp ứng

Ánh xạ FR05, FR11, FR12; test `tests/unit/test_policy.py`, `tests/unit/test_api_endpoints.py`, `tests/spark/test_batch_control_fold.py`, `tests/integration/test_generation_ttl.py`. Classifier và source clock đã có. [TODO] Thu bằng chứng API thay đổi freshness khi không có message mới và khi pause replay. [E08, E11, E20]

<!-- pagebreak -->

## 8 BR05 KPI độ trễ dự kiến

### Mục tiêu nghiệp vụ

Người theo dõi vận hành cần so sánh mức chậm được dự báo giữa các tuyến và biết bao nhiêu chuyến đủ điều kiện để kết luận. KPI phải tránh đếm nhiều lần một chuyến có nhiều điểm dừng hoặc nhiều loại sự kiện. [E02, E13]

### Yêu cầu dữ liệu và kết quả

Đầu vào là prediction đã nối đúng lịch GTFS. Đơn vị phân tích là trip instance, không phải từng prediction. Đầu ra `route_metrics_by_snapshot` phục vụ `GET /api/v1/routes/{route_id}/metrics`: eligible count, late count, late rate, mean delay, exclusions, unknown uncertainty count, metrics as of và policy version. [E05, E11, E13]

### Quy tắc nghiệp vụ và công thức

Mỗi trip instance lấy tối đa một target stop: sự kiện còn ở tương lai, stop đã resolve, không SKIPPED hoặc NO_DATA, stop sequence nhỏ nhất; ưu tiên arrival khi có thời gian sử dụng được, nếu thiếu thì departure. Đánh giá eligibility trên target đó, không chuyển sang stop sau để né target có quality kém. [E13]

`predicted_late_trip_count` = số trip eligible có target delay ≥300 giây.

`predicted_late_rate` = late count / eligible count. Mean delay là trung bình delay của target eligible, giữ cả giá trị âm. Khi eligible count bằng 0, rate và mean là `null`.

Chuyến canceled, added, lịch unresolved hoặc ambiguous, không có target, schedule only, degraded, invalid hoặc thiếu scheduled event bị loại theo reason ưu tiên. Code hiện loại trip nếu có bất kỳ candidate sự kiện target tương lai chưa resolve được stop, kể cả khi còn một target khác resolve được. Unknown uncertainty có thể eligible nhưng được đếm riêng. Giá trị KPI giữ thời điểm tính của generation; khi feed stale, API công bố stale thay vì coi đó là KPI mới. [E08, E13]

### Tiêu chí nghiệm thu

1. **AC05.1** Golden R_A có target delay 360 và 60 giây: eligible 2, late 1, rate 0,5 và mean 210 giây.
2. **AC05.2** Với eligible count 0, API trả `null` cho rate và mean; UI diễn giải chưa có mẫu đủ điều kiện.
3. **AC05.3** Với một trip có nhiều prediction arrival, departure và nhiều stop, mẫu số chỉ tăng tối đa 1.
4. **AC05.4** Target delay đúng 300 giây được tính late; target không eligible có reason, không đóng góp tử số hoặc mẫu số. [E13, E19]

### Truy vết và mức độ đáp ứng

Ánh xạ FR06, FR07; test `tests/spark/test_kpi.py`, `tests/unit/test_api_endpoints.py`, `tests/integration/test_golden_flow.py`. [TODO] Lưu kết quả đối chiếu oracle và API; không diễn giải KPI này thành độ trễ thực tế hoặc tỷ lệ đúng giờ chính thức của MBTA. [E13, E20]

<!-- pagebreak -->

## 9 BR06 Chất lượng dữ liệu và truy vết

### Mục tiêu nghiệp vụ

Data engineer và người đánh giá cần giải thích vì sao bản ghi được giữ, loại khỏi KPI, cách ly hay khử trùng. Người dùng cần biết dữ liệu thiếu hoặc không hợp lệ để không coi trạng thái lỗi là kết quả bình thường. [E02, E14]

### Yêu cầu dữ liệu và kết quả

Đầu vào gồm raw payload, metadata capture, envelope, static version và các record chuẩn hóa. Đầu ra là `quality_by_snapshot`, DLQ, manifest accounting và `GET /api/v1/quality`. Reason đi kèm severity, disposition, đơn vị đếm và sample references. Lineage giữ run, scope, version, snapshot, checksum và raw reference. [E05, E14, E15]

### Quy tắc nghiệp vụ

- Phân biệt `fully_valid`, `partially_valid`, `rejected` và trạng thái snapshot partial. Một record có nhiều warning nên không cộng warning thành tỷ lệ lỗi.
- Hạch toán cùng đơn vị: selected entities = fully valid + partially valid + rejected; trip instances = eligible + tổng excluded.
- Invalid count có thể gồm entity thiếu ID, còn membership là vehicle ID hiện diện; không áp dụng phép cộng giữa hai đơn vị này một cách máy móc.
- JSON/schema/header/incrementality không hỗ trợ, tọa độ sai, timestamp sai đơn vị, lịch ambiguous và duplicate đều có reason đăng ký.
- DLQ giữ excerpt có giới hạn và raw reference. Envelope quá 4 MiB giữ raw và báo PAYLOAD_TOO_LARGE, không cắt bớt entity để publish. [E04, E10, E14]

### Tiêu chí nghiệm thu

1. **AC06.1** Với entity có tọa độ sai, quality có reason và reference tương ứng; không coi nó là vị trí hợp lệ.
2. **AC06.2** Khi một record có hai warning, báo cáo không dùng tổng warning làm số record lỗi duy nhất.
3. **AC06.3** Với snapshot partial, tổng fully valid, partially valid và rejected khớp selected entity count trước publish READY.
4. **AC06.4** Từ một generation serving, truy được manifest và raw checksum; input sai static version bị từ chối hoặc báo lỗi theo contract. [E14, E15, E19]

### Truy vết và mức độ đáp ứng

Ánh xạ FR02, FR03, FR04, FR10, NFR05; test `tests/spark/test_parse.py`, `tests/spark/test_batch_lineage.py`, `tests/integration/test_payload_boundary.py`. [TODO] Thu DLQ, accounting và lineage artifacts từ run thật. Chưa đặt một tỷ lệ data quality đạt chuẩn nếu chưa có baseline đo. [E14, E20]

<!-- pagebreak -->

## 10 BR07 Trạng thái pipeline

### Mục tiêu nghiệp vụ

Data engineer cần phân biệt lỗi nguồn, Kafka backlog, lỗi xử lý và lỗi serving để chọn cách xử lý sự cố. Người đánh giá cần biết dữ liệu đã tới đâu, không kết luận pipeline khỏe chỉ từ việc một process còn chạy. [E02, E18]

### Yêu cầu dữ liệu và kết quả

Giữ component status, heartbeat, batch ID, progress, lỗi cuối, pointer từng feed, clock và run state. `GET /api/v1/pipeline/status` trả generations, components, backlog và latency khi đã đo. `/health/live` và `/health/ready` là các tín hiệu kiểm tra riêng; readiness API không thay thế kiểm chứng freshness đầu cuối. [E11, E18]

### Quy tắc nghiệp vụ

- Tuổi heartbeat = served time trừ last heartbeat theo wall clock, độc lập với source clock replay.
- Backlog tính từ latest offset trừ end offset theo topic và partition khi có số đo; offset chưa biết giữ `null`.
- Latency là số đo visibility theo feed; không có phép đo thì trả `null`, không giả lập 0.
- Khi HTTP timeout, không phát snapshot rỗng giả. View trước còn hạn có thể tiếp tục phục vụ với warning và tuổi tăng.
- Công bố run CREATED, RUNNING, PAUSED, FINISHED hoặc FAILED và trạng thái từng feed; ba feed không có transaction nguyên tử chung. [E15, E18]

### Tiêu chí nghiệm thu

1. **AC07.1** Khi một nguồn timeout, status ghi lỗi nguồn; dữ liệu current không bị thay bằng snapshot rỗng do lỗi fetch.
2. **AC07.2** Khi end offset 100 và latest offset 120, backlog đo cho partition là 20 message; thiếu một offset thì không báo 0.
3. **AC07.3** Khi replay pause, source clock không tiến nhưng heartbeat wall vẫn có thể đổi; API thể hiện hai loại thời gian đúng nghĩa.
4. **AC07.4** Khi visibility latency chưa được đo, trường latency không được trình bày như giá trị đã đạt mục tiêu p95 dưới 10 giây. [E18]

### Truy vết và mức độ đáp ứng

Ánh xạ FR12, NFR03, NFR07; code progress listener, metrics và status API đã có. Test `tests/unit/test_api_endpoints.py`, `tests/integration/test_source_faults.py`; benchmark có harness. [TODO] Lưu mẫu progress và phép đo latency, throughput, backlog. Chưa có RTO hoặc SLO cảnh báo vận hành được nghiệm thu bằng số đo. [E18, E20]

<!-- pagebreak -->

## 11 BR08 Replay và phục hồi

### Mục tiêu nghiệp vụ

Nhóm phát triển cần tái hiện một luồng dữ liệu để demo, kiểm tra lỗi và phục hồi sau gián đoạn. Kết quả sau retry phải không cộng trùng KPI, không lùi trạng thái current và không lộ generation đang ghi dở. [E15]

### Yêu cầu dữ liệu và kết quả

Recording giữ manifest, timeline, raw payload, checksum và static version. Replay hỗ trợ 1x, 10x, 50x, pause, resume và source clock; mỗi run có namespace riêng. Phục hồi sử dụng outbox, Spark checkpoint, manifest, immutable generation và pointer CAS. [E07, E15]

### Quy tắc nghiệp vụ

- Kiểm tra static version và checksum trước replay; không join recording cũ với ZIP mới tải.
- Ghi raw bền vững và outbox trước publish; chỉ đánh dấu published sau ACK. Bảo đảm của dự án là at least once với ghi và publish idempotent.
- Generation BUILDING không được phục vụ. Chỉ READY có đủ expected counts mới được publish qua pointer; pointer chỉ tiến theo source order.
- Snapshot cũ tới muộn giữ audit hoặc history theo protocol nhưng không rollback pointer. Retry giữ tham số build và lần first visibility đã lưu.
- Nếu offsets đã mất vì retention hoặc hạ tầng được dựng lại rỗng, replay vào run mới; không tái dùng checkpoint cũ để giả vờ phục hồi đủ. TTL vẫn chạy khi pause. [E15]

### Tiêu chí nghiệm thu

1. **AC08.1** Khi phát cùng snapshot hai lần, logical serving output và history không tăng trùng; counters delivery có thể phản ánh việc gửi lặp.
2. **AC08.2** Khi snapshot cũ đến sau snapshot mới, current pointer và vị trí phục vụ không lùi.
3. **AC08.3** Khi crash sau partial write và generation cũ còn TTL, API tiếp tục thấy generation cũ; nếu offsets và parent data cần thiết còn giữ, restart cùng checkpoint hoàn tất đúng generation, đối chiếu oracle. Khi input phục hồi đã mất, xử lý theo runbook để tạo run mới.
4. **AC08.4** Khi crash sau CAS nhưng trước checkpoint commit, retry không đổi first visibility hoặc gia hạn expiry ngầm; khi static mismatch, replay từ chối input. [E15, E19]

### Truy vết và mức độ đáp ứng

Ánh xạ FR03, FR10, FR11, NFR02, NFR06; test `tests/unit/test_replay_verify.py`, `tests/integration/test_recovery.py`, `tests/integration/test_outbox_resume.py`, `tests/integration/test_zz_cassandra_down.py`. [TODO] Recording MBTA thật, fault recovery report và thời gian phục hồi đo được. Kafka và Cassandra RF1 không bảo đảm chịu mất host; chưa có RPO hoặc RTO đã nghiệm thu. [E15, E16, E20]

<!-- pagebreak -->

## 12 Ma trận nghiệm thu và thứ tự kiểm chứng

Các BR có liên hệ dữ liệu: static scope và lịch hỗ trợ BR01, BR02, BR03; BR04 xác định tuổi; BR02 là đầu vào BR05; BR06, BR07 và BR08 chứng minh tính đúng và khả năng vận hành của toàn bộ luồng.

| BR | FR hoặc NFR chính | Bằng chứng cần lưu |
|---|---|---|
| BR01 Vehicle position | FR05, FR09, FR10 | API snapshot, history và kết quả duplicate |
| BR02 ETA | FR01, FR06 | Prediction, lịch resolve và quality labels |
| BR03 Service alerts | FR08, FR10 | Selector, biên hiệu lực, partial inheritance |
| BR04 Freshness | FR05, FR11 | Response trước và sau ngưỡng, clock tick |
| BR05 Delay KPI | FR06, FR07 | Numerator, denominator, exclusions và oracle |
| BR06 Data quality | FR02 đến FR04, NFR05 | Quality, DLQ, accounting và raw lineage |
| BR07 Pipeline status | FR12, NFR03, NFR07 | Progress, heartbeat, offsets, latency measured |
| BR08 Replay recovery | FR03, FR10, FR11, NFR02 | Recording, fault report, checkpoint và pointer |

### Kế hoạch kiểm chứng

1. Xác minh 10 route IDs với static version và realtime; lưu source probe và scope report.
2. Chạy unit và runtime tests, sau đó Spark tests cho rules, lịch và target selection. Test Spark gắn mã IT vẫn là test transformation nếu chưa chạy hạ tầng tích hợp.
3. Chạy golden smoke qua Kafka, Spark, Cassandra và API; đối chiếu giá trị kỳ vọng synthetic, ghi rõ provenance.
4. Chạy integration và fault tests cho lỗi nguồn, duplicate, late arrival, partial write, after CAS, Cassandra down và expiry.
5. Capture recording thật, replay và benchmark ở phạm vi 10 tuyến; lưu thông số máy, profile, policy, commit, phiên bản và measurement window. [E19, E20]

Một BR chỉ được ghi đạt sau khi tiêu chí liên quan có kết quả và artifact kiểm chứng. Target p95 <10 giây đo từ enqueue tới first visibility, không bao gồm chờ poll hay thời gian nguồn chưa công bố. UI đẹp hoặc tiến trình chạy thành công không thay thế bằng chứng dữ liệu. [E15, E18, E20]

<!-- pagebreak -->

## 13 Hiện trạng codebase và khác biệt với thiết kế

| Mảng | Những gì có thể xác nhận từ repo |
|---|---|
| Stack | Python 3.11, PySpark 3.5.7, Kafka 4.2.2, Cassandra 5.0.9, FastAPI; React và Vite cho console |
| Tổ chức | contracts, static, ingestion, replay, processing, storage, api, runtime và obs |
| Lưu trữ | Cassandra theo truy vấn; static và curated Parquet; raw archive và SQLite outbox |
| API | Response data, meta, warnings; giới hạn truy vấn và history paging |
| Kiểm thử | Unit, runtime, Spark, integration, oracle độc lập và golden fixture |
| Vận hành | Native supervisor, Docker Compose, Colab notebook và benchmark harness |

Phiên bản trên là phiên bản khai báo hoặc pin trong repo, chưa phải xác nhận runtime đã cài và chạy thành công. Tài liệu chi tiết cho bảy mảng nằm trong `docs/codebase/`. [E06, E11, E20, E21]

### Khác biệt cần phản ánh trong báo cáo

- Thiết kế PDF và một số docs mô tả MVP 5 tuyến; notebook hiện cấu hình 10 tuyến và người dùng đã chốt phạm vi 10 tuyến.
- README nói mặc định synthetic, nhưng cell cấu hình notebook hiện đặt live. Báo cáo phải ghi đúng mode của run nghiệm thu.
- Tài liệu cũ mô tả năm trang và không có nền bản đồ; frontend hiện có sáu màn, MapLibre và nền CARTO. Vì vậy cần phân biệt replay dữ liệu offline với khả năng tải basemap offline.
- Thiết kế PDF dùng cấu hình tài nguyên tham chiếu khoảng 16 GiB; `config/colab.yaml` hiện hướng tới Colab high RAM khoảng 51 GiB và đặt Spark driver 16g, Kafka heap 1g, Cassandra heap 4G cùng direct memory 2G. Đây là cấu hình dự kiến, chưa phải số đo sử dụng RAM.
- `uv.lock` chưa có trong danh sách file khảo sát; `frontend/package-lock.json` đã có. Báo cáo ngày 07/10 chưa phải bằng chứng chạy ở commit hiện tại. [E01, E03, E16, E17, E21]

Hai điểm UI cần kiểm chứng thêm qua chạy thử: chu kỳ refresh toàn cục không gọi lại predictions, trong khi effect ở màn trạm không theo dõi generation hoặc clock, nên ETA đang mở có thể giữ payload cũ. Màn trạm cũng hiển thị uncertainty bằng ký hiệu ± giây; các giá trị MBTA bus 300 và 301 có ngữ nghĩa riêng, không phải khoảng tin cậy thống kê. Đây là phát hiện từ rà soát tĩnh, chưa phải lỗi đã tái hiện runtime. [E22; MBTA GTFS Realtime Documentation]

### Việc còn cần bổ sung

[TODO] Xác minh 10 tuyến; lưu recording thật; chạy các kiểm thử và đo benchmark; ghi kết quả cho cả 32 tiêu chí; kiểm tra capacity và giới hạn fanout của đúng profile sử dụng. Profile Colab cho phép API fanout 10 tuyến, cấu hình cơ sở là 5; phạm vi 10 tuyến phải được kiểm chứng trên cấu hình hiệu lực. [E04]

[ASK USER] Q1 Môi trường nghiệm thu chính là Google Colab hay Linux local native? Lựa chọn này xác định profile và điều kiện tài nguyên của kế hoạch kiểm chứng, không thay đổi định nghĩa tám BR.

<!-- pagebreak -->

## 14 Bằng chứng và tài liệu tham chiếu

Các mã E liên kết đến file trong repo. Bảng liên kết BR với FR là tổng hợp của tài liệu này; FR và các quy tắc nguồn được truy vết qua tài liệu hiện có, code và tests. Tests được nêu là vị trí kiểm chứng, không phải kết quả chạy.

- E01 [README và mục tiêu dự án](../README.md)
- E02 [Nghiệp vụ hiện có](business_requirements.md) và [Thiết kế hệ thống PDF](../system%20design%20full.pdf) trang 3 đến 5 và 17 đến 28
- E03 [Notebook cấu hình phạm vi và chế độ](../notebooks/TransitPulse_MBTA_Colab_Demo.ipynb)
- E04 [Cấu hình cơ sở](../config/base.yaml) và [Override Colab](../config/colab.yaml)
- E05 [Data dictionary](data_dictionary.md), [Schema Cassandra](../src/transitpulse/storage/migrations/0001_initial.cql) và [Định danh](../src/transitpulse/contracts/ids.py)
- E06 [Kiến trúc mô tả](architecture.md) và [Streaming job](../src/transitpulse/processing/stream_job.py)
- E07 [Clock và thời gian GTFS](clock_and_time.md), [GTFS time](../src/transitpulse/static/gtfs_time.py), [Control clock](../src/transitpulse/processing/control_clock.py)
- E08 [Policy classifiers](../src/transitpulse/contracts/policy.py) và [Quality reason registry](../src/transitpulse/contracts/enums.py)
- E09 [Predictions](../src/transitpulse/processing/predictions.py) và [Schedule resolution](../src/transitpulse/processing/schedule.py)
- E10 [Vehicle processing](../src/transitpulse/processing/vehicles.py) và [Serving freshness](../src/transitpulse/api/freshness.py)
- E11 [Query mapping](query_mapping.md), [API routers](../src/transitpulse/api/routers), [API context](../src/transitpulse/api/context.py)
- E12 [Alert processing](../src/transitpulse/processing/alerts.py) và [Alerts API](../src/transitpulse/api/routers/alerts.py)
- E13 [Target selection và route KPI](../src/transitpulse/processing/kpi.py)
- E14 [Quality processing](../src/transitpulse/processing/quality.py), [DLQ](../src/transitpulse/processing/dlq.py), [Quality API](../src/transitpulse/api/routers/quality.py)
- E15 [Publish protocol](publish_protocol.md), [Batch processing](../src/transitpulse/processing/batch.py), [Manifest](../src/transitpulse/storage/manifest.py), [Pointer CAS](../src/transitpulse/storage/pointer.py), [Outbox](../src/transitpulse/ingestion/outbox.py)
- E16 [Báo cáo kiểm chứng đã lưu](../reports/environment_verification.json) và [Giới hạn thiết kế](limitations.md)
- E17 [Frontend README](../frontend/README.md), [React App](../frontend/src/App.jsx), [Map component](../frontend/src/components/TransitMap.jsx)
- E18 [Pipeline API](../src/transitpulse/api/routers/pipeline.py), [Runbook](runbook.md) và [Benchmark phương pháp đo](../benchmarks/README.md)
- E19 [Golden expected values](../tests/fixtures/golden/expected.json) và [Synthetic provenance](../tests/fixtures/golden/PROVENANCE.md)
- E20 [Traceability](traceability.md), [Spark tests](../tests/spark), [Integration tests](../tests/integration) và [Oracle độc lập](../tests/oracle)
- E21 [Python manifest](../pyproject.toml), [Runtime pins](../deploy/runtime-manifest.json), [Frontend manifest](../frontend/package.json) và [Codebase scan](codebase-scan.txt)
- E22 [React polling](../frontend/src/AppContext.jsx) và [Stops view](../frontend/src/views/StopsView.jsx)

### Nguồn chuẩn GTFS

- [GTFS Realtime Reference](https://gtfs.org/documentation/realtime/reference/) cho timestamp, trạng thái và semantics của feed
- [GTFS Realtime Best Practices](https://gtfs.org/documentation/realtime/realtime-best-practices/) cho khuyến nghị độ mới dữ liệu
- [GTFS Schedule Reference](https://gtfs.org/documentation/schedule/reference/) cho ngày dịch vụ, timezone và stop times
- [MBTA GTFS Realtime Documentation](https://raw.githubusercontent.com/mbta/gtfs-documentation/master/reference/gtfs-realtime.md) cho ngữ nghĩa uncertainty riêng của MBTA bus

Các nguồn chuẩn được tham khảo ngày 14/10/2026. Policy của dự án và bằng chứng runtime phải được trình bày riêng với khuyến nghị chuẩn.
