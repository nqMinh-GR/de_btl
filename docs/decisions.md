# Quyết định thiết kế (Architecture Decision Log)

Mỗi mục: mã, quyết định, lý do. Các mốc (polling, stale, late, TTL, budget, p95) là **policy của dự án**,
không phải cam kết của MBTA hay chuẩn mặc định của công nghệ. Mọi thay đổi schema/ID/clock/publish protocol
phải cập nhật fixtures, CQL, API, tài liệu và test cùng lúc.

## 0. Môi trường và quy trình

| Mã | Quyết định | Lý do |
|---|---|---|
| D-ENV-01 | Repo root là thư mục `DE_BTL/` (hai tài liệu đặc tả nằm cùng chỗ). Không dùng git trong quá trình build; người dùng tự push. | Yêu cầu của người dùng. |
| D-ENV-02 | Không thực thi bất kỳ code/test nào trên máy build; mọi trạng thái verification ban đầu là `NOT_RUN`/`NOT_VERIFIED`. Lần chạy đầu là trên Google Colab. | Yêu cầu của người dùng; không bịa PASS. |
| D-ENV-03 | `pyproject.toml` pin `==` cho dependency trực tiếp; `uv.lock` được sinh ở lần bootstrap Colab đầu tiên nếu chưa có (`lock_origin=generated_on_colab`), xuất trong evidence ZIP để commit lại. | Không thể chạy `uv lock` khi build; vẫn tái lập được sau lần chạy đầu. |
| D-ENV-04 | Python core 3.11.17 (bản 3.11 mới nhất mà uv 0.12.23 cung cấp), JDK Temurin 17.0.20.1+1, Spark/PySpark 3.5.7 (Scala 2.12), Kafka 4.2.2 KRaft, Cassandra 5.0.9. | Bộ tham chiếu của đặc tả; Python 3.12 không có `asyncore` cần cho cassandra-driver nếu thiếu libev. |
| D-ENV-05 | Checksum artifact không được ghi cứng nếu chưa kiểm chứng: manifest để `value: null` và lấy file checksum chính thức cùng origin khi bootstrap; kết quả `VERIFIED` chỉ ghi trong `bootstrap_report.json`. | Không bịa checksum. |
| D-ENV-06 | Kafka/Cassandra có URL dự phòng `archive.apache.org` vì `downloads.apache.org` gỡ bản cũ. | Tránh hỏng bootstrap khi có bản mới. |

## 1. Định danh và thứ tự

| Mã | Quyết định | Lý do |
|---|---|---|
| D-ID-01 | `H(s)=sha256[:32]`; `K()` nối bằng `\x1f`, null → `\x00`; số thực đổi sang số nguyên cố định (`e7` lat/lon, `e2` bearing, `e3` speed) trước khi hash. | Python và Spark cho cùng kết quả; tránh `concat_ws` bỏ null gây trùng ID. |
| D-ID-02 | `snapshot_id = sn_ + H(run, scope, feed, header.ts, sha256(canonical header + entities sắp theo canonical))`. Không chứa `published_at`, `capture_id`, UUID retry. | Retry cùng payload logic → cùng ID (spec §6.2). |
| D-ID-03 | `observation_id` không chứa run/snapshot/capture; xe thiếu timestamp → `snapshot_state_sample` (`ss_`) dùng header timestamp làm thời điểm hiệu dụng, `freshness_known=false`. | Poll lại cùng quan sát không tạo lịch sử mới. |
| D-ID-04 | `prediction_id` chỉ dùng trường nguồn (trip, start_date, start_time, route, direction, stop_sequence, stop_id, event_type); thiếu `trip_id` → quarantine, không bịa khóa. | Spec §12.2. |
| D-ID-05 | `source_order = %012d.%017d.%s` (feed_ts, capture µs, snapshot_id); so sánh chuỗi = so tuple; đóng băng trong manifest ở lần build đầu. | Không dùng arrival order hay thời điểm ghi Cassandra. |
| D-ID-06 | `capture_id = <vp|tu|al>-<UTC µs>Z-<sha256[:12]>`, giữ nguyên khi replay. | Truy vết raw. |

## 2. Giao thức publish và phục hồi

| Mã | Quyết định | Lý do |
|---|---|---|
| D-PUB-01 | Manifest `BUILDING` tạo bằng `INSERT IF NOT EXISTS`, đóng băng parent, `metrics_as_of`, `expires_at`, `history_expires_at`; retry dùng lại. | Retry cho cùng kết quả logic (PDF p13-14). |
| D-PUB-02 | Parent = max(pointer hiện tại, snapshot không-trễ trước đó của cùng feed trong cùng micro-batch). | Xử lý nhiều snapshot/feed trong một batch mà vẫn tất định. |
| D-PUB-03 | Ghi bằng `mapPartitions` trả số dòng theo bảng; so với expected trước khi `READY`; pointer CAS (`IF snapshot_id = <đã đọc>`) tối đa 10 lần; pointer bằng snapshot → thành công idempotent; pointer mới hơn → `LATE_SNAPSHOT_NOT_PUBLISHED`. | Visibility chỉ sau READY; không rollback. |
| D-PUB-04 | Không set `maxOffsetsPerTrigger`; giới hạn bằng `MAX_SNAPSHOTS_PER_BATCH=3000` (raise khi vượt). | Giới hạn theo partition có thể tách clock tick khỏi snapshot phát sau nó. |
| D-PUB-05 | Bảo đảm của MVP: at-least-once delivery + ghi/publish idempotent; không exactly-once xuyên Kafka/Parquet/Cassandra. | Spec §10.4, PDF p14. |
| D-PUB-06 | Fault injection bằng arm file one-shot `faults/<run>/<point>.json`, `os._exit(86)`, chỉ khi processor chạy với `--enable-fault-injection`. Processor không tự restart; `demo.py restart-processor` restart cùng run/checkpoint. | Lỗi lặp lại không vô hạn; exit 86 phân biệt với OOM 137. |
| D-PUB-07 | Cột `invalid_reason` trong `vehicles_by_route_snapshot` đóng băng quyết định invalid lúc xử lý; API chỉ tính lại fresh/stale/unknown cho dòng hợp lệ. | Membership đếm xe có ID kể cả invalid. |
| D-PUB-08 | `startingOffsets=earliest` cho checkpoint mới + lọc run bằng Kafka key; chi phí đọc lại dữ liệu run khác trong retention được chấp nhận và ghi trong benchmark. | Không mất message phát trước khi query sẵn sàng (spec §10.1). |

## 3. Nghiệp vụ

| Mã | Quyết định | Lý do |
|---|---|---|
| D-BIZ-01 | Bus uncertainty `mbta_bus_uncertainty_v1`: absent → unknown (eligible, đếm riêng), <300 realtime, 300 schedule-only, 301 degraded, >301/âm invalid. | MBTA bus semantics [S2]. |
| D-BIZ-02 | Target KPI `first_future_resolved_stop_v1`: STU có `stop_sequence` nhỏ nhất với event time > `metrics_as_of`; chỉ đánh giá hợp lệ trên chính target đó; thứ tự lý do loại cố định (`KPI_EXCLUSION_ORDER`). | Một chuyến một phiếu; không chọn stop "đẹp". |
| D-BIZ-03 | Thiếu `start_date`: ứng viên ngày L−1/L/L+1 trong cửa sổ 12 h (`service_date_inference_window_seconds`); ≥2 ứng viên → ambiguous. Có `start_date` nhưng service không active → unresolved, không đoán. | Không tính delay từ lịch đoán. |
| D-BIZ-04 | Freshness: fresh khi −5 ≤ age ≤ 120 (âm → `CLOCK_SKEW`, hiển thị 0); tương lai > 5 s → invalid `FUTURE_TIMESTAMP`; giá trị > 1e11 → `INVALID_TIMESTAMP_UNIT` (không tự đổi ms). | Spec §11.1. |
| D-BIZ-05 | Ngưỡng stale theo feed: VP 120 s, TU 90 s, Alerts 300 s (alerts là lựa chọn dự án). | Spec + quyết định cho alerts. |
| D-BIZ-06 | Alert áp dụng route nếu bất kỳ selector khớp; selector = AND các trường có mặt; selector không resolve → `UNRESOLVED_ALERT_SELECTOR`, không gán route ngẫu nhiên. Snapshot đầy đủ đóng alert vắng mặt; partial giữ `UNCONFIRMED_INHERITED`. | Spec §8.3, §10.2. |
| D-BIZ-07 | `invalid_vehicle_count` đếm entity; `snapshot_membership_count` đếm vehicle_id phân biệt; không ép tổng các lớp bằng membership khi có entity thiếu ID. | PDF p17. |

## 4. Ingestion, replay, API (tóm tắt — chi tiết trong docs/architecture.md)

| Mã | Quyết định |
|---|---|
| D-E01 | httpx sync, một client/thread mỗi feed; timeout 5/20/5/5 s; 3 lần thử; backoff 1·2^k (cap 10 s) × U(0.5,1.5); Retry-After ≤ 300 s; chỉ retry lỗi tạm thời; bỏ qua slot bị lỡ. |
| D-E02 | `raw_sha256` tính trên body đã giải mã; gzip `mtime=0`. |
| D-E06 | Lỗi transport ở collector (`INVALID_JSON`, `INVALID_HEADER`, `UNSUPPORTED_SCHEMA`, `UNSUPPORTED_INCREMENTALITY`, `PAYLOAD_TOO_LARGE`): giữ raw, outbox `rejected`, DLQ, không tạo empty snapshot; Spark vẫn kiểm tra lại. |
| D-E07 | Outbox SQLite WAL `synchronous=FULL`, một file/run, flock; `published` chỉ trong delivery callback. |
| D-E08 | Producer `compression.type=none` để giới hạn 4 MiB đúng bằng kích thước trên dây. |
| D-E13 | Clock replay: tick trước mỗi item tại thời điểm item + mỗi 1 s wall; tick được ACK trước snapshot sau nó. |
| D-E14 | Điều khiển publisher bằng file `publisher_control.json` + ACK `publisher_state.json`. |
| D-E15 | Synthetic = recording sinh tất định phát bằng cùng publisher; golden dùng fixture viết tay. |
| D-E18 | API một worker uvicorn, statement timeout 2 s, budget request 10 s. |
| D-E19 | Cache API chỉ giữ dữ liệu bất biến (generation READY, static); không cache pointer/clock/freshness. |
| D-E22 | History mặc định 1 giờ gần nhất theo application clock, tối đa 24 h, page 100 (max 500), token HMAC. |
| D-E27 | Bản đồ luôn không dùng basemap tile (offline-safe). |

## 5. Giải quyết khác biệt giữa các bản thiết kế

| Mã | Khác biệt | Chốt |
|---|---|---|
| D-X-01 | Định dạng `snapshot_id` (`snp-`+sha256 vs `sn_`+32 hex) | `sn_` + `H(K(...))` (D-ID-02). |
| D-X-02 | Fault injection (env vars vs arm file; exit 86 vs 137) | arm file + 4 điểm lỗi + exit 86 (D-PUB-06). |
| D-X-03 | Accounting membership | membership = fresh + stale + unknown + invalid có ID (D-BIZ-07). |
| D-X-04 | `kafka.max.partition.fetch.bytes` 6 vs 8 MiB | 8 MiB. |
| D-X-05 | Tuổi TU ở bước IT04 (130 vs 170 s) | Theo fixture: header TU1 = 12:00:20Z → 170 s tại 12:03:10Z. |
| D-X-06 | Tên file control publisher | `sessions/<sid>/publisher_control.json`. |

## 6. Quyết định phát sinh trong quá trình xây dựng và review

| Mã | Quyết định | Lý do |
|---|---|---|
| D-B-01 | Lỗi theo từng chuyến trong GTFS (giờ không đơn điệu, thiếu giờ đầu/cuối, stop_sequence/stop_id hỏng, thiếu service_id) là **warning**: chuyến giữ trong `dim_trips` với `schedule_valid=false` và Spark coi là `unresolved` (`UNRESOLVED_TRIP`, không tính delay, loại khỏi KPI). Chỉ lỗi cấu trúc (thiếu file/cột bắt buộc, timezone sai, trùng khóa, thiếu FK trong scope, route cấu hình bị loại) mới làm static load FAILED. | Một chuyến lỗi trên dữ liệu MBTA thật không được chặn cả run. |
| D-B-02 | Snapshot alerts partial kế thừa **mọi** alert của parent vắng mặt, kể cả alert đã hết hạn (trạng thái tính lại là EXPIRED). | Thống nhất design C/D-BIZ-06; không đóng alert khi snapshot không đầy đủ. |
| D-B-03 | Golden S4 (VP1 phát lại sau VP2) tính **đồng thời** duplicate delivery và late delivery; S3 (VP2 lặp khi đang current) chỉ là duplicate. | Hai khái niệm độc lập; bộ đếm `pipeline_status` khớp `expected.json`. |
| D-B-04 | Trip update và alert trùng ID trong cùng snapshot dùng reason `DUPLICATE_OBSERVATION`. | Không thêm mã mới; đơn vị ghi rõ trong `unit`. |
| D-B-05 | Alert bị đóng (vắng mặt trong snapshot đầy đủ) chỉ được ghi trong `accounting_json` của manifest (`closed_absent_from_full_snapshot`), không có hàng phục vụ. | Generation mới đơn giản là không chứa alert đó; không cần bảng thêm. |
| D-B-06 | `snapshot_manifest.parent_available` được đóng băng ở lần build đầu; API coi số hàng **lớn hơn** `expected_counts` (khi ≤ limit) là `generation_inconsistent`. | Retry không đổi kết quả logic; hàng thừa từ lần build bị hủy không bao giờ được phục vụ. |
| D-B-07 | Raw path: live = tương đối RAW_ROOT có tiền tố run; replay/synthetic = tương đối thư mục recording. Lineage thử lần lượt `provenance.recording_dir`, `data/raw`, `data/recordings/<id>`, `data/raw/<id>`. | Recording giữ nguyên cấu trúc khi xuất/nhập bundle. |
| D-B-08 | Collector phát `run_started`/`run_finished` cho run live (`clock_sequence` = epoch ms); run live chuyển CREATED→RUNNING khi có dữ liệu đầu tiên. | Nếu không, run live mãi ở CREATED. |
| D-B-09 | `tests/plugins/test_report.py` chỉ ghi `test_report.json` khi có test trong `tests/integration` được chọn; Spark tests mang `it_case` là vector đơn vị, không phải bằng chứng IT. | Không bao giờ tạo PASS tích hợp giả. |
| D-B-10 | pyspark sdist được build với ràng buộc `setuptools<81` (`[tool.uv] build-constraint-dependencies`). | Tránh hỏng build do setuptools gỡ API cũ. |
| D-B-11 | `source_order` tính micro giây bằng số nguyên; phía Python phải dùng `original_fetched_at` đã parse từ ISO (độ chính xác ms) để khớp Spark. | Tránh lệch 1 µs do số thực. |
| D-B-12 | **Theo yêu cầu người dùng**, notebook Colab mở dashboard Streamlit ra Internet qua Cloudflare *quick tunnel* (`cloudflared` 2026.10.0, URL ngẫu nhiên `https://*.trycloudflare.com`, không cần tài khoản/token). Bật mặc định (`ENABLE_CLOUDFLARE_TUNNEL = True`), tắt được. Đặc tả gốc chỉ nói notebook *không bắt buộc* tunnel; đây là lựa chọn bổ sung có chủ đích. Binary được pin bằng SHA-256 công bố trong release notes và kiểm tra khi bootstrap. | Truy cập web trực tiếp từ Colab chỉ với Run All. Rủi ro: ai có link đều xem được dashboard (chỉ đọc, dữ liệu công khai MBTA/synthetic, không có secret); tunnel kết thúc khi dừng session/VM. |
| D-B-13 | Dashboard và tunnel là thành phần `access`, sống qua các demo session để URL công khai ổn định trong cả phiên Colab; Streamlit chỉ bind 127.0.0.1, tunnel là đường duy nhất ra ngoài. | Link không đổi khi chuyển mode/session. |
| D-B-14 | `trip_instance_key` = `trip_id|YYYY-MM-DD` (ngày dịch vụ ISO) khi resolve được; `trip_id|?<start_date hoặc none>` khi không. | Thống nhất Spark, tham chiếu Python và `expected.json`. |
| D-B-15 | Trong notebook, các bước kiểm thử (unit/runtime/spark, golden smoke, integration, benchmark) ghi **FAIL** rõ ràng (hộp đỏ, tóm tắt ở cell 14/17/18) rồi chạy tiếp để demo và link vẫn có; `STRICT_RUN_ALL=True` làm cell 18 raise khi có bước FAIL. Bootstrap/doctor/infra/start vẫn dừng Run All vì các bước sau phụ thuộc chúng. | Không nuốt lỗi, nhưng một test chưa từng chạy không chặn toàn bộ demo. |
| D-B-16 | Argv của session được giữ ổn định (thời lượng đầu vào tính một lần, lưu trong `session.json`) và thành phần còn sống của cùng session được tái sử dụng, nên Run All lần hai không CONFLICT. | Chạy lại an toàn (CT04). |
| D-B-17 | `infra.py init --external-infra` (tự bật với profile `local-docker`) bỏ qua registry của supervisor và kiểm tra Kafka/Cassandra bằng AdminClient/driver; dùng cho Docker Compose. | Compose tự quản lý tiến trình hạ tầng. |
| D-B-18 | Profile được xác định theo thứ tự: `--profile` > `TP_PROFILE` > `runtime.json` của workdir > mặc định (`colab-native` cho scripts). | Hai entry point (`scripts/*.py`, `transitpulse`) luôn nhắm cùng workdir. |
