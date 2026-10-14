# CONCERNS — Khoảng trống, rủi ro và khác biệt tài liệu

Khảo sát static ngày 14/10/2026. Mức ưu tiên là đánh giá từ bằng chứng source/config, không phải danh sách lỗi đã reproduce. Report bàn giao ngày 07/10 vẫn chưa có evidence runtime (`reports/environment_verification.json`).

### 1) Rủi ro chính theo ưu tiên

| Ưu tiên | Vấn đề | Bằng chứng | Ảnh hưởng | Hành động đề xuất |
|---|---|---|---|---|
| Cao | Chưa có báo cáo kiểm chứng gắn commit hiện tại | `reports/environment_verification.json` viết tay, `commit=null`, NOT_RUN/NOT_VERIFIED; `docs/codebase-scan.txt` có git history | Chưa thể nghiệm thu bằng runtime evidence; báo cáo cũ không phản ánh mọi lần chạy sau đó | `[TODO]` Chạy target đã chọn, lưu commit/config/environment/log/report, chỉ đổi PASS khi có evidence |
| Cao | Không có `uv.lock`; Docker COPY/Makefile yêu cầu file | `docs/versions.md`, `docker/app.Dockerfile`, `Makefile`, `docs/codebase-scan.txt` | Fresh bootstrap resolve transitive dependencies; Docker build bị chặn bởi missing lock | `[TODO]` Resolve và commit lock từ môi trường kiểm chứng, giữ artifact report |
| Cao | Scope/default notebook lệch mô tả bàn giao | `README.md` MVP 5/default synthetic; `config/base.yaml` max 10; notebook live với 10 IDs | Nhầm phạm vi BR/demo, yêu cầu source availability khác synthetic | Dùng scope 10 hiện tại; xác minh 10 IDs bằng static + VP sample, cập nhật mô tả default |
| Cao | Parent TTL mất dữ liệu có thể khiến batch fail lặp lại | `src/transitpulse/processing/batch.py` `_load_parent`, `src/transitpulse/api/context.py` | Cùng run/checkpoint không đủ để chữa permanent parent loss | `[TODO]` Recovery acceptance cần phân biệt restart cùng run và replay recording sang run mới |
| Trung bình | ETA React có thể giữ payload cũ khi người dùng giữ cùng stop | `frontend/src/views/StopsView.jsx` effect dependencies; `frontend/src/AppContext.jsx` periodic jobs/fetchPredictions | Global feed/clock refresh không kích hoạt fetch stop predictions trong effect hiện tại | `[TODO]` Test cùng stop qua nhiều generations; refresh ETA theo clock/route/generation thích hợp |
| Trung bình | UI hiển thị uncertainty như ±seconds cho mọi số | `frontend/src/views/StopsView.jsx`; backend `src/transitpulse/contracts/policy.py` classify 300/301 | Giá trị 300 = SCHEDULE_ONLY, 301 = DEGRADED của policy có thể bị đọc như khoảng sai số | `[TODO]` Hiển thị semantic class backend và test các mốc absent/299 / 300 / 301 / >301 |
| Trung bình | Tài liệu UI/map/access chưa theo source mới | `docs/architecture.md`, `docs/limitations.md`, `docs/decisions.md` D-E27; `frontend/src/App.jsx`, `frontend/src/components/TransitMap.jsx`, `src/transitpulse/runtime/access.py` | Có 6 màn React/CARTO và public same-origin API; tuyên bố map offline-safe chung không còn đúng cho basemap React | Cập nhật architecture/limitations; tách offline pipeline từ phụ thuộc tiles của browser |

Danh sách notebook: `1,15,22,23,28,32,39,57,66,77`. Đây là **cấu hình scope 10**, chưa có source-check report xác nhận chúng trong inventory được khảo sát. `.env.example` để route IDs trống và nguồn phải được kiểm chứng (`notebooks/TransitPulse_MBTA_Colab_Demo.ipynb`, `.env.example`, `src/transitpulse/ingestion/source_check.py`).

### 2) Nợ kỹ thuật

| Mục | Nguồn gốc quan sát được | Vị trí | Rủi ro | Đề xuất |
|---|---|---|---|---|
| Report/document handover cũ | Narrative “không dùng git/chưa có git” từ bàn giao trong khi terminal đã có 9 commits | `docs/decisions.md` D-ENV-01/02, `reports/environment_verification.json`, `docs/codebase-scan.txt` | Người đọc hiểu sai trạng thái hiện tại | Giữ lịch sử bàn giao nhưng ghi timestamp/commit của assessment mới |
| Module lớn | Terminal `wc -l`: batch 1523, collector 1407, publisher 1401, runtime cli_demo1245, colab_ui3347 | Các file dưới đây ở §5 | Chi phí review và thay đổi protocol/lifecycle | Refactor theo trách nhiệm sau khi có test/recovery evidence; giữ interface/contracts |
| Frontend thiếu test/lint scripts | Package scripts chỉ dev/build/preview | `frontend/package.json` | Regression auto-refresh/status/semantics chưa có suite bảo vệ | `[TODO]` Behavior tests nhắm BR ETA/freshness/alerts và request failure |
| Source document link thiếu | README tham chiếu build-prompt Markdown không có trong scan | `README.md`, `docs/codebase-scan.txt` | Không truy được toàn bộ intent gốc qua liên kết | `[TODO]` Khôi phục file hoặc sửa tham chiếu |
| Runtime pin còn cần evidence | Lock NOT_GENERATED, checksum một số artifact null, connector PENDING_FIRST_RESOLVE | `deploy/runtime-manifest.json` | Bộ version pin chưa chứng minh tương thích/download/checksum trên target | Xuất bootstrap_report và pin patch từ lần bootstrap thành công |

Scanner ghi **không tìm TODO/FIXME/HACK trong production code theo bộ lọc của script** (`docs/codebase-scan.txt`). Điều này không chứng minh không có debt. Coverage gaps trong tests được ghi riêng ở `TESTING.md`; không chuyển chúng thành lỗi production mặc định.

### 3) Bảo mật

| Rủi ro | Phân loại | Bằng chứng | Biện pháp hiện có | Khoảng trống |
|---|---|---|---|---|
| Quick tunnel public không auth, gồm API read qua React proxy | Giới hạn access của demo | `src/transitpulse/runtime/access.py`, `src/transitpulse/api/app.py`, `frontend/vite.config.js` | API GET/CORS allowlist, base/preview localhost; expose stop/stop --all dừng access | `[TODO]` Auth/rate limiting nếu dùng ngoài phạm vi dữ liệu public/demo |
| React dev server host:true | Phạm vi bind | `frontend/vite.config.js` | Preview bind 127.0.0.1, API base localhost | Dev server có thể nghe interface ngoài localhost; cần chọn bind khi chạy trên máy mạng |
| Logger truncate nhưng không tự redact mọi text | Logging | `src/transitpulse/obs/logging.py`, `src/transitpulse/ingestion/http_fetch.py` | Header allowlist, excerpt giới hạn; unhandled API error message chung | `[TODO]` Che credentials/custom data trước khi thêm nguồn có secrets |
| Streamlit qua tunnel tắt CORS/XSRF | Giới hạn cấu hình demo | `src/transitpulse/runtime/access.py` dashboard_argv | UI mô tả chỉ đọc; ownership runner/tunnel | Cần review lại nếu thêm form ghi hoặc authentication |

`docs/limitations.md` có một phát biểu CVE/Spark cụ thể. `[TODO]` Xác minh advisory chính thức và phạm vi ảnh hưởng trước khi coi đó là security finding đã được kiểm chứng; khảo sát source này chỉ xác nhận config `spark.ui.enabled=false` và không bổ sung kết luận CVE độc lập (`config/base.yaml`, `src/transitpulse/processing/spark_session.py`). Scan không phát hiện dedicated security/compliance config trong phạm vi bộ dò (`docs/codebase-scan.txt`); đây không phải vulnerability audit.

### 4) Hiệu năng và mở rộng

| Điểm | Bằng chứng | Hiện trạng quan sát | Rủi ro khi tăng tải | Đề xuất |
|---|---|---|---|---|
| Một broker/partition, Cassandra RF1, Spark local | `config/base.yaml`, `src/transitpulse/storage/migrations/0001_initial.cql` | Topology demo cấu hình; chưa có số đo | Không HA/chịu mất host; throughput phụ thuộc một VM | `[TODO]` Benchmark target; thiết kế topology riêng nếu mở rộng |
| Micro-batch hard bound 3000 | `src/transitpulse/processing/batch.py`, `src/transitpulse/processing/stream_job.py`, `config/base.yaml` | Quá bound raise BacklogBoundExceeded; plans xử lý từng snapshot; reader không có maxOffsetsPerTrigger | Backlog lớn có thể dừng tiến triển batch | Đo backlog/driver budget; không đổi cách chia batch tùy tiện vì clock/control ordering |
| Parent read bound 2000/route, 5000total | `config/base.yaml`, `src/transitpulse/processing/batch.py` `_read_parent` | Overflow chuyển “không inheritance” có warning theo deterministic rule | Partial snapshot semantics giảm khi vượt bound | Stress inheritance và công bố reason/coverage |
| Colab driver 16g khác minimum doctor 8 GiB | `config/colab.yaml`, `config/base.yaml`, `tests/spark/conftest.py` | Config Colab nhắm 51 GiB; JVM budget comment≈23 GiB; chưa đo thực tế | Colab thường hoặc máy nhỏ có thể thiếu RAM mặc dù ngưỡng nền không đủ phản ánh budget profile | `[TODO]` Đối chiếu preflight với actual profile/memory; thu RSS/exit evidence |
| Retention/expiry | `config/base.yaml`, `src/transitpulse/processing/sink.py`, `src/transitpulse/api/context.py` | Kafka 6 h, serving 1 h, history 3 ngày; TTL wall clock | Pause/outage lâu dẫn đến expiry; retry không tái tạo input đã mất | Kiểm thử retention boundaries và recovery từ recording |
| API bounded reads/cache | `src/transitpulse/api/repository.py`, `config/base.yaml` | Request 10 s/statement2 s, page max 500, max total 2000; Colab fanout 10 | Fanout/giới hạn rows cần phù hợp scope 10; multi-instance cache/token hiện process-local | Benchmark API riêng; công bố truncation, giữ pagination semantics |

Benchmark README nói p95 visibility latency<10 s là **target chưa đo**, không phải kết quả. Không có symptom throughput/OOM thực đo trong các artifact khảo sát (`benchmarks/README.md`, `reports/environment_verification.json`). Comment Spark fixture mô tả từng gặp heap 4g không đủ, nhưng chưa có log/report xác minh tại commit hiện tại (`tests/spark/conftest.py`).

### 5) Khu vực dễ thay đổi/high churn

Churn = số lần file xuất hiện trong commits 90 ngày theo scan; repo được scan có 9 commits. Số lần thay đổi là tín hiệu review, không tự chứng minh bug (`docs/codebase-scan.txt`).

| Khu vực | Lý do cần chú ý | Churn/độ lớn | Chiến lược thay đổi |
|---|---|---|---|
| `notebooks/TransitPulse_MBTA_Colab_Demo.ipynb` | Entry demo, source modes/scope/defaults/access/report | 7 lần | Kiểm schema + Run All target; đối chiếu defaults với README |
| `README.md` | Intent/runbook nhanh đang lệch notebook | 5 lần | Cập nhật theo source verified, giữ timestamp status |
| `notebooks/colab_ui.py` | UI/lifecycle/evidence helper nhiều trách nhiệm | 3 lần; 3347 dòng | Test helper độc lập và notebook flow |
| `config/colab.yaml` | Heap/VM budget | 3 lần | Resource sampling trên đúng Colab VM |
| `tests/spark/conftest.py` | Shared Spark fixture/heap | 3 lần | Kiểm session isolation; không suy PASS từ comment |
| `src/transitpulse/processing/batch.py` | Generation/parent/CAS/curated orchestration | 1523 dòng (terminal wc -l) | Golden + fault-injection + TTL/recovery tests khi sửa protocol |
| `src/transitpulse/ingestion/collector.py` | Fetch/archive/outbox/reconcile/status | 1407 dòng (terminal wc -l) | Outbox resume/disk/source fault tests |
| `src/transitpulse/replay/publisher.py` | Timeline/control/clock ordering | 1401 dòng (terminal wc -l) | Pause/resume/step/determinism + integration clock checks |
| `src/transitpulse/runtime/cli_demo.py` | Session/infra/input/recovery commands | 1245 dòng (terminal wc -l) | Ownership/lifecycle tests, giữ dữ liệu khi stop |

### 6) Câu hỏi `[ASK USER]`

1. [ASK USER] Q1 — Nghiệm thu runtime ưu tiên Google Colab hay Linux local-native? Tài liệu đã hoàn thành từ source; chọn platform để lập kế hoạch kiểm chứng và thu evidence tiếp theo.

Scope chính dùng 10 route IDs hiện được cấu hình trong notebook. Không yêu cầu chọn lại 5 tuyến. `[TODO]` Xác minh route IDs và sample MBTA thật khi có live source-check; câu hỏi platform không ngăn hoàn thành tài liệu khảo sát.

### 7) Bằng chứng

- `docs/codebase-scan.txt`: recent commits/high churn, scanner TODO và config detections; terminal `git log -1` và `wc -l` dùng trong lượt khảo sát.
- `reports/environment_verification.json`, `docs/versions.md`, `docs/limitations.md`, `docs/decisions.md`, `README.md`.
- `config/base.yaml`, `config/colab.yaml`, `deploy/runtime-manifest.json`, `docker/app.Dockerfile`, `Makefile`.
- `src/transitpulse/processing/batch.py`, `src/transitpulse/processing/stream_job.py`, `src/transitpulse/api/context.py`, `src/transitpulse/runtime/access.py`.
- `frontend/package.json`, `frontend/vite.config.js`, `frontend/src/AppContext.jsx`, `frontend/src/views/StopsView.jsx`, `frontend/src/components/TransitMap.jsx`.
- `notebooks/TransitPulse_MBTA_Colab_Demo.ipynb`, `tests/spark/conftest.py`, `benchmarks/README.md`.
