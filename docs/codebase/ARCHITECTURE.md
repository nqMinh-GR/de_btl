# ARCHITECTURE — Luồng dữ liệu và bảo đảm publish

Mô tả implementation đọc từ source/config ngày 14/10/2026; runtime chưa được xác minh lại. Thiết kế hướng tới TransitPulse MBTA — giám sát xe buýt và độ trễ **dự kiến**, không phải hệ thống đo độ trễ thực tế chính thức của MBTA (`README.md`, `docs/limitations.md`).

### 1) Phong cách kiến trúc

- Pipeline hướng sự kiện theo các giai đoạn ingestion → queue → Spark processing → serving store → API; module chia theo trách nhiệm này (`src/transitpulse/ingestion/collector.py`, `src/transitpulse/processing/stream_job.py`, `src/transitpulse/api/app.py`).
- Static GTFS batch tạo dimensions bất biến cho run; live/replay/synthetic cùng đi qua Kafka/Spark/Cassandra (`src/transitpulse/static/build.py`, `src/transitpulse/replay/publisher.py`, `src/transitpulse/runs.py`).
- Ràng buộc thiết kế: at-least-once + write/publish idempotent; generation phải READY trước khi API đọc; clock live là wall còn replay/synthetic là source; topology demo một broker/một Cassandra RF1 (`docs/publish_protocol.md`, `src/transitpulse/api/context.py`, `src/transitpulse/processing/control_clock.py`, `config/base.yaml`).
- Phạm vi chính của tài liệu dùng 10 tuyến theo notebook: `1,15,22,23,28,32,39,57,66,77`; code cho tối đa 10. README cũ vẫn tuyên bố MVP 5. Danh sách cấu hình chưa thay thế source verification (`README.md`, `config/base.yaml`, `notebooks/TransitPulse_MBTA_Colab_Demo.ipynb`).

### 2) Luồng hệ thống

```text
GTFS Static ZIP -> version/scope -> Spark batch -> Parquet dimensions + Cassandra catalog
MBTA VP/TU/Alerts -> collector -> raw + sidecar + SQLite outbox -> Kafka
Recording/synthetic -> timeline publisher + source-clock control -> Kafka
Kafka + static dimensions -> Spark foreachBatch -> Cassandra BUILDING -> READY -> feed_current CAS
                                               -> Curated Parquet _COMMITTED / DLQ / quality
Cassandra READY + clock -> FastAPI /api/v1 -> React / Streamlit / notebook
Curated _COMMITTED -> Spark report job -> báo cáo lịch sử
```

1. **Chuẩn bị static:** version được tính SHA-256 ZIP; scope gắn route IDs, route type và GTFS version. Static loader validate, build dims/catalog; run lưu GTFS version/scope/policy (`src/transitpulse/contracts/ids.py`, `src/transitpulse/static/build.py`, `src/transitpulse/runs.py`).
2. **Capture live:** mỗi feed có poll thread/client riêng. Thứ tự durable là raw → sidecar → outbox → capture index. Callback broker ACK mới chuyển outbox sang `published`. Fetch lỗi/HTTP304 không được biến thành empty snapshot (`src/transitpulse/ingestion/collector.py`, `src/transitpulse/ingestion/outbox.py`).
3. **Replay:** publisher kiểm tra recording/timeline, phát clock/control và snapshot qua cùng broker; synthetic là recording được tạo có nhãn riêng (`src/transitpulse/replay/publisher.py`, `src/transitpulse/replay/synthetic/recording.py`).
4. **Streaming:** query subscribe VP/TU/Alerts/control, checkpoint theo run và pipeline version; `startingOffsets=earliest`, `failOnDataLoss=true`, trigger 5 s. `foreachBatch` validate/parse/build và xử lý snapshot theo source order (`src/transitpulse/processing/stream_job.py`, `src/transitpulse/processing/batch.py`, `config/base.yaml`).
5. **Publish:** tạo BUILDING bằng LWT và đóng băng parent/clock/expiry; sink ghi bảng serving/history theo partition; so số dòng expected với written; chuyển READY; pointer CAS tiến về source order mới hơn. Curated có `_COMMITTED`; downstream report chỉ đọc committed files (`src/transitpulse/storage/manifest.py`, `src/transitpulse/storage/pointer.py`, `src/transitpulse/processing/sink.py`, `src/transitpulse/processing/batch.py`, `src/transitpulse/processing/curated.py`).
6. **Đọc:** API đọc run context, pointer một lần/feed/request, kiểm tra manifest READY/expiry/counts, truy vấn partition có giới hạn rồi trả `{data, meta, warnings}`. Feed lỗi có thể làm block tương ứng `null`; generation thiếu không được zero-fill (`src/transitpulse/api/context.py`, `src/transitpulse/api/repository.py`, `src/transitpulse/api/models.py`).

### 3) Trách nhiệm tầng/module

| Tầng | Sở hữu | Ranh giới | Bằng chứng |
|---|---|---|---|
| Contracts/static | IDs, schema/policy; lịch, calendar/timezone, catalog | Static version cố định/run | `src/transitpulse/contracts/ids.py`, `src/transitpulse/static/gtfs_time.py`, `src/transitpulse/runs.py` |
| Ingestion/replay | Durable input và timeline/control | Biến đổi KPI ở processing | `src/transitpulse/ingestion/collector.py`, `src/transitpulse/replay/publisher.py`, `src/transitpulse/processing/kpi.py` |
| Processing | Vehicles/predictions/alerts/KPI/quality; transaction visibility bằng generation | Không hứa transaction nguyên tử giữa ba feed | `src/transitpulse/processing/batch.py`, `docs/limitations.md` |
| Storage | Query-shaped tables, LWT manifest/pointer, Cassandra sessions | RF1 demo; không có HA theo config | `src/transitpulse/storage/migrations/0001_initial.cql`, `src/transitpulse/storage/cassandra.py` |
| API/UI | Read model, freshness/request errors, hiển thị dữ liệu | UI gọi HTTP API; API không fetch MBTA | `src/transitpulse/api/repository.py`, `frontend/src/api/client.js`, `app/api_client.py`, `docs/architecture.md` |
| Runtime/observability | Process ownership, lifecycle, metrics/log/lineage, fault recovery | Recovery chạy trong workdir và đúng run/checkpoint | `src/transitpulse/runtime/supervisor.py`, `src/transitpulse/runtime/cli_demo.py`, `src/transitpulse/obs/lineage.py` |

### 4) Pattern dùng lại

| Pattern | Nơi xuất hiện | Mục đích được code/tài liệu thể hiện |
|---|---|---|
| Durable outbox | `src/transitpulse/ingestion/outbox.py` | SQLite WAL/FULL lưu pending; có thể resend sau restart, downstream chịu duplicate |
| Immutable generation + publication pointer | `src/transitpulse/storage/manifest.py`, `src/transitpulse/storage/pointer.py` | Chặn đọc partial writes; CAS không lùi source order |
| Stable ID + canonical serialization | `src/transitpulse/contracts/ids.py`, `src/transitpulse/processing/spark_ids.py` | Retry/dedup và Python/Spark parity |
| Repository interface + injected fake | `src/transitpulse/api/repository.py`, `src/transitpulse/api/memory_repository.py`, `src/transitpulse/api/app.py` | Tách truy vấn Cassandra khỏi API, cho phép unit test |
| Process-wide service container | `src/transitpulse/api/deps.py` | `app.state.services` giữ repo/cache/metrics/signer; `RequestEnv` giữ deadline và request clock |
| Atomic file/commit marker | `src/transitpulse/processing/curated.py` | File JSON atomic và curated staging/commit để retry/report |
| Supervisor ownership | `src/transitpulse/runtime/supervisor.py` | Quản lý đúng PID/component/session; stop có kiểm tra ownership |

### 5) Rủi ro kiến trúc đã biết

- Visibility atomic **theo feed**, không giữa VP/TU/Alerts; màn tổng hợp cần meta/as-of riêng từng feed (`docs/limitations.md`, `src/transitpulse/api/context.py`).
- TTL tính theo wall clock kể cả source-clock replay pause; generation/parent có thể hết hạn. Batch có nhánh yêu cầu tạo run mới và replay khi parent đã mất dữ liệu; restart checkpoint không phục hồi mọi tình huống (`src/transitpulse/processing/batch.py`, `src/transitpulse/api/context.py`).
- Một partition/topic, RF1 và Spark local không bảo vệ mất host. `max_snapshots_per_batch=3000` là fail bound; vượt bound làm batch raise thay vì tự chia tải (`config/base.yaml`, `src/transitpulse/processing/batch.py`). `[TODO]` Đo budget và recovery trên môi trường được chọn.
- **Intent vs reality:** `docs/architecture.md`/`docs/limitations.md` vẫn mô tả notebook/Streamlit và map không basemap; React hiện có 6 màn và CARTO tiles. README nói Colab mặc định synthetic, notebook hiện đặt live và 10 route IDs. Đây là khác biệt source/tài liệu, không bằng chứng live đã chạy (`frontend/src/App.jsx`, `frontend/src/components/TransitMap.jsx`, `notebooks/TransitPulse_MBTA_Colab_Demo.ipynb`).

### 6) Bằng chứng

- `README.md`, `docs/architecture.md`, `docs/publish_protocol.md`, `docs/limitations.md`.
- `src/transitpulse/ingestion/collector.py`, `src/transitpulse/ingestion/outbox.py`, `src/transitpulse/replay/publisher.py`.
- `src/transitpulse/processing/stream_job.py`, `src/transitpulse/processing/batch.py`, `src/transitpulse/processing/sink.py`, `src/transitpulse/processing/curated.py`.
- `src/transitpulse/storage/manifest.py`, `src/transitpulse/storage/pointer.py`, `src/transitpulse/api/context.py`, `src/transitpulse/api/repository.py`.
- `config/base.yaml`, `frontend/src/App.jsx`, `frontend/src/components/TransitMap.jsx`, `notebooks/TransitPulse_MBTA_Colab_Demo.ipynb`.
