# INTEGRATIONS — Nguồn dữ liệu, storage và truy cập

Đây là inventory từ config/source, không phải kết quả gọi nguồn live. Template `.env.example` nêu MBTA feeds công khai và không cần credentials; trong lượt khảo sát không đọc `.env` thật và không in secret.

### 1) Inventory tích hợp

| Hệ thống | Loại | Mục đích | Mô hình auth/config quan sát | Mức phụ thuộc | Bằng chứng |
|---|---|---|---|---|---|
| MBTA `https://cdn.mbta.com/MBTA_GTFS.zip` | HTTP ZIP | Lịch tuyến/trạm/trips/shapes, static version | URL public, User-Agent; template không yêu cầu API key | Cao với live/static | `config/base.yaml`, `.env.example`, `src/transitpulse/static/ingest.py` |
| MBTA `https://cdn.mbta.com/realtime/VehiclePositions.json` | HTTP JSON | Quan sát vị trí | Public; HTTP client/feed | Cao với BR vị trí | `config/base.yaml`, `src/transitpulse/ingestion/http_fetch.py` |
| MBTA `https://cdn.mbta.com/realtime/TripUpdates.json` | HTTP JSON | Dự báo tại stop và predicted-delay | Như VP | Cao với ETA/KPI | `config/base.yaml`, `src/transitpulse/ingestion/collector.py` |
| MBTA `https://cdn.mbta.com/realtime/Alerts.json` | HTTP JSON | Cảnh báo dịch vụ | Như VP | Cao với alerts | `config/base.yaml`, `src/transitpulse/processing/alerts.py` |
| Kafka | Queue | 3 snapshot topics + control + DLQ | Base localhost; Docker service address; chưa cấu hình SASL/TLS trong producer config | Cao pipeline | `config/base.yaml`, `config/docker.yaml`, `src/transitpulse/ingestion/kafka_producer.py` |
| Cassandra | Database | Catalog, published generations, history, quality/status | Config host/port/keyspace/consistency; Cluster không có auth_provider trong factory | Cao serving | `src/transitpulse/storage/cassandra.py`, `config/base.yaml` |
| Cloudflare quick tunnel | Proxy | Public demo React/Streamlit/API tùy target | Không account/token/auth ở quick-tunnel flow; ownership token chỉ quản lý process | Tùy chọn demo | `src/transitpulse/runtime/access.py` |
| CARTO/OpenStreetMap | Browser tile service | Basemap React | URL raster trong browser, không có key config | Basemap | `frontend/src/components/TransitMap.jsx` |
| GitHub, Adoptium, Apache, Node.js, PyPI, Maven Central | Artifact/dependency download | Bootstrap uv/JDK/Kafka/Cassandra/Node/jars/Python dependencies | URL/checksum theo manifest; phụ thuộc Internet khi chưa cache | Cao bootstrap mới | `deploy/runtime-manifest.json`, `scripts/bootstrap_colab.sh`, `src/transitpulse/runtime/bootstrap.py` |
| GitHub Actions | CI service | Lint/unit/repo checks; manual native integration | `permissions: contents: read` | Kiểm chứng | `.github/workflows/ci.yml`, `.github/workflows/integration.yml` |

Mức phụ thuộc trong bảng mô tả vai trò luồng code, không phải SLA đã đo.

### 2) Kho dữ liệu

| Kho | Vai trò | Access layer | Rủi ro/giới hạn | Bằng chứng |
|---|---|---|---|---|
| Filesystem raw gzip + sidecar/index/recording manifest | Giữ input/provenance, replay | `src/transitpulse/ingestion/archive.py`, `src/transitpulse/ingestion/recording.py` | Budget raw 2 GiB, min free 1 GiB base; không tự xóa raw | `config/base.yaml`, `src/transitpulse/ingestion/disk_budget.py` |
| SQLite WAL/FULL per run | Durable outbox snapshot/DLQ | `src/transitpulse/ingestion/outbox.py` | Một collector/run bằng flock; ACK trước local update có thể duplicate | `src/transitpulse/ingestion/outbox.py` |
| Parquet static | Dimension theo GTFS version/scope | `src/transitpulse/static/dims.py`, `src/transitpulse/static/build.py` | Phải đúng version của run | `src/transitpulse/runs.py` |
| Kafka | Buffer/offsets vận chuyển | Producer/publisher/Spark reader | Retention 6 h, 1 partition/topic, RF1; không phải archive lâu dài | `config/base.yaml`, `src/transitpulse/processing/stream_job.py` |
| Cassandra | Serving/query-shaped tables | Storage driver, Spark partition sink, API repository | RF1/LOCAL_ONE; generation TTL 1 h, history 3 ngày base | `config/base.yaml`, `src/transitpulse/storage/migrations/0001_initial.cql`, `src/transitpulse/processing/sink.py` |
| Curated Parquet | History/report input | `src/transitpulse/processing/curated.py`, `src/transitpulse/processing/report_job.py` | Chỉ đọc batch `_COMMITTED`; filesystem của VM có thể mất | `docs/limitations.md`, `src/transitpulse/processing/curated.py` |
| In-process immutable cache | READY/static rows cho API | `src/transitpulse/api/cache.py`, `src/transitpulse/api/context.py` | Cache 256 entries/max30s base; pointer/clock/freshness không được cache theo thiết kế | `config/base.yaml`, `docs/decisions.md` D-E19 |

### 3) Secrets và credentials

- `.env.example`/`frontend/.env.example` chứa cấu hình endpoint/scope, không có secret; `.gitignore` bỏ `.env`, `*.pem`, `*.key`, `.git-credentials`. Đây là kiểm tra template/config, không phải audit toàn bộ git history (`.env.example`, `.gitignore`).
- API không có auth production. `TP_API_TOKEN_KEY` là HMAC **page-token key**, không phải access token đăng nhập; unset thì random 32 bytes/process, restart làm token cũ invalid (`src/transitpulse/api/settings.py`, `src/transitpulse/api/paging.py`, `src/transitpulse/api/app.py`).
- Base API localhost, CORS allowlist GET/no credentials; React dev server `host:true`, preview localhost. Tunnel React proxy `/api` cùng origin nên public console cho phép public read API qua cùng URL (`config/base.yaml`, `frontend/vite.config.js`, `src/transitpulse/runtime/access.py`).
- `[TODO]` Secret rotation/auth/TLS cho deployment ngoài demo chưa được định nghĩa; không coi CORS là kiểm soát truy cập (`src/transitpulse/api/app.py`, `docs/limitations.md`).

### 4) Độ tin cậy và xử lý lỗi

- Fetch: tối đa 3 lần thử; timeout connect/read/write/pool5/20/5/5 s; exponential jitter U(0.5, 1.5), backoff cap 10 s; Retry-After cap 300 s. Retry transient timeout/transport/429/500/502/503/504; conditional 304 không tạo observation mới; cap decoded body 64 MiB (`config/base.yaml`, `src/transitpulse/ingestion/http_fetch.py`).
- Raw/sidecar trước outbox/Kafka. Outbox chỉ published khi delivery callback thành công; restart reconcile có thể re-deliver cùng logical IDs (`src/transitpulse/ingestion/collector.py`, `src/transitpulse/ingestion/outbox.py`).
- Parse/transport rejection giữ raw, durable DLQ/reason, không tạo empty snapshot; disk budget fail-stop toàn collector với bounded drain (`src/transitpulse/ingestion/collector.py`).
- Spark `failOnDataLoss=true`; lỗi count/parent/pointer làm batch thất bại. Publish CAS tối đa 10attempts; out-of-order generation cũ không thành current (`src/transitpulse/processing/stream_job.py`, `src/transitpulse/processing/batch.py`, `config/base.yaml`).
- API khởi động được khi Cassandra down; `/health/live` vẫn trả 200, readiness/data503; lazy reconnect interval 2 s. Statement budget 2 s, request 10 s base; data unavailable không giả 0 (`src/transitpulse/api/app.py`, `src/transitpulse/api/settings.py`, `src/transitpulse/api/context.py`, `config/base.yaml`).
- Không tự đổi live thành synthetic khi nguồn lỗi theo notebook/docs; replay/offline cần input/cache đã chuẩn bị. `[TODO]` Chạy verify offline, retry và recovery trên target (`notebooks/TransitPulse_MBTA_Colab_Demo.ipynb`, `docs/offline_checklist.md`, `reports/environment_verification.json`).

### 5) Observability

- JSON logs chuẩn context ở collector/processor/API/runtime; raw references/capture/snapshot IDs phục vụ truy vết (`src/transitpulse/obs/logging.py`, `src/transitpulse/obs/lineage.py`).
- Prometheus per-component textfiles trong workdir, xuất mỗi 5 s base; `/api/v1/metrics` gộp API registry + `.prom` files. Core demo không chạy Prometheus server riêng (`src/transitpulse/obs/metrics.py`, `src/transitpulse/api/routers/health.py`, `config/base.yaml`).
- `query_progress.jsonl` lưu batch duration/end/latest offsets và backlog; Cassandra pipeline_status ghi component/heartbeat/error (`src/transitpulse/processing/stream_job.py`, `src/transitpulse/storage/migrations/0001_initial.cql`).
- Benchmark sampler lấy PID resources, Kafka backlog, disk và manifest latency, chỉ sinh số có evidence (`benchmarks/samplers.py`, `benchmarks/compute.py`, `benchmarks/README.md`). `[TODO]` Chưa có số đo và dashboard/alert rule ngoài demo trong báo cáo bàn giao (`reports/environment_verification.json`).

### 6) Bằng chứng

- `config/base.yaml`, `config/docker.yaml`, `.env.example`, `frontend/.env.example`, `.gitignore`.
- `src/transitpulse/ingestion/http_fetch.py`, `src/transitpulse/ingestion/collector.py`, `src/transitpulse/ingestion/outbox.py`.
- `src/transitpulse/storage/cassandra.py`, `src/transitpulse/storage/migrations/0001_initial.cql`, `src/transitpulse/processing/stream_job.py`.
- `src/transitpulse/api/app.py`, `src/transitpulse/api/settings.py`, `src/transitpulse/runtime/access.py`, `frontend/vite.config.js`, `frontend/src/components/TransitMap.jsx`.
- `src/transitpulse/obs/metrics.py`, `src/transitpulse/obs/lineage.py`, `benchmarks/README.md`, `deploy/runtime-manifest.json`.
