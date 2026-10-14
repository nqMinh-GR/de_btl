# STRUCTURE — Cấu trúc repository

Khảo sát source/config ngày 14/10/2026. Những thư mục runtime trong sơ đồ `WorkPaths` là đường dẫn chương trình có thể tạo, không chứng minh dữ liệu đang tồn tại (`src/transitpulse/runtime/paths.py`).

### 1) Bản đồ thư mục gốc

| Đường dẫn | Mục đích kiểm chứng được | Bằng chứng |
|---|---|---|
| `src/transitpulse/` | Package Python: static, ingestion, replay, processing, storage, api, runtime, contracts, obs, verify | `src/transitpulse/cli.py`, `docs/codebase-scan.txt` |
| `frontend/` | Console React/Vite: sáu màn, API client, MapLibre | `frontend/package.json`, `frontend/src/App.jsx` |
| `app/` | Dashboard Streamlit với views/components và HTTP API client | `app/streamlit_app.py`, `app/api_client.py` |
| `notebooks/` | Notebook Colab, UI helper, kernel requirements | `notebooks/TransitPulse_MBTA_Colab_Demo.ipynb`, `notebooks/colab_ui.py`, `notebooks/requirements-kernel.txt` |
| `config/` | Base/profile YAML | `src/transitpulse/config.py` |
| `deploy/` | Native runtime manifest và template Kafka/Cassandra/Spark | `deploy/runtime-manifest.json`, `deploy/native/kafka/server.properties.tmpl` |
| `docker/`, `docker-compose.yml` | Image ứng dụng và Compose profile | `docker/app.Dockerfile`, `docker-compose.yml` |
| `scripts/` | Bootstrap, doctor, infra, demo, export, verification, cấu hình GitHub repo, Spark/Kafka probe | `scripts/bootstrap_colab.sh`, `scripts/demo.py`, `scripts/probes/spark_kafka_probe.py` |
| `tests/` | Unit/runtime/Spark/integration, oracle, golden fixtures, report plugin | `tests/conftest.py`, `tests/oracle/scenario.py`, `tests/plugins/test_report.py` |
| `benchmarks/` | Kịch bản throughput/latency/recovery và sinh báo cáo từ evidence | `benchmarks/README.md`, `benchmarks/scenarios.yaml` |
| `docs/` | Thiết kế, BR, dictionary, query mapping, runbook, traceability, các tài liệu khảo sát | `docs/architecture.md`, `docs/business_requirements.md`, `docs/data_dictionary.md` |
| `reports/` | Báo cáo verification bàn giao hiện có | `reports/environment_verification.json` |
| `.github/workflows/` | CI và manual native integration | `.github/workflows/ci.yml`, `.github/workflows/integration.yml` |
| `pyproject.toml`, `Makefile`, `.env.example` | Manifest/lệnh/config mẫu | Các file tương ứng |
| `system design full.pdf` | Tài liệu thiết kế được README tham chiếu | `README.md`, `docs/codebase-scan.txt` |

README còn tham chiếu `TransitPulse_MBTA_Full_Build_Prompt_GitHub_Colab_VI.md`; file này không có trong cây khảo sát. `[TODO]` Khôi phục tài liệu gốc hoặc cập nhật liên kết (`README.md`, `docs/codebase-scan.txt`).

### 2) Điểm vào chương trình

| Entry | Chọn/chạy bằng | Bằng chứng |
|---|---|---|
| CLI chính `transitpulse.cli:main` | Script `transitpulse` được khai báo trong manifest; dispatch module/script bằng interpreter hiện tại | `pyproject.toml`, `src/transitpulse/cli.py` |
| Static/ingestion/replay | `python -m transitpulse.static`, `...ingestion`, `...replay` | `src/transitpulse/static/__main__.py`, `src/transitpulse/ingestion/__main__.py`, `src/transitpulse/replay/__main__.py` |
| Processor | `python -m transitpulse.processing.stream_job` | `src/transitpulse/cli.py`, `src/transitpulse/processing/stream_job.py` |
| API | `uvicorn transitpulse.api.app:create_app --factory --workers 1` | `src/transitpulse/cli.py`, `src/transitpulse/api/app.py` |
| Báo cáo batch | `python -m transitpulse.processing.report_job` | `src/transitpulse/cli.py`, `src/transitpulse/processing/report_job.py` |
| Native lifecycle | `scripts/infra.py`, `scripts/demo.py`, `scripts/doctor.py` wrappers | Các file tương ứng, `Makefile` |
| React | `frontend/index.html` → `frontend/src/main.jsx` → `frontend/src/App.jsx`; Vite scripts | `frontend/index.html`, `frontend/src/main.jsx`, `frontend/package.json` |
| Streamlit | `app/streamlit_app.py` | `Makefile`, `app/streamlit_app.py` |
| Colab | Notebook Run All, gọi cùng scripts và `colab_ui.py` | `notebooks/TransitPulse_MBTA_Colab_Demo.ipynb` |

### 3) Ranh giới module

Cột ranh giới là quy ước thiết kế được ghi trong repo và đối chiếu với entry hiện tại; không phải cơ chế kiểm tra import tự động cho toàn dự án (`docs/architecture.md`).

| Module | Trách nhiệm | Ranh giới thiết kế | Bằng chứng |
|---|---|---|---|
| `static/` | Version ZIP, validate/calendar/timezone/scope, Parquet dims, Cassandra catalog | Dims của run đã tạo giữ version cố định | `src/transitpulse/static/build.py`, `src/transitpulse/static/validation.py`, `src/transitpulse/static/dims.py`, `docs/architecture.md` |
| `ingestion/` | Fetch, raw, scope envelope, durable outbox, Kafka | KPI được xử lý ở processing | `src/transitpulse/ingestion/collector.py`, `src/transitpulse/processing/kpi.py` |
| `replay/` | Verify recording, source timeline/control, synthetic | Publisher phát input đã có; pipeline dùng cùng processing | `src/transitpulse/replay/publisher.py`, `src/transitpulse/replay/timeline.py` |
| `processing/` | Spark builders VP/TU/alerts/KPI/quality, publish generations, curated report | Serving query do API/storage quản lý | `src/transitpulse/processing/batch.py`, `src/transitpulse/processing/report_job.py` |
| `storage/` | Driver/migrations, manifest/pointer LWT, static writer | Cassandra schema theo query | `src/transitpulse/storage/migrations/0001_initial.cql`, `src/transitpulse/storage/pointer.py` |
| `api/` | Repository, request context, validation, envelope, freshness lúc đọc | UI lấy dữ liệu qua API | `src/transitpulse/api/context.py`, `src/transitpulse/api/repository.py`, `app/api_client.py`, `frontend/src/api/client.js` |
| `contracts/` | Envelope, IDs, enums, policy, accounting, topics | Python/Spark IDs phải tương thích | `src/transitpulse/contracts/ids.py`, `src/transitpulse/processing/spark_ids.py` |
| `runtime/` | Bootstrap, paths, process ownership, session/recovery/tunnel/export | Workdir tách source tree; stop giữ dữ liệu | `src/transitpulse/runtime/paths.py`, `src/transitpulse/runtime/supervisor.py`, `docs/runbook.md` |
| `obs/`, `verify/` | JSON logs/metrics/lineage; kiểm tra repo và luồng golden | Oracle kiểm thử độc lập với implementation | `src/transitpulse/obs/logging.py`, `src/transitpulse/verify/golden.py`, `tests/unit/test_oracle_isolation.py` |

### 4) Quy tắc tên và tổ chức

- Python chủ yếu `snake_case.py`: `http_fetch.py`, `scope_filter.py`, `gtfs_time.py`, `stream_job.py`, `memory_repository.py`; các thư mục chia theo giai đoạn pipeline/trách nhiệm (`docs/codebase-scan.txt`).
- React dùng PascalCase component/view files: `AppContext.jsx`, `TransitMap.jsx`, `OverviewView.jsx`, `VehicleHistoryView.jsx`; helper `format.js`, `api/client.js` (`frontend/src/App.jsx`).
- Python import theo package `transitpulse.*`; frontend dùng đường dẫn tương đối; Vite config không khai báo `resolve.alias` (`src/transitpulse/api/app.py`, `frontend/src/App.jsx`, `frontend/vite.config.js`). `[TODO]` Quy ước import/alias toàn nhóm ngoài cấu hình hiện có.
- Generated directories `.venv`, `node_modules`, `__pycache__`, `dist`, `.runtime`, cache/checkpoints/data/logs được gitignore. Không dùng các artifact này để suy ra coding conventions (`.gitignore`).
- Workdir gom raw/static/curated/outbox/checkpoints/runs/sessions/processes/metrics/logs/reports; checkpoint chia theo `<pipeline_version>/<run_id>/<query_name>` (`src/transitpulse/runtime/paths.py`).

### 5) Bằng chứng

- `docs/codebase-scan.txt`, `.gitignore`, `pyproject.toml`, `Makefile`.
- `src/transitpulse/cli.py`, `src/transitpulse/runtime/paths.py`, `src/transitpulse/processing/stream_job.py`, `src/transitpulse/api/app.py`.
- `frontend/index.html`, `frontend/src/main.jsx`, `frontend/src/App.jsx`, `frontend/vite.config.js`, `app/streamlit_app.py`.
- `docs/architecture.md`, `tests/conftest.py`, `tests/unit/test_oracle_isolation.py`.
