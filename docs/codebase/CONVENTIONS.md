# CONVENTIONS — Quy ước code và dữ liệu

Các quy ước dưới đây được đối chiếu config và source, không lấy từ generated output. Không suy ra toàn bộ coding standard của nhóm từ một file (`pyproject.toml`, `.gitignore`).

### 1) Quy tắc đặt tên

| Loại | Pattern quan sát/config | Ví dụ | Bằng chứng |
|---|---|---|---|
| Python file/hàm/biến | `snake_case`; helper nội bộ thường `_` prefix | `http_fetch.py`, `build_session`, `_force_utc` | `src/transitpulse/ingestion/http_fetch.py`, `src/transitpulse/processing/spark_session.py` |
| Python class/type | PascalCase | `ProjectConfig`, `BatchProcessor`, `RequestEnv` | `src/transitpulse/config.py`, `src/transitpulse/processing/batch.py`, `src/transitpulse/api/deps.py` |
| Constant/environment | UPPER_SNAKE_CASE | `RETRYABLE_STATUS`, `TP_WORKDIR`, `TP_API_TOKEN_KEY` | `src/transitpulse/ingestion/http_fetch.py`, `.env.example`, `src/transitpulse/api/settings.py` |
| Ngoại lệ có chủ đích | ID hash `H`, serializer `K`; Spark callback camelCase theo API | `H`, `K`, `onQueryProgress` | `src/transitpulse/contracts/ids.py`, `src/transitpulse/processing/stream_job.py` |
| React file/component | PascalCase JSX; helper JS tên thường/camelCase | `StopsView.jsx`, `TransitMap.jsx`, `apiGet` | `frontend/src/views/StopsView.jsx`, `frontend/src/components/TransitMap.jsx`, `frontend/src/api/client.js` |
| Data keys/IDs | `snake_case` trong JSON/CQL; run/snapshot/capture phân biệt | `run_id`, `snapshot_id`, `capture_id` | `src/transitpulse/contracts/envelope.py`, `src/transitpulse/storage/migrations/0001_initial.cql` |

### 2) Formatting và lint

- Ruff được cấu hình trong `pyproject.toml`: target `py311`, line length 110, rule sets `E/F/W/I/B/UP`; bỏ `E501`, `UP031`; notebook được miễn một số upgrade/import rules. CI chạy `ruff check .` (`pyproject.toml`, `.github/workflows/ci.yml`).
- `make lint` gọi `.venv/bin/python -m ruff check .`. Chưa thấy lệnh `ruff format` được yêu cầu trong Makefile/CI, nên không ghi formatter là bắt buộc (`Makefile`, `.github/workflows/ci.yml`). `[TODO]` Formatter thống nhất nếu nhóm cần.
- `.gitattributes` chỉ định LF cho Python/shell/CQL/YAML/JSON/Markdown/notebook và binary cho PDF/gzip/ZIP (`.gitattributes`).
- Frontend là JSX/JavaScript; `frontend/package.json` chưa có eslint/prettier/typecheck scripts; Vite config không khai báo TypeScript strictness. `[TODO]` Quy tắc lint/typecheck frontend và mức kiểm soát mong muốn (`frontend/package.json`, `frontend/vite.config.js`).

### 3) Import và module

- Python dùng `from __future__ import annotations`, imports stdlib/third-party/project theo nhóm trong các module đại diện; Ruff `I` kiểm tra import order (`src/transitpulse/config.py`, `src/transitpulse/api/app.py`, `pyproject.toml`).
- Python project imports dùng `transitpulse.*`; một số thư viện nặng/dịch vụ import trong function để trì hoãn initialization (`src/transitpulse/processing/spark_session.py`, `src/transitpulse/storage/cassandra.py`).
- Frontend dùng relative imports, chưa có alias config trong Vite (`frontend/src/App.jsx`, `frontend/vite.config.js`). Một số Python module dùng `__all__`, ví dụ static routes; `[TODO]` chưa có quy tắc export/barrel thống nhất được khai báo toàn repo (`src/transitpulse/static/routes.py`).
- Oracle là ngoại lệ kiểm thử có ràng buộc rõ: stdlib-only và sibling imports, không `transitpulse`/`pyspark`, kiểm tra bằng AST/subprocess (`tests/unit/test_oracle_isolation.py`).

### 4) Lỗi và logging

| Tầng | Quy ước | Bằng chứng |
|---|---|---|
| Fetch/collector | Phân loại timeout/connect/http/too_large; lỗi nguồn giữ status/raw evidence, không tạo snapshot rỗng | `src/transitpulse/ingestion/http_fetch.py`, `src/transitpulse/ingestion/collector.py` |
| Processor | Raise lỗi bound/count/manifest/pointer/parent; batch thất bại thay vì publish generation không hoàn chỉnh | `src/transitpulse/processing/batch.py` |
| Repository/API | Database failures thành HTTP 503 retryable; validation 422; missing scope/run404; unhandled 500 message chung | `src/transitpulse/api/repository.py`, `src/transitpulse/api/errors.py` |
| CLI | Một JSON kết quả stdout, progress/log stderr theo entry wrapper | `src/transitpulse/cli.py`, `src/transitpulse/processing/stream_job.py`, `Makefile` |
| UI | Client đọc `{data,meta,warnings}`; lỗi API có code/status/requestId và user message | `frontend/src/api/client.js`, `app/api_client.py` |

- JSON logging qua Python stdlib logging: `ts`, `level`, `component`, `run_id`, `session_id`, `feed`, `snapshot_id`, `capture_id`, `batch_id`, `stage`, `status`, `duration_ms`, `reason`, `msg`, `pid`. `excerpt` giới hạn text; `timed` dùng monotonic timer (`src/transitpulse/obs/logging.py`).
- Header allowlist ở HTTP fetch bỏ Authorization/Set-Cookie; page-token key `repr=False`; API unhandled errors không trả stacktrace (`src/transitpulse/ingestion/http_fetch.py`, `src/transitpulse/api/settings.py`, `src/transitpulse/api/errors.py`). Logger hiện **truncate** text, không có bộ redaction tự động cho mọi key/value; `[TODO]` quy tắc che dữ liệu nếu thêm credentials/input riêng (`src/transitpulse/obs/logging.py`).
- Dữ liệu dùng UTC trong Spark/API; lịch dịch vụ xử lý timezone GTFS riêng. Missing phải giữ `null`/reason; không đổi thiếu prediction thành đúng giờ (`src/transitpulse/processing/spark_session.py`, `src/transitpulse/static/gtfs_time.py`, `src/transitpulse/api/context.py`, `docs/clock_and_time.md`).

### 5) Quy ước kiểm thử

- File `test_*.py` dưới `tests/unit`, `tests/runtime`, `tests/spark`, `tests/integration`; root conftest tự gắn marker theo thư mục (`tests/conftest.py`).
- Unit tests sử dụng `pytest` assertions/fixtures, API `TestClient` + injected `InMemoryRepository`, HTTP `httpx.MockTransport`; fake response không được gọi là quan sát MBTA thật (`tests/unit/test_api_endpoints.py`, `tests/unit/test_ingestion_http.py`).
- Golden fixture synthetic viết tay, expected/oracle độc lập, source IDs không phải MBTA IDs thật (`tests/fixtures/golden/PROVENANCE.md`, `tests/unit/test_oracle_isolation.py`).
- `[TODO]` Chưa có coverage percentage/threshold được khai báo trong `pyproject.toml` hoặc workflows; frontend chưa có test script (`frontend/package.json`).

### 6) Bằng chứng

- `pyproject.toml`, `.gitattributes`, `.gitignore`, `Makefile`, `.github/workflows/ci.yml`.
- `src/transitpulse/contracts/ids.py`, `src/transitpulse/obs/logging.py`, `src/transitpulse/ingestion/http_fetch.py`.
- `src/transitpulse/api/errors.py`, `src/transitpulse/api/repository.py`, `src/transitpulse/api/settings.py`.
- `tests/conftest.py`, `tests/unit/test_ingestion_http.py`, `tests/unit/test_api_endpoints.py`, `tests/unit/test_oracle_isolation.py`, `tests/fixtures/golden/PROVENANCE.md`.
- `frontend/package.json`, `frontend/vite.config.js`, `frontend/src/api/client.js`.
