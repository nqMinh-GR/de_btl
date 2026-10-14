# TESTING — Kiểm thử, oracle và tình trạng bằng chứng

**Tình trạng đọc được:** `reports/environment_verification.json` là báo cáo bàn giao viết tay ngày 07/10/2026, `commit=null`, tests/full-stack/golden/live/replay `NOT_RUN`, managed Colab `NOT_VERIFIED`, benchmark `NO`. Đây là trạng thái của artifact, không kết luận mọi lần chạy sau đó đều thất bại hoặc chưa chạy. `.pytest_cache` không đủ chứng minh PASS ở commit hiện tại. Khảo sát này không chạy test/service.

### 1) Framework và lệnh

- pytest 9.1.1 + pytest-timeout 2.4.0; pytest strict markers; assert/fixtures/monkeypatch, HTTP `MockTransport`, FastAPI `TestClient` (`pyproject.toml`, `tests/unit/test_ingestion_http.py`, `tests/unit/test_api_endpoints.py`).
- Lệnh được khai báo, chưa thực thi trong lượt khảo sát:

```bash
make test                 # unit + runtime, không khởi động services
make test-spark           # local Spark/JDK; không Kafka/Cassandra
make test-integration    # native stack thật
make golden-smoke        # golden -> Kafka -> Spark -> Cassandra -> API
make lint
make check
make benchmark SCENARIO=stress_100,stress_500
```

Không có command coverage trong Makefile/pyproject; `[TODO]` chọn tool/threshold nếu cần. `make test` không bao gồm Spark/integration (`Makefile`).

### 2) Layout kiểm thử

| Khu vực | Nội dung/setup | Bằng chứng |
|---|---|---|
| `tests/unit/` | ID/policy/time/calendar, parsing/scope/outbox, API với fake repo, replay, oracle | `tests/unit/test_ids.py`, `tests/unit/test_gtfs_time.py`, `tests/unit/test_api_endpoints.py` |
| `tests/runtime/` | Checksum/safe extraction, bootstrap/process/session/reset/tunnel/export | `tests/runtime/test_checksums.py`, `tests/runtime/test_supervisor_identity.py`, `tests/runtime/test_tunnel.py` |
| `tests/spark/` | Spark DataFrame builders, parse/static loader, batch sink/publish/curated | `tests/spark/test_predictions.py`, `tests/spark/test_batch_sink.py`, `tests/spark/test_batch_curated.py` |
| `tests/integration/` | Real native stack: golden, recovery, isolation, TTL, outbox, source faults, offline | `tests/integration/conftest.py`, `tests/integration/test_recovery.py`, `tests/integration/test_generation_ttl.py` |
| `tests/fixtures/golden/` | Static/VP/TU/alerts synthetic, timeline, expected, provenance | `tests/fixtures/golden/PROVENANCE.md`, `tests/fixtures/golden/expected.json` |
| `tests/oracle/` | stdlib oracle tách implementation | `tests/oracle/gtfs.py`, `tests/oracle/realtime.py`, `tests/oracle/scenario.py` |
| `tests/plugins/` | IT/CT case report PASS/FAIL/NOT_RUN | `tests/plugins/test_report.py` |

File `test_*.py`; root `tests/conftest.py` tự mark unit/runtime/spark/integration theo directory. Spark fixture session-scoped local[1], UTC, one shuffle partition, UI disabled, driver 16g; clear cache sau mỗi test dùng Spark (`tests/conftest.py`, `tests/spark/conftest.py`). Integration fixture cần `TP_WORKDIR`/runtime usable, start/init Kafka+Cassandra, riêng evidence per testcase, skip nếu demo session đang active (`tests/integration/conftest.py`).

### 3) Ma trận phạm vi

“Có” nghĩa có mã kiểm thử, không nghĩa đã PASS.

| Phạm vi | Có mã? | Đối tượng | Giới hạn/bằng chứng |
|---|---|---|---|
| Unit | Có | Contracts, static helpers, ingestion, API, replay | `tests/unit/`, `tests/conftest.py`; HTTP script/fake repo không xác minh MBTA live |
| Runtime | Có | Lifecycle, bootstrap/checksum/reset/tunnel | `tests/runtime/`, `tests/runtime/fakes.py` |
| Spark transformations | Có | Vehicles, predictions/KPI/alerts/quality, sink/batch | `tests/spark/`; yêu cầu JDK/PySpark và driver 16g trong fixture |
| Integration | Có | IT01–IT16/native services, generation/recovery/offline | `tests/integration/cases.py`, `.github/workflows/integration.yml`; workflow dispatch thủ công |
| Golden end-to-end pipeline | Có | Raw/synthetic → Kafka/Spark/Cassandra/API so expected | `src/transitpulse/verify/golden.py`, `tests/integration/test_golden_flow.py` |
| Browser/UI E2E React | Chưa thấy suite trong inventory | 6view, auto-refresh/freshness/ETA/map | `frontend/package.json` chỉ dev/build/preview; `[TODO]` test UI behavior |
| Managed Colab Run All | Có notebook/checks, chưa có evidence PASS trong report | Bootstrap/tests/session/UI/tunnel/export | `notebooks/TransitPulse_MBTA_Colab_Demo.ipynb`, `reports/environment_verification.json` |
| Benchmark | Có harness | Load/visibility latency/resources/recovery | `benchmarks/README.md`; chưa có measured numbers trong report |

### 4) Mock và isolation

- HTTP tests dùng scripted `httpx.MockTransport`, inject RNG/sleep; phân biệt fake source lỗi với nguồn MBTA thật (`tests/unit/test_ingestion_http.py`, `tests/integration/fake_source.py`).
- API factory nhận settings/repository; `InMemoryRepository` tránh Cassandra ở unit scope, inject wall clock/page-token key (`src/transitpulse/api/app.py`, `src/transitpulse/api/memory_repository.py`, `tests/unit/test_api_endpoints.py`).
- Golden expected viết tay; oracle import constraints được kiểm tra bằng AST và subprocess để tránh tự kiểm tra implementation bằng chính implementation (`tests/fixtures/golden/PROVENANCE.md`, `tests/unit/test_oracle_isolation.py`).
- Integration dùng run/session IDs riêng, supervisor ownership và cleanup fixture; không tự dừng demo session của người dùng (`tests/integration/conftest.py`, `tests/integration/harness.py`).
- Skip do thiếu runtime/dependencies là `NOT_RUN`, không được đếm thành proof PASS (`tests/integration/conftest.py`, `tests/plugins/test_report.py`).

### 5) Coverage và quality signals

- `[TODO]` Coverage tool, threshold và current percentage chưa thấy trong manifest/CI; không báo số suy đoán (`pyproject.toml`, `.github/workflows/ci.yml`).
- CI push/PR chạy Ruff, unit/runtime và repo checks notebook/config/fixtures/scripts/compose/migrations. Manual integration chạy real stack + optional golden smoke. Notebook JSON/schema valid không xác minh Run All trên managed Colab (`.github/workflows/ci.yml`, `.github/workflows/integration.yml`).
- Notebook mặc định `STRICT_RUN_ALL=False`: một số bước test/golden/benchmark/export FAIL được ghi và UI cảnh báo nhưng Run All vẫn tiếp tục. Session/UI mở được không đủ điều kiện nghiệm thu (`notebooks/TransitPulse_MBTA_Colab_Demo.ipynb`).
- Các điểm chưa có runtime evidence trong báo cáo: route 10 live source-check, recording thật/replay, idempotent recovery/TTL, offline, Docker, Colab, benchmark. `[TODO]` Thu report theo commit/config/environment trước khi ghi PASS (`reports/environment_verification.json`, `docs/limitations.md`).
- Frontend có nguy cơ ETA không refresh khi giữ cùng stop và biểu diễn uncertainty chưa theo classifier backend; cần test hành vi nếu sửa. Đây là static-review concerns, chưa được reproduce (`frontend/src/views/StopsView.jsx`, `frontend/src/AppContext.jsx`, `src/transitpulse/contracts/policy.py`; xem `CONCERNS.md`).

### 6) Bằng chứng

- `pyproject.toml`, `Makefile`, `.github/workflows/ci.yml`, `.github/workflows/integration.yml`.
- `tests/conftest.py`, `tests/spark/conftest.py`, `tests/integration/conftest.py`, `tests/integration/harness.py`, `tests/plugins/test_report.py`.
- `tests/unit/test_ingestion_http.py`, `tests/unit/test_api_endpoints.py`, `tests/unit/test_oracle_isolation.py`, `tests/fixtures/golden/PROVENANCE.md`.
- `reports/environment_verification.json`, `notebooks/TransitPulse_MBTA_Colab_Demo.ipynb`, `benchmarks/README.md`, `frontend/package.json`.
