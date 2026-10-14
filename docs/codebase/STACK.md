# STACK — Công nghệ và môi trường TransitPulse MBTA

Phạm vi: khảo sát mã nguồn/config ngày 14/10/2026, commit `fe50c680569b5b4635ebbb0a8032ec6fc8367be8` theo terminal `git log -1`. Phiên bản dưới đây là **khai báo/pin**, không xác nhận phần mềm đã cài hoặc chạy thành công. Báo cáo bàn giao ngày 07/10/2026 vẫn ghi `NOT_RUN`/`NOT_VERIFIED` (`reports/environment_verification.json`).

### 1) Tổng quan runtime

| Hạng mục | Giá trị khai báo | Bằng chứng |
|---|---|---|
| Ngôn ngữ chính | Python; frontend JavaScript/JSX, cấu hình YAML, schema CQL | `pyproject.toml`, `frontend/src/App.jsx`, `config/base.yaml`, `src/transitpulse/storage/migrations/0001_initial.cql` |
| Python core | `requires-python ==3.11.*`; `.python-version` chọn `3.11.17` | `pyproject.toml`, `.python-version` |
| JVM | Eclipse Temurin `17.0.20.1+1` | `deploy/runtime-manifest.json` |
| Node.js | Bootstrap pin `22.21.0`; frontend README yêu cầu Node ≥18 | `deploy/runtime-manifest.json`, `frontend/README.md` |
| Package manager | uv `0.12.23` cho Python; npm và `package-lock.json` cho React | `deploy/runtime-manifest.json`, `.github/workflows/ci.yml`, `frontend/package-lock.json` |
| Build/module | Python `src` package `transitpulse`, backend `uv_build>=0.12.23,<0.13`; frontend ESM + Vite | `pyproject.toml`, `frontend/package.json` |
| Nền tảng native | Manifest nhắm Linux x86_64/glibc; profile `local-native`, `colab-native`; có thêm `local-docker` | `deploy/runtime-manifest.json`, `src/transitpulse/config.py` |

### 2) Framework và dependency runtime

Tất cả 11 dependency Python trực tiếp được pin `==`; đây là version trong manifest, chưa phải kết quả resolve transitive dependencies (`pyproject.toml`, `docs/versions.md`).

| Dependency | Phiên bản | Vai trò | Bằng chứng |
|---|---|---|---|
| pyspark | 3.5.7 | Static batch, Structured Streaming, báo cáo batch | `pyproject.toml`, `src/transitpulse/processing/spark_session.py` |
| confluent-kafka | 2.15.1 | Producer/admin Kafka | `pyproject.toml`, `src/transitpulse/ingestion/kafka_producer.py` |
| cassandra-driver | 3.30.1 | Cassandra read/write và LWT | `pyproject.toml`, `src/transitpulse/storage/cassandra.py` |
| fastapi | 0.142.2 | API phục vụ dữ liệu | `pyproject.toml`, `src/transitpulse/api/app.py` |
| uvicorn | 0.54.0 | ASGI server một worker | `pyproject.toml`, `src/transitpulse/cli.py` |
| pydantic | 2.13.5 | Config và response models | `pyproject.toml`, `src/transitpulse/config.py`, `src/transitpulse/api/models.py` |
| httpx | 0.28.1 | Fetch nguồn và client API Python | `pyproject.toml`, `src/transitpulse/ingestion/http_fetch.py`, `app/api_client.py` |
| pyyaml | 6.0.3 | Đọc config | `pyproject.toml`, `src/transitpulse/config.py` |
| prometheus-client | 0.26.0 | Metric textfiles/endpoint | `pyproject.toml`, `src/transitpulse/obs/metrics.py` |
| psutil | 7.2.2 | Runtime/process/resource sampling | `pyproject.toml`, `benchmarks/samplers.py` |
| tzdata | 2026.5 | Dữ liệu timezone | `pyproject.toml`, `src/transitpulse/static/gtfs_time.py` |
| streamlit, pydeck | 1.65.0, 0.9.3 | Dashboard Python, extra `dashboard` | `pyproject.toml`, `app/streamlit_app.py` |

| Thành phần ngoài Python | Pin/range khai báo | Vai trò | Bằng chứng |
|---|---|---|---|
| Kafka broker | 4.2.2, binary Scala 2.13 | Broker KRaft | `deploy/runtime-manifest.json`, `deploy/native/kafka/server.properties.tmpl` |
| Cassandra | 5.0.9 | Serving database | `deploy/runtime-manifest.json` |
| Spark Kafka connector | `spark-sql-kafka-0-10_2.12:3.5.7`; closure `spark-token-provider-kafka-0-10_2.12:3.5.7`, `kafka-clients:3.4.1`, `commons-pool2:2.11.1` | Spark Kafka reader; jars local sau bootstrap | `deploy/runtime-manifest.json`, `src/transitpulse/processing/spark_session.py` |
| cloudflared | 2026.10.0 | Quick tunnel demo | `deploy/runtime-manifest.json`, `src/transitpulse/runtime/access.py` |
| react, react-dom | `^18.3.1`, `^18.3.1` | Console React | `frontend/package.json` |
| maplibre-gl | `^5.24.0` | Bản đồ | `frontend/package.json`, `frontend/src/components/TransitMap.jsx` |
| lucide-react | `^1.16.0` | Icons | `frontend/package.json` |
| clsx, tailwind-merge | `^2.1.1`, `^2.5.4` | Ghép class UI | `frontend/package.json` |

Range `^` của frontend là ràng buộc khai báo; phiên bản resolve được lưu ở `frontend/package-lock.json`. Không đồng nhất chúng với pin `==` của Python.

### 3) Toolchain phát triển

| Công cụ | Mục đích | Bằng chứng |
|---|---|---|
| pytest 9.1.1, pytest-timeout 2.4.0 | Unit/runtime/Spark/integration test | `pyproject.toml`, `tests/conftest.py` |
| Ruff 0.16.10 | Lint/import-order; target Python 3.11 | `pyproject.toml`, `Makefile` |
| nbformat 5.11.1 | Kiểm tra schema notebook | `pyproject.toml`, `.github/workflows/ci.yml` |
| Vite `^5.4.10`, @vitejs/plugin-react `^4.3.3` | Dev/build/preview frontend | `frontend/package.json`, `frontend/vite.config.js` |
| Tailwind CSS `^3.4.14`, PostCSS `^8.4.47`, Autoprefixer `^10.4.20` | CSS build | `frontend/package.json`, `frontend/postcss.config.js` |
| @types/react `^18.3.12`, @types/react-dom `^18.3.1` | Type declarations ở dev dependencies; mã frontend đang là JSX | `frontend/package.json`, `frontend/src/App.jsx` |
| GNU Make ≥3.82 | Giao diện lệnh với recipe prefix `>` | `Makefile` |
| GitHub Actions | CI push/PR và integration dispatch thủ công | `.github/workflows/ci.yml`, `.github/workflows/integration.yml` |

### 4) Lệnh chính

Các lệnh sau được lấy từ README/Makefile, **chưa thực thi trong lượt khảo sát này**:

```bash
bash scripts/bootstrap_colab.sh --workdir "$PWD/.runtime" --profile local-native
make doctor PROFILE=local-native
make infra-up PROFILE=local-native
make init PROFILE=local-native
make test
make test-spark
make test-integration
make lint
make check
# Chạy trong frontend/ sau khi có Node/npm:
npm ci
npm run build
```

Native `make build` gọi bootstrap; Docker build yêu cầu `uv.lock`. `make react-dev`/`make react-build` gọi frontend scripts; React không có script test trong `frontend/package.json`.

### 5) Môi trường và config

- Thứ tự merge: `config/base.yaml` → YAML profile → environment → overrides explicit; model cấm key lạ và frozen. Policy được lưu vào run metadata (`src/transitpulse/config.py`, `src/transitpulse/runs.py`).
- Template cấu hình: `.env.example` với `TP_PROFILE`, `TP_WORKDIR`, `TP_ROUTE_IDS`, `TP_KAFKA_BOOTSTRAP`, `TP_CASSANDRA_HOSTS`, `TP_API_URL`; generic override `TP__<section>__<key>`. Frontend dùng `VITE_API_BASE` (`src/transitpulse/config.py`, `frontend/.env.example`). Các giá trị template không phải tất cả đều bắt buộc; **live cần scope route IDs có kiểm chứng**, còn synthetic có route riêng.
- Base: Spark `local[2]`, driver `1536m`; Colab hiện khai báo driver `16g`, Kafka heap `1g`, Cassandra heap `4G` + direct `2G`, chú thích VM khoảng 51 GiB (`config/base.yaml`, `config/colab.yaml`). Đây là budget cấu hình, không phải resource measurement.
- Dockerfile base JDK `eclipse-temurin:17.0.20.1_1-jdk-jammy`, uv image `ghcr.io/astral-sh/uv:0.12.23`, Python uv-managed; chạy user `tp` UID 10001 (`docker/app.Dockerfile`). Docker và native đều chưa có bằng chứng chạy trong báo cáo bàn giao.
- `[TODO]` Commit `uv.lock` sau lần resolve có bằng chứng; file chưa có trong cây khảo sát. Nhiều checksum runtime còn `null`, connector `PENDING_FIRST_RESOLVE` (`deploy/runtime-manifest.json`, `docs/versions.md`).

### 6) Bằng chứng

- `pyproject.toml`, `.python-version`, `frontend/package.json`, `frontend/package-lock.json`.
- `config/base.yaml`, `config/colab.yaml`, `config/local.yaml`, `config/docker.yaml`, `.env.example`, `frontend/.env.example`.
- `deploy/runtime-manifest.json`, `docker/app.Dockerfile`, `Makefile`, `.github/workflows/ci.yml`, `.github/workflows/integration.yml`.
- `reports/environment_verification.json`, `docs/versions.md`, `docs/codebase-scan.txt`.
