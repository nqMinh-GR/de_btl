# Phiên bản và trạng thái kiểm chứng

Không dùng `latest` cho bất kỳ dependency, image hay artifact nào. Trạng thái dưới đây là **tại thời điểm bàn
giao**; chưa có lệnh nào của dự án được chạy trên máy build (yêu cầu của người dùng). Lần chạy đầu trên Colab
cập nhật `bootstrap_report.json` và `reports/environment_verification.json`.

## Bộ tham chiếu

| Thành phần | Phiên bản | Nguồn / ghi chú | Trạng thái |
|---|---|---|---|
| Python core | 3.11.17 (`.python-version`) | uv-managed (bản 3.11 mới nhất trong `download-metadata.json` của uv 0.12.23) | NOT_VERIFIED |
| uv | 0.12.23 | GitHub release + `.sha256` | URL_CONFIRMED |
| JDK | Eclipse Temurin 17.0.20.1+1 | Adoptium API (checksum lấy lúc bootstrap) | URL_CONFIRMED |
| Spark / PySpark | 3.5.7, Scala 2.12 | PyPI sdist `pyspark==3.5.7` | NOT_VERIFIED |
| Spark Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7` (+ `spark-token-provider-kafka-0-10_2.12:3.5.7`, `kafka-clients:3.4.1`, `commons-pool2:2.11.1`) | Maven Central, resolve bằng Ivy khi bootstrap, ghi `connector-lock.json` | PENDING_FIRST_RESOLVE |
| Kafka broker | 4.2.2 (Scala 2.13 binary, JVM riêng) | downloads.apache.org, fallback archive.apache.org, `.sha512` | URL_CONFIRMED |
| Cassandra | 5.0.9 | downloads.apache.org, fallback archive.apache.org, `.sha256` | URL_CONFIRMED |
| confluent-kafka | 2.15.1 (wheel cp311 manylinux) | PyPI | NOT_VERIFIED |
| cassandra-driver | 3.30.1 (wheel cp311) | PyPI | NOT_VERIFIED |
| FastAPI / Pydantic / uvicorn | 0.142.2 / 2.13.5 / 0.54.0 | PyPI | NOT_VERIFIED |
| httpx | 0.28.1 | PyPI | NOT_VERIFIED |
| Streamlit / PyDeck (extra `dashboard`) | 1.65.0 / 0.9.3 | PyPI | NOT_VERIFIED |
| pytest / ruff / nbformat (group `dev`) | 9.1.1 / 0.16.10 / 5.11.1 | PyPI | NOT_VERIFIED |
| Docker images (profile local-docker) | `apache/kafka:4.2.2`, `cassandra:5.0.9`, `eclipse-temurin:17-jdk-jammy` | digest chưa ghi (không có quyền Docker) | NOT_RUN |

`URL_CONFIRMED` nghĩa là: ngày 06/10/2026, trong lúc lập kế hoạch, đã kiểm tra bằng HTTP GET (chỉ đọc danh mục,
không tải artifact) rằng artifact và file checksum chính thức tồn tại tại URL ghi trong manifest
(`downloads.apache.org/kafka/4.2.2/`, `downloads.apache.org/cassandra/5.0.9/`, GitHub release uv 0.12.23,
Adoptium API cho Temurin 17.0.20.1+1, `pyspark-3.5.7` trên archive/PyPI, POM connector trên Maven Central).
Checksum của artifact **chưa** được tải về hay so khớp; việc đó xảy ra ở bootstrap và chỉ ghi vào
`bootstrap_report.json`.

## Lockfile

`uv.lock` **chưa tồn tại** khi bàn giao vì không được chạy `uv lock` trên máy build. `pyproject.toml` pin `==`
cho mọi dependency trực tiếp; `requires-python = "==3.11.*"`. Bootstrap trên Colab:

1. Nếu `uv.lock` thiếu → `uv lock` (ghi `lock_origin=generated_on_colab` vào `runtime.json`).
2. `uv sync --locked` (dùng đúng lock vừa sinh hoặc lock đã commit).
3. `export_artifacts.py` đưa `uv.lock` vào evidence ZIP — hãy commit file này vào repository để các lần sau
   tái lập chính xác (`lock_origin=committed`).

## Tương thích đã đối chiếu tài liệu (chưa chạy)

* Kafka broker 4.x yêu cầu Java 17+; `kafka-clients 3.4.1` của connector Spark nằm trong dải client được broker
  4.x hỗ trợ (KIP-896 chỉ loại client rất cũ).
* Cassandra 5.0 chạy JDK 17 với G1 (`jvm17-server.options`): chỉ đặt `MAX_HEAP_SIZE`, không đặt `HEAP_NEWSIZE`.
* Spark 3.5.7 hỗ trợ Java 8/11/17 và Python 3.8–3.11; connector phải đúng Scala 2.12 + Spark 3.5.7.
* cassandra-driver trên Python 3.12+ không có `asyncore`; core dùng Python 3.11.

Bộ phiên bản chỉ được ghi là `VERIFIED` sau khi smoke test producer → Spark Kafka reader → Cassandra → API
chạy thành công (golden smoke trên Colab).
