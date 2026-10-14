# Offline readiness checklist (theo profile)

Ba mức khác nhau, không được đánh đồng:
1. **Input không phụ thuộc MBTA live** (synthetic/replay): vẫn cần Internet cho setup lần đầu.
2. **Đã bootstrap trong phiên, tắt truy cập ngoài để demo**: pipeline dùng input/cache/assets sẵn có.
3. **Cold offline setup từ bundle chuẩn bị trước**: cần binaries/JDK/Python/wheels/JARs/input/assets đúng OS — ngoài phạm vi mặc định.

| Điều kiện | colab-native / local-native | local-docker | Bằng chứng |
|---|---|---|---|
| Binaries + Python env | `tools/` (JDK, Kafka, Cassandra, uv, Python) + `.venv` đã có | images đã pull | `bootstrap_report.json` / `docker image ls` digests |
| Spark Kafka connector | JAR trong `cache/jars/spark-kafka-3.5.7` + `connector-lock.json`; probe `--jars` không mạng PASS | trong image | bootstrap B10 probe |
| Static + recording | static ZIP đúng `gtfs_version`, recording checksum khớp | volume | `replay verify` JSON |
| UI không cần CDN | notebook UI dùng SVG tự chứa; Streamlit/PyDeck không tile (`map_style=None`) | như trên | ảnh chụp/kiểm tra khi tắt mạng |
| Clock & TTL | pause test, stale test, generation expiry test | như trên | IT04, IT14, IT15 |

Cách thử (ghi phương pháp thực tế vào report): sau khi bootstrap xong, chạy replay/synthetic khi chặn
outbound của các tiến trình dự án (ví dụ network namespace trên Linux, hoặc ngắt mạng máy). Không gọi MBTA
trong replay **không** chứng minh mọi dependency/UI đều offline. Trạng thái hiện tại: **NOT_RUN**.
