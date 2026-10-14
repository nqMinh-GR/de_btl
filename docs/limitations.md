# Giới hạn đã biết và điều chưa kiểm chứng

## Chưa kiểm chứng tại thời điểm bàn giao

* **Không có lệnh nào của dự án được chạy trên máy build** (yêu cầu của người dùng): unit tests, Spark tests,
  integration tests, golden smoke, benchmark, Docker, notebook Run All đều là `NOT_RUN`/`NOT_VERIFIED`.
  Lần thực thi đầu tiên là trên Google Colab; lỗi lần đầu là điều có thể xảy ra và notebook in exit code + log
  để sửa.
* `uv.lock` được sinh ở lần bootstrap Colab đầu tiên; cho tới khi commit lại, transitive dependencies có thể
  khác giữa hai lần bootstrap mới.
* Chưa có recording MBTA thật trong repository; mode `replay` cần recording do mode `live`/`record` tạo trước
  (hoặc bundle nhập vào). Chưa có benchmark đo thật — mọi mục tiêu (p95 < 10 s) là **target chưa đo**.
* Profile `local-docker` chưa được build/chạy (không có quyền Docker daemon trên máy build); digest image chưa ghi.
* Colab managed runtime: `NOT_VERIFIED` cho tới khi người dùng chạy notebook.

## Giới hạn thiết kế (cố ý)

* Kafka một broker, Cassandra một node, RF1: không high availability, không chịu mất host. Production cần
  topology, RF, consistency và capacity khác — không suy ra từ demo.
* Bảo đảm xử lý: at-least-once + ghi/publish idempotent; không exactly-once xuyên Kafka/Parquet/Cassandra.
* Pointer LWT kiểm soát visibility theo từng feed; không có transaction nguyên tử xuyên ba feed — response tổng
  hợp công bố snapshot/as-of của từng feed.
* TTL là retention theo wall clock, vẫn chạy khi replay pause; generation hết hạn trả `generation_expired`.
* ETA lấy từ MBTA; không có mô hình ETA riêng và không đánh giá độ chính xác ETA (chưa có ground truth).
  “Độ trễ dự kiến” không phải độ trễ thực tế hay on-time performance chính thức của MBTA.
* MBTA bus thường không có trường uncertainty → `unknown_uncertainty_count` gần bằng số chuyến đủ điều kiện
  trên dữ liệu live; policy chấp nhận và công bố số này.
* Số xe là số vehicle ID có quan sát hợp lệ trong snapshot đã lọc scope, không phải quy mô đội xe MBTA.
* Không suy luận tắc đường, số hành khách hay occupancy đầy đủ từ dữ liệu hiện có.
* Synthetic/stress data có nhãn `source_type=synthetic`; không bao giờ được gọi là xe MBTA thật.
* Demo local không có auth production: host ports bind 127.0.0.1, CORS allowlist; không public Kafka/Cassandra.
* **Cloudflare quick tunnel (Colab, bật mặc định):** dashboard Streamlit (chỉ đọc) được công khai qua một URL
  ngẫu nhiên `*.trycloudflare.com`; **không có xác thực** — ai có link đều xem được trong lúc session chạy. Kafka,
  Cassandra và các cổng khác không được mở. Quick tunnel của Cloudflare dành cho thử nghiệm, không có cam kết
  uptime; URL đổi mỗi lần tạo tunnel mới. Tắt bằng `ENABLE_CLOUDFLARE_TUNNEL = False` hoặc `demo.py expose stop`.
  Streamlit chạy sau tunnel với `server.enableCORS=false` và `server.enableXsrfProtection=false` (cần cho proxy;
  dashboard không có form ghi dữ liệu).
* Colab là môi trường demo theo phiên, có thể bị thu hồi; dữ liệu workdir mất khi VM reset — dựng lại run mới
  từ input bundle (không khôi phục checkpoint cũ vào Kafka/Cassandra rỗng).
* Bản đồ không dùng tile nền (offline-safe): là lược đồ tuyến theo tọa độ.
* PySpark/Spark 3.5.7 (phiên bản do đặc tả chốt) bị ảnh hưởng bởi CVE-2026-32773 (XSS ở Spark History Server,
  sửa trong 3.5.8). Dự án không bật Spark UI (`spark.ui.enabled=false`) và không chạy History Server, nên lỗ
  hổng không khai thác được trong demo; nâng lên 3.5.8 cần đổi đồng bộ pyspark, connector, manifest và đường
  dẫn cache connector.
* Tiến trình bị kill bởi timeout của cell notebook được dọn bằng handler SIGTERM; nếu VM bị thu hồi giữa chừng,
  chạy `demo.py stop` trước khi bắt đầu session mới.
