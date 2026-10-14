# Kịch bản demo ~10 phút

Chuẩn bị trước buổi trình bày (không phụ thuộc một tuyến chắc chắn có xe mọi giờ):
1. Colab: Run All với `DEMO_MODE="synthetic"` tới cell 13 (session READY) — hoặc local native tương đương.
2. Nếu muốn dữ liệu thật: chạy trước `DEMO_MODE="live"` (600 s) để có recording, dùng làm `replay` dự phòng.
3. `doctor` PASS, `source-check` có kết quả thật (hoặc ghi rõ chưa kiểm tra).

| Thời lượng | Nội dung | Thao tác |
|---|---|---|
| 1 phút | Bài toán, người dùng, bốn nguồn MBTA | README §Nguồn; mở `reports/source_probe_report.json` (HTTP status, tuổi header, số entity, tuyến bus) |
| 2 phút | Tuyến/trạm, bản đồ xe, ETA qua API/UI | Notebook tab *Bản đồ* và *Trạm & ETA* (Colab) hoặc Streamlit; chỉ banner LIVE/REPLAY/SYNTHETIC, `application_clock`, `served_at`; `curl "127.0.0.1:8000/api/v1/stops/<stop>/predictions?run_id=<run>"` và giải thích `meta.feeds` |
| 1 phút | Cùng dữ liệu qua raw → Kafka → Spark → Cassandra | `demo.py lineage --feed vehicle_positions`: raw path + sha256 khớp → topic/partition/offset → batch_id → manifest READY → API cùng `snapshot_id` |
| 1 phút | KPI “độ trễ dự kiến” | Tab *Tổng quan*: “Độ trễ dự kiến ≥ 5 phút: X trên Y chuyến đủ điều kiện”, `metrics_as_of`, target policy, lý do loại; JSON `/routes/{id}/metrics` |
| 2 phút | Duplicate + late snapshot + dữ liệu lỗi | Golden run (`demo.py golden-smoke` hoặc nút trong cell 16): phát lại VP2 (counts/history không đổi), phát VP1 trễ (pointer giữ VP2), GPS sai (`INVALID_COORDINATES` trong *Chất lượng*) — mỗi bước in expected/actual |
| 1 phút | Đóng băng feed / tiến clock → stale | `demo.py advance-clock --to 2026-10-05T12:03:10Z`: V1 tuổi 135 s → stale, fresh 0, KPI dự báo gắn stale; `demo.py pause` cho thấy clock đứng, heartbeat wall vẫn chạy |
| 1 phút | Restart processor | `demo.py restart-processor`: batch id tiếp tục, pointer giữ nguyên, số dòng history không đổi |
| 1 phút | Bằng chứng & giới hạn | `test_report.json` (PASS/FAIL/NOT_RUN thật), golden smoke report, benchmark (nếu đã đo) với target tách khỏi kết quả, `docs/limitations.md` (1 node, RF1, không ground truth ETA) |

Kết quả kỳ vọng của golden (synthetic, không phải MBTA): bước S1 — fresh 1 (V1 tuổi 20 s), stale 1 (V2 210 s),
unknown 1 (V3); 2 chuyến đủ điều kiện, 1 chuyến trễ dự kiến (T1 +360 s, T2 +60 s), tỷ lệ 0,5, trung bình 210 s.
Đây là **giá trị kỳ vọng của fixture**, chỉ là kết quả đo khi `golden-smoke` báo PASS.
