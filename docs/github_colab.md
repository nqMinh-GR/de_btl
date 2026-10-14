# Từ máy local tới Google Colab: push, Open in Colab, Run All

## 1. Push repository lên GitHub (người dùng tự làm)

Repo root là thư mục `DE_BTL/`. Tạo một **empty repository** trên GitHub (public để Colab clone không cần token),
rồi:

```bash
cd DE_BTL
git init
git add .
git diff --cached --stat          # kiểm tra: không có .venv/, .runtime/, *.zip lớn, .env, notebook outputs
git commit -m "Build TransitPulse MBTA pipeline and Colab demo"
git branch -M main
git remote add origin "https://github.com/<OWNER>/<REPO>.git"
git push -u origin main
```
Với repo đã có `.git`: đọc `git status`/`git remote -v` trước; không force push, không ghi đè remote.

Cập nhật badge và tham số notebook (không nhúng token):
```bash
python3 scripts/configure_repository.py --repo-url https://github.com/<OWNER>/<REPO>.git --ref main
git add README.md notebooks/TransitPulse_MBTA_Colab_Demo.ipynb && git commit -m "Configure Colab badge" && git push
```
`configure_repository.py` chỉ dùng thư viện chuẩn Python, chạy được trước khi có `.venv`.

## 2. Mở notebook

URL: `https://colab.research.google.com/github/<OWNER>/<REPO>/blob/main/notebooks/TransitPulse_MBTA_Colab_Demo.ipynb`

Cell tham số (cell 2):
```python
GITHUB_REPO_URL = "https://github.com/<OWNER>/<REPO>.git"
GIT_REF = "main"            # demo chính thức: điền commit SHA hoặc release tag
DEMO_MODE = "synthetic"     # synthetic | replay | live
TP_REPO_DIR = "/content/transitpulse-mbta"
TP_WORKDIR = "/content/transitpulse-runtime"
ROUTE_IDS = []              # live: 5 tuyến bus đã xác minh
RECORDING_MANIFEST = ""     # replay: đường dẫn recording_manifest.json
```
Runtime → Run all. Lần đầu cần Internet (clone, uv, Python 3.11, JDK 17, Kafka, Cassandra, PySpark ~317 MB,
connector JARs); bootstrap mất khoảng 10–15 phút, các lần chạy lại trong cùng VM dùng cache.

## 3. Các cell và kết quả cần quan sát

| # | Cell | Quan sát |
|---|---|---|
| 1 | Giới thiệu | Kiến trúc, modes, ý nghĩa KPI (mục tiêu ≠ kết quả đo) |
| 2 | Tham số | Kiểm tra mode/path/speed/duration ngay đầu |
| 3 | Clone/checkout | Repo path, **full commit SHA**, working tree sạch |
| 4 | Bootstrap | `bootstrap_report.json`: Python 3.11.17, JDK 17.0.20.1, Kafka 4.2.2, Cassandra 5.0.9, PySpark 3.5.7, connector verified |
| 5 | Kernel UI | Phiên bản kernel, ipywidgets (core vẫn ở `.venv`) |
| 6 | Doctor | CPU, RAM available, disk, ports, hash config/lock |
| 7 | Infra | Kafka metadata (AdminClient), Cassandra `release_version`, topics, migrations |
| 8 | Unit tests (tùy chọn) | Kết quả thật; lỗi không bị nuốt |
| 9 | Golden smoke (tùy chọn) | Kafka → Spark → Cassandra → API khớp oracle (fresh 1/stale 1/unknown 1; eligible 2, late 1, rate 0,5, mean 210 s) |
| 10 | Benchmark (tùy chọn) | Report riêng, có giới hạn thời gian |
| 11 | Chuẩn bị input | Static version, scope, checksum input |
| 12 | Start session | JSON `session_id`, `run_id`, API URL, log paths |
| 13 | Wait ready | Query progress, generation READY, `/health/ready` |
| 14 | Giao diện 5 tab | Tổng quan, Bản đồ, Trạm & ETA, Cảnh báo, Chất lượng — banner nguồn + clock |
| 15 | Lineage | Snapshot ID → offsets Kafka → batch → Cassandra → API |
| 16 | Kịch bản tương tác | Duplicate / late / stale / restart: expected vs actual (mặc định NOT_RUN) |
| 17 | Export | ZIP + checksum + manifest completeness; tải xuống / copy Drive (tùy chọn) |
| 18 | Stop | Dừng đúng tiến trình của dự án; giữ dữ liệu; kernel không bị kill |

## 3b. Console React qua Cloudflare Tunnel

Với `ENABLE_CLOUDFLARE_TUNNEL = True` (mặc định), sau khi session sẵn sàng, notebook chạy
`demo.py expose start --target react` (`TUNNEL_TARGET = "react"`). Lệnh này làm ba việc:
1. Phục vụ bản build React bằng `vite preview` ở `127.0.0.1:3000`. Trang gọi `/api/v1` trên cùng origin;
   Vite chuyển tiếp tới FastAPI ở `127.0.0.1:8000`.
2. Khởi động `cloudflared tunnel --url http://127.0.0.1:3000`, tạo một quick tunnel và đọc URL công khai
   `https://<ngẫu-nhiên>.trycloudflare.com` từ log.
3. In link để bấm. URL được lưu ở `TP_WORKDIR/runtime/public_urls.json`.

Bootstrap cài Node.js Linux x64 đã ghim rồi `npm ci` và `npm run build` trong `frontend/` với
`VITE_API_BASE=/api/v1`. `--no-tunnel` chỉ bỏ qua `cloudflared`; bản build UI vẫn được tạo.

Các thao tác khác:
* `TUNNEL_TARGET = "dashboard"` mở Streamlit ở `127.0.0.1:8501`. `"api"` mở FastAPI (`/api/v1/...`, `/docs`)
  qua một tunnel riêng. `"both"` mở dashboard và API. `"all"` mở dashboard, API và React.
* Console, dashboard và tunnel giữ nguyên khi đổi session hoặc mode, nên link không đổi trong suốt phiên Colab.
* `demo.py expose status` kiểm tra lại link. `demo.py expose stop --target all` tắt mọi link công khai.
  `demo.py stop --all` tắt tất cả.

Giới hạn:
* Quick tunnel không cần tài khoản Cloudflare và không có cam kết uptime.
* Mỗi lần tạo lại tunnel thì URL đổi.
* Link là công khai, không đăng nhập: ai có link đều xem được console chỉ đọc. Tile bản đồ do trình duyệt
  khách tải từ OpenStreetMap/CARTO.

`cloudflared` được pin ở phiên bản 2026.10.0 và kiểm tra SHA-256 theo release notes chính thức.
Node.js được pin ở phiên bản 22.21.0 và kiểm tra SHA-256 theo `SHASUMS256.txt` chính thức.

## 4. Khi một cell lỗi

Mỗi cell gọi script bằng Python tuyệt đối của `.venv` và dừng khi exit code ≠ 0, in `error.code`, `hint` và
đoạn log. Gửi lại đoạn đó để sửa. Chạy lại cell an toàn: bootstrap dùng cache, `infra start` tái sử dụng tiến
trình đang sống (không format lại Kafka, không xóa dữ liệu), `demo start` trả lại session hiện có.

## 5. Sau lần chạy đầu thành công

* Tải evidence ZIP (cell 17). Nếu `uv.lock` được sinh trên Colab (`lock_origin=generated_on_colab`), lấy
  `uv.lock` trong ZIP, đặt vào repo root, commit và push — các lần sau bootstrap dùng `uv sync --locked` với lock đã commit.
* Cập nhật `reports/environment_verification.json` bằng `scripts/verify_environment.py` (chỉ ghi PASS khi có bằng chứng).

## 6. Rebuild sau khi VM reset

Clone đúng commit → bootstrap → infra start/init → nhập input bundle (static ZIP + recording) → run mới,
checkpoint mới. Không khôi phục checkpoint cũ vào Kafka/Cassandra rỗng; so sánh kết quả logic bằng oracle.
