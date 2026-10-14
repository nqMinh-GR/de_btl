# TransitPulse — console React

Giao diện chỉ đọc cho pipeline MBTA (Kafka → Spark → Cassandra → FastAPI). Sáu màn: tổng quan, bản đồ, trạm, cảnh báo, chất lượng, lịch sử xe. Số liệu đến từ `GET /api/v1`, không còn dữ liệu giả.

## Chạy local

Cần API ở `http://127.0.0.1:8000` và Node.js >= 18.

```bash
make react-dev
```

Vite phục vụ `http://127.0.0.1:3000` và chuyển `/api` tới FastAPI. `VITE_API_BASE` mặc định là `/api/v1` (cùng origin). Đặt biến này chỉ khi cần gọi thẳng một host khác; xem `frontend/.env.example`.

```bash
make react-build
```

## Bản đồ

MapLibre GL vẽ nền đường phố tối CARTO Dark (dữ liệu OpenStreetMap). Xe, trạm và shape tuyến dùng lat/lon thật từ API. Vết di chuyển live nối các vị trí liên tiếp của cùng một xe mỗi lần poll 5 giây. Lịch sử xe vẽ polyline từ `GET /vehicles/{id}/history`.

## Colab

Bootstrap cài Node đã ghim, chạy `npm ci` và `npm run build` với `VITE_API_BASE=/api/v1`. Notebook mặc định `TUNNEL_TARGET = "react"`: `transitpulse.runtime.react_preview` phục vụ `dist/` ở cổng 3000 (và chuyển `/api` sang FastAPI) rồi mở Cloudflare quick tunnel. Mở link `https://….trycloudflare.com`. Link công khai, không đăng nhập; API không được đưa ra URL riêng vì trang gọi `/api/v1` qua proxy cùng origin. Tile bản đồ do trình duyệt khách tải từ CARTO/OSM.
