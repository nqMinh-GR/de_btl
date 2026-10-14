# Runbook vận hành và xử lý sự cố

Mọi lệnh chạy từ repo root với interpreter core `.venv/bin/python` (Python 3.11 do uv quản lý). Biến
`TP_WORKDIR` là work root (Colab: `/content/transitpulse-runtime`; local: `./.runtime`). Mọi CLI in **một JSON**
ra stdout và tiến trình ra stderr; mã thoát: 0 ok, 2 usage, 3 preflight, 4 conflict, 5 readiness/exit,
6 mode không hỗ trợ, 7 artifact/checksum, 1 lỗi khác.

## 1. Khởi động có kiểm soát

```bash
export TP_WORKDIR=/content/transitpulse-runtime            # Linux: ./.runtime và thay mọi `colab-native` bên dưới bằng `local-native`
bash scripts/bootstrap_colab.sh --workdir "$TP_WORKDIR"     # Linux thêm: --profile local-native (profile phải trùng với các lệnh sau)
.venv/bin/python scripts/doctor.py --profile colab-native --workdir "$TP_WORKDIR"
.venv/bin/python scripts/infra.py start --profile colab-native --workdir "$TP_WORKDIR"   # Kafka KRaft + Cassandra
.venv/bin/python scripts/infra.py init  --profile colab-native --workdir "$TP_WORKDIR"   # topics + CQL migrations
.venv/bin/python scripts/demo.py start --profile colab-native --mode synthetic --seed 42 \
    --duration-seconds 600 --workdir "$TP_WORKDIR" --detach                            # JSON: session_id, run_id, api ...
.venv/bin/python scripts/demo.py wait-ready --workdir "$TP_WORKDIR"
.venv/bin/python scripts/demo.py status --workdir "$TP_WORKDIR"
```
Thứ tự nội bộ của `start`: inputs đã kiểm (static version, scope, checksums) → tạo run (`run_metadata`) →
processor Spark với checkpoint của run → chờ `processor_ready.json` → publisher/collector → API `/health/ready`.

Chế độ khác:
```bash
# Live MBTA: chọn tuyến bus đã xác minh trước
.venv/bin/python -m transitpulse.ingestion source-check --workdir "$TP_WORKDIR"
.venv/bin/python -m transitpulse.static list-routes --workdir "$TP_WORKDIR" --download --check-realtime
.venv/bin/python scripts/demo.py start --mode live --route-ids <5 route IDs> --duration-seconds 600 --workdir "$TP_WORKDIR" --detach
# Replay recording thật (đúng static version, không gọi HTTP)
.venv/bin/python scripts/demo.py start --mode replay --recording-manifest <path>/recording_manifest.json --speed 10 --workdir "$TP_WORKDIR" --detach
```

## 2. Dừng

```bash
.venv/bin/python scripts/demo.py stop --workdir "$TP_WORKDIR"          # publisher → drain → processor → API; giữ dữ liệu
.venv/bin/python scripts/demo.py stop --all --workdir "$TP_WORKDIR"    # thêm Kafka (graceful) và Cassandra (nodetool drain)
```
Stop không bao giờ xóa volumes/checkpoints/raw. Reset là lệnh riêng, mặc định chỉ liệt kê (dry-run) và cần
`--confirm <token>`; chỉ tác động đường dẫn bên trong `TP_WORKDIR`.

## 3. Bảng sự cố

| Hiện tượng | Kiểm tra / hành động | Không được làm |
|---|---|---|
| Một feed timeout | `pipeline_status` (`collector:<feed>`), HTTP status, retry, header cuối thành công; view giữ nguyên với badge stale | Publish snapshot rỗng để “reset” trạng thái |
| Outbox tăng | `python -m transitpulse.ingestion outbox-status --workdir "$TP_WORKDIR" --run-id R`; kết nối Kafka, ACK, kích thước payload | Đánh dấu published trước ACK |
| Raw disk gần đầy | `python -m transitpulse.ingestion disk-check --workdir "$TP_WORKDIR"`; budget 2 GiB fail-stop; dừng capture nếu không lưu bền được | Tiếp tục publish rồi báo audit đầy đủ |
| Spark batch fail | `demo.py logs --component processor`, `query_progress.jsonl`, health Cassandra; sửa lỗi rồi `demo.py restart-processor` (cùng checkpoint) | Nuốt exception, tạo checkpoint mới ngẫu nhiên |
| Cassandra unavailable | `infra.py status`, `infra.py logs --component cassandra --file system`; phục hồi node rồi restart processor | Trả KPI 0 thay cho 503 |
| Generation hết hạn | `/runs/{id}` → `expires_at`; báo unavailable, chạy tiếp hoặc tạo run mới | Kéo dài TTL bằng duplicate ngầm |
| Kafka offsets đã mất (retention) | `failOnDataLoss=true` dừng query; replay recording vào **run mới** | Tắt `failOnDataLoss` để báo thành công |
| Replay thiếu static | `python -m transitpulse.replay verify --workdir "$TP_WORKDIR" --manifest ...`; nhập đúng ZIP theo checksum | Join với ZIP live mới tải |
| Port đã bị chiếm | `infra.py status` / lỗi `PORT_IN_USE` hiển thị PID + cmdline; dừng tiến trình của bạn | `pkill java`, `killall python` |
| Processor thoát mã 86 | Fault injection đã kích hoạt (arm file one-shot) — `demo.py restart-processor` | Xóa checkpoint |
| Processor bị kill (137) | Có thể OOM: `demo.py status` (MemAvailable), giảm scope/tốc độ replay | Bỏ Cassandra/Spark để “vừa RAM” |

## 4. Kiểm tra sau phục hồi

1. `GET /api/v1/runs/{run_id}`: pointer từng feed trỏ generation `READY`, `expires_at` còn hạn.
2. `GET /api/v1/quality?run_id=...`: counts/lý do hợp lý; không có duplicate history cho cùng `observation_id`.
3. `demo.py status`: `last_batch_id` tăng, backlog Kafka giảm, clock và nguồn đúng run.
4. Ghi thời gian từ restart tới generation đủ điều kiện đầu tiên (fault_recovery_report). Không tuyên bố phục hồi
   thành công chỉ vì process đã lên.

## 5. Sau khi Colab VM bị reset

Dữ liệu trong workdir mất. Clone đúng commit → bootstrap → `infra.py start/init` (Kafka/Cassandra mới) → nhập
input bundle (static ZIP + recording) → **run mới, checkpoint mới** → so sánh logical outputs bằng oracle. Không
khôi phục checkpoint Spark cũ vào Kafka/Cassandra rỗng.
