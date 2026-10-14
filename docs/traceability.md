# Truy vết yêu cầu → hiện thực → kiểm thử

Trạng thái kiểm thử: **mọi test đều NOT_RUN** khi bàn giao (không được chạy trên máy build). Bảng này cho biết
yêu cầu được hiện thực ở đâu và test nào sẽ chứng minh nó khi chạy (Colab/Linux). `s/` = `src/transitpulse/`,
`t/` = `tests/`. Kết quả chạy thật được ghi tự động vào `<TP_WORKDIR>/reports/test_report.json` (IT/CT) và
`reports/environment_verification.json`.

## Yêu cầu chức năng (PDF tr.4)

| ID | Hiện thực | Kiểm thử |
|---|---|---|
| FR01 Static GTFS, version SHA-256, dims bất biến | `s/static/{ingest,loader,build,sparkio,validation,index,calendar,gtfs_time,schedule}.py`, `s/storage/static_writer.py` | `t/spark/test_static_loader.py`, `t/unit/test_{static_validation,calendar,gtfs_time,schedule_resolution}.py`, IT07 |
| FR02 Thu thập 3 feed, raw trước publish, lỗi không thành empty snapshot | `s/ingestion/{collector,http_fetch,scheduler,archive,feed_parse}.py` | `t/unit/test_ingestion_{collector,http,scheduler,archive,parse}.py`, IT05 |
| FR03 Kafka envelope có schema/checksum/ID ổn định, outbox bền vững | `s/ingestion/{kafka_producer,outbox,envelope_builder}.py`, `s/contracts/{envelope,ids,topics}.py` | `t/unit/test_{ingestion_producer,ingestion_outbox,envelope,ids}.py`, `t/integration/test_outbox_resume.py`, IT13 |
| FR04 Spark parse/validate/explode/join/dedup + quality + raw ref | `s/processing/{parse,vehicles,predictions,alerts,schedule,quality,dlq}.py` | `t/spark/test_{parse,vehicles,predictions,alerts}.py` |
| FR05 Vị trí theo current generation, fresh/stale/unknown/invalid | `s/processing/vehicles.py`, `s/api/freshness.py`, `s/contracts/policy.py` | `t/spark/test_vehicles.py`, `t/unit/test_policy.py`, `t/unit/test_api_endpoints.py`, IT01/02/04/06 |
| FR06 Dự báo, uncertainty MBTA, predicted delay | `s/processing/{predictions,schedule}.py` | `t/spark/test_predictions.py`, IT07, IT08 |
| FR07 KPI tuyến 1 target/trip, numerator/denominator/as-of | `s/processing/kpi.py` | `t/spark/test_kpi.py`, `t/unit/test_api_endpoints.py`, IT01 |
| FR08 Alert selectors/thời hạn, tách MBTA alert với pipeline warning | `s/processing/alerts.py`, `s/ingestion/scope_filter.py`, `s/api/routers/alerts.py` | `t/spark/test_alerts.py`, `t/unit/test_ingestion_scope.py`, IT09 |
| FR09 Lịch sử xe có paging, curated Parquet, Spark batch report | `s/api/{paging.py,routers/vehicles.py}`, `s/processing/{curated,report_job}.py` | `t/unit/test_api_history_paging.py`, `t/spark/test_batch_curated.py`; `report_job.py` chưa có test riêng (chạy bằng `make report RUN_ID=…`) |
| FR10 Generation bất biến, READY + pointer CAS, duplicate/late/retry | `s/processing/batch.py`, `s/storage/{pointer,manifest}.py` | `t/unit/test_storage_pointer_manifest.py`, `t/spark/test_batch_{lineage,helpers}.py`, IT02/03/10/11/15 |
| FR11 Replay đúng static, 1x/10x/50x, pause/resume, source clock | `s/replay/*`, `s/processing/control_clock.py` | `t/unit/test_replay_{publisher,control,timeline,verify}.py`, `t/spark/test_batch_control_fold.py`, IT04, IT14 |
| FR12 API + dashboard + evidence | `s/api/**`, `app/**`, `notebooks/*`, `benchmarks/**`, `s/runtime/{export,golden_smoke}.py` | `t/unit/test_api_*.py`, golden smoke (`demo.py golden-smoke`), IT01–IT16; UI và benchmark chưa có test tự động |

## Yêu cầu phi chức năng (PDF tr.5)

| ID | Hiện thực | Kiểm thử / bằng chứng |
|---|---|---|
| NFR01 Tính đúng | Golden fixture + oracle độc lập `t/oracle/`, `s/verify/golden.py` | `t/unit/test_oracle_golden.py`, `test_oracle_isolation.py`, IT01–IT09 |
| NFR02 Phục hồi | checkpoint, sink idempotent, manifest đóng băng, outbox | IT10, IT11, IT12, `test_outbox_resume.py` |
| NFR03 Độ trễ p95 < 10 s (mục tiêu) | `benchmarks/*` | Chưa đo — chạy `make benchmark SCENARIO=…` |
| NFR04 Dung lượng giới hạn | envelope ≤ 4 MiB, retention topic, raw budget 2 GiB | IT13, `t/unit/test_ingestion_disk_budget.py` |
| NFR05 Truy vết | `s/obs/lineage.py`, manifest lineage | `demo.py lineage`; `t/spark/test_batch_lineage.py` |
| NFR06 Tái lập | pin `==`, `.python-version`, seeds, runtime manifest | `t/unit/test_synthetic_determinism.py`; `uv.lock` sinh ở lần bootstrap đầu |
| NFR07 Quan sát được | `s/obs/{logging,metrics}.py`, `/api/v1/metrics` | `t/unit/test_api_endpoints.py` (metrics) |
| NFR08 Giới hạn truy vấn | `s/api/{repository,paging,params}.py` | `t/unit/test_api_history_paging.py`, `t/unit/test_api_components.py` (CQL bounded, không ALLOW FILTERING) |
| NFR09 An toàn demo | bind 127.0.0.1, CORS allowlist; tunnel Cloudflare **chỉ** dashboard chỉ đọc, bật theo yêu cầu người dùng (D-B-12) | `t/unit/test_api_endpoints.py` (CORS), `t/runtime/test_kafka_format.py`, `t/runtime/test_tunnel.py` |
| NFR10 Demo offline | `s/verify/offline_probe.py` | IT16 |

## Integration scenarios (PDF tr.26)

| ID | Test |
|---|---|
| IT01–IT04 | `t/integration/test_golden_flow.py` |
| IT05 | `t/integration/test_source_faults.py` |
| IT06–IT09 | `t/integration/test_scenarios.py` (vector Spark tương ứng trong `t/spark/` chỉ là unit, không tính là bằng chứng IT) |
| IT10–IT11 | `t/integration/test_recovery.py` |
| IT12 | `t/integration/test_zz_cassandra_down.py` |
| IT13 | `t/integration/test_payload_boundary.py` |
| IT14 | `t/integration/test_run_isolation.py` |
| IT15 | `t/integration/test_generation_ttl.py` |
| IT16 | `t/integration/test_zz_offline.py` |

## Colab / GitHub scenarios (md §27.2)

| ID | Kiểm thử tự động | Ghi chú |
|---|---|---|
| CT01 Clean clone | — | Thủ công: clone mới trên Colab (NOT_RUN) |
| CT02 Bootstrap hai lần | `t/runtime/test_artifact_install.py` (rerun → REUSED_VERIFIED) | Thủ công trên Colab |
| CT03 Start infra hai lần | `t/runtime/test_registry_decisions.py` | |
| CT04 Start demo hai lần | `t/runtime/test_session_logic.py` | |
| CT05 Kernel ≠ core | `t/runtime/test_runtime_env.py` | `colab_ui.py` không import transitpulse |
| CT06 Ports/resource không đạt | `t/runtime/test_doctor_thresholds.py`, `test_registry_decisions.py` | |
| CT07 Checksum sai | `t/runtime/test_artifact_install.py` | |
| CT08 Process exit trước readiness | `t/runtime/test_runner.py` | |
| CT09 Restart processor | IT10, IT11 | |
| CT10 Reset VM + import bundle | — | Thủ công (NOT_RUN) |
| CT11 Replay thiếu static/checksum sai | `t/unit/test_replay_verify.py` | |
| CT12 Live mất mạng | `t/unit/test_ingestion_collector.py`, `test_ingestion_http.py` | |
| CT13 UI gặp 503 | — | Thủ công (notebook UI hiển thị unavailable) |
| CT14 Export khi đang chạy/cuối | `t/runtime/test_export_and_verification.py` (một phần) | |
| CT15 Stop all | `t/runtime/test_registry_decisions.py` | |

CT không có test tự động được ghi `NOT_RUN` (lý do `manual/Colab`) trong `test_report.json`.

## Luật MUST-NOT (đã rà tĩnh)

Không có `ALLOW FILTERING` (chỉ xuất hiện trong comment/assert phủ định), không counter Cassandra,
`failOnDataLoss=true`, không `maxOffsetsPerTrigger`, không `current_timestamp()` trong processing, không
`pkill/killall`, không `reset --hard`, không tag `latest`, không secret trong repo, không empty snapshot khi
fetch lỗi, không thay live bằng synthetic, UI chỉ gọi API. Spark tests mang `it_case` không ghi PASS vào
báo cáo tích hợp (D-B-09).
