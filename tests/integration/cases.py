"""Registry of the 16 integration scenarios (PDF p26, design E §7.4) used by the test report plugin.

Every case is reported in ``<W>/reports/test_report.json`` with PASS / FAIL / NOT_RUN, including cases that were
not collected or not selected in a session. ``module`` names the test file that implements the case so that a
collection error of that file is reported as FAIL for its cases instead of silently disappearing.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ItCase:
    id: str
    title: str
    proves: str
    module: str


IT_CASES: dict[str, ItCase] = {c.id: c for c in (
    ItCase("IT01", "Golden fixture ban đầu", "Counts, rate 0,5 và mean 210 giây đúng ở API và sink",
           "test_golden_flow.py"),
    ItCase("IT02", "Snapshot mới phát hai lần", "Membership không tăng; history cùng observation không trùng",
           "test_golden_flow.py"),
    ItCase("IT03", "Snapshot cũ tới sau snapshot mới", "Pointer và vị trí không rollback; late có audit",
           "test_golden_flow.py"),
    ItCase("IT04", "Clock tiến, không có positions mới", "V1 tuổi 135 giây, fresh 0; predictions cũ báo stale",
           "test_golden_flow.py"),
    ItCase("IT05", "Empty hợp lệ so với malformed hoặc timeout",
           "Empty thay view; malformed giữ view cũ có source warning", "test_source_faults.py"),
    ItCase("IT06", "Missing timestamp và GPS invalid", "Unknown không fresh; invalid disposition và counts đúng",
           "test_scenarios.py"),
    ItCase("IT07", "Ambiguous date và repeated stop", "Không đoán lịch hoặc stop; loại KPI có reason",
           "test_scenarios.py"),
    ItCase("IT08", "Bus uncertainty và cancelled trip", "300, 301, lớn hơn 301, absent xử lý đúng policy",
           "test_scenarios.py"),
    ItCase("IT09", "Partial alerts", "Không đóng hàng loạt alert cũ; inherited lineage đúng", "test_scenarios.py"),
    ItCase("IT10", "Crash partial write trước READY hoặc CAS",
           "API vẫn thấy generation cũ; retry hoàn tất cùng IDs", "test_recovery.py"),
    ItCase("IT11", "Crash sau CAS trước checkpoint commit", "Reuse READY; first visibility và TTL không đổi",
           "test_recovery.py"),
    ItCase("IT12", "Cassandra hoặc required sink down", "Batch fail, offsets không được coi là xử lý thành công",
           "test_zz_cassandra_down.py"),
    ItCase("IT13", "Message gần 4 MiB và vượt ngưỡng",
           "Transport boundary đạt; oversize lưu raw và reject có reason", "test_payload_boundary.py"),
    ItCase("IT14", "Run isolation và replay pause", "Output tách run; clock freeze, wall heartbeat tiếp tục",
           "test_run_isolation.py"),
    ItCase("IT15", "Generation TTL expiry", "503 generation_expired; phân biệt zero rows hợp lệ",
           "test_generation_ttl.py"),
    ItCase("IT16", "Demo mạng tắt", "Pipeline, API, assets và bản đồ core dùng được", "test_zz_offline.py"),
)}

@dataclass(frozen=True)
class CtCase:
    id: str
    title: str
    expected: str


# Colab/GitHub scenarios (spec md §27.2). Automated tests declare @pytest.mark.ct_case("CTxx") (tests/runtime,
# replay verify tests, IT10/IT11 for CT09); every entry without an executed test is reported NOT_RUN 'manual/Colab'.
CT_CASES: dict[str, CtCase] = {c.id: c for c in (
    CtCase("CT01", "Clone vào directory mới", "Unit/native smoke không cần working copy cũ"),
    CtCase("CT02", "Bootstrap hai lần", "Versions/hash đúng, cache reuse, không tạo data loss"),
    CtCase("CT03", "Start infra hai lần", "Một broker/node, PID/config ownership đúng"),
    CtCase("CT04", "Start demo cell hai lần", "Một processor/publisher/API của session; run không sinh trùng"),
    CtCase("CT05", "Kernel version khác core", "Notebook UI chạy qua HTTP/subprocess, Spark workers vẫn Python 3.11"),
    CtCase("CT06", "Ports/resource budget không đạt", "Fail trước start với thông tin cần sửa; không bỏ Cassandra"),
    CtCase("CT07", "Downloads/checksum sai", "Fail artifact setup, không chạy binary chưa verify"),
    CtCase("CT08", "Process exit trước readiness", "Exit code/log rõ, cleanup đúng scope"),
    CtCase("CT09", "Restart processor trong VM", "Kết quả oracle/pointer/history vẫn đúng"),
    CtCase("CT10", "Reset VM và import bundle", "New run/checkpoint, outputs tái lập theo logical oracle"),
    CtCase("CT11", "Replay thiếu static/checksum mismatch", "Fail rõ, không lấy static live mới thay thế"),
    CtCase("CT12", "Mode live mất mạng", "Báo failure/stale, không đổi SYNTHETIC dưới nhãn LIVE"),
    CtCase("CT13", "Notebook UI gặp API 503", "Hiển thị unavailable/expired, không zero-fill"),
    CtCase("CT14", "Export khi running/final", "Manifest ghi đúng completeness, chỉ input/output đã commit"),
    CtCase("CT15", "Stop all", "Chỉ owned processes dừng; workdir data và kernel còn nguyên"),
)}

# Disruptive cases run last (pytest_collection_modifyitems): IT12 stops the owned Cassandra, IT16 may restart infra.
DISRUPTIVE_ORDER: tuple[str, ...] = ("IT12", "IT16")

ADDITIONAL_TESTS: dict[str, str] = {
    "test_outbox_resume.py": "FR03: outbox resume after SIGKILL keeps capture_id/snapshot_id (design E §7.4)",
}
