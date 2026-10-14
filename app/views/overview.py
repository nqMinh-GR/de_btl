"""Page 1 — Tổng quan tuyến: vehicle freshness counts, predicted-late KPI with its denominator, feed health.

All numbers come from ``GET /routes/{route_id}/metrics`` (KPI computed by Spark). The page only formats
them: a 0 / null denominator shows "Chưa có mẫu đủ điều kiện", never 0 %.
"""

from __future__ import annotations

from typing import Any

import streamlit as st

import state
from components import states
from components.badges import esc, html_block, status_badge

NO_ELIGIBLE_TEXT = "Chưa có mẫu đủ điều kiện"
_COUNTS = (
    ("Xe có vị trí mới", "fresh_vehicle_count"),
    ("Vị trí cũ (stale)", "stale_vehicle_count"),
    ("Không rõ tuổi", "unknown_age_vehicle_count"),
    ("Không hợp lệ", "invalid_vehicle_count"),
    ("Có mặt trong snapshot", "snapshot_membership_count"),
)


def _count(block: Any, key: str) -> Any:
    if not isinstance(block, dict):
        return None
    for source in (block.get("counts"), block):
        if isinstance(source, dict) and states.is_num(source.get(key)):
            return source.get(key)
    return None


def kpi_late_text(pm: dict[str, Any]) -> tuple[str, bool]:
    eligible = pm.get("eligible_trip_count")
    if not states.is_num(eligible) or int(eligible) == 0:
        return NO_ELIGIBLE_TEXT, False
    late = pm.get("predicted_late_trip_count")
    threshold = pm.get("late_threshold_seconds")
    threshold_text = (f"{states.num_text(float(threshold) / 60.0, 1)} phút"
                      if states.is_num(threshold) and threshold > 0 else "ngưỡng trễ của policy")
    rate = pm.get("predicted_late_rate")
    rate_text = f" ({states.num_text(float(rate) * 100.0, 1)}%)" if states.is_num(rate) else ""
    late_text = str(int(late)) if states.is_num(late) else states.DASH
    return f"Độ trễ dự kiến ≥ {threshold_text}: {late_text} trên {int(eligible)} chuyến đủ điều kiện{rate_text}", True


def vehicle_metrics(vm: dict[str, Any]) -> None:
    cols = st.columns(len(_COUNTS))
    for col, (label, key) in zip(cols, _COUNTS):
        value = _count(vm, key)
        col.metric(label, states.fmt(value))
    stale = " " + status_badge("STALE") if vm.get("stale") is True else ""
    incomplete = " · số đếm CHƯA ĐẦY ĐỦ (bị giới hạn số dòng)" if vm.get("counts_complete") is False else ""
    html_block(
        "<span style='font-size:0.85rem'>“Xe có vị trí mới” là số xe có quan sát fresh trong snapshot hiện tại, "
        "không phải toàn bộ đội xe MBTA đang hoạt động. Đánh giá lúc "
        f"{esc(states.fmt_ts(vm.get('evaluated_at')))} (đồng hồ ứng dụng); metrics_as_of "
        f"{esc(states.fmt_ts(vm.get('metrics_as_of')))}{esc(incomplete)}{stale}</span>"
    )


def prediction_metrics(pm: dict[str, Any]) -> None:
    text, has_sample = kpi_late_text(pm)
    with st.container(border=True):
        flag = " " + status_badge("STALE") if pm.get("stale") is True else ""
        html_block(f"<span style='font-size:1.15rem;font-weight:600'>{esc(text)}</span>{flag}")
        st.caption("Mẫu số là số chuyến (trip instance) đủ điều kiện tại stop mục tiêu; tỷ lệ do Spark tính và "
                   "API trả về." if has_sample else "Mẫu số bằng 0 hoặc chưa biết nên không hiển thị phần trăm.")
        states.kv({
            "Độ trễ dự kiến trung bình": states.fmt_delay(pm.get("mean_predicted_delay_seconds")),
            "Số chuyến bị loại khỏi KPI": states.fmt(pm.get("excluded_trip_count")),
            "Không rõ độ bất định (đủ điều kiện, đếm riêng)": states.fmt(pm.get("unknown_uncertainty_count")),
            "Số trip instance": states.fmt(pm.get("trip_instance_count")),
            "Chính sách stop mục tiêu": states.fmt(pm.get("target_policy")),
            "policy_version": states.fmt(pm.get("policy_version")),
            "metrics_as_of": states.fmt_ts(pm.get("metrics_as_of")),
            "Tuổi feed TU": states.fmt_seconds(pm.get("feed_age_seconds")),
        })
        excluded = pm.get("excluded_by_reason")
        if isinstance(excluded, dict) and excluded:
            st.markdown("**Lý do loại khỏi KPI**")
            states.table([{"reason": k, "count": v} for k, v in sorted(excluded.items())],
                         [("reason", "Lý do"), ("count", "Số chuyến")])


def _missing_block(res: Any, feed: str, what: str) -> None:
    block = res.feed(feed)
    state_name = block.get("state")
    detail = states.FEED_STATE_TEXT.get(str(state_name), "không có dữ liệu")
    message = f"{what} không khả dụng — feed {states.FEED_LABELS.get(feed, feed)}: {state_name or 'không rõ'} ({detail})."
    if state_name in ("expired", "inconsistent"):
        st.error(message)
    else:
        st.info(message)


def render() -> None:
    st.title("Tổng quan tuyến")
    selection = state.require_run_and_route()
    if selection is None:
        return
    run_id, route_id = selection

    @st.fragment(run_every=state.refresh_interval())
    def panel() -> None:
        res = state.client().route_metrics(run_id, route_id)
        st.caption(f"Tuyến `{route_id}` · run `{run_id[:8]}`")
        if res.source_type == "synthetic":
            st.caption("Tuyến synthetic (fixture có nhãn) — không phải tuyến MBTA đã xác minh.")
        if not res.usable:
            states.render(res, "chỉ số tuyến")
            st.markdown("#### Tình trạng feed")
            states.feed_health(res.meta)
            return
        data = res.data if isinstance(res.data, dict) else {}
        st.markdown("#### Xe trên tuyến")
        vm = data.get("vehicle_metrics")
        if isinstance(vm, dict):
            vehicle_metrics(vm)
        else:
            _missing_block(res, "vehicle_positions", "Chỉ số xe")
        st.markdown("#### Dự báo trễ (KPI do Spark tính)")
        pm = data.get("prediction_metrics")
        if isinstance(pm, dict):
            prediction_metrics(pm)
        else:
            _missing_block(res, "trip_updates", "KPI dự báo trễ")
        st.markdown("#### Tình trạng feed")
        states.feed_health(res.meta)
        coverage = res.meta.get("coverage")
        if isinstance(coverage, dict) and coverage.get("truncated"):
            st.warning("Kết quả bị cắt bớt (coverage.truncated = true).")
        states.pipeline_warnings(res.warnings)

    panel()
