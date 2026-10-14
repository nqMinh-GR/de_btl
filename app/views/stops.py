"""Page 3 — Trạm & dự báo: route → direction → stop, upcoming arrivals/departures with quality badges.

Predicted delay is shown (signed) only when the API provides it; schedule-only, degraded and unknown
uncertainty predictions carry their own badges; INVALID predictions are hidden behind a toggle. An empty
result shows the API ``reason`` (e.g. NO_PREDICTION_DATA) — no arrival time is ever invented.
"""

from __future__ import annotations

import streamlit as st

import state
from components import states
from components.badges import QUALITY_TEXT, html_block, legend_quality

_EVENT = {"arrival": "Đến", "departure": "Đi"}


def _prediction_rows(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        quality = str(r.get("prediction_quality") or "").lower()
        out.append({
            "Giờ dự báo": states.fmt_ts(r.get("predicted_time")),
            "Giờ theo lịch": states.fmt_ts(r.get("scheduled_time")),
            "Trễ dự kiến": states.fmt_delay(r.get("predicted_delay_seconds")),
            "Chất lượng": QUALITY_TEXT.get(quality, quality.upper() or states.DASH),
            "Độ bất định": states.fmt(r.get("uncertainty_seconds"), "s"),
            "Loại": _EVENT.get(str(r.get("event_type") or "").lower(), states.fmt(r.get("event_type"))),
            "Chuyến": states.fmt(r.get("trip_id")),
            "Tuyến": states.fmt(r.get("route_id")),
            "Hướng": states.fmt(r.get("direction_id")),
            "Thứ tự trạm": states.fmt(r.get("stop_sequence")),
            "Phân giải lịch": states.fmt(r.get("schedule_resolution")),
            "Tính KPI": states.fmt(r.get("kpi_eligible")),
        })
    return out


def render() -> None:
    st.title("Trạm & dự báo")
    selection = state.require_run_and_route()
    if selection is None:
        return
    run_id, route_id = selection
    stops_res = state.client().route_stops(run_id, route_id)
    if not stops_res.usable or stops_res.kind == "empty":
        states.render(stops_res, "danh sách trạm của tuyến")
        return
    stops = sorted(stops_res.rows("stops"), key=lambda s: (str(s.get("stop_name") or ""), str(s.get("stop_id"))))
    directions = sorted({int(d) for s in stops for d in (s.get("direction_ids") or []) if states.is_num(d)})
    c1, c2, c3 = st.columns([1, 2, 1])
    direction = c1.selectbox("Hướng", [None] + directions,
                             format_func=lambda d: "Tất cả" if d is None else f"Hướng {d}", key="tp_stop_direction")
    candidates = [s for s in stops if direction is None or direction in (s.get("direction_ids") or [])]
    if not candidates:
        st.info("Không có trạm nào của tuyến theo hướng đã chọn.")
        return
    labels = {str(s["stop_id"]): f"{s.get('stop_name') or s['stop_id']} ({s['stop_id']})" for s in candidates
              if s.get("stop_id")}
    stop_id = c2.selectbox("Trạm", list(labels), format_func=lambda sid: labels.get(sid, sid), key="tp_stop_pick")
    show_invalid = c3.toggle("Hiện cả dự báo INVALID", value=False, key="tp_stop_invalid")
    html_block("<span style='font-size:0.85rem'>Nhãn chất lượng: " + legend_quality() + "</span>")
    st.caption("Danh sách trạm không có thứ tự theo hành trình (API: ordering = none_use_trip_stops).")

    @st.fragment(run_every=state.refresh_interval())
    def panel() -> None:
        res = state.client().predictions(run_id, stop_id, route_id=route_id, direction_id=direction,
                                         include="all" if show_invalid else "displayable")
        if not states.show_or_stop(res, "dự báo tại trạm"):
            st.markdown("#### Tình trạng feed")
            states.feed_health(res.meta)
            return
        rows = res.rows("predictions")
        hidden = 0
        if not show_invalid:
            kept = [r for r in rows if str(r.get("prediction_quality") or "").lower() != "invalid"]
            hidden = len(rows) - len(kept)
            rows = kept
        if rows:
            states.table(_prediction_rows(rows))
        else:
            st.info("Tất cả dự báo trả về đều INVALID (đang ẩn).")
        if hidden:
            st.caption(f"Đã ẩn {hidden} dự báo INVALID.")
        st.caption("“Trễ dự kiến” chỉ có khi lịch được phân giải; “—” nghĩa là không tính được, không phải 0. "
                   "Giờ theo UTC.")
        data = res.data if isinstance(res.data, dict) else {}
        if data.get("truncated"):
            st.warning("Danh sách dự báo bị cắt bớt (truncated = true).")
        st.markdown("#### Tình trạng feed")
        states.feed_health(res.meta)

    panel()
