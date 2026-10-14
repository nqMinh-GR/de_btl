"""Vehicle detail — observed path and observation table from ``GET /vehicles/{id}/history``.

Default range: the last hour of the application clock (API default). Optional UTC bounds must carry an
explicit offset (``Z``); a naive value is rejected by the API (422) and shown as an invalid request.
"Tải thêm" follows ``next_page_token``. Missing speed is shown as "—" and never hides the marker.
This page does not auto-refresh (pages are accumulated); use "Tải lại" to start over.
"""

from __future__ import annotations

import streamlit as st

import state
from components import map_layers, states


def _rows(observations: list[dict]) -> list[dict]:
    out = []
    for o in observations:
        lat, lon = o.get("latitude"), o.get("longitude")
        out.append({
            "Thời điểm": states.fmt_ts(o.get("event_time")),
            "Bản ghi": states.fmt(o.get("record_kind")),
            "Nguồn thời gian": states.fmt(o.get("event_time_origin")),
            "Tọa độ": f"{lat:.5f}, {lon:.5f}" if states.is_num(lat) and states.is_num(lon) else states.DASH,
            "Tốc độ": states.fmt(o.get("speed_mps"), "m/s"),
            "Hướng (bearing)": states.fmt(o.get("bearing")),
            "Chuyến": states.fmt(o.get("trip_id")),
            "Trạm": states.fmt(o.get("stop_id")),
            "Trạng thái dừng": states.fmt(o.get("current_status")),
            "Cờ": states.fmt(o.get("quality_flags")),
        })
    return out


def render() -> None:
    st.title("Lịch sử xe")
    run_id = state.require_run()
    if not run_id:
        return
    c1, c2, c3 = st.columns([2, 2, 2])
    vehicle_id = c1.text_input("Mã xe (vehicle_id)", value=state.selected_vehicle() or "", key="tp_hist_vehicle")
    start = c2.text_input("Từ (UTC ISO, ví dụ 2026-10-05T12:00:00Z)", value="", key="tp_hist_from").strip()
    end = c3.text_input("Đến (UTC ISO, không bắt buộc)", value="", key="tp_hist_to").strip()
    vehicle_id = vehicle_id.strip()
    if not vehicle_id:
        st.info("Nhập mã xe hoặc chọn xe ở trang Bản đồ.")
        return
    state.select_vehicle(vehicle_id)
    cache_key = f"tp_hist::{run_id}::{vehicle_id}::{start}::{end}"
    b1, b2 = st.columns([1, 1])
    reload_clicked = b1.button("Tải lại", key="tp_hist_reload")
    if reload_clicked or cache_key not in st.session_state:
        res = state.client().vehicle_history(run_id, vehicle_id, start=start or None, end=end or None)
        st.session_state[cache_key] = {"first": res, "observations": res.rows("observations") if res.usable else [],
                                       "next": (res.data or {}).get("next_page_token") if res.usable and isinstance(res.data, dict) else None,
                                       "pages": 1}
    store = st.session_state[cache_key]
    first = store["first"]
    if store["next"] and b2.button("Tải thêm", key="tp_hist_more"):
        more = state.client().vehicle_history(run_id, vehicle_id, start=start or None, end=end or None,
                                              page_token=store["next"])
        if more.usable:
            store["observations"] = store["observations"] + more.rows("observations")
            store["next"] = more.data.get("next_page_token") if isinstance(more.data, dict) else None
            store["pages"] += 1
        else:
            states.render(more, "trang lịch sử tiếp theo")
    if not states.show_or_stop(first, "lịch sử xe"):
        return
    data = first.data if isinstance(first.data, dict) else {}
    rng = data.get("range") if isinstance(data.get("range"), dict) else {}
    st.caption(f"Khoảng [{states.fmt_ts(rng.get('start'))}, {states.fmt_ts(rng.get('end'))}) · "
               f"buckets {states.fmt(data.get('buckets'))} · {len(store['observations'])} quan sát · "
               f"{store['pages']} trang" + (" · còn trang tiếp theo" if store["next"] else ""))
    synthetic = first.source_type == "synthetic"
    if synthetic:
        st.info("Dữ liệu kiểm thử — không phải xe MBTA thật.")
    observations = store["observations"]
    if state.map_renderer() == "svg":
        ordered = sorted(observations, key=lambda o: str(o.get("event_time") or ""))
        path = [[o.get("longitude"), o.get("latitude")] for o in ordered]
        svg = map_layers.render_svg([{"shape_id": f"đường quan sát của xe {vehicle_id}", "path": path}], [], [],
                                    synthetic=synthetic)
        if svg:
            st.html(svg)
        else:
            st.caption("Cần ít nhất hai quan sát có tọa độ hợp lệ để vẽ đường đi.")
    else:
        st.pydeck_chart(map_layers.history_deck(observations, synthetic=synthetic))
    states.table(_rows(observations))
