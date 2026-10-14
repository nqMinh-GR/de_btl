"""Page 2 — Bản đồ: route shapes, stops and vehicles from the API, without any basemap.

Renderer ``pydeck`` (default): ``pdk.Deck(map_provider=None, map_style=None)`` with PathLayer and
ScatterplotLayers. Renderer ``svg``: a self-contained SVG schematic with no JavaScript or network assets
(choose it in the sidebar when the browser has no Internet access). Invalid vehicles are listed, not drawn.
"""

from __future__ import annotations

import streamlit as st

import state
from components import map_layers, states
from components.badges import esc, freshness_badge, html_block, legend_freshness


def _vehicle_rows(vehicles: list[dict]) -> list[dict]:
    rows = []
    for v in vehicles:
        cls = map_layers.vehicle_class(v)
        lat, lon = v.get("latitude"), v.get("longitude")
        rows.append({
            "Xe": states.fmt(v.get("vehicle_id")),
            "Nhãn": states.fmt(v.get("label")),
            "Chuyến": states.fmt(v.get("trip_id")),
            "Độ mới": map_layers.FRESHNESS_LABEL.get(cls, cls) + (" (không vẽ)" if cls == "invalid" else ""),
            "Lý do invalid": states.fmt(v.get("invalid_reason")),
            "Quan sát lúc": states.fmt_ts(v.get("observation_time")),
            "Tuổi": states.fmt_seconds(v.get("observation_age_seconds")),
            "Trạng thái dừng": states.fmt(v.get("current_status")),
            "Trạm": states.fmt(v.get("stop_id")),
            "Tọa độ": (f"{lat:.5f}, {lon:.5f}" if states.is_num(lat) and states.is_num(lon) else states.DASH),
            "Tốc độ": states.fmt(v.get("speed_mps"), "m/s"),
            "Cờ": states.fmt(v.get("quality_flags")),
            "Kế thừa": states.fmt(v.get("inherited")),
        })
    return rows


def render() -> None:
    st.title("Bản đồ")
    selection = state.require_run_and_route()
    if selection is None:
        return
    run_id, route_id = selection
    renderer = state.map_renderer()

    @st.fragment(run_every=state.refresh_interval())
    def panel() -> None:
        cl = state.client()
        geo = cl.geometry(run_id, route_id)
        stops = cl.route_stops(run_id, route_id)
        veh = cl.vehicles(run_id, route_id)
        synthetic = veh.source_type == "synthetic" or geo.source_type == "synthetic"
        if synthetic:
            st.info("Dữ liệu kiểm thử — không phải xe MBTA thật: shape, trạm và xe là fixture synthetic có nhãn.")
        shapes = geo.rows("shapes") if geo.usable else []
        stop_rows = stops.rows("stops") if stops.usable else []
        vehicles = veh.rows("vehicles") if veh.usable else []
        for res, what in ((geo, "shape tuyến"), (stops, "danh sách trạm"), (veh, "vị trí xe")):
            if not res.usable or res.kind in ("empty", "stale"):
                states.render(res, what)
        if renderer == "svg":
            svg = map_layers.render_svg(shapes, stop_rows, vehicles, synthetic=synthetic)
            if svg:
                st.html(svg)
            else:
                st.info("Không có tọa độ hợp lệ để vẽ (chưa có shape/trạm/xe).")
        else:
            st.pydeck_chart(map_layers.build_deck(shapes, stop_rows, vehicles, synthetic=synthetic))
        html_block(f"<span style='font-size:0.85rem'>{esc(map_layers.MAP_LABEL)} · {legend_freshness()} · "
                   "trạm: chấm trắng · rê chuột để xem ID, thời điểm quan sát, tuổi và chất lượng.</span>")
        if not veh.usable:
            return
        data = veh.data if isinstance(veh.data, dict) else {}
        counts = data.get("counts") if isinstance(data.get("counts"), dict) else {}
        cols = st.columns(4)
        for col, (label, key) in zip(cols, (("Fresh", "fresh_vehicle_count"), ("Stale", "stale_vehicle_count"),
                                            ("Không rõ tuổi", "unknown_age_vehicle_count"),
                                            ("Invalid (không vẽ)", "invalid_vehicle_count"))):
            col.metric(label, states.fmt(counts.get(key)))
        if data.get("truncated"):
            st.warning("Danh sách xe bị cắt bớt (truncated = true).")
        if vehicles:
            st.markdown("#### Xe trong snapshot")
            states.table(_vehicle_rows(vehicles))
            ids = [str(v.get("vehicle_id")) for v in vehicles if v.get("vehicle_id")]
            current = state.selected_vehicle()
            pick = st.selectbox("Chọn xe để xem lịch sử", ids, index=ids.index(current) if current in ids else 0,
                                key="tp_map_vehicle_pick")
            if pick:
                state.select_vehicle(pick)
                chosen = next((v for v in vehicles if str(v.get("vehicle_id")) == pick), None)
                if chosen is not None:
                    html_block(f"Xe <code>{esc(pick)}</code> {freshness_badge(map_layers.vehicle_class(chosen))}")
                history_page = state.PAGES.get("history")
                if history_page is not None:
                    st.page_link(history_page, label=f"Mở lịch sử xe {pick} (1 giờ gần nhất theo đồng hồ ứng dụng)")

    panel()
