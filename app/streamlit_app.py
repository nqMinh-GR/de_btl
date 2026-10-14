"""TransitPulse MBTA — local Streamlit + PyDeck dashboard (frontend of the FastAPI service only).

Run from the ``app/`` directory so ``app/.streamlit/config.toml`` (127.0.0.1:8501, headless, no usage
stats) is picked up, with the core virtualenv that has the ``dashboard`` extra installed::

    cd app && TP_API_URL=http://127.0.0.1:8000/api/v1 ../.venv/bin/streamlit run streamlit_app.py

The dashboard never reads Cassandra, Kafka, raw files or MBTA, never computes KPIs and never zero-fills.
Panels refresh with ``st.fragment(run_every=ui.refresh_seconds)`` (default 5 s; switchable off in the
sidebar) — there is no ``while True`` loop and no full-page rerun loop.
"""

from __future__ import annotations

import streamlit as st

import state
from api_client import ApiResult
from components import banner, states
from views import alerts, map as map_view, overview, quality, stops, vehicle_history

st.set_page_config(page_title="TransitPulse MBTA", layout="wide", initial_sidebar_state="expanded")

PAGES = {
    "overview": st.Page(overview.render, title="Tổng quan tuyến", icon=":material/dashboard:", url_path="tong-quan",
                        default=True),
    "map": st.Page(map_view.render, title="Bản đồ", icon=":material/map:", url_path="ban-do"),
    "stops": st.Page(stops.render, title="Trạm & dự báo", icon=":material/schedule:", url_path="tram-du-bao"),
    "alerts": st.Page(alerts.render, title="Cảnh báo", icon=":material/warning:", url_path="canh-bao"),
    "quality": st.Page(quality.render, title="Chất lượng & pipeline", icon=":material/monitoring:",
                       url_path="chat-luong-pipeline"),
    "history": st.Page(vehicle_history.render, title="Lịch sử xe", icon=":material/timeline:", url_path="lich-su-xe"),
}
state.PAGES.update(PAGES)


def _run_label(run: dict) -> str:
    source = str(run.get("source_type") or "?").upper()
    created = str(run.get("created_at") or "")[:19].replace("T", " ")
    return f"{source} · {str(run.get('run_id'))[:8]} · {created or '?'} · {run.get('run_state') or '?'}"


def _route_label(route: dict) -> str:
    name = " — ".join(str(x) for x in (route.get("short_name"), route.get("long_name")) if x)
    return f"{name} ({route.get('route_id')})" if name else str(route.get("route_id"))


def _sidebar() -> None:
    settings = state.get_settings()
    client = state.client()
    with st.sidebar:
        st.markdown("### Run và tuyến")
        runs_res: ApiResult = client.runs(limit=50)
        runs = runs_res.rows("runs") if runs_res.usable else []
        if not runs_res.usable or runs_res.kind == "empty":
            states.render(runs_res, "danh sách run")
        labels = {str(r["run_id"]): _run_label(r) for r in runs if r.get("run_id")}
        ids = list(labels)
        requested = st.query_params.get("run_id") or state.current_run_id()
        if requested and requested not in ids:
            ids.insert(0, requested)
            labels[requested] = f"{requested[:8]} (từ URL/phiên trước)"
        if ids:
            index = ids.index(requested) if requested in ids else 0
            run_id = st.selectbox("Run", ids, index=index, format_func=lambda rid: labels.get(rid, rid),
                                  key="tp_sb_run")
            state.set_run(run_id)
        else:
            state.set_run(None)
            st.caption("Chưa có run nào trong API.")
        run_id = state.current_run_id()
        if run_id:
            routes_res = client.routes(run_id)
            routes = routes_res.rows("routes") if routes_res.usable else []
            if not routes_res.usable or routes_res.kind == "empty":
                states.render(routes_res, "danh sách tuyến")
            route_labels = {str(r["route_id"]): _route_label(r) for r in routes if r.get("route_id")}
            route_ids = list(route_labels)
            wanted = st.query_params.get("route_id") or state.current_route_id()
            if route_ids:
                route_id = st.selectbox("Tuyến", route_ids, index=route_ids.index(wanted) if wanted in route_ids else 0,
                                        format_func=lambda rid: route_labels.get(rid, rid), key="tp_sb_route")
                state.set_route(route_id)
            else:
                state.set_route(None)
            missing = (routes_res.data or {}).get("missing_route_ids") if isinstance(routes_res.data, dict) else None
            if missing:
                st.caption(f"Tuyến trong scope nhưng thiếu static: {', '.join(map(str, missing))}")
        st.divider()
        st.toggle(f"Tự làm mới mỗi {states.num_text(settings.refresh_seconds, 1)} giây", value=True,
                  key=state.KEY_AUTO)
        st.radio("Trình vẽ bản đồ", list(state.MAP_RENDERERS),
                 index=list(state.MAP_RENDERERS).index(settings.map_renderer), key=state.KEY_MAP_RENDERER,
                 format_func=lambda r: "PyDeck (không basemap)" if r == "pydeck" else "SVG (offline hoàn toàn)",
                 horizontal=True)
        st.caption(f"API: `{settings.api_base_url}` · không dùng Mapbox token, không tải tile bản đồ.")


navigation = st.navigation({
    "TransitPulse MBTA": [PAGES["overview"], PAGES["map"], PAGES["stops"], PAGES["alerts"], PAGES["quality"]],
    "Chi tiết": [PAGES["history"]],
})
_sidebar()
banner.render_banner_fragment()
navigation.run()
