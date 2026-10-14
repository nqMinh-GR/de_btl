"""Dashboard settings, cached API client and session/query-param state.

Settings come from environment variables first (``TP_API_URL``, ``TP_UI_REFRESH_SECONDS``,
``TP_UI_HTTP_TIMEOUT_SECONDS``, ``TP_MAP_RENDERER``), then from the project configuration when the core
package is importable (``ui.api_base_url`` / ``ui.refresh_seconds`` / ``ui.http_timeout_seconds``), then
defaults. Reading configuration is the only use of the core package; data always comes from the API.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import streamlit as st

from api_client import DEFAULT_BASE_URL, ApiClient

MAP_RENDERERS = ("pydeck", "svg")

# st.Page objects registered by streamlit_app.py (used for st.page_link to the vehicle history view).
PAGES: dict[str, Any] = {}

KEY_RUN = "tp_run_id"
KEY_ROUTE = "tp_route_id"
KEY_VEHICLE = "tp_vehicle_id"
KEY_AUTO = "tp_auto_refresh"
KEY_MAP_RENDERER = "tp_map_renderer"


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    refresh_seconds: float
    http_timeout_seconds: float
    map_renderer: str


def _float_env(name: str) -> float | None:
    raw = os.environ.get(name)
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value > 0 else None


@st.cache_resource
def get_settings() -> Settings:
    api_base = os.environ.get("TP_API_URL")
    refresh = _float_env("TP_UI_REFRESH_SECONDS")
    timeout = _float_env("TP_UI_HTTP_TIMEOUT_SECONDS")
    if api_base is None or refresh is None or timeout is None:
        try:  # optional: the dashboard also works without the core package installed
            from transitpulse.config import load_config

            cfg = load_config()
            api_base = api_base or cfg.ui.api_base_url
            refresh = refresh or float(cfg.ui.refresh_seconds)
            timeout = timeout or float(getattr(cfg.ui, "http_timeout_seconds", 5) or 5)
        except Exception:  # noqa: BLE001 - fall back to documented defaults
            pass
    renderer = os.environ.get("TP_MAP_RENDERER", "pydeck").strip().lower()
    return Settings(
        api_base_url=(api_base or DEFAULT_BASE_URL).rstrip("/"),
        refresh_seconds=max(1.0, refresh or 5.0),
        http_timeout_seconds=max(1.0, timeout or 5.0),
        map_renderer=renderer if renderer in MAP_RENDERERS else "pydeck",
    )


@st.cache_resource
def get_client(base_url: str, timeout: float) -> ApiClient:
    """One httpx client per (base URL, timeout) for the whole Streamlit server process."""
    return ApiClient(base_url, timeout=timeout)


def client() -> ApiClient:
    settings = get_settings()
    return get_client(settings.api_base_url, settings.http_timeout_seconds)


def refresh_interval() -> float | None:
    """``run_every`` for st.fragment: the configured period, or None when auto-refresh is switched off."""
    if not st.session_state.get(KEY_AUTO, True):
        return None
    return get_settings().refresh_seconds


def map_renderer() -> str:
    value = st.session_state.get(KEY_MAP_RENDERER) or get_settings().map_renderer
    return value if value in MAP_RENDERERS else "pydeck"


def current_run_id() -> str | None:
    value = st.session_state.get(KEY_RUN)
    return str(value) if value else None


def current_route_id() -> str | None:
    value = st.session_state.get(KEY_ROUTE)
    return str(value) if value else None


def set_run(run_id: str | None) -> None:
    previous = st.session_state.get(KEY_RUN)
    st.session_state[KEY_RUN] = run_id
    if run_id:
        if st.query_params.get("run_id") != run_id:
            st.query_params["run_id"] = run_id
    elif "run_id" in st.query_params:
        del st.query_params["run_id"]
    if previous != run_id:
        st.session_state.pop(KEY_VEHICLE, None)


def set_route(route_id: str | None) -> None:
    st.session_state[KEY_ROUTE] = route_id
    if route_id:
        if st.query_params.get("route_id") != route_id:
            st.query_params["route_id"] = route_id
    elif "route_id" in st.query_params:
        del st.query_params["route_id"]


def selected_vehicle() -> str | None:
    value = st.session_state.get(KEY_VEHICLE)
    return str(value) if value else None


def select_vehicle(vehicle_id: str | None) -> None:
    st.session_state[KEY_VEHICLE] = vehicle_id


def require_run() -> str | None:
    """Return the selected run id, or render an info box and return None."""
    run_id = current_run_id()
    if not run_id:
        st.info("Chưa chọn run. Khởi động một session (scripts/demo.py start) rồi chọn run ở thanh bên.")
    return run_id


def require_run_and_route() -> tuple[str, str] | None:
    run_id = require_run()
    if not run_id:
        return None
    route_id = current_route_id()
    if not route_id:
        st.info("Chưa chọn tuyến. Chọn một tuyến trong phạm vi run ở thanh bên.")
        return None
    return run_id, route_id
