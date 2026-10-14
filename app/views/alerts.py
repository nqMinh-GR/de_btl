"""Page 4 — Cảnh báo: MBTA service alerts (scope, resolved selectors, active periods, state at the clock)
and, in a separate block, pipeline data-quality warnings taken only from the response ``warnings``."""

from __future__ import annotations

import json
from typing import Any

import streamlit as st

import state
from components import states
from components.badges import badge, esc, html_block, status_badge


def _period(period: Any) -> str:
    if not isinstance(period, dict):
        return esc(period)
    start = states.fmt_ts(period.get("start")) if period.get("start") else "không giới hạn"
    end = states.fmt_ts(period.get("end")) if period.get("end") else "không giới hạn"
    return f"{esc(start)} → {esc(end)}"


def _alert(a: dict[str, Any], synthetic: bool) -> None:
    with st.container(border=True):
        prefix = badge("DỮ LIỆU KIỂM THỬ", "#ffffff", "#6f42c1") + " " if synthetic else ""
        header = a.get("header_text") or "(không có tiêu đề)"
        html_block(f"{prefix}{status_badge(a.get('active_state_at_clock'))} <b>{esc(header)}</b>")
        html_block(
            "<span style='font-size:0.85rem'>"
            f"Hiệu ứng {esc(a.get('effect'))} · nguyên nhân {esc(a.get('cause'))} · mức độ "
            f"{esc(a.get('severity_level'))} · tuyến {esc(states.fmt(a.get('route_ids')))} · "
            f"<code>{esc(a.get('alert_id'))}</code></span>"
        )
        description = a.get("description_text")
        if description:
            html_block(esc(description).replace("\n", "<br>"))
        periods = a.get("active_periods") or []
        html_block("<b>Thời hạn:</b> " + ("<br>".join(_period(p) for p in periods) if periods
                                          else "không có active_period (luôn áp dụng)"))
        selectors = a.get("informed_entities")
        selector_text = json.dumps(selectors, ensure_ascii=False, default=str)[:800] if selectors else states.DASH
        html_block(f"<b>Selector đã resolve:</b> <code>{esc(selector_text)}</code> · match "
                   f"{esc(states.fmt(a.get('match_kinds')))} · resolution {esc(a.get('selector_resolution'))}")
        lineage = []
        if a.get("inherited"):
            lineage.append(f"kế thừa từ <code>{esc(states.short(a.get('inherited_from_snapshot_id'), 14))}</code>")
        if a.get("confirmation_status"):
            lineage.append(status_badge(a.get("confirmation_status")))
        if lineage:
            html_block("<b>Lineage:</b> " + " ".join(lineage))
        url = a.get("url")
        if isinstance(url, str) and url.startswith("https://"):
            st.markdown(f"[Chi tiết từ nguồn]({url})")


def render() -> None:
    st.title("Cảnh báo")
    run_id = state.require_run()
    if not run_id:
        return
    route_id = state.current_route_id()
    only_route = st.toggle("Chỉ cảnh báo của tuyến đang chọn", value=bool(route_id), key="tp_alerts_route_only",
                           disabled=not route_id)

    @st.fragment(run_every=state.refresh_interval())
    def panel() -> None:
        res = state.client().alerts(run_id, route_id if only_route else None)
        synthetic = res.source_type == "synthetic"
        st.markdown("#### Cảnh báo dịch vụ " + ("(dữ liệu kiểm thử)" if synthetic else "MBTA"))
        if states.show_or_stop(res, "cảnh báo dịch vụ"):
            for alert in res.rows("alerts"):
                _alert(alert, synthetic)
            if isinstance(res.data, dict) and res.data.get("truncated"):
                st.warning("Danh sách cảnh báo bị cắt bớt (truncated = true).")
        states.pipeline_warnings(res.warnings)
        st.caption("Cảnh báo dịch vụ là dữ liệu nguồn; cảnh báo chất lượng pipeline chỉ đến từ trường warnings "
                   "của API và được tách riêng.")

    panel()
