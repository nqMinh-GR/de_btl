"""Page 5 — Chất lượng & pipeline: quality reasons per feed (units, dispositions), accounting, generation
versions/expiry, component heartbeats, Kafka backlog and latency only when measured ("chưa đo" otherwise)."""

from __future__ import annotations

from typing import Any

import streamlit as st

import state
from components import states

_REASON_COLUMNS = [("reason_code", "Lý do"), ("severity", "Mức"), ("disposition", "Xử lý"), ("count", "Số lượng"),
                   ("unit", "Đơn vị"), ("category", "Nhóm")]
_COMPONENT_COLUMNS = [
    ("component", "Thành phần"), ("status", "Trạng thái"), ("last_heartbeat_at", "Heartbeat"),
    ("heartbeat_age_seconds", "Tuổi heartbeat (s, wall)"), ("last_success_at", "Thành công gần nhất"),
    ("last_batch_id", "Batch"), ("last_batch_duration_seconds", "Thời gian batch (s)"),
    ("snapshots_published_total", "Snapshot đã publish"), ("duplicate_deliveries_total", "Duplicate"),
    ("late_deliveries_total", "Late"), ("last_visibility_latency_seconds", "Latency hiển thị (s)"),
    ("last_rejected_reason", "Từ chối gần nhất"), ("last_error", "Lỗi gần nhất"),
]


def _components(data: Any) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    comps = data.get("components")
    if comps is None:
        comps = data.get("source_health")
    if isinstance(comps, dict):
        return [{"component": k, **v} for k, v in comps.items() if isinstance(v, dict)]
    return [c for c in (comps or []) if isinstance(c, dict)]


def _ts_formatters(columns: list[tuple[str, str]]) -> dict[str, Any]:
    return {key: states.fmt_ts for key, _label in columns if key.endswith("_at")}


def render() -> None:
    st.title("Chất lượng & pipeline")
    run_id = state.require_run()
    if not run_id:
        return

    @st.fragment(run_every=state.refresh_interval())
    def panel() -> None:
        cl = state.client()
        quality = cl.quality(run_id)
        pipeline = cl.pipeline_status(run_id)
        health = cl.health_ready()

        st.markdown("#### Chất lượng dữ liệu theo feed")
        if states.show_or_stop(quality, "báo cáo chất lượng (/quality)"):
            states.feed_health(quality.meta)
            data = quality.data if isinstance(quality.data, dict) else {}
            feeds = data.get("feeds") if isinstance(data.get("feeds"), dict) else {}
            for feed in states.FEEDS:
                block = feeds.get(feed)
                meta_state = quality.feed(feed).get("state")
                with st.expander(f"{states.FEED_LABELS.get(feed, feed)} — {meta_state or 'không rõ'}",
                                 expanded=isinstance(block, dict)):
                    if not isinstance(block, dict):
                        st.caption(f"Không có generation cho feed này "
                                   f"({states.FEED_STATE_TEXT.get(str(meta_state), 'không rõ')}).")
                        continue
                    states.kv({k: block.get(k) for k in (
                        "snapshot_id", "is_partial", "is_empty", "source_entity_count", "selected_entity_count",
                        "fully_valid_entity_count", "partially_valid_entity_count", "rejected_entity_count",
                    ) if k in block})
                    st.markdown("**Lý do chất lượng**")
                    states.table(block.get("reasons") or [], _REASON_COLUMNS)
                    if isinstance(block.get("accounting"), dict) and block["accounting"]:
                        st.markdown("**Accounting (đối soát số lượng)**")
                        states.kv(block["accounting"])
            source_health = _components(data)
            if source_health:
                st.markdown("**Sức khỏe nguồn (collector/publisher)**")
                states.table(source_health, _COMPONENT_COLUMNS, formatters=_ts_formatters(_COMPONENT_COLUMNS))

        st.markdown("#### Pipeline (Spark / Kafka / Cassandra)")
        if health.usable and isinstance(health.data, dict):
            checks = health.data.get("checks") if isinstance(health.data.get("checks"), dict) else {}
            states.kv({"API /health/ready": health.data.get("status"), **checks})
        else:
            states.render(health, "kiểm tra /health/ready")
        if not states.show_or_stop(pipeline, "trạng thái pipeline (/pipeline/status)"):
            return
        pdata = pipeline.data if isinstance(pipeline.data, dict) else {}
        states.kv({k: pdata.get(k) for k in ("run_state", "state_reason", "clock_mode", "application_clock",
                                             "clock_sequence", "clock_updated_at") if k in pdata},
                  formatters={"application_clock": states.fmt_ts, "clock_updated_at": states.fmt_ts})
        states.table(_components(pdata), _COMPONENT_COLUMNS, formatters=_ts_formatters(_COMPONENT_COLUMNS))
        generations = pdata.get("generations")
        if isinstance(generations, dict) and generations:
            st.markdown("**Generation hiện tại (phiên bản, as-of, hạn dùng)**")
            states.feed_health({"feeds": generations})
        backlog = pdata.get("kafka_backlog")
        if isinstance(backlog, list) and backlog:
            st.markdown("**Kafka backlog (đã đo)**")
            states.table(backlog)
        else:
            st.markdown("**Kafka backlog:** chưa đo")
        latency = pdata.get("latency")
        if isinstance(latency, dict) and latency:
            st.markdown("**Độ trễ hiển thị (đã đo)**")
            states.kv(latency)
        else:
            st.markdown("**Latency:** chưa đo")
        states.pipeline_warnings(list(quality.warnings) + list(pipeline.warnings))

    panel()
