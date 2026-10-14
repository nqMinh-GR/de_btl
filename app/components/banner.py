"""Always-visible banner: LIVE / REPLAY / SYNTHETIC, short run id, run state, clocks and generations.

Replay and synthetic runs show the historical application clock ("Đồng hồ lịch sử") next to the wall-clock
``served_at``; a paused or finished replay keeps its historical clock and its REPLAY label (never "live").
The banner refreshes in its own fragment, so a failing panel never removes the source label.
"""

from __future__ import annotations

from typing import Any

import streamlit as st

import state
from api_client import ApiResult
from components import states
from components.badges import SYNTHETIC_NOTICE, esc, html_block, source_badge, status_badge

_BOX = ("border:1px solid #d0d7de;border-radius:8px;padding:8px 12px;margin-bottom:8px;display:flex;"
        "flex-wrap:wrap;gap:4px 18px;align-items:center")


def banner_html(res: ApiResult) -> str:
    meta = res.meta if isinstance(res.meta, dict) else {}
    data = res.data if isinstance(res.data, dict) else {}
    source = res.source_type
    run_id = meta.get("run_id") or data.get("run_id")
    app_clock = meta.get("application_clock") or data.get("application_clock")
    scenario = meta.get("scenario_id") or data.get("scenario_id")
    items = [source_badge(source)]
    if source == "synthetic":
        text = f"<b>{esc(SYNTHETIC_NOTICE)}</b>"
        if scenario:
            text += f" · kịch bản <code>{esc(scenario)}</code>"
        items.append(text)
    items.append(f'Run <code title="{esc(run_id)}">{esc(states.short(run_id, 8))}</code>')
    items.append(f"Trạng thái {status_badge(meta.get('run_state') or data.get('run_state'))}")
    clock_label = "Đồng hồ lịch sử" if source in ("replay", "synthetic") else "Đồng hồ ứng dụng (wall)"
    items.append(f"{clock_label}: <b>{esc(states.fmt_ts(app_clock))}</b>")
    items.append(f"Phục vụ lúc (wall): {esc(states.fmt_ts(meta.get('served_at')))}")
    items.append(f"GTFS <code>{esc(states.short(meta.get('gtfs_version') or data.get('gtfs_version'), 12))}</code>")
    return f'<div style="{_BOX}">' + "".join(f"<span>{i}</span>" for i in items) + "</div>"


def generations_html(res: ApiResult) -> str:
    data = res.data if isinstance(res.data, dict) else {}
    gens = data.get("generations") if isinstance(data.get("generations"), dict) else res.meta.get("feeds")
    if not isinstance(gens, dict) or not gens:
        return ""
    parts = []
    for feed in states.FEEDS:
        block = gens.get(feed)
        if not isinstance(block, dict):
            continue
        flag = " " + status_badge("STALE") if block.get("stale") is True else ""
        parts.append(
            f"{esc(states.FEED_LABELS.get(feed, feed))}: {status_badge(block.get('state'))}{flag} "
            f"<code>{esc(states.short(block.get('snapshot_id'), 14))}</code> as-of "
            f"{esc(states.fmt_ts(block.get('metrics_as_of') or block.get('feed_timestamp')))}"
        )
    return '<div style="font-size:0.85rem;margin:-4px 0 8px 0">' + " &nbsp;|&nbsp; ".join(parts) + "</div>"


def render_banner(res: ApiResult) -> None:
    if res.usable:
        html_block(banner_html(res))
        gens = generations_html(res)
        if gens:
            html_block(gens)
    else:
        states.render(res, "trạng thái run (banner)")


def render_banner_fragment() -> None:
    """Banner wrapped in its own auto-refreshing fragment (``run_every`` = ui.refresh_seconds)."""

    @st.fragment(run_every=state.refresh_interval())
    def _banner() -> None:
        run_id = state.current_run_id()
        if not run_id:
            st.info("Chưa có run nào được chọn — banner nguồn dữ liệu sẽ hiện khi có run.")
            return
        render_banner(state.client().run(run_id))

    _banner()


def is_synthetic(*results: Any) -> bool:
    return any(isinstance(r, ApiResult) and r.source_type == "synthetic" for r in results)
