"""Small HTML badges (inline styles, every text escaped) shared by the dashboard pages."""

from __future__ import annotations

import html
from typing import Any

import streamlit as st

DASH = "—"
SYNTHETIC_NOTICE = "Dữ liệu kiểm thử — không phải MBTA"

GREEN, AMBER, RED, GREY, BLUE, PURPLE = "#1b7f3b", "#bf8700", "#cf222e", "#6e7781", "#0969da", "#6f42c1"

SOURCE_STYLE = {
    "live": ("LIVE", "#ffffff", GREEN),
    "replay": ("REPLAY", "#1f1300", "#f2b400"),
    "synthetic": ("SYNTHETIC", "#ffffff", PURPLE),
}
FRESHNESS_STYLE = {
    "fresh": ("FRESH", "#ffffff", "#1a9850"),
    "stale": ("STALE", "#1f1300", "#f08c00"),
    "unknown": ("KHÔNG RÕ TUỔI", "#ffffff", "#8c8c8c"),
    "invalid": ("INVALID", "#ffffff", RED),
}
QUALITY_STYLE = {
    "realtime": ("REALTIME", "#ffffff", GREEN),
    "schedule_only": ("CHỈ THEO LỊCH", "#ffffff", BLUE),
    "degraded": ("DEGRADED", "#1f1300", "#f2b400"),
    "unknown_uncertainty": ("KHÔNG RÕ ĐỘ BẤT ĐỊNH", "#ffffff", GREY),
    "invalid": ("INVALID", "#ffffff", RED),
}
QUALITY_TEXT = {key: label for key, (label, _fg, _bg) in QUALITY_STYLE.items()}
STATUS_COLORS = {
    "PASS": GREEN, "OK": GREEN, "READY": GREEN, "RUNNING": GREEN, "AVAILABLE": GREEN, "ACTIVE": GREEN,
    "STARTING": BLUE, "EMPTY": BLUE, "UPCOMING": BLUE, "CREATED": GREY,
    "WARN": AMBER, "PAUSED": AMBER, "STALE": AMBER, "PARTIAL": AMBER, "UNKNOWN_DURATION": AMBER,
    "BUDGET_EXHAUSTED": AMBER, "STEP_WAIT": AMBER, "UNCONFIRMED_INHERITED": AMBER,
    "FAIL": RED, "FAILED": RED, "ERROR": RED, "EXPIRED": RED, "UNAVAILABLE": RED, "INCONSISTENT": RED,
    "FINISHED": GREY, "STOPPED": GREY, "NOT_RUN": GREY, "NOT_APPLICABLE": GREY, "ALWAYS": GREY,
    "CONFIRMED": GREEN,
}


def esc(value: Any) -> str:
    return DASH if value is None else html.escape(str(value), quote=True)


def badge(text: Any, fg: str = "#ffffff", bg: str = GREY, title: str | None = None) -> str:
    tip = f' title="{esc(title)}"' if title else ""
    return (f'<span style="display:inline-block;padding:1px 8px;border-radius:10px;font-weight:600;'
            f'font-size:0.8rem;background:{bg};color:{fg}"{tip}>{esc(text)}</span>')


def status_badge(value: Any) -> str:
    if value is None or value == "":
        return DASH
    color = STATUS_COLORS.get(str(value).upper(), GREY)
    return badge(value, "#1f1300" if color == AMBER else "#ffffff", color)


def source_badge(source_type: str | None) -> str:
    label, fg, bg = SOURCE_STYLE.get((source_type or "").lower(), ("NGUỒN CHƯA RÕ", "#ffffff", GREY))
    return badge(label, fg, bg)


def freshness_badge(status: str | None) -> str:
    label, fg, bg = FRESHNESS_STYLE.get((status or "unknown").lower(), FRESHNESS_STYLE["unknown"])
    return badge(label, fg, bg)


def quality_badge(quality: str | None) -> str:
    key = (quality or "").lower()
    label, fg, bg = QUALITY_STYLE.get(key, (key.upper() or DASH, "#ffffff", GREY))
    return badge(label, fg, bg)


def html_block(fragment: str) -> None:
    """Render a single-line HTML block (all dynamic text must already be escaped)."""
    st.markdown("<div>" + fragment.replace("\n", " ") + "</div>", unsafe_allow_html=True)


def legend_quality() -> str:
    return " ".join(badge(label, fg, bg) for label, fg, bg in QUALITY_STYLE.values())


def legend_freshness() -> str:
    return " ".join(badge(label, fg, bg) for label, fg, bg in FRESHNESS_STYLE.values())
