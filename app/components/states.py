"""UI states and formatting: one dedicated rendering per ApiResult kind, tables, feed health, warnings.

Rules: a failed call never shows numbers (no zero-fill); missing values are shown as "—"; stale data stays
visible with its timestamp and a STALE badge; expired generations show "không khả dụng".
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Sequence
from typing import Any

import streamlit as st

from api_client import ApiResult
from components.badges import DASH, esc, html_block, status_badge

FEEDS = ("vehicle_positions", "trip_updates", "alerts")
FEED_LABELS = {
    "vehicle_positions": "Vị trí xe (VP)",
    "trip_updates": "Dự báo chuyến (TU)",
    "alerts": "Cảnh báo dịch vụ (Alerts)",
}
FEED_STATE_TEXT = {
    "available": "có generation READY",
    "empty": "snapshot hợp lệ nhưng rỗng",
    "stale": "dữ liệu cũ (stale) — vẫn hiển thị kèm thời điểm",
    "expired": "generation đã hết hạn — không hiển thị số cũ",
    "unavailable": "chưa có generation READY",
    "inconsistent": "generation thiếu dòng so với manifest — không hiển thị số",
    "not_applicable": "không áp dụng",
}
_UNAVAILABLE_TEXT = {
    "generation_unavailable": "Chưa có generation READY — pipeline đang chờ/đang xử lý snapshot đầu tiên.",
    "generation_inconsistent": "Generation thiếu dòng so với manifest — không hiển thị (không coi là 0 xe).",
    "database_unavailable": "Cassandra không truy cập được.",
    "database_timeout": "Truy vấn Cassandra quá thời gian.",
    "run_clock_unavailable": "Run chưa có đồng hồ ứng dụng (chưa nhận clock tick đầu tiên).",
    "bad_response": "Phản hồi API không đúng định dạng.",
}


# ------------------------------------------------------------------------------------------------ formatting
def is_num(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def num_text(value: float, digits: int = 1) -> str:
    text = f"{float(value):.{digits}f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def fmt(value: Any, unit: str = "") -> str:
    """Plain (unescaped) display text; None -> "—" (never 0)."""
    if value is None:
        return DASH
    if isinstance(value, bool):
        return "có" if value else "không"
    if isinstance(value, float):
        if not math.isfinite(value):
            return DASH
        text = num_text(value, 2)
    elif isinstance(value, (list, tuple, set)):
        text = ", ".join(str(v) for v in value) or "(không có)"
    elif isinstance(value, dict):
        text = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    else:
        text = str(value)
    return f"{text} {unit}" if unit else text


def fmt_ts(value: Any) -> str:
    if value is None or value == "":
        return DASH
    text = str(value)
    if len(text) >= 19 and text[10] == "T" and (text.endswith("Z") or text.endswith("+00:00")):
        return text[:10] + " " + text[11:19] + " UTC"
    return text


def fmt_seconds(value: Any) -> str:
    if not is_num(value):
        return DASH
    v = float(value)
    text = f"{num_text(v, 1)} s"
    if abs(v) >= 60:
        minutes, seconds = divmod(int(round(abs(v))), 60)
        text += f" ({minutes} phút {seconds:02d} giây)"
    return text


def fmt_delay(value: Any) -> str:
    """Signed predicted delay; None stays "—" (not computable), never 0."""
    if not is_num(value):
        return DASH
    v = float(value)
    sign = "+" if v > 0 else ("−" if v < 0 else "±")
    text = f"{sign}{num_text(abs(v), 0)} s"
    if abs(v) >= 60:
        minutes, seconds = divmod(int(round(abs(v))), 60)
        text += f" ({sign}{minutes}:{seconds:02d})"
    return text


def short(value: Any, n: int = 12) -> str:
    if value is None or value == "":
        return DASH
    text = str(value)
    return text if len(text) <= n else text[:n] + "…"


# ------------------------------------------------------------------------------------------------ states
def render(res: ApiResult | None, what: str) -> None:
    """Dedicated rendering for every non-OK kind (and for ``None`` = not requested)."""
    if res is None:
        st.info(f"Chưa có yêu cầu cho {what}.")
        return
    details = []
    if res.request_id:
        details.append(f"request_id `{res.request_id}`")
    if res.error and res.error.get("retryable") is not None:
        details.append("có thể thử lại" if res.error.get("retryable") else "không nên thử lại")
    if res.error and res.error.get("hint"):
        details.append(f"gợi ý: {res.error.get('hint')}")
    suffix = (" · " + " · ".join(details)) if details else ""
    code = res.error_code or ""
    message = res.error_message or ""
    if res.kind == "unreachable":
        st.error(f"**Không kết nối được API** khi tải {what}. API chưa chạy hoặc đã dừng "
                 f"(kiểm tra `scripts/demo.py status`). Chi tiết: {message}{suffix}")
    elif res.kind == "expired":
        st.error(f"**Không khả dụng — generation đã hết hạn** (503 `generation_expired`) cho {what}. "
                 f"Không hiển thị số cũ; chờ snapshot mới.{suffix}")
    elif res.kind == "unavailable":
        explain = _UNAVAILABLE_TEXT.get(code, f"API trả lỗi {res.http_status or '?'}.")
        st.error(f"**Không khả dụng ({res.http_status or '?'})** cho {what}: {explain} `{code or 'không có mã'}` "
                 f"{message}{suffix}")
    elif res.kind == "not_found":
        st.warning(f"**Không tìm thấy (404)** cho {what}: `{code}` {message}{suffix}")
    elif res.kind == "invalid":
        st.warning(f"**Yêu cầu không hợp lệ ({res.http_status or 422})** cho {what}: `{code}` {message}{suffix}")
    elif res.kind == "empty":
        st.info(f"**Không có dữ liệu** cho {what}: phản hồi hợp lệ nhưng rỗng (không phải số 0 mặc định). "
                f"Lý do: {res.reason or 'không có lý do kèm theo'}{suffix}")
    elif res.kind == "stale":
        st.warning(f"**Dữ liệu cũ (stale)** cho {what}: vẫn hiển thị giá trị gần nhất kèm thời điểm.{suffix}")


def show_or_stop(res: ApiResult, what: str) -> bool:
    """Render the state of a non-usable / empty result; return True when the caller can render data."""
    if not res.usable or res.kind == "empty":
        render(res, what)
        return False
    if res.kind == "stale":
        render(res, what)
    return True


# ------------------------------------------------------------------------------------------------ tables
def table(rows: Sequence[dict[str, Any]], columns: Sequence[tuple[str, str]] | None = None,
          *, formatters: dict[str, Any] | None = None) -> None:
    """Display rows with every cell converted to text ("—" for missing), so nothing is coerced to 0."""
    rows = [r for r in rows if isinstance(r, dict)]
    if not rows:
        st.caption("(không có dòng nào)")
        return
    if columns is None:
        keys: list[str] = []
        for r in rows:
            for k in r:
                if k not in keys:
                    keys.append(k)
        columns = [(k, k) for k in keys]
    formatters = formatters or {}
    out = []
    for r in rows:
        out.append({label: (formatters[key](r.get(key)) if key in formatters else fmt(r.get(key)))
                    for key, label in columns})
    st.dataframe(out, hide_index=True)


def kv(mapping: dict[str, Any], *, formatters: dict[str, Any] | None = None) -> None:
    formatters = formatters or {}
    rows = [{"Trường": k, "Giá trị": formatters[k](v) if k in formatters else fmt(v)} for k, v in mapping.items()]
    if rows:
        st.dataframe(rows, hide_index=True)


def feed_health(meta: dict[str, Any]) -> None:
    feeds = meta.get("feeds") if isinstance(meta, dict) else None
    if not isinstance(feeds, dict) or not feeds:
        st.caption("API không trả thông tin feed cho yêu cầu này.")
        return
    rows = []
    for feed in list(FEEDS) + [f for f in feeds if f not in FEEDS]:
        block = feeds.get(feed)
        if not isinstance(block, dict):
            continue
        state = block.get("state")
        flags = [str(state).upper() if state else DASH]
        if block.get("stale") is True and state != "stale":
            flags.append("STALE")
        if block.get("is_partial") is True:
            flags.append("PARTIAL")
        rows.append({
            "Feed": FEED_LABELS.get(feed, feed),
            "Trạng thái": " · ".join(flags),
            "Ý nghĩa": FEED_STATE_TEXT.get(str(state), DASH),
            "snapshot_id": short(block.get("snapshot_id"), 16),
            "feed_timestamp": fmt_ts(block.get("feed_timestamp")),
            "metrics_as_of": fmt_ts(block.get("metrics_as_of")),
            "Tuổi nguồn": fmt_seconds(block.get("source_age_seconds")),
            "Ngưỡng stale": fmt(block.get("stale_threshold_seconds"), "s"),
            "Hết hạn lúc": fmt_ts(block.get("expires_at")),
        })
    st.dataframe(rows, hide_index=True)


def pipeline_warnings(items: Iterable[dict[str, Any]], title: str = "Cảnh báo chất lượng pipeline") -> None:
    st.markdown(f"#### {title}")
    seen = set()
    shown = 0
    for w in items:
        if not isinstance(w, dict):
            continue
        key = (w.get("code"), w.get("feed_type"), w.get("message"))
        if key in seen:
            continue
        seen.add(key)
        shown += 1
        feed = f" ({esc(w.get('feed_type'))})" if w.get("feed_type") else ""
        html_block(f"{status_badge(w.get('category') or 'INFO')} <code>{esc(w.get('code'))}</code>{feed} — "
                   f"{esc(w.get('message'))}")
    if not shown:
        st.caption("Không có cảnh báo pipeline trong phản hồi lần này.")
