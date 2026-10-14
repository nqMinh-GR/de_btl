"""TransitPulse MBTA — notebook frontend and cell helpers for Google Colab.

Frontend
    ``TransitPulseUI`` shows five tabs (Tổng quan, Bản đồ, Trạm & ETA, Cảnh báo, Chất lượng dữ liệu) built
    only from the FastAPI service through ``ApiClient`` (urllib, 5 s timeout). It never reads Kafka,
    Cassandra, raw files or MBTA, never computes a KPI and never zero-fills: a missing value is shown as
    "—", and every non-OK outcome (empty / stale / expired / 503 / 404 / 422 / unreachable) has its own
    panel. All text that reaches HTML or SVG goes through ``html.escape(..., quote=True)``.

Cell helpers
    ``load_runtime`` (reads ``W/runtime/runtime.json``), ``run_cli`` (absolute core interpreter from
    runtime.json, stderr streamed as progress, the single stdout JSON document parsed, ``CellFailed`` raised
    with code / hint / log excerpt), ``stream_command``, ``run_pytest``, display helpers, ``FaultLab``
    (interactive fault scenarios, NOT_RUN by default) and ``StopPanel``.

Constraints
    Standard library + IPython.display + ipywidgets (7.7.x and 8.x). Python 3.9+ syntax because the Colab
    kernel may differ from the core Python 3.11. This module must not import transitpulse, pyspark or
    cassandra. The notebook (re)loads it with ``importlib.util.spec_from_file_location`` in every cell, so
    the handle of the active UI lives in a tiny shared object stored in ``sys.modules``: showing a new UI
    closes the previous one (callbacks unobserved, timers cancelled, widgets closed).
"""

from __future__ import annotations

import hashlib
import html
import http.client
import json
import math
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
import types
import urllib.error
import urllib.parse
import urllib.request
import uuid
import warnings
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

try:  # optional: without ipywidgets the UI falls back to static HTML panels
    import ipywidgets as widgets  # type: ignore[import-not-found]
except Exception:  # noqa: BLE001 - any import problem simply means "no widgets"
    widgets = None

try:
    from IPython.display import HTML, display  # type: ignore[import-not-found]
except Exception:  # noqa: BLE001
    HTML = None
    display = None

__all__ = [
    "ApiClient",
    "ApiResult",
    "CellFailed",
    "FaultLab",
    "StopPanel",
    "TransitPulseUI",
    "TunnelPanel",
    "benchmark_status",
    "close_active",
    "core_env",
    "expose_stop",
    "failed_steps",
    "record_failure",
    "show_failure_summary",
    "load_runtime",
    "read_json",
    "record_step",
    "render_svg_map",
    "run_cli",
    "run_pytest",
    "session_activity",
    "show_cli_result",
    "stream_command",
    "write_json_atomic",
]

# ================================================================================================
# Constants
# ================================================================================================
DASH = "—"
DEFAULT_API_BASE = "http://127.0.0.1:8000/api/v1"
FEEDS = ("vehicle_positions", "trip_updates", "alerts")
FEED_LABELS = {
    "vehicle_positions": "Vị trí xe (VP)",
    "trip_updates": "Dự báo chuyến (TU)",
    "alerts": "Cảnh báo dịch vụ (Alerts)",
}
TAB_TITLES = ("Tổng quan", "Bản đồ", "Trạm & ETA", "Cảnh báo", "Chất lượng dữ liệu")
MAX_AUTO_REFRESH_TICKS = 60
SYNTHETIC_NOTICE = "Dữ liệu kiểm thử — không phải xe MBTA thật"
MAP_LABEL = "Lược đồ tuyến theo tọa độ (không có bản đồ nền)"
NO_ELIGIBLE_TEXT = "Chưa có mẫu đủ điều kiện"
USABLE_KINDS = ("ok", "stale", "empty")
RESULT_KINDS = ("ok", "empty", "stale", "expired", "unavailable", "not_found", "invalid", "unreachable")

SOURCE_STYLE = {
    "live": ("LIVE", "#ffffff", "#1b7f3b"),
    "replay": ("REPLAY", "#1f1300", "#f2b400"),
    "synthetic": ("SYNTHETIC", "#ffffff", "#6f42c1"),
}
FRESHNESS_COLORS = {"fresh": "#1a9850", "stale": "#f08c00", "unknown": "#8c8c8c", "invalid": "#cf222e"}
FRESHNESS_LABELS = {"fresh": "FRESH", "stale": "STALE", "unknown": "KHÔNG RÕ TUỔI", "invalid": "INVALID"}
QUALITY_BADGES = {
    "realtime": ("REALTIME", "#ffffff", "#1b7f3b"),
    "schedule_only": ("CHỈ THEO LỊCH", "#ffffff", "#0969da"),
    "degraded": ("DEGRADED", "#1f1300", "#f2b400"),
    "unknown_uncertainty": ("KHÔNG RÕ ĐỘ BẤT ĐỊNH", "#ffffff", "#6e7781"),
    "invalid": ("INVALID", "#ffffff", "#cf222e"),
}
_GREEN, _AMBER, _RED, _GREY, _BLUE = "#1b7f3b", "#bf8700", "#cf222e", "#6e7781", "#0969da"
STATUS_COLORS = {
    "PASS": _GREEN, "OK": _GREEN, "READY": _GREEN, "RUNNING": _GREEN, "APPLIED": _GREEN, "VERIFIED": _GREEN,
    "AVAILABLE": _GREEN, "ACTIVE": _GREEN, "LIVE": _GREEN, "PASSED": _GREEN, "TRUE": _GREEN,
    "REUSED": _BLUE, "STARTING": _BLUE, "EMPTY": _BLUE, "UPCOMING": _BLUE, "CREATED": _GREY,
    "WARN": _AMBER, "PAUSED": _AMBER, "STALE": _AMBER, "INCONCLUSIVE": _AMBER, "BUDGET_EXHAUSTED": _AMBER,
    "PASSED_WITH_WARNINGS": _AMBER, "PARTIAL": _AMBER, "STEP_WAIT": _AMBER, "UNKNOWN_DURATION": _AMBER,
    "FAIL": _RED, "FAILED": _RED, "ERROR": _RED, "EXPIRED": _RED, "UNAVAILABLE": _RED, "INCONSISTENT": _RED,
    "FALSE": _RED, "CONFLICT": _RED,
    "NOT_RUN": _GREY, "NOT_VERIFIED": _GREY, "SKIPPED": _GREY, "STOPPED": _GREY, "FINISHED": _GREY,
    "EXITED": _GREY, "NOT_APPLICABLE": _GREY, "ALWAYS": _GREY,
    "REACHABLE": _GREEN, "NOT_REACHABLE_YET": _AMBER, "UNKNOWN": _GREY,
}
FEED_STATE_TEXT = {
    "available": "có generation READY",
    "empty": "snapshot hợp lệ nhưng rỗng",
    "stale": "dữ liệu cũ (stale) — vẫn hiển thị kèm thời điểm",
    "expired": "generation đã hết hạn — không hiển thị số cũ",
    "unavailable": "chưa có generation READY (đang chờ dữ liệu/xử lý)",
    "inconsistent": "generation thiếu dòng so với manifest — không hiển thị số",
    "not_applicable": "không áp dụng cho yêu cầu này",
}

_CSS = (
    "<style>"
    ".tp-root{font-family:system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;font-size:13px;"
    "line-height:1.45;color:#1f2933}"
    ".tp-root h4{margin:12px 0 4px 0;font-size:14px}"
    ".tp-banner{border:1px solid #d0d7de;border-radius:8px;padding:8px 12px;background:#f6f8fa;"
    "color:#1f2933;display:flex;flex-wrap:wrap;gap:4px 16px;align-items:center}"
    ".tp-badge{display:inline-block;padding:1px 8px;border-radius:10px;font-weight:600;font-size:12px}"
    ".tp-card{border:1px solid #d0d7de;border-radius:8px;padding:10px 12px;margin:6px 0;background:#ffffff;"
    "color:#1f2933}"
    ".tp-kpi{font-size:16px;font-weight:600}"
    ".tp-muted{color:#57606a;font-size:12px}"
    ".tp-grid{display:flex;flex-wrap:wrap;gap:8px;margin:4px 0}"
    ".tp-stat{border:1px solid #d0d7de;border-radius:8px;padding:6px 12px;min-width:130px;background:#ffffff;"
    "color:#1f2933}"
    ".tp-stat b{display:block;font-size:20px}"
    ".tp-table{border-collapse:collapse;margin:4px 0 10px 0;font-size:12px;background:#ffffff;color:#1f2933}"
    ".tp-table th,.tp-table td{border:1px solid #d0d7de;padding:3px 7px;text-align:left;vertical-align:top}"
    ".tp-table th{background:#f6f8fa}"
    ".tp-state{border-left:4px solid #6e7781;padding:7px 12px;margin:6px 0;background:#f6f8fa;color:#1f2933}"
    ".tp-state.err{border-color:#cf222e;background:#fff5f5}"
    ".tp-state.warn{border-color:#d4a72c;background:#fffbea}"
    ".tp-state.info{border-color:#0969da;background:#f0f7ff}"
    ".tp-loading{color:#0969da;font-weight:600}"
    ".tp-root pre{white-space:pre-wrap;word-break:break-word;background:#f6f8fa;color:#1f2933;padding:6px;"
    "border-radius:6px;max-height:360px;overflow:auto}"
    "</style>"
)

# ================================================================================================
# Shared state (survives re-loading this module with spec_from_file_location)
# ================================================================================================
_SHARED_KEY = "_transitpulse_colab_ui_shared"


def _shared() -> types.ModuleType:
    mod = sys.modules.get(_SHARED_KEY)
    if mod is None:
        mod = types.ModuleType(_SHARED_KEY)
        mod.active = {}  # type: ignore[attr-defined]
        sys.modules[_SHARED_KEY] = mod
    return mod


_ACTIVE = _shared().active.get("ui")  # type: ignore[attr-defined]


def _activate(kind: str, obj: Any) -> None:
    """Register ``obj`` as the active instance of ``kind``; close the previous one first."""
    global _ACTIVE
    registry = _shared().active  # type: ignore[attr-defined]
    previous = registry.get(kind)
    if previous is not None and previous is not obj:
        try:
            previous.close()
        except Exception:  # noqa: BLE001 - closing a stale UI must never break the new one
            pass
    registry[kind] = obj
    if kind == "ui":
        _ACTIVE = obj


def _deactivate(kind: str, obj: Any) -> None:
    global _ACTIVE
    registry = _shared().active  # type: ignore[attr-defined]
    if registry.get(kind) is obj:
        registry.pop(kind, None)
        if kind == "ui":
            _ACTIVE = None


def active(kind: str = "ui") -> Any:
    return _shared().active.get(kind)  # type: ignore[attr-defined]


def close_active() -> None:
    """Close every registered UI object (auto-refresh timers stop, widgets close)."""
    global _ACTIVE
    registry = _shared().active  # type: ignore[attr-defined]
    for kind in list(registry):
        obj = registry.pop(kind, None)
        try:
            if obj is not None:
                obj.close()
        except Exception:  # noqa: BLE001
            pass
    _ACTIVE = None


# ================================================================================================
# Time and formatting helpers
# ================================================================================================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 instant with an explicit offset (``Z`` accepted). Naive values return None."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text[-1] in "Zz":
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)


def esc(value: Any) -> str:
    if value is None:
        return DASH
    return html.escape(str(value), quote=True)


def _is_num(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _num_text(value: float, digits: int = 1) -> str:
    text = ("%." + str(int(digits)) + "f") % float(value)
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def fmt_value(value: Any, unit: str = "", digits: Optional[int] = None) -> str:
    """Escaped display text; ``None`` (and non-finite numbers) become "—", never 0."""
    if value is None:
        return DASH
    if isinstance(value, bool):
        text = "có" if value else "không"
    elif isinstance(value, float):
        if not math.isfinite(value):
            return DASH
        text = _num_text(value, digits) if digits is not None else "{:g}".format(value)
    elif isinstance(value, int):
        text = str(value)
    elif isinstance(value, (list, tuple, set, frozenset)):
        items = [str(v) for v in value]
        text = ", ".join(items) if items else "(không có)"
    elif isinstance(value, dict):
        text = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        if len(text) > 300:
            text = text[:300] + "…"
    else:
        text = str(value)
    if unit:
        text = text + " " + unit
    return html.escape(text, quote=True)


def fmt_ts(value: Any) -> str:
    """'2026-10-05T12:00:30.000Z' -> '2026-10-05 12:00:30 UTC' (escaped); anything else shown as-is."""
    if value is None or value == "":
        return DASH
    text = str(value)
    if len(text) >= 19 and text[10] == "T" and (text.endswith("Z") or text.endswith("+00:00")):
        return esc(text[:10] + " " + text[11:19] + " UTC")
    return esc(text)


def fmt_seconds(value: Any) -> str:
    if not _is_num(value):
        return DASH
    v = float(value)
    text = "%s s" % _num_text(v, 1)
    if abs(v) >= 60:
        minutes, seconds = divmod(int(round(abs(v))), 60)
        text += " (%d phút %02d giây)" % (minutes, seconds)
    return esc(text)


def fmt_delay(value: Any) -> str:
    """Signed predicted delay; None stays "—" (not computable), never 0."""
    if not _is_num(value):
        return DASH
    v = float(value)
    sign = "+" if v > 0 else ("−" if v < 0 else "±")
    text = "%s%s s" % (sign, _num_text(abs(v), 0))
    if abs(v) >= 60:
        minutes, seconds = divmod(int(round(abs(v))), 60)
        text += " (%s%d:%02d)" % (sign, minutes, seconds)
    return esc(text)


def short_id(value: Any, n: int = 12) -> str:
    if value is None or value == "":
        return DASH
    text = str(value)
    if len(text) <= n:
        return "<code>%s</code>" % esc(text)
    return '<code title="%s">%s…</code>' % (esc(text), esc(text[:n]))


def badge(text: Any, fg: str = "#ffffff", bg: str = _GREY, title: Optional[str] = None) -> str:
    tip = ' title="%s"' % esc(title) if title else ""
    return '<span class="tp-badge" style="background:%s;color:%s"%s>%s</span>' % (bg, fg, tip, esc(text))


def status_badge(value: Any) -> str:
    if value is None or value == "":
        return DASH
    text = str(value)
    color = STATUS_COLORS.get(text.upper(), _GREY)
    fg = "#1f1300" if color == _AMBER else "#ffffff"
    return badge(text, fg, color)


def _note(level: str, title: str, body: str = "") -> str:
    """level: err | warn | info | plain. ``title`` is plain text; ``body`` is pre-escaped HTML."""
    cls = {"err": "tp-state err", "warn": "tp-state warn", "info": "tp-state info"}.get(level, "tp-state")
    return '<div class="%s"><b>%s</b>%s</div>' % (cls, esc(title), ("<br>" + body) if body else "")


def _wrap(fragment: str) -> str:
    return _CSS + '<div class="tp-root">' + fragment + "</div>"


def _strip_tags(fragment: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", fragment)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    return html.unescape(re.sub(r"[ \t]+", " ", text)).strip()


def _multiline(value: Any) -> str:
    if value is None or value == "":
        return DASH
    return esc(value).replace("\n", "<br>")


def seg(value: Any) -> str:
    """URL-quote one path segment (route/stop/vehicle/run IDs may contain any character)."""
    return urllib.parse.quote(str(value), safe="")


def _num_field(row: Any, *keys: str) -> Optional[float]:
    if not isinstance(row, dict):
        return None
    for key in keys:
        value = row.get(key)
        if _is_num(value):
            return float(value)
    return None


def _first(row: Any, *keys: str) -> Any:
    if not isinstance(row, dict):
        return None
    for key in keys:
        value = row.get(key)
        if value is not None and value != "":
            return value
    return None


def extract_list(data: Any, key: str) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict) and isinstance(data.get(key), list):
        rows = data[key]
    else:
        rows = []
    return [r for r in rows if isinstance(r, dict)]


# ================================================================================================
# Notebook process helpers (runtime.json, CLIs, pytest, reports)
# ================================================================================================
class CellFailed(RuntimeError):
    """A notebook step failed. Mirrors the CLI error contract: code, message, hint, log_excerpt."""

    def __init__(
        self,
        code: Any,
        message: Any,
        *,
        hint: Any = None,
        log_excerpt: Any = None,
        exit_code: Optional[int] = None,
        result: Optional[dict] = None,
    ) -> None:
        self.code = str(code or "FAILED")
        self.message = str(message or "")
        self.hint = None if hint is None else str(hint)
        self.log_excerpt = None if log_excerpt is None else str(log_excerpt)
        self.exit_code = exit_code
        self.result = result
        RuntimeError.__init__(self, self._text())

    def _text(self) -> str:
        lines = ["[%s] %s" % (self.code, self.message)]
        if self.exit_code is not None:
            lines.append("exit code: %s" % self.exit_code)
        if self.hint:
            lines.append("Gợi ý: %s" % self.hint)
        if self.log_excerpt:
            excerpt = self.log_excerpt
            if len(excerpt) > 4000:
                excerpt = "…" + excerpt[-4000:]
            lines.append("Trích log:\n" + excerpt)
        return "\n".join(lines)


def read_json(path: Any) -> Any:
    """Return the parsed JSON document, or None when the file does not exist."""
    try:
        with open(str(path), "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise CellFailed("JSON_UNREADABLE", "Không đọc được %s: %s" % (path, exc)) from exc


def write_json_atomic(path: Any, obj: Any) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(".%s.%s.tmp" % (target.name, uuid.uuid4().hex[:8]))
    with open(str(tmp), "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(str(tmp), str(target))
    return target


def runtime_json_path(workdir: Any) -> Path:
    return Path(str(workdir)).expanduser() / "runtime" / "runtime.json"


def load_runtime(workdir: Any) -> Dict[str, Any]:
    """Load ``W/runtime/runtime.json`` written by the bootstrap (prepare_runtime.py)."""
    path = runtime_json_path(workdir)
    if not path.is_file():
        raise CellFailed(
            "RUNTIME_MISSING",
            "Chưa có %s." % path,
            hint="Chạy cell 4 (bootstrap). Sau khi restart kernel: chạy lại cell 2 rồi chạy tiếp cell cần dùng.",
        )
    runtime = read_json(path)
    if not isinstance(runtime, dict):
        raise CellFailed("RUNTIME_INVALID", "%s không phải một JSON object." % path, hint="Chạy lại cell 4.")
    core = (runtime.get("python") or {}).get("core_python")
    if not core or not Path(str(core)).is_file():
        raise CellFailed(
            "CORE_PYTHON_MISSING",
            "runtime.json không trỏ tới interpreter core hợp lệ: %r" % (core,),
            hint="Chạy lại cell 4 (bootstrap); .venv của repo có thể đã bị xóa.",
        )
    if not runtime.get("repo_dir") or not runtime.get("workdir"):
        raise CellFailed("RUNTIME_INVALID", "runtime.json thiếu repo_dir/workdir.", hint="Chạy lại cell 4.")
    return runtime


_STRIP_ENV = ("PYTHONPATH", "PYTHONHOME", "PYTHONSTARTUP", "SPARK_HOME", "CLASSPATH", "VIRTUAL_ENV", "JAVA_HOME")
_KEEP_ENV = ("PATH", "HOME", "LANG", "LC_ALL", "USER", "LOGNAME")


def core_env(runtime: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """Clean environment for core subprocesses: a few basics + ``runtime.json`` ``env`` (canonical)."""
    env: Dict[str, str] = {}
    for key in _KEEP_ENV:
        if os.environ.get(key):
            env[key] = os.environ[key]
    env.setdefault("LANG", "C.UTF-8")
    for key, value in ((runtime or {}).get("env") or {}).items():
        if value is not None:
            env[str(key)] = str(value)
    env["PYTHONUNBUFFERED"] = "1"
    env.setdefault("PYTHONIOENCODING", "utf-8")
    for key, value in (extra or {}).items():
        env[str(key)] = str(value)
    return env


def bootstrap_env(extra: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """Kernel environment minus variables that leak a foreign Python/Spark/Java into the bootstrap."""
    env = {k: v for k, v in os.environ.items() if k not in _STRIP_ENV and not k.startswith("PYSPARK_")}
    env["PYTHONUNBUFFERED"] = "1"
    for key, value in (extra or {}).items():
        env[str(key)] = str(value)
    return env


def _pretty_progress(line: str) -> str:
    text = line.split("\r")[-1].rstrip()
    if text.startswith("{") and text.endswith("}"):
        try:
            rec = json.loads(text)
        except ValueError:
            rec = None
        if isinstance(rec, dict):
            ts = str(rec.get("ts") or "")[11:19]
            bits = [str(b) for b in (ts, rec.get("level"), rec.get("component"), rec.get("stage"), rec.get("status")) if b]
            msg = rec.get("msg") or rec.get("message") or ""
            text = " ".join(bits) + ((": " + str(msg)) if msg else "")
    if len(text) > 400:
        text = text[:400] + "…"
    return text


def _terminate(proc: "subprocess.Popen[bytes]") -> None:
    """Stop only the direct child (the CLI). Detached services run in their own sessions."""
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
            proc.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            pass
    except OSError:
        pass


def stream_command(
    argv: Sequence[Any],
    *,
    cwd: Any = None,
    env: Optional[Dict[str, str]] = None,
    timeout: float = 600.0,
    label: Optional[str] = None,
    capture_stdout: bool = False,
    echo: bool = True,
    tail_lines: int = 200,
    max_echo_lines: int = 4000,
) -> Tuple[int, str, List[str], float]:
    """Run ``argv`` (an argument list, never a shell string) with a hard timeout.

    Progress (stderr, or stdout+stderr when ``capture_stdout`` is False) is streamed line by line.
    Returns ``(returncode, captured_stdout, progress_tail, seconds)``; raises ``CellFailed`` on timeout.
    """
    args = [str(a) for a in argv]
    name = label or os.path.basename(args[0])
    started = time.monotonic()
    if echo:
        print("▶ %s (giới hạn %d s)" % (name, int(timeout)), flush=True)
    try:
        proc = subprocess.Popen(
            args,
            cwd=str(cwd) if cwd else None,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE if capture_stdout else subprocess.STDOUT,
        )
    except OSError as exc:
        raise CellFailed("COMMAND_NOT_STARTED", "Không chạy được %s: %s" % (args[0], exc)) from exc

    lines: "queue.Queue[Optional[bytes]]" = queue.Queue()
    stdout_chunks: List[bytes] = []

    def _pump(stream: Any) -> None:
        try:
            for raw in iter(stream.readline, b""):
                lines.put(raw)
        except (OSError, ValueError):
            pass
        finally:
            lines.put(None)

    def _collect(stream: Any) -> None:
        try:
            for raw in iter(stream.readline, b""):
                stdout_chunks.append(raw)
        except (OSError, ValueError):
            pass

    threads = [threading.Thread(target=_pump, args=(proc.stderr if capture_stdout else proc.stdout,), daemon=True)]
    if capture_stdout:
        threads.append(threading.Thread(target=_collect, args=(proc.stdout,), daemon=True))
    for t in threads:
        t.start()

    tail: "deque[str]" = deque(maxlen=max(10, int(tail_lines)))
    echoed = 0
    eof = False
    exited_at: Optional[float] = None
    last_beat = started
    deadline = started + float(timeout)
    while True:
        now = time.monotonic()
        if now > deadline:
            _terminate(proc)
            raise CellFailed(
                "TIMEOUT",
                "%s vượt quá %d giây; chỉ tiến trình CLI này bị dừng." % (name, int(timeout)),
                hint="Xem trích log; kiểm tra trạng thái bằng demo.py status trước khi chạy lại.",
                log_excerpt="\n".join(tail),
            )
        if proc.poll() is not None:
            if eof:
                break
            if exited_at is None:
                exited_at = now
            elif now - exited_at > 3.0 and lines.empty():  # a grandchild holds the pipe; the CLI has exited
                break
        try:
            raw = lines.get(timeout=0.5)
        except queue.Empty:
            if echo and now - last_beat >= 30:
                print("  … vẫn đang chạy (%d s)" % int(now - started), flush=True)
                last_beat = now
            continue
        if raw is None:
            eof = True
            continue
        line = raw.decode("utf-8", "replace").rstrip("\n")
        tail.append(line.split("\r")[-1])
        if echo:
            if echoed < max_echo_lines:
                print("  │ " + _pretty_progress(line), flush=True)
            elif echoed == max_echo_lines:
                print("  │ … (ẩn bớt dòng tiến trình; log đầy đủ nằm trong workdir)", flush=True)
            echoed += 1
            last_beat = now
    rc = proc.returncode if proc.returncode is not None else proc.wait(timeout=5)
    for t in threads:
        t.join(timeout=2.0)
    elapsed = time.monotonic() - started
    if echo:
        print("■ %s: exit %s sau %.1f s" % (name, rc, elapsed), flush=True)
    return int(rc), b"".join(stdout_chunks).decode("utf-8", "replace"), list(tail), elapsed


def parse_json_document(text: str) -> Optional[Dict[str, Any]]:
    """Parse the single JSON object a CLI prints on stdout (tolerates stray lines before it)."""
    body = (text or "").strip()
    if not body:
        return None
    try:
        obj = json.loads(body)
        return obj if isinstance(obj, dict) else None
    except ValueError:
        pass
    for line in reversed(body.splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if isinstance(obj, dict):
                return obj
    start = body.find("{")
    if start >= 0:
        try:
            obj, _end = json.JSONDecoder().raw_decode(body[start:])
            return obj if isinstance(obj, dict) else None
        except ValueError:
            return None
    return None


def run_cli(
    runtime: Dict[str, Any],
    script: str,
    *args: Any,
    timeout: float = 600.0,
    label: Optional[str] = None,
    check: bool = True,
    echo: bool = True,
    show: Optional[Callable[[Dict[str, Any]], Any]] = None,
    extra_env: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Run ``<core_python> <repo>/scripts/<script> *args --workdir W`` and return its stdout JSON.

    stderr is streamed as progress. ``show`` (if given) renders the JSON before any failure is raised.
    With ``check`` a non-zero exit or ``ok: false`` raises ``CellFailed(code, message, hint, log_excerpt)``.
    """
    repo = Path(str(runtime["repo_dir"]))
    workdir = str(runtime["workdir"])
    core = str(runtime["python"]["core_python"])
    script_path = repo / "scripts" / script
    if not script_path.is_file():
        raise CellFailed("SCRIPT_MISSING", "Không thấy %s." % script_path, hint="Clone chưa đầy đủ hoặc sai ref (cell 3).")
    str_args = [str(a) for a in args]
    argv = [core, str(script_path)] + str_args
    if "--workdir" not in str_args:
        argv += ["--workdir", workdir]
    name = label or ("%s %s" % (script, str_args[0] if str_args else "")).strip()
    rc, out, tail, elapsed = stream_command(
        argv, cwd=repo, env=core_env(runtime, extra_env), timeout=timeout, label=name, capture_stdout=True, echo=echo
    )
    doc = parse_json_document(out)
    if doc is None:
        raise CellFailed(
            "NO_JSON_OUTPUT",
            "%s không in một tài liệu JSON hợp lệ trên stdout (exit %s)." % (script, rc),
            hint="Mỗi CLI phải in đúng một JSON trên stdout; xem tiến trình stderr phía trên.",
            log_excerpt="\n".join(tail[-60:]) or out[-2000:],
            exit_code=rc,
        )
    doc["_cli"] = {"script": script, "args": str_args, "exit_code": rc, "seconds": round(elapsed, 1)}
    if script == "demo.py" and str_args[:1] == ["benchmark"] and "status" not in doc:
        # demo.py benchmark has no top-level status, only `statuses` counts: derive it here so no caller can fall
        # back to a default (MEASURED only if statuses.MEASURED > 0, PARTIAL if PARTIAL > 0, else NOT_RUN).
        doc["status"] = benchmark_status(doc)
        doc["_cli"]["derived_status"] = True
    if show is not None:
        try:
            show(doc)
        except Exception as exc:  # noqa: BLE001 - display problems never hide the CLI outcome
            print("(không hiển thị được kết quả: %s)" % exc)
    if check and (rc != 0 or doc.get("ok") is False):
        err = doc.get("error") if isinstance(doc.get("error"), dict) else {}
        raise CellFailed(
            err.get("code") or ("EXIT_%s" % rc),
            err.get("message") or ("%s thất bại (exit %s)." % (name, rc)),
            hint=err.get("hint"),
            log_excerpt=err.get("log_excerpt") or "\n".join(tail[-60:]),
            exit_code=rc,
            result=doc,
        )
    return doc


_PYTEST_EXIT = {
    1: "Có test thất bại — xem danh sách FAILED phía trên và file junit.xml.",
    2: "pytest bị ngắt.",
    3: "Lỗi nội bộ pytest.",
    4: "Sai cách gọi pytest (marker/đường dẫn).",
    5: "Không thu thập được test nào — thư mục test có thể thiếu.",
}


def run_pytest(
    runtime: Dict[str, Any],
    paths: Sequence[str],
    *,
    marker: str,
    report_name: str,
    timeout: float = 2400.0,
    extra_args: Sequence[str] = (),
    check: bool = True,
) -> Dict[str, Any]:
    """Run pytest with the core interpreter and record PASS / FAIL in notebook_steps.json.

    ``check=True``: a failure raises ``CellFailed``. ``check=False`` (Run All keeps going): a failure is recorded
    as FAIL with the junit path and a bounded output excerpt, shown in a prominent red box, and returned as
    ``{"status": "FAIL", ...}`` — never swallowed silently and never reported as PASS.
    """
    repo = Path(str(runtime["repo_dir"]))
    workdir = Path(str(runtime["workdir"]))
    step = "pytest_" + report_name
    junit = workdir / "reports" / report_name / "junit.xml"
    junit.parent.mkdir(parents=True, exist_ok=True)
    argv = [str(runtime["python"]["core_python"]), "-m", "pytest"] + list(paths)
    argv += ["-m", marker, "-q", "--junitxml", str(junit)] + list(extra_args)
    detail = {"junit": str(junit), "marker": marker, "paths": list(paths)}
    try:
        rc, _out, tail, elapsed = stream_command(
            argv,
            cwd=repo,
            env=core_env(runtime, {"TP_WORKDIR": str(workdir)}),
            timeout=timeout,
            label="pytest -m '%s'" % marker,
        )
    except CellFailed as exc:  # timeout or interpreter not startable
        if check:
            record_step(workdir, step, "FAIL", code=exc.code, **detail)
            raise
        record_failure(workdir, step, exc, **detail)
        return {"status": "FAIL", "junit": str(junit), "code": exc.code, "exit_code": exc.exit_code}
    if rc == 0:
        record_step(workdir, step, "PASS", exit_code=rc, seconds=round(elapsed, 1), **detail)
        return {"status": "PASS", "junit": str(junit), "seconds": round(elapsed, 1), "exit_code": rc}
    failure = CellFailed(
        "PYTEST_EXIT_%d" % rc,
        "pytest -m '%s' thất bại (exit %d)." % (marker, rc),
        hint=_PYTEST_EXIT.get(rc, "Xem log phía trên."),
        log_excerpt="\n".join(tail[-80:]),
        exit_code=rc,
    )
    if check:
        record_step(workdir, step, "FAIL", code=failure.code, exit_code=rc, seconds=round(elapsed, 1), **detail)
        raise failure
    record_failure(workdir, step, failure, seconds=round(elapsed, 1), **detail)
    return {"status": "FAIL", "junit": str(junit), "seconds": round(elapsed, 1), "code": failure.code, "exit_code": rc}


def record_step(workdir: Any, step: str, status: str, **detail: Any) -> None:
    """Persist a notebook step status (PASS / FAIL / NOT_RUN / ...) in ``W/reports/notebook/``.

    Returns None on purpose: notebook cells often end with this call and must not echo a dict as output.
    """
    path = notebook_steps_path(workdir)
    try:
        doc = read_json(path)
    except CellFailed:
        doc = None
    if not isinstance(doc, dict) or not isinstance(doc.get("steps"), dict):
        doc = {"schema_version": 1, "steps": {}}
    entry = {"status": status, "recorded_at": iso_utc(utcnow())}
    entry.update(detail)
    doc["steps"][step] = entry
    doc["updated_at"] = entry["recorded_at"]
    write_json_atomic(path, doc)


def benchmark_status(doc: Any) -> str:
    """Step status from ``demo.py benchmark`` JSON ``statuses`` counts {MEASURED, PARTIAL, NOT_RUN}.

    MEASURED if statuses.MEASURED > 0, else PARTIAL if statuses.PARTIAL > 0, else NOT_RUN — never defaults to
    MEASURED (a missing/unknown ``statuses`` block means nothing was measured).
    """
    statuses = doc.get("statuses") if isinstance(doc, dict) else None
    if not isinstance(statuses, dict):
        return "NOT_RUN"

    def count(key: str) -> float:
        value = statuses.get(key)
        return float(value) if _is_num(value) else 0.0

    if count("MEASURED") > 0:
        return "MEASURED"
    if count("PARTIAL") > 0:
        return "PARTIAL"
    return "NOT_RUN"


def notebook_steps_path(workdir: Any) -> Path:
    return Path(str(workdir)) / "reports" / "notebook" / "notebook_steps.json"


def _fail_box(title: str, body: str) -> str:
    """Prominent red box (``title`` plain text, ``body`` pre-escaped HTML)."""
    return ('<div style="border:3px solid #cf222e;background:#fff5f5;color:#1f2933;border-radius:8px;'
            'padding:10px 14px;margin:8px 0"><div style="color:#cf222e;font-size:16px;font-weight:700">%s</div>%s</div>'
            % (esc(title), body))


def record_failure(workdir: Any, step: str, exc: BaseException, **detail: Any) -> None:
    """Record ``step`` as FAIL (code, message, hint, bounded output excerpt) and show it in a red box.

    Used where Run All must continue (tests, golden smoke, benchmark, export): the failure stays visible in the
    cell, in the summaries of cells 14/17/18 and in notebook_steps.json (exported in the ZIP). Never raises.
    """
    code = getattr(exc, "code", None) or type(exc).__name__
    message = getattr(exc, "message", None) or str(exc)
    hint = getattr(exc, "hint", None)
    excerpt = getattr(exc, "log_excerpt", None)
    if excerpt and len(str(excerpt)) > 3000:
        excerpt = "…" + str(excerpt)[-3000:]
    try:
        record_step(workdir, step, "FAIL", code=code, message=message, hint=hint,
                    exit_code=getattr(exc, "exit_code", None), log_excerpt=excerpt, **detail)
    except Exception as rec_exc:  # noqa: BLE001 - recording must not hide the original failure
        print("Không ghi được notebook_steps.json: %s" % rec_exc)
    body = "Mã <code>%s</code> — %s" % (esc(code), _multiline(message))
    if hint:
        body += "<br>Gợi ý: %s" % _multiline(hint)
    if detail.get("junit"):
        body += "<br>junit: <code>%s</code>" % esc(detail.get("junit"))
    if excerpt:
        body += "<pre>%s</pre>" % esc(excerpt)
    body += ("<div>Run All vẫn tiếp tục để session, UI và link vẫn dùng được; lỗi này được ghi FAIL trong "
             "reports/notebook/notebook_steps.json và hiện lại ở tóm tắt cell 14/17/18.</div>")
    show_html(_fail_box("Bước %s THẤT BẠI" % step, body))


def failed_steps(workdir: Any) -> List[Dict[str, Any]]:
    """Steps recorded as FAIL in notebook_steps.json (an unreadable file counts as a failure)."""
    try:
        doc = read_json(notebook_steps_path(workdir))
    except CellFailed as exc:
        return [{"step": "notebook_steps.json", "status": "FAIL", "code": exc.code, "message": exc.message}]
    steps = doc.get("steps") if isinstance(doc, dict) else None
    if not isinstance(steps, dict):
        return []
    out = []
    for name, entry in steps.items():
        if isinstance(entry, dict) and str(entry.get("status") or "").upper() == "FAIL":
            row = {"step": name}
            row.update(entry)
            out.append(row)
    return out


def render_failure_summary(workdir: Any, where: str = "") -> str:
    """Red summary of every FAIL step, or '' when none failed (nothing is ever reported as PASS here)."""
    failed = failed_steps(workdir)
    if not failed:
        return ""
    table = rows_table(failed, columns=["step", "code", "message", "junit", "recorded_at"])
    note = ("<div>Các lỗi trên là kết quả thật, không bị bỏ qua: Run All vẫn chạy tiếp để demo vẫn dùng được. "
            "Chi tiết trong reports/notebook/notebook_steps.json (có trong ZIP export). Đặt STRICT_RUN_ALL = True "
            "ở cell 2 để cell 18 báo lỗi khi còn bước FAIL.</div>")
    title = "%d bước đã THẤT BẠI%s" % (len(failed), (" — " + where) if where else "")
    return _fail_box(title, table + note)


def show_failure_summary(workdir: Any, where: str = "") -> List[Dict[str, Any]]:
    """Display the red summary (or a one-line note when no step failed); returns the failed steps."""
    fragment = render_failure_summary(workdir, where)
    if fragment:
        show_html(fragment)
    else:
        print("Không có bước nào được ghi FAIL trong notebook_steps.json%s." % ((" (" + where + ")") if where else ""))
    return failed_steps(workdir)


def expose_stop(runtime: Dict[str, Any], *, target: str = "both",
                show: bool = True) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[str]]:
    """``demo.py expose stop --target both``: close every public access component (dashboard + API tunnels).

    Does not need cloudflared. Never raises: returns (json, error_text, error_code) and records ``tunnel_stop``
    (PASS / FAIL, or NOT_RUN when the tunnel was disabled at bootstrap).
    """
    args = ["expose", "stop", "--target", target]
    try:
        doc = run_cli(runtime, "demo.py", *args, timeout=300, label="demo.py " + " ".join(args),
                      show=(lambda d: show_cli_result(d, "demo.py " + " ".join(args))) if show else None)
    except CellFailed as exc:
        if exc.code == "TUNNEL_DISABLED":
            record_step(runtime["workdir"], "tunnel_stop", "NOT_RUN", via="demo.py " + " ".join(args), code=exc.code)
            return exc.result, None, exc.code
        record_failure(runtime["workdir"], "tunnel_stop", exc, via="demo.py " + " ".join(args))
        return exc.result, "[%s] %s" % (exc.code, exc.message), exc.code
    except Exception as exc:  # noqa: BLE001
        record_failure(runtime["workdir"], "tunnel_stop", exc, via="demo.py " + " ".join(args))
        return None, "%s: %s" % (type(exc).__name__, exc), "EXCEPTION"
    record_step(runtime["workdir"], "tunnel_stop", "PASS", via="demo.py " + " ".join(args))
    tunnel = active("tunnel")
    if tunnel is not None:
        try:
            tunnel.close()
        except Exception:  # noqa: BLE001
            pass
    return doc, None, None


_ACTIVE_SESSION_STATES = ("RUNNING", "PAUSED", "STARTING", "STEP_WAIT")
SESSION_BUSY_HINT = ("Dừng session demo trước bằng cell 18 (nút Dừng session) hoặc scripts/demo.py stop, "
                     "rồi chạy lại cell này.")


def session_activity(workdir: Any) -> Dict[str, Any]:
    """Read-only check whether a demo session still holds the single processor slot.

    Reads ``W/processes/registry.json`` (``current_session``) and ``W/sessions/<sid>/session.json``
    (``state``). ``busy`` is True when the current session is RUNNING / PAUSED (or still starting), or when
    its state cannot be read (conservative). Never starts, stops or modifies anything.
    """
    root = Path(str(workdir))
    info: Dict[str, Any] = {"busy": False, "session_id": None, "state": None, "processor_state": None,
                            "reason": None}
    try:
        registry = read_json(root / "processes" / "registry.json")
    except CellFailed as exc:
        info.update(busy=True, reason="không đọc được registry.json (%s)" % exc.message)
        return info
    if not isinstance(registry, dict):
        return info  # no registry yet: nothing has been started in this workdir
    comps = registry.get("components") if isinstance(registry.get("components"), dict) else {}
    proc = comps.get("processor") if isinstance(comps.get("processor"), dict) else {}
    info["processor_state"] = proc.get("state")
    sid = registry.get("current_session")
    if not sid:
        return info
    info["session_id"] = str(sid)
    try:
        session = read_json(root / "sessions" / str(sid) / "session.json")
    except CellFailed:
        session = None
    state = session.get("state") if isinstance(session, dict) else None
    info["state"] = state
    if state is None or str(state).upper() in _ACTIVE_SESSION_STATES:
        info["busy"] = True
        info["reason"] = "session %s đang ở trạng thái %s" % (sid, state or "không rõ")
    return info


def sha256_file(path: Any) -> str:
    digest = hashlib.sha256()
    with open(str(path), "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def export_zip_path(doc: Dict[str, Any], workdir: Any) -> Path:
    """Locate the ZIP written by export_artifacts.py (JSON field first, newest ``W/artifacts/*.zip`` else)."""
    candidates: List[Any] = []
    for key in ("zip_path", "zip", "artifact_path", "archive_path", "path"):
        candidates.append(doc.get(key))
    for key in ("artifact", "zip", "export", "result"):
        nested = doc.get(key)
        if isinstance(nested, dict):
            for sub in ("zip_path", "path", "zip"):
                candidates.append(nested.get(sub))
    for cand in candidates:
        if isinstance(cand, str) and cand.endswith(".zip") and Path(cand).is_file():
            return Path(cand)
    art_dir = Path(str(workdir)) / "artifacts"
    zips = sorted(art_dir.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True) if art_dir.is_dir() else []
    if zips:
        return zips[0]
    raise CellFailed("EXPORT_ZIP_NOT_FOUND", "Không tìm thấy ZIP artifact sau khi export.", hint="Xem JSON export phía trên.")


def export_reported_sha(doc: Dict[str, Any]) -> Optional[str]:
    for key in ("zip_sha256", "sha256"):
        value = doc.get(key)
        if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value):
            return value
    for key in ("artifact", "zip"):
        nested = doc.get(key)
        if isinstance(nested, dict):
            value = nested.get("sha256")
            if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value):
                return value
    return None


def copy_finished_zip(zip_path: Any, dest_dir: Any, expected_sha256: str) -> Path:
    """Copy a finished ZIP to ``dest_dir`` (tmp name + os.replace) and verify its sha256 after copying."""
    src = Path(str(zip_path))
    dest_root = Path(str(dest_dir))
    dest_root.mkdir(parents=True, exist_ok=True)
    final = dest_root / src.name
    tmp = dest_root / (".%s.%s.partial" % (src.name, uuid.uuid4().hex[:8]))
    shutil.copyfile(str(src), str(tmp))
    copied = sha256_file(tmp)
    if copied != expected_sha256:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise CellFailed("COPY_CHECKSUM_MISMATCH", "sha256 bản sao khác bản gốc.", hint="Thử lại; không dùng bản sao lỗi.")
    os.replace(str(tmp), str(final))
    return final


# ------------------------------------------------------------------------------------------------
# Generic display of CLI JSON documents
# ------------------------------------------------------------------------------------------------
_STATUS_KEYS = ("status", "overall", "state", "verdict", "outcome", "result")


def _cell_html(key: str, value: Any) -> str:
    if key in _STATUS_KEYS and isinstance(value, str):
        return status_badge(value)
    if key in ("passed", "ok") and isinstance(value, bool):
        return status_badge("PASS" if value else "FAIL")
    if isinstance(key, str) and (key.endswith("_at") or key in ("ts", "clock", "application_clock")):
        return fmt_ts(value) if isinstance(value, str) else fmt_value(value)
    return fmt_value(value)


def kv_table(mapping: Dict[str, Any], *, title: Optional[str] = None, order: Sequence[str] = ()) -> str:
    if not mapping:
        return ""
    keys = [k for k in order if k in mapping] + [k for k in mapping if k not in order]
    rows = "".join(
        "<tr><th>%s</th><td>%s</td></tr>" % (esc(k), _cell_html(str(k), mapping[k]))
        for k in keys
        if not str(k).startswith("_")
    )
    head = "<h4>%s</h4>" % esc(title) if title else ""
    return '%s<table class="tp-table">%s</table>' % (head, rows)


def rows_table(rows: Sequence[Dict[str, Any]], *, columns: Optional[Sequence[str]] = None,
               title: Optional[str] = None, max_rows: int = 200, max_columns: int = 12) -> str:
    rows = [r for r in rows if isinstance(r, dict)]
    head = "<h4>%s</h4>" % esc(title) if title else ""
    if not rows:
        return head + '<div class="tp-muted">(không có dòng nào)</div>'
    if columns is None:
        cols: List[str] = []
        for r in rows:
            for k in r:
                if k not in cols and not str(k).startswith("_"):
                    cols.append(k)
        columns = cols[:max_columns]
    header = "".join("<th>%s</th>" % esc(c) for c in columns)
    body = "".join(
        "<tr>%s</tr>" % "".join("<td>%s</td>" % _cell_html(str(c), r.get(c)) for c in columns) for r in rows[:max_rows]
    )
    more = '<div class="tp-muted">… còn %d dòng</div>' % (len(rows) - max_rows) if len(rows) > max_rows else ""
    return '%s<table class="tp-table"><tr>%s</tr>%s</table>%s' % (head, header, body, more)


def json_details(obj: Any, summary: str = "JSON đầy đủ", max_chars: int = 20000) -> str:
    text = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n… (cắt bớt)"
    return "<details><summary>%s</summary><pre>%s</pre></details>" % (esc(summary), esc(text))


def render_cli_result(doc: Any, title: Optional[str] = None, preferred: Sequence[str] = ()) -> str:
    if not isinstance(doc, dict):
        return _note("warn", "Không có kết quả JSON")
    ok = doc.get("ok")
    head_badge = status_badge("OK" if ok is True else ("FAIL" if ok is False else "?"))
    parts = ["<h4>%s %s</h4>" % (head_badge, esc(title or doc.get("action") or "Kết quả"))]
    err = doc.get("error")
    if isinstance(err, dict) and err:
        body = "Mã: <code>%s</code><br>%s" % (esc(err.get("code")), _multiline(err.get("message")))
        if err.get("hint"):
            body += "<br>Gợi ý: %s" % _multiline(err.get("hint"))
        if err.get("log_excerpt"):
            body += "<pre>%s</pre>" % esc(str(err.get("log_excerpt"))[-4000:])
        parts.append(_note("err", "Lỗi", body))
    scalars = {k: v for k, v in doc.items() if k not in ("error",) and not isinstance(v, (dict, list))}
    parts.append(kv_table(scalars, order=list(preferred) + ["ok", "action"]))
    for key, value in doc.items():
        if str(key).startswith("_") or key == "error":
            continue
        if isinstance(value, dict) and value:
            if all(isinstance(v, dict) for v in value.values()):
                rows = []
                for name, sub in value.items():
                    row = {"name": name}
                    row.update(sub)
                    rows.append(row)
                parts.append(rows_table(rows, title=str(key)))
            else:
                parts.append(kv_table(value, title=str(key)))
        elif isinstance(value, list) and value:
            if all(isinstance(v, dict) for v in value):
                parts.append(rows_table(value, title=str(key)))
            else:
                parts.append("<h4>%s</h4><div>%s</div>" % (esc(key), fmt_value(value)))
    parts.append(json_details({k: v for k, v in doc.items() if not str(k).startswith("_")}))
    return "".join(parts)


def show_html(fragment: str) -> None:
    if display is not None and HTML is not None:
        display(HTML(_wrap(fragment)))
    else:
        print(_strip_tags(fragment))


def show_cli_result(doc: Any, title: Optional[str] = None, preferred: Sequence[str] = ()) -> None:
    show_html(render_cli_result(doc, title, preferred))


def show_bootstrap_report(report: Any) -> None:
    if not isinstance(report, dict):
        show_html(_note("warn", "Chưa có bootstrap_report.json", esc("Bootstrap chưa ghi báo cáo (xem log phía trên).")))
        return
    show_cli_result(report, "Bootstrap report", ("overall", "lock_origin", "profile"))


def show_kv(mapping: Dict[str, Any], title: Optional[str] = None) -> None:
    show_html(kv_table(mapping, title=title))


def show_api_probe(pairs: Sequence[Tuple[str, "ApiResult"]]) -> None:
    rows = []
    for label, res in pairs:
        rows.append({
            "request": label,
            "status": res.kind.upper(),
            "http": res.http_status,
            "detail": res.error_code or (res.data.get("status") if isinstance(res.data, dict) else None),
            "request_id": res.request_id,
        })
    show_html(rows_table(rows, columns=["request", "status", "http", "detail", "request_id"], title="Kiểm tra API"))


# ================================================================================================
# HTTP client (urllib, timeout) and result classification
# ================================================================================================
class ApiResult(object):
    """Outcome of one API call. ``kind`` ∈ ok | empty | stale | expired | unavailable | not_found |
    invalid | unreachable. ``data`` is never replaced by zeros when the call fails."""

    __slots__ = ("kind", "http_status", "data", "meta", "warnings", "error", "path", "elapsed_ms")

    def __init__(self, kind: str, http_status: Optional[int] = None, data: Any = None,
                 meta: Optional[dict] = None, warnings: Optional[list] = None, error: Optional[dict] = None,
                 path: str = "", elapsed_ms: Optional[float] = None) -> None:
        self.kind = kind
        self.http_status = http_status
        self.data = data
        self.meta = meta if isinstance(meta, dict) else {}
        self.warnings = [w for w in (warnings or []) if isinstance(w, dict)]
        self.error = error if isinstance(error, dict) else None
        self.path = path
        self.elapsed_ms = elapsed_ms

    def __repr__(self) -> str:
        return "ApiResult(kind=%r, http_status=%r, path=%r)" % (self.kind, self.http_status, self.path)

    @property
    def usable(self) -> bool:
        return self.kind in USABLE_KINDS

    def feed(self, feed_type: str) -> Dict[str, Any]:
        feeds = self.meta.get("feeds")
        block = feeds.get(feed_type) if isinstance(feeds, dict) else None
        return dict(block) if isinstance(block, dict) else {}

    @property
    def source_type(self) -> Optional[str]:
        value = self.meta.get("source_type")
        if not value and isinstance(self.data, dict):
            value = self.data.get("source_type")
        return str(value).lower() if value else None

    @property
    def stale(self) -> bool:
        feeds = self.meta.get("feeds")
        if isinstance(feeds, dict):
            for block in feeds.values():
                if isinstance(block, dict) and (block.get("stale") is True or block.get("state") == "stale"):
                    return True
        return any(w.get("code") == "FEED_STALE" for w in self.warnings)

    @property
    def reason(self) -> Optional[str]:
        if isinstance(self.data, dict):
            reason = self.data.get("reason")
            if reason:
                detail = self.data.get("reason_detail")
                return "%s — %s" % (reason, detail) if detail else str(reason)
        for w in self.warnings:
            if w.get("code") in ("EMPTY_SNAPSHOT", "NO_PREDICTION_DATA"):
                return "%s — %s" % (w.get("code"), w.get("message") or "")
        return None

    @property
    def error_code(self) -> Optional[str]:
        return str(self.error.get("code")) if self.error and self.error.get("code") else None

    @property
    def error_message(self) -> Optional[str]:
        return str(self.error.get("message")) if self.error and self.error.get("message") else None

    @property
    def request_id(self) -> Optional[str]:
        if self.error and self.error.get("request_id"):
            return str(self.error.get("request_id"))
        value = self.meta.get("request_id")
        return str(value) if value else None


def classify_response(status: int, body: bytes, *, path: str = "", primary_feed: Optional[str] = None,
                      list_key: Optional[str] = None, elapsed_ms: Optional[float] = None) -> ApiResult:
    """Map an HTTP response onto an ``ApiResult`` kind (pure function, used by tests too)."""
    try:
        doc = json.loads(body.decode("utf-8")) if body else None
    except (ValueError, UnicodeDecodeError):
        doc = None
    if status < 200 or status >= 300:
        err = doc.get("error") if isinstance(doc, dict) and isinstance(doc.get("error"), dict) else None
        if err is None:
            snippet = body[:300].decode("utf-8", "replace") if body else ""
            err = {"code": "http_%d" % status, "message": snippet}
        meta = doc.get("meta") if isinstance(doc, dict) else None
        code = str(err.get("code") or "")
        if status == 404:
            kind = "not_found"
        elif status in (400, 422):
            kind = "invalid"
        elif status == 503 and code == "generation_expired":
            kind = "expired"
        else:
            kind = "unavailable"
        return ApiResult(kind, status, None, meta, None, err, path, elapsed_ms)
    if not isinstance(doc, dict) or "data" not in doc:
        err = {"code": "bad_response", "message": "Phản hồi không có dạng {data, meta, warnings}."}
        return ApiResult("unavailable", status, None, None, None, err, path, elapsed_ms)
    data = doc.get("data")
    meta = doc.get("meta") if isinstance(doc.get("meta"), dict) else {}
    result = ApiResult("ok", status, data, meta, doc.get("warnings") or [], None, path, elapsed_ms)
    rows = None
    if isinstance(data, list):
        rows = data
    elif list_key and isinstance(data, dict) and isinstance(data.get(list_key), list):
        rows = data.get(list_key)
    feed_state = result.feed(primary_feed).get("state") if primary_feed else None
    if (rows is not None and len(rows) == 0) or feed_state == "empty":
        result.kind = "empty"
    elif primary_feed:
        block = result.feed(primary_feed)
        if block.get("stale") is True or feed_state == "stale" or any(
            w.get("code") == "FEED_STALE" and w.get("feed_type") in (None, primary_feed) for w in result.warnings
        ):
            result.kind = "stale"
    elif result.stale:
        result.kind = "stale"
    return result


class ApiClient(object):
    """Minimal JSON client for the TransitPulse API (HTTP GET only, no proxies, bounded timeout)."""

    def __init__(self, base_url: Optional[str] = None, timeout: float = 5.0) -> None:
        self.base_url = str(base_url or DEFAULT_API_BASE).rstrip("/")
        self.timeout = float(timeout)
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def url(self, path: str, params: Optional[Dict[str, Any]] = None) -> str:
        query = {k: v for k, v in (params or {}).items() if v is not None and v != ""}
        full = self.base_url + "/" + path.lstrip("/")
        if query:
            full += "?" + urllib.parse.urlencode(query, doseq=True)
        return full

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, *, primary_feed: Optional[str] = None,
            list_key: Optional[str] = None) -> ApiResult:
        url = self.url(path, params)
        request = urllib.request.Request(
            url, headers={"Accept": "application/json", "X-Request-ID": "nb-" + uuid.uuid4().hex[:12]}, method="GET"
        )
        started = time.monotonic()
        try:
            with self._opener.open(request, timeout=self.timeout) as resp:
                status = int(resp.getcode())
                body = resp.read()
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            try:
                body = exc.read()
            except Exception:  # noqa: BLE001
                body = b""
        except (urllib.error.URLError, OSError, ValueError, http.client.HTTPException) as exc:
            reason = getattr(exc, "reason", None) or exc
            return ApiResult(
                "unreachable", None, None, None, None,
                {"code": "api_unreachable", "message": "%s: %s" % (type(exc).__name__, reason)},
                path, (time.monotonic() - started) * 1000.0,
            )
        elapsed = (time.monotonic() - started) * 1000.0
        return classify_response(status, body, path=path, primary_feed=primary_feed, list_key=list_key,
                                 elapsed_ms=elapsed)


# ================================================================================================
# Pure HTML renderers (escaped). Each takes ApiResult objects (or None = not requested).
# ================================================================================================
def state_panel(res: Optional[ApiResult], what: str, base_url: Optional[str] = None) -> str:
    """Dedicated panel for every non-OK outcome; also handles ``None`` (not requested yet)."""
    if res is None:
        return _note("info", "Chưa có yêu cầu", esc("Chọn run/tuyến/trạm để tải %s." % what))
    code = res.error_code or ""
    message = res.error_message or ""
    extra = []
    if res.request_id:
        extra.append("request_id: <code>%s</code>" % esc(res.request_id))
    if res.error and res.error.get("retryable") is not None:
        extra.append("có thể thử lại: %s" % fmt_value(bool(res.error.get("retryable"))))
    if res.error and res.error.get("hint"):
        extra.append("gợi ý: %s" % esc(res.error.get("hint")))
    tail = ("<br><span class='tp-muted'>%s</span>" % " · ".join(extra)) if extra else ""
    if res.kind == "unreachable":
        body = esc("Không gọi được API%s khi tải %s. API chưa chạy hoặc đã dừng — kiểm tra cell 12–13 "
                   "(demo.py status). Chi tiết: %s" % ((" tại " + base_url) if base_url else "", what, message))
        return _note("err", "Không kết nối được API", body + tail)
    if res.kind == "expired":
        body = esc("Generation hiện tại của %s đã hết hạn (expires_at ≤ now). Không hiển thị số cũ; chờ snapshot "
                   "mới hoặc kiểm tra collector/publisher." % what)
        return _note("err", "Dữ liệu đã hết hạn (503 generation_expired)", body + tail)
    if res.kind == "unavailable":
        explain = {
            "generation_unavailable": "Chưa có generation READY — pipeline đang chờ/đang xử lý snapshot đầu tiên.",
            "generation_inconsistent": "Generation thiếu dòng so với manifest — không hiển thị (không coi là 0 xe).",
            "database_unavailable": "Cassandra không truy cập được.",
            "database_timeout": "Truy vấn Cassandra quá thời gian.",
            "run_clock_unavailable": "Run chưa có đồng hồ ứng dụng (chưa nhận clock tick đầu tiên).",
            "bad_response": "Phản hồi API không đúng định dạng.",
        }.get(code, "API trả lỗi %s." % (res.http_status if res.http_status else "?"))
        body = esc("%s (%s) %s" % (explain, code or "không có mã", message))
        return _note("err", "Không khả dụng (%s)" % (res.http_status or "?"), body + tail)
    if res.kind == "not_found":
        return _note("warn", "Không tìm thấy (404)", esc("%s: %s" % (code, message)) + tail)
    if res.kind == "invalid":
        return _note("warn", "Yêu cầu không hợp lệ (%s)" % (res.http_status or 422), esc("%s: %s" % (code, message)) + tail)
    if res.kind == "empty":
        reason = res.reason or "không có lý do kèm theo"
        return _note("info", "Không có dữ liệu",
                     esc("Phản hồi hợp lệ nhưng rỗng cho %s (không phải lỗi, không phải số 0 mặc định). Lý do: %s"
                         % (what, reason)) + tail)
    return ""


def _source_of(*items: Any) -> Optional[str]:
    for item in items:
        if isinstance(item, ApiResult) and item.source_type:
            return item.source_type
        if isinstance(item, dict):
            value = item.get("source_type") or item.get("mode")
            if value:
                return str(value).lower()
    return None


def render_banner(run_res: Optional[ApiResult] = None, session: Optional[Dict[str, Any]] = None, *,
                  loading: bool = False, api_base: Optional[str] = None) -> str:
    """Always-visible banner: source (LIVE/REPLAY/SYNTHETIC), run, state, clocks, GTFS version."""
    session = session or {}
    meta = run_res.meta if run_res is not None else {}
    data = run_res.data if run_res is not None and isinstance(run_res.data, dict) else {}
    source = _source_of(run_res, data, session)
    label, fg, bg = SOURCE_STYLE.get(source or "", ("NGUỒN CHƯA RÕ", "#ffffff", _GREY))
    run_id = meta.get("run_id") or data.get("run_id") or session.get("run_id")
    run_state = meta.get("run_state") or data.get("run_state")
    app_clock = meta.get("application_clock") or data.get("application_clock")
    served_at = meta.get("served_at")
    gtfs = meta.get("gtfs_version") or data.get("gtfs_version") or (session.get("scope") or {}).get("gtfs_version")
    scenario = meta.get("scenario_id") or data.get("scenario_id")
    items = [badge(label, fg, bg)]
    if source == "synthetic":
        text = "<b>%s</b>" % esc(SYNTHETIC_NOTICE)
        if scenario:
            text += " · kịch bản <code>%s</code>" % esc(scenario)
        items.append(text)
    items.append("Run %s" % short_id(run_id, 8))
    items.append("Trạng thái %s" % status_badge(run_state))
    if source in ("replay", "synthetic"):
        items.append("Đồng hồ lịch sử: <b>%s</b>" % fmt_ts(app_clock))
    else:
        items.append("Đồng hồ ứng dụng (wall): <b>%s</b>" % fmt_ts(app_clock))
    items.append("Phục vụ lúc (wall): %s" % fmt_ts(served_at))
    if meta.get("clock_sequence") is not None:
        items.append("clock_sequence %s" % fmt_value(meta.get("clock_sequence")))
    items.append("GTFS %s" % short_id(gtfs, 12))
    if session.get("session_id"):
        items.append("Phiên %s" % short_id(session.get("session_id"), 16))
    if session.get("deadline_at"):
        items.append("Hạn chạy %s" % fmt_ts(session.get("deadline_at")))
    if loading:
        items.append('<span class="tp-loading">Đang tải…</span>')
    out = '<div class="tp-banner">%s</div>' % "".join("<span>%s</span>" % i for i in items)
    if run_res is not None and not run_res.usable:
        out += state_panel(run_res, "trạng thái run", api_base)
    return out


def feed_health_table(meta: Dict[str, Any]) -> str:
    feeds = meta.get("feeds") if isinstance(meta, dict) else None
    if not isinstance(feeds, dict) or not feeds:
        return '<div class="tp-muted">API không trả thông tin feed cho yêu cầu này.</div>'
    rows = []
    for feed in list(FEEDS) + [f for f in feeds if f not in FEEDS]:
        block = feeds.get(feed)
        if not isinstance(block, dict):
            continue
        state = block.get("state")
        flags = status_badge(state)
        if block.get("stale") is True and state != "stale":
            flags += " " + status_badge("STALE")
        if block.get("is_partial") is True:
            flags += " " + status_badge("PARTIAL")
        rows.append(
            "<tr><td>%s</td><td>%s<br><span class='tp-muted'>%s</span></td><td>%s</td><td>%s</td><td>%s</td>"
            "<td>%s</td><td>%s</td><td>%s</td></tr>"
            % (
                esc(FEED_LABELS.get(feed, feed)), flags, esc(FEED_STATE_TEXT.get(str(state), "")),
                short_id(block.get("snapshot_id"), 14), fmt_ts(block.get("feed_timestamp")),
                fmt_ts(block.get("metrics_as_of")), fmt_seconds(block.get("source_age_seconds")),
                fmt_value(block.get("stale_threshold_seconds"), "s"), fmt_ts(block.get("expires_at")),
            )
        )
    header = ("<tr><th>Feed</th><th>Trạng thái</th><th>snapshot_id</th><th>feed_timestamp</th><th>metrics_as_of</th>"
              "<th>Tuổi nguồn</th><th>Ngưỡng stale</th><th>Hết hạn lúc</th></tr>")
    return '<table class="tp-table">%s%s</table>' % (header, "".join(rows))


def warnings_list(warning_items: Iterable[Dict[str, Any]], title: str = "Cảnh báo chất lượng pipeline") -> str:
    seen = set()
    rows = []
    for w in warning_items:
        if not isinstance(w, dict):
            continue
        key = (w.get("code"), w.get("feed_type"), w.get("message"))
        if key in seen:
            continue
        seen.add(key)
        rows.append("<li>%s %s%s — %s</li>" % (
            status_badge(w.get("category") or "INFO"), "<code>%s</code>" % esc(w.get("code")),
            (" (%s)" % esc(w.get("feed_type"))) if w.get("feed_type") else "", esc(w.get("message")),
        ))
    if not rows:
        return '<h4>%s</h4><div class="tp-muted">Không có cảnh báo pipeline trong các phản hồi lần này.</div>' % esc(title)
    return "<h4>%s</h4><ul>%s</ul>" % (esc(title), "".join(rows))


_VEHICLE_COUNTS = (
    ("Xe có vị trí mới (fresh)", ("fresh_vehicle_count", "fresh"), "fresh"),
    ("Vị trí cũ (stale)", ("stale_vehicle_count", "stale"), "stale"),
    ("Không rõ tuổi", ("unknown_age_vehicle_count", "unknown_age", "unknown"), "unknown"),
    ("Không hợp lệ (invalid)", ("invalid_vehicle_count", "invalid"), "invalid"),
    ("Có mặt trong snapshot", ("snapshot_membership_count", "snapshot_membership"), None),
)


def _count_from(block: Any, keys: Sequence[str]) -> Any:
    if not isinstance(block, dict):
        return None
    for source in (block.get("counts"), block):
        if isinstance(source, dict):
            for key in keys:
                value = source.get(key)
                if _is_num(value):
                    return value
    return None


def vehicle_stats(block: Any) -> str:
    cells = []
    for label, keys, cls in _VEHICLE_COUNTS:
        value = _count_from(block, keys)
        color = FRESHNESS_COLORS.get(cls or "", "#1f2933")
        cells.append('<div class="tp-stat"><span class="tp-muted">%s</span><b style="color:%s">%s</b></div>'
                     % (esc(label), color, fmt_value(value)))
    return '<div class="tp-grid">%s</div>' % "".join(cells)


def kpi_late_text(pm: Dict[str, Any]) -> Tuple[str, bool]:
    """KPI card wording. The rate/count come from Spark via the API; nothing is computed here."""
    eligible = pm.get("eligible_trip_count")
    if not _is_num(eligible) or int(eligible) == 0:
        return NO_ELIGIBLE_TEXT, False
    late = pm.get("predicted_late_trip_count")
    threshold = pm.get("late_threshold_seconds")
    threshold_text = ("%s phút" % _num_text(float(threshold) / 60.0, 1)) if _is_num(threshold) and threshold > 0 \
        else "ngưỡng trễ của policy"
    rate = pm.get("predicted_late_rate")
    rate_text = (" (%s%%)" % _num_text(float(rate) * 100.0, 1)) if _is_num(rate) else ""
    late_text = str(int(late)) if _is_num(late) else DASH
    return ("Độ trễ dự kiến ≥ %s: %s trên %s chuyến đủ điều kiện%s"
            % (threshold_text, late_text, int(eligible), rate_text)), True


def kpi_late_card(pm: Dict[str, Any]) -> str:
    text, has_sample = kpi_late_text(pm)
    flags = ""
    if pm.get("stale") is True:
        flags += " " + status_badge("STALE")
    rows = {
        "Độ trễ dự kiến trung bình (chuyến đủ điều kiện)": fmt_delay(pm.get("mean_predicted_delay_seconds")),
        "Số chuyến bị loại khỏi KPI": fmt_value(pm.get("excluded_trip_count")),
        "Không rõ độ bất định (vẫn đủ điều kiện, đếm riêng)": fmt_value(pm.get("unknown_uncertainty_count")),
        "Số trip instance trong snapshot": fmt_value(pm.get("trip_instance_count")),
        "Chính sách chọn stop mục tiêu": fmt_value(pm.get("target_policy")),
        "policy_version": fmt_value(pm.get("policy_version")),
        "metrics_as_of": fmt_ts(pm.get("metrics_as_of")),
        "Tuổi feed TU": fmt_seconds(pm.get("feed_age_seconds")),
    }
    table = '<table class="tp-table">%s</table>' % "".join(
        "<tr><th>%s</th><td>%s</td></tr>" % (esc(k), v) for k, v in rows.items()
    )
    excluded = pm.get("excluded_by_reason")
    if isinstance(excluded, dict) and excluded:
        table += rows_table([{"reason": k, "count": v} for k, v in sorted(excluded.items())],
                            columns=["reason", "count"], title="Lý do loại khỏi KPI")
    note = ("Mẫu số là số chuyến (trip instance) đủ điều kiện; tỷ lệ do Spark tính và API trả về — notebook chỉ "
            "hiển thị." if has_sample else "Mẫu số bằng 0 hoặc chưa biết nên không hiển thị phần trăm.")
    return ('<div class="tp-card"><div class="tp-kpi">%s%s</div><div class="tp-muted">%s</div>%s</div>'
            % (esc(text), flags, esc(note), table))


def _feed_block_missing(res: ApiResult, feed: str, what: str) -> str:
    block = res.feed(feed)
    state = block.get("state")
    detail = FEED_STATE_TEXT.get(str(state), "không có dữ liệu")
    level = "err" if state in ("expired", "inconsistent") else "info"
    return _note(level, "%s không khả dụng" % what,
                 esc("Feed %s: %s (%s)." % (FEED_LABELS.get(feed, feed), state or "không rõ", detail)))


def _scope_note(run_res: Optional[ApiResult], session: Dict[str, Any], source: Optional[str],
                route_label: Optional[str]) -> str:
    data = run_res.data if run_res is not None and isinstance(run_res.data, dict) else {}
    scope = session.get("scope") if isinstance(session.get("scope"), dict) else {}
    route_ids = data.get("route_ids") or scope.get("route_ids") or []
    text = "Phạm vi run: %d tuyến (%s)" % (len(route_ids), ", ".join(str(r) for r in route_ids) or DASH)
    if source == "synthetic":
        text += " — tuyến synthetic, không phải tuyến MBTA đã xác minh."
    elif scope.get("verified_mbta") is True:
        text += " — đã xác minh với static/realtime MBTA."
    if route_label:
        text += " · Đang xem: %s" % route_label
    return '<div class="tp-muted">%s</div>' % esc(text)


def render_overview(metrics_res: Optional[ApiResult], run_res: Optional[ApiResult] = None, *,
                    route_label: Optional[str] = None, session: Optional[Dict[str, Any]] = None,
                    base_url: Optional[str] = None) -> str:
    session = session or {}
    source = _source_of(metrics_res, run_res, session)
    parts = [_scope_note(run_res, session, source, route_label)]
    if metrics_res is None:
        parts.append(state_panel(None, "chỉ số tuyến"))
        return _wrap("".join(parts))
    if not metrics_res.usable:
        parts.append(state_panel(metrics_res, "chỉ số tuyến", base_url))
        parts.append("<h4>Tình trạng feed</h4>" + feed_health_table(metrics_res.meta))
        return _wrap("".join(parts))
    data = metrics_res.data if isinstance(metrics_res.data, dict) else {}
    vm = data.get("vehicle_metrics")
    pm = data.get("prediction_metrics")
    parts.append("<h4>Xe trên tuyến</h4>")
    if isinstance(vm, dict):
        parts.append(vehicle_stats(vm))
        stale = " " + status_badge("STALE") if vm.get("stale") is True else ""
        complete = "" if vm.get("counts_complete") is not False else " · số đếm CHƯA ĐẦY ĐỦ (bị giới hạn số dòng)"
        parts.append('<div class="tp-muted">"Xe có vị trí mới" là số xe có quan sát fresh trong snapshot hiện tại, '
                     "không phải toàn bộ đội xe MBTA đang hoạt động. Đánh giá lúc %s (đồng hồ ứng dụng); "
                     "metrics_as_of %s%s%s</div>"
                     % (fmt_ts(vm.get("evaluated_at")), fmt_ts(vm.get("metrics_as_of")), esc(complete), stale))
    else:
        parts.append(_feed_block_missing(metrics_res, "vehicle_positions", "Chỉ số xe"))
    parts.append("<h4>Dự báo trễ (KPI do Spark tính)</h4>")
    if isinstance(pm, dict):
        parts.append(kpi_late_card(pm))
    else:
        parts.append(_feed_block_missing(metrics_res, "trip_updates", "KPI dự báo trễ"))
    parts.append("<h4>Tình trạng feed</h4>" + feed_health_table(metrics_res.meta))
    coverage = metrics_res.meta.get("coverage")
    if isinstance(coverage, dict) and coverage.get("truncated"):
        parts.append(_note("warn", "Kết quả bị cắt bớt", esc("Coverage: %s" % json.dumps(coverage, ensure_ascii=False))))
    parts.append(warnings_list(metrics_res.warnings))
    return _wrap("".join(parts))


def _vehicle_class(v: Dict[str, Any]) -> str:
    status = str(v.get("freshness_status") or v.get("freshness") or "unknown").lower()
    return status if status in FRESHNESS_COLORS else "unknown"


def _vehicle_tooltip(v: Dict[str, Any], synthetic: bool) -> str:
    lines = []
    if synthetic:
        lines.append(SYNTHETIC_NOTICE)
    vid = _first(v, "vehicle_id")
    label = _first(v, "label", "vehicle_label")
    lines.append("Xe %s%s" % (vid if vid is not None else DASH, (" (%s)" % label) if label else ""))
    lines.append("Chuyến: %s" % (_first(v, "trip_id") or DASH))
    lines.append("Quan sát lúc: %s" % (_first(v, "observation_time") or "không có timestamp"))
    age = v.get("observation_age_seconds")
    lines.append("Tuổi: %s" % (("%s s" % _num_text(float(age), 1)) if _is_num(age) else DASH))
    lines.append("Trạng thái: %s" % FRESHNESS_LABELS.get(_vehicle_class(v), DASH))
    flags = v.get("quality_flags") or []
    lines.append("Cờ chất lượng: %s" % (", ".join(str(f) for f in flags) if flags else "không có"))
    return "\n".join(lines)


def _finite_lonlat(lon: Any, lat: Any) -> Optional[Tuple[float, float]]:
    if not (_is_num(lon) and _is_num(lat)):
        return None
    lon_f, lat_f = float(lon), float(lat)
    if not (-180.0 <= lon_f <= 180.0 and -90.0 <= lat_f <= 90.0):
        return None
    return lon_f, lat_f


def render_svg_map(shapes: Sequence[Dict[str, Any]], stops: Sequence[Dict[str, Any]],
                   vehicles: Sequence[Dict[str, Any]], *, synthetic: bool = False,
                   width: int = 800, height: int = 500, pad: int = 28) -> str:
    """Self-contained SVG schematic (equirectangular with cos(lat0)); no tiles, no external assets."""
    paths: List[Tuple[Dict[str, Any], List[Tuple[float, float]]]] = []
    for shape in shapes:
        pts = []
        for p in shape.get("path") or shape.get("points") or []:
            if isinstance(p, (list, tuple)) and len(p) >= 2:
                ll = _finite_lonlat(p[0], p[1])
                if ll is not None:
                    pts.append(ll)
        if len(pts) >= 2:
            paths.append((shape, pts))
    stop_pts = []
    for s in stops:
        ll = _finite_lonlat(_num_field(s, "longitude", "stop_lon", "lon"), _num_field(s, "latitude", "stop_lat", "lat"))
        if ll is not None:
            stop_pts.append((s, ll))
    veh_pts = []
    for v in vehicles:
        cls = _vehicle_class(v)
        if cls == "invalid":
            continue  # invalid positions are listed in the table, never plotted
        ll = _finite_lonlat(_num_field(v, "longitude", "lon"), _num_field(v, "latitude", "lat"))
        if ll is not None:
            veh_pts.append((v, ll, cls))
    all_pts = [ll for _s, pts in paths for ll in pts] + [ll for _s, ll in stop_pts] + [ll for _v, ll, _c in veh_pts]
    if not all_pts:
        return _note("info", MAP_LABEL, esc("Không có tọa độ hợp lệ để vẽ (chưa có shape/trạm/xe)."))
    min_lon = min(p[0] for p in all_pts)
    max_lon = max(p[0] for p in all_pts)
    min_lat = min(p[1] for p in all_pts)
    max_lat = max(p[1] for p in all_pts)
    k = max(math.cos(math.radians((min_lat + max_lat) / 2.0)), 0.01)
    span_x = (max_lon - min_lon) * k
    span_y = max_lat - min_lat
    scale_x = (width - 2 * pad) / span_x if span_x > 0 else float("inf")
    scale_y = (height - 2 * pad) / span_y if span_y > 0 else float("inf")
    scale = min(scale_x, scale_y)
    if not math.isfinite(scale):
        scale = 1.0
    off_x = (width - span_x * scale) / 2.0
    off_y = (height - span_y * scale) / 2.0

    def xy(ll: Tuple[float, float]) -> Tuple[float, float]:
        return off_x + (ll[0] - min_lon) * k * scale, off_y + (max_lat - ll[1]) * scale

    out = ['<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="100%%" '
           'style="max-width:%dpx;background:#fbfcfd;border:1px solid #d0d7de;border-radius:6px" role="img" '
           'aria-label="%s">' % (width, height, width, esc(MAP_LABEL))]
    direction_colors = {0: "#4a6fa5", 1: "#8a5aa8"}
    for shape, pts in paths:
        dirs = shape.get("direction_ids") or ([shape.get("direction_id")] if shape.get("direction_id") is not None else [])
        color = direction_colors.get(dirs[0] if len(dirs) == 1 else -1, "#5b6b7f")
        coords = " ".join("%.1f,%.1f" % xy(ll) for ll in pts)
        tip = "Shape %s · hướng %s · %d điểm" % (shape.get("shape_id"), ",".join(str(d) for d in dirs) or DASH, len(pts))
        out.append('<polyline points="%s" fill="none" stroke="%s" stroke-width="2.5" stroke-opacity="0.85" '
                   'stroke-linejoin="round"><title>%s</title></polyline>' % (coords, color, esc(tip)))
    for stop, ll in stop_pts:
        x, y = xy(ll)
        tip = "Trạm %s (%s)" % (stop.get("stop_name") or DASH, stop.get("stop_id"))
        out.append('<circle cx="%.1f" cy="%.1f" r="3.5" fill="#ffffff" stroke="#24292f" stroke-width="1.2">'
                   "<title>%s</title></circle>" % (x, y, esc(tip)))
    for v, ll, cls in veh_pts:
        x, y = xy(ll)
        out.append('<circle cx="%.1f" cy="%.1f" r="6.5" fill="%s" stroke="#ffffff" stroke-width="1.5">'
                   "<title>%s</title></circle>" % (x, y, FRESHNESS_COLORS[cls], esc(_vehicle_tooltip(v, synthetic))))
    legend = [("fresh", "Xe fresh"), ("stale", "Xe stale"), ("unknown", "Xe không rõ tuổi")]
    out.append('<rect x="8" y="8" width="150" height="%d" rx="4" fill="#ffffff" fill-opacity="0.9" stroke="#d0d7de"/>'
               % (18 * (len(legend) + 1) + 8))
    for i, (cls, text) in enumerate(legend):
        cy = 24 + 18 * i
        out.append('<circle cx="22" cy="%d" r="5.5" fill="%s"/><text x="34" y="%d" font-size="12" fill="#1f2933">%s</text>'
                   % (cy, FRESHNESS_COLORS[cls], cy + 4, esc(text)))
    cy = 24 + 18 * len(legend)
    out.append('<circle cx="22" cy="%d" r="3.5" fill="#ffffff" stroke="#24292f"/><text x="34" y="%d" font-size="12" '
               'fill="#1f2933">Trạm</text>' % (cy, cy + 4))
    if synthetic:
        out.append('<text x="%d" y="%d" font-size="12" text-anchor="end" fill="#6f42c1">%s</text>'
                   % (width - 10, height - 10, esc(SYNTHETIC_NOTICE)))
    out.append("</svg>")
    caption = ('<div class="tp-muted">%s · phép chiếu equirectangular (hệ số cos vĩ độ trung tâm), Bắc ở trên. '
               "Rê chuột lên xe/trạm/shape để xem chi tiết.</div>" % esc(MAP_LABEL))
    return "".join(out) + caption


def render_map_panel(geom_res: Optional[ApiResult], stops_res: Optional[ApiResult], veh_res: Optional[ApiResult], *,
                     route_label: Optional[str] = None, base_url: Optional[str] = None) -> str:
    source = _source_of(veh_res, geom_res, stops_res)
    synthetic = source == "synthetic"
    shapes = extract_list(geom_res.data, "shapes") if geom_res is not None and geom_res.usable else []
    stops = extract_list(stops_res.data, "stops") if stops_res is not None and stops_res.usable else []
    vehicles = extract_list(veh_res.data, "vehicles") if veh_res is not None and veh_res.usable else []
    parts = []
    if route_label:
        parts.append('<div class="tp-muted">Tuyến: %s</div>' % esc(route_label))
    if synthetic:
        parts.append(_note("info", SYNTHETIC_NOTICE, esc("Tọa độ, shape và xe là fixture synthetic có nhãn, không phải MBTA.")))
    for res, what in ((geom_res, "shape tuyến"), (stops_res, "danh sách trạm"), (veh_res, "vị trí xe")):
        if res is None or not res.usable or res.kind == "empty":
            parts.append(state_panel(res, what, base_url))
        elif res.kind == "stale":
            parts.append(_note("warn", "Dữ liệu %s đã cũ (stale)" % what,
                               esc("Vẫn hiển thị giá trị gần nhất kèm thời điểm; xem bảng feed ở tab Tổng quan.")))
    parts.append(render_svg_map(shapes, stops, vehicles, synthetic=synthetic))
    if veh_res is not None and veh_res.usable:
        data = veh_res.data if isinstance(veh_res.data, dict) else {}
        parts.append("<h4>Số xe (API tính lại tại đồng hồ ứng dụng)</h4>" + vehicle_stats(data))
        if data.get("truncated"):
            parts.append(_note("warn", "Danh sách xe bị cắt bớt", esc("API giới hạn số dòng (truncated=true).")))
        rows = []
        for v in vehicles:
            cls = _vehicle_class(v)
            lat = _num_field(v, "latitude", "lat")
            lon = _num_field(v, "longitude", "lon")
            pos = ("%.5f, %.5f" % (lat, lon)) if lat is not None and lon is not None else DASH
            rows.append(
                "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>"
                "<td>%s</td><td>%s</td></tr>"
                % (
                    esc(v.get("vehicle_id")), esc(_first(v, "label", "vehicle_label")), esc(v.get("trip_id")),
                    badge(FRESHNESS_LABELS.get(cls, cls), "#ffffff", FRESHNESS_COLORS.get(cls, _GREY)),
                    (" " + esc(v.get("invalid_reason")) + " (không vẽ)") if cls == "invalid" else "",
                    fmt_ts(v.get("observation_time")), fmt_seconds(v.get("observation_age_seconds")),
                    esc(v.get("current_status")), esc(pos), fmt_value(v.get("quality_flags")),
                    fmt_value(v.get("inherited")),
                )
            )
        header = ("<tr><th>Xe</th><th>Nhãn</th><th>Chuyến</th><th>Độ mới</th><th>Quan sát lúc</th><th>Tuổi</th>"
                  "<th>Trạng thái dừng</th><th>Tọa độ</th><th>Cờ</th><th>Kế thừa</th></tr>")
        parts.append('<table class="tp-table">%s%s</table>' % (header, "".join(rows)) if rows else "")
    return _wrap("".join(parts))


def render_predictions_panel(pred_res: Optional[ApiResult], *, stop_label: Optional[str] = None,
                             include_invalid: bool = False, base_url: Optional[str] = None) -> str:
    parts = []
    if stop_label:
        parts.append('<div class="tp-muted">Trạm: %s</div>' % esc(stop_label))
    legend = " ".join(badge(t, fg, bg) for t, fg, bg in QUALITY_BADGES.values())
    parts.append('<div class="tp-muted">Nhãn chất lượng: %s</div>' % legend)
    if pred_res is None or not pred_res.usable or pred_res.kind == "empty":
        parts.append(state_panel(pred_res, "dự báo tại trạm", base_url))
        if pred_res is not None and pred_res.usable:
            parts.append("<h4>Tình trạng feed</h4>" + feed_health_table(pred_res.meta))
        return _wrap("".join(parts))
    if pred_res.kind == "stale":
        parts.append(_note("warn", "Dự báo đã cũ (stale)", esc("Feed trip_updates quá ngưỡng stale; giá trị kèm thời điểm.")))
    rows = extract_list(pred_res.data, "predictions")
    hidden = 0
    if not include_invalid:
        kept = [r for r in rows if str(r.get("prediction_quality") or "").lower() != "invalid"]
        hidden = len(rows) - len(kept)
        rows = kept
    body = []
    for r in rows:
        quality = str(r.get("prediction_quality") or "").lower()
        text, fg, bg = QUALITY_BADGES.get(quality, (quality.upper() or DASH, "#ffffff", _GREY))
        event = {"arrival": "Đến", "departure": "Đi"}.get(str(r.get("event_type") or "").lower(), r.get("event_type"))
        body.append(
            "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>"
            "<td>%s</td><td>%s</td><td>%s</td></tr>"
            % (
                fmt_ts(r.get("predicted_time")), fmt_ts(r.get("scheduled_time")), fmt_delay(r.get("predicted_delay_seconds")),
                badge(text, fg, bg), fmt_value(r.get("uncertainty_seconds"), "s"), esc(event), esc(r.get("trip_id")),
                esc(r.get("route_id")), fmt_value(r.get("direction_id")), fmt_value(r.get("stop_sequence")),
                esc(r.get("schedule_resolution")) + ("<br>Tính KPI: " + fmt_value(r.get("kpi_eligible"))
                                                     if r.get("kpi_eligible") is not None else ""),
            )
        )
    header = ("<tr><th>Giờ dự báo</th><th>Giờ theo lịch</th><th>Trễ dự kiến</th><th>Chất lượng</th><th>Độ bất định</th>"
              "<th>Loại</th><th>Chuyến</th><th>Tuyến</th><th>Hướng</th><th>Thứ tự trạm</th><th>Phân giải lịch</th></tr>")
    if body:
        parts.append('<table class="tp-table">%s%s</table>' % (header, "".join(body)))
    else:
        parts.append(_note("info", "Không có dự báo hiển thị được", esc("Tất cả dự báo trả về đều INVALID (đang ẩn).")))
    if hidden:
        parts.append('<div class="tp-muted">Đã ẩn %d dự báo INVALID (bật "Hiện cả dự báo INVALID" để xem).</div>' % hidden)
    parts.append('<div class="tp-muted">"Trễ dự kiến" chỉ có khi lịch được phân giải; "—" nghĩa là không tính được, '
                 "không phải 0. Giờ theo UTC.</div>")
    parts.append("<h4>Tình trạng feed</h4>" + feed_health_table(pred_res.meta))
    return _wrap("".join(parts))


def _period_text(period: Any) -> str:
    if not isinstance(period, dict):
        return esc(period)
    start = fmt_ts(period.get("start")) if period.get("start") else "không giới hạn"
    end = fmt_ts(period.get("end")) if period.get("end") else "không giới hạn"
    return "%s → %s" % (start, end)


def _alert_card(a: Dict[str, Any], synthetic: bool) -> str:
    state = a.get("active_state_at_clock")
    header = _first(a, "header_text") or "(không có tiêu đề)"
    meta_line = "Hiệu ứng %s · nguyên nhân %s · mức độ %s" % (
        esc(a.get("effect")), esc(a.get("cause")), esc(_first(a, "severity_level", "severity")))
    periods = a.get("active_periods") or []
    period_html = "<br>".join(_period_text(p) for p in periods) if periods else "Không có active_period (luôn áp dụng)"
    selectors = a.get("informed_entities") if a.get("informed_entities") is not None else a.get("selectors")
    selector_html = esc(json.dumps(selectors, ensure_ascii=False, default=str)[:600]) if selectors else DASH
    lineage = []
    if a.get("inherited"):
        lineage.append("kế thừa từ %s" % short_id(a.get("inherited_from_snapshot_id"), 14))
    if a.get("confirmation_status"):
        lineage.append(status_badge(a.get("confirmation_status")))
    url = a.get("url")
    url_html = ""
    if isinstance(url, str) and url.startswith("https://"):
        url_html = '<br><a href="%s" target="_blank" rel="noopener noreferrer">%s</a>' % (esc(url), esc(url))
    prefix = badge("DỮ LIỆU KIỂM THỬ", "#ffffff", "#6f42c1") + " " if synthetic else ""
    return (
        '<div class="tp-card">%s%s <b>%s</b><div class="tp-muted">%s · tuyến %s · <code>%s</code></div>'
        '<div style="margin-top:4px">%s</div><table class="tp-table"><tr><th>Thời hạn</th><td>%s</td></tr>'
        "<tr><th>Selector đã resolve</th><td>%s<br><span class='tp-muted'>match: %s · resolution: %s</span></td></tr>"
        "<tr><th>Lineage</th><td>%s</td></tr></table>%s</div>"
        % (
            prefix, status_badge(state), esc(header), meta_line, fmt_value(a.get("route_ids")), esc(a.get("alert_id")),
            _multiline(a.get("description_text")), period_html, selector_html, fmt_value(a.get("match_kinds")),
            esc(a.get("selector_resolution")), " ".join(lineage) or DASH, url_html,
        )
    )


def render_alerts_panel(alerts_res: Optional[ApiResult], pipeline_warnings: Iterable[Dict[str, Any]] = (), *,
                        base_url: Optional[str] = None) -> str:
    synthetic = _source_of(alerts_res) == "synthetic"
    parts = ["<h4>Cảnh báo dịch vụ %s</h4>" % ("(dữ liệu kiểm thử)" if synthetic else "MBTA")]
    if alerts_res is None or not alerts_res.usable or alerts_res.kind == "empty":
        parts.append(state_panel(alerts_res, "cảnh báo dịch vụ", base_url))
    else:
        if alerts_res.kind == "stale":
            parts.append(_note("warn", "Feed alerts đã cũ (stale)", esc("Giữ giá trị gần nhất kèm thời điểm.")))
        for a in extract_list(alerts_res.data, "alerts"):
            parts.append(_alert_card(a, synthetic))
    parts.append(warnings_list(pipeline_warnings))
    parts.append('<div class="tp-muted">Cảnh báo dịch vụ là dữ liệu nguồn; cảnh báo chất lượng pipeline chỉ đến từ '
                 "trường warnings của API và được tách riêng.</div>")
    return _wrap("".join(parts))


def _components(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    comps = data.get("components")
    if comps is None:
        comps = data.get("source_health")
    if isinstance(comps, dict):
        rows = []
        for name, comp in comps.items():
            if isinstance(comp, dict):
                row = {"component": name}
                row.update(comp)
                rows.append(row)
        return rows
    return [c for c in (comps or []) if isinstance(c, dict)]


def render_quality_panel(quality_res: Optional[ApiResult], pipeline_res: Optional[ApiResult],
                         health_res: Optional[ApiResult] = None, *, base_url: Optional[str] = None) -> str:
    parts = ["<h4>Chất lượng dữ liệu theo feed</h4>"]
    if quality_res is None or not quality_res.usable:
        parts.append(state_panel(quality_res, "báo cáo chất lượng (/quality)", base_url))
    else:
        data = quality_res.data if isinstance(quality_res.data, dict) else {}
        feeds = data.get("feeds") if isinstance(data.get("feeds"), dict) else {}
        parts.append(feed_health_table(quality_res.meta))
        for feed in FEEDS:
            block = feeds.get(feed)
            meta_block = quality_res.feed(feed)
            parts.append("<h4>%s %s</h4>" % (esc(FEED_LABELS.get(feed, feed)), status_badge(meta_block.get("state"))))
            if not isinstance(block, dict):
                parts.append('<div class="tp-muted">Không có generation cho feed này (%s).</div>'
                             % esc(FEED_STATE_TEXT.get(str(meta_block.get("state")), "không rõ")))
                continue
            counts = {k: block.get(k) for k in (
                "snapshot_id", "is_partial", "is_empty", "source_entity_count", "selected_entity_count",
                "fully_valid_entity_count", "partially_valid_entity_count", "rejected_entity_count") if k in block}
            parts.append(kv_table(counts))
            reasons = [r for r in (block.get("reasons") or []) if isinstance(r, dict)]
            parts.append(rows_table(reasons, columns=["reason_code", "severity", "disposition", "count", "unit", "category"],
                                    title="Lý do chất lượng"))
            if isinstance(block.get("accounting"), dict) and block.get("accounting"):
                parts.append(kv_table(block["accounting"], title="Accounting (đối soát số lượng)"))
        health = _components(data)
        if health:
            parts.append(rows_table(health, columns=["component", "status", "last_heartbeat_at", "heartbeat_age_seconds",
                                                     "last_success_at", "last_rejected_reason", "last_error"],
                                    title="Sức khỏe nguồn (collector/publisher)"))
    parts.append("<h4>Pipeline (Spark / Kafka / Cassandra)</h4>")
    if health_res is not None:
        if health_res.usable and isinstance(health_res.data, dict):
            checks = health_res.data.get("checks") if isinstance(health_res.data.get("checks"), dict) else {}
            summary = {"api /health/ready": health_res.data.get("status")}
            summary.update(checks)
            parts.append(kv_table(summary))
        else:
            parts.append(state_panel(health_res, "kiểm tra /health/ready", base_url))
    if pipeline_res is None or not pipeline_res.usable:
        parts.append(state_panel(pipeline_res, "trạng thái pipeline (/pipeline/status)", base_url))
        return _wrap("".join(parts))
    pdata = pipeline_res.data if isinstance(pipeline_res.data, dict) else {}
    parts.append(kv_table({k: pdata.get(k) for k in ("run_state", "state_reason", "clock_mode", "application_clock",
                                                    "clock_sequence", "clock_updated_at") if k in pdata}))
    parts.append(rows_table(_components(pdata), columns=[
        "component", "status", "last_heartbeat_at", "heartbeat_age_seconds", "last_success_at", "last_batch_id",
        "last_batch_duration_seconds", "snapshots_published_total", "duplicate_deliveries_total",
        "late_deliveries_total", "last_visibility_latency_seconds", "last_error"], title="Thành phần"))
    generations = pdata.get("generations")
    if isinstance(generations, dict) and generations:
        parts.append(feed_health_table({"feeds": generations}))
    backlog = pdata.get("kafka_backlog")
    if isinstance(backlog, list) and backlog:
        parts.append(rows_table(backlog, title="Kafka backlog (đã đo)"))
    else:
        parts.append('<div><b>Kafka backlog:</b> chưa đo</div>')
    latency = pdata.get("latency")
    if isinstance(latency, dict) and latency:
        parts.append(kv_table(latency, title="Độ trễ hiển thị (đã đo)"))
    else:
        parts.append('<div><b>Latency:</b> chưa đo</div>')
    return _wrap("".join(parts))


def collect_warnings(results: Dict[str, Optional[ApiResult]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for res in results.values():
        if isinstance(res, ApiResult):
            items.extend(res.warnings)
    return items


def _route_label(route: Dict[str, Any]) -> str:
    short = _first(route, "short_name", "route_short_name")
    long_name = _first(route, "long_name", "route_long_name")
    rid = route.get("route_id")
    name = " — ".join(str(x) for x in (short, long_name) if x)
    return "%s (%s)" % (name, rid) if name else str(rid)


def _stop_label(stop: Dict[str, Any]) -> str:
    name = _first(stop, "stop_name")
    return "%s (%s)" % (name, stop.get("stop_id")) if name else str(stop.get("stop_id"))


def _run_label(run: Dict[str, Any]) -> str:
    source = str(run.get("source_type") or "?").upper()
    created = str(run.get("created_at") or "")[:19].replace("T", " ")
    state = run.get("run_state") or run.get("state") or "?"
    return "%s · %s · %s · %s" % (source, str(run.get("run_id"))[:8], created or "?", state)


# ================================================================================================
# Notebook UI (ipywidgets 7.7 / 8.x; static HTML fallback)
# ================================================================================================
def _set_tab_titles(tab: Any, titles: Sequence[str]) -> None:
    try:
        if hasattr(tab, "titles"):  # ipywidgets 8
            tab.titles = tuple(titles)
            return
    except Exception:  # noqa: BLE001
        pass
    for i, title in enumerate(titles):  # ipywidgets 7
        tab.set_title(i, title)


def _enable_colab_widgets() -> None:
    """Colab renders ipywidgets 7.7 natively; 8.x needs Colab's custom widget manager. No-op elsewhere."""
    if widgets is None:
        return
    try:
        major = int(str(getattr(widgets, "__version__", "7")).split(".")[0])
    except ValueError:
        major = 7
    if major < 8:
        return
    try:
        from google.colab import output  # type: ignore[import-not-found]

        output.enable_custom_widget_manager()
    except Exception:  # noqa: BLE001 - not on Colab, or already enabled
        pass


def _kernel_loop() -> Any:
    """The kernel's running asyncio loop (callbacks then run in the kernel thread), else None."""
    try:
        import asyncio

        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            pass
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            loop = asyncio.get_event_loop()
        return loop if loop.is_running() else None
    except Exception:  # noqa: BLE001
        return None


class TransitPulseUI(object):
    """Five-tab notebook UI. Every number comes from the API; failed panels show their own state."""

    def __init__(self, api_base: Optional[str] = None, session: Optional[Dict[str, Any]] = None, *,
                 timeout: float = 5.0, refresh_seconds: float = 5.0,
                 max_auto_ticks: int = MAX_AUTO_REFRESH_TICKS, route_id: Optional[str] = None,
                 stop_id: Optional[str] = None, client: Optional[ApiClient] = None) -> None:
        self.session = dict(session or {})
        base = api_base or (self.session.get("api") or {}).get("base_url") or DEFAULT_API_BASE
        self.client = client or ApiClient(base, timeout=timeout)
        self.refresh_seconds = max(2.0, float(refresh_seconds))
        self.max_auto_ticks = max(1, min(int(max_auto_ticks), MAX_AUTO_REFRESH_TICKS))
        self.run_id: Optional[str] = self.session.get("run_id")
        self.route_id = route_id
        self.stop_id = stop_id
        self.direction_id: Optional[str] = None
        self.include_invalid = False
        self.last_results: Dict[str, Optional[ApiResult]] = {}
        self._runs: List[Dict[str, Any]] = []
        self._routes: List[Dict[str, Any]] = []
        self._stops: List[Dict[str, Any]] = []
        self._stops_res: Optional[ApiResult] = None
        self._static_cache: Dict[Tuple[str, str, str], ApiResult] = {}
        self._closed = False
        self._busy = False
        self._suspend = False
        self._timer: Any = None
        self._map_timer: Any = None
        self._map_live = False
        self._auto_remaining = 0
        self._auto_seconds = self.refresh_seconds
        self._observers: List[Tuple[Any, Callable]] = []
        self._clicks: List[Tuple[Any, Callable]] = []
        self._widgets: List[Any] = []
        self._root: Any = None
        self._banner: Any = None
        self._status: Any = None
        self._panels: List[Any] = []
        self._run_dd = self._route_dd = self._stop_dd = self._dir_dd = self._invalid_cb = None
        self._auto_btn = self._stop_auto_btn = None

    # ------------------------------------------------------------------ data loading
    def _scope_route_ids(self) -> List[str]:
        scope = self.session.get("scope") if isinstance(self.session.get("scope"), dict) else {}
        return [str(r) for r in (scope.get("route_ids") or [])]

    def _load_runs(self) -> ApiResult:
        res = self.client.get("/runs", {"limit": 50}, list_key="runs")
        rows = extract_list(res.data, "runs") if res.usable else []
        self._runs = [r for r in rows if r.get("run_id")]
        if self.run_id is None and self._runs:
            self.run_id = str(self._runs[0]["run_id"])
        self.last_results["runs"] = res
        return res

    def _load_routes(self) -> Optional[ApiResult]:
        self._routes = []
        if not self.run_id:
            return None
        res = self.client.get("/routes", {"run_id": self.run_id}, list_key="routes")
        self._routes = [r for r in extract_list(res.data, "routes") if r.get("route_id")] if res.usable else []
        ids = [str(r["route_id"]) for r in self._routes]
        if self.route_id not in ids:
            preferred = [r for r in self._scope_route_ids() if r in ids]
            self.route_id = preferred[0] if preferred else (ids[0] if ids else None)
        self.last_results["routes"] = res
        return res

    def _static_get(self, kind: str, path: str, list_key: str) -> ApiResult:
        key = (kind, str(self.run_id), str(self.route_id))
        cached = self._static_cache.get(key)
        if cached is not None:
            return cached
        res = self.client.get(path, {"run_id": self.run_id}, list_key=list_key)
        if res.kind in ("ok", "empty"):  # static catalogue rows are immutable per GTFS version
            self._static_cache[key] = res
        return res

    def _load_stops(self) -> Optional[ApiResult]:
        self._stops = []
        if not (self.run_id and self.route_id):
            self._stops_res = None
            return None
        res = self._static_get("stops", "/routes/%s/stops" % seg(self.route_id), "stops")
        rows = [r for r in extract_list(res.data, "stops") if r.get("stop_id")] if res.usable else []
        self._stops = sorted(rows, key=lambda r: (str(r.get("stop_name") or ""), str(r.get("stop_id"))))
        ids = [str(r["stop_id"]) for r in self._stops]
        if self.stop_id not in ids:
            self.stop_id = ids[0] if ids else None
        self._stops_res = res
        return res

    def fetch_all(self) -> Dict[str, Optional[ApiResult]]:
        """One request per panel (sequential, each bounded by the client timeout)."""
        results: Dict[str, Optional[ApiResult]] = {}
        if not self.run_id:
            return results
        c = self.client
        run = self.run_id
        results["run"] = c.get("/runs/%s" % seg(run))
        if results["run"].kind == "unreachable":
            return results  # same API for every panel: do not wait for N more timeouts
        if self.route_id:
            route = seg(self.route_id)
            results["metrics"] = c.get("/routes/%s/metrics" % route, {"run_id": run})
            results["geometry"] = self._static_get("geometry", "/routes/%s/geometry" % route, "shapes")
            results["stops"] = self._stops_res
            results["vehicles"] = c.get("/vehicles", {"run_id": run, "route_id": self.route_id},
                                        primary_feed="vehicle_positions", list_key="vehicles")
            if self.stop_id:
                results["predictions"] = c.get(
                    "/stops/%s/predictions" % seg(self.stop_id),
                    {"run_id": run, "route_id": self.route_id, "direction_id": self.direction_id,
                     "include": "all" if self.include_invalid else "displayable"},
                    primary_feed="trip_updates", list_key="predictions",
                )
        results["alerts"] = c.get("/alerts", {"run_id": run, "route_id": self.route_id},
                                  primary_feed="alerts", list_key="alerts")
        results["quality"] = c.get("/quality", {"run_id": run})
        results["pipeline"] = c.get("/pipeline/status", {"run_id": run})
        results["health"] = c.get("/health/ready")
        return results

    def _labels(self) -> Tuple[Optional[str], Optional[str]]:
        route = next((r for r in self._routes if str(r.get("route_id")) == str(self.route_id)), None)
        stop = next((s for s in self._stops if str(s.get("stop_id")) == str(self.stop_id)), None)
        route_label = _route_label(route) if route else (str(self.route_id) if self.route_id else None)
        stop_label = _stop_label(stop) if stop else (str(self.stop_id) if self.stop_id else None)
        return route_label, stop_label

    def render_all(self, results: Dict[str, Optional[ApiResult]]) -> Tuple[str, List[str]]:
        base = self.client.base_url
        banner = _wrap(render_banner(results.get("run"), self.session, api_base=base))
        if not self.run_id:
            runs_res = self.last_results.get("runs")
            if runs_res is not None and not runs_res.usable:
                panel = _wrap(state_panel(runs_res, "danh sách run", base))
            else:
                panel = _wrap(_note("info", "Chưa có run nào", esc("Chạy cell 12 (start) rồi bấm Làm mới.")))
            return banner, [panel] * len(TAB_TITLES)
        run_res = results.get("run")
        if run_res is not None and run_res.kind == "unreachable":
            panel = _wrap(state_panel(run_res, "dữ liệu run", base))
            return banner, [panel] * len(TAB_TITLES)
        route_label, stop_label = self._labels()
        if not self.route_id:
            routes_res = self.last_results.get("routes")
            msg = state_panel(routes_res, "danh sách tuyến", base) if routes_res is not None and not routes_res.usable \
                else _note("info", "Chưa có tuyến", esc("Run chưa có tuyến trong phạm vi hoặc /routes rỗng."))
            route_panels = [_wrap(msg)] * 3
        else:
            route_panels = [
                render_overview(results.get("metrics"), run_res, route_label=route_label, session=self.session,
                                base_url=base),
                render_map_panel(results.get("geometry"), results.get("stops"), results.get("vehicles"),
                                 route_label=route_label, base_url=base),
                render_predictions_panel(results.get("predictions"), stop_label=stop_label,
                                         include_invalid=self.include_invalid, base_url=base),
            ]
        alerts = render_alerts_panel(results.get("alerts"), collect_warnings(results), base_url=base)
        quality = render_quality_panel(results.get("quality"), results.get("pipeline"), results.get("health"),
                                       base_url=base)
        return banner, route_panels + [alerts, quality]

    def render_static_html(self) -> str:
        """Everything in one static HTML document (fallback when widgets are unavailable)."""
        self._load_runs()
        self._load_routes()
        self._load_stops()
        results = self.fetch_all()
        self.last_results.update(results)
        banner, panels = self.render_all(results)
        out = [banner]
        for title, panel in zip(TAB_TITLES, panels):
            out.append(_wrap("<h3>%s</h3>" % esc(title)) + panel)
        out.append(_wrap('<div class="tp-muted">Chế độ HTML tĩnh (không có ipywidgets). Chạy lại cell để làm mới; '
                         "chụp lúc %s.</div>" % fmt_ts(iso_utc(utcnow()))))
        return "".join(out)

    # ------------------------------------------------------------------ widgets
    def _w(self, widget: Any) -> Any:
        self._widgets.append(widget)
        return widget

    def _observe(self, widget: Any, handler: Callable) -> None:
        widget.observe(handler, names="value")
        self._observers.append((widget, handler))

    def _on_click(self, button: Any, handler: Callable) -> None:
        button.on_click(handler)
        self._clicks.append((button, handler))

    def _build(self) -> Any:
        W = widgets
        style = {"description_width": "initial"}
        self._banner = self._w(W.HTML(value=_wrap(render_banner(None, self.session, loading=True))))
        self._status = self._w(W.HTML(value=""))
        self._run_dd = self._w(W.Dropdown(options=[], description="Run", style=style, layout=W.Layout(width="440px")))
        self._route_dd = self._w(W.Dropdown(options=[], description="Tuyến", style=style, layout=W.Layout(width="360px")))
        self._stop_dd = self._w(W.Dropdown(options=[], description="Trạm", style=style, layout=W.Layout(width="360px")))
        self._dir_dd = self._w(W.Dropdown(options=[("Tất cả hướng", ""), ("Hướng 0", "0"), ("Hướng 1", "1")], value="",
                                          description="Hướng", style=style, layout=W.Layout(width="200px")))
        self._invalid_cb = self._w(W.Checkbox(value=False, description="Hiện cả dự báo INVALID", indent=False))
        refresh_btn = self._w(W.Button(description="Làm mới", button_style="primary", tooltip="Gọi lại API"))
        self._auto_btn = self._w(W.Button(description="Tự làm mới (≤ %d lần)" % self.max_auto_ticks,
                                          tooltip="Làm mới mỗi %d giây, tối đa %d lần" % (int(self.refresh_seconds),
                                                                                          self.max_auto_ticks)))
        self._stop_auto_btn = self._w(W.Button(description="Dừng tự làm mới", button_style="warning", disabled=True))
        self._panels = [self._w(W.HTML(value=_wrap('<span class="tp-loading">Đang tải…</span>'))) for _ in TAB_TITLES]
        tab = self._w(W.Tab(children=self._panels))
        _set_tab_titles(tab, TAB_TITLES)
        row1 = self._w(W.HBox([self._run_dd, self._route_dd], layout=W.Layout(flex_flow="row wrap")))
        row2 = self._w(W.HBox([self._stop_dd, self._dir_dd, self._invalid_cb], layout=W.Layout(flex_flow="row wrap")))
        row3 = self._w(W.HBox([refresh_btn, self._auto_btn, self._stop_auto_btn], layout=W.Layout(flex_flow="row wrap")))
        self._root = self._w(W.VBox([self._banner, row1, row2, row3, self._status, tab]))
        self._observe(self._run_dd, self._on_run_change)
        self._observe(self._route_dd, self._on_route_change)
        self._observe(self._stop_dd, self._on_stop_change)
        self._observe(self._dir_dd, self._on_direction_change)
        self._observe(self._invalid_cb, self._on_invalid_change)
        self._on_click(refresh_btn, self._on_refresh_click)
        self._on_click(self._auto_btn, self._on_auto_click)
        self._on_click(self._stop_auto_btn, self._on_stop_auto_click)
        return self._root

    def _set_options(self, dropdown: Any, options: List[Tuple[str, str]], value: Optional[str]) -> None:
        if dropdown is None:
            return
        self._suspend = True
        try:
            current = [tuple(o) if isinstance(o, (list, tuple)) else o for o in (dropdown.options or ())]
            if current != [tuple(o) for o in options]:  # avoid re-rendering an unchanged (maybe open) dropdown
                dropdown.options = options
            values = [v for _label, v in options]
            target = value if value in values else (values[0] if values else None)
            if dropdown.value != target:
                dropdown.value = target
        finally:
            self._suspend = False

    def _sync_widgets(self) -> None:
        run_opts = [(_run_label(r), str(r["run_id"])) for r in self._runs]
        if self.run_id and self.run_id not in [v for _l, v in run_opts]:
            run_opts.insert(0, ("%s (từ phiên hiện tại)" % self.run_id[:8], self.run_id))
        self._set_options(self._run_dd, run_opts, self.run_id)
        self._set_options(self._route_dd, [(_route_label(r), str(r["route_id"])) for r in self._routes], self.route_id)
        self._set_options(self._stop_dd, [(_stop_label(s), str(s["stop_id"])) for s in self._stops], self.stop_id)

    def _set_status(self, fragment: str) -> None:
        if self._status is not None:
            self._status.value = _wrap(fragment)

    def refresh(self, reload_lists: bool = False) -> Dict[str, Optional[ApiResult]]:
        """Re-query the API for every panel. Never blocks forever (each request has a timeout)."""
        if self._closed or self._busy:
            return self.last_results
        self._busy = True
        started = time.monotonic()
        try:
            self._set_status('<span class="tp-loading">Đang tải…</span>')
            if reload_lists or not self._runs:
                self._load_runs()
            if reload_lists or not self._routes:
                self._load_routes()
            if reload_lists or self._stops_res is None or not self._stops_res.usable:
                self._load_stops()
            self._sync_widgets()
            results = self.fetch_all()
            self.last_results.update(results)
            banner, panels = self.render_all(results)
            if self._banner is not None:
                self._banner.value = banner
            for widget, fragment in zip(self._panels, panels):
                if widget.value != fragment:
                    widget.value = fragment
            auto = (" · tự làm mới còn %d lần" % self._auto_remaining) if self._timer is not None else ""
            self._set_status('<span class="tp-muted">Cập nhật lúc %s · %d yêu cầu · %.1f s%s</span>'
                             % (fmt_ts(iso_utc(utcnow())), len(results), time.monotonic() - started, esc(auto)))
            return results
        except Exception as exc:  # noqa: BLE001 - a UI error must not kill the kernel callback loop
            self._set_status(_note("err", "Lỗi khi làm mới giao diện", esc("%s: %s" % (type(exc).__name__, exc))))
            return self.last_results
        finally:
            self._busy = False

    def refresh_map(self) -> None:
        """Re-query vehicle positions and redraw only the map tab."""
        if self._closed or self._busy or not self.run_id or not self.route_id or len(self._panels) < 2:
            return
        self._busy = True
        try:
            veh = self.client.get(
                "/vehicles", {"run_id": self.run_id, "route_id": self.route_id},
                primary_feed="vehicle_positions", list_key="vehicles",
            )
            self.last_results["vehicles"] = veh
            route_label, _stop_label = self._labels()
            fragment = render_map_panel(
                self.last_results.get("geometry"),
                self.last_results.get("stops") or self._stops_res,
                veh, route_label=route_label, base_url=self.client.base_url,
            )
            panel = self._panels[1]
            if panel.value != fragment:
                panel.value = fragment
            self._set_status(
                '<span class="tp-muted">Bản đồ cập nhật lúc %s · mỗi %d giây cho đến khi đóng UI</span>'
                % (fmt_ts(iso_utc(utcnow())), int(self.refresh_seconds))
            )
        except Exception as exc:  # noqa: BLE001 - a map tick must not kill the kernel callback loop
            self._set_status(_note("err", "Lỗi khi cập nhật bản đồ", esc("%s: %s" % (type(exc).__name__, exc))))
        finally:
            self._busy = False

    # ------------------------------------------------------------------ callbacks
    def _on_run_change(self, change: Any) -> None:
        if self._suspend or self._closed:
            return
        self.run_id = change.get("new") or None
        self.route_id = None
        self.stop_id = None
        self._routes = []
        self._stops = []
        self._stops_res = None
        self.refresh()

    def _on_route_change(self, change: Any) -> None:
        if self._suspend or self._closed:
            return
        self.route_id = change.get("new") or None
        self.stop_id = None
        self._stops = []
        self._stops_res = None
        self.refresh()

    def _on_stop_change(self, change: Any) -> None:
        if self._suspend or self._closed:
            return
        self.stop_id = change.get("new") or None
        self.refresh()

    def _on_direction_change(self, change: Any) -> None:
        if self._suspend or self._closed:
            return
        self.direction_id = change.get("new") or None
        self.refresh()

    def _on_invalid_change(self, change: Any) -> None:
        if self._suspend or self._closed:
            return
        self.include_invalid = bool(change.get("new"))
        self.refresh()

    def _on_refresh_click(self, _button: Any) -> None:
        self.refresh(reload_lists=True)

    def _on_auto_click(self, _button: Any) -> None:
        self.start_auto_refresh()

    def _on_stop_auto_click(self, _button: Any) -> None:
        self.stop_auto_refresh()
        self._set_status('<span class="tp-muted">Đã dừng tự làm mới.</span>')

    # ------------------------------------------------------------------ bounded auto refresh
    def start_auto_refresh(self, seconds: Optional[float] = None, max_ticks: Optional[int] = None) -> None:
        """Refresh every ``seconds`` at most ``max_ticks`` (≤ 60) times; one timer per UI, never a loop."""
        if self._closed:
            return
        self.stop_auto_refresh()
        self._auto_seconds = max(2.0, float(seconds or self.refresh_seconds))
        self._auto_remaining = max(1, min(int(max_ticks or self.max_auto_ticks), MAX_AUTO_REFRESH_TICKS))
        if self._auto_btn is not None:
            self._auto_btn.disabled = True
        if self._stop_auto_btn is not None:
            self._stop_auto_btn.disabled = False
        self._schedule()

    def _schedule(self) -> None:
        loop = _kernel_loop()
        if loop is not None:
            self._timer = loop.call_later(self._auto_seconds, self._tick)
        else:
            timer = threading.Timer(self._auto_seconds, self._tick)
            timer.daemon = True
            timer.start()
            self._timer = timer

    def _tick(self) -> None:
        if self._closed or self._auto_remaining <= 0:
            self._timer = None
            return
        self._auto_remaining -= 1
        try:
            self.refresh()
        except Exception:  # noqa: BLE001
            pass
        if not self._closed and self._auto_remaining > 0:
            self._schedule()
        else:
            self._timer = None
            if self._auto_btn is not None:
                self._auto_btn.disabled = False
            if self._stop_auto_btn is not None:
                self._stop_auto_btn.disabled = True
            if not self._closed:
                self._set_status('<span class="tp-muted">Đã hết số lần tự làm mới; bấm Làm mới hoặc bật lại.</span>')

    def start_map_live(self) -> None:
        """Keep redrawing the map tab until the UI is closed. Does not block the cell."""
        if self._closed or not self._panels:
            return
        self.stop_map_live()
        self._map_live = True
        self._schedule_map()

    def _schedule_map(self) -> None:
        loop = _kernel_loop()
        if loop is not None:
            self._map_timer = loop.call_later(self.refresh_seconds, self._map_tick)
        else:
            timer = threading.Timer(self.refresh_seconds, self._map_tick)
            timer.daemon = True
            timer.start()
            self._map_timer = timer

    def _map_tick(self) -> None:
        if self._closed or not self._map_live:
            self._map_timer = None
            return
        try:
            self.refresh_map()
        except Exception:  # noqa: BLE001
            pass
        if not self._closed and self._map_live:
            self._schedule_map()
        else:
            self._map_timer = None

    def stop_map_live(self) -> None:
        self._map_live = False
        timer = self._map_timer
        self._map_timer = None
        if timer is not None:
            try:
                timer.cancel()
            except Exception:  # noqa: BLE001
                pass

    def stop_auto_refresh(self) -> None:
        timer = self._timer
        self._timer = None
        self._auto_remaining = 0
        if timer is not None:
            try:
                timer.cancel()
            except Exception:  # noqa: BLE001
                pass
        if self._auto_btn is not None:
            self._auto_btn.disabled = False
        if self._stop_auto_btn is not None:
            self._stop_auto_btn.disabled = True

    # ------------------------------------------------------------------ lifecycle
    def show(self, auto_refresh: bool = False) -> "TransitPulseUI":
        """Display the UI (closing the previously shown one) and load data once."""
        _activate("ui", self)
        if widgets is None or display is None:
            html_doc = self.render_static_html()
            if display is not None and HTML is not None:
                display(HTML(html_doc))
            else:
                print(_strip_tags(html_doc))
            return self
        _enable_colab_widgets()
        display(self._build())
        self.refresh(reload_lists=True)
        self.start_map_live()
        if auto_refresh:
            self.start_auto_refresh()
        return self

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.stop_map_live()
        self.stop_auto_refresh()
        for widget, handler in self._observers:
            try:
                widget.unobserve(handler, names="value")
            except Exception:  # noqa: BLE001
                pass
        for button, handler in self._clicks:
            try:
                button.on_click(handler, remove=True)
            except Exception:  # noqa: BLE001
                pass
        self._observers = []
        self._clicks = []
        for widget in reversed(self._widgets):
            try:
                widget.close()
            except Exception:  # noqa: BLE001
                pass
        self._widgets = []
        _deactivate("ui", self)


# ================================================================================================
# Interactive fault scenarios (cell 16) — NOT_RUN until a button is pressed
# ================================================================================================
FAULT_SCENARIOS = (
    {
        "id": "duplicate_snapshot",
        "button": "Duplicate: phát lại snapshot",
        "title": "Phát lại (duplicate) một snapshot đã xử lý",
        "cli": ("inject", "--scenario", "duplicate_snapshot"),
        "source_clock_only": True,
        "counter": "duplicate_deliveries_total",
        "expected": "Snapshot trùng không tạo lịch sử/KPI mới: con trỏ vehicle_positions không lùi, "
                    "duplicate_deliveries_total của processor tăng (DUPLICATE_SNAPSHOT_DELIVERY), "
                    "số chuyến đủ điều kiện/chuyến trễ không bị cộng thêm vì bản trùng.",
    },
    {
        "id": "late_snapshot",
        "button": "Late: gửi snapshot cũ",
        "title": "Gửi snapshot cũ hơn con trỏ hiện tại (late/out-of-order)",
        "cli": ("inject", "--scenario", "late_snapshot"),
        "source_clock_only": True,
        "counter": "late_deliveries_total",
        "expected": "Snapshot cũ được ghi READY nhưng không publish: con trỏ không lùi (feed_timestamp không giảm), "
                    "late_deliveries_total tăng (LATE_SNAPSHOT_NOT_PUBLISHED).",
    },
    {
        "id": "advance_clock",
        "button": "Tiến đồng hồ nguồn",
        "title": "Tiến đồng hồ nguồn khi feed ngừng cập nhật (stale)",
        "cli": ("advance-clock",),
        "source_clock_only": True,
        "counter": None,
        "expected": "Đồng hồ ứng dụng tới mốc yêu cầu; nếu feed ngừng (nên Tạm dừng trước) thì VP quá 120 s và "
                    "TU quá 90 s chuyển STALE; không xe nào bị tính fresh sai.",
    },
    {
        "id": "restart_processor",
        "button": "Restart processor",
        "title": "Dừng và chạy lại processor cùng run/checkpoint (cùng VM, Kafka, Cassandra)",
        "cli": ("restart-processor",),
        "source_clock_only": False,
        "counter": None,
        "timeout": 600,
        "expected": "Processor chạy lại cùng run_id và checkpoint: batch id tiếp tục, con trỏ không lùi, "
                    "không nhân đôi lịch sử/KPI.",
    },
)
_SCENARIO_BY_ID = {s["id"]: s for s in FAULT_SCENARIOS}


def _component(data: Any, name: str) -> Dict[str, Any]:
    for comp in _components(data):
        if comp.get("component") == name or comp.get("name") == name:
            return comp
    return {}


def _comp_field(comp: Dict[str, Any], key: str) -> Any:
    value = comp.get(key)
    if value is None and isinstance(comp.get("details"), dict):
        value = comp["details"].get(key)
    return value


def observe_pipeline_state(client: ApiClient, run_id: str, route_id: Optional[str] = None) -> Dict[str, Any]:
    """Read-only snapshot of API values used to compare before/after a fault scenario."""
    snap: Dict[str, Any] = {"observed_at": iso_utc(utcnow())}
    quality = client.get("/quality", {"run_id": run_id})
    snap["api_kind"] = quality.kind
    snap["application_clock"] = quality.meta.get("application_clock")
    snap["run_state"] = quality.meta.get("run_state")
    for feed in FEEDS:
        block = quality.feed(feed)
        snap[feed] = {k: block.get(k) for k in ("state", "snapshot_id", "feed_timestamp", "stale")}
    pipeline = client.get("/pipeline/status", {"run_id": run_id})
    proc = _component(pipeline.data, "processor") if pipeline.usable else {}
    snap["processor"] = {k: _comp_field(proc, k) for k in (
        "status", "last_batch_id", "duplicate_deliveries_total", "late_deliveries_total", "snapshots_published_total")}
    if route_id:
        metrics = client.get("/routes/%s/metrics" % seg(route_id), {"run_id": run_id})
        data = metrics.data if metrics.usable and isinstance(metrics.data, dict) else {}
        pm = data.get("prediction_metrics") if isinstance(data.get("prediction_metrics"), dict) else {}
        vm = data.get("vehicle_metrics") if isinstance(data.get("vehicle_metrics"), dict) else {}
        snap["kpi"] = {k: pm.get(k) for k in ("eligible_trip_count", "predicted_late_trip_count", "predicted_late_rate")}
        snap["vehicles"] = {
            "fresh": _count_from(vm, ("fresh_vehicle_count",)),
            "stale": _count_from(vm, ("stale_vehicle_count",)),
            "unknown_age": _count_from(vm, ("unknown_age_vehicle_count",)),
            "membership": _count_from(vm, ("snapshot_membership_count",)),
        }
    return snap


def _pointer_not_regressed(before: Dict[str, Any], after: Dict[str, Any], feed: str = "vehicle_positions") -> Optional[bool]:
    b = parse_iso((before.get(feed) or {}).get("feed_timestamp"))
    a = parse_iso((after.get(feed) or {}).get("feed_timestamp"))
    if a is None or b is None:
        return None
    return a >= b


def _increased(before: Any, after: Any) -> bool:
    return _is_num(before) and _is_num(after) and float(after) > float(before)


def judge_fault(scenario_id: str, before: Dict[str, Any], after: Dict[str, Any], record: Dict[str, Any]) -> Tuple[str, str]:
    """PASS / FAIL / INCONCLUSIVE from API signals only (comparisons, no KPI computation)."""
    spec = _SCENARIO_BY_ID[scenario_id]
    kept = _pointer_not_regressed(before, after)
    if scenario_id in ("duplicate_snapshot", "late_snapshot"):
        key = spec["counter"]
        b = (before.get("processor") or {}).get(key)
        a = (after.get("processor") or {}).get(key)
        if kept is False:
            return "FAIL", "Con trỏ vehicle_positions lùi về snapshot cũ hơn (feed_timestamp giảm)."
        if _increased(b, a):
            return "PASS", "%s tăng (%s → %s); con trỏ không lùi." % (key, b, a)
        return "INCONCLUSIVE", "Chưa thấy %s tăng (%s → %s) trong thời gian chờ; xem /quality và log processor." % (key, b, a)
    if scenario_id == "advance_clock":
        target = parse_iso(record.get("target_clock"))
        clock = parse_iso(after.get("application_clock"))
        vp = (after.get("vehicle_positions") or {}).get("stale")
        tu = (after.get("trip_updates") or {}).get("stale")
        if clock is not None and target is not None and clock >= target:
            return "PASS", "Đồng hồ ứng dụng = %s (≥ mốc). VP stale=%s, TU stale=%s." % (after.get("application_clock"), vp, tu)
        return "INCONCLUSIVE", "Đồng hồ ứng dụng (%s) chưa tới mốc %s." % (after.get("application_clock"), record.get("target_clock"))
    if scenario_id == "restart_processor":
        cli = record.get("cli_result") if isinstance(record.get("cli_result"), dict) else {}
        batches = "batch %s → %s, downtime %s s" % (cli.get("prev_batch_id"), cli.get("resumed_batch_id"),
                                                    cli.get("downtime_s"))
        if kept is False:
            return "FAIL", "Con trỏ vehicle_positions lùi sau khi restart (%s)." % batches
        status = str((after.get("processor") or {}).get("status") or "")
        if kept is True and after.get("api_kind") in USABLE_KINDS:
            return "PASS", "Processor chạy lại cùng run (status %s, %s); con trỏ không lùi." % (status or "?", batches)
        return "INCONCLUSIVE", "Chưa đọc được con trỏ sau restart (API: %s; %s)." % (after.get("api_kind"), batches)
    return "INCONCLUSIVE", "Không có quy tắc đánh giá."


def render_fault_record(rec: Dict[str, Any]) -> str:
    before = rec.get("before") or {}
    after = rec.get("after") or {}
    fields = [
        ("application_clock", lambda s: s.get("application_clock")),
        ("VP snapshot_id", lambda s: (s.get("vehicle_positions") or {}).get("snapshot_id")),
        ("VP feed_timestamp", lambda s: (s.get("vehicle_positions") or {}).get("feed_timestamp")),
        ("VP stale", lambda s: (s.get("vehicle_positions") or {}).get("stale")),
        ("TU stale", lambda s: (s.get("trip_updates") or {}).get("stale")),
        ("processor last_batch_id", lambda s: (s.get("processor") or {}).get("last_batch_id")),
        ("duplicate_deliveries_total", lambda s: (s.get("processor") or {}).get("duplicate_deliveries_total")),
        ("late_deliveries_total", lambda s: (s.get("processor") or {}).get("late_deliveries_total")),
        ("KPI eligible / late", lambda s: "%s / %s" % ((s.get("kpi") or {}).get("eligible_trip_count"),
                                                     (s.get("kpi") or {}).get("predicted_late_trip_count")) if s.get("kpi") else None),
        ("Xe fresh / stale / unknown", lambda s: "%s / %s / %s" % ((s.get("vehicles") or {}).get("fresh"),
                                                                   (s.get("vehicles") or {}).get("stale"),
                                                                   (s.get("vehicles") or {}).get("unknown_age")) if s.get("vehicles") else None),
    ]
    rows = "".join("<tr><th>%s</th><td>%s</td><td>%s</td></tr>" % (esc(name), fmt_value(fn(before)), fmt_value(fn(after)))
                   for name, fn in fields)
    parts = [
        '<div class="tp-card"><b>%s</b> %s' % (esc(rec.get("title") or rec.get("scenario")), status_badge(rec.get("status"))),
        '<div class="tp-muted">inject lúc %s · quan sát lúc %s · run %s</div>'
        % (fmt_ts(rec.get("injected_at")), fmt_ts(rec.get("observed_at")), short_id(rec.get("run_id"), 8)),
        "<div><b>Kỳ vọng:</b> %s</div>" % esc(rec.get("expected")),
        "<div><b>Thực tế:</b> %s</div>" % esc(rec.get("reason")),
    ]
    if before or after:
        parts.append('<table class="tp-table"><tr><th>Giá trị (API)</th><th>Trước</th><th>Sau</th></tr>%s</table>' % rows)
    if rec.get("error_code"):
        parts.append(_note("err", "CLI lỗi: %s" % rec.get("error_code"), esc(rec.get("hint") or "")))
    if rec.get("log_reference"):
        parts.append('<div class="tp-muted">Log: %s</div>' % esc(rec.get("log_reference")))
    if rec.get("cli_result"):
        parts.append(json_details(rec.get("cli_result"), "JSON của CLI"))
    parts.append("</div>")
    return "".join(parts)


def _trim(doc: Any, limit: int = 4000) -> Any:
    if not isinstance(doc, dict):
        return doc
    text = json.dumps({k: v for k, v in doc.items() if not str(k).startswith("_")}, ensure_ascii=False, default=str)
    if len(text) <= limit:
        return json.loads(text)
    return {"truncated_json": text[:limit] + "…"}


class FaultLab(object):
    """Buttons for duplicate / late / advance clock / restart processor with expected vs actual."""

    def __init__(self, runtime: Dict[str, Any], api_base: Optional[str] = None, start: Optional[Dict[str, Any]] = None, *,
                 route_id: Optional[str] = None, settle_seconds: float = 30.0, client: Optional[ApiClient] = None) -> None:
        self.runtime = runtime
        self.start = dict(start or {})
        self.run_id = self.start.get("run_id")
        self.mode = str(self.start.get("mode") or self.start.get("source_type") or "").lower()
        scope = self.start.get("scope") if isinstance(self.start.get("scope"), dict) else {}
        routes = scope.get("route_ids") or []
        self.route_id = route_id or (str(routes[0]) if routes else None)
        base = api_base or (self.start.get("api") or {}).get("base_url") or (runtime.get("api") or {}).get("base_url")
        self.client = client or ApiClient(base or DEFAULT_API_BASE)
        self.settle_seconds = max(0.0, min(float(settle_seconds), 120.0))
        self.results_path = Path(str(runtime["workdir"])) / "reports" / "notebook" / "fault_results.json"
        self.results: Dict[str, Dict[str, Any]] = {
            s["id"]: {"scenario": s["id"], "title": s["title"], "status": "NOT_RUN", "expected": s["expected"]}
            for s in FAULT_SCENARIOS
        }
        existing = None
        try:
            existing = read_json(self.results_path)
        except CellFailed:
            existing = None
        if isinstance(existing, dict) and existing.get("run_id") == self.run_id:
            for sid, rec in (existing.get("scenarios") or {}).items():
                if sid in self.results and isinstance(rec, dict):
                    self.results[sid] = rec
        self._closed = False
        self._busy = False
        self._widgets: List[Any] = []
        self._buttons: List[Tuple[Any, bool]] = []
        self._out: Any = None
        self._summary: Any = None
        self._advance: Any = None

    @property
    def supports_source_clock(self) -> bool:
        return self.mode in ("replay", "synthetic")

    def _log_reference(self) -> str:
        comps = self.start.get("components") if isinstance(self.start.get("components"), dict) else {}
        proc = comps.get("processor") if isinstance(comps.get("processor"), dict) else {}
        log = proc.get("log") or proc.get("stdout_log")
        cmd = "demo.py logs --component processor --lines 200 --workdir %s" % self.runtime.get("workdir")
        return ("%s ; %s" % (log, cmd)) if log else cmd

    def _save(self, record: Dict[str, Any]) -> None:
        self.results[record["scenario"]] = record
        doc = {
            "schema_version": 1,
            "run_id": self.run_id,
            "session_id": self.start.get("session_id"),
            "mode": self.mode,
            "updated_at": iso_utc(utcnow()),
            "note": "NOT_RUN = chưa chạy; PASS/FAIL/INCONCLUSIVE dựa trên tín hiệu đọc từ API sau khi inject.",
            "scenarios": self.results,
        }
        try:
            write_json_atomic(self.results_path, doc)
        except OSError as exc:
            print("Không ghi được %s: %s" % (self.results_path, exc))

    def run(self, scenario_id: str, advance_seconds: int = 160) -> Dict[str, Any]:
        spec = _SCENARIO_BY_ID[scenario_id]
        record: Dict[str, Any] = {
            "scenario": scenario_id, "title": spec["title"], "expected": spec["expected"], "run_id": self.run_id,
            "mode": self.mode, "status": "ERROR",
        }
        if not self.run_id:
            record.update(status="NOT_RUN", reason="Chưa có session (chạy cell 12).")
            self._save(record)
            return record
        if spec["source_clock_only"] and not self.supports_source_clock:
            record.update(status="NOT_RUN", reason="Không hỗ trợ ở chế độ %s (đồng hồ wall); chỉ replay/synthetic."
                          % (self.mode.upper() or "?"))
            self._save(record)
            return record
        before = observe_pipeline_state(self.client, self.run_id, self.route_id)
        record["before"] = before
        args = list(spec["cli"])
        if scenario_id == "advance_clock":
            clock = parse_iso(before.get("application_clock"))
            if clock is None:
                record.update(reason="API chưa trả application_clock nên không tính được mốc đích.")
                self._save(record)
                return record
            seconds = max(1, min(int(advance_seconds), 3600))
            record["target_clock"] = iso_utc(clock + timedelta(seconds=seconds))
            args += ["--to", record["target_clock"]]
        record["injected_at"] = iso_utc(utcnow())
        try:
            cli = run_cli(self.runtime, "demo.py", *args, timeout=float(spec.get("timeout", 300)),
                          label="demo.py " + " ".join(args))
        except CellFailed as exc:
            record.update(status="ERROR", reason=exc.message, error_code=exc.code, hint=exc.hint,
                          cli_result=_trim(exc.result), log_reference=self._log_reference())
            self._save(record)
            return record
        record["cli_result"] = _trim(cli)
        deadline = time.monotonic() + self.settle_seconds
        while True:
            after = observe_pipeline_state(self.client, self.run_id, self.route_id)
            verdict, why = judge_fault(scenario_id, before, after, record)
            if verdict in ("PASS", "FAIL") or time.monotonic() >= deadline:
                break
            time.sleep(3.0)
        record.update(after=after, status=verdict, reason=why, observed_at=after.get("observed_at"),
                      log_reference=self._log_reference())
        self._save(record)
        return record

    def control(self, command: str) -> Dict[str, Any]:
        """pause / resume through demo.py (replay and synthetic only)."""
        if not self.supports_source_clock:
            raise CellFailed("UNSUPPORTED_MODE", "%s chỉ hỗ trợ replay/synthetic." % command)
        return run_cli(self.runtime, "demo.py", command, timeout=120, label="demo.py " + command,
                       show=lambda d: show_cli_result(d, "demo.py " + command))

    def summary_html(self) -> str:
        rows = []
        for spec in FAULT_SCENARIOS:
            rec = self.results.get(spec["id"]) or {}
            rows.append({"kịch bản": spec["title"], "status": rec.get("status", "NOT_RUN"),
                         "inject lúc": rec.get("injected_at"), "kết luận": rec.get("reason")})
        note = ('<div class="tp-muted">NOT_RUN = chưa chạy (mặc định). PASS/FAIL dựa trên tín hiệu đọc từ API; '
                "INCONCLUSIVE = chưa đủ tín hiệu trong %d giây chờ. Kết quả ghi vào %s.</div>"
                % (int(self.settle_seconds), esc(self.results_path)))
        return rows_table(rows, columns=["kịch bản", "status", "inject lúc", "kết luận"], title="Kết quả kịch bản") + note

    def _set_busy(self, busy: bool) -> None:
        self._busy = busy
        for button, supported in self._buttons:
            button.disabled = busy or not supported

    def _handler(self, action: str) -> Callable:
        def _run(_button: Any) -> None:
            if self._busy or self._closed:
                return
            self._set_busy(True)
            try:
                with self._out:
                    print("=== %s — %s ===" % (action, iso_utc(utcnow())))
                    if action in ("pause", "resume"):
                        self.control(action)
                    else:
                        seconds = self._advance.value if self._advance is not None else 160
                        rec = self.run(action, advance_seconds=seconds)
                        show_html(render_fault_record(rec))
            except Exception as exc:  # noqa: BLE001
                with self._out:
                    print("Lỗi: %s" % exc)
            finally:
                self._set_busy(False)
                if self._summary is not None:
                    self._summary.value = _wrap(self.summary_html())
        return _run

    def show(self) -> "FaultLab":
        _activate("faults", self)
        self._save_initial()
        intro = ("<h4>Kịch bản kiểm tra tương tác (tùy chọn)</h4><div class='tp-muted'>Run %s · chế độ %s · tuyến quan sát %s. "
                 "Mỗi nút ghi thời điểm inject, kỳ vọng, giá trị trước/sau đọc từ API và tham chiếu log.</div>"
                 % (short_id(self.run_id, 8), esc(self.mode.upper() or "?"), esc(self.route_id)))
        if not self.supports_source_clock:
            intro += _note("info", "Chế độ LIVE", esc("Duplicate/late/advance-clock/pause chỉ áp dụng cho replay và "
                                                      "synthetic (đồng hồ nguồn); chỉ restart processor khả dụng."))
        if widgets is None or display is None:
            show_html(intro + self.summary_html() + _note(
                "info", "Không có ipywidgets",
                esc("Gọi lab.run('duplicate_snapshot' | 'late_snapshot' | 'advance_clock' | 'restart_processor') trong một cell.")))
            return self
        W = widgets
        buttons = []
        for spec in FAULT_SCENARIOS:
            supported = (not spec["source_clock_only"]) or self.supports_source_clock
            b = W.Button(description=spec["button"], button_style="warning", disabled=not supported,
                         tooltip=spec["expected"][:250], layout=W.Layout(width="auto"))
            b.on_click(self._handler(spec["id"]))
            self._buttons.append((b, supported))
            buttons.append(b)
        controls = []
        for action, label in (("pause", "Tạm dừng (pause)"), ("resume", "Tiếp tục (resume)")):
            b = W.Button(description=label, disabled=not self.supports_source_clock, layout=W.Layout(width="auto"))
            b.on_click(self._handler(action))
            self._buttons.append((b, self.supports_source_clock))
            controls.append(b)
        self._advance = W.BoundedIntText(value=160, min=1, max=3600, description="Số giây tiến đồng hồ",
                                         style={"description_width": "initial"}, layout=W.Layout(width="240px"))
        self._summary = W.HTML(value=_wrap(self.summary_html()))
        self._out = W.Output(layout=W.Layout(border="1px solid #d0d7de", max_height="520px", overflow="auto"))
        root = W.VBox([
            W.HTML(value=_wrap(intro)),
            W.HBox(buttons[:2] + controls, layout=W.Layout(flex_flow="row wrap")),
            W.HBox([buttons[2], self._advance, buttons[3]], layout=W.Layout(flex_flow="row wrap")),
            self._summary,
            self._out,
        ])
        self._widgets = [root, self._summary, self._out, self._advance] + buttons + controls
        _enable_colab_widgets()
        display(root)
        return self

    def _save_initial(self) -> None:
        if not self.results_path.exists():
            self._save(dict(self.results[FAULT_SCENARIOS[0]["id"]]))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for widget in reversed(self._widgets):
            try:
                widget.close()
            except Exception:  # noqa: BLE001
                pass
        self._widgets = []
        _deactivate("faults", self)


# ================================================================================================
# Public link through a Cloudflare quick tunnel (cell 14) — demo.py expose start|status|stop
# ================================================================================================
TUNNEL_TARGETS = ("dashboard", "api", "both")
TUNNEL_SECURITY_NOTE = (
    "Link công khai, chỉ đọc, không có đăng nhập: bất kỳ ai có link đều xem được dashboard (và API nếu "
    "TUNNEL_TARGET là api/both) chừng nào session còn chạy. Chỉ chia sẻ khi chấp nhận công khai; tắt bằng nút "
    "'Tắt tunnel' hoặc 'Dừng tất cả' ở cell 18 (hoặc demo.py expose stop --target both). Link không được ghi vào "
    "report."
)


def _safe_public_url(value: Any) -> Optional[str]:
    """Return ``value`` when it is an https URL without credentials (safe to render as a link), else None."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        parts = urllib.parse.urlsplit(text)
    except ValueError:
        return None
    if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
        return None
    return text


def _public_link(url: Optional[str], size: int = 20) -> str:
    return ('<a href="%s" target="_blank" rel="noopener noreferrer" style="font-size:%dpx;font-weight:700;'
            'color:#0969da;word-break:break-all">%s</a>' % (esc(url), size, esc(url)))


def render_tunnel(doc: Any, *, error: Optional[str] = None, error_code: Optional[str] = None) -> str:
    """Prominent clickable public link(s) + local URL + reachability + security note (all escaped).

    Contract (demo.py expose start|status): {ok, action, op, target, reused, dashboard:{state, local_url, pid, log,
    reused, alive}, tunnels:[{target, component, public_url, public_api_base, reachable, state, log, origin, reused,
    decision}], warnings, public_urls_file, security_note, error}. An unreachable URL is a warning, not a failure.
    """
    parts = ["<h4>Link công khai (Cloudflare quick tunnel)</h4>"]
    if error_code == "TUNNEL_DISABLED":
        parts.append(_note("info", "Tunnel đã bị tắt khi bootstrap (--no-tunnel)",
                           esc("Đặt ENABLE_CLOUDFLARE_TUNNEL = True ở cell 2, chạy lại cell 4 (bootstrap) rồi cell 14. "
                               "UI notebook ở trên vẫn dùng được.")))
    elif error is not None:
        parts.append(_note("err", "Không mở được tunnel — Run All vẫn tiếp tục; UI notebook ở trên vẫn dùng được",
                           esc(error)))
    if isinstance(doc, dict):
        dash = doc.get("dashboard") if isinstance(doc.get("dashboard"), dict) else {}
        if dash:
            alive = "" if dash.get("alive") is not False else " " + status_badge("STOPPED")
            parts.append("<div>Dashboard trong VM: <code>%s</code> · trạng thái %s%s%s</div>"
                         % (esc(dash.get("local_url")), status_badge(dash.get("state")), alive,
                            " · tái sử dụng" if dash.get("reused") else ""))
        tunnels = [t for t in (doc.get("tunnels") or []) if isinstance(t, dict)]
        if not tunnels and error is None:
            parts.append(_note("warn", "CLI không trả về tunnel nào", esc("Xem log của demo.py expose.")))
        pending = False
        for t in tunnels:
            url = _safe_public_url(t.get("public_url"))
            link = _public_link(url) if url else \
                '<span style="font-size:15px">%s</span>' % esc(t.get("public_url") or "chưa có URL công khai")
            api_base = _safe_public_url(t.get("public_api_base"))
            api_html = ("<div>API công khai (chỉ đọc): %s</div>" % _public_link(api_base, 15)) if api_base else ""
            reachable = t.get("reachable")
            reach = "REACHABLE" if reachable is True else ("NOT_REACHABLE_YET" if reachable is False else "UNKNOWN")
            pending = pending or reachable is False
            meta = "target %s · %s %s%s%s" % (
                esc(t.get("target")), status_badge(t.get("state")), status_badge(reach),
                " · tái sử dụng link cũ" if t.get("reused") else "",
                (" · %s" % esc(t.get("decision"))) if t.get("decision") else "")
            parts.append('<div class="tp-card"><div class="tp-muted">%s</div>%s%s'
                         '<div class="tp-muted">origin %s · log: %s</div></div>'
                         % (meta, link, api_html, esc(t.get("origin")), esc(t.get("log"))))
        if pending:
            parts.append(_note("info", "Link chưa truy cập được (cảnh báo, không phải lỗi)",
                               esc("DNS của trycloudflare.com có thể mất tới khoảng 1 phút sau khi tạo. "
                                   "Đợi một lát rồi bấm 'Kiểm tra lại link'.")))
        warn_items = doc.get("warnings") or []
        if warn_items:
            items = "".join("<li>%s</li>" % esc(w if isinstance(w, str) else json.dumps(w, ensure_ascii=False, default=str))
                            for w in warn_items)
            parts.append("<div><b>Cảnh báo:</b><ul>%s</ul></div>" % items)
        if doc.get("security_note"):
            parts.append(_note("warn", "Lưu ý bảo mật (từ demo.py expose)", _multiline(doc.get("security_note"))))
        if doc.get("public_urls_file"):
            parts.append('<div class="tp-muted">Danh sách link hiện hành: <code>%s</code></div>'
                         % esc(doc.get("public_urls_file")))
    parts.append(_note("warn", "Bảo mật", esc(TUNNEL_SECURITY_NOTE)))
    return "".join(parts)


class TunnelPanel(object):
    """Opens/re-checks the public link with ``demo.py expose``; never raises (the notebook UI keeps working)."""

    def __init__(self, runtime: Dict[str, Any], target: str = "dashboard") -> None:
        if target not in TUNNEL_TARGETS:
            raise CellFailed("INVALID_TUNNEL_TARGET", "TUNNEL_TARGET phải là dashboard | api | both (đang là %r)." % target)
        self.runtime = runtime
        self.target = target
        self.last: Optional[Dict[str, Any]] = None
        self._closed = False
        self._busy = False
        self._widgets: List[Any] = []
        self._view: Any = None
        self._out: Any = None

    def _call(self, action: str, *extra: str,
              timeout: float = 300.0) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[str]]:
        """Run ``demo.py expose <action>``; returns (json, error_text, error_code) and never raises."""
        args = ["expose", action] + list(extra)
        try:
            doc = run_cli(self.runtime, "demo.py", *args, timeout=timeout, label="demo.py " + " ".join(args))
            return doc, None, None
        except CellFailed as exc:
            text = "[%s] %s" % (exc.code, exc.message)
            if exc.hint:
                text += " — Gợi ý: %s" % exc.hint
            return exc.result, text, exc.code
        except Exception as exc:  # noqa: BLE001 - a tunnel problem must never stop Run All
            return None, "%s: %s" % (type(exc).__name__, exc), "EXCEPTION"

    def _record(self, doc: Optional[Dict[str, Any]], error: Optional[str], code: Optional[str], action: str) -> str:
        tunnels = [t for t in ((doc or {}).get("tunnels") or []) if isinstance(t, dict)]
        has_url = any(_safe_public_url(t.get("public_url")) for t in tunnels)
        reachable = [t.get("reachable") for t in tunnels]
        if code == "TUNNEL_DISABLED":
            status = "NOT_RUN"  # bootstrap ran with --no-tunnel
        elif error is not None or not has_url:
            status = "FAIL"
        elif reachable and all(r is True for r in reachable):
            status = "PASS"
        else:
            status = "WARN"  # unreachable yet (DNS): a warning, not a failure
        try:  # the public URL itself is a capability link: never written to reports
            record_step(self.runtime["workdir"], "tunnel", status, action=action, target=self.target,
                        tunnel_count=len(tunnels), has_public_url=has_url, reachable=reachable,
                        reused=[t.get("reused") for t in tunnels], error=error, error_code=code,
                        public_url_recorded=False)
        except Exception:  # noqa: BLE001
            pass
        return status

    def _render(self, fragment: str) -> None:
        if self._view is not None:
            self._view.value = _wrap(fragment)
            return
        if widgets is None or display is None:
            show_html(fragment + _note("info", "Kiểm tra lại", esc("Gọi tunnel.status() trong một cell để kiểm tra lại link.")))
            return
        W = widgets
        self._view = W.HTML(value=_wrap(fragment))
        recheck = W.Button(description="Kiểm tra lại link", button_style="info", layout=W.Layout(width="auto"),
                           tooltip="demo.py expose status")
        recheck.on_click(self._on_recheck)
        self._out = W.Output(layout=W.Layout(max_height="240px", overflow="auto"))
        root = W.VBox([self._view, recheck, self._out])
        self._widgets = [root, self._view, recheck, self._out]
        _enable_colab_widgets()
        display(root)

    def start(self, ready_timeout: int = 120) -> Optional[Dict[str, Any]]:
        """``demo.py expose start --target T --ready-timeout N``; shows the link or the error, never raises."""
        _activate("tunnel", self)
        wait = max(10, int(ready_timeout))
        doc, error, code = self._call("start", "--target", self.target, "--ready-timeout", str(wait),
                                      timeout=self.cli_timeout(wait))
        self.last = doc
        self._record(doc, error, code, "start")
        self._render(render_tunnel(doc, error=error, error_code=code))
        return doc

    @property
    def n_targets(self) -> int:
        return 2 if self.target == "both" else 1

    def cli_timeout(self, ready_timeout: int) -> float:
        """Bound for the ``expose start`` CLI: 2*ready_timeout + n_targets*150 + 60 s (CLI worst case + margin)."""
        return float(2 * int(ready_timeout) + self.n_targets * 150 + 60)

    def status(self) -> Optional[Dict[str, Any]]:
        """``demo.py expose status --target T`` (re-check reachability; same JSON shape); never raises."""
        doc, error, code = self._call("status", "--target", self.target, timeout=float(self.n_targets * 90 + 60))
        if doc is not None:
            self.last = doc
        self._record(doc, error, code, "status")
        self._render(render_tunnel(doc, error=error, error_code=code))
        return doc

    def _on_recheck(self, _button: Any) -> None:
        if self._busy or self._closed:
            return
        self._busy = True
        try:
            with self._out:
                self.status()
        finally:
            self._busy = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for widget in reversed(self._widgets):
            try:
                widget.close()
            except Exception:  # noqa: BLE001
                pass
        self._widgets = []
        self._view = None
        _deactivate("tunnel", self)


# ================================================================================================
# Stop panel (cell 18)
# ================================================================================================
class StopPanel(object):
    """Buttons: demo.py stop (session only; API, dashboard and tunnel stay up), demo.py stop --all (everything,
    incl. Kafka, Cassandra, dashboard and tunnel) and demo.py expose stop --target both (all public access
    only). Data is kept."""

    def __init__(self, runtime: Dict[str, Any]) -> None:
        self.runtime = runtime
        self._closed = False
        self._busy = False
        self._widgets: List[Any] = []
        self._out: Any = None

    def stop(self, all_components: bool = False) -> Dict[str, Any]:
        ui = active("ui")
        if ui is not None:
            try:
                ui.stop_auto_refresh()
            except Exception:  # noqa: BLE001
                pass
        args = ["stop", "--all"] if all_components else ["stop"]
        doc = run_cli(self.runtime, "demo.py", *args, timeout=900, label="demo.py " + " ".join(args),
                      show=lambda d: show_cli_result(d, "demo.py " + " ".join(args), ("data_preserved",)))
        record_step(self.runtime["workdir"], "stop", "PASS", all=all_components,
                    data_preserved=doc.get("data_preserved"))
        if all_components:  # stop --all also stops the dashboard and the tunnel (WP7 contract)
            record_step(self.runtime["workdir"], "tunnel_stop", "PASS", via="demo.py stop --all")
            tunnel = active("tunnel")
            if tunnel is not None:
                try:
                    tunnel.close()
                except Exception:  # noqa: BLE001
                    pass
        return doc

    def stop_tunnel(self) -> Optional[Dict[str, Any]]:
        """Close every public access component (demo.py expose stop --target both: dashboard AND API tunnels);
        the session and the notebook UI keep running. Failures are recorded and shown in red."""
        doc, error, _code = expose_stop(self.runtime, target="both", show=True)
        if error is None:
            print("Đã đóng mọi link công khai (dashboard và API).")
        return doc

    def _handler(self, action: str) -> Callable:
        def _run(_button: Any) -> None:
            if self._busy or self._closed:
                return
            self._busy = True
            try:
                with self._out:
                    if action == "tunnel":
                        self.stop_tunnel()
                    else:
                        self.stop(action == "all")
            except Exception as exc:  # noqa: BLE001
                with self._out:
                    print("Lỗi: %s" % exc)
            finally:
                self._busy = False
        return _run

    def show(self) -> "StopPanel":
        _activate("stop", self)
        intro = ("<h4>Dừng demo</h4><div class='tp-muted'>'Dừng session' (demo.py stop): dừng publisher → chờ "
                 "processor xử lý hết backlog → dừng processor; API, dashboard và tunnel vẫn chạy (link công khai "
                 "vẫn mở, hiển thị dữ liệu cuối). 'Tắt tunnel' (demo.py expose stop --target both): chỉ đóng mọi thành phần truy "
                 "cập công khai. 'Dừng tất cả' (demo.py stop --all): dừng mọi thứ kể cả Kafka, Cassandra, API, "
                 "dashboard và tunnel. Không xóa dữ liệu, checkpoint hay inputs; kernel không bị dừng.</div>")
        if widgets is None or display is None:
            show_html(intro + _note("info", "Không có ipywidgets",
                                    esc("Gọi panel.stop(), panel.stop(True) hoặc panel.stop_tunnel() trong một cell.")))
            return self
        W = widgets
        b_session = W.Button(description="Dừng session (giữ Kafka/Cassandra)", button_style="warning",
                             layout=W.Layout(width="auto"))
        b_tunnel = W.Button(description="Tắt tunnel", button_style="info", layout=W.Layout(width="auto"),
                            tooltip="demo.py expose stop --target both — đóng mọi link công khai (dashboard và API)")
        b_all = W.Button(description="Dừng tất cả", button_style="danger", layout=W.Layout(width="auto"))
        b_session.on_click(self._handler("session"))
        b_tunnel.on_click(self._handler("tunnel"))
        b_all.on_click(self._handler("all"))
        self._out = W.Output(layout=W.Layout(border="1px solid #d0d7de", max_height="420px", overflow="auto"))
        root = W.VBox([W.HTML(value=_wrap(intro)), W.HBox([b_session, b_tunnel, b_all]), self._out])
        self._widgets = [root, b_session, b_tunnel, b_all, self._out]
        _enable_colab_widgets()
        display(root)
        return self

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for widget in reversed(self._widgets):
            try:
                widget.close()
            except Exception:  # noqa: BLE001
                pass
        self._widgets = []
        _deactivate("stop", self)
