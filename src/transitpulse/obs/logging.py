"""Structured JSON logging (one JSON object per line on stdout; the supervisor captures it to a file).

Schema: ts, level, component, run_id, session_id, feed, snapshot_id, capture_id, batch_id, stage,
status (ok|retry|failed|rejected|skipped|reused), duration_ms, reason, msg, pid (+ extra fields).
Bodies and secrets are never logged; excerpts are truncated by :func:`excerpt`.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

_STD_FIELDS = ("feed", "snapshot_id", "capture_id", "batch_id", "stage", "status", "duration_ms", "reason")
_CONTEXT: dict[str, Any] = {"component": "transitpulse", "run_id": None, "session_id": None}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "component": _CONTEXT["component"],
            "run_id": _CONTEXT["run_id"],
            "session_id": _CONTEXT["session_id"],
        }
        for f in _STD_FIELDS:
            out[f] = getattr(record, f, None)
        out["msg"] = record.getMessage()
        out["pid"] = os.getpid()
        extra = getattr(record, "extra_fields", None)
        if isinstance(extra, dict):
            for k, v in extra.items():
                out.setdefault(k, v)
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)[-4000:]
        return json.dumps(out, default=str, ensure_ascii=False)


def configure(component: str, *, run_id: str | None = None, session_id: str | None = None, level: str = "INFO") -> None:
    """Configure root logging once per process. Idempotent."""
    _CONTEXT.update(component=component, run_id=run_id, session_id=session_id)
    root = logging.getLogger()
    for h in list(root.handlers):
        if getattr(h, "_transitpulse", False):
            root.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler._transitpulse = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(level)
    for noisy in ("py4j", "cassandra", "urllib3", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def set_context(**fields: Any) -> None:
    _CONTEXT.update({k: v for k, v in fields.items() if k in _CONTEXT})


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log_event(
    logger: logging.Logger,
    stage: str,
    status: str,
    msg: str = "",
    *,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    """Log a stage event with the standard fields; unknown keyword fields go to the JSON as extras."""
    std = {k: fields.pop(k) for k in list(fields) if k in _STD_FIELDS}
    std["stage"], std["status"] = stage, status
    logger.log(level, msg or f"{stage}:{status}", extra={**std, "extra_fields": fields})


@contextmanager
def timed(logger: logging.Logger, stage: str, **fields: Any):
    """Context manager logging duration_ms with a monotonic timer; status=failed on exception."""
    t0 = time.monotonic()
    try:
        yield
    except Exception as exc:
        log_event(logger, stage, "failed", str(exc)[:500], level=logging.ERROR,
                  duration_ms=round((time.monotonic() - t0) * 1000, 3), **fields)
        raise
    log_event(logger, stage, "ok", duration_ms=round((time.monotonic() - t0) * 1000, 3), **fields)


def excerpt(text: str | bytes | None, limit: int = 512) -> str | None:
    if text is None:
        return None
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    return text if len(text) <= limit else text[:limit] + "…"
