"""HTTP client of the Streamlit dashboard for the TransitPulse FastAPI service.

Every call returns an :class:`ApiResult` whose ``kind`` is one of
``ok | empty | stale | expired | unavailable | not_found | invalid | unreachable``; each kind has its own UI
state (``components/states.py``). The client never zero-fills a failed response, never computes a KPI and
never talks to Cassandra, Kafka, raw files or MBTA. This module does not import streamlit: the cached
instance lives in :func:`state.get_client` (``st.cache_resource``).
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

import httpx

DEFAULT_BASE_URL = "http://127.0.0.1:8000/api/v1"
KINDS = ("ok", "empty", "stale", "expired", "unavailable", "not_found", "invalid", "unreachable")
USABLE_KINDS = frozenset({"ok", "stale", "empty"})


def seg(value: Any) -> str:
    """URL-quote one path segment (IDs may contain any character)."""
    return quote(str(value), safe="")


@dataclass
class ApiResult:
    """Outcome of one API call; ``data`` is ``None`` for every failed call (never zeros)."""

    kind: str
    path: str
    http_status: int | None = None
    data: Any = None
    meta: dict[str, Any] = field(default_factory=dict)
    warnings: list[dict[str, Any]] = field(default_factory=list)
    error: dict[str, Any] | None = None
    elapsed_ms: float | None = None

    @property
    def usable(self) -> bool:
        return self.kind in USABLE_KINDS

    def feed(self, feed_type: str) -> dict[str, Any]:
        feeds = self.meta.get("feeds") if isinstance(self.meta, dict) else None
        block = feeds.get(feed_type) if isinstance(feeds, dict) else None
        return dict(block) if isinstance(block, dict) else {}

    def rows(self, key: str) -> list[dict[str, Any]]:
        data = self.data
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict) and isinstance(data.get(key), list):
            rows = data[key]
        else:
            rows = []
        return [r for r in rows if isinstance(r, dict)]

    @property
    def source_type(self) -> str | None:
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
    def reason(self) -> str | None:
        if isinstance(self.data, dict) and self.data.get("reason"):
            detail = self.data.get("reason_detail")
            return f"{self.data['reason']} — {detail}" if detail else str(self.data["reason"])
        for w in self.warnings:
            if w.get("code") in ("EMPTY_SNAPSHOT", "NO_PREDICTION_DATA"):
                return f"{w.get('code')} — {w.get('message') or ''}"
        return None

    @property
    def error_code(self) -> str | None:
        return str(self.error["code"]) if self.error and self.error.get("code") else None

    @property
    def error_message(self) -> str | None:
        return str(self.error["message"]) if self.error and self.error.get("message") else None

    @property
    def request_id(self) -> str | None:
        if self.error and self.error.get("request_id"):
            return str(self.error["request_id"])
        value = self.meta.get("request_id") if isinstance(self.meta, dict) else None
        return str(value) if value else None


def classify_response(
    status: int,
    body: bytes,
    *,
    path: str = "",
    primary_feed: str | None = None,
    list_key: str | None = None,
    elapsed_ms: float | None = None,
) -> ApiResult:
    """Map an HTTP status + body onto an :class:`ApiResult` (pure; same rules as notebooks/colab_ui.py)."""
    try:
        doc = json.loads(body.decode("utf-8")) if body else None
    except (ValueError, UnicodeDecodeError):
        doc = None
    if not 200 <= status < 300:
        err = doc.get("error") if isinstance(doc, dict) and isinstance(doc.get("error"), dict) else None
        if err is None:
            err = {"code": f"http_{status}", "message": body[:300].decode("utf-8", "replace") if body else ""}
        meta = doc.get("meta") if isinstance(doc, dict) and isinstance(doc.get("meta"), dict) else {}
        code = str(err.get("code") or "")
        if status == 404:
            kind = "not_found"
        elif status in (400, 422):
            kind = "invalid"
        elif status == 503 and code == "generation_expired":
            kind = "expired"
        else:
            kind = "unavailable"
        return ApiResult(kind, path, status, None, meta, [], err, elapsed_ms)
    if not isinstance(doc, dict) or "data" not in doc:
        err = {"code": "bad_response", "message": "Phản hồi không có dạng {data, meta, warnings}."}
        return ApiResult("unavailable", path, status, None, {}, [], err, elapsed_ms)
    meta = doc.get("meta") if isinstance(doc.get("meta"), dict) else {}
    warnings = [w for w in (doc.get("warnings") or []) if isinstance(w, dict)]
    result = ApiResult("ok", path, status, doc.get("data"), meta, warnings, None, elapsed_ms)
    data = result.data
    rows = data if isinstance(data, list) else (data.get(list_key) if list_key and isinstance(data, dict) else None)
    feed_state = result.feed(primary_feed).get("state") if primary_feed else None
    if (isinstance(rows, list) and not rows) or feed_state == "empty":
        result.kind = "empty"
    elif primary_feed:
        block = result.feed(primary_feed)
        if block.get("stale") is True or feed_state == "stale" or any(
            w.get("code") == "FEED_STALE" and w.get("feed_type") in (None, primary_feed) for w in warnings
        ):
            result.kind = "stale"
    elif result.stale:
        result.kind = "stale"
    return result


class ApiClient:
    """Synchronous httpx client (GET only, no proxies, 5 s read / 2 s connect timeouts)."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        timeout: float = 5.0,
        connect_timeout: float = 2.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = (base_url or DEFAULT_BASE_URL).rstrip("/")
        self._client = httpx.Client(
            timeout=httpx.Timeout(timeout, connect=connect_timeout),
            headers={"Accept": "application/json"},
            transport=transport,
            trust_env=False,
        )

    def close(self) -> None:
        self._client.close()

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        primary_feed: str | None = None,
        list_key: str | None = None,
    ) -> ApiResult:
        query = {k: v for k, v in (params or {}).items() if v is not None and v != ""}
        url = self.base_url + "/" + path.lstrip("/")
        started = time.monotonic()
        try:
            resp = self._client.get(url, params=query, headers={"X-Request-ID": "ui-" + uuid.uuid4().hex[:12]})
        except httpx.HTTPError as exc:
            err = {"code": "api_unreachable", "message": f"{type(exc).__name__}: {exc}"}
            return ApiResult("unreachable", path, None, None, {}, [], err, (time.monotonic() - started) * 1000.0)
        return classify_response(
            resp.status_code,
            resp.content,
            path=path,
            primary_feed=primary_feed,
            list_key=list_key,
            elapsed_ms=(time.monotonic() - started) * 1000.0,
        )

    # ------------------------------------------------------------------ endpoint helpers (§8 contract)
    def health_ready(self) -> ApiResult:
        return self.get("/health/ready")

    def runs(self, limit: int = 50) -> ApiResult:
        return self.get("/runs", {"limit": limit}, list_key="runs")

    def run(self, run_id: str) -> ApiResult:
        return self.get(f"/runs/{seg(run_id)}")

    def routes(self, run_id: str) -> ApiResult:
        return self.get("/routes", {"run_id": run_id}, list_key="routes")

    def geometry(self, run_id: str, route_id: str) -> ApiResult:
        return self.get(f"/routes/{seg(route_id)}/geometry", {"run_id": run_id}, list_key="shapes")

    def route_stops(self, run_id: str, route_id: str) -> ApiResult:
        return self.get(f"/routes/{seg(route_id)}/stops", {"run_id": run_id}, list_key="stops")

    def vehicles(self, run_id: str, route_id: str | None = None) -> ApiResult:
        return self.get("/vehicles", {"run_id": run_id, "route_id": route_id},
                        primary_feed="vehicle_positions", list_key="vehicles")

    def vehicle_history(
        self,
        run_id: str,
        vehicle_id: str,
        *,
        start: str | None = None,
        end: str | None = None,
        page_size: int = 100,
        page_token: str | None = None,
    ) -> ApiResult:
        params = {"run_id": run_id, "from": start, "to": end, "page_size": page_size, "page_token": page_token}
        return self.get(f"/vehicles/{seg(vehicle_id)}/history", params, list_key="observations")

    def predictions(
        self,
        run_id: str,
        stop_id: str,
        *,
        route_id: str | None = None,
        direction_id: int | None = None,
        include: str = "displayable",
        limit: int = 50,
    ) -> ApiResult:
        params = {"run_id": run_id, "route_id": route_id, "direction_id": direction_id, "include": include,
                  "limit": limit}
        return self.get(f"/stops/{seg(stop_id)}/predictions", params, primary_feed="trip_updates",
                        list_key="predictions")

    def route_metrics(self, run_id: str, route_id: str) -> ApiResult:
        return self.get(f"/routes/{seg(route_id)}/metrics", {"run_id": run_id})

    def alerts(self, run_id: str, route_id: str | None = None) -> ApiResult:
        return self.get("/alerts", {"run_id": run_id, "route_id": route_id}, primary_feed="alerts", list_key="alerts")

    def quality(self, run_id: str) -> ApiResult:
        return self.get("/quality", {"run_id": run_id})

    def pipeline_status(self, run_id: str) -> ApiResult:
        return self.get("/pipeline/status", {"run_id": run_id})
