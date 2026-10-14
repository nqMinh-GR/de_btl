"""Pure-ASGI request middleware: request id, no-store caching header, metrics and access log.

* Accepts a client ``X-Request-ID`` matching ``^[\\w.-]{1,64}$``, otherwise generates ``req-<12 hex>``;
  the id is stored in ``scope["state"]["request_id"]`` (read by handlers and error envelopes) and
  echoed in the response header.
* Adds ``Cache-Control: no-store``: responses depend on the moving application clock.
* Records ``tp_api_requests_total`` / ``tp_api_request_duration_seconds`` labelled with the matched
  route template (``unmatched`` otherwise, so arbitrary paths cannot explode label cardinality).
"""

from __future__ import annotations

import logging
import re
import time

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from transitpulse.api.errors import REQUEST_ID_HEADER, new_request_id
from transitpulse.api.metrics import ApiMetrics, route_label
from transitpulse.obs.logging import get_logger, log_event

_LOG = get_logger("transitpulse.api.access")
_REQUEST_ID_RE = re.compile(r"^[\w.-]{1,64}$", re.ASCII)


class RequestContextMiddleware:
    """Outermost application middleware (inside Starlette's ServerErrorMiddleware)."""

    def __init__(self, app: ASGIApp, metrics: ApiMetrics, base_path: str = "") -> None:
        self.app = app
        self.metrics = metrics
        self.base_path = base_path

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        supplied = Headers(scope=scope).get(REQUEST_ID_HEADER)
        request_id = supplied if supplied and _REQUEST_ID_RE.match(supplied) else new_request_id()
        state = scope.setdefault("state", {})
        state["request_id"] = request_id
        started = time.monotonic()
        status_holder = {"status": 500}

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                status_holder["status"] = int(message["status"])
                message.setdefault("headers", [])
                headers = MutableHeaders(scope=message)
                headers[REQUEST_ID_HEADER] = request_id
                if "cache-control" not in headers:
                    headers["Cache-Control"] = "no-store"
            await send(message)

        try:
            await self.app(scope, receive, send_with_headers)
        finally:
            elapsed = time.monotonic() - started
            route = route_label(scope.get("route"), self.base_path)
            status = status_holder["status"]
            self.metrics.observe_request(route, status, elapsed)
            log_event(
                _LOG,
                "request",
                "ok" if status < 500 else "failed",
                f"{scope.get('method', '?')} {route} -> {status}",
                level=logging.INFO if status < 500 else logging.WARNING,
                duration_ms=round(elapsed * 1000, 3),
                request_id=request_id,
                http_status=status,
                path=scope.get("path"),
            )
