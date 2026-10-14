"""Structured errors: ``{"error": {code, message, request_id, retryable, details, hint}, "meta": {...}}``.

HTTP status by code (design E §4.3, PDF p20):

* 404 ``run_not_found``, ``route_not_in_scope``, ``stop_not_in_scope``, ``trip_not_found``, ``not_found``
* 422 ``validation_error``, ``history_range_too_large``, ``invalid_page_token``, ``route_id_required``,
  ``naive_datetime``
* 503 ``database_unavailable``, ``database_timeout``, ``schema_not_ready``, ``generation_expired``,
  ``generation_unavailable``, ``generation_inconsistent``, ``run_clock_unavailable``

Every handler returns the same envelope, echoes ``X-Request-ID`` and never leaks stack traces.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from transitpulse.api.models import ErrorBody, ErrorEnvelope, Meta
from transitpulse.api.repository import RepositoryError
from transitpulse.contracts.envelope import utcnow
from transitpulse.obs.logging import get_logger, log_event

_LOG = get_logger("transitpulse.api.errors")
REQUEST_ID_HEADER = "X-Request-ID"
RETRY_AFTER_SECONDS = 2


class ApiError(Exception):
    """An expected, client-visible failure rendered as the error envelope."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        details: list[dict[str, Any]] | None = None,
        hint: str | None = None,
        meta: Meta | None = None,
    ) -> None:
        super().__init__(f"{code}: {message}")
        self.status_code = status_code
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = details
        self.hint = hint
        self.meta = meta


# ------------------------------------------------------------------------------------------ helpers
def new_request_id() -> str:
    return "req-" + uuid.uuid4().hex[:12]


def request_id_of(request: Request) -> str:
    """Request id set by the request-context middleware (generated if the middleware did not run)."""
    rid = getattr(request.state, "request_id", None)
    if isinstance(rid, str) and rid:
        return rid
    rid = new_request_id()
    request.state.request_id = rid
    return rid


def _wall_now(request: Request) -> Any:
    services = getattr(request.app.state, "services", None)
    if services is not None:
        return services.wall_now()
    return utcnow()


def minimal_meta(request: Request) -> Meta:
    """Meta for errors raised before any run context exists."""
    return Meta(request_id=request_id_of(request), served_at=_wall_now(request))


def error_response(
    request: Request,
    status_code: int,
    code: str,
    message: str,
    *,
    retryable: bool = False,
    details: list[dict[str, Any]] | None = None,
    hint: str | None = None,
    meta: Meta | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    """Render the error envelope (the only non-2xx body shape of the API)."""
    rid = request_id_of(request)
    body = ErrorEnvelope(
        error=ErrorBody(code=code, message=message, request_id=rid, retryable=retryable, details=details, hint=hint),
        meta=meta if meta is not None else minimal_meta(request),
    )
    out_headers = {REQUEST_ID_HEADER: rid, "Cache-Control": "no-store"}
    if retryable and status_code == 503:
        out_headers["Retry-After"] = str(RETRY_AFTER_SECONDS)
    if headers:
        out_headers.update(headers)
    return JSONResponse(status_code=status_code, content=body.model_dump(mode="json"), headers=out_headers)


# ------------------------------------------------------------------------------------------ handlers
async def _api_error_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, ApiError)
    return error_response(
        request,
        exc.status_code,
        exc.code,
        exc.message,
        retryable=exc.retryable,
        details=exc.details,
        hint=exc.hint,
        meta=exc.meta,
    )


async def _repository_error_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, RepositoryError)
    services = getattr(request.app.state, "services", None)
    if services is not None:
        services.metrics.db_errors.labels(kind=exc.code).inc()
    log_event(_LOG, "request", "failed", exc.message, level=logging.WARNING, reason=exc.code,
              request_id=request_id_of(request))
    return error_response(
        request,
        503,
        exc.code,
        exc.message,
        retryable=True,
        hint=exc.hint,
    )


def _validation_details(exc: RequestValidationError) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for err in exc.errors():
        out.append(
            {
                "loc": [str(part) for part in err.get("loc", ())],
                "msg": str(err.get("msg", "")),
                "type": str(err.get("type", "")),
            }
        )
    return out


async def _validation_error_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)
    return error_response(
        request,
        422,
        "validation_error",
        "request parameters are invalid",
        retryable=False,
        details=_validation_details(exc),
        hint="fix the parameters listed in details[].loc and retry",
    )


_HTTP_CODES = {404: "not_found", 405: "method_not_allowed", 406: "not_acceptable"}


async def _http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, StarletteHTTPException)
    code = _HTTP_CODES.get(exc.status_code, "http_error")
    message = exc.detail if isinstance(exc.detail, str) and exc.detail else code.replace("_", " ")
    return error_response(request, exc.status_code, code, message, retryable=False, headers=exc.headers)


async def _unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
    rid = request_id_of(request)
    _LOG.error(
        "unhandled error",
        exc_info=exc,
        extra={"stage": "request", "status": "failed", "extra_fields": {"request_id": rid}},
    )
    return error_response(request, 500, "internal_error", "internal server error", retryable=False)


def install_error_handlers(app: FastAPI) -> None:
    """Register every handler so that all non-2xx responses share the error envelope."""
    app.add_exception_handler(ApiError, _api_error_handler)
    app.add_exception_handler(RepositoryError, _repository_error_handler)
    app.add_exception_handler(RequestValidationError, _validation_error_handler)
    app.add_exception_handler(StarletteHTTPException, _http_exception_handler)
    app.add_exception_handler(Exception, _unhandled_error_handler)
