"""API Prometheus metrics (design E §6, D-E23).

Registry ``tp_api_*``:
* ``tp_api_requests_total{route,status}`` — route is the matched path template (bounded cardinality);
* ``tp_api_request_duration_seconds{route}`` (histogram);
* ``tp_api_db_errors_total{kind}`` — ``database_unavailable`` / ``database_timeout`` / ``schema_not_ready``;
* ``tp_api_cache_hits_total`` / ``tp_api_cache_misses_total``;
* ``tp_api_feed_state_total{endpoint,feed,state}`` — data availability seen by clients.

``GET /api/v1/metrics`` returns the live API registry followed by every other component's exported
textfile (``<W>/metrics/*.prom``, read with :func:`transitpulse.obs.metrics.read_textfiles`); the API's
own textfile is skipped there so no metric family appears twice.
"""

from __future__ import annotations

import re
from pathlib import Path

from prometheus_client import generate_latest

from transitpulse.obs.metrics import ComponentMetrics, read_textfiles

COMPONENT = "api"
OWN_PREFIX = "tp_api_"
_DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
_NAME_SPLIT = re.compile(r"[{\s]")


UNMATCHED_ROUTE = "unmatched"


def route_label(route: object, base_path: str) -> str:
    """Full path template of a matched route (e.g. ``/api/v1/vehicles``), used as a metric label.

    Since FastAPI 0.137 ``route.path`` of an included router is router-relative (no mount prefix), so the
    configured base path is prepended unless the template already carries it. Unmatched requests get one
    fixed label, so arbitrary paths cannot inflate label cardinality.
    """
    path = getattr(route, "path", None)
    if not isinstance(path, str) or not path:
        return UNMATCHED_ROUTE
    if base_path and path != base_path and not path.startswith(base_path + "/"):
        return base_path + path
    return path


def _metric_name(line: str) -> str | None:
    """Metric family/sample name of one exposition line (``None`` for other comments/blank lines)."""
    if line.startswith("# HELP ") or line.startswith("# TYPE "):
        parts = line.split(" ", 3)
        return parts[2] if len(parts) >= 3 else None
    if not line or line.startswith("#"):
        return None
    return _NAME_SPLIT.split(line, maxsplit=1)[0]


class ApiMetrics:
    """The API process registry (one per :func:`~transitpulse.api.app.create_app` call)."""

    def __init__(self) -> None:
        self.component = ComponentMetrics(COMPONENT)
        c = self.component
        self.requests = c.counter("requests_total", "HTTP requests by route template and status code", ("route", "status"))
        self.request_duration = c.histogram(
            "request_duration_seconds", "HTTP request duration by route template", ("route",), buckets=_DURATION_BUCKETS
        )
        self.db_errors = c.counter("db_errors_total", "Serving-store errors by kind", ("kind",))
        self.cache_hits = c.counter("cache_hits_total", "Immutable cache hits")
        self.cache_misses = c.counter("cache_misses_total", "Immutable cache misses")
        self.feed_state = c.counter(
            "feed_state_total", "Feed generation states served, by endpoint", ("endpoint", "feed", "state")
        )

    def observe_request(self, route: str, status: int, seconds: float) -> None:
        self.requests.labels(route=route, status=str(status)).inc()
        self.request_duration.labels(route=route).observe(max(0.0, seconds))

    def observe_feed_state(self, endpoint: str, feed_type: str, state: str) -> None:
        self.feed_state.labels(endpoint=endpoint, feed=feed_type, state=state).inc()

    def render(self, metrics_dir: Path | None) -> str:
        """Prometheus text: live API registry + other components' textfiles."""
        own = generate_latest(self.component.registry).decode("utf-8")
        if metrics_dir is None:
            return own
        foreign = [
            line
            for line in read_textfiles(metrics_dir).splitlines()
            if line and not (_metric_name(line) or "").startswith(OWN_PREFIX)
        ]
        if not foreign:
            return own
        return own.rstrip("\n") + "\n" + "\n".join(foreign) + "\n"

    def start_exporter(self, metrics_dir: Path, interval_seconds: float) -> None:
        self.component.start_exporter(metrics_dir, interval_seconds)

    def stop(self, metrics_dir: Path | None) -> None:
        self.component.stop(metrics_dir)
