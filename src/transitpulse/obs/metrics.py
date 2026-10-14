"""Prometheus metrics per component, exported as textfiles (no Prometheus server in the core demo).

Each process creates ONE :class:`ComponentMetrics` and calls :meth:`start_exporter`; the registry is
written atomically every ``observability.metrics_export_seconds`` to ``<W>/metrics/<component>.prom``.
``GET /api/v1/metrics`` serves the API registry plus every ``*.prom`` file, so no extra ports are
opened and the last values survive a crash. Metric names carry their unit (``_seconds``, ``_bytes``,
``_total``).
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, write_to_textfile


class ComponentMetrics:
    def __init__(self, component: str) -> None:
        self.component = component
        self.registry = CollectorRegistry(auto_describe=True)
        self._metrics: dict[str, object] = {}
        self._exporter: threading.Thread | None = None
        self._stop = threading.Event()
        self.exported = Gauge(
            f"tp_{component}_metrics_exported_timestamp_seconds",
            "Wall time of the last textfile export",
            registry=self.registry,
        )

    def _name(self, name: str) -> str:
        return name if name.startswith("tp_") else f"tp_{self.component}_{name}"

    def counter(self, name: str, doc: str, labels: tuple[str, ...] = ()) -> Counter:
        key = self._name(name)
        if key not in self._metrics:
            self._metrics[key] = Counter(key, doc, labels, registry=self.registry)
        return self._metrics[key]  # type: ignore[return-value]

    def gauge(self, name: str, doc: str, labels: tuple[str, ...] = ()) -> Gauge:
        key = self._name(name)
        if key not in self._metrics:
            self._metrics[key] = Gauge(key, doc, labels, registry=self.registry)
        return self._metrics[key]  # type: ignore[return-value]

    def histogram(
        self, name: str, doc: str, labels: tuple[str, ...] = (), buckets: tuple[float, ...] | None = None
    ) -> Histogram:
        key = self._name(name)
        if key not in self._metrics:
            kwargs = {"buckets": buckets} if buckets else {}
            self._metrics[key] = Histogram(key, doc, labels, registry=self.registry, **kwargs)
        return self._metrics[key]  # type: ignore[return-value]

    def export_once(self, metrics_dir: Path) -> Path:
        metrics_dir.mkdir(parents=True, exist_ok=True)
        self.exported.set(time.time())
        target = metrics_dir / f"{self.component}.prom"
        write_to_textfile(str(target), self.registry)  # writes a temp file then renames (atomic)
        return target

    def start_exporter(self, metrics_dir: Path, interval_seconds: float) -> None:
        if self._exporter is not None:
            return

        def loop() -> None:
            while not self._stop.wait(interval_seconds):
                try:
                    self.export_once(metrics_dir)
                except OSError:
                    pass

        self._exporter = threading.Thread(target=loop, name=f"metrics-{self.component}", daemon=True)
        self._exporter.start()

    def stop(self, metrics_dir: Path | None = None) -> None:
        self._stop.set()
        if metrics_dir is not None:
            try:
                self.export_once(metrics_dir)
            except OSError:
                pass


def read_textfiles(metrics_dir: Path, prefixes: tuple[str, ...] = ("tp_",)) -> str:
    """Concatenate exported *.prom files (only lines of known prefixes, comments kept)."""
    if not metrics_dir.is_dir():
        return ""
    chunks: list[str] = []
    for f in sorted(metrics_dir.glob("*.prom")):
        try:
            text = f.read_text(encoding="utf-8")
        except OSError:
            continue
        lines = [ln for ln in text.splitlines() if ln.startswith("#") or ln.startswith(prefixes)]
        chunks.append("\n".join(lines))
    return "\n".join(chunks) + ("\n" if chunks else "")


def pid() -> int:
    return os.getpid()
