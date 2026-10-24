"""Benchmark measurements that do not need Spark: window overlap, Cassandra duration_ms, API probe."""

from __future__ import annotations

import importlib.util
import io
import json
import logging
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

from transitpulse.obs.logging import JsonFormatter, get_logger, log_event


def _compute():
    path = Path(__file__).resolve().parents[2] / "benchmarks" / "compute.py"
    spec = importlib.util.spec_from_file_location("benchmark_compute", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).isoformat().replace("+00:00", "Z")


def test_batch_stats_counts_a_batch_that_overlaps_the_window():
    compute = _compute()
    start, end = 1_000_000.0, 1_000_180.0
    overlap = SimpleNamespace(timestamp=_iso(end + 20), duration_ms=40_000, num_input_rows=3)
    before = SimpleNamespace(timestamp=_iso(start - 1), duration_ms=1_000, num_input_rows=1)
    inside = SimpleNamespace(timestamp=_iso(start + 10), duration_ms=None, num_input_rows=2)
    after = SimpleNamespace(timestamp=_iso(end + 5), duration_ms=None, num_input_rows=9)
    out = compute.batch_stats([overlap, before, inside, after], start, end)
    assert out["batches"] == 2
    assert out["input_rows"] == 5
    assert out["n"] == 1
    assert out["p50"] == 40.0
    assert out["p95"] is None


def test_cassandra_write_duration_is_logged_and_read_back(tmp_path):
    logger = get_logger("test.benchmark.cassandra_write")
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        log_event(logger, "cassandra_write", "ok", batch_id=7, feed="trip_updates", snapshot_id="snap",
                  duration_ms=2500.0)
    finally:
        logger.removeHandler(handler)
    doc = json.loads(buf.getvalue())
    assert doc["stage"] == "cassandra_write"
    assert doc["status"] == "ok"
    assert doc["duration_ms"] == 2500.0

    path = tmp_path / "processor.log"
    ts = _iso(1_000_010)
    path.write_text(
        json.dumps({"stage": "cassandra_write", "ts": ts, "status": "ok"}) + "\n"
        + json.dumps({"stage": "cassandra_write", "ts": ts, "status": "ok", "duration_ms": 2500}) + "\n"
        + json.dumps({"stage": "builder", "ts": ts, "status": "ok", "duration_ms": 10}) + "\n",
        encoding="utf-8",
    )
    out = _compute().log_stage_durations(path, "cassandra_write", 1_000_000, 1_000_180)
    assert out["n"] == 1
    assert out["p50"] == 2.5


def test_api_probe_against_a_local_server():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib API
            body = b'{"data":{}}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):  # noqa: A003 - silence the test server
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/api/v1/vehicles?run_id=r&route_id=1"
        out = _compute().probe_http_latency(url, samples=100, timeout=5)
    finally:
        server.shutdown()
        server.server_close()
    assert out["n"] == 100
    assert out["errors"] == 0
    assert out["p50"] is not None
    assert out["p95"] is not None
    assert out["p99"] is not None
    assert "not browser UI latency" in out["source"]
