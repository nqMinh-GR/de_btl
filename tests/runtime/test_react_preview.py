"""The React expose server serves dist/ and proxies /api without Vite."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

from transitpulse.runtime.react_preview import resolve_static, serve_background


def _get(url: str, *, accept: str = "application/json") -> tuple[int, bytes, str]:
    req = Request(url, headers={"Accept": accept})
    with urlopen(req, timeout=5) as resp:
        return resp.status, resp.read(), resp.headers.get("Content-Type", "")


def test_resolve_static_spa_asset_and_traversal(tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<html>ok</html>", encoding="utf-8")
    (dist / "assets" / "app.js").write_text("console.log(1)", encoding="utf-8")

    assert resolve_static(dist, "/") == (dist / "index.html").resolve()
    assert resolve_static(dist, "/routes/1") == (dist / "index.html").resolve()
    assert resolve_static(dist, "/assets/app.js") == (dist / "assets" / "app.js").resolve()
    assert resolve_static(dist, "/assets/missing.js") is None
    assert resolve_static(dist, "/../../etc/passwd") is None


def test_preview_serves_index_and_proxies_api(tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<html>console</html>", encoding="utf-8")

    class Api(_Handler):
        pass

    api = ThreadingHTTPServer(("127.0.0.1", 0), Api)
    api_thread = threading.Thread(target=api.serve_forever, daemon=True)
    api_thread.start()
    api_port = api.server_address[1]
    preview, _thread = serve_background(dist, "127.0.0.1", 0, f"http://127.0.0.1:{api_port}")
    port = preview.server_address[1]
    try:
        status, body, ctype = _get(f"http://127.0.0.1:{port}/")
        assert status == 200
        assert b"console" in body
        assert "html" in ctype

        status, body, _ctype = _get(f"http://127.0.0.1:{port}/map")
        assert status == 200
        assert b"console" in body

        status, body, ctype = _get(f"http://127.0.0.1:{port}/api/v1/health")
        assert status == 200
        assert json.loads(body) == {"ok": True}
        assert "json" in ctype
    finally:
        preview.shutdown()
        api.shutdown()


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        payload = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt: str, *args: object) -> None:
        return
