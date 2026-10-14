"""Serve the built React console without Vite.

``vite preview`` on the Colab VM stays alive but never accepts HTTP, so expose
times out after 120s. This module does the two things that preview was doing:
serve ``frontend/dist`` (with an ``index.html`` fallback for client routes) and
reverse-proxy ``/api`` to the local FastAPI process. One origin is enough for
the Cloudflare tunnel.
"""

from __future__ import annotations

import argparse
import http.client
import mimetypes
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("application/javascript", ".mjs")
mimetypes.add_type("text/css", ".css")
mimetypes.add_type("image/svg+xml", ".svg")

_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
        "te", "trailers", "transfer-encoding", "upgrade", "content-length", "host"}


def resolve_static(dist: Path, url_path: str) -> Path | None:
    """Map a URL path onto a file inside ``dist``.

    A missing client route falls back to ``index.html``. A missing file that
    has an extension (an asset) is a 404. A path that escapes ``dist`` is refused.
    """
    root = dist.resolve()
    rel = unquote(urlsplit(url_path).path).lstrip("/")
    if not rel or rel.endswith("/"):
        return root / "index.html"
    candidate = (root / rel).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        return None
    if candidate.is_file():
        return candidate
    if Path(rel).suffix:
        return None
    return root / "index.html"


class _Handler(BaseHTTPRequestHandler):
    dist: Path
    api_host: str
    api_port: int
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: object) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch()

    def do_HEAD(self) -> None:  # noqa: N802
        self._dispatch()

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch()

    def do_PUT(self) -> None:  # noqa: N802
        self._dispatch()

    def do_DELETE(self) -> None:  # noqa: N802
        self._dispatch()

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._dispatch()

    def do_PATCH(self) -> None:  # noqa: N802
        self._dispatch()

    def _dispatch(self) -> None:
        path = urlsplit(self.path).path
        if path == "/api" or path.startswith("/api/"):
            self._proxy()
        else:
            self._static()

    def _static(self) -> None:
        target = resolve_static(self.dist, self.path)
        if target is None or not target.is_file():
            body = b"not found"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
            return
        data = target.read_bytes()
        ctype = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def _proxy(self) -> None:
        length = int(self.headers.get("Content-Length") or "0")
        body = self.rfile.read(length) if length else None
        headers = {k: v for k, v in self.headers.items() if k.lower() not in _HOP}
        try:
            conn = http.client.HTTPConnection(self.api_host, self.api_port, timeout=30)
            conn.request(self.command, self.path, body=body, headers=headers)
            resp = conn.getresponse()
            payload = resp.read()
            status = resp.status
            forwarded = [(k, v) for k, v in resp.getheaders() if k.lower() not in _HOP]
            conn.close()
        except OSError as exc:
            payload = f"api unavailable: {exc}".encode()
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(payload)
            return
        self.send_response(status)
        for key, value in forwarded:
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)


def make_server(dist: Path, host: str, port: int, api_origin: str) -> ThreadingHTTPServer:
    parsed = urlsplit(api_origin)
    api_host = parsed.hostname or "127.0.0.1"
    api_port = parsed.port or (443 if parsed.scheme == "https" else 80)

    class Handler(_Handler):
        pass

    Handler.dist = dist.resolve()
    Handler.api_host = api_host
    Handler.api_port = api_port
    return ThreadingHTTPServer((host, port), Handler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve the built React console and proxy /api")
    parser.add_argument("--dist", required=True, type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3000)
    parser.add_argument("--api", default="http://127.0.0.1:8000")
    args = parser.parse_args(argv)
    index = args.dist / "index.html"
    if not index.is_file():
        print(f"react preview: {index} is missing", file=sys.stderr)
        return 1
    server = make_server(args.dist, args.host, args.port, args.api)
    bound_host, bound_port = server.server_address[:2]
    print(f"react preview listening on http://{bound_host}:{bound_port} dist={args.dist.resolve()}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def serve_background(dist: Path, host: str, port: int, api_origin: str) -> tuple[ThreadingHTTPServer, threading.Thread]:
    """Start the preview on a daemon thread. Used by tests; ``port=0`` picks a free port."""
    server = make_server(dist, host, port, api_origin)
    thread = threading.Thread(target=server.serve_forever, name="react-preview", daemon=True)
    thread.start()
    return server, thread


if __name__ == "__main__":
    raise SystemExit(main())
