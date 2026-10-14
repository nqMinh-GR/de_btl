"""IT16 offline probe: synthetic replay -> processor -> Cassandra -> API (+ Streamlit when installed) with no network.

Methods (decision D-E26, the method actually used is recorded in the report):

* ``netns``: the probe runs inside ``bwrap --dev-bind / / --unshare-net --die-with-parent`` (started by the IT16 test)
  and brings up its own Kafka/Cassandra on the namespace loopback (``--manage-infra``);
* ``guard``: no network namespace available; every Python process started here gets the
  ``verify/offline_guard`` ``sitecustomize`` socket guard (``TP_OFFLINE_GUARD=1``), black-hole HTTP(S) proxies
  and black-hole JVM proxy properties. Native sockets of librdkafka/JVMs are not intercepted (stated limit).

Checks: the three generations are READY and current, the API answers 200 on the core endpoints, the Spark processor
was started without ``spark.jars.packages``/``--packages`` and resolved nothing with Ivy, served dashboard HTML
references no non-loopback asset, ``notebooks/colab_ui.py`` loads no external asset; optionally a Playwright browser
pass blocks every non-loopback request (NOT_RUN when Playwright is not installed).

CLI: ``python -m transitpulse.verify.offline_probe --workdir W --profile P --method guard|netns --report OUT
[--manage-infra] [--speed 10] [--timeout-seconds 900]``. Prints one JSON document; exit 0 when every executed check
passed, 1 otherwise.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import subprocess
import sys
import time
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import httpx

GUARD_DIR = Path(__file__).resolve().parent / "offline_guard"
BLACKHOLE_PROXY = "http://127.0.0.1:9"
_LOOPBACK_RE = re.compile(r"^(https?:)?//(127\.\d+\.\d+\.\d+|localhost|\[::1\])(:\d+)?(/|$)", re.IGNORECASE)
_ABS_URL_RE = re.compile(r"^(https?:)?//", re.IGNORECASE)
_JS_ASSET_HOSTS = ("fonts.googleapis.com", "fonts.gstatic.com", "cdn.jsdelivr.net", "unpkg.com", "cdnjs.cloudflare.com",
                   "api.mapbox.com", "basemaps.cartocdn.com", "tile.openstreetmap.org")
_ASSET_PATTERNS = (
    re.compile(r"""<script[^>]+src\s*=\s*["'](https?:)?//(?!127\.|localhost)""", re.IGNORECASE),
    re.compile(r"""<link[^>]+href\s*=\s*["'](https?:)?//(?!127\.|localhost)""", re.IGNORECASE),
    re.compile(r"""<img[^>]+src\s*=\s*["'](https?:)?//(?!127\.|localhost)""", re.IGNORECASE),
    re.compile(r"""url\(\s*["']?(https?:)?//(?!127\.|localhost)""", re.IGNORECASE),
    re.compile(r"""@import\s+["']?(https?:)?//(?!127\.|localhost)""", re.IGNORECASE),
)
IVY_MARKERS = (":: resolving dependencies ::", "Ivy Default Cache set to", "ivy-default-cache", ":: retrieving ::")


class _AssetParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.assets: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        names = {"script": ("src",), "link": ("href",), "img": ("src", "srcset"), "iframe": ("src",),
                 "source": ("src", "srcset"), "video": ("src",), "audio": ("src",)}.get(tag.lower(), ())
        for k, v in attrs:
            if k.lower() in names and v:
                self.assets.append(v)


def external_assets(html: str) -> tuple[list[str], list[str]]:
    """(all asset references, the non-loopback absolute ones)."""
    p = _AssetParser()
    p.feed(html)
    ext = [a for a in p.assets if _ABS_URL_RE.match(a) and not _LOOPBACK_RE.match(a)]
    return p.assets, ext


def source_asset_violations(text: str) -> list[str]:
    hits: list[str] = []
    for pat in _ASSET_PATTERNS:
        hits.extend(m.group(0)[:160] for m in pat.finditer(text))
    return hits


def guard_env(log_path: Path) -> dict[str, str]:
    java_opts = ("-Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=9 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=9 "
                 "-Dhttp.nonProxyHosts=localhost|127.*|[::1]")
    return {
        "PYTHONPATH": str(GUARD_DIR),
        "TP_OFFLINE_GUARD": "1",
        "TP_OFFLINE_GUARD_LOG": str(log_path),
        "HTTP_PROXY": BLACKHOLE_PROXY, "HTTPS_PROXY": BLACKHOLE_PROXY, "ALL_PROXY": BLACKHOLE_PROXY,
        "http_proxy": BLACKHOLE_PROXY, "https_proxy": BLACKHOLE_PROXY, "all_proxy": BLACKHOLE_PROXY,
        "NO_PROXY": "127.0.0.1,localhost,::1", "no_proxy": "127.0.0.1,localhost,::1",
        "JAVA_TOOL_OPTIONS": java_opts,
        "STREAMLIT_BROWSER_GATHER_USAGE_STATS": "false",
    }


def read_guard_log(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
    except FileNotFoundError:
        pass
    return out


class Probe:
    def __init__(self, args: argparse.Namespace) -> None:
        from transitpulse.config import load_config, repo_root
        from transitpulse.runtime.manifest import load_runtime, subprocess_env
        from transitpulse.runtime.paths import WorkPaths
        from transitpulse.verify.flows import StackContext
        from transitpulse.verify.stack import StackHarness, new_session_id

        self.args = args
        self.paths = WorkPaths.from_workdir(args.workdir)
        self.cfg = load_config(args.profile, workdir=self.paths.root)
        self.runtime = load_runtime(self.paths)
        self.repo_dir = repo_root()
        self.report_path = Path(args.report)
        self.out_dir = self.report_path.parent
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.guard_log = self.out_dir / "offline_guard_violations.jsonl"
        self.extra = guard_env(self.guard_log)
        self.env = subprocess_env(self.runtime, self.extra)
        self.session_id = new_session_id("offline")
        self.h = StackHarness(cfg=self.cfg, paths=self.paths, runtime=self.runtime, profile=args.profile,
                              session_id=self.session_id, repo_dir=self.repo_dir, evidence_dir=self.out_dir)
        self.h.env = dict(self.env)
        self.ctx = StackContext(cfg=self.cfg, paths=self.paths, runtime=self.runtime, profile=args.profile,
                                repo_dir=self.repo_dir, golden_dir=self.repo_dir / "tests" / "fixtures" / "golden",
                                env=dict(self.env))
        self.checks: list[dict[str, Any]] = []
        self.not_run: list[dict[str, Any]] = []
        self.api: Any = None
        self.dashboard: subprocess.Popen[bytes] | None = None
        self.dashboard_port: int | None = None
        self.run: Any = None

    # ------------------------------------------------------------------ helpers
    def check(self, name: str, passed: bool, detail: Any = None) -> None:
        self.checks.append({"name": name, "passed": bool(passed), "detail": detail})

    def _start_dashboard(self, api_base: str) -> None:
        from transitpulse.verify.stack import free_port, tail_file, wait_until

        app = self.repo_dir / "app" / "streamlit_app.py"
        if importlib.util.find_spec("streamlit") is None:
            self.not_run.append({"name": "dashboard", "reason": "streamlit is not installed in the core venv "
                                 "(optional extra 'dashboard'); notebook UI asset scan used instead"})
            return
        if not app.is_file():
            self.not_run.append({"name": "dashboard", "reason": f"{app} not found"})
            return
        self.dashboard_port = free_port()
        log_path = self.out_dir / "dashboard.log"
        env = dict(self.env)
        env.update({"TP_API_URL": api_base, "TP_WORKDIR": str(self.paths.root), "TP_PROFILE": self.args.profile})
        argv = [str(self.runtime.core_python), "-m", "streamlit", "run", str(app), "--server.address", "127.0.0.1",
                "--server.port", str(self.dashboard_port), "--server.headless", "true",
                "--browser.gatherUsageStats", "false", "--global.developmentMode", "false"]
        with open(log_path, "ab") as log:
            self.dashboard = subprocess.Popen(argv, env=env, cwd=str(self.repo_dir / "app"), stdin=subprocess.DEVNULL,
                                              stdout=log, stderr=subprocess.STDOUT, start_new_session=True)

        def healthy() -> tuple[bool, Any]:
            if self.dashboard is not None and self.dashboard.poll() is not None:
                raise RuntimeError(f"streamlit exited {self.dashboard.returncode}: {tail_file(log_path)}")
            try:
                r = httpx.get(f"http://127.0.0.1:{self.dashboard_port}/_stcore/health", timeout=2, trust_env=False)
                return r.status_code == 200, r.status_code
            except httpx.HTTPError as exc:
                return False, str(exc)

        wait_until(healthy, 180, "streamlit /_stcore/health")

    def _dashboard_checks(self) -> None:
        if self.dashboard_port is None:
            return
        base = f"http://127.0.0.1:{self.dashboard_port}"
        r = httpx.get(base + "/", timeout=10, trust_env=False)
        self.check("dashboard_html_200", r.status_code == 200, r.status_code)
        assets, external = external_assets(r.text)
        self.check("dashboard_html_no_external_assets", not external, {"assets": assets[:50], "external": external})
        js_hits: dict[str, list[str]] = {}
        for a in assets:
            if a.endswith(".js") and not _ABS_URL_RE.match(a):
                try:
                    js = httpx.get(base + "/" + a.lstrip("./").lstrip("/"), timeout=20, trust_env=False).text
                except httpx.HTTPError:
                    continue
                found = [h for h in _JS_ASSET_HOSTS if h in js]
                if found:
                    js_hits[a] = found
        # Informational: host names inside bundled JS are strings, not proven fetches (the browser pass decides).
        self.checks.append({"name": "dashboard_js_external_host_strings", "passed": True, "informational": True,
                            "detail": js_hits})
        self._browser_pass(base)

    def _browser_pass(self, base: str) -> None:
        if importlib.util.find_spec("playwright") is None:
            self.not_run.append({"name": "browser_offline_pass", "reason": "Playwright is not installed (D-E26)"})
            return
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            self.not_run.append({"name": "browser_offline_pass", "reason": f"Playwright import failed: {exc}"})
            return
        blocked: list[str] = []
        shots: list[str] = []
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                page = browser.new_page()

                def route(r: Any) -> None:
                    url = r.request.url
                    if _LOOPBACK_RE.match(url) or url.startswith(("data:", "blob:", "about:")):
                        r.continue_()
                    else:
                        blocked.append(url)
                        r.abort()

                page.route("**/*", route)
                page.goto(base + "/", wait_until="networkidle", timeout=60000)
                page.wait_for_timeout(5000)
                shot = self.out_dir / "dashboard_offline.png"
                page.screenshot(path=str(shot), full_page=True)
                shots.append(str(shot))
                links = page.locator("a[href^='/']").all()[:4]
                for i, link in enumerate(links):
                    try:
                        link.click(timeout=5000)
                        page.wait_for_timeout(4000)
                        p = self.out_dir / f"dashboard_offline_{i + 2}.png"
                        page.screenshot(path=str(p), full_page=True)
                        shots.append(str(p))
                    except Exception:  # noqa: BLE001 - navigation differences are recorded, not fatal
                        continue
                browser.close()
        except Exception as exc:  # noqa: BLE001 - e.g. browser binaries not installed
            self.not_run.append({"name": "browser_offline_pass", "reason": f"browser could not run: {exc}"[:500]})
            return
        self.check("browser_pages_rendered_offline", bool(shots), {"screenshots": shots, "blocked_requests": blocked})

    # ------------------------------------------------------------------ main flow
    def execute(self) -> dict[str, Any]:
        from transitpulse.replay.control import read_publisher_state, send_command
        from transitpulse.verify.stack import ApiServer, wait_until

        args = self.args
        t0 = time.monotonic()
        if args.manage_infra:
            self.h.infra("start", timeout=900)
            self.h.infra("init", timeout=600)
        recording = self.ctx.materialize("golden")
        routes = recording.route_ids or tuple(self.ctx.timeline.get("scope_route_ids", []))
        static = self.ctx.static_for(recording.static_zip_path, tuple(routes))
        self.run = self.h.create_run(static=static, source_type="synthetic", initial_clock=recording.start_clock,
                                     scenario_id="offline_replay", fixture_kind=recording.fixture_kind,
                                     recording_id=recording.recording_id, speed=float(args.speed),
                                     label=f"{self.session_id}:offline")
        run_id = self.run.run_id
        try:
            self.h.start_processor(run_id, extra_env=self.extra)
            session_dir = self.paths.session_dir(self.session_id)
            session_dir.mkdir(parents=True, exist_ok=True)
            pub_argv = [str(self.runtime.core_python), "-m", "transitpulse.replay", "publish", "--workdir",
                        str(self.paths.root), "--run-id", run_id, "--manifest", str(recording.manifest_path),
                        "--session-id", self.session_id, "--speed", str(args.speed)]
            self.h.start_component("publisher", pub_argv, run_id=run_id, env=self.env,
                                   ready=lambda: (session_dir / "publisher_state.json").exists(), ready_timeout=120)
            self.api = ApiServer(runtime=self.runtime, paths=self.paths, cfg=self.cfg, profile=args.profile,
                                 repo_dir=self.repo_dir, log_path=self.out_dir / "api.log", extra_env=self.extra)
            api_base = self.api.start()
            try:
                self._start_dashboard(api_base)
            except Exception as exc:  # noqa: BLE001 - recorded as a failed check
                self.check("dashboard_started", False, str(exc)[:1000])

            def finished() -> tuple[bool, Any]:
                st = read_publisher_state(session_dir) or {}
                if st.get("state") == "FAILED":
                    raise RuntimeError(f"publisher failed: {st.get('error')}")
                return st.get("state") == "FINISHED", st.get("state")

            wait_until(finished, float(args.timeout_seconds), "publisher FINISHED", interval=2.0)
            self.h.wait_caught_up(run_id, timeout=300)
            for feed in ("vehicle_positions", "trip_updates", "alerts"):
                ptr = self.h.pointer(run_id, feed)
                man = self.h.manifest(run_id, feed, ptr["snapshot_id"]) if ptr else None
                self.check(f"generation_ready[{feed}]", bool(man and man.get("status") == "READY"),
                           {"snapshot_id": (ptr or {}).get("snapshot_id"), "status": (man or {}).get("status")})
            client = httpx.Client(timeout=15, trust_env=False)
            try:
                for path, params in (("health/live", {}), (f"runs/{run_id}", {}), ("vehicles", {"run_id": run_id}),
                                     (f"routes/{routes[0]}/metrics", {"run_id": run_id}),
                                     ("alerts", {"run_id": run_id}), ("quality", {"run_id": run_id})):
                    r = client.get(f"{api_base}/{path}", params=params)
                    self.check(f"api_200[{path.split('/')[0]}]", r.status_code == 200, r.status_code)
            finally:
                client.close()
            entry = self.h.sup.entry("processor") or {}
            argv_text = " ".join(str(a) for a in entry.get("argv") or [])
            self.check("spark_no_packages_argument", "--packages" not in argv_text
                       and "spark.jars.packages" not in argv_text, argv_text[:2000])
            log_text = ""
            log_path = self.h.component_log("processor")
            if log_path and log_path.is_file():
                log_text = log_path.read_text(encoding="utf-8", errors="replace")
            ivy = [m for m in IVY_MARKERS if m in log_text]
            self.check("spark_no_ivy_resolution", not ivy, {"markers": ivy, "log": str(log_path)})
            self._dashboard_checks()
            colab_ui = self.repo_dir / "notebooks" / "colab_ui.py"
            if colab_ui.is_file():
                hits = source_asset_violations(colab_ui.read_text(encoding="utf-8"))
                self.check("notebook_ui_no_external_assets", not hits, hits[:20])
            else:
                self.not_run.append({"name": "notebook_ui_asset_scan", "reason": f"{colab_ui} not found"})
            if self.h.component_alive("publisher"):
                send_command(session_dir, "finish", timeout=30)
        finally:
            self._shutdown()
        violations = read_guard_log(self.guard_log)
        executed = [c for c in self.checks if not c.get("informational")]
        ok = bool(executed) and all(c["passed"] for c in executed)
        return {
            "ok": ok, "action": "offline-probe", "method": args.method, "run_id": run_id,
            "session_id": self.session_id, "duration_seconds": round(time.monotonic() - t0, 1),
            "checks": self.checks, "not_run": self.not_run, "blocked_connections": violations[:200],
            "blocked_connection_count": len(violations),
            "limits": ("guard method: native sockets of librdkafka/JVMs are not intercepted; JVM HTTP(S) gets a "
                       "black-hole proxy; Kafka/Cassandra listen on loopback") if args.method == "guard" else
                      "netns method: the probe ran in a network namespace with only a loopback interface",
            "error": None,
        }

    def _shutdown(self) -> None:
        if self.dashboard is not None and self.dashboard.poll() is None:
            self.dashboard.terminate()
            try:
                self.dashboard.wait(20)
            except subprocess.TimeoutExpired:
                self.dashboard.kill()
        if self.api is not None:
            self.api.stop()
        for comp in ("publisher", "processor"):
            try:
                self.h.stop_component(comp)
            except Exception:  # noqa: BLE001 - shutdown is best effort, state is reported
                pass
        if self.args.manage_infra:
            try:
                self.h.infra("stop", timeout=600, check=False)
            except Exception:  # noqa: BLE001
                pass
        self.h.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m transitpulse.verify.offline_probe", description=__doc__.split("\n")[0])
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--profile", default="colab-native")
    ap.add_argument("--method", choices=("guard", "netns"), required=True)
    ap.add_argument("--report", required=True)
    ap.add_argument("--manage-infra", action="store_true")
    ap.add_argument("--speed", type=float, default=10.0)
    ap.add_argument("--timeout-seconds", type=float, default=900.0)
    args = ap.parse_args(argv)
    try:
        result = Probe(args).execute()
    except Exception as exc:  # noqa: BLE001 - one JSON document with the error, exit 1
        result = {"ok": False, "action": "offline-probe", "method": args.method,
                  "error": {"code": type(exc).__name__, "message": str(exc)[:4000], "hint": None, "log_excerpt": None}}
    report = Path(args.report)
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
    sys.stdout.write(json.dumps(result, default=str) + "\n")
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
