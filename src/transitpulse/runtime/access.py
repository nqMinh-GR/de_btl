"""Public access from Colab: the Streamlit dashboard and Cloudflare quick tunnels (supervisor scope "access").

    demo.py expose start|status|stop [--target dashboard|api|both] [--ready-timeout 120]

* ``dashboard``: ``streamlit run app/streamlit_app.py`` bound to 127.0.0.1:8501 (cwd ``app/`` so its
  ``.streamlit/config.toml`` applies; every relevant setting is also passed explicitly). Readiness:
  ``GET http://127.0.0.1:8501/_stcore/health`` returns ``ok``.
* ``tunnel`` / ``tunnel-api``: ``cloudflared tunnel --no-autoupdate --url http://127.0.0.1:8501`` (or :8000 for
  the API). cloudflared logs to stderr (captured in ``W/logs/<component>/stdout.log``); readiness = a
  ``https://<words>.trycloudflare.com`` URL appears in THIS incarnation's part of the log (byte offset recorded
  at spawn). Reachability of the public URL is probed best effort (DNS propagation can lag; not fatal).
* The access components outlive demo sessions: the public URL stays stable for the Colab VM's lifetime and
  ``demo.py stop`` leaves them running; ``demo.py stop --all`` or ``demo.py expose stop`` stops them.
* cloudflared cannot carry the owner token on its command line; ownership is verified through the runner
  wrapper (``supervisor.is_owned_child``) and the token in its environment. A cloudflared that is not ours is
  never signalled.

Security: a quick tunnel makes the read-only dashboard reachable by anyone who knows the random URL. There is
no authentication; the data served is public MBTA data or synthetic test data; the tunnel ends with the
Colab session (or ``expose stop``).
"""

from __future__ import annotations

import argparse
import logging
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from transitpulse.runtime.cli_common import (
    EXIT_UNSUPPORTED,
    EXIT_USAGE,
    CliError,
    Ctx,
    Result,
    http_get,
)
from transitpulse.runtime.manifest import subprocess_env
from transitpulse.runtime.supervisor import OWNER_PLACEHOLDER, Supervisor, spawn_gate_bytes
from transitpulse.runtime.util import atomic_write_json, iso_now, read_json_or_none, sha256_file

log = logging.getLogger("transitpulse.runtime.access")

DASHBOARD_HOST = "127.0.0.1"
DASHBOARD_PORT = 8501
TUNNEL_URL_RE = re.compile(r"https://([a-z0-9-]+)\.trycloudflare\.com\b")
NON_TUNNEL_HOSTS = frozenset({"api", "www"})
TUNNEL_COMPONENT = {"dashboard": "tunnel", "api": "tunnel-api"}
ALL_TARGETS: tuple[str, ...] = ("dashboard", "api")
SECURITY_NOTE = ("Quick tunnel: anyone with the random URL can open the read-only dashboard (no authentication). "
                 "Data is public MBTA data or synthetic test data. The tunnel ends with the Colab session or "
                 "`demo.py expose stop`.")


# --------------------------------------------------------------------------------------------------
# Pure helpers (unit tested)
# --------------------------------------------------------------------------------------------------

def parse_tunnel_url(text: str) -> str | None:
    """First quick-tunnel URL in cloudflared output. ``api.trycloudflare.com`` (the quick-tunnel API host that
    appears in error messages) is not a tunnel URL."""
    for m in TUNNEL_URL_RE.finditer(text or ""):
        host = m.group(1)
        if host in NON_TUNNEL_HOSTS:
            continue
        return f"https://{host}.trycloudflare.com"
    return None


def read_log_since(path: Path, offset: int | None, max_bytes: int = 1024 * 1024) -> str:
    """Text appended to ``path`` after ``offset`` (bounded); empty when unreadable."""
    try:
        size = path.stat().st_size
        start = int(offset or 0)
        if start > size:  # rotated/truncated: read the whole current file
            start = 0
        with open(path, "rb") as fh:
            fh.seek(start)
            data = fh.read(max_bytes)
    except OSError:
        return ""
    return data.decode("utf-8", errors="replace")


def decide_expose(entry: dict[str, Any] | None, *, alive: bool) -> str:
    """START (nothing usable), REUSE (owned tunnel with a known URL) or WAIT_URL (owned, URL not seen yet)."""
    if not entry or not alive:
        return "START"
    return "REUSE" if entry.get("public_url") else "WAIT_URL"


def targets_of(target: str) -> list[str]:
    if target == "both":
        return ["dashboard", "api"]
    if target in ("dashboard", "api"):
        return [target]
    raise CliError("USAGE", f"unknown --target {target!r}", hint="dashboard | api | both", exit_code=EXIT_USAGE)


# --------------------------------------------------------------------------------------------------
# Components
# --------------------------------------------------------------------------------------------------

def dashboard_local_url() -> str:
    return f"http://{DASHBOARD_HOST}:{DASHBOARD_PORT}"


def dashboard_argv(ctx: Ctx) -> list[str]:
    app = ctx.repo_dir / "app" / "streamlit_app.py"
    return [ctx.python, "-m", "streamlit", "run", str(app), "--server.address", DASHBOARD_HOST,
            "--server.port", str(DASHBOARD_PORT), "--server.headless", "true", "--browser.gatherUsageStats", "false",
            "--server.enableCORS", "false", "--server.enableXsrfProtection", "false",
            "--", "--tp-owner", OWNER_PLACEHOLDER]


def ensure_dashboard(ctx: Ctx, sup: Supervisor, *, ready_timeout: float) -> dict[str, Any]:
    from transitpulse.runtime.cli_demo import app_fingerprint, core_env

    app = ctx.repo_dir / "app" / "streamlit_app.py"
    if not app.is_file():
        raise CliError("DASHBOARD_MISSING", f"{app} not found", exit_code=EXIT_UNSUPPORTED)
    env = {**core_env(ctx), "TP_API_URL": ctx.api_base, "TP_WORKDIR": str(ctx.paths.root),
           "STREAMLIT_BROWSER_GATHER_USAGE_STATS": "false"}
    health = f"{dashboard_local_url()}/_stcore/health"

    def ready() -> bool:
        status, body = http_get(health, timeout=3.0)
        return status == 200 and str(body).strip().strip('"') == "ok"

    alive = sup.entry("dashboard")
    if alive and sup.is_alive("dashboard") and ready():
        # Already serving: reuse as is (a code/config change does not tear down the public URL mid-session).
        return dashboard_view(sup, alive, reused=True)
    entry = sup.start("dashboard", dashboard_argv(ctx), env=env, scope="access", ports=(DASHBOARD_PORT,), ready=ready,
                      ready_timeout=ready_timeout, fingerprint_extra=app_fingerprint(ctx),
                      min_mem_available_bytes=spawn_gate_bytes("dashboard"), graceful_timeout=10.0,
                      cwd=ctx.repo_dir / "app")
    return dashboard_view(sup, entry, reused=entry.get("reused"))


def dashboard_view(sup: Supervisor, entry: dict[str, Any] | None, *, reused: Any = None) -> dict[str, Any]:
    if not entry:
        return {"state": "NOT_RUNNING", "local_url": dashboard_local_url(), "pid": None, "log": None, "reused": None}
    alive = sup.is_alive("dashboard")
    return {"state": entry.get("state") if alive else "EXITED", "local_url": dashboard_local_url(),
            "pid": entry.get("pid"), "wrapper_pid": entry.get("wrapper_pid"), "log": entry.get("stdout_log"),
            "reused": reused, "alive": alive}


def tunnel_origin(ctx: Ctx, target: str) -> str:
    if target == "dashboard":
        return dashboard_local_url()
    return f"http://127.0.0.1:{ctx.cfg.api.port}"


def cloudflared_path(ctx: Ctx) -> Path:
    rt = ctx.require_runtime()
    tunnel = rt.raw.get("tunnel")
    if not tunnel or not tunnel.get("cloudflared"):
        raise CliError("TUNNEL_DISABLED", "cloudflared is not available (bootstrap ran with --no-tunnel, or its "
                       "install/verification failed: see W/runtime/bootstrap_report.json step B6T)",
                       hint=f"re-run `bash scripts/bootstrap_colab.sh --workdir {ctx.paths.root}` without --no-tunnel",
                       exit_code=EXIT_UNSUPPORTED)
    path = Path(tunnel["cloudflared"])
    if not path.is_file():
        raise CliError("TUNNEL_BINARY_MISSING", f"{path} not found", hint="re-run bootstrap", exit_code=EXIT_UNSUPPORTED)
    return path


def probe_public(url: str | None, *, total_timeout: float = 60.0, path: str = "") -> bool:
    """Best-effort GET of the public URL with retries (DNS of a fresh quick tunnel can lag)."""
    if not url:
        return False
    deadline = time.monotonic() + max(0.0, total_timeout)
    attempt = 0
    while True:
        attempt += 1
        req = urllib.request.Request(url + path, headers={"User-Agent": "transitpulse-expose-probe"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310 - https quick tunnel URL
                if 200 <= resp.status < 400:
                    return True
        except urllib.error.HTTPError as exc:
            log.debug("public URL probe attempt %d: HTTP %s", attempt, exc.code)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            log.debug("public URL probe attempt %d: %s", attempt, exc)
        if time.monotonic() >= deadline:
            return False
        time.sleep(min(5.0, max(0.5, deadline - time.monotonic())))


def ensure_tunnel(ctx: Ctx, sup: Supervisor, target: str, *, ready_timeout: float = 90.0,
                  probe_seconds: float = 60.0) -> dict[str, Any]:
    binary = cloudflared_path(ctx)
    component = TUNNEL_COMPONENT[target]
    origin = tunnel_origin(ctx, target)
    home = ctx.paths.tmp / "cloudflared-home"  # no ~/.cloudflared config may interfere with a quick tunnel
    home.mkdir(parents=True, exist_ok=True)
    env = subprocess_env(ctx.require_runtime(), {"HOME": str(home)})
    # --grace-period 5s: cloudflared's default (30 s) would outlast the supervisor's 15 s graceful timeout when a
    # browser websocket is open, ending every stop in SIGKILL.
    argv = [str(binary), "tunnel", "--no-autoupdate", "--grace-period", "5s", "--url", origin]
    before = sup.entry(component)
    decision = decide_expose(before, alive=sup.is_alive(component))

    def ready() -> bool:
        e = sup.entry(component) or {}
        url = parse_tunnel_url(read_log_since(Path(e.get("stdout_log") or ""), e.get("log_offset")))
        if url and e.get("owner_token"):
            if e.get("public_url") != url:
                sup.annotate(component, e["owner_token"], public_url=url, url_seen_at=iso_now())
            return True
        return False

    if decision == "REUSE" and before is not None:
        # Alive owned tunnel with a known URL: keep it as is so the public URL stays stable.
        entry = {**before, "reused": True}
    else:
        entry = sup.start(component, argv, env=env, scope="access", ready=ready, ready_timeout=ready_timeout,
                          fingerprint_extra=f"cloudflared={sha256_file(binary)};origin={origin}",
                          min_mem_available_bytes=spawn_gate_bytes(component), graceful_timeout=15.0)
    current = sup.entry(component) or entry
    url = current.get("public_url")
    probe_path = ctx.cfg.api.base_path + "/health/live" if target == "api" else ""
    reachable = probe_public(url, total_timeout=probe_seconds, path=probe_path)
    sup.annotate(component, current["owner_token"], reachable=reachable, reachable_checked_at=iso_now())
    return {"target": target, "component": component, "public_url": url,
            "public_api_base": (url + ctx.cfg.api.base_path) if (url and target == "api") else None,
            "reachable": reachable, "state": current.get("state"), "pid": current.get("pid"),
            "wrapper_pid": current.get("wrapper_pid"), "child_verified": sup.owned_child_pid(component) is not None,
            "log": current.get("stdout_log"), "origin": origin, "reused": bool(entry.get("reused")),
            "decision": decision}


def tunnel_view(ctx: Ctx, sup: Supervisor, target: str, *, probe: bool) -> dict[str, Any]:
    component = TUNNEL_COMPONENT[target]
    e = sup.entry(component)
    alive = sup.is_alive(component)
    url = (e or {}).get("public_url") if alive else None
    if alive and e and not url:
        # expose start was interrupted before readiness: recover the URL from this incarnation's log.
        url = parse_tunnel_url(read_log_since(Path(e.get("stdout_log") or ""), e.get("log_offset")))
        if url and e.get("owner_token"):
            sup.annotate(component, e["owner_token"], public_url=url, url_seen_at=iso_now(), recovered=True)
    if not alive or not url:
        reachable = False
    elif probe:  # one quick attempt
        path = (ctx.cfg.api.base_path + "/health/live") if target == "api" else ""
        reachable = probe_public(url, total_timeout=0.0, path=path)
    else:
        reachable = bool((e or {}).get("reachable"))
    return {"target": target, "component": component, "public_url": url, "reachable": reachable,
            "state": ((e or {}).get("state") if alive else ("EXITED" if e else "NOT_RUNNING")),
            "pid": (e or {}).get("pid"), "child_verified": sup.owned_child_pid(component) is not None if alive else False,
            "log": (e or {}).get("stdout_log"), "origin": tunnel_origin(ctx, target)}


def public_urls_path(ctx: Ctx) -> Path:
    return ctx.paths.runtime_dir / "public_urls.json"


def write_public_urls(ctx: Ctx, dashboard: dict[str, Any], tunnels: list[dict[str, Any]]) -> Path:
    path = public_urls_path(ctx)
    atomic_write_json(path, {"updated_at": iso_now(), "dashboard": dashboard, "tunnels": tunnels,
                             "security_note": SECURITY_NOTE})
    return path


def read_public_urls(ctx: Ctx, sup: Supervisor) -> dict[str, Any]:
    """Public URLs whose tunnel is still alive (used by demo.py status)."""
    data = read_json_or_none(public_urls_path(ctx)) or {}
    out = []
    for t in data.get("tunnels", []) or []:
        comp = t.get("component")
        alive = bool(comp) and sup.is_alive(comp)
        out.append({"target": t.get("target"), "public_url": t.get("public_url") if alive else None,
                    "alive": alive, "reachable": t.get("reachable") if alive else False})
    return {"dashboard": {"local_url": dashboard_local_url(), "alive": sup.is_alive("dashboard")}, "tunnels": out,
            "file": str(public_urls_path(ctx)) if data else None}


# --------------------------------------------------------------------------------------------------
# CLI: demo.py expose start|status|stop
# --------------------------------------------------------------------------------------------------

def cmd_expose(ctx: Ctx, args: argparse.Namespace) -> Result:
    from transitpulse.runtime.cli_infra import supervisor_for

    sup = supervisor_for(ctx)
    targets = targets_of(args.target)
    if args.op == "start":
        return _expose_start(ctx, sup, targets, args)
    if args.op == "stop":
        return _expose_stop(ctx, sup, targets)
    views = {t: tunnel_view(ctx, sup, t, probe=t in targets) for t in ALL_TARGETS}
    tunnels = [views[t] for t in targets]
    dashboard = dashboard_view(sup, sup.entry("dashboard"))
    write_public_urls(ctx, dashboard, [views[t] for t in ALL_TARGETS])
    return Result({"op": "status", "target": args.target, "dashboard": dashboard, "tunnels": tunnels,
                   "warnings": [], "public_urls_file": str(public_urls_path(ctx)), "security_note": SECURITY_NOTE})


def _expose_start(ctx: Ctx, sup: Supervisor, targets: list[str], args: argparse.Namespace) -> Result:
    from transitpulse.runtime.cli_demo import ensure_api

    ctx.require_runtime()
    cloudflared_path(ctx)  # fail fast (exit 6) when the tunnel is disabled
    warnings: list[str] = []
    timeout = float(args.ready_timeout)
    if not sup.is_alive("api"):
        if sup.is_alive("kafka") and sup.is_alive("cassandra"):
            try:
                ensure_api(ctx, sup, ready_timeout=timeout)
            except Exception as exc:  # noqa: BLE001 - the dashboard still starts and shows "unavailable"
                warnings.append(f"API could not be started ({exc}); the dashboard shows 'unavailable' until it runs")
        else:
            warnings.append("infra/API not running: the dashboard shows 'unavailable' until `infra.py start/init` "
                            "and `demo.py start`")
    dashboard: dict[str, Any] = dashboard_view(sup, sup.entry("dashboard"))
    tunnels: list[dict[str, Any]] = []
    if "dashboard" in targets:
        dashboard = ensure_dashboard(ctx, sup, ready_timeout=timeout)
        tunnels.append(ensure_tunnel(ctx, sup, "dashboard", ready_timeout=min(timeout, 90.0)))
    if "api" in targets:
        tunnels.append(ensure_tunnel(ctx, sup, "api", ready_timeout=min(timeout, 90.0)))
    for t in tunnels:
        if not t["reachable"]:
            warnings.append(f"{t['public_url']} is not reachable yet (quick-tunnel DNS can take a minute); "
                            "re-check with `demo.py expose status`")
    # The file always describes BOTH targets (an `--target api` start must not drop the dashboard URL).
    started = {t["target"]: t for t in tunnels}
    path = write_public_urls(ctx, dashboard, [started.get(t) or tunnel_view(ctx, sup, t, probe=False)
                                              for t in ALL_TARGETS])
    reused = all(bool(t.get("reused")) for t in tunnels) and \
        ("dashboard" not in targets or bool(dashboard.get("reused")))
    return Result({"op": "start", "target": "+".join(targets) if len(targets) > 1 else targets[0], "reused": reused,
                   "dashboard": dashboard, "tunnels": tunnels, "warnings": warnings, "public_urls_file": str(path),
                   "security_note": SECURITY_NOTE})


def _expose_stop(ctx: Ctx, sup: Supervisor, targets: list[str]) -> Result:
    stopped: dict[str, Any] = {}
    for t in targets:
        stopped[TUNNEL_COMPONENT[t]] = sup.stop(TUNNEL_COMPONENT[t])
    if "dashboard" in targets:
        stopped["dashboard"] = sup.stop("dashboard")
    tunnels = [tunnel_view(ctx, sup, t, probe=False) for t in ALL_TARGETS]
    dashboard = dashboard_view(sup, sup.entry("dashboard"))
    write_public_urls(ctx, dashboard, tunnels)
    return Result({"op": "stop", "target": "+".join(targets), "stopped": stopped, "dashboard": dashboard,
                   "tunnels": tunnels, "warnings": [], "public_urls_file": str(public_urls_path(ctx))})


def stop_access(sup: Supervisor) -> dict[str, Any]:
    """Used by ``demo.py stop --all``: tunnels first, then the dashboard."""
    return {c: sup.stop(c) for c in ("tunnel", "tunnel-api", "dashboard")}


__all__ = ["cmd_expose", "parse_tunnel_url", "decide_expose", "read_log_since", "stop_access", "read_public_urls",
           "SECURITY_NOTE"]
