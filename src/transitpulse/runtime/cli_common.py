"""Shared CLI plumbing for scripts/*.py (one JSON document on stdout, progress on stderr).

Contract (docs/interfaces.md, global rules):
* stdout carries exactly ONE JSON document ``{"ok", "action", ..., "error": {code, message, hint,
  log_excerpt} | null}``. During the action, file descriptor 1 is redirected to stderr so that stray
  prints of libraries or child processes can never corrupt it.
* Exit codes: 0 ok, 2 usage, 3 preflight, 4 conflict, 5 readiness/exit, 6 unsupported mode,
  7 artifact/checksum, 1 other failure.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import signal
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_PREFLIGHT = 3
EXIT_CONFLICT = 4
EXIT_READINESS = 5
EXIT_UNSUPPORTED = 6
EXIT_ARTIFACT = 7

log = logging.getLogger("transitpulse.cli")


class CliError(Exception):
    """Expected failure with a stable code, a human message, an actionable hint and an exit code."""

    def __init__(self, code: str, message: str, *, hint: str | None = None, exit_code: int = EXIT_FAILURE,
                 log_excerpt: str | None = None, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint
        self.exit_code = exit_code
        self.log_excerpt = log_excerpt
        self.details = dict(details or {})

    def error_json(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "hint": self.hint, "log_excerpt": self.log_excerpt}


@dataclass
class Result:
    """Successful (or partially successful) action payload. ``ok`` defaults to ``exit_code == 0``."""

    payload: dict[str, Any] = field(default_factory=dict)
    exit_code: int = EXIT_OK
    error: dict[str, Any] | None = None


class JsonArgumentParser(argparse.ArgumentParser):
    """argparse that reports usage errors through the JSON contract (exit 2) instead of printing."""

    def error(self, message: str) -> None:  # type: ignore[override]
        raise CliError("USAGE", message, hint=f"run `{self.prog} --help`", exit_code=EXIT_USAGE)

    def exit(self, status: int = 0, message: str | None = None) -> None:  # type: ignore[override]
        if message:
            sys.stderr.write(message)
        raise _HelpExit(status)


class _HelpExit(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(status)
        self.status = status


class _ProgressFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = time.strftime("%H:%M:%S", time.gmtime(record.created))
        msg = record.getMessage()
        prefix = "" if record.levelno == logging.INFO else f"{record.levelname}: "
        out = f"[{ts}Z] {prefix}{msg}"
        if record.exc_info and record.levelno >= logging.ERROR:
            out += "\n" + self.formatException(record.exc_info)[-3000:]
        return out


def configure_progress_logging(level: int = logging.INFO) -> None:
    """Human-readable progress lines on stderr (the notebook streams them)."""
    root = logging.getLogger()
    for h in list(root.handlers):
        if getattr(h, "_tp_cli", False):
            root.removeHandler(h)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_ProgressFormatter())
    handler._tp_cli = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(level)
    for noisy in ("py4j", "cassandra", "urllib3", "httpx", "httpcore", "confluent_kafka"):
        logging.getLogger(noisy).setLevel(logging.ERROR)


def emit_json(fd: int, doc: Mapping[str, Any]) -> None:
    data = (json.dumps(doc, default=str, ensure_ascii=False, sort_keys=False) + "\n").encode("utf-8")
    view = memoryview(data)
    while view:
        n = os.write(fd, view)
        view = view[n:]


class Terminated(KeyboardInterrupt):
    """Raised by the SIGTERM handler so that ``finally``/cleanup blocks run (e.g. a notebook cell timeout)."""


def install_sigterm_handler() -> None:
    """First SIGTERM raises :class:`Terminated` in the main thread; later ones are logged and ignored so the
    cleanup they triggered (stopping the processes this command started) is not interrupted. A Python-level
    handler is reset to the default in exec'd children, so spawned components keep normal SIGTERM handling."""

    def _during_cleanup(signum: int, _frame: Any) -> None:
        log.warning("received signal %s during cleanup; finishing cleanup first", signum)

    def _first(signum: int, _frame: Any) -> None:
        signal.signal(signal.SIGTERM, _during_cleanup)
        raise Terminated(f"terminated by signal {signum}")

    signal.signal(signal.SIGTERM, _first)


def run_main(action_of: Callable[[argparse.Namespace], str], parser: argparse.ArgumentParser,
             handler: Callable[[argparse.Namespace], Result], argv: Sequence[str] | None = None) -> int:
    """Parse args, run ``handler`` with stdout redirected to stderr, print one JSON document, return exit code."""
    configure_progress_logging()
    install_sigterm_handler()
    real_stdout = os.dup(1)
    os.dup2(2, 1)
    action = "unknown"
    try:
        with contextlib.redirect_stdout(sys.stderr):
            try:
                args = parser.parse_args(list(argv) if argv is not None else None)
                action = action_of(args)
                result = handler(args)
                ok = bool(result.payload.get("ok", result.exit_code == EXIT_OK))
                doc = {"ok": ok, "action": action, **{k: v for k, v in result.payload.items() if k != "ok"},
                       "error": result.error}
                exit_code = result.exit_code
            except _HelpExit as exc:
                doc = {"ok": exc.status == 0, "action": "help", "error": None}
                exit_code = exc.status
            except CliError as exc:
                doc = {"ok": False, "action": action, **exc.details, "error": exc.error_json()}
                exit_code = exc.exit_code
                log.error("%s: %s%s", exc.code, exc.message, f" (hint: {exc.hint})" if exc.hint else "")
            except KeyboardInterrupt as exc:
                terminated = isinstance(exc, Terminated)
                doc = {"ok": False, "action": action,
                       "error": {"code": "TERMINATED" if terminated else "INTERRUPTED",
                                 "message": str(exc) if terminated else "interrupted by the user",
                                 "hint": "processes started by this command were stopped during cleanup; "
                                         "check `demo.py status`", "log_excerpt": None}}
                exit_code = EXIT_FAILURE
            except Exception as exc:  # noqa: BLE001 - last-resort reporting through the JSON contract
                mapped = map_exception(exc)
                doc = {"ok": False, "action": action, "error": mapped.error_json()}
                exit_code = mapped.exit_code
                log.error("%s: %s", mapped.code, mapped.message, exc_info=mapped.code == "INTERNAL_ERROR")
            sys.stderr.flush()
    finally:
        os.dup2(real_stdout, 1)
        os.close(real_stdout)
    emit_json(1, doc)
    return exit_code


def map_exception(exc: BaseException) -> CliError:
    """Map known exception types of the runtime to CLI errors (codes and exit codes).

    This module stays importable with the plain system Python (stdlib only) because
    scripts/configure_repository.py runs before any .venv exists; runtime types are matched by class name.
    """
    if isinstance(exc, CliError):
        return exc
    name = type(exc).__name__
    if name == "SupervisorError" and hasattr(exc, "code"):
        return CliError(exc.code, exc.message, hint=exc.hint, exit_code=exc.exit_code,  # type: ignore[attr-defined]
                        log_excerpt=exc.log_excerpt)  # type: ignore[attr-defined]
    if name == "ArtifactError" and hasattr(exc, "code"):
        return CliError(exc.code, exc.message, hint=exc.hint, exit_code=EXIT_ARTIFACT)  # type: ignore[attr-defined]
    if isinstance(exc, FileNotFoundError) and "runtime.json" in str(exc):
        return CliError("RUNTIME_NOT_BOOTSTRAPPED", str(exc), hint="run bash scripts/bootstrap_colab.sh --workdir W",
                        exit_code=EXIT_PREFLIGHT)
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-4000:]
    return CliError("INTERNAL_ERROR", f"{type(exc).__name__}: {exc}", hint="see stderr for the traceback",
                    exit_code=EXIT_FAILURE, log_excerpt=tb)


# --------------------------------------------------------------------------------------------------
# Context (profile, work directory, config, runtime)
# --------------------------------------------------------------------------------------------------

# Default of the native scripts (scripts/*.py). transitpulse.config / cli.py keep local-native as THEIR default;
# every caller resolves through resolve_profile() so the precedence is the same everywhere.
DEFAULT_PROFILE = "colab-native"


def resolve_profile(workdir: str | os.PathLike[str] | None, explicit: str | None,
                    *, default: str = DEFAULT_PROFILE, environ: Mapping[str, str] | None = None) -> str:
    """Profile precedence: explicit ``--profile`` > ``TP_PROFILE`` > ``<workdir>/runtime/runtime.json`` profile
    (when present) > ``default``. Stdlib only (usable by WP9's cli.py without a bootstrap)."""
    if explicit:
        return str(explicit)
    env = os.environ if environ is None else environ
    if env.get("TP_PROFILE"):
        return str(env["TP_PROFILE"])
    if workdir:
        runtime_json = Path(workdir).expanduser() / "runtime" / "runtime.json"
        try:
            data = json.loads(runtime_json.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            data = None
        if isinstance(data, dict) and data.get("profile"):
            return str(data["profile"])
    return default


def wcmd(workdir: Any, command: str) -> str:
    """Copy-paste command hint carrying the ACTUAL work directory, e.g. `scripts/demo.py stop --workdir /x`."""
    return f"`{command} --workdir {workdir}`"


def add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--profile", default=None,
                   help="colab-native | local-native | local-docker (default: $TP_PROFILE, then runtime.json, "
                        "then colab-native)")
    p.add_argument("--workdir", default=None, help="work directory (default: $TP_WORKDIR or the profile's workdir)")


@dataclass
class Ctx:
    profile: str
    paths: Any            # WorkPaths
    cfg: Any              # ProjectConfig
    repo_dir: Path
    runtime: Any | None   # RuntimeInfo

    def require_runtime(self) -> Any:
        if self.runtime is None:
            raise CliError("RUNTIME_NOT_BOOTSTRAPPED", f"{self.paths.runtime_json} not found",
                           hint=f"run: bash scripts/bootstrap_colab.sh --workdir {self.paths.root}",
                           exit_code=EXIT_PREFLIGHT)
        return self.runtime

    @property
    def api_base(self) -> str:
        host = self.cfg.api.host
        if host in ("0.0.0.0", "::", ""):
            host = "127.0.0.1"
        return f"http://{host}:{self.cfg.api.port}{self.cfg.api.base_path}"

    @property
    def python(self) -> str:
        if self.runtime is not None:
            return str(self.runtime.core_python)
        return sys.executable


def load_ctx(args: argparse.Namespace, *, need_runtime: bool = True) -> Ctx:
    from transitpulse.config import PROFILES, load_config, repo_root
    from transitpulse.runtime.manifest import load_runtime
    from transitpulse.runtime.paths import WorkPaths

    workdir = getattr(args, "workdir", None) or os.environ.get("TP_WORKDIR")
    profile = resolve_profile(workdir, getattr(args, "profile", None))
    runtime = None
    if workdir:
        paths = WorkPaths.from_workdir(workdir)
        if paths.runtime_json.is_file():
            try:
                runtime = load_runtime(paths)
            except (OSError, ValueError) as exc:
                raise CliError("RUNTIME_INVALID", str(exc),
                               hint=wcmd(paths.root, "bash scripts/bootstrap_colab.sh"),
                               exit_code=EXIT_PREFLIGHT) from exc
    if profile not in PROFILES:
        raise CliError("USAGE", f"unknown profile {profile!r}", hint=f"use one of {sorted(PROFILES)}",
                       exit_code=EXIT_USAGE)
    if not workdir:
        cfg0 = load_config(profile)
        paths = WorkPaths.from_workdir(cfg0.workdir)
        if paths.runtime_json.is_file():
            runtime = load_runtime(paths)
    cfg = load_config(profile, workdir=paths.root)
    if need_runtime and runtime is None:
        raise CliError("RUNTIME_NOT_BOOTSTRAPPED", f"{paths.runtime_json} not found",
                       hint=f"run: bash scripts/bootstrap_colab.sh --workdir {paths.root}", exit_code=EXIT_PREFLIGHT)
    repo_dir = Path(runtime.raw["repo_dir"]) if runtime is not None else repo_root()
    return Ctx(profile=profile, paths=paths, cfg=cfg, repo_dir=repo_dir, runtime=runtime)


# --------------------------------------------------------------------------------------------------
# Child CLIs and HTTP
# --------------------------------------------------------------------------------------------------

def parse_json_stdout(text: str) -> dict[str, Any] | None:
    text = text.strip()
    if not text:
        return None
    try:
        doc = json.loads(text)
        return doc if isinstance(doc, dict) else None
    except ValueError:
        pass
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                doc = json.loads(line)
            except ValueError:
                continue
            if isinstance(doc, dict):
                return doc
    return None


def run_json_cli(argv: Sequence[str], *, env: Mapping[str, str], cwd: Path, timeout: float, what: str,
                 exit_map: Mapping[int, int] | None = None) -> dict[str, Any]:
    """Run a project CLI that follows the one-JSON-on-stdout contract; stream its stderr to ours.

    Raises CliError carrying the child's error code/message/hint when it fails.
    """
    log.info("running %s", what)
    t0 = time.monotonic()
    try:
        proc = subprocess.run(list(argv), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=None,
                              cwd=str(cwd), env=dict(env), timeout=timeout, check=False)
    except subprocess.TimeoutExpired as exc:
        raise CliError(f"{_code(what)}_TIMEOUT", f"{what} did not finish within {int(timeout)} s",
                       hint="see the progress lines above", exit_code=EXIT_READINESS) from exc
    except OSError as exc:
        raise CliError(f"{_code(what)}_SPAWN_FAILED", f"cannot run {what}: {exc}", exit_code=EXIT_FAILURE) from exc
    out = proc.stdout.decode("utf-8", errors="replace")
    doc = parse_json_stdout(out)
    log.info("%s finished in %.1f s (exit %s)", what, time.monotonic() - t0, proc.returncode)
    if proc.returncode != 0 or doc is None or doc.get("ok") is False:
        err = (doc or {}).get("error") or {}
        code = err.get("code") or f"{_code(what)}_FAILED"
        message = err.get("message") or f"{what} failed with exit code {proc.returncode}"
        exit_code = (exit_map or {}).get(proc.returncode, EXIT_FAILURE)
        raise CliError(code, f"{what}: {message}", hint=err.get("hint"), exit_code=exit_code,
                       log_excerpt=err.get("log_excerpt") or (out[-2000:] if doc is None else None),
                       details={"child": {"argv": list(argv)[:12], "exit_code": proc.returncode, "result": doc}})
    return doc


def _code(what: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in what.upper()).strip("_")[:40] or "CHILD"


def http_get(url: str, *, timeout: float = 5.0) -> tuple[int | None, Any]:
    """GET returning (status, parsed JSON or text). (None, error string) when unreachable."""
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "transitpulse-cli"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - loopback API
            body = resp.read(4 * 1024 * 1024)
            status = resp.status
    except urllib.error.HTTPError as exc:
        status = exc.code
        try:
            body = exc.read(1024 * 1024)
        except OSError:
            body = b""
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return None, str(exc)
    try:
        return status, json.loads(body.decode("utf-8"))
    except ValueError:
        return status, body.decode("utf-8", errors="replace")[:2000]


def wait_until(pred: Callable[[], bool], *, timeout: float, interval: float = 1.0, what: str = "condition",
               progress_every: float = 10.0) -> bool:
    t0 = time.monotonic()
    last = t0
    while True:
        try:
            if pred():
                return True
        except Exception as exc:  # noqa: BLE001 - probes are retried until the deadline
            log.debug("%s probe raised %s", what, exc)
        now = time.monotonic()
        if now - t0 >= timeout:
            return False
        if now - last >= progress_every:
            log.info("waiting for %s (%.0f/%.0f s)", what, now - t0, timeout)
            last = now
        time.sleep(interval)


def script_cmd(ctx: Ctx, script: str, *args: str) -> list[str]:
    """Argv a user (or the notebook) can run to reproduce a follow-up command."""
    return [ctx.python, str(ctx.repo_dir / "scripts" / script), *args, "--workdir", str(ctx.paths.root)]
