"""API process settings.

The API reads the same layered project configuration as every other component
(:func:`transitpulse.config.load_config`, profile from ``TP_PROFILE``, work directory from
``TP_WORKDIR``) and adds three process-local values:

* ``token_key``: HMAC key for page tokens (``TP_API_TOKEN_KEY``; random per process when unset, so
  tokens issued before a restart are rejected with 422 ``invalid_page_token`` and clients restart
  from the first page);
* ``wall_clock``: the wall-clock source (injectable for tests; production uses UTC now);
* ``configure_logging``: whether :func:`create_app` installs the JSON log handler.
"""

from __future__ import annotations

import os
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from transitpulse.config import ApiSection, ProjectConfig, load_config
from transitpulse.contracts.envelope import utcnow
from transitpulse.runtime.paths import WorkPaths

TOKEN_KEY_ENV = "TP_API_TOKEN_KEY"
MIN_TOKEN_KEY_BYTES = 16
DEFAULT_RECONNECT_INTERVAL_SECONDS = 2.0


def _random_token_key() -> bytes:
    return secrets.token_bytes(32)


@dataclass(frozen=True)
class ApiSettings:
    """Immutable settings of one API process."""

    cfg: ProjectConfig
    paths: WorkPaths | None = None
    token_key: bytes = field(default_factory=_random_token_key, repr=False)
    configure_logging: bool = True
    wall_clock: Callable[[], datetime] = utcnow
    reconnect_interval_seconds: float = DEFAULT_RECONNECT_INTERVAL_SECONDS

    def __post_init__(self) -> None:
        if len(self.token_key) < MIN_TOKEN_KEY_BYTES:
            raise ValueError(f"page-token key must have at least {MIN_TOKEN_KEY_BYTES} bytes")
        if self.reconnect_interval_seconds <= 0:
            raise ValueError("reconnect_interval_seconds must be positive")

    # ------------------------------------------------------------------ derived values
    @property
    def api(self) -> ApiSection:
        return self.cfg.api

    @property
    def base_path(self) -> str:
        """Normalised mount path of every endpoint (``/api/v1``; no trailing slash)."""
        raw = (self.cfg.api.base_path or "").strip()
        if not raw or raw == "/":
            return ""
        return "/" + raw.strip("/")

    @property
    def metrics_dir(self) -> Path | None:
        """``<W>/metrics`` when a work directory is configured, else ``None`` (textfiles disabled)."""
        return self.paths.metrics_dir if self.paths is not None else None

    # ------------------------------------------------------------------ constructors
    @classmethod
    def from_config(
        cls,
        cfg: ProjectConfig,
        *,
        token_key: bytes | None = None,
        configure_logging: bool = True,
        wall_clock: Callable[[], datetime] = utcnow,
        reconnect_interval_seconds: float = DEFAULT_RECONNECT_INTERVAL_SECONDS,
    ) -> ApiSettings:
        """Build settings from an already loaded configuration (used by tests and embedders)."""
        return cls(
            cfg=cfg,
            paths=_work_paths(cfg),
            token_key=token_key if token_key is not None else _random_token_key(),
            configure_logging=configure_logging,
            wall_clock=wall_clock,
            reconnect_interval_seconds=reconnect_interval_seconds,
        )

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> ApiSettings:
        """Settings of the uvicorn factory process: ``TP_PROFILE``, ``TP_WORKDIR``, ``TP_API_TOKEN_KEY``."""
        env = dict(os.environ if environ is None else environ)
        cfg = load_config(env.get("TP_PROFILE") or None, environ=env)
        raw_key = env.get(TOKEN_KEY_ENV)
        key = raw_key.encode("utf-8") if raw_key else None
        return cls.from_config(cfg, token_key=key)


def _work_paths(cfg: ProjectConfig) -> WorkPaths | None:
    """Work paths when a work directory is configured; the API itself never requires one."""
    try:
        return WorkPaths.from_workdir(cfg.workdir)
    except (ValueError, RuntimeError):
        return None
