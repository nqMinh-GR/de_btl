"""Runtime manifests.

* ``deploy/runtime-manifest.json`` (committed): pinned artifact versions, official URLs and checksum
  sources. Checksum values stay ``null`` until verified values are committed (decision D-ENV-05);
  bootstrap fetches the official checksum file from the same origin and records ``VERIFIED`` only in
  ``W/runtime/bootstrap_report.json``.
* ``W/runtime/runtime.json`` (written by ``prepare_runtime.py``): absolute paths, versions, hashes and
  the canonical subprocess environment. Every launcher reads it instead of trusting the caller's
  environment (a notebook kernel may have its own Java, pyspark or PYTHONPATH).
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from transitpulse.runtime.paths import WorkPaths

RUNTIME_SCHEMA_VERSION = 1
MANIFEST_SCHEMA_VERSION = 1

# Inherited variables that must never leak from the caller (kernel) into a core subprocess.
STRIPPED_ENV_KEYS = frozenset({
    "PYTHONPATH", "PYTHONHOME", "PYTHONSTARTUP", "PYTHONUSERBASE", "SPARK_HOME", "CLASSPATH", "VIRTUAL_ENV",
    "JAVA_HOME", "JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "JDK_JAVA_OPTIONS", "HADOOP_CONF_DIR", "SPARK_CONF_DIR",
    "CONDA_PREFIX", "HEAP_NEWSIZE",
})
STRIPPED_ENV_PREFIXES = ("PYSPARK_",)
# Only these are taken from the caller; everything else comes from runtime.json.
INHERITED_ENV_KEYS = ("PATH", "HOME", "LANG")

ARTIFACT_STATUSES = frozenset({"PENDING", "URL_CONFIRMED", "PINNED", "VERIFIED", "MISMATCH", "INTEGRATION_VERIFIED"})
# "pinned-release-notes": the publisher posts the digest only in its release notes (cloudflared); the value is
# then committed (status PINNED) and is the sole reference.
CHECKSUM_FORMATS = frozenset({"sha256sum", "bare-or-sha256sum", "gpg-print-md", "pinned-release-notes"})
ADOPTIUM_FORMAT_PREFIX = "adoptium-api"
_HEX_LEN = {"sha256": 64, "sha512": 128, "sha1": 40}


class ManifestError(ValueError):
    """The committed runtime manifest is malformed (fail before downloading anything)."""


@dataclass(frozen=True)
class RuntimeInfo:
    raw: dict
    core_python: Path
    java_home: Path
    spark_home: Path
    connector_jars: tuple[str, ...]
    kafka_home: Path
    cassandra_home: Path
    env: dict[str, str]

    @property
    def repo_dir(self) -> Path:
        return Path(self.raw["repo_dir"])

    @property
    def profile(self) -> str:
        # The profile recorded by bootstrap (always written). CLIs resolve the effective profile with
        # cli_common.resolve_profile (explicit > TP_PROFILE > runtime.json > default).
        return str(self.raw.get("profile") or "colab-native")

    @property
    def repo_commit(self) -> str | None:
        return self.raw.get("repo_commit")

    @property
    def kafka_config(self) -> Path:
        return Path(self.raw["kafka"]["config"])

    @property
    def cassandra_conf_dir(self) -> Path:
        return Path(self.raw["cassandra"]["conf_dir"])

    @property
    def api_base_url(self) -> str:
        return str(self.raw.get("api", {}).get("base_url") or "http://127.0.0.1:8000/api/v1")


def runtime_from_dict(raw: Mapping[str, Any]) -> RuntimeInfo:
    try:
        return RuntimeInfo(
            raw=dict(raw),
            core_python=Path(raw["python"]["core_python"]),
            java_home=Path(raw["java"]["java_home"]),
            spark_home=Path(raw["spark"]["spark_home"]),
            connector_jars=tuple(str(j) for j in raw["spark"].get("connector_jars", [])),
            kafka_home=Path(raw["kafka"]["home"]),
            cassandra_home=Path(raw["cassandra"]["home"]),
            env={str(k): str(v) for k, v in raw.get("env", {}).items()},
        )
    except (KeyError, TypeError) as exc:
        raise ValueError(f"runtime.json is missing a required field: {exc}") from exc


def load_runtime(paths: WorkPaths) -> RuntimeInfo:
    """Read ``W/runtime/runtime.json``. Raises FileNotFoundError with a hint when bootstrap has not run."""
    path = paths.runtime_json
    if not path.is_file():
        raise FileNotFoundError(
            f"{path} not found: the native runtime is not bootstrapped for this work directory. "
            f"Run: bash scripts/bootstrap_colab.sh --workdir {paths.root}"
        )
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    if raw.get("schema_version") != RUNTIME_SCHEMA_VERSION:
        raise ValueError(f"{path}: unsupported schema_version {raw.get('schema_version')!r}; re-run bootstrap")
    recorded = raw.get("workdir")
    if recorded and Path(recorded).resolve() != paths.root.resolve():
        raise ValueError(f"{path} was written for workdir {recorded}, not {paths.root}; re-run bootstrap")
    return runtime_from_dict(raw)


def clean_base_env(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """The only variables taken from the caller: PATH, HOME, LANG (never PYTHONPATH, SPARK_HOME, ...)."""
    src = os.environ if environ is None else environ
    return {k: src[k] for k in INHERITED_ENV_KEYS if k in src and src[k]}


def subprocess_env(runtime: RuntimeInfo, extra: Mapping[str, str] | None = None,
                   *, environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Canonical environment for every core subprocess.

    Starts from a CLEAN env (PATH, HOME, LANG of the caller), overlays ``runtime.env`` (JAVA_HOME,
    PATH with the pinned JDK and .venv first, PYSPARK_PYTHON, TZ=UTC, TP_*), then ``extra``.
    Variables in :data:`STRIPPED_ENV_KEYS` / ``PYSPARK_*`` are dropped unless runtime.env or ``extra``
    set them deliberately.
    """
    base = clean_base_env(environ)
    deliberate = dict(runtime.env)
    if extra:
        deliberate.update({str(k): str(v) for k, v in extra.items()})
    env = {k: v for k, v in base.items() if not _stripped(k)}
    env.update(deliberate)
    return env


def _stripped(key: str) -> bool:
    return key in STRIPPED_ENV_KEYS or key.startswith(STRIPPED_ENV_PREFIXES)


# --------------------------------------------------------------------------------------------------
# deploy/runtime-manifest.json
# --------------------------------------------------------------------------------------------------

def manifest_path(repo_dir: Path) -> Path:
    return repo_dir / "deploy" / "runtime-manifest.json"


def load_artifact_manifest(repo_dir: Path) -> dict[str, Any]:
    path = manifest_path(repo_dir)
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError as exc:
        raise ManifestError(f"{path} not found") from exc
    except json.JSONDecodeError as exc:
        raise ManifestError(f"{path} is not valid JSON: {exc}") from exc
    problems = validate_artifact_manifest(data)
    if problems:
        raise ManifestError(f"{path} is invalid: " + "; ".join(problems))
    return data


def validate_artifact_manifest(data: Any) -> list[str]:
    """Schema checks for the committed manifest. Returns a list of problems (empty = valid)."""
    problems: list[str] = []
    if not isinstance(data, dict):
        return ["top level must be an object"]
    if data.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        problems.append(f"schema_version must be {MANIFEST_SCHEMA_VERSION}")
    arts = data.get("artifacts")
    if not isinstance(arts, list) or not arts:
        problems.append("artifacts must be a non-empty list")
        arts = []
    seen: set[str] = set()
    for i, a in enumerate(arts):
        where = f"artifacts[{i}]"
        if not isinstance(a, dict):
            problems.append(f"{where} must be an object")
            continue
        aid = a.get("id")
        if not isinstance(aid, str) or not re.fullmatch(r"[a-z0-9_-]+", aid or ""):
            problems.append(f"{where}.id invalid")
        elif aid in seen:
            problems.append(f"{where}.id duplicated: {aid}")
        else:
            seen.add(aid)
        binary = a.get("type") == "binary"
        for key in ("version", "install_to") if binary else ("version", "top_dir", "install_to"):
            if not isinstance(a.get(key), str) or not a.get(key):
                problems.append(f"{where}.{key} must be a non-empty string")
        top = a.get("top_dir") or ""
        if not binary and ("/" in top or top in (".", "..")):
            problems.append(f"{where}.top_dir must be a single path component")
        if binary and not re.fullmatch(r"0?[0-7]{3}", str(a.get("mode", "0755"))):
            problems.append(f"{where}.mode must be an octal file mode such as 0755")
        inst = a.get("install_to") or ""
        if inst.startswith("/") or ".." in Path(inst).parts:
            problems.append(f"{where}.install_to must be relative to the work directory")
        urls = a.get("urls")
        if not isinstance(urls, list) or not urls:
            problems.append(f"{where}.urls must be a non-empty list")
        else:
            for u in urls:
                if not isinstance(u, str) or not u.startswith("https://"):
                    problems.append(f"{where}.urls must all be https:// URLs ({u!r})")
        ck = a.get("checksum")
        if not isinstance(ck, dict):
            problems.append(f"{where}.checksum must be an object")
        else:
            alg = ck.get("algorithm")
            if alg not in ("sha256", "sha512"):
                problems.append(f"{where}.checksum.algorithm must be sha256 or sha512")
            value = ck.get("value")
            if value is not None:
                if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]+", value) or \
                        len(value) != _HEX_LEN.get(str(alg), -1):
                    problems.append(f"{where}.checksum.value must be null or lowercase hex of the algorithm length")
            fmt = ck.get("format")
            if not isinstance(fmt, str) or not (fmt in CHECKSUM_FORMATS or fmt.startswith(ADOPTIUM_FORMAT_PREFIX)):
                problems.append(f"{where}.checksum.format unsupported: {fmt!r}")
            elif fmt == "pinned-release-notes" and (value is None or a.get("status") != "PINNED"):
                problems.append(f"{where}: format pinned-release-notes requires a value and status PINNED")
            src = ck.get("official_source")
            if not isinstance(src, str) or not src:
                problems.append(f"{where}.checksum.official_source missing")
            elif fmt and isinstance(fmt, str) and fmt.startswith(ADOPTIUM_FORMAT_PREFIX) and not src.startswith("https://"):
                problems.append(f"{where}.checksum.official_source must be an https URL")
        if a.get("status") not in ARTIFACT_STATUSES:
            problems.append(f"{where}.status must be one of {sorted(ARTIFACT_STATUSES)}")
        if a.get("status") == "VERIFIED":
            problems.append(f"{where}.status VERIFIED is recorded only in bootstrap_report.json, never in git")
    for required in ("uv", "jdk", "kafka", "cassandra"):
        if required not in seen:
            problems.append(f"artifact {required!r} missing")
    conn = data.get("spark_connector")
    if not isinstance(conn, dict) or not isinstance(conn.get("coordinate"), str):
        problems.append("spark_connector.coordinate missing")
    elif not re.fullmatch(r"[A-Za-z0-9_.-]+:[A-Za-z0-9_.-]+:[A-Za-z0-9_.-]+", conn["coordinate"]):
        problems.append("spark_connector.coordinate must be group:artifact:version")
    py = data.get("pyspark")
    if not isinstance(py, dict) or not isinstance(py.get("version"), str):
        problems.append("pyspark.version missing")
    return problems


def artifact_entry(manifest: Mapping[str, Any], artifact_id: str) -> dict[str, Any]:
    for a in manifest.get("artifacts", []):
        if a.get("id") == artifact_id:
            return dict(a)
    raise ManifestError(f"artifact {artifact_id!r} not in runtime manifest")


def unpinned_artifacts(manifest: Mapping[str, Any]) -> list[str]:
    return [a["id"] for a in manifest.get("artifacts", []) if (a.get("checksum") or {}).get("value") is None]


def render_runtime_env_file(env: Mapping[str, str]) -> str:
    """``runtime.env``: shell-quoted KEY=VALUE lines (source with ``set -a; . runtime.env; set +a``). No secrets."""
    import shlex

    lines = ["# Generated by scripts/prepare_runtime.py — canonical environment of core subprocesses.",
             "# Usage: set -a; . runtime.env; set +a"]
    for key in sorted(env):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        lines.append(f"{key}={shlex.quote(str(env[key]))}")
    return "\n".join(lines) + "\n"
