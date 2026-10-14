"""runtime.json loading, the canonical subprocess environment and the committed runtime manifest."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from transitpulse.runtime.cli_common import resolve_profile, wcmd
from transitpulse.runtime.manifest import (
    load_artifact_manifest,
    load_runtime,
    render_runtime_env_file,
    runtime_from_dict,
    subprocess_env,
    validate_artifact_manifest,
)
from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.spark_launch import processor_argv, processor_env


def _runtime_doc(workdir: Path) -> dict:
    return {
        "schema_version": 1, "profile": "colab-native", "repo_dir": "/r", "repo_commit": None, "workdir": str(workdir),
        "python": {"core_python": "/r/.venv/bin/python", "version": "3.11.17", "lock_origin": "committed"},
        "java": {"java_home": "/w/tools/jdk/jdk-17.0.20.1+1", "version": "17.0.20.1"},
        "kafka": {"home": "/w/tools/kafka/kafka_2.13-4.2.2", "config": "/w/conf/kafka/server.properties"},
        "cassandra": {"home": "/w/tools/cassandra/apache-cassandra-5.0.9", "conf_dir": "/w/conf/cassandra"},
        "spark": {"spark_home": "/r/.venv/lib/python3.11/site-packages/pyspark", "connector_jars": ["/w/a.jar"]},
        "api": {"base_url": "http://127.0.0.1:8000/api/v1"},
        "env": {"JAVA_HOME": "/w/tools/jdk/jdk-17.0.20.1+1", "PATH": "/w/tools/jdk/jdk-17.0.20.1+1/bin:/r/.venv/bin:/usr/bin",
                "PYSPARK_PYTHON": "/r/.venv/bin/python", "TZ": "UTC", "TP_WORKDIR": str(workdir)},
    }


def test_subprocess_env_starts_clean_and_strips_kernel_leaks() -> None:
    rt = runtime_from_dict(_runtime_doc(Path("/w")))
    caller = {"PATH": "/opt/colab/bin:/usr/bin", "HOME": "/root", "LANG": "en_US.UTF-8", "PYTHONPATH": "/env/python",
              "SPARK_HOME": "/usr/local/lib/python3.12/dist-packages/pyspark", "PYSPARK_SUBMIT_ARGS": "--master x",
              "CLASSPATH": "/x.jar", "VIRTUAL_ENV": "/kernel-venv", "JAVA_HOME": "/usr/lib/jvm/java-11",
              "COLAB_SECRET": "should-not-leak"}
    env = subprocess_env(rt, {"EXTRA": "1"}, environ=caller)
    for leaked in ("PYTHONPATH", "SPARK_HOME", "PYSPARK_SUBMIT_ARGS", "CLASSPATH", "VIRTUAL_ENV", "COLAB_SECRET"):
        assert leaked not in env
    assert env["JAVA_HOME"] == "/w/tools/jdk/jdk-17.0.20.1+1"
    assert env["PATH"].startswith("/w/tools/jdk/jdk-17.0.20.1+1/bin:/r/.venv/bin")
    assert env["HOME"] == "/root" and env["TZ"] == "UTC" and env["EXTRA"] == "1"
    assert env["PYSPARK_PYTHON"] == "/r/.venv/bin/python"


def test_processor_launch_uses_core_python_and_spark_home() -> None:
    rt = runtime_from_dict(_runtime_doc(Path("/w")))
    paths = WorkPaths(Path("/w"))
    argv = processor_argv(rt, paths, "run-1", profile="colab-native", fault_injection=True)
    assert argv[:3] == ["/r/.venv/bin/python", "-m", "transitpulse.processing.stream_job"]
    assert "--enable-fault-injection" in argv and argv[argv.index("--run-id") + 1] == "run-1"
    env = processor_env(rt)
    assert env["SPARK_HOME"].endswith("site-packages/pyspark")
    assert env["PYSPARK_DRIVER_PYTHON"] == "/r/.venv/bin/python"
    assert env["SPARK_LOCAL_IP"] == "127.0.0.1"


def test_load_runtime_missing_gives_bootstrap_hint(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError) as exc:
        load_runtime(WorkPaths.from_workdir(tmp_path))
    assert "bootstrap_colab.sh" in str(exc.value)


def test_load_runtime_rejects_other_workdir(tmp_path: Path) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    paths.runtime_dir.mkdir(parents=True)
    paths.runtime_json.write_text(json.dumps(_runtime_doc(tmp_path / "other")))
    with pytest.raises(ValueError):
        load_runtime(paths)
    paths.runtime_json.write_text(json.dumps(_runtime_doc(paths.root)))
    assert load_runtime(paths).connector_jars == ("/w/a.jar",)


def test_profile_precedence(tmp_path: Path) -> None:
    """explicit --profile > TP_PROFILE > runtime.json profile > default (colab-native for the scripts)."""
    w = tmp_path / "w"
    (w / "runtime").mkdir(parents=True)
    (w / "runtime" / "runtime.json").write_text(json.dumps({"profile": "local-native"}))
    assert resolve_profile(w, "colab-native", environ={"TP_PROFILE": "local-docker"}) == "colab-native"
    assert resolve_profile(w, None, environ={"TP_PROFILE": "local-docker"}) == "local-docker"
    assert resolve_profile(w, None, environ={}) == "local-native"
    assert resolve_profile(tmp_path / "empty", None, environ={}) == "colab-native"
    assert resolve_profile(None, None, environ={}, default="local-native") == "local-native"


def test_cli_modules_run_with_python_m(repo_dir: Path) -> None:
    """docker compose / Make call `python -m transitpulse.runtime.<module>`: each needs a __main__ guard."""
    for name in ("cli_infra", "cli_demo", "doctor", "export", "bootstrap", "verification", "repo_config", "runner"):
        text = (repo_dir / "src" / "transitpulse" / "runtime" / f"{name}.py").read_text(encoding="utf-8")
        assert 'if __name__ == "__main__":' in text and "sys.exit(main())" in text, name


def test_hints_carry_the_actual_workdir() -> None:
    assert wcmd("/content/tp", "scripts/demo.py stop") == "`scripts/demo.py stop --workdir /content/tp`"


def test_runtime_env_file_is_shell_quoted() -> None:
    text = render_runtime_env_file({"A": "plain", "B": "with space", "C": "it's"})
    assert "A=plain" in text
    assert "B='with space'" in text
    assert "C='it'\"'\"'s'" in text


def test_committed_manifest_is_valid_and_never_invents_checksums(repo_dir: Path) -> None:
    manifest = load_artifact_manifest(repo_dir)
    ids = {a["id"] for a in manifest["artifacts"]}
    assert {"uv", "jdk", "kafka", "cassandra"} <= ids
    for a in manifest["artifacts"]:
        value = a["checksum"]["value"]
        assert value is None or re.fullmatch(r"[0-9a-f]{64}|[0-9a-f]{128}", value)
        assert a["status"] != "VERIFIED"
        assert all(u.startswith("https://") for u in a["urls"])
    apache = [a for a in manifest["artifacts"] if a["id"] in ("kafka", "cassandra")]
    assert all(any("archive.apache.org" in u for u in a["urls"]) for a in apache)
    assert manifest["spark_connector"]["coordinate"] == "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"


def test_manifest_validation_reports_problems() -> None:
    problems = validate_artifact_manifest({"schema_version": 1, "artifacts": [
        {"id": "uv", "version": "1", "top_dir": "a/b", "install_to": "/abs", "urls": ["http://x"],
         "checksum": {"algorithm": "md5", "value": "zz", "format": "weird", "official_source": ""},
         "status": "VERIFIED"}]})
    joined = " | ".join(problems)
    for needle in ("top_dir", "install_to", "https", "algorithm", "format", "VERIFIED", "artifact 'jdk' missing"):
        assert needle in joined
