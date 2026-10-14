"""Export exclusion/redaction rules and the evidence-only verification report."""

from __future__ import annotations

import json
from pathlib import Path

from transitpulse.runtime.export import is_excluded, mask_tunnel_urls, masked_public_urls, redact
from transitpulse.runtime.paths import WorkPaths
from transitpulse.runtime.verification import build_report, junit_status


def test_redaction_of_tokens_and_credentials() -> None:
    text = "Authorization: Bearer abc\npassword=hunter2\nuse ghp_" + "a" * 36 + " here\nplain line\n"
    out = redact(text)
    assert "hunter2" not in out
    assert "abc" not in out
    assert "ghp_" not in out
    assert "plain line" in out
    assert out.count("[REDACTED]") == 3


def test_public_tunnel_url_never_exported(tmp_path: Path) -> None:
    url = "https://seasonal-deck-organisms-sf.trycloudflare.com"
    assert url not in redact(f"INF |  {url}  |\n")
    assert mask_tunnel_urls(f'{{"note": "open {url}"}}') == '{"note": "open <redacted>"}'
    p = tmp_path / "public_urls.json"
    p.write_text(json.dumps({"tunnels": [{"target": "dashboard", "public_url": url, "public_api_base": None,
                                          "log": "/w/logs/tunnel/stdout.log"}],
                             "dashboard": {"local_url": "http://127.0.0.1:8501"}}))
    data = json.loads(masked_public_urls(p))
    assert data["tunnels"][0]["public_url"] == "<redacted>"
    assert data["tunnels"][0]["public_api_base"] is None
    assert data["dashboard"]["local_url"] == "http://127.0.0.1:8501"
    assert "trycloudflare" not in json.dumps(data)


def test_live_infra_and_env_files_are_never_exported(tmp_path: Path) -> None:
    w = tmp_path / "w"
    for rel in ("infra/kafka/data/00000.log", "runtime/runtime.env", "checkpoints/p1/r/offsets/0",
                "tools/jdk/bin/java", "cache/uv/x", "artifacts/old.zip", "processes/registry.json"):
        p = w / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")
        assert is_excluded(p, w), rel
    keep = w / "reports" / "notebook" / "notebook_steps.json"
    keep.parent.mkdir(parents=True)
    keep.write_text("{}")
    assert not is_excluded(keep, w)
    last = w / "processes" / "last_start.json"
    last.write_text("{}")
    assert not is_excluded(last, w)


def test_empty_workdir_reports_nothing_as_passing(tmp_path: Path, repo_dir: Path) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    paths.ensure_base_dirs()
    report = build_report(paths, repo_dir)
    statuses = {k: v["status"] for k, v in report["statuses"].items()}
    assert statuses["managed_colab_run_all"] == "NOT_VERIFIED"
    assert statuses["benchmark_measured"] == "NO"
    assert statuses["live_mbta_capture"] == "NOT_RUN"
    for key in ("unit_tests", "native_full_stack_linux", "docker_full_stack_local", "clean_clone",
                "notebook_schema_validation", "replay_actual_recording", "synthetic_golden_smoke", "offline_test"):
        assert statuses[key] == "NOT_RUN", key


def test_junit_statuses(tmp_path: Path) -> None:
    ok = tmp_path / "ok.xml"
    ok.write_text('<testsuites><testsuite tests="3" failures="0" errors="0" skipped="1"/></testsuites>')
    bad = tmp_path / "bad.xml"
    bad.write_text('<testsuite tests="2" failures="1" errors="0" skipped="0"/>')
    assert junit_status(ok, command="c")["status"] == "PASS"
    assert junit_status(bad, command="c")["status"] == "FAIL"
    assert junit_status(tmp_path / "missing.xml", command="c")["status"] == "NOT_RUN"


def test_golden_report_drives_status(tmp_path: Path, repo_dir: Path) -> None:
    paths = WorkPaths.from_workdir(tmp_path / "w")
    paths.ensure_base_dirs()
    d = paths.reports / "golden_smoke"
    d.mkdir(parents=True)
    (d / "run-1.json").write_text(json.dumps({"status": "PASS", "run_id": "run-1", "steps": [
        {"step": "S1", "passed": True}], "finished_at": "2026-10-06T10:00:00.000Z"}))
    report = build_report(paths, repo_dir)
    golden = report["statuses"]["synthetic_golden_smoke"]
    assert golden["status"] == "PASS" and golden["evidence"] == [str(d / "run-1.json")]
