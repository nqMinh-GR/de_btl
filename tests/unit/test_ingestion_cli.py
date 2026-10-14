"""Unit tests for the ingestion CLI contract: exactly one JSON document on stdout, documented exit codes."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from transitpulse.ingestion.__main__ import main

RUN = "6c7d8e9f-0a1b-4c2d-8e3f-4a5b6c7d8e9f"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for key in list(os.environ):
        if key.startswith("TP_") and key != "TP_REPO_DIR":
            monkeypatch.delenv(key, raising=False)
    yield
    # The CLI installs the JSON log handler on the root logger with the (captured) stderr of this test;
    # remove it so later tests never write to a closed capture stream.
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, "_transitpulse", False):
            root.removeHandler(handler)


def run_cli(capsys: pytest.CaptureFixture[str], argv: list[str]) -> tuple[int, dict[str, Any]]:
    code = main(argv)
    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 1, out                     # exactly one JSON document on stdout
    return code, json.loads(out[0])


def test_usage_error_is_reported_as_json(capsys: pytest.CaptureFixture[str]) -> None:
    code, doc = run_cli(capsys, ["collect", "--workdir", "/tmp/x"])            # --run-id missing
    assert code == 2 and doc["ok"] is False and doc["action"] == "collect" and doc["error"]["code"] == "USAGE"
    code, doc = run_cli(capsys, [])
    assert code == 2 and doc["error"]["code"] == "USAGE"


def test_help_goes_to_stderr(capsys: pytest.CaptureFixture[str]) -> None:
    code, doc = run_cli(capsys, ["--help"])
    assert code == 0 and doc == {"ok": True, "action": "help", "error": None}


def test_unsafe_run_id_is_a_usage_error(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    code, doc = run_cli(capsys, ["outbox-status", "--workdir", str(tmp_path), "--run-id", "../etc"])
    assert code == 2 and doc["error"]["code"] == "USAGE"


def test_source_check_offline(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    code, doc = run_cli(capsys, ["source-check", "--workdir", str(tmp_path), "--offline", "--route-ids", "R1, R2"])
    assert code == 3 and doc["ok"] is False and doc["action"] == "source-check"
    assert doc["error"]["code"] == "SOURCES_NOT_CHECKED"
    assert {s["status"] for s in doc["sources"].values()} == {"not_checked"}
    report = json.loads(Path(doc["report_path"]).read_text(encoding="utf-8"))
    assert report["route_ids"] == ["R1", "R2"]


def test_outbox_status_without_outbox(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    code, doc = run_cli(capsys, ["outbox-status", "--workdir", str(tmp_path), "--run-id", RUN])
    assert code == 3 and doc["exists"] is False and doc["error"]["code"] == "OUTBOX_NOT_FOUND"


def test_disk_check_lists_runs(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    run_dir = tmp_path / "data" / "raw" / RUN / "alerts" / "2026-10-05"
    run_dir.mkdir(parents=True)
    (run_dir / "x.json.gz").write_bytes(b"x" * 1234)
    code, doc = run_cli(capsys, ["disk-check", "--workdir", str(tmp_path)])
    assert code in (0, 3) and doc["action"] == "disk-check"
    assert doc["runs"] == [{"run_id": RUN, "raw_bytes": 1234, "used_fraction": round(1234 / doc["budget_bytes_per_run"], 6),
                            "state": "ok"}]


def test_collect_for_a_missing_run_is_a_preflight_error(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    code, doc = run_cli(capsys, ["record", "--workdir", str(tmp_path), "--run-id", RUN, "--duration-seconds", "1"])
    assert code == 3 and doc["ok"] is False and doc["action"] == "record"
    assert doc["error"]["code"] == "RUN_NOT_FOUND"
