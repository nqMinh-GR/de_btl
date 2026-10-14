"""The supervisor's process wrapper, exercised with tiny Python children (no services, a few seconds).

CT08: a process that exits (or is stopped by its budget) leaves a clear exit code/signal record.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.ct_case("CT08")

TOKEN = "tp:000000000000:api:test"


def _run_wrapper(tmp_path: Path, child: list[str], *extra: str, timeout: float = 60) -> tuple[int, dict]:
    exit_file = tmp_path / "exits" / "api.json"
    argv = [sys.executable, "-m", "transitpulse.runtime.runner", "--owner", TOKEN, "--component", "api",
            "--exit-file", str(exit_file), *extra, "--", *child]
    rc = subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True, timeout=timeout, check=False).returncode
    return rc, json.loads(exit_file.read_text())


def test_exit_code_is_recorded(tmp_path: Path) -> None:
    rc, data = _run_wrapper(tmp_path, [sys.executable, "-c", "import sys; sys.exit(3)"])
    assert rc == 3
    assert data["state"] == "EXITED" and data["exit_code"] == 3 and data["signal"] is None
    assert data["owner"] == TOKEN and isinstance(data["child_pid"], int)


def test_deadline_sends_sigterm_to_child(tmp_path: Path) -> None:
    t0 = time.monotonic()
    rc, data = _run_wrapper(tmp_path, [sys.executable, "-c", "import time; time.sleep(60)"],
                            "--deadline", f"{time.time() + 1.0:.3f}", "--term-timeout", "10")
    assert time.monotonic() - t0 < 30
    assert data["terminated_by"] == "deadline"
    assert data["signal"] == 15 and data["exit_code"] is None
    assert rc == 128 + 15


def test_missing_executable_reports_127(tmp_path: Path) -> None:
    rc, data = _run_wrapper(tmp_path, [str(tmp_path / "does-not-exist")])
    assert rc == 127
    assert data["exit_code"] == 127 and "error" in data
