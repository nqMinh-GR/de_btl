"""Doctor threshold evaluation and port status mapping."""

from __future__ import annotations

import pytest

from transitpulse.runtime.doctor import classify_port_status, evaluate_min, summarize
from transitpulse.runtime.proc import GIB, parse_meminfo


@pytest.mark.ct_case("CT06")
@pytest.mark.parametrize("observed,fail,warn,expected", [
    (12 * GIB, 10 * GIB, None, "PASS"),
    (9 * GIB, 10 * GIB, None, "FAIL"),
    (8 * GIB, 6 * GIB, 10 * GIB, "WARN"),
    (5 * GIB, 6 * GIB, 10 * GIB, "FAIL"),
    (10 * GIB, 6 * GIB, 10 * GIB, "PASS"),
    (None, 6 * GIB, 10 * GIB, "WARN"),
    (2, 2, None, "PASS"),
    (1, 2, None, "FAIL"),
])
def test_evaluate_min(observed, fail, warn, expected) -> None:
    assert evaluate_min(observed, fail_below=fail, warn_below=warn) == expected


@pytest.mark.ct_case("CT06")
@pytest.mark.parametrize("state,expected", [
    ("FREE", "PASS"), ("OWNED", "PASS"), ("OURS_UNREGISTERED", "WARN"), ("UNKNOWN_OWNER", "WARN"), ("FOREIGN", "FAIL"),
])
def test_port_status(state: str, expected: str) -> None:
    status, hint = classify_port_status(state, None)
    assert status == expected
    if expected != "PASS":
        assert hint


def test_summary_counts() -> None:
    checks = [{"status": "PASS"}, {"status": "WARN"}, {"status": "FAIL"}, {"status": "PASS"}, {"status": "INFO"}]
    assert summarize(checks) == {"PASS": 2, "WARN": 1, "FAIL": 1, "INFO": 1}


def test_meminfo_parsing_converts_kb_to_bytes() -> None:
    text = "MemTotal:       13290456 kB\nMemAvailable:   11817380 kB\nHugePages_Total:       0\n"
    mem = parse_meminfo(text)
    assert mem["MemTotal"] == 13290456 * 1024
    assert mem["MemAvailable"] == 11817380 * 1024
    assert mem["HugePages_Total"] == 0
