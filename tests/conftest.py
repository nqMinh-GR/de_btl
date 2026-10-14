"""Root pytest configuration.

* Markers are applied from the directory: tests/unit -> unit, tests/spark -> spark,
  tests/integration -> integration, tests/runtime -> runtime. Tests outside these trees must mark
  themselves explicitly.
* Fixture/oracle paths are exposed as fixtures so tests never hard-code absolute paths.
"""

from __future__ import annotations

from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parent
REPO_DIR = TESTS_DIR.parent
GOLDEN_DIR = TESTS_DIR / "fixtures" / "golden"

_DIR_MARKERS = {"unit": "unit", "spark": "spark", "integration": "integration", "runtime": "runtime"}


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        try:
            rel = Path(str(item.fspath)).resolve().relative_to(TESTS_DIR)
        except ValueError:
            continue
        top = rel.parts[0] if rel.parts else ""
        marker = _DIR_MARKERS.get(top)
        if marker and not item.get_closest_marker(marker):
            item.add_marker(getattr(pytest.mark, marker))


@pytest.fixture(scope="session")
def repo_dir() -> Path:
    return REPO_DIR


@pytest.fixture(scope="session")
def golden_dir() -> Path:
    return GOLDEN_DIR
