"""The golden oracle must be independent of the implementation it checks (design C §9, PDF p26).

Walks the AST of every module in ``tests/oracle/`` and fails on:
* any import whose module or imported name contains ``transitpulse`` or ``pyspark``;
* any absolute import outside a small stdlib allowlist;
* relative imports that leave the oracle package;
* dynamic imports (``__import__``, ``importlib``) that could bypass the static check.
A subprocess check additionally verifies that importing the oracle loads neither package.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parents[1]
ORACLE_DIR = TESTS_DIR / "oracle"
FORBIDDEN = ("transitpulse", "pyspark")
STDLIB_ALLOWLIST = {
    "__future__", "csv", "json", "datetime", "zoneinfo", "collections", "math", "re", "pathlib",
    "dataclasses", "typing", "functools", "itertools",
}
ORACLE_MODULES = {"gtfs", "realtime", "scenario"}
EXPECTED_FILES = {"__init__.py", "gtfs.py", "realtime.py", "scenario.py"}


def _oracle_files() -> list[Path]:
    return sorted(ORACLE_DIR.glob("*.py"))


def _imports(path: Path) -> list[tuple[int, str, int, list[str]]]:
    """(lineno, module, level, imported names) for every import statement in the file."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: list[tuple[int, str, int, list[str]]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.append((node.lineno, alias.name, 0, []))
        elif isinstance(node, ast.ImportFrom):
            found.append((node.lineno, node.module or "", node.level, [a.name for a in node.names]))
    return found


def test_oracle_package_has_expected_modules():
    present = {p.name for p in _oracle_files()}
    assert EXPECTED_FILES <= present, f"missing oracle modules: {sorted(EXPECTED_FILES - present)}"


@pytest.mark.parametrize("path", _oracle_files(), ids=lambda p: p.name)
def test_no_forbidden_imports(path: Path):
    offending = []
    for lineno, module, _level, names in _imports(path):
        for text in [module, *names]:
            if any(f in text.lower() for f in FORBIDDEN):
                offending.append(f"{path.name}:{lineno}: {module} {names}")
    assert not offending, "oracle must not import the implementation: " + "; ".join(offending)


@pytest.mark.parametrize("path", _oracle_files(), ids=lambda p: p.name)
def test_only_stdlib_or_sibling_imports(path: Path):
    problems = []
    for lineno, module, level, names in _imports(path):
        if level == 0:
            top = module.split(".")[0]
            if top not in STDLIB_ALLOWLIST:
                problems.append(f"{path.name}:{lineno}: absolute import {module!r} not in stdlib allowlist")
        else:
            if level != 1:
                problems.append(f"{path.name}:{lineno}: relative import leaves the oracle package")
            target = module.split(".")[0] if module else None
            if target is not None and target not in ORACLE_MODULES:
                problems.append(f"{path.name}:{lineno}: relative import of unknown module {module!r}")
            if target is None and not set(names) <= ORACLE_MODULES:
                problems.append(f"{path.name}:{lineno}: 'from . import {names}' outside the oracle modules")
    assert not problems, "; ".join(problems)


@pytest.mark.parametrize("path", _oracle_files(), ids=lambda p: p.name)
def test_no_dynamic_imports(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    bad = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.id if isinstance(func, ast.Name) else (func.attr if isinstance(func, ast.Attribute) else "")
            if name in {"__import__", "import_module", "exec", "eval"}:
                bad.append(f"{path.name}:{node.lineno}: {name}()")
        if isinstance(node, ast.Name) and node.id == "importlib":
            bad.append(f"{path.name}:{node.lineno}: importlib")
    assert not bad, "dynamic imports are not allowed in the oracle: " + "; ".join(bad)


def test_importing_oracle_loads_no_forbidden_module():
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(TESTS_DIR)!r})\n"
        "import oracle.gtfs, oracle.realtime, oracle.scenario\n"
        "bad = sorted(m for m in sys.modules if m.split('.')[0] in ('transitpulse', 'pyspark'))\n"
        "print(','.join(bad))\n"
    )
    proc = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "", f"oracle import pulled in: {proc.stdout.strip()}"
