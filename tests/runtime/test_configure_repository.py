"""configure_repository: URL validation, README badge markers, notebook parameters cell (JSON-level edit)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from transitpulse.runtime.cli_common import CliError
from transitpulse.runtime.repo_config import (
    BADGE_END,
    BADGE_START,
    NOTEBOOK_REL,
    RepoUrlError,
    configure,
    normalize_repo_url,
    update_notebook,
    update_readme_text,
    validate_ref,
)


@pytest.mark.parametrize("url,expected", [
    ("https://github.com/owner/repo.git", "https://github.com/owner/repo.git"),
    ("https://github.com/owner/repo", "https://github.com/owner/repo.git"),
    ("https://github.com/Some-Org/transit.pulse_mbta/", "https://github.com/Some-Org/transit.pulse_mbta.git"),
])
def test_valid_urls_are_normalized(url: str, expected: str) -> None:
    assert normalize_repo_url(url)[0] == expected


@pytest.mark.parametrize("url", [
    "http://github.com/owner/repo.git",                 # not https
    "https://user:ghp_secret@github.com/owner/repo.git",  # credentials
    "https://token@github.com/owner/repo.git",
    "https://gitlab.com/owner/repo.git",                # not github
    "https://github.com:443/owner/repo.git",            # port
    "https://github.com.evil.example/owner/repo.git",
    "https://github.com/owner/repo.git?x=1",
    "https://github.com/owner",                          # missing repo
    "https://github.com/owner/repo/extra",
    "https://github.com/<OWNER>/<REPO>.git",             # placeholder
    "git@github.com:owner/repo.git",
    "",
])
def test_invalid_urls_rejected(url: str) -> None:
    with pytest.raises(RepoUrlError):
        normalize_repo_url(url)


@pytest.mark.parametrize("ref", ["main", "release/v1.0", "a" * 40, "v0.1.0"])
def test_valid_refs(ref: str) -> None:
    assert validate_ref(ref) == ref


@pytest.mark.parametrize("ref", ["-x", "../main", "a..b", "/abs", "x.lock", "x y"])
def test_invalid_refs(ref: str) -> None:
    with pytest.raises(RepoUrlError):
        validate_ref(ref)


def test_readme_badge_replaced_between_markers_idempotently() -> None:
    text = f"# T\n\n{BADGE_START}\nold badge\n{BADGE_END}\n\nrest\n"
    once = update_readme_text(text, "[badge]")
    assert once == f"# T\n\n{BADGE_START}\n[badge]\n{BADGE_END}\n\nrest\n"
    assert update_readme_text(once, "[badge]") == once


def test_readme_without_markers_fails() -> None:
    with pytest.raises(CliError):
        update_readme_text("# no markers\n", "[badge]")


PLACEHOLDER_COLAB = ("https://colab.research.google.com/github/<OWNER>/<REPO>/blob/main/notebooks/"
                     "TransitPulse_MBTA_Colab_Demo.ipynb")
INSTRUCTION = "chạy `scripts/configure_repository.py --repo-url https://github.com/<owner>/<repo>.git`\n"


def _notebook() -> dict:
    return {
        "nbformat": 4, "nbformat_minor": 5, "metadata": {"transitpulse": {"repo_url": "https://github.com/<OWNER>/<REPO>.git"}},
        "cells": [
            {"cell_type": "markdown", "metadata": {},
             "source": ["# Title\n",
                        f"[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)]({PLACEHOLDER_COLAB})\n",
                        f"Open in Colab: `{PLACEHOLDER_COLAB}`\n", INSTRUCTION,
                        "Docs: https://github.com/mbta/gtfs-documentation\n"]},
            {"cell_type": "code", "metadata": {"tags": ["parameters"]}, "execution_count": 3,
             "outputs": [{"output_type": "stream", "name": "stdout", "text": ["secret?\n"]}],
             "source": ["GITHUB_REPO_URL = \"https://github.com/<OWNER>/<REPO>.git\"  # repo\n",
                        "GIT_REF = \"main\"\n", "DEMO_MODE = \"synthetic\"\n"]},
            {"cell_type": "code", "metadata": {}, "execution_count": 4, "outputs": [{"output_type": "error"}],
             "source": "print('x')"},
        ],
    }


def test_notebook_parameters_updated_and_outputs_cleared() -> None:
    nb = update_notebook(_notebook(), "https://github.com/o/r.git", "v1")
    src = "".join(nb["cells"][1]["source"])
    assert 'GITHUB_REPO_URL = "https://github.com/o/r.git"  # repo' in src
    assert 'GIT_REF = "v1"' in src
    assert 'DEMO_MODE = "synthetic"' in src
    assert isinstance(nb["cells"][1]["source"], list)
    assert all(c.get("outputs", []) == [] for c in nb["cells"] if c["cell_type"] == "code")
    assert all(c.get("execution_count") is None for c in nb["cells"] if c["cell_type"] == "code")
    assert nb["metadata"]["transitpulse"]["repo_url"] == "https://github.com/o/r.git"


def test_notebook_intro_badge_rewritten_but_instructions_kept() -> None:
    nb = update_notebook(_notebook(), "https://github.com/o/r.git", "v1")
    intro = "".join(nb["cells"][0]["source"])
    expected = "https://colab.research.google.com/github/o/r/blob/v1/notebooks/TransitPulse_MBTA_Colab_Demo.ipynb"
    assert "<OWNER>" not in intro.replace(INSTRUCTION, "")
    assert intro.count(expected) == 2
    assert INSTRUCTION in intro                                   # instructional example untouched
    assert "https://github.com/mbta/gtfs-documentation" in intro   # unrelated GitHub links untouched
    again = update_notebook(nb, "https://github.com/o2/r2.git", "main")  # reconfigure to another repo
    assert "github/o2/r2/blob/main/notebooks" in "".join(again["cells"][0]["source"])


def test_configure_modules_are_stdlib_only(repo_dir: Path) -> None:
    """configure_repository.py runs with the system python3 before any .venv exists."""
    import ast

    allowed_internal = {"transitpulse.runtime.cli_common"}
    for rel in ("src/transitpulse/runtime/repo_config.py", "src/transitpulse/runtime/cli_common.py"):
        tree = ast.parse((repo_dir / rel).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            else:
                continue
            for name in names:
                assert not name.startswith(("yaml", "pydantic", "transitpulse.config")) or \
                    _inside_function(tree, node), f"{rel} imports {name} at module level"
                if name.startswith("transitpulse") and not _inside_function(tree, node):
                    assert name in allowed_internal, f"{rel} imports {name} at module level"


def _inside_function(tree, target) -> bool:
    import ast

    for fn in ast.walk(tree):
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(n is target for n in ast.walk(fn)):
            return True
    return False


def test_script_inserts_src_and_passes_repo_root(repo_dir: Path) -> None:
    text = (repo_dir / "scripts" / "configure_repository.py").read_text(encoding="utf-8")
    assert 'sys.path.insert(0, str(REPO_ROOT / "src"))' in text
    assert "main(default_repo_dir=REPO_ROOT)" in text


def test_notebook_without_parameters_cell_fails() -> None:
    nb = _notebook()
    nb["cells"][1]["metadata"] = {}
    with pytest.raises(CliError):
        update_notebook(nb, "https://github.com/o/r.git", None)


def test_configure_end_to_end_dry_run_and_apply(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text(f"# T\n{BADGE_START}\n{BADGE_END}\n", encoding="utf-8")
    nb_path = tmp_path / NOTEBOOK_REL
    nb_path.parent.mkdir(parents=True)
    nb_path.write_text(json.dumps(_notebook()), encoding="utf-8")
    args = argparse.Namespace(repo_url="https://github.com/o/r", ref="main", dry_run=True, repo_dir=str(tmp_path))
    res = configure(args)
    assert set(res.payload["changed_files"]) == {"README.md", NOTEBOOK_REL}
    assert BADGE_START + "\n" + BADGE_END in (tmp_path / "README.md").read_text()  # dry run: unchanged
    args.dry_run = False
    configure(args)
    readme = (tmp_path / "README.md").read_text()
    assert "colab.research.google.com/github/o/r/blob/main/notebooks/TransitPulse_MBTA_Colab_Demo.ipynb" in readme
    again = configure(args)
    assert again.payload["changed_files"] == []
