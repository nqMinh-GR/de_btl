"""``scripts/configure_repository.py``: set the GitHub repository URL once the remote is known.

    python3 scripts/configure_repository.py --repo-url https://github.com/OWNER/REPO.git [--ref main] [--dry-run]

Runs on the user's machine BEFORE pushing, with the plain system ``python3`` (no .venv): this module and
everything it imports (``cli_common``) are stdlib-only and Python 3.9 compatible; it never imports
``transitpulse.config``, yaml or pydantic. The wrapper puts ``<repo>/src`` on ``sys.path`` and passes the repo root.

* Validates the URL: https only, host exactly github.com, no userinfo (tokens), no port/query/fragment,
  path ``/OWNER/REPO`` or ``/OWNER/REPO.git``; placeholders like ``<OWNER>`` are rejected.
* README.md: replaces the content between ``<!-- colab-badge:start -->`` and ``<!-- colab-badge:end -->`` with
  the Open-in-Colab badge for ``notebooks/TransitPulse_MBTA_Colab_Demo.ipynb`` at ``--ref``.
* Notebook (edited as JSON): rewrites ``GITHUB_REPO_URL = ...`` (and ``GIT_REF = ...`` when --ref is given) in the
  cell tagged ``parameters``, rewrites every Open-in-Colab URL of this notebook in markdown cells (the intro
  badge still holding ``<OWNER>/<REPO>``), sets ``metadata.transitpulse.repo_url`` and clears all outputs.
Idempotent; prints a unified diff on stderr and one JSON document on stdout.
"""

from __future__ import annotations

import argparse
import difflib
import json
import logging
import os
import re
import urllib.parse
from pathlib import Path
from typing import Any

from transitpulse.runtime.cli_common import EXIT_USAGE, CliError, JsonArgumentParser, Result, run_main

log = logging.getLogger("transitpulse.runtime.repo_config")


def atomic_write_text(path: Path, text: str) -> None:
    """Stdlib-only atomic write (temp file in the same directory + fsync + os.replace)."""
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)

NOTEBOOK_REL = "notebooks/TransitPulse_MBTA_Colab_Demo.ipynb"
BADGE_START = "<!-- colab-badge:start -->"
BADGE_END = "<!-- colab-badge:end -->"
_OWNER = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})$")
_REPO = re.compile(r"^[A-Za-z0-9._-]{1,100}$")
_REF = re.compile(r"^[A-Za-z0-9._/-]{1,200}$")
# Any Open-in-Colab URL of this notebook: owner/repo may be placeholders (<OWNER>), ref may contain '/'.
COLAB_NOTEBOOK_URL = re.compile(
    r"https://colab\.research\.google\.com/github/[^/\s)`\"']+/[^/\s)`\"']+/blob/[^\s)`\"']+?/"
    + re.escape(NOTEBOOK_REL)
)


class RepoUrlError(ValueError):
    pass


def normalize_repo_url(url: str) -> tuple[str, str, str]:
    """Return ``(normalized_url, owner, repo)``; raise RepoUrlError for anything unsafe or not GitHub."""
    if not isinstance(url, str) or not url.strip():
        raise RepoUrlError("repository URL is empty")
    url = url.strip()
    if "<" in url or ">" in url:
        raise RepoUrlError("URL still contains a placeholder such as <OWNER>/<REPO>")
    parts = urllib.parse.urlsplit(url)
    if parts.scheme != "https":
        raise RepoUrlError("only https:// URLs are accepted")
    if parts.username is not None or parts.password is not None or "@" in parts.netloc:
        raise RepoUrlError("URLs with credentials (user:token@) are rejected; never embed tokens in URLs")
    if parts.netloc.lower() != "github.com":
        raise RepoUrlError("host must be exactly github.com (no port, no other host)")
    if parts.query or parts.fragment:
        raise RepoUrlError("query strings and fragments are not allowed")
    segs = [s for s in parts.path.split("/") if s]
    if len(segs) != 2:
        raise RepoUrlError("path must be /OWNER/REPO or /OWNER/REPO.git")
    owner, repo = segs
    if repo.endswith(".git"):
        repo = repo[:-4]
    if not _OWNER.match(owner) or owner.endswith("-") or "--" in owner:
        raise RepoUrlError(f"invalid GitHub owner {owner!r}")
    if not _REPO.match(repo) or repo in (".", ".."):
        raise RepoUrlError(f"invalid GitHub repository name {repo!r}")
    return f"https://github.com/{owner}/{repo}.git", owner, repo


def validate_ref(ref: str) -> str:
    if re.fullmatch(r"[0-9a-f]{40}", ref):
        return ref
    if not _REF.match(ref) or ".." in ref or ref.startswith(("-", "/")) or ref.endswith(("/", ".lock")) \
            or "//" in ref:
        raise RepoUrlError(f"invalid git ref {ref!r}")
    return ref


def colab_url(owner: str, repo: str, ref: str) -> str:
    return f"https://colab.research.google.com/github/{owner}/{repo}/blob/{ref}/{NOTEBOOK_REL}"


def badge_markdown(owner: str, repo: str, ref: str) -> str:
    return (f"[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)]"
            f"({colab_url(owner, repo, ref)})")


def update_readme_text(text: str, badge: str) -> str:
    start = text.find(BADGE_START)
    end = text.find(BADGE_END)
    if start < 0 or end < 0 or end < start:
        raise CliError("README_MARKERS_MISSING", f"README.md must contain {BADGE_START} ... {BADGE_END}",
                       hint="add the two marker lines where the badge should appear", exit_code=EXIT_USAGE)
    return text[: start + len(BADGE_START)] + "\n" + badge + "\n" + text[end:]


def _set_line(source: str, var: str, value: str) -> tuple[str, bool]:
    pattern = re.compile(rf"^(\s*{var}\s*=\s*)(\"[^\"\n]*\"|'[^'\n]*')(.*)$", re.MULTILINE)
    new, n = pattern.subn(lambda m: f"{m.group(1)}{json.dumps(value)}{m.group(3)}", source, count=1)
    return new, n > 0


def update_notebook(nb: dict[str, Any], url: str, ref: str | None) -> dict[str, Any]:
    cells = nb.get("cells")
    if not isinstance(cells, list):
        raise CliError("NOTEBOOK_INVALID", "notebook has no cells list", exit_code=EXIT_USAGE)
    target = None
    for cell in cells:
        tags = ((cell.get("metadata") or {}).get("tags")) or []
        if cell.get("cell_type") == "code" and "parameters" in tags:
            target = cell
            break
    if target is None:
        raise CliError("NOTEBOOK_PARAMETERS_MISSING", "no code cell tagged 'parameters' in the notebook",
                       exit_code=EXIT_USAGE)
    src = target.get("source", "")
    as_list = isinstance(src, list)
    text = "".join(src) if as_list else str(src)
    text, ok = _set_line(text, "GITHUB_REPO_URL", url)
    if not ok:
        raise CliError("NOTEBOOK_PARAMETERS_MISSING", "GITHUB_REPO_URL = \"...\" not found in the parameters cell",
                       exit_code=EXIT_USAGE)
    if ref is not None:
        text, ok = _set_line(text, "GIT_REF", ref)
        if not ok:
            raise CliError("NOTEBOOK_PARAMETERS_MISSING", "GIT_REF = \"...\" not found in the parameters cell",
                           exit_code=EXIT_USAGE)
    target["source"] = text.splitlines(keepends=True) if as_list else text
    _, owner, repo = normalize_repo_url(url)
    new_colab = colab_url(owner, repo, ref or "main")
    for cell in cells:
        if cell.get("cell_type") == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
        elif cell.get("cell_type") == "markdown":
            # The intro badge / "Open in Colab" URL of THIS notebook (e.g. still <OWNER>/<REPO>). Other GitHub
            # links and instructional examples such as `--repo-url https://github.com/<owner>/<repo>.git` stay.
            msrc = cell.get("source", "")
            m_list = isinstance(msrc, list)
            mtext = "".join(msrc) if m_list else str(msrc)
            replaced = COLAB_NOTEBOOK_URL.sub(new_colab, mtext)
            if replaced != mtext:
                cell["source"] = replaced.splitlines(keepends=True) if m_list else replaced
    meta = nb.setdefault("metadata", {})
    tp = meta.setdefault("transitpulse", {})
    tp["repo_url"] = url
    if ref is not None:
        tp["ref"] = ref
    return nb


def dump_notebook(nb: dict[str, Any]) -> str:
    return json.dumps(nb, indent=1, ensure_ascii=False) + "\n"


def default_repo_root() -> Path:
    """``src/transitpulse/runtime/repo_config.py`` -> repository root (no transitpulse.config import)."""
    return Path(__file__).resolve().parents[3]


def configure(args: argparse.Namespace) -> Result:
    try:
        url, owner, repo = normalize_repo_url(args.repo_url)
        ref = validate_ref(args.ref) if args.ref is not None else None
    except RepoUrlError as exc:
        raise CliError("INVALID_REPO_URL", str(exc), hint="example: https://github.com/OWNER/REPO.git",
                       exit_code=EXIT_USAGE) from exc
    root = Path(args.repo_dir).resolve() if args.repo_dir else default_repo_root()
    badge_ref = ref or "main"
    changes: list[dict[str, Any]] = []
    diffs: list[str] = []

    readme = root / "README.md"
    if not readme.is_file():
        raise CliError("README_MISSING", f"{readme} not found", exit_code=EXIT_USAGE)
    old = readme.read_text(encoding="utf-8")
    new = update_readme_text(old, badge_markdown(owner, repo, badge_ref))
    if new != old:
        changes.append({"file": "README.md"})
        diffs.append("".join(difflib.unified_diff(old.splitlines(True), new.splitlines(True), "a/README.md",
                                                  "b/README.md")))
        if not args.dry_run:
            atomic_write_text(readme, new)

    nb_path = root / NOTEBOOK_REL
    if not nb_path.is_file():
        raise CliError("NOTEBOOK_MISSING", f"{nb_path} not found", exit_code=EXIT_USAGE)
    old_nb_text = nb_path.read_text(encoding="utf-8")
    try:
        nb = json.loads(old_nb_text)
    except ValueError as exc:
        raise CliError("NOTEBOOK_INVALID", f"{nb_path} is not valid JSON: {exc}", exit_code=EXIT_USAGE) from exc
    new_nb_text = dump_notebook(update_notebook(nb, url, ref))
    if new_nb_text != old_nb_text:
        changes.append({"file": NOTEBOOK_REL})
        diffs.append("".join(difflib.unified_diff(old_nb_text.splitlines(True), new_nb_text.splitlines(True),
                                                  f"a/{NOTEBOOK_REL}", f"b/{NOTEBOOK_REL}", n=1)))
        if not args.dry_run:
            atomic_write_text(nb_path, new_nb_text)
    diff = "".join(diffs)
    for line in diff.splitlines()[:400]:
        log.info("%s", line)
    return Result({"repo_url": url, "owner": owner, "repo": repo, "ref": ref, "colab_url": colab_url(owner, repo,
                   badge_ref), "changed_files": [c["file"] for c in changes], "dry_run": bool(args.dry_run),
                   "diff": diff[:20000]})


def build_parser() -> JsonArgumentParser:
    p = JsonArgumentParser(prog="configure_repository.py",
                           description="Set the GitHub repository URL in the README badge and the notebook parameters.")
    p.add_argument("--repo-url", required=True, help="https://github.com/OWNER/REPO.git (no credentials)")
    p.add_argument("--ref", default=None, help="branch, tag or 40-hex commit for the badge and GIT_REF")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--repo-dir", default=None)
    return p


def main(argv: list[str] | None = None, *, default_repo_dir: Path | None = None) -> int:
    def handler(args: argparse.Namespace) -> Result:
        if not args.repo_dir and default_repo_dir is not None:
            args.repo_dir = str(default_repo_dir)
        return configure(args)

    return run_main(lambda _a: "configure-repository", build_parser(), handler, argv)


if __name__ == "__main__":  # python -m transitpulse.runtime.repo_config ...
    import sys

    sys.exit(main())
