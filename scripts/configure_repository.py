"""Set the GitHub repository URL in the README Open-in-Colab badge and in the notebook (parameters cell, intro
badge, metadata). Runs with the plain system Python BEFORE pushing; no .venv and no third-party package needed:

    python3 scripts/configure_repository.py --repo-url https://github.com/OWNER/REPO.git [--ref main] [--dry-run]

Rejects credentials in URLs, non-https and non-github.com URLs. Idempotent; prints the diff on stderr and one
JSON document on stdout. Implementation: transitpulse.runtime.repo_config (stdlib-only, Python >= 3.9).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
# This script must work without the core .venv (the editable install may not exist yet): import the package
# straight from the checkout. repo_config and cli_common use the standard library only.
sys.path.insert(0, str(REPO_ROOT / "src"))

try:
    from transitpulse.runtime.repo_config import main
except ImportError as exc:
    sys.stdout.write(json.dumps({"ok": False, "action": "configure-repository", "error": {
        "code": "IMPORT_FAILED", "message": f"cannot import transitpulse.runtime.repo_config: {exc}",
        "hint": f"run from a complete checkout ({REPO_ROOT}/src must exist) with python3 >= 3.9",
        "log_excerpt": None}}) + "\n")
    sys.exit(3)

if __name__ == "__main__":
    sys.exit(main(default_repo_dir=REPO_ROOT))
