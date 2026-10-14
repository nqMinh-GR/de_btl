"""Write W/reports/environment_verification.json (spec §27.3) from evidence files that exist.

    <repo>/.venv/bin/python scripts/verify_environment.py --workdir W [--out PATH]

Statuses: Unit tests, Native full-stack Linux, Docker full-stack local, Clean clone, Notebook schema
validation (PASS/FAIL/NOT_RUN), Managed Google Colab Run All (PASS/FAIL/NOT_VERIFIED), Live MBTA capture
(VERIFIED/FAILED/NOT_RUN), Replay actual recording, Synthetic golden smoke, Offline test (PASS/FAIL/NOT_RUN),
Benchmark measured (YES/NO). Nothing is reported as passing without an evidence file.
Implementation: transitpulse.runtime.verification.
"""

from __future__ import annotations

import json
import sys

try:
    from transitpulse.runtime.verification import main
except ImportError as exc:
    sys.stdout.write(json.dumps({"ok": False, "action": "verify-environment", "error": {
        "code": "CORE_ENV_MISSING", "message": f"cannot import transitpulse: {exc}",
        "hint": "run with <repo>/.venv/bin/python after bootstrap", "log_excerpt": None}}) + "\n")
    sys.exit(3)

if __name__ == "__main__":
    sys.exit(main())
