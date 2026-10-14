"""Demo session lifecycle: prepare, start, wait-ready, status, logs, pause/resume/step/advance-clock/inject,
restart-processor, stop, reset, golden-smoke, lineage, benchmark, expose (public dashboard URL through a
Cloudflare quick tunnel: `demo.py expose start --workdir W`).

    <repo>/.venv/bin/python scripts/demo.py start --profile colab-native --mode synthetic --seed 42 \
        --duration-seconds 600 --detach --workdir W
    <repo>/.venv/bin/python scripts/demo.py status --workdir W

Prints one JSON document on stdout (the notebook reads it; it never hard-codes a run id); progress on
stderr. Implementation: transitpulse.runtime.cli_demo.
"""

from __future__ import annotations

import json
import sys

try:
    from transitpulse.runtime.cli_demo import main
except ImportError as exc:
    sys.stdout.write(json.dumps({"ok": False, "action": "demo", "error": {
        "code": "CORE_ENV_MISSING", "message": f"cannot import transitpulse: {exc}",
        "hint": "run with <repo>/.venv/bin/python after `bash scripts/bootstrap_colab.sh --workdir W`",
        "log_excerpt": None}}) + "\n")
    sys.exit(3)

if __name__ == "__main__":
    sys.exit(main())
