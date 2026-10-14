"""Package evidence and reproducible inputs into W/artifacts/transitpulse_<run>_<ts>.zip (+ manifest).

    <repo>/.venv/bin/python scripts/export_artifacts.py [--session-id S | --run-id R] [--max-zip-mb 200] \
        [--log-tail-lines 2000] [--no-static-zip] --workdir W

Never includes runtime.env/environment dumps, tokens or live Kafka/Cassandra files. Prints one JSON document
with ``zip_path``, ``zip_sha256``, ``completeness`` (REPLAYABLE|EVIDENCE_ONLY). Implementation:
transitpulse.runtime.export.
"""

from __future__ import annotations

import json
import sys

try:
    from transitpulse.runtime.export import main
except ImportError as exc:
    sys.stdout.write(json.dumps({"ok": False, "action": "export", "error": {
        "code": "CORE_ENV_MISSING", "message": f"cannot import transitpulse: {exc}",
        "hint": "run with <repo>/.venv/bin/python after `bash scripts/bootstrap_colab.sh --workdir W`",
        "log_excerpt": None}}) + "\n")
    sys.exit(3)

if __name__ == "__main__":
    sys.exit(main())
