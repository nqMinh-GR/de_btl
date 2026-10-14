"""Kafka + Cassandra lifecycle for the native profiles (no Docker, no systemd, no sudo).

    <repo>/.venv/bin/python scripts/infra.py start  --profile colab-native --workdir W
    <repo>/.venv/bin/python scripts/infra.py init   --profile colab-native --workdir W
    <repo>/.venv/bin/python scripts/infra.py status|logs|stop|adopt ... --workdir W

Prints one JSON document on stdout; progress on stderr. Implementation: transitpulse.runtime.cli_infra.
"""

from __future__ import annotations

import json
import sys

try:
    from transitpulse.runtime.cli_infra import main
except ImportError as exc:
    sys.stdout.write(json.dumps({"ok": False, "action": "infra", "error": {
        "code": "CORE_ENV_MISSING", "message": f"cannot import transitpulse: {exc}",
        "hint": "run with <repo>/.venv/bin/python after `bash scripts/bootstrap_colab.sh --workdir W`",
        "log_excerpt": None}}) + "\n")
    sys.exit(3)

if __name__ == "__main__":
    sys.exit(main())
