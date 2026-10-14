"""Native runtime preflight: resources, versions, ports, configuration and lock hashes.

    <repo>/.venv/bin/python scripts/doctor.py --profile colab-native [--kernel-python 3.12.11] [--check-network] \
        --workdir W

Prints one JSON document ``{ok, action: "doctor", checks: [{id, status, observed, threshold, hint}], summary}``;
exit code 3 when a check FAILs. Implementation: transitpulse.runtime.doctor.
"""

from __future__ import annotations

import json
import sys

try:
    from transitpulse.runtime.doctor import main
except ImportError as exc:
    sys.stdout.write(json.dumps({"ok": False, "action": "doctor", "error": {
        "code": "CORE_ENV_MISSING", "message": f"cannot import transitpulse: {exc}",
        "hint": "run with <repo>/.venv/bin/python after `bash scripts/bootstrap_colab.sh --workdir W`",
        "log_excerpt": None}}) + "\n")
    sys.exit(3)

if __name__ == "__main__":
    sys.exit(main())
