"""Bootstrap phase B (normally exec'd by scripts/bootstrap_colab.sh with the core .venv interpreter).

    <repo>/.venv/bin/python scripts/prepare_runtime.py --workdir W [--repo-dir R] [--strict] [--offline]
                                                       [--emit-pin-patch]

Installs/verifies JDK 17 (Temurin), Kafka 4.2.2 and Cassandra 5.0.9 with official checksums, resolves the
Spark Kafka connector closure, renders native configs, writes W/runtime/runtime.json + runtime.env and
W/runtime/bootstrap_report.json, and runs compatibility probes. Prints one JSON document on stdout.
Implementation: transitpulse.runtime.bootstrap.
"""

from __future__ import annotations

import json
import sys

try:
    from transitpulse.runtime.bootstrap import main
except ImportError as exc:  # the package is installed into the core .venv by `uv sync`
    sys.stdout.write(json.dumps({"ok": False, "action": "bootstrap", "error": {
        "code": "CORE_ENV_MISSING", "message": f"cannot import transitpulse: {exc}",
        "hint": "run `bash scripts/bootstrap_colab.sh --workdir W` (it creates .venv and runs this script)",
        "log_excerpt": None}}) + "\n")
    sys.exit(3)

if __name__ == "__main__":
    sys.exit(main())
