"""IT16: the demo works with the network off (synthetic replay -> Spark -> Cassandra -> API, UI assets).

Method selection (decision D-E26, recorded in the report):
1. ``netns``: ``bwrap --dev-bind / / --unshare-net --die-with-parent`` is usable -> the host-namespace infra is stopped
   (owned processes only), the probe starts its own Kafka/Cassandra on the namespace loopback and runs everything
   there; afterwards the host infra is started again (disruptive, hence ordered last);
2. ``guard``: otherwise every Python process of the probe gets the ``sitecustomize`` socket guard plus black-hole
   HTTP(S)/JVM proxies, in the host namespace (limits are reported by the probe).
The browser pass (Playwright) is optional and reported NOT_RUN inside the evidence when unavailable.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.slow]

BWRAP_ARGS = ("--dev-bind", "/", "/", "--unshare-net", "--die-with-parent")


def netns_available() -> tuple[bool, str]:
    exe = shutil.which("bwrap")
    if exe is None:
        return False, "bwrap not installed"
    try:
        proc = subprocess.run([exe, *BWRAP_ARGS, "--", "/bin/true"], capture_output=True, text=True, timeout=30,
                              check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"bwrap probe failed: {exc}"
    if proc.returncode != 0:
        return False, f"bwrap cannot create a network namespace (rc {proc.returncode}): {proc.stderr.strip()[:300]}"
    return True, exe


@pytest.mark.it_case("IT16")
def test_it16_offline_replay(harness: Any, case_record: Any) -> None:
    out_dir = harness.evidence_dir / "offline"
    out_dir.mkdir(parents=True, exist_ok=True)
    report = out_dir / "offline_probe_report.json"
    usable, detail = netns_available()
    method = "netns" if usable else "guard"
    probe = [str(harness.runtime.core_python), "-m", "transitpulse.verify.offline_probe", "--workdir",
             str(harness.paths.root), "--profile", harness.profile, "--method", method, "--report", str(report),
             "--speed", "10"]
    env = dict(harness.env)
    if harness.component_alive("processor"):
        harness.stop_processor()
    infra_restarted = None
    if method == "netns":
        harness.infra("stop", timeout=600)
        argv = [detail, *BWRAP_ARGS, "--", *probe, "--manage-infra"]
    else:
        argv = probe
    try:
        proc = subprocess.run(argv, env=env, cwd=str(harness.repo_dir), capture_output=True, text=True,
                              timeout=2700, check=False)
    finally:
        if method == "netns":
            harness.infra("stop", timeout=600, check=False)
            infra_restarted = harness.infra("start", timeout=900)
            harness.infra("init", timeout=600)
    (out_dir / "offline_probe.stderr.log").write_text(proc.stderr[-200000:], encoding="utf-8")
    try:
        result = json.loads(report.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        result = {"ok": False, "error": {"message": f"no probe report; stdout={proc.stdout[-2000:]!r}"}}
    case_record.evidence(str(report))
    case_record.expected(generations_ready=True, api_200=True, no_external_assets=True, spark_without_packages=True)
    case_record.actual(method=method, method_detail=detail if method == "netns" else f"no netns: {detail}",
                       checks=result.get("checks"), not_run=result.get("not_run"),
                       blocked_connection_count=result.get("blocked_connection_count"), limits=result.get("limits"),
                       infra_restarted=bool(infra_restarted), probe_exit_code=proc.returncode)
    failed = [c for c in result.get("checks") or [] if not c.get("passed") and not c.get("informational")]
    assert result.get("ok") and proc.returncode == 0, (
        f"offline probe ({method}) failed: error={result.get('error')} failed_checks={failed} "
        f"stderr tail={proc.stderr[-1500:]}")
    assert Path(report).is_file()
