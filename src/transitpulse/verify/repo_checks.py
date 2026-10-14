"""Static repository checks used by CI (``.github/workflows/ci.yml``); nothing here starts services.

``python -m transitpulse.verify.repo_checks [--only notebooks,config,fixtures,scripts,compose,migrations]``

* notebooks: ``nbformat`` schema validation, no outputs, no execution counts, a ``parameters``-tagged cell, and no
  token-like strings (GitHub/AWS/Slack tokens, private keys, inline ``token=`` assignments);
* config: every profile of ``config/`` validates (unknown keys fail) with an empty environment;
* fixtures: golden fixture files exist, parse, and the timeline only references existing files and labels;
* scripts: the documented entry scripts and the runtime manifest exist; manifest URLs are https;
* compose: images are pinned (no ``latest``), host ports bind 127.0.0.1 only;
* migrations: ``NNNN_name.cql`` naming.

Prints one JSON document; exit 0 when every check passes, 1 otherwise. A JSON-valid notebook is NOT evidence that
"Run All" passed.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

TOKEN_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("github_token", re.compile(r"\b(gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,})\b")),
    ("aws_access_key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("slack_token", re.compile(r"\bxox[abposr]-[A-Za-z0-9-]{10,}\b")),
    ("private_key", re.compile(r"-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----")),
    ("google_api_key", re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b")),
    ("inline_secret", re.compile(r"""(?i)\b(token|password|passwd|secret|api_key)\s*[:=]\s*["'][A-Za-z0-9_\-/+]{16,}["']""")),
)
REQUIRED_SCRIPTS = ("scripts/bootstrap_colab.sh", "scripts/prepare_runtime.py", "scripts/infra.py", "scripts/demo.py",
                    "scripts/doctor.py", "scripts/export_artifacts.py")
GOLDEN_STATIC = ("agency", "routes", "stops", "trips", "stop_times", "calendar", "calendar_dates", "shapes", "feed_info")
MAIN_NOTEBOOK = "notebooks/TransitPulse_MBTA_Colab_Demo.ipynb"


class Report:
    def __init__(self) -> None:
        self.checks: list[dict[str, Any]] = []

    def add(self, name: str, ok: bool, detail: Any = None) -> None:
        self.checks.append({"name": name, "status": "PASS" if ok else "FAIL", "detail": detail})

    @property
    def ok(self) -> bool:
        return all(c["status"] == "PASS" for c in self.checks)


def scan_tokens(text: str) -> list[str]:
    hits = []
    for name, pat in TOKEN_PATTERNS:
        for m in pat.finditer(text):
            hits.append(f"{name}: {m.group(0)[:12]}...")
    return hits


def check_notebooks(root: Path, rep: Report) -> None:
    import nbformat

    nbs = sorted((root / "notebooks").glob("*.ipynb"))
    rep.add("notebook_present", (root / MAIN_NOTEBOOK).is_file(), MAIN_NOTEBOOK)
    for path in nbs:
        rel = str(path.relative_to(root))
        text = path.read_text(encoding="utf-8")
        try:
            nb = nbformat.reads(text, as_version=4)
            nbformat.validate(nb)
            rep.add(f"notebook_schema[{rel}]", True, f"nbformat {nb.nbformat}.{nb.nbformat_minor}")
        except Exception as exc:  # noqa: BLE001 - reported as a failed check
            rep.add(f"notebook_schema[{rel}]", False, str(exc)[:500])
            continue
        outputs = [i for i, c in enumerate(nb.cells) if c.cell_type == "code" and c.get("outputs")]
        counts = [i for i, c in enumerate(nb.cells) if c.cell_type == "code" and c.get("execution_count") is not None]
        rep.add(f"notebook_no_outputs[{rel}]", not outputs, {"cells_with_outputs": outputs})
        rep.add(f"notebook_no_execution_counts[{rel}]", not counts, {"cells_with_counts": counts})
        bad_meta = [i for i, c in enumerate(nb.cells) if not isinstance(c.get("metadata", {}), dict)]
        rep.add(f"notebook_cell_metadata[{rel}]", not bad_meta, {"invalid": bad_meta})
        if rel == MAIN_NOTEBOOK:
            tagged = [i for i, c in enumerate(nb.cells) if "parameters" in (c.get("metadata", {}).get("tags") or [])]
            rep.add(f"notebook_parameters_cell[{rel}]", bool(tagged), {"cells": tagged})
        hits = scan_tokens(text)
        rep.add(f"notebook_no_secrets[{rel}]", not hits, hits[:10])


def check_config(root: Path, rep: Report) -> None:
    from transitpulse.config import PROFILES, load_config

    for profile in PROFILES:
        with tempfile.TemporaryDirectory() as tmp:
            try:
                cfg = load_config(profile, workdir=tmp, environ={}, config_dir=root / "config")
                rep.add(f"config_valid[{profile}]", True, {"config_hash": cfg.config_hash()[:16]})
            except Exception as exc:  # noqa: BLE001 - reported as a failed check
                rep.add(f"config_valid[{profile}]", False, str(exc)[:800])
    for f in sorted((root / "config").glob("*.yaml")) + [root / ".env.example"]:
        if f.is_file():
            hits = scan_tokens(f.read_text(encoding="utf-8"))
            rep.add(f"config_no_secrets[{f.name}]", not hits, hits[:5])


def check_fixtures(root: Path, rep: Report) -> None:
    g = root / "tests" / "fixtures" / "golden"
    missing = [f"static/{n}.txt" for n in GOLDEN_STATIC if not (g / "static" / f"{n}.txt").is_file()]
    missing += [n for n in ("PROVENANCE.md", "timeline.json", "expected.json") if not (g / n).is_file()]
    rep.add("golden_files_present", not missing, {"missing": missing})
    parsed: dict[str, Any] = {}
    for p in sorted(g.rglob("*.json")):
        rel = str(p.relative_to(g))
        try:
            parsed[rel] = json.loads(p.read_text(encoding="utf-8"))
        except ValueError as exc:
            rep.add(f"golden_json[{rel}]", False, str(exc))
    rep.add("golden_json_parses", all(isinstance(v, (dict, list)) for v in parsed.values()), sorted(parsed))
    bad_rt = []
    for rel, doc in parsed.items():
        if rel.startswith("rt/"):
            header = doc.get("header") if isinstance(doc, dict) else None
            if not isinstance(header, dict) or not isinstance(header.get("timestamp"), int) \
                    or header.get("gtfs_realtime_version") not in ("1.0", "2.0"):
                bad_rt.append(rel)
    rep.add("golden_rt_headers", not bad_rt, {"invalid": bad_rt})
    timeline = parsed.get("timeline.json")
    problems: list[str] = []
    if isinstance(timeline, dict):
        for run_key, run in (timeline.get("runs") or {}).items():
            labels: set[str] = set()
            for step in run.get("steps", []):
                for item in step.get("items", []):
                    kind = item.get("kind")
                    if kind == "capture":
                        if not (g / str(item.get("file"))).is_file():
                            problems.append(f"{run_key}/{step.get('step')}: missing file {item.get('file')}")
                        if item.get("label"):
                            labels.add(item["label"])
                    elif kind == "republish" and item.get("label") not in labels:
                        problems.append(f"{run_key}/{step.get('step')}: republish of unknown label {item.get('label')}")
    else:
        problems.append("timeline.json is not an object")
    rep.add("golden_timeline_references", not problems, problems[:20])
    expected = parsed.get("expected.json")
    steps = ((expected or {}).get("runs", {}).get("golden", {}) or {}).get("steps") if isinstance(expected, dict) else None
    rep.add("golden_expected_has_steps", bool(steps), {"golden_steps": len(steps or [])})
    for p in sorted((root / "tests" / "fixtures" / "scenarios").glob("*/expected.json")):
        try:
            json.loads(p.read_text(encoding="utf-8"))
            rep.add(f"scenario_expected[{p.parent.name}]", True)
        except ValueError as exc:
            rep.add(f"scenario_expected[{p.parent.name}]", False, str(exc))


def check_scripts(root: Path, rep: Report) -> None:
    missing = [s for s in REQUIRED_SCRIPTS if not (root / s).is_file()]
    rep.add("scripts_present", not missing, {"missing": missing})
    manifest = root / "deploy" / "runtime-manifest.json"
    try:
        data = json.loads(manifest.read_text(encoding="utf-8"))
        urls = [u for a in data.get("artifacts", []) for u in a.get("urls", [])]
        bad = [u for u in urls if not u.startswith("https://")]
        rep.add("runtime_manifest_urls_https", bool(urls) and not bad, {"non_https": bad, "count": len(urls)})
    except (OSError, ValueError) as exc:
        rep.add("runtime_manifest_parses", False, str(exc))


def check_compose(root: Path, rep: Report) -> None:
    import yaml

    path = root / "docker-compose.yml"
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        rep.add("compose_parses", False, str(exc))
        return
    services = doc.get("services", {}) if isinstance(doc, dict) else {}
    rep.add("compose_parses", bool(services), sorted(services))
    unpinned, public_ports = [], []
    for name, svc in services.items():
        image = svc.get("image")
        if isinstance(image, str) and "${" not in image and (":" not in image.rsplit("/", 1)[-1]
                                                              or image.endswith(":latest")):
            unpinned.append(f"{name}: {image}")
        for port in svc.get("ports", []) or []:
            spec = port if isinstance(port, str) else str((port or {}).get("host_ip", "")) + ":"
            if not str(spec).startswith("127.0.0.1:"):
                public_ports.append(f"{name}: {port}")
    rep.add("compose_images_pinned", not unpinned, unpinned)
    rep.add("compose_ports_loopback_only", not public_ports, public_ports)
    dockerfile = root / "docker" / "app.Dockerfile"
    if dockerfile.is_file():
        froms = re.findall(r"(?im)^FROM\s+(\S+)", dockerfile.read_text(encoding="utf-8"))
        bad = [f for f in froms if f.endswith(":latest") or ":" not in f.split("/")[-1]]
        rep.add("dockerfile_base_pinned", not bad, {"from": froms, "unpinned": bad})


def check_migrations(root: Path, rep: Report) -> None:
    d = root / "src" / "transitpulse" / "storage" / "migrations"
    files = sorted(p.name for p in d.glob("*.cql"))
    bad = [f for f in files if not re.fullmatch(r"\d{4}_[a-z0-9_]+\.cql", f)]
    rep.add("migrations_named", bool(files) and not bad, {"files": files, "bad": bad})


CHECKS = {"notebooks": check_notebooks, "config": check_config, "fixtures": check_fixtures, "scripts": check_scripts,
          "compose": check_compose, "migrations": check_migrations}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m transitpulse.verify.repo_checks")
    ap.add_argument("--only", default=",".join(CHECKS), help="comma-separated subset of " + ",".join(CHECKS))
    ap.add_argument("--repo-dir", default=None)
    args = ap.parse_args(argv)
    from transitpulse.config import repo_root

    root = Path(args.repo_dir).resolve() if args.repo_dir else repo_root()
    rep = Report()
    for name in [n.strip() for n in args.only.split(",") if n.strip()]:
        fn = CHECKS.get(name)
        if fn is None:
            rep.add(f"unknown_check[{name}]", False, f"expected one of {sorted(CHECKS)}")
            continue
        try:
            fn(root, rep)
        except Exception as exc:  # noqa: BLE001 - a crashing check is a failed check
            rep.add(f"{name}_crashed", False, f"{type(exc).__name__}: {exc}"[:800])
    failed = [c for c in rep.checks if c["status"] != "PASS"]
    sys.stdout.write(json.dumps({"ok": rep.ok, "action": "repo-checks", "checks": rep.checks,
                                 "failed": [c["name"] for c in failed], "error": None}, indent=2, default=str) + "\n")
    return 0 if rep.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
