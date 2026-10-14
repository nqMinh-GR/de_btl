"""``demo.py reset``: explicit, confirmed deletion of selected work-directory areas.

Dry run by default: lists real paths (all strictly inside W) with sizes and a ``confirm_token`` = hash of
the exact list. ``--confirm <token>`` deletes only if the recomputed token matches. Refuses while any
owned process is running. Never follows a symlink out of W and never touches anything outside W.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from transitpulse.runtime.paths import WorkPaths

TARGETS: tuple[str, ...] = ("session", "checkpoints", "kafka", "cassandra", "data", "all")


def target_paths(paths: WorkPaths, target: str) -> list[Path]:
    root = paths.root
    if target == "session":
        return [root / "sessions", paths.processes / "exits", paths.registry_json, paths.processes / "last_start.json"]
    if target == "checkpoints":
        return [root / "checkpoints"]
    if target == "kafka":
        return [root / "infra" / "kafka"]
    if target == "cassandra":
        return [root / "infra" / "cassandra"]
    if target == "data":
        return [root / "data", root / "state", root / "runs", root / "faults", paths.metrics_dir,
                paths.runtime_dir / "infra_init.json"]
    if target == "all":
        out: list[Path] = []
        for t in ("session", "checkpoints", "kafka", "cassandra", "data"):
            out.extend(target_paths(paths, t))
        out.extend([paths.spark_local, paths.tmp])
        return out
    raise ValueError(f"unknown reset target {target!r}; choose from {', '.join(TARGETS)}")


def cache_paths(paths: WorkPaths) -> list[Path]:
    return [paths.cache]


def _inside(root_real: str, candidate: Path) -> str | None:
    """Real path of ``candidate`` if it is strictly inside ``root_real`` (symlinks are not followed)."""
    parent_real = os.path.realpath(candidate.parent)
    full = os.path.join(parent_real, candidate.name)
    if os.path.commonpath([root_real, full]) != root_real or full == root_real:
        return None
    return full


def _size(path: str) -> tuple[int, int]:
    if os.path.islink(path) or os.path.isfile(path):
        try:
            return os.lstat(path).st_size, 1
        except OSError:
            return 0, 0
    total = 0
    files = 0
    for dirpath, _dirs, names in os.walk(path, followlinks=False):
        for n in names:
            try:
                total += os.lstat(os.path.join(dirpath, n)).st_size
                files += 1
            except OSError:
                continue
    return total, files


def plan_reset(paths: WorkPaths, targets: Sequence[str], *, include_caches: bool = False) -> dict[str, Any]:
    root_real = os.path.realpath(paths.root)
    selected: list[Path] = []
    for t in targets:
        selected.extend(target_paths(paths, t))
    if include_caches:
        selected.extend(cache_paths(paths))
    entries: list[dict[str, Any]] = []
    rejected: list[str] = []
    seen: set[str] = set()
    for p in selected:
        real = _inside(root_real, p)
        if real is None:
            rejected.append(str(p))
            continue
        if real in seen:
            continue
        seen.add(real)
        exists = os.path.lexists(real)
        size, files = _size(real) if exists else (0, 0)
        entries.append({"path": real, "exists": exists, "is_symlink": os.path.islink(real), "bytes": size,
                        "files": files})
    entries.sort(key=lambda e: e["path"])
    token = confirm_token([e["path"] for e in entries if e["exists"]], targets, include_caches)
    return {"workdir": root_real, "targets": list(targets), "include_caches": include_caches, "paths": entries,
            "rejected": rejected, "confirm_token": token,
            "total_bytes": sum(e["bytes"] for e in entries)}


def confirm_token(existing_paths: Iterable[str], targets: Sequence[str], include_caches: bool) -> str:
    payload = json.dumps({"paths": sorted(existing_paths), "targets": sorted(targets), "caches": include_caches},
                         sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def execute_reset(plan: dict[str, Any]) -> list[str]:
    """Delete the planned paths (each re-validated inside the work directory). Returns deleted paths."""
    root_real = plan["workdir"]
    deleted: list[str] = []
    for e in plan["paths"]:
        p = e["path"]
        if not e["exists"] or not os.path.lexists(p):
            continue
        if os.path.commonpath([root_real, p]) != root_real or p == root_real:
            raise ValueError(f"refusing to delete {p}: outside {root_real}")
        if os.path.islink(p) or os.path.isfile(p):
            os.unlink(p)
        else:
            shutil.rmtree(p)
        deleted.append(p)
    return deleted
