"""``feed_current`` pointer (WP4; interfaces §6, design B §1/§4, PDF p30).

The pointer row is LWT-only: it is created with ``INSERT ... IF NOT EXISTS`` and only ever changed with
``UPDATE ... IF snapshot_id = <the value read>`` (compare-and-set). It has no TTL. Reads that feed a CAS
decision use ``serial=True``.
"""

from __future__ import annotations

from typing import Any

from transitpulse.storage.cassandra import (
    execute,
    lwt_existing_row,
    normalize_row,
    unset_if_none,
    was_applied,
)

POINTER_COLUMNS: tuple[str, ...] = (
    "run_id", "feed_type", "snapshot_id", "generation_id", "source_order", "feed_timestamp",
    "metrics_as_of", "view_published_at", "expires_at", "is_partial", "is_empty",
)
_VALUE_COLUMNS: tuple[str, ...] = POINTER_COLUMNS[2:]

SELECT_POINTER = "SELECT * FROM feed_current WHERE run_id = ? AND feed_type = ?"
INSERT_POINTER = (
    f"INSERT INTO feed_current ({', '.join(POINTER_COLUMNS)}) "
    f"VALUES ({', '.join('?' for _ in POINTER_COLUMNS)}) IF NOT EXISTS"
)
CAS_POINTER = (
    f"UPDATE feed_current SET {', '.join(f'{c} = ?' for c in _VALUE_COLUMNS)} "
    "WHERE run_id = ? AND feed_type = ? IF snapshot_id = ?"
)


def _require(row: dict[str, Any]) -> None:
    missing = [c for c in POINTER_COLUMNS if c not in row]
    if missing:
        raise ValueError(f"pointer row is missing columns {missing}")
    for c in ("run_id", "feed_type", "snapshot_id", "generation_id", "source_order"):
        if not row.get(c):
            raise ValueError(f"pointer column {c} must be set")


def read_pointer(session, run_id: str, feed_type: str, *, serial: bool = False) -> dict | None:
    """Current ``feed_current`` row (aware UTC datetimes) or ``None``."""
    return normalize_row(execute(session, SELECT_POINTER, (run_id, feed_type), serial=serial).one())


def bootstrap_pointer(session, row: dict) -> tuple[bool, dict | None]:
    """First publication of a feed: ``INSERT ... IF NOT EXISTS``.

    Returns ``(True, None)`` when applied, otherwise ``(False, <existing row>)``.
    """
    _require(row)
    rs = execute(session, INSERT_POINTER, unset_if_none([row[c] for c in POINTER_COLUMNS]))
    if was_applied(rs):
        return True, None
    return False, lwt_existing_row(rs)


def cas_pointer(session, row: dict, expected_snapshot_id: str) -> bool:
    """``UPDATE feed_current ... IF snapshot_id = expected``. Every pointer column is replaced."""
    _require(row)
    if not expected_snapshot_id:
        raise ValueError("expected_snapshot_id is required for a compare-and-set")
    params = [row[c] for c in _VALUE_COLUMNS] + [row["run_id"], row["feed_type"], expected_snapshot_id]
    return was_applied(execute(session, CAS_POINTER, params))
