"""Idempotent CQL migrations (WP4; interfaces §6, design B §1).

Files: ``storage/migrations/NNNN_<name>.cql`` applied in lexical order. Each file is split with the rule
documented at the top of ``0001_initial.cql``: drop every line whose first non-blank characters are
``--``, then split on ``;`` that ends a line (no ``;`` may appear inside string literals).

Procedure (never TRUNCATE, never DROP, nothing is deleted):
  1. Bootstrap: execute the ``CREATE KEYSPACE`` and ``CREATE TABLE ... schema_migrations`` statements of
     the FIRST migration (both ``IF NOT EXISTS``) and wait for schema agreement.
  2. For every migration: read ``schema_migrations`` (SERIAL). Recorded with the same checksum ->
     ``SKIPPED``; recorded with a different checksum -> :class:`MigrationChecksumMismatch` ("schema
     drift"; nothing is executed). Not recorded -> execute its statements one at a time, wait for schema
     agreement, then ``INSERT ... IF NOT EXISTS`` the record -> ``APPLIED``.

The checksum is sha256 of the UTF-8 file text with CRLF/CR normalized to LF (``.gitattributes`` already
forces LF for ``*.cql``; the normalization keeps the checksum stable on any checkout).

The same parser also extracts table column types (:func:`table_schemas`), which the Spark sink uses to
convert values and validate builder outputs: the CQL file is THE column contract.
"""

from __future__ import annotations

import getpass
import hashlib
import re
import socket
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from transitpulse.config import ProjectConfig

MIGRATION_FILE_RE = re.compile(r"^(\d{4})_[A-Za-z0-9_]+\.cql$")
_FORBIDDEN_START = re.compile(r"^\s*(DROP|TRUNCATE|DELETE)\b", re.IGNORECASE)
_ALTER_DROP = re.compile(r"^\s*ALTER\s+TABLE\s+\S+\s+(DROP|RENAME)\b", re.IGNORECASE)
_ALLOWED_START = re.compile(
    r"^\s*(CREATE\s+(KEYSPACE|TABLE|TYPE|INDEX|CUSTOM\s+INDEX|MATERIALIZED\s+VIEW)\s+IF\s+NOT\s+EXISTS\b"
    r"|ALTER\s+TABLE\s+\S+\s+(ADD|WITH)\b)",
    re.IGNORECASE,
)
_KEYSPACE_RE = re.compile(r"^\s*CREATE\s+KEYSPACE\s+IF\s+NOT\s+EXISTS\s+(\w+)\b", re.IGNORECASE)
_SCHEMA_MIGRATIONS_RE = re.compile(
    r"^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(?:(\w+)\.)?schema_migrations\b", re.IGNORECASE
)
_CREATE_TABLE_RE = re.compile(r"^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(?:(\w+)\.)?(\w+)\s*\(", re.IGNORECASE)
_ALTER_ADD_RE = re.compile(r"^\s*ALTER\s+TABLE\s+(?:(\w+)\.)?(\w+)\s+ADD\s+(.+)$", re.IGNORECASE | re.DOTALL)

DDL_TIMEOUT_SECONDS = 120.0
SCHEMA_AGREEMENT_WAIT_SECONDS = 60.0


class MigrationError(RuntimeError):
    """Invalid migration file or failed migration step."""


class MigrationChecksumMismatch(MigrationError):
    """A recorded migration differs from the file on disk (schema drift). Never auto-repaired."""

    def __init__(self, migration_id: str, recorded: str, current: str) -> None:
        super().__init__(
            f"schema drift: migration {migration_id} was applied with checksum {recorded} but the file now has "
            f"{current}; add a new migration instead of editing an applied one"
        )
        self.migration_id = migration_id
        self.recorded = recorded
        self.current = current


@dataclass(frozen=True)
class Migration:
    migration_id: str
    path: Path
    checksum_sha256: str
    description: str
    statements: tuple[str, ...]


@dataclass(frozen=True)
class TableSchema:
    name: str
    columns: dict[str, str] = field(default_factory=dict)   # column -> normalized CQL type, file order
    partition_key: tuple[str, ...] = ()
    clustering_key: tuple[str, ...] = ()

    @property
    def primary_key(self) -> tuple[str, ...]:
        return self.partition_key + self.clustering_key


# --------------------------------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------------------------------

def normalize_text(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def checksum(text: str) -> str:
    return hashlib.sha256(normalize_text(text).encode("utf-8")).hexdigest()


def split_statements(text: str) -> list[str]:
    """Apply the documented splitting rule; returns statements without the trailing ``;``."""
    statements: list[str] = []
    current: list[str] = []
    for line in normalize_text(text).split("\n"):
        if line.lstrip().startswith("--"):
            continue
        current.append(line)
        if line.rstrip().endswith(";"):
            stmt = "\n".join(current).strip()
            stmt = stmt[:-1].rstrip()          # drop the terminating ';'
            current = []
            if stmt:
                statements.append(stmt)
    rest = "\n".join(current).strip()
    if rest:
        raise MigrationError(f"unterminated CQL statement (missing ';' at end of line): {rest[:200]!r}")
    return statements


def validate_statement(stmt: str) -> None:
    """Only additive, idempotent DDL is allowed in migrations."""
    if _FORBIDDEN_START.match(stmt) or _ALTER_DROP.match(stmt):
        raise MigrationError(f"destructive statement is not allowed in migrations: {stmt[:120]!r}")
    if not _ALLOWED_START.match(stmt):
        raise MigrationError(
            f"migration statements must be CREATE ... IF NOT EXISTS or ALTER TABLE ... ADD: {stmt[:120]!r}"
        )


def _description(text: str) -> str:
    for line in normalize_text(text).split("\n"):
        s = line.strip()
        if s.startswith("--"):
            d = s.lstrip("-").strip()
            if d:
                return d[:200]
    return ""


def default_migrations_dir() -> Path:
    return Path(__file__).resolve().parent / "migrations"


def load_migrations(migrations_dir: Path | None = None) -> list[Migration]:
    mdir = Path(migrations_dir) if migrations_dir else default_migrations_dir()
    if not mdir.is_dir():
        raise MigrationError(f"migrations directory not found: {mdir}")
    out: list[Migration] = []
    for path in sorted(mdir.iterdir()):
        if not path.is_file() or not path.name.endswith(".cql"):
            continue
        if not MIGRATION_FILE_RE.match(path.name):
            raise MigrationError(f"migration file name must match NNNN_<name>.cql: {path.name}")
        text = path.read_text(encoding="utf-8")
        stmts = split_statements(text)
        for s in stmts:
            validate_statement(s)
        out.append(Migration(path.stem, path, checksum(text), _description(text), tuple(stmts)))
    ids = [m.migration_id[:4] for m in out]
    if len(ids) != len(set(ids)):
        raise MigrationError(f"duplicate migration numbers in {mdir}: {ids}")
    return out


def _split_top_level(body: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    cur: list[str] = []
    for ch in body:
        if ch in "(<":
            depth += 1
        elif ch in ")>":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    tail = "".join(cur).strip()
    if tail:
        parts.append(tail)
    return [p for p in parts if p]


def _matching_paren_body(stmt: str, open_idx: int) -> str:
    depth = 0
    for i in range(open_idx, len(stmt)):
        if stmt[i] == "(":
            depth += 1
        elif stmt[i] == ")":
            depth -= 1
            if depth == 0:
                return stmt[open_idx + 1:i]
    raise MigrationError(f"unbalanced parentheses in {stmt[:120]!r}")


def _norm_type(t: str) -> str:
    return re.sub(r"\s+", "", t).lower()


def _strip_inline_comments(body: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in body.split("\n"))


def parse_table_schemas(statements: list[str] | tuple[str, ...]) -> dict[str, TableSchema]:
    """Column types and primary keys of every ``CREATE TABLE`` (+ ``ALTER TABLE ... ADD``)."""
    tables: dict[str, TableSchema] = {}
    for stmt in statements:
        m = _CREATE_TABLE_RE.match(stmt)
        if m:
            name = m.group(2)
            body = _strip_inline_comments(_matching_paren_body(stmt, m.end() - 1))
            cols: dict[str, str] = {}
            pk_part: tuple[str, ...] = ()
            ck_part: tuple[str, ...] = ()
            for item in _split_top_level(body):
                item_n = " ".join(item.split())
                if item_n.upper().startswith("PRIMARY KEY"):
                    inner = _matching_paren_body(item_n, item_n.index("("))
                    elems = _split_top_level(inner)
                    first = elems[0]
                    if first.startswith("("):
                        pk_part = tuple(x.strip() for x in first.strip("()").split(",") if x.strip())
                    else:
                        pk_part = (first.strip(),)
                    ck_part = tuple(e.strip() for e in elems[1:])
                    continue
                tokens = item_n.split(" ", 1)
                if len(tokens) != 2:
                    raise MigrationError(f"cannot parse column definition {item_n!r} in table {name}")
                col, ctype = tokens[0], tokens[1]
                inline_pk = False
                if ctype.upper().endswith(" PRIMARY KEY"):
                    ctype = ctype[: -len(" PRIMARY KEY")]
                    inline_pk = True
                if ctype.upper().endswith(" STATIC"):
                    ctype = ctype[: -len(" STATIC")]
                cols[col] = _norm_type(ctype)
                if inline_pk:
                    pk_part = (col,)
            if not pk_part:
                raise MigrationError(f"table {name} has no primary key")
            tables[name] = TableSchema(name, cols, pk_part, ck_part)
            continue
        a = _ALTER_ADD_RE.match(stmt)
        if a:
            name = a.group(2)
            if name not in tables:
                raise MigrationError(f"ALTER TABLE {name} before its CREATE TABLE")
            spec = a.group(3).strip()
            if spec.startswith("("):
                spec = spec.strip("()")
            cols = dict(tables[name].columns)
            for item in _split_top_level(spec):
                tokens = " ".join(item.split()).split(" ", 1)
                cols[tokens[0]] = _norm_type(tokens[1])
            old = tables[name]
            tables[name] = TableSchema(name, cols, old.partition_key, old.clustering_key)
    return tables


@lru_cache(maxsize=4)
def _cached_schemas(mdir: str) -> dict[str, TableSchema]:
    stmts: list[str] = []
    for mig in load_migrations(Path(mdir)):
        stmts.extend(mig.statements)
    return parse_table_schemas(stmts)


def table_schemas(migrations_dir: Path | None = None) -> dict[str, TableSchema]:
    """Effective table schemas after all migrations (cached; a fresh dict is returned each call)."""
    mdir = str(Path(migrations_dir).resolve() if migrations_dir else default_migrations_dir())
    return dict(_cached_schemas(mdir))


# --------------------------------------------------------------------------------------------------
# Applying
# --------------------------------------------------------------------------------------------------

def _keyspace_of(stmt: str) -> str | None:
    m = _KEYSPACE_RE.match(stmt)
    return m.group(1) if m else None


def _wait_schema_agreement(session) -> None:
    cluster = getattr(session, "cluster", None)
    control = getattr(cluster, "control_connection", None) if cluster is not None else None
    if control is None:
        return
    if not control.wait_for_schema_agreement(wait_time=SCHEMA_AGREEMENT_WAIT_SECONDS):
        raise MigrationError(f"schema agreement not reached within {SCHEMA_AGREEMENT_WAIT_SECONDS:.0f}s")


def _execute_ddl(session, stmt: str) -> None:
    try:
        session.execute(stmt, timeout=DDL_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 - re-raised unless it is the documented idempotent case
        msg = str(exc).lower()
        is_add = bool(_ALTER_ADD_RE.match(stmt))
        if is_add and ("conflicts with an existing column" in msg or "already exist" in msg):
            return  # ALTER TABLE ... ADD re-executed after a crash before the migration record
        raise


def _applied_by() -> str:
    try:
        user = getpass.getuser()
    except Exception:  # noqa: BLE001 - containers without a passwd entry
        user = "unknown"
    return f"{user}@{socket.gethostname()}"


def apply_migrations(
    cfg: ProjectConfig,
    *,
    migrations_dir: Path | None = None,
    session=None,
    applied_by: str | None = None,
) -> list[dict[str, Any]]:
    """Apply pending migrations; returns ``[{migration_id, status: APPLIED|SKIPPED, checksum_sha256}]``.

    ``session`` may be injected (tests); by default a keyspace-less session from
    :func:`transitpulse.storage.cassandra.get_session` is used.
    """
    from transitpulse.storage.cassandra import execute, get_session, lwt_existing_row, was_applied

    migrations = load_migrations(migrations_dir)
    if not migrations:
        return []
    ks = cfg.cassandra.keyspace
    if session is None:
        session = get_session(cfg, keyspace=False)

    first = migrations[0]
    ks_stmts = [s for s in first.statements if _KEYSPACE_RE.match(s)]
    sm_stmts = [s for s in first.statements if _SCHEMA_MIGRATIONS_RE.match(s)]
    if len(ks_stmts) != 1 or len(sm_stmts) != 1:
        raise MigrationError(
            f"{first.path.name} must contain exactly one CREATE KEYSPACE and one CREATE TABLE schema_migrations"
        )
    declared = _keyspace_of(ks_stmts[0])
    if declared != ks:
        raise MigrationError(
            f"migrations create keyspace {declared!r} but cassandra.keyspace is {ks!r}; the keyspace name is "
            "fixed by the migration files"
        )
    for stmt in (ks_stmts[0], sm_stmts[0]):
        _execute_ddl(session, stmt)
    _wait_schema_agreement(session)

    select_cql = f"SELECT migration_id, checksum_sha256 FROM {ks}.schema_migrations WHERE migration_id = ?"
    insert_cql = (
        f"INSERT INTO {ks}.schema_migrations (migration_id, checksum_sha256, description, applied_at, applied_by) "
        "VALUES (?, ?, ?, ?, ?) IF NOT EXISTS"
    )
    who = applied_by or _applied_by()
    results: list[dict[str, Any]] = []
    for mig in migrations:
        existing = execute(session, select_cql, (mig.migration_id,), serial=True).one()
        if existing is not None:
            recorded = existing.get("checksum_sha256")
            if recorded != mig.checksum_sha256:
                raise MigrationChecksumMismatch(mig.migration_id, str(recorded), mig.checksum_sha256)
            results.append({"migration_id": mig.migration_id, "status": "SKIPPED",
                            "checksum_sha256": mig.checksum_sha256})
            continue
        for stmt in mig.statements:
            _execute_ddl(session, stmt)
        _wait_schema_agreement(session)
        rs = execute(
            session, insert_cql,
            (mig.migration_id, mig.checksum_sha256, mig.description, datetime.now(UTC), who),
        )
        if not was_applied(rs):
            row = lwt_existing_row(rs) or {}
            recorded = row.get("checksum_sha256")
            if recorded != mig.checksum_sha256:
                raise MigrationChecksumMismatch(mig.migration_id, str(recorded), mig.checksum_sha256)
            results.append({"migration_id": mig.migration_id, "status": "SKIPPED",
                            "checksum_sha256": mig.checksum_sha256})
            continue
        results.append({"migration_id": mig.migration_id, "status": "APPLIED",
                        "checksum_sha256": mig.checksum_sha256})
    return results
