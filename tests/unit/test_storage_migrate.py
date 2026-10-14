"""Migration splitting, checksum, validation, schema parsing and apply logic (fake session, no Cassandra)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from transitpulse.processing.context import PER_PARTITION_COUNT_KEYS, TABLE_TTL_CLASS
from transitpulse.storage import migrate
from transitpulse.storage.manifest import MANIFEST_COLUMNS
from transitpulse.storage.migrate import (
    MigrationChecksumMismatch,
    MigrationError,
    apply_migrations,
    checksum,
    load_migrations,
    parse_table_schemas,
    split_statements,
    table_schemas,
    validate_statement,
)
from transitpulse.storage.pointer import POINTER_COLUMNS

CFG = SimpleNamespace(cassandra=SimpleNamespace(keyspace="transitpulse"))


# ------------------------------------------------------------------------------------------ fakes
class FakePrepared:
    def __init__(self, cql: str) -> None:
        self.query_string = cql
        self.is_idempotent = False


class FakeResult:
    def __init__(self, rows=None, applied=None) -> None:
        self._rows = list(rows or [])
        self._applied = applied

    @property
    def was_applied(self) -> bool:
        if self._applied is None:
            raise RuntimeError("not an LWT result")
        return self._applied

    def one(self):
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)


class MigrationSession:
    """Records DDL and keeps schema_migrations rows in memory."""

    def __init__(self, recorded: dict[str, str] | None = None) -> None:
        self.recorded = dict(recorded or {})
        self.ddl: list[str] = []
        self.calls: list[tuple[str, tuple, dict]] = []

    def prepare(self, cql: str) -> FakePrepared:
        return FakePrepared(cql)

    def execute(self, stmt, params=None, **kwargs):
        cql = stmt.query_string if isinstance(stmt, FakePrepared) else stmt
        params = tuple(params or ())
        self.calls.append((cql, params, kwargs))
        if cql.startswith("SELECT migration_id"):
            mid = params[0]
            if mid in self.recorded:
                return FakeResult([{"migration_id": mid, "checksum_sha256": self.recorded[mid]}])
            return FakeResult([])
        if cql.startswith("INSERT INTO transitpulse.schema_migrations"):
            mid, chk = params[0], params[1]
            if mid in self.recorded:
                return FakeResult([{"[applied]": False, "migration_id": mid, "checksum_sha256": self.recorded[mid]}],
                                  applied=False)
            self.recorded[mid] = chk
            return FakeResult([{"[applied]": True}], applied=True)
        self.ddl.append(cql)
        return FakeResult([])


MINI_0001 = """-- mini schema for tests
CREATE KEYSPACE IF NOT EXISTS transitpulse WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS transitpulse.schema_migrations (
  migration_id text PRIMARY KEY,
  checksum_sha256 text
);

-- a table
CREATE TABLE IF NOT EXISTS transitpulse.t1 (
  a text,
  b int,
  c map<text, int>,
  PRIMARY KEY ((a), b)
) WITH CLUSTERING ORDER BY (b DESC);
"""


def _write(d: Path, name: str, text: str) -> Path:
    p = d / name
    p.write_text(text, encoding="utf-8")
    return p


# ------------------------------------------------------------------------------------------ splitting
def test_split_drops_comment_lines_and_splits_on_line_ending_semicolon():
    stmts = split_statements(MINI_0001)
    assert len(stmts) == 3
    assert stmts[0].startswith("CREATE KEYSPACE IF NOT EXISTS transitpulse")
    assert not any(s.endswith(";") for s in stmts)
    assert all("--" not in s for s in stmts)
    assert "WITH CLUSTERING ORDER BY (b DESC)" in stmts[2]


def test_split_rejects_unterminated_statement():
    with pytest.raises(MigrationError):
        split_statements("CREATE TABLE IF NOT EXISTS ks.x (a text PRIMARY KEY)\n")


def test_split_handles_crlf():
    assert split_statements(MINI_0001.replace("\n", "\r\n")) == split_statements(MINI_0001)


@pytest.mark.parametrize("stmt", [
    "DROP TABLE transitpulse.t1",
    "TRUNCATE transitpulse.t1",
    "ALTER TABLE transitpulse.t1 DROP b",
    "CREATE TABLE transitpulse.t2 (a text PRIMARY KEY)",
    "DELETE FROM transitpulse.t1 WHERE a = 'x'",
])
def test_destructive_or_non_idempotent_statements_are_rejected(stmt):
    with pytest.raises(MigrationError):
        validate_statement(stmt)


def test_additive_statements_are_allowed():
    validate_statement("CREATE TABLE IF NOT EXISTS transitpulse.t2 (a text PRIMARY KEY)")
    validate_statement("ALTER TABLE transitpulse.t1 ADD d text")


def test_checksum_is_newline_normalized_and_content_sensitive():
    assert checksum(MINI_0001) == checksum(MINI_0001.replace("\n", "\r\n"))
    assert checksum(MINI_0001) != checksum(MINI_0001 + "\n-- edited\n")


# ------------------------------------------------------------------------------------------ real schema
def test_initial_migration_parses_and_matches_code_contracts():
    migs = load_migrations()
    assert migs and migs[0].migration_id == "0001_initial"
    schemas = table_schemas()
    for t in TABLE_TTL_CLASS:
        assert t in schemas, t
    assert set(MANIFEST_COLUMNS) == set(schemas["snapshot_manifest"].columns)
    assert tuple(schemas["feed_current"].columns) == POINTER_COLUMNS
    v = schemas["vehicles_by_route_snapshot"]
    assert v.partition_key == ("run_id", "snapshot_id", "route_id")
    assert v.clustering_key == ("vehicle_id",)
    assert v.columns["quality_flags"] == "set<text>"
    assert v.columns["observation_time"] == "timestamp"
    h = schemas["vehicle_observations_by_day"]
    assert h.clustering_key == ("event_time", "observation_id")
    assert h.columns["utc_date"] == "date"
    assert schemas["run_metadata"].partition_key == ("run_id",)
    assert schemas["pipeline_status"].primary_key == ("run_id", "component")
    assert schemas["route_metrics_by_snapshot"].columns["excluded_trip_counts"] == "map<text,int>"
    for table, key in PER_PARTITION_COUNT_KEYS.items():
        assert key in schemas[table].partition_key


def test_parse_table_schemas_alter_add():
    stmts = split_statements(MINI_0001) + ["ALTER TABLE transitpulse.t1 ADD d set<int>"]
    s = parse_table_schemas(stmts)["t1"]
    assert s.partition_key == ("a",) and s.clustering_key == ("b",)
    assert s.columns == {"a": "text", "b": "int", "c": "map<text,int>", "d": "set<int>"}


# ------------------------------------------------------------------------------------------ apply
def test_apply_migrations_applies_then_skips(tmp_path):
    _write(tmp_path, "0001_initial.cql", MINI_0001)
    sess = MigrationSession()
    res = apply_migrations(CFG, migrations_dir=tmp_path, session=sess)
    assert res == [{"migration_id": "0001_initial", "status": "APPLIED", "checksum_sha256": checksum(MINI_0001)}]
    # bootstrap (keyspace + schema_migrations) then every statement of the migration
    assert sess.ddl[0].startswith("CREATE KEYSPACE") and "schema_migrations" in sess.ddl[1]
    assert any("transitpulse.t1" in s for s in sess.ddl)
    assert not any(s.upper().startswith(("DROP", "TRUNCATE")) for s in sess.ddl)
    # the record insert is an LWT
    inserts = [c for c in sess.calls if c[0].startswith("INSERT INTO transitpulse.schema_migrations")]
    assert inserts and inserts[0][0].rstrip().endswith("IF NOT EXISTS")

    sess2 = MigrationSession(sess.recorded)
    res2 = apply_migrations(CFG, migrations_dir=tmp_path, session=sess2)
    assert res2[0]["status"] == "SKIPPED"
    assert len(sess2.ddl) == 2           # only the idempotent bootstrap statements


def test_apply_migrations_detects_checksum_drift(tmp_path):
    _write(tmp_path, "0001_initial.cql", MINI_0001)
    sess = MigrationSession({"0001_initial": "0" * 64})
    with pytest.raises(MigrationChecksumMismatch):
        apply_migrations(CFG, migrations_dir=tmp_path, session=sess)
    assert len(sess.ddl) == 2            # nothing beyond the bootstrap was executed


def test_apply_migrations_applies_only_new_files(tmp_path):
    _write(tmp_path, "0001_initial.cql", MINI_0001)
    second = "-- add column\nALTER TABLE transitpulse.t1 ADD d text;\n"
    _write(tmp_path, "0002_add_d.cql", second)
    sess = MigrationSession({"0001_initial": checksum(MINI_0001)})
    res = apply_migrations(CFG, migrations_dir=tmp_path, session=sess)
    assert [r["status"] for r in res] == ["SKIPPED", "APPLIED"]
    assert sess.ddl[-1].startswith("ALTER TABLE transitpulse.t1 ADD d text")


def test_apply_migrations_concurrent_record_with_same_checksum_is_skipped(tmp_path):
    _write(tmp_path, "0001_initial.cql", MINI_0001)

    class RacingSession(MigrationSession):
        def execute(self, stmt, params=None, **kwargs):
            cql = stmt.query_string if isinstance(stmt, FakePrepared) else stmt
            if cql.startswith("INSERT INTO transitpulse.schema_migrations"):
                self.recorded[params[0]] = params[1]      # another migrator won the IF NOT EXISTS race
            return super().execute(stmt, params, **kwargs)

    res = apply_migrations(CFG, migrations_dir=tmp_path, session=RacingSession())
    assert res[0]["status"] == "SKIPPED"


def test_apply_migrations_requires_matching_keyspace(tmp_path):
    _write(tmp_path, "0001_initial.cql", MINI_0001)
    cfg = SimpleNamespace(cassandra=SimpleNamespace(keyspace="other"))
    with pytest.raises(MigrationError):
        apply_migrations(cfg, migrations_dir=tmp_path, session=MigrationSession())


def test_bad_migration_file_name_is_rejected(tmp_path):
    _write(tmp_path, "initial.cql", MINI_0001)
    with pytest.raises(MigrationError):
        load_migrations(tmp_path)


def test_default_dir_is_package_migrations():
    assert migrate.default_migrations_dir().name == "migrations"
    assert (migrate.default_migrations_dir() / "0001_initial.cql").is_file()
