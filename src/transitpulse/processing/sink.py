"""Executor-side Cassandra writer for generation tables (design B §4 "Writer", interfaces §7).

Shipped to Spark Python workers through ``rdd.mapPartitions(make_partition_writer(conf, plan, fault))``.
The closure captures ONLY small picklable values (plain dicts built on the driver by
:func:`connection_conf` / :func:`table_plan`), never the ProjectConfig, a Session or a SparkSession.

Per Python worker (``spark.python.worker.reuse=true``) one lazily created ``Cluster``/``Session`` is cached
in a module global and shut down at interpreter exit. Statements are prepared once per (table, column
tuple): ``INSERT INTO <table> (<cols>) VALUES (?, ...) USING TTL ?``.

Rows arrive as ``(table, values_tuple)`` with values in ``plan[table]["columns"]`` order. Conversions:
``None`` -> ``UNSET_VALUE`` (no tombstones), epoch-ms ``int`` for ``timestamp`` columns -> aware UTC
``datetime`` (exact integer arithmetic), ``list`` for ``set<...>`` columns -> ``set``, empty collections ->
``UNSET_VALUE``. Buffers of ``write_buffer_rows`` rows are flushed with
``execute_concurrent_with_args(session, stmt, params, concurrency=write_concurrency,
raise_on_first_error=True)`` and a small bounded retry (writes are idempotent upserts). The TTL is
recomputed per flush as ``ceil(expires_at) - floor(now)`` (Cassandra counts whole server seconds, so rows
never expire before ``expires_at``); a TTL below 1 raises (``USING TTL 0`` would mean "never expire").
The writer yields ``(table, rows_written, seconds)`` per table.
"""

from __future__ import annotations

import atexit
import math
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_STATE: dict[str, Any] = {"key": None, "cluster": None, "session": None}
_PREPARED: dict[tuple[str, tuple[str, ...]], Any] = {}
_LOCK = threading.Lock()
_ATEXIT = {"registered": False}

KIND_SCALAR, KIND_TS, KIND_DATE, KIND_SET, KIND_LIST, KIND_MAP = "scalar", "ts", "date", "set", "list", "map"


class GenerationExpiredError(RuntimeError):
    """The remaining TTL of a generation is below one second; writing would create immortal rows."""


class SinkWriteError(RuntimeError):
    """A buffered write failed after the bounded retries."""


# --------------------------------------------------------------------------------------------------
# Driver-side plan builders (pure)
# --------------------------------------------------------------------------------------------------

def connection_conf(cfg) -> dict[str, Any]:
    """Picklable connection settings for executors (built on the driver)."""
    c = cfg.cassandra
    return {
        "hosts": list(c.hosts),
        "port": int(c.port),
        "keyspace": c.keyspace,
        "local_dc": c.local_dc,
        "consistency": c.consistency,
        "serial_consistency": c.serial_consistency,
        "connect_timeout": float(c.connect_timeout_seconds),
        "request_timeout": float(c.request_timeout_seconds),
        "concurrency": int(c.write_concurrency),
        "buffer_rows": int(c.write_buffer_rows),
        "max_attempts": 3,
        "retry_backoff_seconds": 0.5,
    }


def kind_of(cql_type: str) -> str:
    t = cql_type.replace(" ", "").lower()
    if t == "timestamp":
        return KIND_TS
    if t == "date":
        return KIND_DATE
    if t.startswith("set<") or t.startswith("frozen<set<"):
        return KIND_SET
    if t.startswith("list<") or t.startswith("frozen<list<"):
        return KIND_LIST
    if t.startswith("map<") or t.startswith("frozen<map<"):
        return KIND_MAP
    return KIND_SCALAR


def table_plan(table: str, columns: Iterable[str], column_types: Mapping[str, str], expires_at_epoch_s: float) -> dict:
    cols = list(columns)
    unknown = [c for c in cols if c not in column_types]
    if unknown:
        raise ValueError(f"{table}: columns not in the CQL schema: {unknown}")
    return {"columns": cols, "kinds": [kind_of(column_types[c]) for c in cols], "expires_at": float(expires_at_epoch_s)}


def insert_cql(table: str, columns: Iterable[str]) -> str:
    cols = list(columns)
    return f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)}) USING TTL ?"


def remaining_seconds(expires_at_epoch_s: float, now_epoch_s: float | None = None) -> int:
    """Whole seconds a row written now with TTL lives up to ``expires_at`` (may be <= 0).

    Cassandra sets ``localDeletionTime = floor(server now) + ttl`` in whole seconds, so
    ``ceil(expires_at) - floor(now)`` guarantees rows never expire BEFORE ``expires_at`` (they may outlive
    it by < 1 s; the API decides expiry from ``expires_at`` itself). ``ceil(expires_at - now)`` could
    expire them up to 1 s early, turning ``generation_expired`` into ``generation_inconsistent``.
    """
    now = time.time() if now_epoch_s is None else float(now_epoch_s)
    return int(math.ceil(float(expires_at_epoch_s)) - math.floor(now))


def ttl_seconds(expires_at_epoch_s: float, now_epoch_s: float | None = None) -> int:
    """TTL for a write now: :func:`remaining_seconds`, raising when below 1 (TTL 0 = never expire)."""
    ttl = remaining_seconds(expires_at_epoch_s, now_epoch_s)
    if ttl < 1:
        raise GenerationExpiredError(f"remaining TTL {ttl}s < 1s (expires_at={expires_at_epoch_s}, "
                                     f"now={now_epoch_s if now_epoch_s is not None else 'wall'})")
    return ttl


# --------------------------------------------------------------------------------------------------
# Value conversion (pure)
# --------------------------------------------------------------------------------------------------

def _unset():
    from cassandra.query import UNSET_VALUE

    return UNSET_VALUE


def convert_value(value: Any, kind: str, unset: Any = None) -> Any:
    """Python value from a Spark Row -> driver bind value (``unset`` is returned for None/empty)."""
    if unset is None:
        unset = _unset()
    if value is None:
        return unset
    if kind == KIND_TS:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"timestamp columns must arrive as epoch-ms int, got {type(value).__name__}")
        return _EPOCH + timedelta(milliseconds=value)
    if kind == KIND_DATE:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        raise TypeError(f"date columns must arrive as datetime.date, got {type(value).__name__}")
    if kind == KIND_SET:
        s = set(value)
        return s if s else unset
    if kind == KIND_LIST:
        lst = list(value)
        return lst if lst else unset
    if kind == KIND_MAP:
        m = dict(value)
        return m if m else unset
    return value


def convert_row(values: Iterable[Any], kinds: list[str], unset: Any = None) -> tuple[Any, ...]:
    vals = tuple(values)
    if len(vals) != len(kinds):
        raise ValueError(f"row has {len(vals)} values but the plan has {len(kinds)} columns")
    return tuple(convert_value(v, k, unset) for v, k in zip(vals, kinds, strict=True))


# --------------------------------------------------------------------------------------------------
# Worker-side session
# --------------------------------------------------------------------------------------------------

def _shutdown() -> None:
    with _LOCK:
        cluster = _STATE.get("cluster")
        _STATE.update(key=None, cluster=None, session=None)
        _PREPARED.clear()
    if cluster is not None:
        try:
            cluster.shutdown()
        except Exception:  # noqa: BLE001 - interpreter exit
            pass


def worker_session(conf: Mapping[str, Any]):
    """Lazily created, per-Python-worker cached Session (never pickled)."""
    from cassandra import ConsistencyLevel
    from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
    from cassandra.policies import DCAwareRoundRobinPolicy
    from cassandra.query import dict_factory

    key = (tuple(conf["hosts"]), conf["port"], conf["keyspace"])
    with _LOCK:
        sess = _STATE.get("session")
        if sess is not None and _STATE.get("key") == key and not getattr(sess, "is_shutdown", False):
            return sess
        old = _STATE.get("cluster")
        if old is not None:
            try:
                old.shutdown()
            except Exception:  # noqa: BLE001
                pass
        _PREPARED.clear()
        profile = ExecutionProfile(
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=conf["local_dc"]),
            consistency_level=ConsistencyLevel.name_to_value[str(conf["consistency"]).upper()],
            serial_consistency_level=ConsistencyLevel.name_to_value[str(conf["serial_consistency"]).upper()],
            request_timeout=conf["request_timeout"],
            row_factory=dict_factory,
        )
        cluster = Cluster(contact_points=list(conf["hosts"]), port=conf["port"],
                          execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                          connect_timeout=conf["connect_timeout"])
        session = cluster.connect(conf["keyspace"])
        _STATE.update(key=key, cluster=cluster, session=session)
        if not _ATEXIT["registered"]:
            atexit.register(_shutdown)
            _ATEXIT["registered"] = True
        return session


def _prepared(session, table: str, columns: tuple[str, ...]):
    k = (table, columns)
    with _LOCK:
        ps = _PREPARED.get(k)
    if ps is None:
        ps = session.prepare(insert_cql(table, columns))
        ps.is_idempotent = True
        with _LOCK:
            _PREPARED[k] = ps
    return ps


def _retryable() -> tuple[type[BaseException], ...]:
    """Transient failures only (timeouts, unavailable, connection loss); validation errors are not retried."""
    from cassandra import OperationTimedOut, RequestExecutionException
    from cassandra.cluster import NoHostAvailable
    from cassandra.connection import ConnectionException

    return (RequestExecutionException, OperationTimedOut, NoHostAvailable, ConnectionException)


def _flush(session, conf: Mapping[str, Any], table: str, spec: Mapping[str, Any], buf: list[tuple]) -> float:
    from cassandra.concurrent import execute_concurrent_with_args

    if not buf:
        return 0.0
    stmt = _prepared(session, table, tuple(spec["columns"]))
    retryable = _retryable()
    attempts = max(1, int(conf.get("max_attempts", 3)))
    backoff = float(conf.get("retry_backoff_seconds", 0.5))
    t0 = time.monotonic()
    for attempt in range(1, attempts + 1):
        ttl = ttl_seconds(spec["expires_at"])          # recomputed per flush (and per retry)
        params = [vals + (ttl,) for vals in buf]
        try:
            execute_concurrent_with_args(session, stmt, params, concurrency=int(conf["concurrency"]),
                                         raise_on_first_error=True)
            return time.monotonic() - t0
        except retryable as exc:
            if attempt >= attempts:
                raise SinkWriteError(
                    f"write to {table} failed after {attempts} attempts ({len(buf)} rows): {exc!r}") from exc
            time.sleep(backoff * (2 ** (attempt - 1)))
    raise AssertionError("unreachable")  # pragma: no cover


def make_partition_writer(
    conf: Mapping[str, Any],
    plan: Mapping[str, Mapping[str, Any]],
    fault: Mapping[str, Any] | None = None,
) -> Callable[[Iterator[tuple[str, tuple]]], Iterator[tuple[str, int, float]]]:
    """Build the ``mapPartitions`` function. ``fault={"fail_after_rows": n}`` = executor_write_error."""
    conf = dict(conf)
    plan = {t: dict(spec) for t, spec in plan.items()}
    fail_after = int(fault["fail_after_rows"]) if fault else None

    def write_partition(rows: Iterator[tuple[str, tuple]]) -> Iterator[tuple[str, int, float]]:
        from transitpulse.processing.faults import InjectedWriteError

        unset = _unset()
        session = None
        buffers: dict[str, list[tuple]] = {}
        written: dict[str, int] = {}
        seconds: dict[str, float] = {}
        seen = 0
        size = max(1, int(conf["buffer_rows"]))

        def flush(t: str) -> None:
            nonlocal session
            buf = buffers.get(t)
            if not buf:
                return
            if session is None:
                session = worker_session(conf)
            seconds[t] = seconds.get(t, 0.0) + _flush(session, conf, t, plan[t], buf)
            written[t] = written.get(t, 0) + len(buf)
            buffers[t] = []

        for table, values in rows:
            spec = plan.get(table)
            if spec is None:
                raise ValueError(f"no write plan for table {table!r}")
            buffers.setdefault(table, []).append(convert_row(values, spec["kinds"], unset))
            seen += 1
            if len(buffers[table]) >= size:
                flush(table)
            if fail_after is not None and seen >= fail_after:
                for t in list(buffers):
                    flush(t)
                raise InjectedWriteError(f"fault injection executor_write_error after {seen} rows")
        for t in list(buffers):
            flush(t)
        for t, n in written.items():
            yield (t, n, seconds.get(t, 0.0))

    return write_partition
