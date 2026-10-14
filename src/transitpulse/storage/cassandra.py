"""Driver-side Cassandra session management (WP4; interfaces §6).

* One process-cached ``Session`` per (contact points, keyspace) built from ``ProjectConfig.cassandra``.
* The legacy session attributes requested by the interface (``row_factory = dict_factory``, default
  consistency from config, serial consistency SERIAL, ``default_timeout``) are expressed through the
  driver's execution profiles, which is the only supported way once profiles are used (setting the
  legacy attributes on a profile-based session raises). The effect is identical:

  - ``EXEC_PROFILE_DEFAULT``: ``consistency_level = cfg.cassandra.consistency`` (LOCAL_ONE on the RF1
    demo), ``serial_consistency_level = cfg.cassandra.serial_consistency`` (SERIAL, used by every LWT),
    ``request_timeout = cfg.cassandra.request_timeout_seconds``, ``row_factory = dict_factory``.
  - ``SERIAL_PROFILE``: same, but ``consistency_level = SERIAL``. Used to READ rows that are managed by
    lightweight transactions (pointer, manifest, run clock, guarded status rows) so that a read observes
    every committed Paxos round.

* ``prepare(session, cql)`` caches PreparedStatements per session and CQL text.
* The Spark executor-side writer has its OWN lazily created session (``processing/sink.py``); a Session
  cannot be pickled to executors.

Values read back from Cassandra are normalized by :func:`normalize_row`: timestamps become timezone-aware
UTC datetimes (the driver returns naive UTC), ``date`` becomes ``datetime.date``, maps become ``dict`` and
sets become sorted lists.
"""

from __future__ import annotations

import atexit
import threading
import weakref
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any

from transitpulse.config import ProjectConfig

SERIAL_PROFILE = "transitpulse_serial_read"

_LOCK = threading.RLock()
_SESSIONS: dict[tuple[Any, ...], tuple[Any, Any]] = {}   # key -> (cluster, session)
_PREPARED: weakref.WeakKeyDictionary[Any, dict[str, Any]] = weakref.WeakKeyDictionary()
_ATEXIT_REGISTERED = False


# --------------------------------------------------------------------------------------------------
# Session / cluster
# --------------------------------------------------------------------------------------------------

def consistency_level(name: str) -> int:
    """Map a configured consistency name (e.g. ``LOCAL_ONE``) to the driver constant."""
    from cassandra import ConsistencyLevel

    try:
        return ConsistencyLevel.name_to_value[name.strip().upper()]
    except KeyError as exc:
        raise ValueError(f"unknown Cassandra consistency level {name!r}") from exc


def _profiles(cfg: ProjectConfig) -> dict[Any, Any]:
    from cassandra.cluster import EXEC_PROFILE_DEFAULT, ExecutionProfile
    from cassandra.policies import DCAwareRoundRobinPolicy
    from cassandra.query import dict_factory

    c = cfg.cassandra
    serial = consistency_level(c.serial_consistency)
    default = ExecutionProfile(
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=c.local_dc),
        consistency_level=consistency_level(c.consistency),
        serial_consistency_level=serial,
        request_timeout=c.request_timeout_seconds,
        row_factory=dict_factory,
    )
    serial_read = ExecutionProfile(
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=c.local_dc),
        consistency_level=serial,
        serial_consistency_level=serial,
        request_timeout=c.request_timeout_seconds,
        row_factory=dict_factory,
    )
    return {EXEC_PROFILE_DEFAULT: default, SERIAL_PROFILE: serial_read}


def build_cluster(cfg: ProjectConfig):
    """A new (not connected) ``Cluster``; protocol version is negotiated by the driver."""
    from cassandra.cluster import Cluster

    c = cfg.cassandra
    return Cluster(
        contact_points=list(c.hosts),
        port=c.port,
        execution_profiles=_profiles(cfg),
        connect_timeout=c.connect_timeout_seconds,
    )


def _session_key(cfg: ProjectConfig, keyspace: bool) -> tuple[Any, ...]:
    c = cfg.cassandra
    return (
        tuple(c.hosts), c.port, c.local_dc, c.consistency, c.serial_consistency,
        float(c.request_timeout_seconds), float(c.connect_timeout_seconds), c.keyspace if keyspace else None,
    )


def get_session(cfg: ProjectConfig, *, keyspace: bool = True):
    """Process-cached Session (thread-safe; shared by the batch thread and the query listener)."""
    global _ATEXIT_REGISTERED
    key = _session_key(cfg, keyspace)
    with _LOCK:
        hit = _SESSIONS.get(key)
        if hit is not None and not getattr(hit[1], "is_shutdown", False):
            return hit[1]
        cluster = build_cluster(cfg)
        try:
            session = cluster.connect(cfg.cassandra.keyspace if keyspace else None)
        except Exception:
            cluster.shutdown()
            raise
        _SESSIONS[key] = (cluster, session)
        if not _ATEXIT_REGISTERED:
            atexit.register(shutdown)
            _ATEXIT_REGISTERED = True
        return session


def shutdown() -> None:
    """Shut down every cached cluster (idempotent)."""
    with _LOCK:
        items = list(_SESSIONS.values())
        _SESSIONS.clear()
    for cluster, _session in items:
        try:
            cluster.shutdown()
        except Exception:  # noqa: BLE001 - best effort at interpreter exit
            pass


def prepare(session, cql: str, *, idempotent: bool = False):
    """Cached PreparedStatement for ``cql`` on ``session``.

    ``idempotent=True`` marks plain upserts as safe for driver-level retries; LWT statements must stay
    non-idempotent (a retried Paxos round could observe its own first attempt).
    """
    with _LOCK:
        per_session = _PREPARED.get(session)
        if per_session is None:
            per_session = {}
            _PREPARED[session] = per_session
        ps = per_session.get(cql)
    if ps is None:
        ps = session.prepare(cql)
        if idempotent:
            try:
                ps.is_idempotent = True
            except AttributeError:  # pragma: no cover - fake sessions in unit tests
                pass
        with _LOCK:
            per_session[cql] = ps
    return ps


def execute(session, cql: str, params: Sequence[Any] | None = None, *, serial: bool = False,
            idempotent: bool = False, timeout: float | None = None):
    """Prepare (cached) and execute ``cql`` with positional ``params``.

    ``serial=True`` executes with the SERIAL read profile (use it for reads of LWT-managed rows).
    """
    from cassandra.cluster import EXEC_PROFILE_DEFAULT

    ps = prepare(session, cql, idempotent=idempotent)
    kwargs: dict[str, Any] = {"execution_profile": SERIAL_PROFILE if serial else EXEC_PROFILE_DEFAULT}
    if timeout is not None:
        kwargs["timeout"] = timeout
    return session.execute(ps, tuple(params or ()), **kwargs)


def unset_if_none(values: Sequence[Any]) -> tuple[Any, ...]:
    """Replace ``None`` by ``UNSET_VALUE`` (no tombstone; column left untouched / absent)."""
    from cassandra.query import UNSET_VALUE

    return tuple(UNSET_VALUE if v is None else v for v in values)


# --------------------------------------------------------------------------------------------------
# LWT result helpers
# --------------------------------------------------------------------------------------------------

def was_applied(rs) -> bool:
    """``ResultSet.was_applied`` for LWT statements (never the truthiness of the result)."""
    return bool(rs.was_applied)


def lwt_existing_row(rs) -> dict[str, Any] | None:
    """Row returned by a NOT applied LWT (``[applied]`` removed, values normalized).

    A failed ``INSERT ... IF NOT EXISTS`` returns the whole existing row; a failed conditional UPDATE
    returns only the columns named in the IF clause.
    """
    row = rs.one()
    if row is None:
        return None
    out = dict(row)
    out.pop("[applied]", None)
    return normalize_row(out)


# --------------------------------------------------------------------------------------------------
# Value normalization
# --------------------------------------------------------------------------------------------------

def to_utc(dt: datetime | None) -> datetime | None:
    """Aware UTC datetime; naive values are interpreted as UTC (that is what the driver returns)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def ms_floor(dt: datetime) -> datetime:
    """Truncate to millisecond precision (Cassandra ``timestamp`` precision); keeps tzinfo."""
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)


def epoch_ms(dt: datetime) -> int:
    """Exact epoch milliseconds of an aware (or naive-UTC) datetime (integer arithmetic, no float)."""
    d = to_utc(dt)
    assert d is not None
    delta = d - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1000 + delta.microseconds // 1000


def normalize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, datetime):
        return to_utc(v)
    if isinstance(v, date):
        return v
    cls = type(v).__name__
    if cls == "Date" and hasattr(v, "date"):          # cassandra.util.Date
        return v.date()
    if isinstance(v, Mapping):                         # OrderedMapSerializedKey / dict
        return {k: normalize_value(x) for k, x in v.items()}
    if isinstance(v, (set, frozenset)) or cls == "SortedSet":
        items = [normalize_value(x) for x in v]
        try:
            return sorted(items)
        except TypeError:  # pragma: no cover - mixed types never occur in this schema
            return items
    if isinstance(v, (list, tuple)):
        return [normalize_value(x) for x in v]
    return v


def normalize_row(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {k: normalize_value(v) for k, v in row.items()}
