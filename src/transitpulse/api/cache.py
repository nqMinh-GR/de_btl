"""Immutable-only LRU cache (D-E19).

Only values that can never change for their key are cached:
* READY ``snapshot_manifest`` rows, keyed by ``(run_id, feed_type, snapshot_id)``;
* generation rows, keyed by ``(table, run_id, feed_type, snapshot_id, partition key, limit)``;
* static rows, keyed by ``gtfs_version`` and the static key.

Run metadata (clock, state), feed pointers, ``pipeline_status`` and every freshness/age computed at
the application clock are **never** cached, so clock changes, pointer swaps and expiry are visible on
the very next request. Entry lifetime is ``min(cache_max_ttl_seconds, requested ttl)``; callers pass
``expires_at - now`` for generation data so nothing outlives the generation's expiry.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Hashable
from typing import Any

MISSING: Any = object()
"""Sentinel returned by :meth:`ImmutableCache.get` on a miss (``None`` is a cacheable value)."""


class ImmutableCache:
    """Thread-safe LRU with a per-entry TTL measured on the monotonic clock."""

    def __init__(
        self,
        max_entries: int,
        max_ttl_seconds: float,
        *,
        on_hit: Callable[[], None] | None = None,
        on_miss: Callable[[], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_entries < 0:
            raise ValueError("max_entries must be >= 0")
        self._max_entries = int(max_entries)
        self._max_ttl = float(max_ttl_seconds)
        self._on_hit = on_hit
        self._on_miss = on_miss
        self._clock = clock
        self._lock = threading.Lock()
        self._entries: OrderedDict[Hashable, tuple[float, Any]] = OrderedDict()

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def get(self, key: Hashable) -> Any:
        """Cached value or :data:`MISSING` (expired entries are dropped)."""
        now = self._clock()
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                expires, value = entry
                if expires > now:
                    self._entries.move_to_end(key)
                    hit = True
                else:
                    del self._entries[key]
                    hit = False
            else:
                hit = False
        if hit:
            if self._on_hit is not None:
                self._on_hit()
            return value
        if self._on_miss is not None:
            self._on_miss()
        return MISSING

    def put(self, key: Hashable, value: Any, ttl_seconds: float) -> bool:
        """Store ``value`` for ``min(ttl_seconds, max_ttl)``; returns False when not stored."""
        ttl = min(float(ttl_seconds), self._max_ttl)
        if ttl <= 0 or self._max_entries == 0:
            return False
        expires = self._clock() + ttl
        with self._lock:
            self._entries[key] = (expires, value)
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)
        return True

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
