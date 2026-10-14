"""Per-feed polling scheduler (design E §1.1).

One daemon thread per feed. Feeds are staggered (feed ``i`` of ``n`` starts at ``i * interval / n``)
so the three requests never leave together. Slots are on a fixed grid (``start + k * interval``):

* a poll that overruns its slot does not trigger catch-up requests; the missed slots are skipped and
  counted (``missed_polls``), so the scheduler never bursts;
* a server ``Retry-After`` longer than the interval defers the next poll to ``now + retry_after``;
* an exception in one feed's task is logged and that feed continues on its next slot; other feeds are
  never affected.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Mapping, Sequence

from transitpulse.obs.logging import log_event

log = logging.getLogger(__name__)

PollTask = Callable[[str], float | None]


def advance_slot(next_due: float, interval: float, now: float, defer_seconds: float | None) -> tuple[float, int]:
    """Return ``(next_due, missed_slots)`` after a poll that was due at ``next_due`` finished at ``now``.

    Pure function (monotonic seconds). Missed slots are skipped, never replayed. A deferral longer
    than the interval moves the next poll to ``now + defer_seconds``.
    """
    if interval <= 0:
        raise ValueError("poll interval must be positive")
    nxt = next_due + interval
    missed = 0
    if nxt < now:
        missed = int((now - nxt) // interval) + 1
        nxt += missed * interval
    if defer_seconds is not None and defer_seconds > interval:
        nxt = max(nxt, now + defer_seconds)
    return nxt, missed


class PollScheduler:
    """Runs ``task(feed)`` on a staggered fixed grid, one thread per feed, until ``stop`` is set.

    ``task`` returns an optional deferral in seconds (the fetch's ``retry_after_s``).
    """

    def __init__(
        self,
        feeds: Sequence[str],
        intervals: Mapping[str, float],
        task: PollTask,
        stop: threading.Event,
        *,
        on_missed: Callable[[str, int], None] | None = None,
        on_error: Callable[[str, BaseException], None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not feeds:
            raise ValueError("at least one feed is required")
        for feed in feeds:
            if float(intervals[feed]) <= 0:
                raise ValueError(f"poll interval for {feed} must be positive")
        self.feeds = tuple(feeds)
        self.intervals = {f: float(intervals[f]) for f in feeds}
        self._task = task
        self._stop = stop
        self._on_missed = on_missed
        self._on_error = on_error
        self._mono = monotonic
        self._threads: dict[str, threading.Thread] = {}
        self._start: float | None = None

    def start(self) -> None:
        if self._threads:
            raise RuntimeError("scheduler already started")
        self._start = self._mono()
        n = len(self.feeds)
        for idx, feed in enumerate(self.feeds):
            offset = idx * self.intervals[feed] / n
            t = threading.Thread(target=self._loop, args=(feed, self._start + offset),
                                 name=f"poll-{feed}", daemon=True)
            self._threads[feed] = t
            t.start()

    def join(self, timeout: float) -> list[str]:
        """Wait up to ``timeout`` seconds in total; return the feeds whose thread is still alive."""
        deadline = self._mono() + max(0.0, timeout)
        for t in self._threads.values():
            t.join(max(0.0, deadline - self._mono()))
        return [f for f, t in self._threads.items() if t.is_alive()]

    def alive(self) -> list[str]:
        return [f for f, t in self._threads.items() if t.is_alive()]

    def _loop(self, feed: str, first_due: float) -> None:
        interval = self.intervals[feed]
        next_due = first_due
        while not self._stop.wait(max(0.0, next_due - self._mono())):
            defer: float | None = None
            try:
                defer = self._task(feed)
            except Exception as exc:  # one feed must never stop the others
                log_event(log, "fetch", "failed", f"poll task raised: {type(exc).__name__}: {exc}"[:500],
                          level=logging.ERROR, feed=feed)
                log.debug("poll task traceback", exc_info=True)
                if self._on_error is not None:
                    try:
                        self._on_error(feed, exc)
                    except Exception:
                        log.exception("on_error callback failed for feed %s", feed)
            next_due, missed = advance_slot(next_due, interval, self._mono(), defer)
            if missed and self._on_missed is not None:
                try:
                    self._on_missed(feed, missed)
                except Exception:
                    log.exception("on_missed callback failed for feed %s", feed)
