"""Background samplers of the benchmark harness (design E §8). They only observe; they never change the stack.

* :class:`ResourceSampler` — every ``interval`` s, CPU % and RSS of each supervised component (the registry pid plus
  all its children, e.g. Spark's JVM and Python workers under the processor), via ``psutil``.
* :class:`BacklogSampler` — every ``interval`` s, Kafka backlog per topic/partition = high watermark
  (``Consumer.get_watermark_offsets(tp, cached=False)``) minus the processor's latest committed end offset from
  ``query_progress.jsonl``.
* :func:`disk_usage` — apparent bytes of the Kafka, Cassandra, raw, recordings, checkpoint and curated directories.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psutil

from transitpulse.verify.progress import latest_end_offsets


class _Sampler:
    def __init__(self, interval: float) -> None:
        self.interval = float(interval)
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def _sample(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def _loop(self) -> None:
        while not self._stop.is_set():
            t0 = time.monotonic()
            try:
                rows = self._sample()
                with self._lock:
                    self.samples.extend(rows)
            except Exception as exc:  # noqa: BLE001 - a sampler must never kill the benchmark
                if len(self.errors) < 50:
                    self.errors.append(f"{type(exc).__name__}: {exc}"[:300])
            self._stop.wait(max(0.0, self.interval - (time.monotonic() - t0)))

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._loop, name=type(self).__name__, daemon=True)
            self._thread.start()

    def stop(self) -> list[dict[str, Any]]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(self.interval + 10)
        with self._lock:
            return list(self.samples)


class ResourceSampler(_Sampler):
    """CPU % (sum over the process tree, 100 = one core) and RSS bytes per supervised component."""

    def __init__(self, supervisor: Any, components: Iterable[str], interval: float = 2.0) -> None:
        super().__init__(interval)
        self.sup = supervisor
        self.components = tuple(components)
        self._procs: dict[int, psutil.Process] = {}

    def _tree(self, component: str) -> list[psutil.Process]:
        entry = self.sup.entry(component) or {}
        roots = [entry.get(k) for k in ("wrapper_pid", "pid")]
        out: dict[int, psutil.Process] = {}
        for pid in roots:
            if not isinstance(pid, int) or pid <= 1:
                continue
            try:
                proc = self._procs.get(pid) or psutil.Process(pid)
                out[proc.pid] = proc
                for child in proc.children(recursive=True):
                    out[child.pid] = child
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return list(out.values())

    def _sample(self) -> list[dict[str, Any]]:
        now_wall, now_mono = time.time(), time.monotonic()
        rows = []
        for comp in self.components:
            cpu = 0.0
            rss = 0
            n = 0
            primed = 0
            for proc in self._tree(comp):
                key = proc.pid
                cached = self._procs.get(key)
                try:
                    if cached is None or cached.create_time() != proc.create_time():
                        self._procs[key] = proc
                        proc.cpu_percent(None)       # first call primes the counter (CPU counted from next sample)
                        cached = proc
                        primed += 1
                    else:
                        cpu += cached.cpu_percent(None)
                    rss += cached.memory_info().rss
                    n += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    self._procs.pop(key, None)
            rows.append({"t_wall": now_wall, "t_mono": now_mono, "component": comp, "cpu_percent": round(cpu, 2),
                         "rss_bytes": rss, "processes": n, "cpu_primed_processes": primed})
        return rows


class BacklogSampler(_Sampler):
    """Kafka high watermark minus the processor's latest committed end offset, per topic/partition."""

    def __init__(self, bootstrap: str, progress_path: Path, topics: Iterable[str], interval: float = 5.0) -> None:
        super().__init__(interval)
        from confluent_kafka import Consumer

        self.progress_path = progress_path
        self.topics = tuple(topics)
        self._consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"tp-bench-{uuid.uuid4().hex[:8]}",
                                   "enable.auto.commit": False})
        self._partitions: dict[str, list[int]] = {}

    def _parts(self, topic: str) -> list[int]:
        if topic not in self._partitions:
            md = self._consumer.list_topics(topic, timeout=10)
            self._partitions[topic] = sorted(md.topics[topic].partitions)
        return self._partitions[topic]

    def measure(self) -> list[dict[str, Any]]:
        from confluent_kafka import TopicPartition

        ends = latest_end_offsets(self.progress_path)
        now_wall = time.time()
        rows = []
        for topic in self.topics:
            for part in self._parts(topic):
                wm = self._consumer.get_watermark_offsets(TopicPartition(topic, part), timeout=10, cached=False)
                if wm is None:
                    continue
                high = int(wm[1])
                processed = ends.get((topic, part))
                rows.append({"t_wall": now_wall, "topic": topic, "partition": part, "high_watermark": high,
                             "processed_end_offset": processed,
                             "backlog_messages": None if processed is None else max(0, high - processed)})
        return rows

    def _sample(self) -> list[dict[str, Any]]:
        return self.measure()

    def stop(self) -> list[dict[str, Any]]:
        out = super().stop()
        try:
            self._consumer.close()
        except Exception:  # noqa: BLE001 - closing a consumer at shutdown is best effort
            pass
        return out


def _dir_bytes(root: Path) -> int | None:
    if not root.exists():
        return None
    total = 0
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    try:
                        if e.is_dir(follow_symlinks=False):
                            stack.append(Path(e.path))
                        elif e.is_file(follow_symlinks=False):
                            total += e.stat(follow_symlinks=False).st_size
                    except OSError:
                        continue
        except OSError:
            continue
    return total


def disk_usage(paths: Any) -> dict[str, Any]:
    """Apparent size (bytes) of the stateful directories of the work dir, with the measurement time."""
    dirs = {
        "kafka": paths.kafka_data,
        "cassandra": paths.cassandra_root,
        "raw": paths.raw_root,
        "recordings": paths.recordings_root,
        "checkpoints": paths.root / "checkpoints",
        "curated": paths.curated_root,
    }
    out: dict[str, Any] = {"t_wall": time.time(), "method": "apparent file sizes (os.scandir, no symlinks)"}
    for name, d in dirs.items():
        out[name] = _dir_bytes(Path(d))
    return out
