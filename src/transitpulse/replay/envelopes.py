"""Re-derive snapshot envelopes from archived raw captures (D-E12).

For every capture the publisher re-reads the raw file (sha256 verified), re-parses it with the
collector's parser, re-applies the REPLAY run's scope filter and recomputes ``snapshot_id`` with the new
``run_id`` through :func:`transitpulse.ingestion.envelope_builder.envelope_from_capture`. ``capture_id``,
``raw_path`` and ``original_fetched_at`` are kept from the recording; ``published_at`` is set by the
producer at send time.

The ingestion modules (WP2) and the scope index loader (WP1) are imported when a
:class:`CaptureEnveloper` is constructed, so that this module (and the publisher) can be imported and
unit-tested with a fake enveloper.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from transitpulse.contracts.accounting import count_candidates
from transitpulse.contracts.envelope import envelope_size
from transitpulse.replay.timeline import CaptureRef
from transitpulse.runs import RunRecord
from transitpulse.runtime.paths import resolve_under

PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"


@dataclass(frozen=True)
class RawLocation:
    """Same fields as ``ingestion.archive.RawRef``."""

    raw_path: str
    raw_sha256: str
    raw_bytes: int
    stored_sha256: str
    stored_bytes: int


@dataclass(frozen=True)
class EnvelopeOutcome:
    envelope: dict[str, Any] | None
    rejection_code: str | None
    rejection_detail: str | None
    candidates: int
    selected_count: int
    feed_timestamp: int | None
    size_bytes: int | None = None

    @property
    def ok(self) -> bool:
        return self.envelope is not None and self.rejection_code is None


class Enveloper(Protocol):
    """What the publisher needs to turn captures into envelopes (real: :class:`CaptureEnveloper`)."""

    def read_capture(self, capture: CaptureRef) -> bytes: ...

    def raw_location(self, capture: CaptureRef) -> RawLocation: ...

    def build(self, *, feed_type: str, body: bytes, fetched_at: datetime, raw: RawLocation, capture_id: str,
              source_url: str | None, http_status: int | None) -> EnvelopeOutcome: ...

    def archive_injected(self, *, feed_type: str, capture_id: str, fetched_at: datetime, body: bytes,
                         meta: dict[str, Any]) -> RawLocation: ...


class CaptureEnveloper:
    """Default enveloper backed by the collector's parser, scope filter, archive and envelope builder."""

    def __init__(
        self,
        *,
        run: RunRecord,
        recording_dir: Path,
        raw_root: Path,
        max_snapshot_bytes: int,
        scope_dir: Path | None = None,
        component: str = "publisher",
    ) -> None:
        from transitpulse.ingestion.archive import RawArchive, RawRef
        from transitpulse.ingestion.envelope_builder import envelope_from_capture
        from transitpulse.ingestion.feed_parse import Rejection, parse_feed
        from transitpulse.ingestion.scope_filter import ScopeFilter
        from transitpulse.static.index import load_scope_index

        self._run = run
        self._recording_dir = Path(recording_dir)
        self._raw_root = Path(raw_root)
        self._max_bytes = int(max_snapshot_bytes)
        self._component = component
        self._recording_archive = RawArchive(self._recording_dir)
        self._live_archive = RawArchive(self._raw_root)
        self._RawRef = RawRef
        self._envelope_from_capture = envelope_from_capture
        self._parse_feed = parse_feed
        self._Rejection = Rejection
        index = load_scope_index(Path(scope_dir or run.static_scope_dir))
        if index.gtfs_version != run.gtfs_version:
            raise ValueError(
                f"scope index {scope_dir or run.static_scope_dir} belongs to GTFS version {index.gtfs_version}, "
                f"run {run.run_id} pins {run.gtfs_version}"
            )
        self._filter = ScopeFilter(index)

    def read_capture(self, capture: CaptureRef) -> bytes:
        if capture.raw_path is None:
            raise ValueError(f"capture {capture.capture_id} has no raw file")
        return self._recording_archive.read_capture(capture.raw_path, expected_sha256=capture.raw_sha256)

    def raw_location(self, capture: CaptureRef) -> RawLocation:
        if capture.raw_path is None or capture.raw_sha256 is None:
            raise ValueError(f"capture {capture.capture_id} has no raw file")
        stored = resolve_under(self._recording_dir, capture.raw_path)
        stored_bytes = stored.stat().st_size if stored.is_file() else 0
        return RawLocation(
            raw_path=capture.raw_path,
            raw_sha256=capture.raw_sha256,
            raw_bytes=int(capture.raw_bytes or 0),
            stored_sha256=capture.stored_sha256 or "",
            stored_bytes=stored_bytes,
        )

    def build(self, *, feed_type: str, body: bytes, fetched_at: datetime, raw: RawLocation, capture_id: str,
              source_url: str | None, http_status: int | None) -> EnvelopeOutcome:
        parsed = self._parse_feed(body, fetched_at)
        if isinstance(parsed, self._Rejection):
            return EnvelopeOutcome(None, parsed.reason_code, parsed.detail, 0, 0, None)
        selected, accounting = self._filter.select(feed_type, parsed.entities)
        raw_ref = self._RawRef(
            raw_path=raw.raw_path,
            raw_sha256=raw.raw_sha256,
            raw_bytes=raw.raw_bytes,
            stored_sha256=raw.stored_sha256,
            stored_bytes=raw.stored_bytes,
        )
        http = {"status": http_status, "etag": None, "last_modified": None, "content_type": None}
        env = self._envelope_from_capture(
            run=self._run,
            feed_type=feed_type,
            parsed=parsed,
            selected=selected,
            accounting=accounting,
            raw_ref=raw_ref,
            capture_id=capture_id,
            fetched_at=fetched_at,
            source_url=source_url or "",
            http=http,
            scenario_id=self._run.scenario_id,
            component=self._component,
        )
        size = envelope_size(env)
        candidates = count_candidates(feed_type, selected)
        if size > self._max_bytes:
            return EnvelopeOutcome(None, PAYLOAD_TOO_LARGE, f"envelope {size} bytes > limit {self._max_bytes}",
                                   candidates, len(selected), parsed.feed_timestamp, size)
        return EnvelopeOutcome(dict(env), None, None, candidates, len(selected), parsed.feed_timestamp, size)

    def archive_injected(self, *, feed_type: str, capture_id: str, fetched_at: datetime, body: bytes,
                         meta: dict[str, Any]) -> RawLocation:
        ref = self._live_archive.write_capture(self._run.run_id, feed_type, capture_id, fetched_at, body, meta)
        return RawLocation(
            raw_path=ref.raw_path,
            raw_sha256=ref.raw_sha256,
            raw_bytes=ref.raw_bytes,
            stored_sha256=ref.stored_sha256,
            stored_bytes=ref.stored_bytes,
        )
