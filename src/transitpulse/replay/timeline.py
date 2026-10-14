"""Replay timeline built from a recording manifest (design E §3.2, interfaces §4-§5, D-E13, D-E15).

Ordering rules
--------------
* Real recordings (``source_type`` ``live``/``replay``) are ordered by ``(at, FEED_ORDER, seq)``: one
  interleaved timeline across the three feeds (never "all positions first").
* Synthetic recordings follow their explicit ``seq`` so that late items (an older ``at`` after a newer
  one) can be expressed. Late items never move the clock backwards (the publisher enforces this).
* ``tick``, ``republish``, ``arm_fault``, ``malformed_envelope`` and ``barrier`` items are allowed
  only for synthetic recordings (D-E15).

Special items without an ``at`` inherit the time of the previous item (or the timeline start), so a
barrier never advances the clock.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Any

from transitpulse.contracts.enums import FEED_ORDER
from transitpulse.contracts.envelope import parse_iso_utc

CAPTURE = "capture"
FETCH_EVENT = "fetch_event"
TICK = "tick"
REPUBLISH = "republish"
ARM_FAULT = "arm_fault"
MALFORMED_ENVELOPE = "malformed_envelope"
BARRIER = "barrier"

BASE_KINDS = frozenset({CAPTURE, FETCH_EVENT})
SPECIAL_KINDS = frozenset({TICK, REPUBLISH, ARM_FAULT, MALFORMED_ENVELOPE, BARRIER})
ALL_KINDS = BASE_KINDS | SPECIAL_KINDS
PARSE_OK = "ok"
SYNTHETIC = "synthetic"


class TimelineError(ValueError):
    """The manifest items cannot form a valid replay timeline."""


@dataclass(frozen=True)
class CaptureRef:
    """Reference to one archived raw capture (relative ``raw_path`` under the recording directory)."""

    capture_id: str
    raw_path: str | None
    raw_sha256: str | None
    raw_bytes: int | None
    stored_sha256: str | None
    parse_status: str
    http_status: int | None
    feed_timestamp: int | None
    source_url: str | None = None

    @property
    def is_ok(self) -> bool:
        return self.parse_status == PARSE_OK


@dataclass(frozen=True)
class TimelineItem:
    seq: int
    kind: str
    at: datetime                         # source time (original_fetched_at for captures)
    feed_type: str | None = None
    capture: CaptureRef | None = None
    params: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    label: str | None = None

    def describe(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "kind": self.kind,
            "at": self.at.isoformat(),
            "feed_type": self.feed_type,
            "capture_id": self.capture.capture_id if self.capture else None,
            "label": self.label,
        }


def _int_or_none(value: Any, what: str, seq: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TimelineError(f"item seq={seq}: {what} must be an integer, got {value!r}")
    return value


def _parse_at(value: Any, seq: Any) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TimelineError(f"item seq={seq}: 'at' must be an ISO-8601 UTC string, got {value!r}")
    try:
        return parse_iso_utc(value)
    except ValueError as exc:
        raise TimelineError(f"item seq={seq}: invalid 'at' {value!r}: {exc}") from exc


def manifest_start_clock(manifest: Mapping[str, Any]) -> datetime | None:
    """Explicit start clock of a recording (``start_clock`` or ``time_range.start_clock``), if any."""
    for raw in (manifest.get("start_clock"), (manifest.get("time_range") or {}).get("start_clock")):
        if isinstance(raw, str) and raw:
            return parse_iso_utc(raw)
    return None


def build_timeline(manifest: Mapping[str, Any], *, allow_special: bool | None = None) -> list[TimelineItem]:
    """Validate and order the manifest items.

    ``allow_special`` defaults to ``manifest.source_type == "synthetic"``; the publisher passes the
    run's own source type so that a non-synthetic run can never execute synthetic-only items.
    """
    source_type = manifest.get("source_type")
    synthetic = source_type == SYNTHETIC
    if allow_special is None:
        allow_special = synthetic
    raw_items = manifest.get("items")
    if not isinstance(raw_items, list):
        raise TimelineError("recording manifest has no 'items' list")
    feeds = manifest.get("feeds") or {}

    seen: set[int] = set()
    checked: list[dict[str, Any]] = []
    for pos, it in enumerate(raw_items):
        if not isinstance(it, Mapping):
            raise TimelineError(f"item #{pos} is not an object")
        seq = it.get("seq")
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            raise TimelineError(f"item #{pos}: 'seq' must be a non-negative integer, got {seq!r}")
        if seq in seen:
            raise TimelineError(f"duplicate item seq {seq}")
        seen.add(seq)
        kind = it.get("kind")
        if kind not in ALL_KINDS:
            raise TimelineError(f"item seq={seq}: unknown kind {kind!r}")
        if kind in SPECIAL_KINDS and not allow_special:
            raise TimelineError(
                f"item seq={seq}: kind {kind!r} is only allowed for synthetic recordings and synthetic runs "
                "(D-E15)"
            )
        checked.append(dict(it))

    if synthetic:
        ordered = sorted(checked, key=lambda d: d["seq"])
    else:
        for d in checked:
            if d.get("at") is None:
                raise TimelineError(f"item seq={d['seq']}: real recordings require 'at' on every item")
        ordered = sorted(
            checked,
            key=lambda d: (_parse_at(d["at"], d["seq"]), FEED_ORDER.get(d.get("feed_type") or "", 99), d["seq"]),
        )

    start = manifest_start_clock(manifest)
    if start is None:
        for d in ordered:
            parsed = _parse_at(d.get("at"), d["seq"])
            if parsed is not None:
                start = parsed
                break
    if start is None:
        raise TimelineError("cannot determine the timeline start: no item has an 'at' and no start_clock")

    items: list[TimelineItem] = []
    capture_positions: dict[int, int] = {}
    previous_at = start
    for d in ordered:
        seq = d["seq"]
        kind = d["kind"]
        at = _parse_at(d.get("at"), seq)
        if at is None:
            if kind in (CAPTURE, FETCH_EVENT, TICK):
                raise TimelineError(f"item seq={seq}: kind {kind!r} requires 'at'")
            at = previous_at
        params_raw = d.get("params") or {}
        if not isinstance(params_raw, Mapping):
            raise TimelineError(f"item seq={seq}: 'params' must be an object")
        params = dict(params_raw)
        feed_type = d.get("feed_type")
        label = d.get("label")
        capture: CaptureRef | None = None

        if kind in (CAPTURE, FETCH_EVENT):
            if feed_type not in FEED_ORDER:
                raise TimelineError(f"item seq={seq}: invalid feed_type {feed_type!r}")
        if kind == CAPTURE:
            capture_id = d.get("capture_id")
            if not isinstance(capture_id, str) or not capture_id:
                raise TimelineError(f"item seq={seq}: capture requires 'capture_id'")
            parse_status = d.get("parse_status") or PARSE_OK
            raw_path = d.get("raw_path")
            if raw_path is not None and (not isinstance(raw_path, str) or not raw_path):
                raise TimelineError(f"item seq={seq}: invalid raw_path {raw_path!r}")
            if parse_status == PARSE_OK and raw_path is None:
                raise TimelineError(f"item seq={seq}: an ok capture requires 'raw_path'")
            feed_meta = feeds.get(feed_type) if isinstance(feeds, Mapping) else None
            capture = CaptureRef(
                capture_id=capture_id,
                raw_path=raw_path,
                raw_sha256=d.get("raw_sha256"),
                raw_bytes=_int_or_none(d.get("raw_bytes"), "raw_bytes", seq),
                stored_sha256=d.get("stored_sha256"),
                parse_status=str(parse_status),
                http_status=_int_or_none(d.get("http_status"), "http_status", seq),
                feed_timestamp=_int_or_none(d.get("feed_timestamp"), "feed_timestamp", seq),
                source_url=(feed_meta or {}).get("source_url") if isinstance(feed_meta, Mapping) else None,
            )
            if raw_path is not None and not capture.raw_sha256:
                raise TimelineError(f"item seq={seq}: capture with raw_path requires 'raw_sha256'")
            capture_positions[seq] = len(items)
        elif kind == REPUBLISH:
            of_seq = params.get("of_seq")
            if isinstance(of_seq, bool) or not isinstance(of_seq, int):
                raise TimelineError(f"item seq={seq}: republish requires integer params.of_seq")
            if of_seq not in capture_positions:
                raise TimelineError(
                    f"item seq={seq}: republish of_seq={of_seq} does not reference an earlier capture item"
                )
            feed_type = feed_type or items[capture_positions[of_seq]].feed_type
        elif kind == MALFORMED_ENVELOPE:
            feed_type = feed_type or params.get("feed_type")
            if feed_type not in FEED_ORDER:
                raise TimelineError(f"item seq={seq}: malformed_envelope requires a valid params.feed_type")
            if not isinstance(params.get("value_text"), str):
                raise TimelineError(f"item seq={seq}: malformed_envelope requires params.value_text (string)")
        elif kind == ARM_FAULT:
            point = params.get("point")
            match = params.get("match") or {}
            if not isinstance(point, str) or not point:
                raise TimelineError(f"item seq={seq}: arm_fault requires params.point")
            if not isinstance(match, Mapping) or match.get("feed_type") not in FEED_ORDER:
                raise TimelineError(f"item seq={seq}: arm_fault requires params.match.feed_type")
            feed_type = feed_type or match.get("feed_type")
        elif kind == BARRIER:
            lab = params.get("label", label)
            if not isinstance(lab, str) or not lab:
                raise TimelineError(f"item seq={seq}: barrier requires params.label")
            label = label or lab

        items.append(
            TimelineItem(
                seq=seq,
                kind=kind,
                at=at,
                feed_type=feed_type,
                capture=capture,
                params=MappingProxyType(params),
                label=label if isinstance(label, str) and label else None,
            )
        )
        previous_at = at
    return items


def timeline_start(manifest: Mapping[str, Any], items: Sequence[TimelineItem]) -> datetime:
    """Initial source clock of a run replaying this recording."""
    explicit = manifest_start_clock(manifest)
    if explicit is not None:
        return explicit
    first = (manifest.get("time_range") or {}).get("first_fetched_at")
    candidates = [it.at for it in items]
    if isinstance(first, str) and first:
        candidates.append(parse_iso_utc(first))
    if not candidates:
        raise TimelineError("empty recording: no items and no time range")
    return min(candidates)


def items_by_seq(items: Sequence[TimelineItem]) -> dict[int, TimelineItem]:
    return {it.seq: it for it in items}
