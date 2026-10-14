"""IT13: message size boundary (MAX_SNAPSHOT_BYTES = 4 MiB serialized envelope; topic/broker limit 5 MiB).

* producer level (exact): an envelope of exactly 4 MiB is acknowledged; 4 MiB + 1 B raises ``PayloadTooLarge`` before
  anything is produced; raw 4.5 MiB bytes are accepted by the broker (headroom) and 6 MiB fail with the producer's
  ``MSG_SIZE_TOO_LARGE``;
* collector level (simulated source): a capture whose envelope is 4 MiB - 1 KiB becomes a READY generation; a capture
  whose envelope exceeds the limit keeps its raw file (sha256 verified), gets an outbox row ``rejected`` with
  ``PAYLOAD_TOO_LARGE`` and a DLQ reference, and never reaches the snapshot topic. The collector-level oversize body
  targets limit + 256 B so that the small, header-dependent ``http`` block cannot move it under the limit; the exact
  +1 B boundary is the producer-level case (the component that enforces it on the final bytes).
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

import pytest

from integration.fake_source import FakeMbtaSource, FakeResponse, body_bytes, empty_doc, now_epoch, padded_vehicles_doc
from integration.harness import (
    FixtureRun,
    create_fault_run,
    dlq_records,
    raw_capture_shas,
    start_collector,
    wait_for_outbox,
    wait_until,
)

pytestmark = [pytest.mark.integration, pytest.mark.slow]

VP = "vehicle_positions"
FOREIGN_KEY = b"it13-direct|vehicle_positions"     # a run id no processor ever filters for


def _capture_id(body: bytes, fetched: datetime) -> str:
    from transitpulse.contracts.ids import capture_id

    return capture_id("vp", fetched, hashlib.sha256(body).hexdigest())


def _raw_ref(paths: Any, run: Any, body: bytes, fetched: datetime) -> Any:
    """RawRef exactly as RawArchive would return it (raw_path layout and length are what matter for the size)."""
    from transitpulse.ingestion.archive import RawArchive, RawRef

    rel = RawArchive(paths.raw_root).capture_relpath(run.run_id, VP, fetched, _capture_id(body, fetched))
    return RawRef(raw_path=rel, raw_sha256=hashlib.sha256(body).hexdigest(), raw_bytes=len(body),
                  stored_sha256="0" * 64, stored_bytes=0)


def _envelope(factory: Any, paths: Any, run: Any, body: bytes, fetched: datetime, source_url: str,
              http: dict[str, Any] | None) -> dict[str, Any]:
    return factory.from_body(feed_type=VP, body=body, raw_ref=_raw_ref(paths, run, body, fetched),
                             capture_id=_capture_id(body, fetched), fetched_at=fetched, source_url=source_url,
                             http=http)


def sized_body(factory: Any, paths: Any, run: Any, target: int, *, header_ts: int, fetched: datetime,
               source_url: str, http: dict[str, Any] | None) -> tuple[bytes, dict[str, Any]]:
    """Body of a valid two-vehicle snapshot whose serialized envelope is exactly ``target`` bytes."""
    from transitpulse.contracts.envelope import envelope_size

    pad = 0
    for _ in range(16):
        body = body_bytes(padded_vehicles_doc(header_ts, pad))
        env = _envelope(factory, paths, run, body, fetched, source_url, http)
        size = envelope_size(env)
        if size == target:
            return body, env
        pad += target - size
        if pad < 0:
            raise AssertionError(f"the base envelope ({size} B) is already larger than the target {target} B")
    raise AssertionError(f"could not size an envelope to exactly {target} bytes")


@pytest.mark.it_case("IT13")
def test_it13_producer_size_limits(harness: Any, stack_ctx: Any, case_record: Any) -> None:
    from confluent_kafka import KafkaException

    from transitpulse.contracts.envelope import envelope_size, utcnow
    from transitpulse.contracts.topics import TOPIC_BY_FEED
    from transitpulse.ingestion.kafka_producer import PayloadTooLarge, is_size_error
    from transitpulse.verify.stack import HarnessError

    fr = FixtureRun.prepare(harness, stack_ctx, "golden", start=False)
    run = fr.run
    case_record.run_id = run.run_id
    limit = int(harness.cfg.kafka.max_snapshot_bytes)
    topic = TOPIC_BY_FEED[VP]
    fetched = utcnow()
    header_ts = int(fetched.timestamp()) - 1
    url = "synthetic://IT13/vehicle_positions"

    body, _ = sized_body(fr.factory, harness.paths, run, limit, header_ts=header_ts, fetched=fetched,
                         source_url=url, http=None)
    env = fr.factory.archive_and_build(harness.paths, feed_type=VP, body=body, fetched_at=fetched, source_url=url)
    assert envelope_size(env) == limit, f"archived envelope is {envelope_size(env)} B, wanted exactly {limit}"
    accepted = harness.publish_envelope(env)

    _, env_over = sized_body(fr.factory, harness.paths, run, limit + 1, header_ts=header_ts, fetched=fetched,
                             source_url=url, http=None)
    high_before = harness.watermarks(topic, accepted.partition)[1]
    with pytest.raises(PayloadTooLarge) as too_large:
        harness.producer(run.run_id).send_envelope(env_over)
    assert too_large.value.size == limit + 1 and too_large.value.limit == limit, str(too_large.value)
    assert harness.watermarks(topic, accepted.partition)[1] == high_before, "an oversize envelope reached the topic"

    headroom = harness.publish_raw_bytes(topic, FOREIGN_KEY, b"x" * (4 * 1024 * 1024 + 512 * 1024))
    size_error: Any = None
    try:
        harness.publish_raw_bytes(topic, FOREIGN_KEY, b"x" * (6 * 1024 * 1024))
    except KafkaException as exc:
        size_error = exc.args[0] if exc.args else exc
        assert is_size_error(size_error), f"6 MiB failed with an unexpected error: {exc}"
    except HarnessError as exc:
        size_error = str(exc)
        assert "MSG_SIZE_TOO_LARGE" in size_error, size_error
    assert size_error is not None, "a 6 MiB message was accepted (limits are misconfigured)"
    case_record.actual(exact_limit={"bytes": limit, "offset": accepted.offset}, limit_plus_1=str(too_large.value),
                       raw_4_5_mib_offset=headroom.offset, raw_6_mib_error=str(size_error))


@pytest.mark.it_case("IT13")
def test_it13_collector_payload_boundary(harness: Any, stack_ctx: Any, case_record: Any) -> None:
    from transitpulse.contracts.envelope import utcnow
    from transitpulse.contracts.topics import DLQ, TOPIC_BY_FEED
    from transitpulse.verify.fixture import EnvelopeFactory

    run = create_fault_run(harness, stack_ctx, "IT13")
    run_id = run.run_id
    case_record.run_id = run_id
    limit = int(harness.cfg.kafka.max_snapshot_bytes)
    topic = TOPIC_BY_FEED[VP]
    factory = EnvelopeFactory(run, component="collector")
    http = {"status": 200, "etag": None, "last_modified": None, "content_type": "application/json"}
    harness.start_processor(run_id)
    with FakeMbtaSource() as fake:
        url = fake.url(VP)
        t = now_epoch()
        fake.set("trip_updates", FakeResponse.json(empty_doc(t - 1)))
        fake.set("alerts", FakeResponse.json(empty_doc(t - 1)))
        near_body, near_env = sized_body(factory, harness.paths, run, limit - 1024, header_ts=t - 1, fetched=utcnow(),
                                         source_url=url, http=http)
        p1 = fake.set(VP, FakeResponse.raw(near_body))
        start_collector(harness, run, fake)
        try:
            fake.wait_for(VP, served="body", phase=p1, timeout=90)
            near_sid = str(near_env["snapshot_id"])
            harness.wait_pointer(run_id, VP, near_sid, timeout=420)
            man = harness.manifest(run_id, VP, near_sid) or {}
            assert man.get("status") == "READY", f"near-limit generation is {man.get('status')}"
            part = harness.partitions(topic)[0]
            high_after_near = harness.watermarks(topic, part)[1]
            dlq_start = harness.high_watermarks([DLQ])

            t2 = now_epoch()
            over_body, _ = sized_body(factory, harness.paths, run, limit + 256, header_ts=t2 - 1, fetched=utcnow(),
                                      source_url=url, http=http)
            over_sha = hashlib.sha256(over_body).hexdigest()
            p2 = fake.set(VP, FakeResponse.raw(over_body))
            fake.wait_for(VP, served="body", phase=p2, timeout=90)
            rows = wait_for_outbox(
                harness.paths, run_id,
                lambda rs: any(r.get("state") == "rejected" and r.get("reason") == "PAYLOAD_TOO_LARGE"
                               and r.get("raw_sha256") == over_sha for r in rs),
                "outbox row rejected with PAYLOAD_TOO_LARGE", feed=VP)
            assert over_sha in raw_capture_shas(harness.paths, run_id, VP), "oversize raw capture was not kept"

            def dlq_ref() -> tuple[bool, Any]:
                recs = [d for d in dlq_records(harness, run_id, dlq_start) if d.get("reason_code") == "PAYLOAD_TOO_LARGE"]
                return any(d.get("raw_sha256") == over_sha for d in recs), [d.get("raw_sha256") for d in recs]

            wait_until(dlq_ref, 120, "DLQ record PAYLOAD_TOO_LARGE referencing the raw capture", interval=2.0)
            high_now = harness.watermarks(topic, part)[1]
            leaked = [m["offset"] for m in harness.run_messages(run_id, VP, high_after_near, high_now, part)
                      if (m.get("json") or {}).get("raw_sha256") == over_sha]
            assert not leaked, f"the oversize capture reached {topic} at offsets {leaked}"
            assert high_now == high_after_near, (
                f"{topic} end offset changed ({high_after_near} -> {high_now}) although the source served 304 after "
                "the near-limit capture and the oversize capture was rejected")
            assert (harness.pointer(run_id, VP) or {}).get("snapshot_id") == near_sid
            case_record.actual(
                near_limit={"envelope_bytes": limit - 1024, "snapshot_id": near_sid, "status": man.get("status")},
                oversize={"target_envelope_bytes": limit + 256, "raw_sha256": over_sha, "topic_end_offset": high_now,
                          "outbox": [(r.get("state"), r.get("reason")) for r in rows if r.get("raw_sha256") == over_sha]})
        finally:
            harness.stop_component("collector")
