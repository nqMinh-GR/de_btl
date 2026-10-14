"""IT15: a generation past its ``expires_at`` is ``503 generation_expired``, distinct from a valid empty snapshot.

The run pins ``current_generation_ttl_seconds=30`` (and ``min_publish_ttl_seconds=5`` so that a 30 s generation can
still be published) in its policy at creation; the processor gets the same values through ``TP__STORAGE__*``.
A VP generation is published at t0; a valid EMPTY alerts generation is published ~25 s later; once the VP
generation's ``expires_at`` has passed (wall clock), ``/vehicles`` must be 503 ``generation_expired`` while
``/alerts`` is 200 with ``[]`` and state ``empty``; run metadata and pointers (no TTL) stay intact.
"""

from __future__ import annotations

import json
import time
from datetime import timedelta
from typing import Any

import pytest

from integration.harness import FixtureRun, api_json, wait_until

pytestmark = [pytest.mark.integration, pytest.mark.slow]

VP, AL = "vehicle_positions", "alerts"
TTL = 30
OVERRIDES = {"storage": {"current_generation_ttl_seconds": TTL, "min_publish_ttl_seconds": 5}}
PROCESSOR_ENV = {"TP__STORAGE__CURRENT_GENERATION_TTL_SECONDS": str(TTL), "TP__STORAGE__MIN_PUBLISH_TTL_SECONDS": "5"}


@pytest.mark.it_case("IT15")
def test_it15_generation_ttl_expiry(harness: Any, stack_ctx: Any, case_record: Any) -> None:
    fr = FixtureRun.prepare(harness, stack_ctx, "golden", overrides=OVERRIDES, extra_env=PROCESSOR_ENV)
    run = fr.run
    run_id = run.run_id
    case_record.run_id = run_id
    assert run.policy.current_generation_ttl_seconds == TTL, "TTL override was not pinned in the run policy"
    first = fr.step_names[0]
    vp_item = fr.first_capture(first, VP)
    assert vp_item is not None
    label, d = fr.apply_item(first, vp_item) or (None, None)
    vp_sid = fr.labels[str(vp_item["label"])]
    ptr_vp = harness.wait_pointer(run_id, VP, vp_sid, timeout=300)
    vp_expires = ptr_vp["expires_at"]
    vp_published = ptr_vp.get("view_published_at")
    man = harness.manifest(run_id, VP, vp_sid) or {}
    ttl_observed = (man["expires_at"] - man["first_materialized_at"]).total_seconds() if man.get(
        "first_materialized_at") else None
    assert ttl_observed is not None and abs(ttl_observed - TTL) <= 1.0, (
        f"generation TTL is {ttl_observed}s, expected {TTL}s (policy pinned at run creation)")

    # valid empty alerts snapshot about 25 s after t0 (it must outlive the VP generation at check time)
    wait_until(lambda: (time.time() >= vp_expires.timestamp() - 8, None), 60, "t0 + ~22 s")
    clock = harness.current_clock(run_id) or run.initial_clock
    fetched = clock + timedelta(seconds=30)
    header_ts = int(fetched.timestamp()) - 5
    body = json.dumps({"header": {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET",
                                  "timestamp": header_ts}, "entity": []}, separators=(",", ":")).encode()
    env = fr.factory.archive_and_build(harness.paths, feed_type=AL, body=body, fetched_at=fetched,
                                       source_url="synthetic://IT15/alerts")
    harness.publish_clock(run, fetched)
    harness.publish_envelope(env)
    ptr_al = harness.wait_pointer(run_id, AL, str(env["snapshot_id"]), timeout=120)
    al_expires = ptr_al["expires_at"]

    wait_until(lambda: (time.time() >= vp_expires.timestamp() + 2, None), 60, "VP generation expired")
    assert al_expires.timestamp() > time.time() + 1, (
        f"the empty alerts generation expired too early for the comparison (expires {al_expires})")
    st_v, body_v = api_json(harness, "vehicles", run_id=run_id)
    st_a, body_a = api_json(harness, "alerts", run_id=run_id)
    code_v = (body_v.get("error") or {}).get("code")
    assert st_v == 503 and code_v == "generation_expired", f"/vehicles after expiry: {st_v} {code_v}"
    alerts = (body_a.get("data") or {}).get("alerts")
    state_a = (((body_a.get("meta") or {}).get("feeds") or {}).get(AL) or {}).get("state")
    assert st_a == 200 and alerts == [] and state_a == "empty", f"/alerts: {st_a} {alerts} state={state_a}"
    row = harness.run_row(run_id) or {}
    assert row.get("run_id") == run_id and row.get("policy_json"), "run_metadata must survive generation expiry"
    assert (harness.pointer(run_id, VP) or {}).get("snapshot_id") == vp_sid, "pointer rows have no TTL"
    case_record.actual(vp={"snapshot_id": vp_sid, "view_published_at": vp_published, "expires_at": vp_expires,
                           "ttl_seconds": ttl_observed, "api": [st_v, code_v]},
                       alerts={"snapshot_id": env["snapshot_id"], "expires_at": al_expires,
                               "api": [st_a, alerts, state_a]},
                       label=label, delivery_offset=getattr(d, "offset", None))
