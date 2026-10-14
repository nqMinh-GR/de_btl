"""pipeline_status lineage counters per golden step (lead decision: S3 = duplicate, S4 = duplicate + late)."""

from __future__ import annotations

from transitpulse.processing.batch import (
    RESULT_LATE,
    RESULT_PUBLISHED,
    RESULT_REUSED,
    SnapshotResult,
    lineage_totals,
)


def res(result: str, *, redelivery: bool, in_batch: int = 0, status: str = "READY",
        outcome: str | None = "PUBLISHED") -> SnapshotResult:
    return SnapshotResult("vehicle_positions", "sn_x", "gn_x", "reused" if redelivery else "built", result,
                          redelivery, in_batch + int(redelivery), True,
                          {"status": status, "publish_outcome": outcome}, in_batch_duplicates=in_batch)


def test_s1_new_generation():
    assert lineage_totals([res(RESULT_PUBLISHED, redelivery=False)]) == {
        "snapshots_built_total": 1, "snapshots_published_total": 1, "late_deliveries_total": 0,
        "duplicate_deliveries_total": 0}


def test_s3_redelivery_of_current_generation_is_only_a_duplicate():
    assert lineage_totals([res(RESULT_REUSED, redelivery=True)]) == {
        "snapshots_built_total": 0, "snapshots_published_total": 0, "late_deliveries_total": 0,
        "duplicate_deliveries_total": 1}


def test_s4_redelivery_of_older_generation_is_duplicate_and_late():
    assert lineage_totals([res(RESULT_LATE, redelivery=True)]) == {
        "snapshots_built_total": 0, "snapshots_published_total": 0, "late_deliveries_total": 1,
        "duplicate_deliveries_total": 1}


def test_unseen_late_snapshot_is_built_but_not_published():
    r = res(RESULT_LATE, redelivery=False, outcome="LATE_NOT_PUBLISHED")
    assert lineage_totals([r]) == {"snapshots_built_total": 1, "snapshots_published_total": 0,
                                   "late_deliveries_total": 1, "duplicate_deliveries_total": 0}


def test_in_batch_copies_count_per_delivery():
    assert lineage_totals([res(RESULT_PUBLISHED, redelivery=False, in_batch=2)])["duplicate_deliveries_total"] == 2
    late = lineage_totals([res(RESULT_LATE, redelivery=True, in_batch=1)])
    assert late["late_deliveries_total"] == 2 and late["duplicate_deliveries_total"] == 2
