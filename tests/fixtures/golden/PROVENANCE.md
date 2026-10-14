# Golden fixture `golden-v1` — provenance

**Synthetic, hand-written test data. NOT MBTA data.** No byte of this directory was downloaded from, generated
from, or copied out of an MBTA feed. Agency `SYN`, routes `R_A`/`R_B`/`R_X`, stops `S1..S9`, trips `T1..T7`/`TX`,
vehicles `V1..V8` and alerts `A1..A8` are test identifiers, not verified MBTA identifiers. The JSON documents only
imitate the *shape* of the MBTA GTFS-Realtime JSON feeds (header `gtfs_realtime_version "2.0"`,
`incrementality "FULL_DATASET"`, integer epoch-second `timestamp`, `entity[]`, string enums such as `SCHEDULED`,
`CANCELED`, `ADDED`, `IN_TRANSIT_TO`, integer `direction_id`).

## 1. How the expected values were produced

* Every value in `expected.json` was derived **by hand** from design C (§2 GTFS time, §3 calendar / trip instance /
  stop join, §4 uncertainty and delay, §5 target KPI, §6 freshness, §7 alerts, §8 fixture) and from the spec
  (md §11 and §17.1, PDF pp. 17-18 and 26). Each number was computed twice, independently, before writing.
  Nothing was obtained by running code (the build machine executes nothing, decision D-ENV-02).
* The values were **never** taken from the implementation. `tests/oracle/` is a separate stdlib-only
  re-implementation (it may not import `transitpulse` or `pyspark`; `tests/unit/test_oracle_isolation.py`).
  `tests/unit/test_oracle_golden.py` asserts `oracle(fixture) == expected.json`; pipeline tests (Spark, API,
  Cassandra, IT01-IT09, the Colab golden smoke) assert `pipeline == expected.json`.
* `inputs_sha256` is `null`: SHA-256 digests cannot be computed by hand. The materializer
  (`synth-generate --kind golden`) records them in `labels.json` / the recording manifest.
* Run-specific ids are replaced by fixture **labels** (`VP1`, `TU1`, `ALERTS2`, ...). Instants are written as
  `YYYY-MM-DDTHH:MM:SSZ` (second precision); consumers compare parsed instants.

## 2. Epoch table

Reference instant `E0 = 1791201600 = 2026-10-05T12:00:00Z`. Check: 1970-01-01..2026-01-01 is 56 years with 14
leap days (1972..2024) = 20 454 days; Jan-Sep 2026 = 273 days; Oct 5 = day 20 731;
20 731 x 86 400 = 1 791 158 400 = 2026-10-05T00:00:00Z; + 43 200 = 1 791 201 600. 20 731 mod 7 = 4 and
1970-01-01 was a Thursday, so 2026-10-05 is a **Monday** (WKDY active). Local time is EDT (UTC-4) on that day.

| epoch | ISO (UTC) | offset from E0 | use |
|---|---|---|---|
| 1791172800 | 2026-10-05T04:00:00Z | -28800 | service anchor of 2026-10-05 (local noon EDT 16:00Z - 12 h) |
| 1791190800 | 2026-10-05T09:00:00Z | -10800 | A4 active_period start |
| 1791197999 | 2026-10-05T10:59:59Z | -3601 | alert-state vector (one second before A1 starts) |
| 1791198000 | 2026-10-05T11:00:00Z | -3600 | A1/A5 start, A4 end |
| 1791201420 | 2026-10-05T11:57:00Z | -180 | V2 observation (VP1) |
| 1791201480 | 2026-10-05T11:58:00Z | -120 | T2 seq1 S4 scheduled departure (07:58 EDT) |
| 1791201509 | 2026-10-05T11:58:29Z | -91 | freshness vector, age 121 s at 12:00:30Z |
| 1791201510 | 2026-10-05T11:58:30Z | -90 | freshness vector, age 120 s at 12:00:30Z |
| 1791201600 | 2026-10-05T12:00:00Z | 0 | E0; T2 seq1 predicted departure (TU1, already past at S1) |
| 1791201610 | 2026-10-05T12:00:10Z | +10 | V1 observation (VP1, IT06); V4/V5/no-id timestamps (IT06) |
| 1791201620 | 2026-10-05T12:00:20Z | +20 | header of VP1, TU1, ALERTS1, IT07_TU, IT08_TU |
| 1791201630 | 2026-10-05T12:00:30Z | +30 | clock of golden S1 and of IT06/IT07/IT08/IT09 S1; header of IT06_VP |
| 1791201633 | 2026-10-05T12:00:33Z | +33 | V7 observation (+3 s, CLOCK_SKEW) |
| 1791201635 | 2026-10-05T12:00:35Z | +35 | freshness vector (+5 s, still fresh) |
| 1791201636 | 2026-10-05T12:00:36Z | +36 | freshness vector (+6 s, FUTURE_TIMESTAMP) |
| 1791201640 | 2026-10-05T12:00:40Z | +40 | V6 observation (+10 s, FUTURE_TIMESTAMP) |
| 1791201655 | 2026-10-05T12:00:55Z | +55 | V1 observation (VP2) |
| 1791201660 | 2026-10-05T12:01:00Z | +60 | header of VP2 and ALERTS2; clock of golden S2-S4 and IT09 S2 |
| 1791201720 | 2026-10-05T12:02:00Z | +120 | header of ALERTS3; clock of IT09 S3 |
| 1791201780 | 2026-10-05T12:03:00Z | +180 | T6 seq2 S6 scheduled (08:03 EDT) |
| 1791201790 | 2026-10-05T12:03:10Z | +190 | clock of golden S5 |
| 1791201840 | 2026-10-05T12:04:00Z | +240 | ADDED_1 S5 predicted arrival (IT08) |
| 1791201900 | 2026-10-05T12:05:00Z | +300 | T1/T2 S3 scheduled (08:05 EDT); T5 S6 scheduled |
| 1791201960 | 2026-10-05T12:06:00Z | +360 | T2 seq2 predicted (TU1, IT08); T7 seq2 predicted (IT08); T6 S7 scheduled |
| 1791202020 | 2026-10-05T12:07:00Z | +420 | T6 S7 predicted (IT07); T7 S6 scheduled |
| 1791202080 | 2026-10-05T12:08:00Z | +480 | T2 seq3 S2 scheduled; T6 seq2 predicted (IT08) |
| 1791202140 | 2026-10-05T12:09:00Z | +540 | T2 seq3 predicted (TU1); T5 S6 predicted (IT07, IT08) |
| 1791202200 | 2026-10-05T12:10:00Z | +600 | T1 seq4 S4 scheduled; T6 S5 predicted (IT07) |
| 1791202260 | 2026-10-05T12:11:00Z | +660 | T1 seq3 predicted (TU1, IT08) |
| 1791202320 | 2026-10-05T12:12:00Z | +720 | T2 seq4 S1 scheduled |
| 1791202380 | 2026-10-05T12:13:00Z | +780 | T2 seq4 predicted (TU1) |
| 1791202560 | 2026-10-05T12:16:00Z | +960 | T1 seq4 predicted (TU1) |
| 1791205200 | 2026-10-05T13:00:00Z | +3600 | A1 active_period end |
| 1791212400 | 2026-10-05T15:00:00Z | +10800 | A2 start (future) |
| 1791219600 | 2026-10-05T17:00:00Z | +18000 | A2 end |
| 1791244200 | 2026-10-05T23:50:00Z | +42600 | T4 seq1 predicted departure (IT07) = 19:50 EDT on 2026-10-05 |
| 1791255600 | 2026-10-06T03:00:00Z | +54000 | resolution vector clock (23:00 EDT on 2026-10-05) |
| 1791259200 | 2026-10-06T04:00:00Z | +57600 | 2026-10-05 + 24:00:00 (T4 seq3) |
| 1791263400 | 2026-10-06T05:10:00Z | +61800 | 2026-10-05 + 25:10:00 (T4 seq4) |
| 1791201610000 | 2026-10-05T12:00:10Z | milliseconds | V8 timestamp in **milliseconds** -> INVALID_TIMESTAMP_UNIT (> 1e11) |

`tests/unit/test_golden_fixture_consistency.py` re-checks every row of this table, checks that it equals
`expected.json["epochs"]`, that every integer epoch used in `rt/*.json` is listed, and that every UTC instant of
2026-10-05/06 written in `expected.json` or `timeline.json` is listed.

## 3. Static GTFS (`static/*.txt`) — design C §8 verbatim

* agency `SYN` (America/New_York); routes R_A, R_B (route_type 3) and R_X (route_type 2, out of scope);
  stops S1-S7, S9 (no parent stations); services WKDY (Mon-Fri) and DAILY, 2026-09-01..2026-12-31;
  calendar_dates WKDY 2026-10-12 removed (2), WKDY 2026-10-10 (Saturday) added (1).
* trips T1/T3/T4 (R_A dir 0), T2 (R_A dir 1), T5/T6/T7 (R_B dir 0), TX (R_X). T4 uses DAILY and runs past midnight
  (`24:00:00`, `25:10:00`); T6 is a loop that visits S5 at sequence 1 and 4. Shape points are the stop coordinates
  in trip order.
* Scope `[R_A, R_B]`, row counts source -> scoped: agency 1->1, routes 3->2, trips 8->7, stop_times 28->26
  (T1 4 + T2 4 + T3 4 + T4 4 + T5 3 + T6 4 + T7 3; TX's 2 dropped), stops 8->7 (S9 only used by TX), shape points
  17->15 (SH_X's 2 dropped), shape ids 5->4, calendar 2->2, calendar_dates 2->2, feed_info 1->1.
* Service dates: DAILY = 30 + 31 + 30 + 31 = **122**. WKDY: 2026-09-01 is a Tuesday (34 days before Monday
  2026-10-05); 122 days = 17 weeks + 3 days -> 85 + 3 (Tue, Wed, Thu) = 88 weekdays; minus Monday 10-12, plus
  Saturday 10-10 = **88**. Total 210. Range 2026-09-01..2026-12-31.
* Validation: status PASSED, infos only: REPEATED_STOP_IN_TRIP [T6], SERVICE_TIME_GE_24H [T4],
  OUT_OF_SCOPE_FILTERED [R_X].
* First departure / last arrival seconds: T1 28500/29400, T2 28680/29520, T3 29400/30300, T4 71400/90600,
  T5 28800/29400, T6 28800/29340, T7 28920/29520.
* DST vectors (design C §2) recomputed: anchor = UTC(local noon) - 12 h. 2026-03-08 (spring forward 02:00 EST =
  07:00Z): noon is EDT -> anchor 04:00Z, so 00:00:00 -> 04:00Z (23:00 EST Mar 7), 01:30 -> 05:30Z, 08:05 -> 12:05Z
  (local-midnight method would give 05:00Z/06:30Z/13:05Z). 2026-11-01 (fall back 02:00 EDT = 06:00Z): noon is EST ->
  anchor 05:00Z, 00:00 -> 05:00Z (01:00 EDT, first occurrence), 08:05 -> 13:05Z. 2026-10-31 25:10 -> 11-01 05:10Z
  (01:10 EDT); 2026-03-07 25:10 -> 03-08 06:10Z (01:10 EST, before the 07:00Z switch).

## 4. Golden run (spec md §17.1, PDF p18/p26; design C §8)

Policy: stale 120 s (VP), 90 s (TU), 300 s (alerts); future tolerance 5 s; late threshold 300 s; inference window
43 200 s; `mbta_bus_uncertainty_v1`; `first_future_resolved_stop_v1`.

**S1 — clock 12:00:30Z; VP1, TU1, ALERTS1 (headers 12:00:20Z, age 10 s, none stale).**
* V1 obs 12:00:10Z -> age 30 - 10 = 20 s fresh; V2 obs 11:57:00Z -> 1630 - 1420 = 210 s stale; V3 no timestamp ->
  unknown (state sample, event time = header 12:00:20Z, MISSING_OBSERVATION_TIMESTAMP). R_A fresh 1 / stale 1 /
  unknown 1 / invalid 0 / membership 3; R_B all 0.
* TU1 (start_date 20261005 is a Monday -> WKDY active; start_time equals first departure -> resolved/descriptor).
  Anchor 2026-10-05 = 04:00Z. T1: seq3 S3 sched 08:05 = 12:05Z, predicted 12:11Z -> +360 (arr and dep);
  seq4 S4 sched 12:10Z, predicted 12:16Z -> +360. T2: seq1 S4 dep sched 11:58Z, predicted 12:00:00Z -> +120 (past);
  seq2 S3 12:05Z -> 12:06Z +60 (arr, dep); seq3 S2 12:08Z -> 12:09Z +60 (arr, dep); seq4 S1 12:12Z -> 12:13Z +60.
  Nine predictions. Targets (first future resolved stop, arrival preferred): T1 seq3 (+360, late),
  T2 seq2 (+60; seq1 at 12:00:00Z is not after 12:00:30Z). Eligible 2, late 1, rate 1/2 = 0.5,
  mean (360 + 60)/2 = 210.0, unknown_uncertainty 2 (no uncertainty field). R_B: 0 trips, rate/mean null.
* ALERTS1 at 12:00:30Z: A1 {SYN, R_A, 3} 11:00-13:00 -> R_A ACTIVE; A2 {R_B} 15:00-17:00 -> R_B UPCOMING;
  A3 {S3} no period -> R_A ALWAYS (S3 is served only by R_A); A4 {SYN} 09:00-11:00 -> R_A, R_B EXPIRED;
  A5 {route_type 3} from 11:00 -> R_A, R_B ACTIVE; A6 {R_A AND S5} -> NO_SCOPE_MATCH (S5 is an R_B stop);
  A7 {S_UNKNOWN} -> UNRESOLVED_ALERT_SELECTOR; A8 {trip T5} OR {R_A, direction 1} -> R_B (via T5) and R_A
  (R_A has direction 1 = T2), ALWAYS.

**S2 — clock 12:01:00Z; VP2 (header 12:01:00Z).** Pointer VP2. V1 obs 12:00:55Z -> age 5 fresh; V3 unknown;
V2 absent -> membership 2. History rows: V1 {12:00:10Z, 12:00:55Z} = 2, V2 {11:57:00Z} = 1, V3 two state samples
keyed by headers {12:00:20Z, 12:01:00Z} = 2. TU/alerts ages 40 s.

**S3 — VP2 republished.** Same snapshot -> duplicate delivery (cumulative 1), no rebuild, pointer unchanged,
counts and history unchanged.

**S4 — VP1 republished (late).** Source order (header 12:00:20Z) < pointer VP2 (12:01:00Z): duplicate delivery
(cumulative 2) and late delivery (LATE_SNAPSHOT_NOT_PUBLISHED, labels [VP1]); pointer stays VP2, V1 stays at
(42.3519, -71.0581), clock stays 12:01:00Z, history unchanged.

**S5 — tick 12:03:10Z only.** V1 age 1790 - 1655 = 135 > 120 -> stale: fresh 0, stale 1, unknown 1, membership 2.
The VP2 generation as stored by Spark (metrics_as_of 12:01:00Z) still says fresh 1. VP header age 130 > 120 ->
VP feed stale. TU age 1790 - 1620 = 170 > 90 -> `prediction_metrics.stale = true` (KPI values unchanged, as of
12:00:30Z). Alerts age 170 <= 300 -> not stale; alert states unchanged at 12:03:10Z.

## 5. Scenario runs

**IT06 (clock 12:00:30Z, header 12:00:30Z, all entities route R_A).** V1 obs 12:00:10Z fresh, age 20; V3 unknown; V4 lat 91
INVALID_COORDINATES; V5 (0,0) INVALID_COORDINATES; V6 +10 s FUTURE_TIMESTAMP; V7 +3 s fresh, CLOCK_SKEW, age 0;
V8 1791201610000 (ms) INVALID_TIMESTAMP_UNIT; entity without `vehicle.vehicle.id` MISSING_VEHICLE_ID (rejected ->
DLQ, `is_partial = true`). fresh 2, stale 0, unknown 1, invalid 5 (entities, incl. the one without ID),
membership 7 (distinct IDs incl. invalid ones with an ID) — D-BIZ-07 / D-X-03. Quality: INVALID_COORDINATES 2,
FUTURE_TIMESTAMP 1, INVALID_TIMESTAMP_UNIT 1, MISSING_VEHICLE_ID 1, MISSING_OBSERVATION_TIMESTAMP 1, CLOCK_SKEW 1.
S2 publishes a malformed envelope (truncated JSON) -> DLQ INVALID_JSON; the view is unchanged.

**IT07 (clock 12:00:30Z).** T4 has no start_date: local date L = 2026-10-05. DAILY is active on Oct 4/5/6.
Window [anchor + 71400 - 43200, anchor + 90600 + 43200]: Oct 4 = [Oct 4 11:50Z, Oct 5 17:10Z] contains the clock;
Oct 5 = [Oct 5 11:50Z, Oct 6 17:10Z] contains it; Oct 6 starts Oct 6 11:50Z -> 2 candidates -> ambiguous, delay
null, AMBIGUOUS_SERVICE_DATE. (Vector: at 2026-10-06T03:00Z only Oct 5 qualifies -> inferred 2026-10-05.)
T6 STUs without stop_sequence: S5 appears at seq 1 and 4 -> UNRESOLVED_STOP_SEQUENCE (future, so the trip is
excluded); S7 -> seq3, sched 12:06Z, predicted 12:07Z, display delay 60. T5 S6 unique -> seq2, sched 12:05Z,
predicted 12:09Z -> +240 eligible. R_A: eligible 0, rate/mean null, {AMBIGUOUS_SERVICE_DATE: 1}. R_B: eligible 1,
late 0, rate 0.0, mean 240.0, {UNRESOLVED_STOP_SEQUENCE: 1}, unknown_uncertainty 1.

**IT08 (clock 12:00:30Z, start_date 20261005).** T1 u=301 degraded, display delay 360, DEGRADED_PREDICTION;
T2 u=300 schedule_only, delay null, SCHEDULE_ONLY_PREDICTION; T3 CANCELED (no STUs) CANCELED_TRIP; T5 u=360
invalid, delay null, INVALID_PREDICTION; T6 seq2 S6 sched 08:03 = 12:03Z, predicted 12:08Z -> +300 = late
(boundary, >= 300), uncertainty absent; T7 seq2 S6 sched 08:07 = 12:07Z, predicted 12:06Z -> -60 (kept negative),
u=60 realtime; ADDED_1 (R_B) not_applicable, ADDED_TRIP. R_A: 3 instances, eligible 0, excluded
{CANCELED_TRIP 1, DEGRADED_PREDICTION 1, SCHEDULE_ONLY_PREDICTION 1}. R_B: 4 instances, eligible 2, late 1,
rate 0.5, mean (300 - 60)/2 = 120.0, excluded {ADDED_TRIP 1, INVALID_PREDICTION 1}, unknown_uncertainty 1.

**IT09 (alerts only).** S1 ALERTS1 at 12:00:30Z as in golden S1. S2 ALERTS2 (full, header 12:01:00Z, ALERTS1
without A3): A3 closed, ABSENT_FROM_FULL_SNAPSHOT, closed_at 12:01:00Z. S3 ALERTS3 (header 12:02:00Z): A1, A2
valid, A5 malformed (`active_period.start = "abc"` fails the LONG schema -> INVALID_ENTITY, DLQ), A4/A8 omitted ->
`is_partial = true`; previous route rows absent or rejected (A4, A5, A8) are carried UNCONFIRMED_INHERITED with
`inherited_from = ALERTS2` (where last confirmed), nothing is closed, A3 stays closed.
R_A = {A1, A4i, A5i, A8i}, R_B = {A2, A4i, A5i, A8i}.

## 6. Deviations and choices (relative to design C, other designs and the spec)

The spec and design C agree on every golden number (V1 20 s, V2 210 s, V3 unknown, T1 +360, T2 +60, rate 0.5,
mean 210 s; S2 membership 2; S4 no rollback; S5 V1 135 s stale and predictions stale). Choices made here:

1. **IT06 header = 12:00:30Z** (design C leaves it open). Design B §5 tests FUTURE_TIMESTAMP against the feed
   header, design C §6 / `contracts.policy.vehicle_freshness` against the clock; with header == clock both give the
   same result for V6 (+10 s) and V7 (+3 s).
2. **IT06 malformed message** is a `malformed_envelope` timeline item (interfaces §4 kind, `value_text` inline,
   label `IT06_BAD`) in its own step S2, not a separate file.
3. **IT06 counts** follow D-BIZ-07 / D-X-03: invalid counts entities (5, including the ID-less entity),
   membership counts IDs (7).
4. **IT09 A4** (EXPIRED) is inherited in the partial ALERTS3, as design C §7/§8 and D-BIZ-06 state ("partial keeps
   absent alerts"). Design B §5 would *not* carry parent alerts whose `end < metrics_as_of`; the lead must pick one
   rule for WP5 (open question).
5. **IT09** follows design C, not the different variant in design E §7.4.
6. **Golden timeline** is design C's S1-S5. Design E's IT02 extra snapshot S2' (identical V1 observation ->
   DUPLICATE_OBSERVATION) and IT03's unseen S1b are not part of this fixture; IT02/IT03 assert with S3/S4.
7. **S4** counts both as a duplicate delivery and as a late delivery (design B `process_snapshot`).
8. Predictions with SCHEDULE_ONLY/INVALID uncertainty keep `scheduled_at` (the schedule is resolved) but have a
   null delay (design C §4). For excluded trips the target fields are still reported (design C §5 computes the
   target before the exclusion reason); `late` is null unless the trip is eligible.
9. History of invalid vehicles (V4, V5, V6, V8) is not asserted (policy not specified).
10. Alert `first_seen` is carried from the parent route row; inherited rows keep it.
11. `inputs_sha256` is null (not computable by hand).
