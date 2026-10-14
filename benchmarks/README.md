# Benchmarks

Harness for the load, latency and recovery measurements of spec md §18 / PDF p27 (design E §8). It runs against
the real native stack (Kafka, Cassandra, Spark processor started by the supervisor) and **never fills in a number by
hand**: every value is computed from evidence collected during the run, otherwise it is `null` with a reason, and
every scenario × repetition row has `status` `MEASURED`, `PARTIAL` (window truncated) or `NOT_RUN` (with the reason).

Current state of this repository: **no benchmark has been run** (all results are `NOT_RUN` until executed on the
target machine). Targets such as "p95 visibility latency < 10 s" are reference targets, not results.

## Run

```bash
# replay of a real live recording (record one first: make start-live / transitpulse record)
make benchmark SCENARIO=replay_10x RECORDING=/path/to/recording_manifest.json
# synthetic stress (generated, labelled synthetic)
make benchmark SCENARIO=stress_100,stress_500
# recovery
make benchmark SCENARIO=recovery_partial_write,recovery_after_cas,recovery_cassandra_down,recovery_collector_outbox

# direct form (one JSON document on stdout, progress on stderr)
.venv/bin/python benchmarks/run_benchmark.py --scenario replay_10x --recording-manifest P \
    --repetitions 3 --warmup 60 --measure 120 --workdir "$TP_WORKDIR"
```

The benchmark refuses to run (rows `NOT_RUN`) while a demo session is active; stop it first with
`scripts/demo.py stop`. It never stops a demo session or any process it did not start.

## Scenarios (`scenarios.yaml`)

| Scenario | Input | What is measured |
|---|---|---|
| `replay_1x`, `replay_10x`, `replay_50x` | a real recording (`--recording-manifest`) replayed by the replay publisher | throughput, latency, backlog, resources at 1x / 10x / 50x |
| `stress_100`, `stress_500`, `stress_1000` | `replay synth-generate --kind stress` (5 routes, replicated static trips) | the same at 100 / 500 / 1000 normalized candidate records per second |
| `recovery_partial_write`, `recovery_after_cas` | golden fixture + one-shot fault arm file | processor exit 86, restart on the same checkpoint, time to the correct VP2 generation |
| `recovery_cassandra_down` | golden fixture, owned Cassandra stopped | batch failure, restart, time to the correct generation |
| `recovery_collector_outbox` | fake source, collector with an unreachable broker, SIGKILL | outbox resume time, records lost / duplicated |

A *normalized candidate record* is one VehiclePositions entity, one arrival or departure event of a
stop_time_update, or one alert informed_entity selector (`contracts.accounting.count_candidates`). Snapshots,
entities and candidate records are different units and are never summed together.

## Procedure (each repetition uses a NEW run)

1. Create the run, start the processor, wait until `processor_ready.json` is written.
2. Start the samplers: psutil every 2 s over the supervisor pids and their children (CPU %, RSS); Kafka backlog every
   5 s (high watermark minus the latest `query_progress.jsonl` end offset, per topic/partition); disk usage of the
   Kafka, Cassandra, raw, recordings, checkpoint and curated directories at start and end.
3. Start the replay publisher.
4. Measurement window `[t0 + warmup, t0 + warmup + measure)`, truncated at the end of the timeline (`PARTIAL`).
   Example: a 2 h recording at 50x lasts 144 s, so warm-up 60 s and 84 s measured.
5. Send `finish`, drain until the backlog is 0 (bounded by `drain_timeout_seconds`), stop the processor.

## Computations (`compute.py`)

| Metric | Source and method |
|---|---|
| snapshots/s, bytes/s, declared candidates/s | publisher `publish_log.jsonl` (snapshot lines in the window) ÷ window seconds; cross-checked with a Kafka scan of the run's messages |
| normalized candidates/s, unique records/s, duplicate fraction | Δ of `tp_processor_normalized_candidates_total`, `_unique_records_total`, `_duplicate_records_total` (processor textfile at the window edges); fraction = duplicates / (unique + duplicates) with the denominator reported |
| visibility latency p50/p95/p99 | `snapshot_manifest.visibility_latency_ms` (else `first_visible_at − published_at`) of generations first enqueued in the window; late / expired / never-visible generations counted separately |
| source age | `metrics_as_of − observation_time` of vehicles (sampled VP generations); never added to latency |
| batch duration | `query_progress.jsonl` `durationMs` |
| Cassandra write latency | processor JSON log lines with `stage=cassandra_write` and `duration_ms` |
| resources | CPU mean/max %, RSS max per component and total (MiB) |
| Cassandra rows written | table totals of `expected_counts` of generations READY in the window |
| recovery | from the start of recovery actions to the expected generation being current and matching `expected.json`; records lost / duplicated against the golden history |

Percentiles: `statistics.quantiles(method="inclusive")`, p50 = median; p95 is `null` when n < 20 and p99 is `null`
when n < 100 (n, min and max are always reported) — decision D-E25.

## Outputs (`<TP_WORKDIR>/reports/benchmarks/<bench_id>/`)

* `benchmark_results.json`, `benchmark_results.csv` — one row per scenario × repetition;
* `environment.json` — CPU model and count, RAM, free disk, versions, commit, config hash, cache state;
* `resource_samples.csv`, `backlog_samples.csv`;
* `benchmark_report.md` — the protocol actually executed, targets separated from measured values, replay and
  synthetic results in separate sections, limitations;
* `fault_recovery_report.md` — per fault: injection time, expected vs actual, recovery time, log references;
* `evidence/` — per-step check results of the recovery scenarios.

## Limitations

Single broker, single Cassandra node (RF1) and `local[2]` Spark: results describe the machine they ran on. The
processor JVM starts cold for every repetition and a fresh checkpoint reads the topics from `earliest`
(D-PUB-08), so the warm-up absorbs that catch-up. API and UI latency are not part of this protocol.
