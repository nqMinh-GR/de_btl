"""TransitPulse benchmark harness (design E §8; spec md §18; PDF p27).

Usage (one JSON document on stdout, progress on stderr)::

    .venv/bin/python benchmarks/run_benchmark.py --scenario replay_10x --recording-manifest P \
        [--repetitions 3] [--warmup 60] [--measure 120] [--workdir W] [--profile P] [--max-seconds N]
    .venv/bin/python benchmarks/run_benchmark.py --scenario stress_100,stress_500
    .venv/bin/python benchmarks/run_benchmark.py --scenario recovery_partial_write,recovery_cassandra_down

Throughput procedure per repetition (a NEW run each time; NOT_RUN when a demo session is active):
create the run -> start the processor and wait until ready -> start samplers (psutil every 2 s, Kafka backlog every
5 s, disk at start) -> start the replay publisher -> measurement window [t0+warmup, t0+warmup+measure) truncated at the
end of the timeline (PARTIAL) -> ``finish`` -> drain until the backlog is 0 (bounded) -> stop the processor -> compute.
Recovery scenarios inject one fault on owned processes and time the recovery against the golden expectations.

Outputs: ``<W>/reports/benchmarks/<bench_id>/`` (see report.py). No number is ever filled in by hand.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import platform
import shlex
import shutil
import subprocess
import sys
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

import compute
import report
import samplers
from transitpulse.config import load_config, repo_root
from transitpulse.contracts.envelope import iso_utc, parse_iso_utc, utcnow
from transitpulse.contracts.topics import CONTROL, SNAPSHOT_TOPICS
from transitpulse.runtime.paths import WorkPaths

HERE = Path(__file__).resolve().parent
VP = "vehicle_positions"
SESSION_COMPONENTS = ("processor", "publisher", "collector", "api", "dashboard")
RESOURCE_COMPONENTS = ("kafka", "cassandra", "processor", "publisher", "collector")


def log(msg: str) -> None:
    sys.stderr.write(f"[benchmark {datetime.now(UTC).strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()


def load_scenarios(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    if not isinstance(doc, dict) or not isinstance(doc.get("scenarios"), dict):
        raise ValueError(f"{path}: expected 'scenarios' mapping")
    return doc


def _git_commit(repo: Path) -> str | None:
    try:
        p = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True, text=True, timeout=10,
                           check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return p.stdout.strip() if p.returncode == 0 else None


def environment(paths: WorkPaths, cfg: Any, runtime: Any | None, profile: str) -> dict[str, Any]:
    cpu_model = None
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace").splitlines():
            if line.lower().startswith("model name"):
                cpu_model = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    ram = None
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                ram = round(int(line.split()[1]) * 1024 / 2**30, 2)
    except (OSError, ValueError):
        pass
    raw = runtime.raw if runtime is not None else {}
    try:
        free = round(shutil.disk_usage(paths.root).free / 2**30, 1)
    except OSError:
        free = None
    return {
        "os": platform.platform(), "cpu": {"model": cpu_model, "count": os.cpu_count()}, "ram_gib": ram,
        "disk_free_gib": free, "profile": profile,
        "python": (raw.get("python") or {}).get("version") or platform.python_version(),
        "java": (raw.get("java") or {}).get("version"), "spark": (raw.get("spark") or {}).get("version"),
        "kafka": (raw.get("kafka") or {}).get("home"), "cassandra": (raw.get("cassandra") or {}).get("home"),
        "commit": raw.get("repo_commit") or _git_commit(repo_root()),
        "config_hash": cfg.config_hash() if cfg is not None else None,
        "cache_state": "processor JVM and Python workers start cold for every repetition; OS page cache and the "
                       "Kafka/Cassandra caches are not reset between repetitions",
    }


def _row(bench_id: str, name: str, scen: dict[str, Any], rep: int, protocol: dict[str, Any]) -> dict[str, Any]:
    return {"bench_id": bench_id, "scenario": name, "kind": scen.get("kind"), "repetition": rep, "run_id": None,
            "source_type": None, "speed": scen.get("speed"), "target_rate": scen.get("target_rate"),
            "warmup_s": protocol["warmup_s"], "measured_s": 0.0, "status": "NOT_RUN", "metrics": {},
            "samples_n": 0, "notes": [], "input": {}}


class Bench:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.repo = repo_root()
        self.paths = WorkPaths.from_workdir(args.workdir)
        self.profile = args.profile
        self.cfg = load_config(self.profile, workdir=self.paths.root)
        self.runtime: Any = None
        self.h: Any = None
        self.ctx: Any = None
        self.api: Any = None
        self.resource_samples: list[dict[str, Any]] = []
        self.backlog_samples: list[dict[str, Any]] = []
        self.session_id = f"bench-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"

    # ------------------------------------------------------------------ setup
    def setup(self) -> str | None:
        """Returns a NOT_RUN reason, or None when the stack is usable."""
        from transitpulse.runtime.manifest import load_runtime, subprocess_env
        from transitpulse.verify.flows import StackContext
        from transitpulse.verify.stack import StackHarness

        try:
            self.runtime = load_runtime(self.paths)
        except (FileNotFoundError, ValueError) as exc:
            return f"runtime not available: {exc}"
        self.h = StackHarness(cfg=self.cfg, paths=self.paths, runtime=self.runtime, profile=self.profile,
                              session_id=self.session_id, repo_dir=self.repo,
                              evidence_dir=self.out_dir / "evidence")
        for comp in SESSION_COMPONENTS:
            entry = self.h.sup.entry(comp)
            if entry and self.h.component_alive(comp):
                return (f"a session component is running ({comp}, session {entry.get('session_id')}); stop the demo "
                        "session first (scripts/demo.py stop) — the benchmark never stops it")
        try:
            self.h.infra("start", timeout=900)
            self.h.infra("init", timeout=600)
        except Exception as exc:  # noqa: BLE001 - reported as NOT_RUN reason
            return f"infra could not be started: {exc}"[:1000]
        self.ctx = StackContext(cfg=self.cfg, paths=self.paths, runtime=self.runtime, profile=self.profile,
                                repo_dir=self.repo, golden_dir=self.repo / "tests" / "fixtures" / "golden",
                                env=subprocess_env(self.runtime))
        return None

    @property
    def out_dir(self) -> Path:
        return self.paths.reports / "benchmarks" / self.args.bench_id

    def ensure_api(self) -> str:
        from transitpulse.verify.stack import ApiServer

        if self.api is None:
            self.api = ApiServer(runtime=self.runtime, paths=self.paths, cfg=self.cfg, profile=self.profile,
                                 repo_dir=self.repo, log_path=self.out_dir / "evidence" / "api.log")
            self.h.api_base = self.api.start()
        return self.api.base_url

    def close(self) -> None:
        if self.h is not None:
            for comp in ("publisher", "collector", "processor"):
                entry = self.h.sup.entry(comp) or {}
                if entry.get("session_id") == self.session_id and self.h.component_alive(comp):
                    try:
                        self.h.stop_component(comp)
                    except Exception as exc:  # noqa: BLE001 - shutdown continues
                        log(f"could not stop {comp}: {exc}")
            self.h.close()
        if self.api is not None:
            self.api.stop()

    # ------------------------------------------------------------------ inputs
    def recording_for(self, name: str, scen: dict[str, Any], row: dict[str, Any]) -> tuple[Any, Any] | None:
        from transitpulse.verify.fixture import load_recording, verify_recording_cli

        kind = scen.get("kind")
        if kind == "replay":
            if not self.args.recording_manifest:
                row["notes"].append("replay scenarios need --recording-manifest (a real live recording); none given")
                return None
            manifest = Path(self.args.recording_manifest).resolve()
            info = verify_recording_cli(core_python=self.runtime.core_python, env=self.ctx.env, repo_dir=self.repo,
                                        workdir=self.paths.root, manifest_path=manifest,
                                        static_zip=Path(self.args.static_zip) if self.args.static_zip else None)
            rec = load_recording(manifest, info)
            zip_path = Path(self.args.static_zip) if self.args.static_zip else (
                self.paths.static_version_dir(str(info["gtfs_version"])) / "MBTA_GTFS.zip")
            rec = dataclasses.replace(rec, static_zip_path=zip_path)
            row["input"] = {"recording_id": rec.recording_id, "manifest_sha256": rec.manifest.get("manifest_sha256"),
                            "gtfs_version": info.get("gtfs_version"), "items": info.get("item_count")}
        else:
            duration = int(self.args.warmup + self.args.measure + 30)
            rec = self.ctx.materialize("stress", seed=int(scen.get("seed", 42)), rate=int(scen["target_rate"]),
                                       duration_seconds=duration)
            row["input"] = {"recording_id": rec.recording_id, "seed": scen.get("seed", 42),
                            "generator_version": rec.manifest.get("generator_version"),
                            "manifest_sha256": rec.manifest.get("manifest_sha256"), "duration_s": duration}
        routes = tuple(self.args.route_ids.split(",")) if self.args.route_ids else rec.route_ids
        static = self.ctx.static_for(rec.static_zip_path, routes)
        row["input"]["route_ids"] = list(routes)
        return rec, static

    # ------------------------------------------------------------------ throughput
    def prom_snapshot(self) -> dict[str, float | None]:
        from transitpulse.verify.progress import prom_sum, read_prom_file

        samples_ = read_prom_file(self.paths.metrics_dir / "processor.prom")
        names = ("tp_processor_normalized_candidates_total", "tp_processor_unique_records_total",
                 "tp_processor_duplicate_records_total", "tp_processor_batch_failures_total")
        return {n: prom_sum(samples_, n) for n in names}

    def run_throughput(self, name: str, scen: dict[str, Any], rep: int, row: dict[str, Any]) -> None:
        from transitpulse.replay.control import read_publisher_state, send_command
        from transitpulse.verify.progress import read_progress

        got = self.recording_for(name, scen, row)
        if got is None:
            return
        rec, static = got
        speed = float(scen.get("speed", 1))
        source_type = "replay" if scen.get("kind") == "replay" else "synthetic"
        run = self.h.create_run(static=static, source_type=source_type, initial_clock=rec.start_clock,
                                scenario_id=f"benchmark:{name}", fixture_kind=rec.fixture_kind,
                                seed=scen.get("seed"), recording_id=rec.recording_id, speed=speed,
                                label=f"{self.args.bench_id}:{name}:r{rep}")
        row.update(run_id=run.run_id, source_type=source_type)
        log(f"{name} r{rep}: run {run.run_id}, starting processor")
        self.h.start_processor(run.run_id)
        disk_start = samplers.disk_usage(self.paths)
        highs_start = self.h.high_watermarks(SNAPSHOT_TOPICS)
        res = samplers.ResourceSampler(self.h.sup, RESOURCE_COMPONENTS, self.args.resource_interval)
        back = samplers.BacklogSampler(self.cfg.kafka.bootstrap_servers, self.paths.query_progress_log(run.run_id),
                                       (*SNAPSHOT_TOPICS, CONTROL), self.args.backlog_interval)
        res.start()
        back.start()
        session = f"{self.session_id}-{name}-r{rep}"
        session_dir = self.paths.session_dir(session)
        session_dir.mkdir(parents=True, exist_ok=True)
        argv = [str(self.runtime.core_python), "-m", "transitpulse.replay", "publish", "--workdir",
                str(self.paths.root), "--run-id", run.run_id, "--manifest", str(rec.manifest_path), "--session-id",
                session, "--speed", str(speed)]
        state_file = session_dir / "publisher_state.json"
        self.h.start_component("publisher", argv, run_id=run.run_id, env=self.h.env, ready=state_file.exists,
                               ready_timeout=180)
        t0 = time.time()
        win_start = t0 + self.args.warmup
        win_end_planned = win_start + self.args.measure
        prom_start: dict[str, float | None] | None = None
        finished_at = None
        log(f"{name} r{rep}: publisher started; warm-up {self.args.warmup}s, window {self.args.measure}s")
        while time.time() < win_end_planned:
            st = read_publisher_state(session_dir) or {}
            if st.get("state") in ("FINISHED", "FAILED"):
                finished_at = time.time()
                if st.get("state") == "FAILED":
                    row["notes"].append(f"publisher FAILED: {st.get('error')}")
                break
            if prom_start is None and time.time() >= win_start:
                prom_start = self.prom_snapshot()
            time.sleep(1.0)
        win_end = min(win_end_planned, finished_at or win_end_planned)
        prom_end = self.prom_snapshot() if prom_start is not None else None
        if self.h.component_alive("publisher"):
            try:
                send_command(session_dir, "finish", timeout=30)
            except Exception as exc:  # noqa: BLE001 - recorded, the publisher is stopped below anyway
                row["notes"].append(f"finish command not acknowledged: {exc}")
            deadline = time.time() + 60
            while time.time() < deadline and (read_publisher_state(session_dir) or {}).get("state") not in (
                    "FINISHED", "FAILED"):
                time.sleep(1.0)
        self.h.stop_component("publisher")
        drained = True
        try:
            self.h.wait_caught_up(run.run_id, timeout=self.args.drain_timeout)
        except Exception as exc:  # noqa: BLE001 - backlog not drained is a reported fact
            drained = False
            row["notes"].append(f"backlog not drained within {self.args.drain_timeout}s: {exc}"[:400])
        final_backlog = back.measure()
        self.h.stop_processor()
        res_samples = res.stop()
        back_samples = back.stop()
        disk_end = samplers.disk_usage(self.paths)
        highs_end = self.h.high_watermarks(SNAPSHOT_TOPICS)
        for s in res_samples:
            self.resource_samples.append({"scenario": name, "repetition": rep, **s})
        for s in back_samples:
            self.backlog_samples.append({"scenario": name, "repetition": rep, **s})

        measured = max(0.0, win_end - win_start)
        finished_early = finished_at is not None and finished_at < win_end_planned
        row["measured_s"] = round(measured, 1)
        row["status"] = compute.status_for(measured, self.args.measure, finished_early)
        if row["status"] == "NOT_RUN":
            row["notes"].append(f"the timeline ended {round((finished_at or t0) - t0, 1)}s after start, before the "
                                f"end of the {self.args.warmup}s warm-up")
        elif row["status"] == "PARTIAL":
            row["notes"].append(f"window truncated at the end of the timeline: measured {measured:.1f}s of "
                                f"{self.args.measure}s")
        manifests = [m for f in ("vehicle_positions", "trip_updates", "alerts") for m in self.h.manifests(run.run_id, f)]
        messages = []
        for (topic, part), high in highs_end.items():
            messages += self.h.run_messages(run.run_id, topic, highs_start.get((topic, part), 0), high, part)
        m: dict[str, Any] = {}
        m.update(compute.publish_log_rates(session_dir / "publish_log.jsonl", win_start, win_end))
        m["kafka_cross_check"] = compute.kafka_scan_rates(messages, win_start, win_end)
        m.update(compute.processor_rates(prom_start, prom_end, measured))
        m["visibility_latency"] = compute.visibility_latency(manifests, win_start, win_end)
        m["source_age"] = self.source_age(run, manifests, win_start, win_end)
        m["batch_duration"] = compute.batch_stats(read_progress(self.paths.query_progress_log(run.run_id)), win_start,
                                                  win_end)
        m["cassandra_write"] = compute.log_stage_durations(self.h.component_log("processor"), "cassandra_write",
                                                           win_start, win_end)
        m["resources"] = compute.resources(res_samples, win_start, win_end)
        m["backlog"] = compute.backlog(back_samples, win_start, win_end)
        m["backlog_after_drain"] = {"drained": drained, "samples": final_backlog}
        m["disk_delta"] = compute.disk_delta(disk_start, disk_end)
        m["cassandra_rows_written"] = compute.rows_written(manifests, win_start, win_end)
        fails = compute.prom_delta(prom_start or {}, prom_end or {}, "tp_processor_batch_failures_total")
        batches = m["batch_duration"].get("batches") or 0
        m["error_rate"] = compute.metric(fails / batches, "failed batch / batch", "processor.prom + query_progress",
                                         "delta(batch_failures_total) / batches in window") if (
            fails is not None and batches) else compute.missing("failed batch / batch", "processor.prom",
                                                                "no batch counter or no batch in the window")
        m["api_latency"] = compute.missing("s", "API", "not part of this protocol (no API load generator)")
        row["metrics"] = m
        row["samples_n"] = len([s for s in res_samples if compute.in_window(s.get("t_wall"), win_start, win_end)])
        row["window"] = {"start": iso_utc(datetime.fromtimestamp(win_start, UTC)),
                         "end": iso_utc(datetime.fromtimestamp(win_end, UTC))}

    def source_age(self, run: Any, manifests: list[dict[str, Any]], start: float, end: float) -> dict[str, Any]:
        vp = []
        for m in manifests:
            published = compute.parse_instant(m.get("published_at"))
            if (m.get("feed_type") == VP and m.get("status") == "READY" and m.get("metrics_as_of") is not None
                    and published is not None and compute.in_window(published.timestamp(), start, end)):
                vp.append(m)
        step = max(1, len(vp) // 60)
        ages: list[float] = []
        for man in vp[::step]:
            as_of = compute.parse_instant(man["metrics_as_of"])
            for route in run.route_ids:
                for r in self.h.vehicle_rows(run.run_id, man["snapshot_id"], route):
                    obs = r.get("observation_time")
                    if obs is not None and as_of is not None:
                        ages.append((as_of - obs).total_seconds())
        out = compute.percentiles(ages, unit="s", source="metrics_as_of - observation_time of vehicles in VP "
                                                         "generations enqueued in the window (sampled <= 60)")
        out["note"] = "source age is NOT pipeline latency and is never added to it"
        return out

    # ------------------------------------------------------------------ recovery
    def run_recovery(self, name: str, scen: dict[str, Any], rep: int, row: dict[str, Any]) -> None:
        fault = str(scen.get("fault"))
        if fault == "collector_outbox":
            self.recovery_outbox(row)
            return
        from transitpulse.verify.flows import FixtureRun

        self.ensure_api()
        fi = fault != "cassandra_down"
        fr = FixtureRun.prepare(self.h, self.ctx, "golden", fault_injection=fi)
        row.update(run_id=fr.run.run_id, source_type="synthetic")
        first = fr.step_names[0]
        out1 = fr.run_step(first)
        if not out1.passed:
            row["notes"].append(f"golden {first} did not match expected.json before the fault; recovery not timed")
            row["recovery"] = {"fault": fault, "failed_checks": _failed(out1)}
            return
        step = next((s for s in fr.step_names[1:] if fr.first_capture(s, VP) is not None), None)
        cap = dict(fr.first_capture(step, VP) or {}) if step else {}
        sid2 = str(fr.envelope_for(str(cap["label"]), cap)["snapshot_id"])
        before = fr.lineage_state()
        injected = utcnow()
        if fi:
            self.h.arm_fault(fr.run.run_id, fault, {"feed_type": VP, "snapshot_id": sid2})
            for item in fr.items(step):
                fr.apply_item(step, item)
        else:
            self.h.stop_cassandra()
            for item in fr.items(step):
                if item.get("kind") in ("tick", "capture"):
                    at = parse_iso_utc(str(item["at"]))
                    cur = self.h.current_clock(fr.run.run_id)
                    if cur is None or at > cur:
                        self.h.publish_clock(fr.run, at, wait_applied=False)
                    if item.get("kind") == "capture":
                        self.h.publish_envelope(fr.envelope_for(str(item["label"]), item))
        code = self.h.wait_processor_exit(900)
        t_recover = time.monotonic()
        recovery_started = utcnow()
        if not fi:
            self.h.start_cassandra()
        self.h.restart_processor(fr.run.run_id, fault_injection=fi)
        self.h.wait_caught_up(fr.run.run_id, timeout=600)
        self.h.wait_pointer(fr.run.run_id, VP, sid2, timeout=300)
        api, cass = fr.check(step)
        lineage = fr.check_lineage(step, before)
        recovery_s = time.monotonic() - t_recover
        exp = (fr.expected_step(step) or {}).get("expect") or {}
        lost, dup = self.history_diff(fr, exp)
        passed = bool(api and api.get("passed")) and bool(cass and cass.get("passed")) and bool(
            lineage and lineage.get("passed"))
        row["status"] = "MEASURED"
        row["recovery"] = {
            "fault": fault, "injected_at": iso_utc(injected), "exit_code": code,
            "recovery_started_at": iso_utc(recovery_started), "recovery_seconds": round(recovery_s, 2),
            "matches_expected": passed, "records_lost": lost, "records_duplicated": dup,
            "failed_checks": [c["name"] for res in (api, cass, lineage) if res for c in res.get("checks", [])
                              if not c.get("passed")][:30],
            "fault_scope": "Cassandra node (owned process) + processor exit" if not fi else
            "processor process, one micro-batch of one run",
            "logs": [str(p) for p in (self.h.component_log("processor"),) if p],
            "definition": "from the start of recovery actions (Cassandra start / processor restart) to the VP2 "
                          "generation being current and matching expected.json",
        }

    def history_diff(self, fr: Any, exp: dict[str, Any]) -> tuple[int | None, int | None]:
        hist = exp.get("history") or {}
        if not hist:
            return None, None
        lost = dup = 0
        clock = fr.h.current_clock(fr.run.run_id)
        for vid, spec in hist.items():
            rows = fr.h.history_rows(fr.run.run_id, vid, [clock.date()])
            oids = [r["observation_id"] for r in rows]
            want = int((spec or {}).get("rows", 0))
            lost += max(0, want - len(set(oids)))
            dup += max(0, len(oids) - want) + (len(oids) - len(set(oids)))
        return lost, dup

    def recovery_outbox(self, row: dict[str, Any]) -> None:
        from transitpulse.verify.fake_source import FakeMbtaSource, FakeResponse, gtfs_rt_doc, vp_entity

        rec = self.ctx.materialize("golden")
        static = self.ctx.static_for(rec.static_zip_path, rec.route_ids)
        run = self.h.create_run(static=static, source_type="synthetic", initial_clock=utcnow(),
                                scenario_id="simulated_source_fault:benchmark_outbox", fixture_kind="simulated_source_fault")
        row.update(run_id=run.run_id, source_type="synthetic")
        start_highs = self.h.high_watermarks(SNAPSHOT_TOPICS)

        def collector(extra: dict[str, str]) -> None:
            env = dict(self.h.env)
            env.update(fake.collector_env())
            env.update(extra)
            argv = [str(self.runtime.core_python), "-m", "transitpulse.ingestion", "collect", "--workdir",
                    str(self.paths.root), "--run-id", run.run_id, "--profile", self.profile, "--duration-seconds", "900"]
            outbox = self.paths.outbox_db(run.run_id)
            self.h.start_component("collector", argv, run_id=run.run_id, env=env, ready=outbox.exists, ready_timeout=120)

        with FakeMbtaSource() as fake:
            t = int(time.time())
            for feed in ("trip_updates", "alerts"):
                fake.set(feed, FakeResponse.json(gtfs_rt_doc(t - 1, [])))
            fake.set(VP, FakeResponse.json(gtfs_rt_doc(t - 1, [vp_entity("V1", route_id="R_A", lat=42.3515,
                                                                         lon=-71.0585, timestamp=t - 10)])))
            collector({"TP_KAFKA_BOOTSTRAP": "127.0.0.1:1"})
            try:
                pending = self._wait_pending(run.run_id)
                for feed in ("vehicle_positions", "trip_updates", "alerts"):
                    fake.set(feed, FakeResponse.not_modified())
                injected = utcnow()
                pid = self.h.kill_component_hard("collector", "transitpulse.ingestion")
                time.sleep(2.0)
                self.h.stop_component("collector")
                t_recover = time.monotonic()
                started = utcnow()
                collector({})
                published = self._wait_published(run.run_id, pending)
                recovery_s = time.monotonic() - t_recover
            finally:
                self.h.stop_component("collector")
        end_highs = self.h.high_watermarks(SNAPSHOT_TOPICS)
        seen: dict[str, int] = {}
        for (topic, part), high in end_highs.items():
            for msg in self.h.run_messages(run.run_id, topic, start_highs.get((topic, part), 0), high, part):
                cid = (msg.get("json") or {}).get("capture_id")
                if cid:
                    seen[cid] = seen.get(cid, 0) + 1
        lost = sum(1 for cid in pending if cid not in seen)
        dup = sum(n - 1 for cid, n in seen.items() if cid in pending and n > 1)
        row["status"] = "MEASURED" if published else "PARTIAL"
        row["recovery"] = {
            "fault": "collector SIGKILL with pending outbox rows (unreachable broker)", "injected_at": iso_utc(injected),
            "exit_code": f"SIGKILL pid {pid}", "recovery_started_at": iso_utc(started),
            "recovery_seconds": round(recovery_s, 2) if published else None, "matches_expected": lost == 0,
            "records_lost": lost, "records_duplicated": dup, "failed_checks": [] if lost == 0 else ["pending lost"],
            "fault_scope": "collector process of one run (simulated source)",
            "logs": [str(p) for p in (self.h.component_log("collector"),) if p],
            "definition": "from the collector restart to every previously pending outbox row being acknowledged",
        }

    def _wait_pending(self, run_id: str) -> set[str]:
        import sqlite3

        db = self.paths.outbox_db(run_id)
        deadline = time.time() + 120
        while time.time() < deadline:
            if db.exists():
                con = sqlite3.connect(str(db), timeout=10)
                try:
                    rows = con.execute("SELECT capture_id FROM outbox WHERE kind = 'snapshot' AND state = 'pending'"
                                       ).fetchall()
                finally:
                    con.close()
                if rows:
                    time.sleep(3.0)
                    return {r[0] for r in rows}
            time.sleep(1.0)
        raise RuntimeError("no pending outbox row appeared within 120 s")

    def _wait_published(self, run_id: str, pending: set[str], timeout: float = 300.0) -> bool:
        import sqlite3

        db = self.paths.outbox_db(run_id)
        deadline = time.time() + timeout
        while time.time() < deadline:
            con = sqlite3.connect(str(db), timeout=10)
            try:
                rows = dict(con.execute("SELECT capture_id, state FROM outbox WHERE kind = 'snapshot'").fetchall())
            finally:
                con.close()
            if all(rows.get(cid) == "published" for cid in pending):
                return True
            time.sleep(1.0)
        return False


def _failed(outcome: Any) -> list[str]:
    out = []
    for res in (outcome.api, outcome.cassandra, outcome.lineage):
        for c in (res or {}).get("checks", []):
            if not c.get("passed"):
                out.append(c["name"])
    return out[:30]


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="benchmarks/run_benchmark.py", description=__doc__.split("\n")[0])
    ap.add_argument("--scenario", action="append", required=True, help="scenario name(s); repeat or comma-separate")
    ap.add_argument("--workdir", default=os.environ.get("TP_WORKDIR"))
    ap.add_argument("--profile", default=os.environ.get("TP_PROFILE") or "local-native")
    ap.add_argument("--recording-manifest", default=None)
    ap.add_argument("--static-zip", default=None)
    ap.add_argument("--route-ids", default=None)
    ap.add_argument("--repetitions", type=int, default=None)
    ap.add_argument("--warmup", type=float, default=None)
    ap.add_argument("--measure", type=float, default=None)
    ap.add_argument("--drain-timeout", type=float, default=None)
    ap.add_argument("--max-seconds", type=float, default=None, help="overall budget; later repetitions -> NOT_RUN")
    ap.add_argument("--bench-id", default=None)
    ap.add_argument("--scenarios-file", default=str(HERE / "scenarios.yaml"))
    args = ap.parse_args(argv)
    if not args.workdir:
        args.workdir = str(load_config(args.profile).workdir)
    return args


def main(argv: list[str] | None = None) -> int:
    started = time.time()
    try:
        args = parse_args(argv)
        spec = load_scenarios(Path(args.scenarios_file))
    except (SystemExit, ValueError, OSError) as exc:
        if isinstance(exc, SystemExit) and exc.code in (0, None):
            return 0
        sys.stdout.write(json.dumps({"ok": False, "action": "benchmark", "error": {
            "code": "usage", "message": str(exc), "hint": "see --help", "log_excerpt": None}}) + "\n")
        return 2
    defaults = spec.get("defaults") or {}
    names = [n.strip() for chunk in args.scenario for n in chunk.split(",") if n.strip()]
    unknown = [n for n in names if n not in spec["scenarios"]]
    if unknown:
        sys.stdout.write(json.dumps({"ok": False, "action": "benchmark", "error": {
            "code": "unknown_scenario", "message": f"unknown scenario(s) {unknown}",
            "hint": f"choose from {sorted(spec['scenarios'])}", "log_excerpt": None}}) + "\n")
        return 2
    args.warmup = float(args.warmup if args.warmup is not None else defaults.get("warmup_seconds", 60))
    args.measure = float(args.measure if args.measure is not None else defaults.get("measure_seconds", 120))
    args.drain_timeout = float(args.drain_timeout if args.drain_timeout is not None
                               else defaults.get("drain_timeout_seconds", 60))
    args.resource_interval = float(defaults.get("resource_interval_seconds", 2))
    args.backlog_interval = float(defaults.get("backlog_interval_seconds", 5))
    args.bench_id = args.bench_id or f"bench-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    bench = Bench(args)
    protocol = {"command": "python " + shlex.join([sys.argv[0], *(argv if argv is not None else sys.argv[1:])]),
                "warmup_s": args.warmup, "measure_s": args.measure, "drain_timeout_s": args.drain_timeout,
                "repetitions": args.repetitions or defaults.get("repetitions", 3),
                "resource_interval_s": args.resource_interval, "backlog_interval_s": args.backlog_interval}
    results: list[dict[str, Any]] = []
    error = None
    reason = None
    try:
        reason = bench.setup()
        for name in names:
            scen = dict(spec["scenarios"][name])
            reps = int(args.repetitions or scen.get("repetitions") or defaults.get("repetitions", 3))
            for rep in range(1, reps + 1):
                row = _row(args.bench_id, name, scen, rep, protocol)
                results.append(row)
                if reason:
                    row["notes"].append(f"NOT_RUN: {reason}")
                    continue
                if args.max_seconds and time.time() - started > args.max_seconds:
                    row["notes"].append(f"NOT_RUN: --max-seconds {args.max_seconds} budget exhausted")
                    continue
                try:
                    if scen.get("kind") == "recovery":
                        bench.run_recovery(name, scen, rep, row)
                    else:
                        bench.run_throughput(name, scen, rep, row)
                except Exception as exc:  # noqa: BLE001 - one failed repetition never stops the others
                    row["status"] = "PARTIAL" if row.get("metrics") else "NOT_RUN"
                    row["notes"].append(f"error: {type(exc).__name__}: {exc}"[:1500])
                    log(traceback.format_exc()[-3000:])
                    for comp in ("publisher", "collector", "processor"):
                        entry = bench.h.sup.entry(comp) if bench.h else None
                        if entry and entry.get("session_id") == bench.session_id and bench.h.component_alive(comp):
                            bench.h.stop_component(comp)
                log(f"{name} r{rep}: {row['status']}")
    except Exception as exc:  # noqa: BLE001 - reported in the single JSON document
        error = {"code": type(exc).__name__, "message": str(exc)[:2000], "hint": None,
                 "log_excerpt": traceback.format_exc()[-2000:]}
    finally:
        bench.close()
    doc = {"bench_id": args.bench_id, "generated_at": iso_utc(utcnow()), "protocol": protocol,
           "environment": environment(bench.paths, bench.cfg, bench.runtime, args.profile),
           "targets": spec.get("targets") or {}, "results": results,
           "statuses": {s: sum(1 for r in results if r["status"] == s) for s in ("MEASURED", "PARTIAL", "NOT_RUN")}}
    files = report.write_outputs(bench.out_dir, doc, bench.resource_samples, bench.backlog_samples)
    sys.stdout.write(json.dumps({
        "ok": error is None, "action": "benchmark", "bench_id": args.bench_id, "out_dir": str(bench.out_dir),
        "files": files, "statuses": doc["statuses"],
        "results": [{"scenario": r["scenario"], "repetition": r["repetition"], "status": r["status"],
                     "run_id": r["run_id"], "notes": r["notes"][:3]} for r in results],
        "error": error}, default=str) + "\n")
    return 0 if error is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
