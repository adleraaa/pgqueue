"""Throughput and latency benchmark.

1. Throughput: pre-load N no-op jobs, start W worker processes behind a
   barrier, and time how long they take to drain the queue. Measured from the
   first claim to the last ack using database timestamps. Run for both claim
   modes (SKIP LOCKED vs plain FOR UPDATE).
2. Latency: one producer enqueues at a fixed rate that the workers can keep
   up with; report p50/p95/p99 of enqueue-to-start latency (the
   `wait_seconds` column: claim time minus run_at, both from the DB clock).

Handlers do no work, so these numbers measure queue overhead (two short
transactions per job), not a realistic workload.

    python -m bench.benchmark            # writes results/benchmark.json
    python -m bench.plot                 # writes results/benchmark.png
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import os
import platform
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

import psycopg

from pgqueue import ClaimMode, Queue, Worker, migrate

from .localdb import database_url

TASK = "noop"


def _worker_main(url: str, worker_id: str, mode: str, poll: float, barrier, stop, stats) -> None:
    logging.basicConfig(level=logging.ERROR)
    with Queue(url, max_size=2) as q:
        worker = Worker(
            q,
            {TASK: lambda job: None},
            worker_id=worker_id,
            claim_mode=ClaimMode(mode),
            poll_interval=poll,
            reap_interval=3600,  # nothing crashes here; keep the reaper out of the numbers
        )
        barrier.wait()
        worker.run(stop)
        stats.put(asdict(worker.stats))


class _Workers:
    """Start W worker processes and release them all at once."""

    def __init__(self, url: str, count: int, mode: ClaimMode, poll: float) -> None:
        ctx = mp.get_context("spawn")
        self.barrier = ctx.Barrier(count + 1)
        self.stop = ctx.Event()
        self.stats = ctx.Queue()
        self.procs = [
            ctx.Process(
                target=_worker_main,
                args=(url, f"bench-{i}", mode.value, poll, self.barrier, self.stop, self.stats),
            )
            for i in range(count)
        ]

    def __enter__(self) -> _Workers:
        for p in self.procs:
            p.start()
        self.barrier.wait()  # every worker has imported, connected and is ready
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop.set()
        self.collected = [self.stats.get(timeout=30) for _ in self.procs]
        for p in self.procs:
            p.join()

    def totals(self) -> dict[str, int]:
        keys = self.collected[0]
        return {k: sum(s[k] for s in self.collected) for k in keys}


def _reset(conn: psycopg.Connection) -> None:
    conn.execute("TRUNCATE jobs RESTART IDENTITY")


def _wait_until_done(conn: psycopg.Connection, expected: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while conn.execute("SELECT count(*) FROM jobs WHERE state = 'succeeded'").fetchone()[0] < (
        expected
    ):
        if time.monotonic() > deadline:
            raise TimeoutError(f"jobs not finished after {timeout}s")
        time.sleep(0.05)


def throughput_run(url: str, workers: int, mode: ClaimMode, jobs: int, poll: float) -> dict:
    with psycopg.connect(url, autocommit=True) as conn:
        _reset(conn)
        conn.execute("INSERT INTO jobs (task) SELECT %s FROM generate_series(1, %s)", (TASK, jobs))
        conn.execute("ANALYZE jobs")
        with _Workers(url, workers, mode, poll) as w:
            _wait_until_done(conn, jobs, timeout=600)
        seconds = conn.execute(
            "SELECT extract(epoch FROM max(finished_at) - min(started_at))::float8 FROM jobs"
        ).fetchone()[0]
    return {
        "workers": workers,
        "mode": mode.value,
        "jobs": jobs,
        "seconds": round(seconds, 3),
        "jobs_per_second": round(jobs / seconds, 1),
        **{f"worker_{k}": v for k, v in w.totals().items()},
    }


def latency_run(
    url: str, workers: int, mode: ClaimMode, rate: float, duration: float, poll: float
) -> dict:
    with psycopg.connect(url, autocommit=True) as conn:
        _reset(conn)
        with _Workers(url, workers, mode, poll), Queue(url) as producer:
            total = int(rate * duration)
            start = time.monotonic()
            for i in range(total):
                # Fixed schedule (not "sleep 1/rate after each enqueue") so a
                # slow enqueue does not lower the offered rate.
                delay = start + i / rate - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                producer.enqueue(TASK)
            offered_seconds = time.monotonic() - start
            _wait_until_done(conn, total, timeout=120)
        p50, p95, p99, mx = conn.execute(
            "SELECT percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY wait_seconds)"
            " || max(wait_seconds) FROM jobs"
        ).fetchone()[0]
    return {
        "workers": workers,
        "mode": mode.value,
        "jobs": total,
        "offered_rate_per_second": round(total / offered_seconds, 1),
        "p50_ms": round(p50 * 1000, 2),
        "p95_ms": round(p95 * 1000, 2),
        "p99_ms": round(p99 * 1000, 2),
        "max_ms": round(mx * 1000, 2),
    }


def environment(url: str) -> dict[str, Any]:
    with psycopg.connect(url) as conn:
        settings = {
            name: conn.execute(f"SHOW {name}").fetchone()[0]
            for name in ("server_version", "fsync", "synchronous_commit", "shared_buffers")
        }
    return {
        "os": platform.platform(),
        "cpu": platform.processor(),
        "logical_cpus": os.cpu_count(),
        "python": platform.python_version(),
        "postgres": settings,
        "database": "local embedded PostgreSQL (pixeltable-pgserver) over TCP on 127.0.0.1"
        if "DATABASE_URL" not in os.environ
        else "DATABASE_URL",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="pgqueue throughput/latency benchmark")
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8, 16, 32])
    parser.add_argument("--jobs", type=int, default=5000)
    parser.add_argument("--repeats", type=int, default=3, help="throughput runs per point")
    parser.add_argument("--poll", type=float, default=0.01, help="worker idle poll interval (s)")
    parser.add_argument("--rate", type=float, default=100.0, help="latency test enqueue rate")
    parser.add_argument("--duration", type=float, default=10.0, help="latency test seconds")
    parser.add_argument("--latency-poll", type=float, default=0.05)
    parser.add_argument("--out", type=Path, default=Path("results/benchmark.json"))
    args = parser.parse_args()

    url = database_url("pgqueue_bench")
    migrate(url)
    result: dict[str, Any] = {
        "environment": environment(url),
        "config": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
        "throughput": [],
        "latency": [],
    }
    for mode in ClaimMode:
        for w in args.workers:
            runs = [throughput_run(url, w, mode, args.jobs, args.poll) for _ in range(args.repeats)]
            # Report the median run; keep every run's rate to show the spread.
            row = sorted(runs, key=lambda r: r["jobs_per_second"])[len(runs) // 2]
            row["all_runs_jobs_per_second"] = [r["jobs_per_second"] for r in runs]
            print(json.dumps(row), flush=True)
            result["throughput"].append(row)
    for mode in ClaimMode:
        for w in args.workers:
            row = latency_run(url, w, mode, args.rate, args.duration, args.latency_poll)
            print(json.dumps(row), flush=True)
            result["latency"].append(row)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
