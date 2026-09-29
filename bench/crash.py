"""Crash-safety test: run worker processes, SIGKILL some of them mid-job, and
check that every job still reaches a terminal state.

Each handler call first records a "delivery" row in a separate table (committed
immediately), then sleeps to simulate work. A job whose worker is killed after
that insert but before its ack is delivered again after the lease expires, so

    duplicate deliveries = delivery rows - distinct jobs delivered

measures how often at-least-once delivery actually re-ran work.

    python -m bench.crash --jobs 1000 --workers 6 --kills 30
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import random
import time
from pathlib import Path
from typing import Any

import psycopg

from pgqueue import Job, Queue, Worker, migrate

from .localdb import database_url

TASK = "crash-test"


def worker_main(
    url: str,
    worker_id: str,
    lease: float,
    work: tuple[float, float],
    invocations: Any = None,
) -> None:
    """Run one worker process.

    `invocations` is an optional shared multiprocessing.Value counting every
    handler call, including calls that fail before recording a delivery row.
    The crash test does not use it: a process killed while holding its lock
    would deadlock the others.
    """
    logging.basicConfig(level=logging.ERROR)
    recorder = psycopg.connect(url, autocommit=True)

    def handler(job: Job) -> dict[str, Any]:
        nonlocal recorder
        if invocations is not None:
            with invocations.get_lock():
                invocations.value += 1
        if recorder.broken or recorder.closed:  # e.g. after a database restart
            recorder = psycopg.connect(url, autocommit=True)
        recorder.execute(
            "INSERT INTO crash_deliveries (job_id, worker_id, attempt) VALUES (%s, %s, %s)",
            (job.id, worker_id, job.attempts),
        )
        time.sleep(random.uniform(*work))
        return {"worker": worker_id}

    with Queue(url, max_size=3) as q:
        Worker(
            q,
            {TASK: handler},
            worker_id=worker_id,
            lease_seconds=lease,
            poll_interval=0.02,
            reap_interval=lease / 2,
        ).run()


def reset(url: str) -> None:
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS crash_deliveries ("
            " job_id bigint NOT NULL, worker_id text NOT NULL, attempt int NOT NULL,"
            " at timestamptz NOT NULL DEFAULT now())"
        )
        conn.execute("TRUNCATE jobs, crash_deliveries RESTART IDENTITY")


def run_crash_test(
    url: str,
    *,
    jobs: int = 300,
    workers: int = 4,
    kills: int = 10,
    kill_interval: float = 0.3,
    lease: float = 1.0,
    work: tuple[float, float] = (0.01, 0.05),
    seed: int = 0,
    timeout: float = 120.0,
) -> dict[str, Any]:
    migrate(url)
    reset(url)
    rng = random.Random(seed)
    with Queue(url) as q:
        for i in range(jobs):
            # Generous max_attempts: this test is about losing jobs, not dead-lettering.
            q.enqueue(TASK, {"n": i}, max_attempts=50)

    ctx = mp.get_context("spawn")  # same behaviour on Windows and Linux
    procs: dict[str, mp.Process] = {}
    next_id = 0

    def spawn() -> None:
        nonlocal next_id
        wid = f"crash-{next_id}"
        next_id += 1
        p = ctx.Process(target=worker_main, args=(url, wid, lease, work), daemon=True)
        p.start()
        procs[wid] = p

    for _ in range(workers):
        spawn()

    monitor = psycopg.connect(url, autocommit=True)

    def running_jobs(worker_id: str) -> int:
        return monitor.execute(
            "SELECT count(*) FROM jobs WHERE state = 'running' AND locked_by = %s", (worker_id,)
        ).fetchone()[0]

    started = time.monotonic()
    # Sampled just before each kill, so approximate: the victim may finish or
    # claim a job between this query and the kill.
    kills_mid_job = 0
    # Let the workers import and connect before the first kill.
    while monitor.execute("SELECT count(*) FROM jobs WHERE attempts > 0").fetchone()[0] == 0:
        time.sleep(0.05)
    for _ in range(kills):
        time.sleep(kill_interval)
        victim = rng.choice(sorted(procs))
        kills_mid_job += running_jobs(victim) > 0
        procs.pop(victim).kill()  # SIGKILL on Linux, TerminateProcess on Windows
        spawn()

    wait_for_terminal(monitor, timeout)
    elapsed = time.monotonic() - started
    for p in procs.values():
        p.kill()
        p.join()
    summary = summarize(monitor)
    monitor.close()

    return {
        "jobs": jobs,
        "workers": workers,
        "kills": kills,
        "kills_while_holding_a_job": kills_mid_job,
        "lease_seconds": lease,
        "work_seconds": list(work),
        "seed": seed,
        "elapsed_seconds": round(elapsed, 2),
        **summary,
    }


def wait_for_terminal(conn: psycopg.Connection, timeout: float) -> None:
    """Poll until no job is queued or running; raise TimeoutError otherwise."""
    deadline = time.monotonic() + timeout
    while True:
        open_jobs = conn.execute(
            "SELECT count(*) FROM jobs WHERE state IN ('queued', 'running')"
        ).fetchone()[0]
        if open_jobs == 0:
            return
        if time.monotonic() > deadline:
            states = dict(
                conn.execute("SELECT state, count(*) FROM jobs GROUP BY state").fetchall()
            )
            raise TimeoutError(f"{open_jobs} job(s) still open after {timeout}s: {states}")
        time.sleep(0.1)


def summarize(conn: psycopg.Connection) -> dict[str, Any]:
    """Final job states plus delivery counts from the crash_deliveries table."""
    states = dict(conn.execute("SELECT state, count(*) FROM jobs GROUP BY state").fetchall())
    deliveries, distinct, max_per_job = conn.execute(
        "SELECT count(*), count(DISTINCT job_id), coalesce(max(n), 0)"
        " FROM crash_deliveries JOIN (SELECT job_id, count(*) AS n FROM crash_deliveries"
        " GROUP BY job_id) per_job USING (job_id)"
    ).fetchone()
    never_delivered = conn.execute(
        "SELECT count(*) FROM jobs j WHERE NOT EXISTS"
        " (SELECT 1 FROM crash_deliveries d WHERE d.job_id = j.id)"
    ).fetchone()[0]
    failed_attempts = conn.execute("SELECT coalesce(sum(errors), 0) FROM jobs").fetchone()[0]
    return {
        "final_states": states,
        "jobs_in_table": sum(states.values()),
        "non_terminal_jobs": states.get("queued", 0) + states.get("running", 0),
        "never_delivered": never_delivered,
        "deliveries": deliveries,
        "distinct_jobs_delivered": distinct,
        "duplicate_deliveries": deliveries - distinct,
        "max_deliveries_of_one_job": max_per_job,
        # Expired leases plus handler errors (the latter only if the database
        # went away while a handler was recording its delivery).
        "failed_attempts": int(failed_attempts),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--kills", type=int, default=30)
    parser.add_argument("--kill-interval", type=float, default=0.3)
    parser.add_argument("--lease", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--out", type=Path, default=Path("results/crash_test.json"))
    args = parser.parse_args()

    result = run_crash_test(
        database_url("pgqueue_bench"),
        jobs=args.jobs,
        workers=args.workers,
        kills=args.kills,
        kill_interval=args.kill_interval,
        lease=args.lease,
        seed=args.seed,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    lost = result["jobs"] - result["final_states"].get("succeeded", 0)
    raise SystemExit(1 if lost else 0)


if __name__ == "__main__":
    main()
