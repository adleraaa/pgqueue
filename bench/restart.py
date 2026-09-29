"""Database-restart test: restart PostgreSQL while workers are busy and check
that every job still succeeds once the database is back.

By default it restarts the embedded server in ./.pgdata with `pg_ctl restart
-m fast` (which disconnects every session and aborts open transactions). Pass
--restart-cmd to restart some other server, e.g. in CI:

    python -m bench.restart --restart-cmd "docker restart <container>"
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import shlex
import subprocess
import time
from pathlib import Path
from typing import Any

import psycopg

from pgqueue import Queue, migrate

from .crash import TASK, reset, summarize, wait_for_terminal, worker_main
from .localdb import PGDATA, database_url, pg_ctl


def embedded_restart_cmd() -> list[str]:
    return [str(pg_ctl()), "restart", "-D", str(PGDATA), "-m", "fast", "-w"]


def describe_command(cmd: list[str]) -> str:
    """The command for the results file, without machine-specific paths."""
    shown = [Path(cmd[0]).name]
    for prev, arg in zip(cmd, cmd[1:], strict=False):
        shown.append("<pgdata>" if prev == "-D" else arg)
    return " ".join(shown)


def _wait_for_db(url: str, timeout: float = 60.0) -> float:
    """Block until the database accepts connections; return seconds waited."""
    start = time.monotonic()
    while True:
        try:
            with psycopg.connect(url, connect_timeout=2) as conn:
                conn.execute("SELECT 1")
            return time.monotonic() - start
        except psycopg.OperationalError:
            if time.monotonic() - start > timeout:
                raise
            time.sleep(0.1)


def run_restart_test(
    url: str,
    restart_cmd: list[str],
    *,
    jobs: int = 500,
    workers: int = 4,
    lease: float = 5.0,
    restart_at: float = 0.3,
    work: tuple[float, float] = (0.01, 0.03),
    timeout: float = 120.0,
) -> dict[str, Any]:
    """Restart the database once `restart_at` (a fraction) of the jobs have succeeded."""
    migrate(url)
    reset(url)
    with Queue(url) as q:
        for i in range(jobs):
            q.enqueue(TASK, {"n": i}, max_attempts=50)

    ctx = mp.get_context("spawn")
    # Counts handler calls in shared memory, so calls that fail before they can
    # record a delivery row (because the database is down) are counted too.
    invocations = ctx.Value("q", 0)
    procs = [
        ctx.Process(
            target=worker_main, args=(url, f"restart-{i}", lease, work, invocations), daemon=True
        )
        for i in range(workers)
    ]
    for p in procs:
        p.start()

    # Trigger on progress rather than elapsed time, so most jobs are still
    # queued at the restart however fast or slow the machine is.
    threshold = max(1, int(jobs * restart_at))
    with psycopg.connect(url, autocommit=True) as conn:
        while (
            conn.execute("SELECT count(*) FROM jobs WHERE state = 'succeeded'").fetchone()[0]
            < threshold
        ):
            time.sleep(0.01)
        done_before = conn.execute(
            "SELECT count(*) FROM jobs WHERE state = 'succeeded'"
        ).fetchone()[0]
        running_before = conn.execute(
            "SELECT count(*) FROM jobs WHERE state = 'running'"
        ).fetchone()[0]

    started = time.monotonic()
    # Not capture_output: on Windows the restarted postmaster inherits the pipe
    # handles, so waiting for EOF on them would hang forever.
    subprocess.run(restart_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    unavailable_seconds = time.monotonic() - started + _wait_for_db(url)

    with psycopg.connect(url, autocommit=True) as conn:
        wait_for_terminal(conn, timeout)
        recovery_seconds = time.monotonic() - started
        for p in procs:
            p.kill()
            p.join()
        summary = summarize(conn)

    return {
        "jobs": jobs,
        "workers": workers,
        "lease_seconds": lease,
        "restart_command": describe_command(restart_cmd),
        "succeeded_before_restart": done_before,
        "running_at_restart": running_before,
        "restart_and_reconnect_seconds": round(unavailable_seconds, 2),
        "seconds_from_restart_to_all_terminal": round(recovery_seconds, 2),
        **summary,
        # Every handler call, including the ones that raised before recording a
        # delivery row. handler_invocations - deliveries of those were cut short.
        "handler_invocations": invocations.value,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--jobs", type=int, default=500)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--lease", type=float, default=5.0)
    parser.add_argument(
        "--restart-at", type=float, default=0.3, help="restart after this fraction succeeded"
    )
    parser.add_argument("--restart-cmd", help="shell command that restarts the database")
    parser.add_argument("--out", type=Path, default=Path("results/restart_test.json"))
    args = parser.parse_args()
    # The pool logs every failed reconnect attempt during the restart; too noisy here.
    logging.getLogger("psycopg.pool").setLevel(logging.ERROR)

    cmd = shlex.split(args.restart_cmd) if args.restart_cmd else embedded_restart_cmd()
    result = run_restart_test(
        database_url("pgqueue_bench"),
        cmd,
        jobs=args.jobs,
        workers=args.workers,
        lease=args.lease,
        restart_at=args.restart_at,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result["final_states"] == {"succeeded": args.jobs} else 1)


if __name__ == "__main__":
    main()
