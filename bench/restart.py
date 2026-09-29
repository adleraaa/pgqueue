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
from .localdb import ROOT, database_url


def embedded_restart_cmd() -> list[str]:
    import pixeltable_pgserver

    bin_dir = Path(pixeltable_pgserver.__file__).parent / "pginstall18" / "bin"
    return [str(bin_dir / "pg_ctl"), "restart", "-D", str(ROOT / ".pgdata"), "-m", "fast", "-w"]


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
    restart_after: float = 1.5,
    work: tuple[float, float] = (0.01, 0.03),
    timeout: float = 120.0,
) -> dict[str, Any]:
    migrate(url)
    reset(url)
    with Queue(url) as q:
        for i in range(jobs):
            q.enqueue(TASK, {"n": i}, max_attempts=50)

    ctx = mp.get_context("spawn")
    procs = [
        ctx.Process(target=worker_main, args=(url, f"restart-{i}", lease, work), daemon=True)
        for i in range(workers)
    ]
    for p in procs:
        p.start()

    with psycopg.connect(url, autocommit=True) as conn:
        while conn.execute("SELECT count(*) FROM jobs WHERE attempts > 0").fetchone()[0] == 0:
            time.sleep(0.05)
        time.sleep(restart_after)
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
        "restart_command": " ".join(
            Path(restart_cmd[0]).name if i == 0 else a for i, a in enumerate(restart_cmd)
        ),
        "succeeded_before_restart": done_before,
        "running_at_restart": running_before,
        "restart_and_reconnect_seconds": round(unavailable_seconds, 2),
        "seconds_from_restart_to_all_terminal": round(recovery_seconds, 2),
        **summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--jobs", type=int, default=500)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--lease", type=float, default=5.0)
    parser.add_argument("--restart-cmd", help="shell command that restarts the database")
    parser.add_argument("--out", type=Path, default=Path("results/restart_test.json"))
    args = parser.parse_args()
    # The pool logs every failed reconnect attempt during the restart; too noisy here.
    logging.getLogger("psycopg.pool").setLevel(logging.ERROR)

    cmd = shlex.split(args.restart_cmd) if args.restart_cmd else embedded_restart_cmd()
    result = run_restart_test(
        database_url("pgqueue_bench"), cmd, jobs=args.jobs, workers=args.workers, lease=args.lease
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result["final_states"] == {"succeeded": args.jobs} else 1)


if __name__ == "__main__":
    main()
