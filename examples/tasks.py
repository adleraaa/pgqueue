"""Example handlers used by docker-compose and the Quickstart.

Run with: pgqueue worker --handlers examples.tasks:HANDLERS
"""

from __future__ import annotations

import time

from pgqueue import Job


def echo(job: Job) -> dict:
    return {"echo": job.payload}


def sleep(job: Job) -> dict:
    seconds = float(job.payload.get("seconds", 1.0))
    time.sleep(seconds)
    return {"slept": seconds}


def always_fail(job: Job) -> None:
    raise RuntimeError(f"attempt {job.attempts} failed on purpose")


HANDLERS = {"echo": echo, "sleep": sleep, "fail": always_fail}
