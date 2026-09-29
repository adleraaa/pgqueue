"""Graceful shutdown of the `pgqueue worker` command on SIGTERM.

Linux/macOS only: on Windows, Popen.terminate() is TerminateProcess, which
kills the process without running any signal handler.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from pgqueue import JobState, Queue

ROOT = Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="needs POSIX signals")


def wait_until(condition, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out waiting for {what}")
        time.sleep(0.05)


def test_sigterm_finishes_current_job_then_exits(queue: Queue, db_url: str) -> None:
    first, _ = queue.enqueue("sleep", {"seconds": 1.5}, priority=1)
    second, _ = queue.enqueue("sleep", {"seconds": 0.1})
    proc = subprocess.Popen(
        [sys.executable, "-m", "pgqueue", "worker", "--handlers", "examples.tasks:HANDLERS"],
        cwd=ROOT,
        env={**os.environ, "DATABASE_URL": db_url},
    )
    try:
        wait_until(lambda: queue.get(first.id).state is JobState.RUNNING, 30, "the first claim")
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=15) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    # The job that was running when SIGTERM arrived was finished and acked...
    assert queue.get(first.id).state is JobState.SUCCEEDED
    # ...and the worker did not start another one after the signal.
    assert queue.get(second.id).state is JobState.QUEUED
