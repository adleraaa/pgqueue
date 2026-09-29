"""Worker library: claim jobs, run handlers, keep leases alive, report outcomes.

Delivery is at-least-once. A job whose worker dies after the handler ran but
before the ack reached the database will run again on another worker, so
handlers with side effects should be idempotent (Job.id is a natural dedupe key).
"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import psycopg

from .models import Job
from .queue import ClaimMode, Queue

log = logging.getLogger(__name__)

Handler = Callable[[Job], Any]


@dataclass
class WorkerStats:
    claims: int = 0  # claim queries issued
    empty_claims: int = 0  # claim queries that returned no job
    succeeded: int = 0
    failed: int = 0
    lost_leases: int = 0  # ack/fail rejected because the lease had been revoked


class _Heartbeat:
    """Background thread that extends a job's lease while its handler runs."""

    def __init__(self, queue: Queue, job: Job, lease_seconds: float, interval: float) -> None:
        self._queue = queue
        self._job = job
        self._lease = lease_seconds
        self._interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"heartbeat-{job.id}", daemon=True)
        self.lost = False

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                if not self._queue.heartbeat(self._job, self._lease):
                    # Reaped or cancelled. Keep the handler running (it cannot be
                    # interrupted safely); its ack will simply be rejected.
                    self.lost = True
                    log.warning("job %s: lease lost while running", self._job.id)
                    return
            except psycopg.OperationalError as exc:
                # The database may come back before the lease expires; keep trying.
                log.warning("job %s: heartbeat failed: %s", self._job.id, exc)

    def __enter__(self) -> _Heartbeat:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join()


class Worker:
    def __init__(
        self,
        queue: Queue,
        handlers: Mapping[str, Handler],
        *,
        queue_name: str = "default",
        worker_id: str | None = None,
        lease_seconds: float = 30.0,
        heartbeat_interval: float | None = None,
        poll_interval: float = 0.5,
        reap_interval: float = 5.0,
        claim_mode: ClaimMode = ClaimMode.SKIP_LOCKED,
    ) -> None:
        self.queue = queue
        self.handlers = dict(handlers)
        self.queue_name = queue_name
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.lease_seconds = lease_seconds
        # Three heartbeats per lease period tolerates one or two slow/failed ones.
        self.heartbeat_interval = heartbeat_interval or lease_seconds / 3
        self.poll_interval = poll_interval
        self.reap_interval = reap_interval
        self.claim_mode = claim_mode
        self.stats = WorkerStats()
        self._last_reap = float("-inf")

    def run(self, stop: threading.Event | None = None, max_jobs: int | None = None) -> WorkerStats:
        """Process jobs until `stop` is set (or `max_jobs` jobs were handled)."""
        stop = stop or threading.Event()
        backoff = self.poll_interval
        handled = 0
        log.info("worker %s started on queue %r", self.worker_id, self.queue_name)
        while not stop.is_set() and (max_jobs is None or handled < max_jobs):
            try:
                self._maybe_reap()
                jobs = self.queue.claim(
                    self.worker_id,
                    queue=self.queue_name,
                    lease_seconds=self.lease_seconds,
                    mode=self.claim_mode,
                )
            except psycopg.OperationalError as exc:
                # Database down or restarting: back off exponentially up to 10 s.
                log.warning("database unavailable (%s); retrying in %.1fs", exc, backoff)
                stop.wait(backoff)
                backoff = min(backoff * 2, 10.0)
                continue
            backoff = self.poll_interval
            self.stats.claims += 1
            if not jobs:
                self.stats.empty_claims += 1
                stop.wait(self.poll_interval)
                continue
            for job in jobs:
                self.process(job)
                handled += 1
        log.info("worker %s stopped: %s", self.worker_id, self.stats)
        return self.stats

    def _maybe_reap(self) -> None:
        now = time.monotonic()
        if now - self._last_reap >= self.reap_interval:
            self._last_reap = now
            reaped = self.queue.reap_expired()
            if reaped:
                log.warning("re-queued %d job(s) with expired leases: %s", len(reaped), reaped)

    def process(self, job: Job) -> None:
        """Run one claimed job and record the outcome."""
        handler = self.handlers.get(job.task)
        error: str | None = None
        result: Any = None
        with _Heartbeat(self.queue, job, self.lease_seconds, self.heartbeat_interval):
            try:
                if handler is None:
                    raise LookupError(f"no handler registered for task {job.task!r}")
                result = handler(job)
                json.dumps(result)  # fail the attempt now if the result cannot be stored
            except Exception as exc:  # any handler error is a failed attempt
                error = f"{type(exc).__name__}: {exc}"
        try:
            if error is None:
                ok = self.queue.ack(job, result)
                self.stats.succeeded += ok
            else:
                updated = self.queue.fail(job, error)
                ok = updated is not None
                self.stats.failed += ok
                if updated is not None:
                    log.info(
                        "job %s attempt %d failed -> %s: %s",
                        job.id,
                        job.attempts,
                        updated.state,
                        error,
                    )
        except psycopg.OperationalError as exc:
            # The outcome could not be recorded. The lease will expire and the
            # reaper will re-queue the job: this is where duplicates come from.
            log.warning("job %s: could not record outcome (%s)", job.id, exc)
            return
        if not ok:
            self.stats.lost_leases += 1
            log.warning("job %s: lease was revoked; outcome discarded", job.id)
