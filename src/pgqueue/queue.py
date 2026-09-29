"""The queue itself: every state transition of a job is one SQL statement here.

Callers (the HTTP API, workers, the benchmark) never write to the jobs table
directly, so the rules in this module are the whole contract:

* enqueue is idempotent per (queue, idempotency_key);
* claim hands each queued job to exactly one worker at a time, with a lease;
* ack / fail / heartbeat only succeed for the worker holding the current lease
  (checked with a per-claim fencing token);
* the reaper returns jobs with expired leases to the queue.
"""

from __future__ import annotations

import json
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .models import Job, JobState

# Upper bounds (seconds) of the enqueue-to-start latency histogram buckets.
WAIT_BUCKETS: tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
)


def _contains_nul(value: Any) -> bool:
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(_contains_nul(k) or _contains_nul(v) for k, v in value.items())
    if isinstance(value, list | tuple):
        return any(_contains_nul(v) for v in value)
    return False


def to_jsonb(value: Any) -> str:
    """Serialize `value` for a jsonb column, or raise ValueError/TypeError.

    json.dumps happily writes NaN/Infinity and NUL characters (as \\u0000), but
    PostgreSQL's jsonb rejects both. Checking here turns what would be a
    DataError at INSERT/UPDATE time into an ordinary validation error.
    """
    text = json.dumps(value, allow_nan=False)
    if _contains_nul(value):
        raise ValueError("PostgreSQL jsonb cannot store NUL (\\u0000) characters")
    return text


class JobNotFound(LookupError):
    pass


class InvalidTransition(RuntimeError):
    """The job exists but is not in a state that allows the requested change."""


class ClaimMode(StrEnum):
    # FOR UPDATE SKIP LOCKED: concurrent claimers skip rows another transaction
    # has locked and take the next one, so workers never wait on each other.
    SKIP_LOCKED = "skip_locked"
    # Plain FOR UPDATE: claimers queue up behind the same head-of-queue row.
    # Kept only so the benchmark can show the difference.
    NAIVE = "naive"


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with "equal jitter".

    The deterministic half keeps delays growing with each attempt; the random
    half spreads out retries of jobs that failed together (e.g. during an outage
    of a downstream service) so they do not all come back at the same instant.
    """

    base_seconds: float = 1.0
    max_seconds: float = 300.0

    def delay(self, attempt: int, rng: random.Random | None = None) -> float:
        """Delay before the retry that follows failed attempt number `attempt` (1-based)."""
        rng = rng or random.Random()
        capped = min(self.max_seconds, self.base_seconds * 2 ** max(attempt - 1, 0))
        return capped / 2 + rng.uniform(0, capped / 2)


@dataclass
class QueueMetrics:
    """Aggregates read from the jobs table for /metrics."""

    depth: dict[tuple[str, str], int] = field(default_factory=dict)  # (queue, state) -> jobs
    failures: dict[str, int] = field(default_factory=dict)  # queue -> failed attempts
    wait_buckets: dict[str, list[int]] = field(default_factory=dict)  # cumulative, per bucket
    wait_count: dict[str, int] = field(default_factory=dict)
    wait_sum: dict[str, float] = field(default_factory=dict)

    @property
    def queues(self) -> list[str]:
        names = {q for q, _ in self.depth} | set(self.failures) | set(self.wait_count)
        return sorted(names)


_ENQUEUE_SQL = """
INSERT INTO jobs (queue, task, payload, priority, idempotency_key, max_attempts, run_at)
VALUES (%(queue)s, %(task)s, %(payload)s::jsonb, %(priority)s, %(key)s, %(max_attempts)s,
        COALESCE(%(run_at)s::timestamptz, now() + make_interval(secs => %(delay)s)))
ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING *
"""

_CLAIM_TEMPLATE = """
WITH next AS (
    SELECT id FROM jobs
    WHERE state = 'queued' AND queue = %(queue)s AND run_at <= now()
    ORDER BY priority DESC, run_at, id
    LIMIT %(limit)s
    FOR UPDATE{skip_locked}
)
UPDATE jobs AS j
SET state = 'running',
    attempts = j.attempts + 1,
    locked_by = %(worker_id)s,
    lease_token = gen_random_uuid(),
    lease_expires_at = now() + make_interval(secs => %(lease)s),
    started_at = now(),
    -- Measured from when the job became runnable: run_at for delayed jobs,
    -- enqueued_at for jobs scheduled in the past (so a backfilled job with an
    -- old run_at does not show up as hours of queueing latency).
    wait_seconds = COALESCE(j.wait_seconds,
                            extract(epoch FROM now() - GREATEST(j.run_at, j.enqueued_at))),
    updated_at = now()
FROM next
WHERE j.id = next.id
RETURNING j.*
"""

_CLAIM_SQL = {
    ClaimMode.SKIP_LOCKED: _CLAIM_TEMPLATE.format(skip_locked=" SKIP LOCKED"),
    ClaimMode.NAIVE: _CLAIM_TEMPLATE.format(skip_locked=""),
}

_HEARTBEAT_SQL = """
UPDATE jobs
SET lease_expires_at = now() + make_interval(secs => %(lease)s), updated_at = now()
WHERE id = %(id)s AND lease_token = %(token)s AND state = 'running'
RETURNING id
"""

_ACK_SQL = """
UPDATE jobs
SET state = 'succeeded', result = %(result)s::jsonb, finished_at = now(),
    lease_token = NULL, lease_expires_at = NULL, updated_at = now()
WHERE id = %(id)s AND lease_token = %(token)s AND state = 'running'
RETURNING id
"""

# attempts was already incremented by the claim, so attempts >= max_attempts
# means this was the last allowed attempt.
_FAIL_SQL = """
UPDATE jobs
SET errors = errors + 1,
    last_error = %(error)s,
    state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
    run_at = CASE WHEN attempts >= max_attempts THEN run_at
                  ELSE now() + make_interval(secs => %(delay)s) END,
    finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
    lease_token = NULL, lease_expires_at = NULL, updated_at = now()
WHERE id = %(id)s AND lease_token = %(token)s AND state = 'running'
RETURNING *
"""

# An expired lease counts as a failed attempt. That is what stops a job that
# crashes every worker that runs it from being retried forever.
_REAP_SQL = """
UPDATE jobs
SET errors = errors + 1,
    last_error = 'lease expired: worker crashed, stalled or lost its connection',
    state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
    finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
    lease_token = NULL, lease_expires_at = NULL, updated_at = now()
WHERE state = 'running' AND lease_expires_at < now()
RETURNING id
"""

_CANCEL_SQL = """
UPDATE jobs
SET state = 'cancelled', finished_at = now(),
    lease_token = NULL, lease_expires_at = NULL, updated_at = now()
WHERE id = %(id)s AND state IN ('queued', 'running')
RETURNING *
"""


class Queue:
    """Thread-safe handle to the queue, backed by a psycopg connection pool."""

    def __init__(
        self,
        conninfo: str,
        *,
        min_size: int = 1,
        max_size: int = 4,
        retry: RetryPolicy | None = None,
        timeout: float = 30.0,
    ) -> None:
        """`timeout` is how long a call waits for a pooled connection (for
        example while the database restarts) before raising PoolTimeout."""
        self.retry = retry or RetryPolicy()
        self.pool = ConnectionPool(
            conninfo,
            min_size=min_size,
            max_size=max_size,
            timeout=timeout,
            kwargs={"row_factory": dict_row},
            # Validate connections on checkout so that after a database restart
            # stale connections are replaced instead of failing the next query.
            check=ConnectionPool.check_connection,
            open=True,
        )

    def close(self) -> None:
        self.pool.close()

    def __enter__(self) -> Queue:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _fetch_all(self, query: str, params: dict[str, Any] | None = None) -> list[dict]:
        # `with pool.connection()` commits on success and rolls back on error.
        with self.pool.connection() as conn:
            return conn.execute(query, params).fetchall()  # type: ignore[arg-type]

    # -- producer side ---------------------------------------------------------

    def enqueue(
        self,
        task: str,
        payload: dict[str, Any] | None = None,
        *,
        queue: str = "default",
        priority: int = 0,
        delay_seconds: float = 0.0,
        run_at: datetime | None = None,
        idempotency_key: str | None = None,
        max_attempts: int = 5,
    ) -> tuple[Job, bool]:
        """Insert a job. Returns (job, created).

        If `idempotency_key` is already used in this queue, nothing is inserted
        and the existing job is returned with created=False, whatever state it is in.
        """
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if run_at is not None and delay_seconds:
            raise ValueError("pass either run_at or delay_seconds, not both")
        if run_at is not None and run_at.tzinfo is None:
            # psycopg would send it as `timestamp` and PostgreSQL would read it in
            # the server's TimeZone, silently shifting the job by hours.
            raise ValueError("run_at must be timezone-aware")
        params = {
            "queue": queue,
            "task": task,
            "payload": to_jsonb(payload or {}),
            "priority": priority,
            "key": idempotency_key,
            "max_attempts": max_attempts,
            "run_at": run_at,
            "delay": float(delay_seconds),
        }
        with self.pool.connection() as conn:
            row = conn.execute(_ENQUEUE_SQL, params).fetchone()
            if row is not None:
                return Job.model_validate(row), True
            # ON CONFLICT DO NOTHING waited for any concurrent insert of the same
            # key to commit, so this (new-snapshot) SELECT is guaranteed to see it.
            row = conn.execute(
                "SELECT * FROM jobs WHERE queue = %s AND idempotency_key = %s",
                (queue, idempotency_key),
            ).fetchone()
        if row is None:  # pragma: no cover - only if the row was deleted in between
            raise RuntimeError("idempotency conflict but existing job not found")
        return Job.model_validate(row), False

    def get(self, job_id: int) -> Job | None:
        rows = self._fetch_all("SELECT * FROM jobs WHERE id = %(id)s", {"id": job_id})
        return Job.model_validate(rows[0]) if rows else None

    def cancel(self, job_id: int) -> Job:
        """Cancel a queued or running job.

        Cancelling a running job revokes its lease: the worker keeps executing
        the handler (Python cannot safely interrupt it), but its ack is rejected
        and the job is not retried.
        """
        rows = self._fetch_all(_CANCEL_SQL, {"id": job_id})
        if rows:
            return Job.model_validate(rows[0])
        job = self.get(job_id)
        if job is None:
            raise JobNotFound(job_id)
        raise InvalidTransition(f"job {job_id} is already {job.state}")

    # -- worker side -----------------------------------------------------------

    def claim(
        self,
        worker_id: str,
        *,
        queue: str = "default",
        lease_seconds: float = 30.0,
        limit: int = 1,
        mode: ClaimMode = ClaimMode.SKIP_LOCKED,
    ) -> list[Job]:
        """Atomically move up to `limit` runnable jobs to `running` and lease them."""
        rows = self._fetch_all(
            _CLAIM_SQL[mode],
            {"queue": queue, "limit": limit, "worker_id": worker_id, "lease": lease_seconds},
        )
        jobs = [Job.model_validate(r) for r in rows]
        # UPDATE ... RETURNING does not preserve the CTE's ORDER BY.
        jobs.sort(key=lambda j: (-j.priority, j.run_at, j.id))
        return jobs

    def heartbeat(self, job: Job, lease_seconds: float = 30.0) -> bool:
        """Extend the lease. False means the lease was lost (reaped or cancelled)."""
        rows = self._fetch_all(
            _HEARTBEAT_SQL, {"id": job.id, "token": job.lease_token, "lease": lease_seconds}
        )
        return bool(rows)

    def ack(self, job: Job, result: Any = None) -> bool:
        """Mark the job succeeded. False means the lease was lost and nothing changed."""
        rows = self._fetch_all(
            _ACK_SQL, {"id": job.id, "token": job.lease_token, "result": to_jsonb(result)}
        )
        return bool(rows)

    def fail(self, job: Job, error: str) -> Job | None:
        """Record a failed attempt; schedule a retry with backoff or dead-letter the job.

        Returns the updated job, or None if the lease was lost.
        """
        rows = self._fetch_all(
            _FAIL_SQL,
            {
                "id": job.id,
                "token": job.lease_token,
                # text columns cannot hold NUL either; exception messages can.
                "error": error.replace("\x00", "")[:2000],
                "delay": self.retry.delay(job.attempts),
            },
        )
        return Job.model_validate(rows[0]) if rows else None

    def reap_expired(self) -> list[int]:
        """Return jobs whose lease has expired to the queue (or dead-letter them).

        Safe to run from many processes at once: each row is updated by exactly
        one of them. A reap and a concurrent heartbeat for the same job are
        serialized by the row lock, and whichever commits first decides: if the
        heartbeat commits first, the reaper re-checks its WHERE clause, sees the
        extended lease and skips the row; if the reaper commits first, the
        heartbeat's token no longer matches and it reports the lease as lost.
        Both orders are safe.
        """
        return [r["id"] for r in self._fetch_all(_REAP_SQL)]

    # -- observability ---------------------------------------------------------

    def metrics(self) -> QueueMetrics:
        m = QueueMetrics()
        failures: dict[str, int] = defaultdict(int)
        with self.pool.connection() as conn:
            # One snapshot for all three queries. Under the default READ
            # COMMITTED each statement sees a new snapshot, so a claim committing
            # between them could make a bucket count exceed the histogram's
            # _count, which Prometheus treats as a broken histogram.
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            for r in conn.execute(
                "SELECT queue, state, count(*) AS n, sum(errors) AS errors"
                " FROM jobs GROUP BY queue, state"
            ):
                m.depth[(r["queue"], r["state"])] = r["n"]
                failures[r["queue"]] += int(r["errors"])
            for r in conn.execute(
                "SELECT queue, count(*) AS n, sum(wait_seconds) AS total"
                " FROM jobs WHERE wait_seconds IS NOT NULL GROUP BY queue"
            ):
                m.wait_count[r["queue"]] = r["n"]
                m.wait_sum[r["queue"]] = float(r["total"])
                m.wait_buckets[r["queue"]] = [0] * len(WAIT_BUCKETS)
            for r in conn.execute(
                "SELECT j.queue, b.le, count(*) FILTER (WHERE j.wait_seconds <= b.le) AS n"
                " FROM jobs j CROSS JOIN unnest(%s::float8[]) AS b(le)"
                " WHERE j.wait_seconds IS NOT NULL GROUP BY j.queue, b.le",
                (list(WAIT_BUCKETS),),
            ):
                m.wait_buckets[r["queue"]][WAIT_BUCKETS.index(r["le"])] = r["n"]
        m.failures = dict(failures)
        return m


__all__ = [
    "WAIT_BUCKETS",
    "ClaimMode",
    "InvalidTransition",
    "Job",
    "JobNotFound",
    "JobState",
    "Queue",
    "QueueMetrics",
    "RetryPolicy",
    "to_jsonb",
]
