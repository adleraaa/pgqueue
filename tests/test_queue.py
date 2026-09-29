from __future__ import annotations

import random
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from pgqueue import ClaimMode, InvalidTransition, JobNotFound, JobState, Queue, RetryPolicy


def test_enqueue_defaults(queue: Queue) -> None:
    job, created = queue.enqueue("email", {"to": "a@example.com"})
    assert created
    assert job.state is JobState.QUEUED
    assert job.payload == {"to": "a@example.com"}
    assert job.attempts == 0 and job.max_attempts == 5
    assert queue.get(job.id) == job


def test_idempotency_key_returns_existing_job(queue: Queue) -> None:
    first, created1 = queue.enqueue("email", {"n": 1}, idempotency_key="order-42")
    second, created2 = queue.enqueue("email", {"n": 2}, idempotency_key="order-42")
    assert created1 and not created2
    assert second.id == first.id
    assert second.payload == {"n": 1}  # the duplicate's payload is ignored
    # Same key in a different queue is a different job.
    other, created3 = queue.enqueue("email", queue="other", idempotency_key="order-42")
    assert created3 and other.id != first.id


def test_concurrent_duplicate_enqueues_create_one_job(queue: Queue) -> None:
    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(lambda _: queue.enqueue("t", idempotency_key="k"), range(16)))
    assert len({job.id for job, _ in results}) == 1
    assert sum(created for _, created in results) == 1


def test_claim_order_priority_then_fifo(queue: Queue) -> None:
    low, _ = queue.enqueue("t", priority=0)
    high, _ = queue.enqueue("t", priority=10)
    low2, _ = queue.enqueue("t", priority=0)
    claimed = queue.claim("w", limit=3)
    assert [j.id for j in claimed] == [high.id, low.id, low2.id]
    assert all(j.state is JobState.RUNNING and j.attempts == 1 for j in claimed)
    assert all(j.lease_token is not None and j.locked_by == "w" for j in claimed)
    assert queue.claim("w") == []


def test_delayed_job_is_not_claimable_until_run_at(queue: Queue, sql) -> None:
    job, _ = queue.enqueue("t", delay_seconds=3600)
    assert queue.claim("w") == []
    sql.execute("UPDATE jobs SET run_at = now() WHERE id = %s", (job.id,))
    assert [j.id for j in queue.claim("w")] == [job.id]


def test_run_at_absolute(queue: Queue) -> None:
    future = datetime.now(UTC) + timedelta(hours=1)
    job, _ = queue.enqueue("t", run_at=future)
    assert abs((job.run_at - future).total_seconds()) < 0.001
    assert queue.claim("w") == []


def test_claims_are_scoped_to_queue(queue: Queue) -> None:
    queue.enqueue("t", queue="emails")
    assert queue.claim("w", queue="default") == []
    assert len(queue.claim("w", queue="emails")) == 1


def test_skip_locked_does_not_wait_for_locked_rows(queue: Queue, db_url: str) -> None:
    first, _ = queue.enqueue("t")
    second, _ = queue.enqueue("t")
    # Another transaction holds a row lock on the head of the queue.
    with psycopg.connect(db_url) as other, ThreadPoolExecutor(1) as pool:
        other.execute("SELECT id FROM jobs WHERE id = %s FOR UPDATE", (first.id,))
        claimed = pool.submit(queue.claim, "w").result(timeout=5)
        assert [j.id for j in claimed] == [second.id]
        other.rollback()


def test_naive_claim_blocks_behind_locked_row(queue: Queue, db_url: str) -> None:
    first, _ = queue.enqueue("t")
    queue.enqueue("t")
    with psycopg.connect(db_url) as other, ThreadPoolExecutor(1) as pool:
        other.execute("SELECT id FROM jobs WHERE id = %s FOR UPDATE", (first.id,))
        future = pool.submit(queue.claim, "w", mode=ClaimMode.NAIVE)
        with pytest.raises(TimeoutError):
            future.result(timeout=0.5)  # stuck waiting for the lock
        other.rollback()
        # Once the lock is released the row still matches, so it is claimed.
        assert [j.id for j in future.result(timeout=5)] == [first.id]


def test_concurrent_claims_never_hand_out_a_job_twice(queue: Queue) -> None:
    ids = {queue.enqueue("t")[0].id for _ in range(200)}
    claimed: list[int] = []
    lock = threading.Lock()

    def drain(worker: str) -> None:
        while jobs := queue.claim(worker, limit=3):
            with lock:
                claimed.extend(j.id for j in jobs)

    with ThreadPoolExecutor(6) as pool:
        list(pool.map(drain, [f"w{i}" for i in range(6)]))
    assert sorted(claimed) == sorted(ids)


def test_ack_marks_succeeded_and_stores_result(queue: Queue) -> None:
    queue.enqueue("t")
    [job] = queue.claim("w")
    assert queue.ack(job, {"answer": 42})
    done = queue.get(job.id)
    assert done.state is JobState.SUCCEEDED
    assert done.result == {"answer": 42}
    assert done.finished_at is not None and done.lease_expires_at is None
    assert not queue.ack(job)  # second ack is rejected


def test_fail_schedules_retry_with_backoff(queue: Queue) -> None:
    queue.enqueue("t", max_attempts=3)
    [job] = queue.claim("w")
    failed = queue.fail(job, "boom")
    assert failed.state is JobState.QUEUED
    assert failed.errors == 1 and failed.last_error == "boom"
    # Attempt 1 with the default policy (base 1 s): delay in [0.5, 1.0] s.
    delay = (failed.run_at - failed.updated_at).total_seconds()
    assert 0.5 <= delay <= 1.0
    assert queue.claim("w") == []  # not yet runnable


def test_job_is_dead_lettered_after_max_attempts(queue: Queue, sql) -> None:
    queue.enqueue("t", max_attempts=2)
    for attempt in (1, 2):
        sql.execute("UPDATE jobs SET run_at = now()")  # skip the backoff wait
        [job] = queue.claim("w")
        assert job.attempts == attempt
        result = queue.fail(job, f"error {attempt}")
    assert result.state is JobState.DEAD
    assert result.errors == 2 and result.finished_at is not None
    sql.execute("UPDATE jobs SET run_at = now()")
    assert queue.claim("w") == []


def test_reaper_requeues_expired_leases_only(queue: Queue, expire_lease) -> None:
    queue.enqueue("t")
    queue.enqueue("t")
    stuck, healthy = queue.claim("w", limit=2)
    expire_lease(stuck.id)
    assert queue.reap_expired() == [stuck.id]
    reaped = queue.get(stuck.id)
    assert reaped.state is JobState.QUEUED and reaped.errors == 1
    assert "lease expired" in reaped.last_error
    assert queue.get(healthy.id).state is JobState.RUNNING


def test_reaper_dead_letters_when_attempts_exhausted(queue: Queue, expire_lease) -> None:
    queue.enqueue("t", max_attempts=1)
    [job] = queue.claim("w")
    expire_lease(job.id)
    queue.reap_expired()
    assert queue.get(job.id).state is JobState.DEAD


def test_stale_worker_cannot_ack_after_its_lease_was_reaped(queue: Queue, expire_lease) -> None:
    queue.enqueue("t")
    [stale] = queue.claim("worker-a")
    expire_lease(stale.id)
    queue.reap_expired()
    [fresh] = queue.claim("worker-b")
    assert fresh.id == stale.id and fresh.attempts == 2
    # Worker A wakes up and tries to finish: its fencing token is out of date.
    assert not queue.ack(stale, "late")
    assert queue.fail(stale, "late") is None
    assert not queue.heartbeat(stale)
    assert queue.ack(fresh, "on time")
    assert queue.get(fresh.id).result == "on time"


def test_heartbeat_extends_lease(queue: Queue, expire_lease) -> None:
    queue.enqueue("t")
    [job] = queue.claim("w", lease_seconds=5)
    expire_lease(job.id)
    assert queue.heartbeat(job, lease_seconds=60)
    assert queue.reap_expired() == []
    remaining = queue.get(job.id).lease_expires_at - datetime.now(UTC)
    assert remaining > timedelta(seconds=50)


def test_cancel_queued_and_running(queue: Queue) -> None:
    queued, _ = queue.enqueue("t")
    assert queue.cancel(queued.id).state is JobState.CANCELLED
    assert queue.claim("w") == []

    queue.enqueue("t")
    [running] = queue.claim("w")
    assert queue.cancel(running.id).state is JobState.CANCELLED
    assert not queue.ack(running)  # the worker's lease was revoked
    assert queue.get(running.id).state is JobState.CANCELLED


def test_cancel_errors(queue: Queue) -> None:
    with pytest.raises(JobNotFound):
        queue.cancel(999)
    queue.enqueue("t")
    [job] = queue.claim("w")
    queue.ack(job)
    with pytest.raises(InvalidTransition):
        queue.cancel(job.id)


def test_wait_seconds_recorded_on_first_claim_only(queue: Queue, sql, expire_lease) -> None:
    job, _ = queue.enqueue("t")
    sql.execute("UPDATE jobs SET run_at = now() - interval '2 seconds' WHERE id = %s", (job.id,))
    [claimed] = queue.claim("w")
    assert 2.0 <= claimed.wait_seconds < 3.0
    expire_lease(job.id)
    queue.reap_expired()
    [again] = queue.claim("w")
    assert again.wait_seconds == claimed.wait_seconds


def test_metrics_aggregates(queue: Queue) -> None:
    queue.enqueue("t")
    queue.enqueue("t")
    queue.enqueue("t", queue="other")
    [job] = queue.claim("w")
    queue.fail(job, "x")
    m = queue.metrics()
    assert m.depth[("default", "queued")] == 2
    assert m.depth[("other", "queued")] == 1
    assert m.failures == {"default": 1, "other": 0}
    assert m.wait_count == {"default": 1}
    assert m.wait_buckets["default"][-1] == 1


@pytest.mark.parametrize("attempt", [1, 2, 5, 12])
def test_retry_policy_bounds(attempt: int) -> None:
    policy = RetryPolicy(base_seconds=2, max_seconds=60)
    capped = min(60, 2 * 2 ** (attempt - 1))
    rng = random.Random(attempt)
    delays = [policy.delay(attempt, rng) for _ in range(200)]
    assert all(capped / 2 <= d <= capped for d in delays)
    assert len(set(delays)) > 1  # jittered, not constant


def test_enqueue_rejects_bad_max_attempts(queue: Queue) -> None:
    with pytest.raises(ValueError):
        queue.enqueue("t", max_attempts=0)
