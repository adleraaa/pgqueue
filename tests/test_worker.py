from __future__ import annotations

import threading
import time

from pgqueue import Job, JobState, Queue, RetryPolicy, Worker


def run_until(worker: Worker, done, timeout: float = 10.0) -> None:
    """Run the worker in a thread until done() is true."""
    stop = threading.Event()
    thread = threading.Thread(target=worker.run, args=(stop,))
    thread.start()
    try:
        deadline = time.monotonic() + timeout
        while not done():
            assert time.monotonic() < deadline, "timed out waiting for the worker"
            time.sleep(0.02)
    finally:
        stop.set()
        thread.join(timeout=10)


def all_terminal(queue: Queue, ids: list[int]) -> bool:
    return all(queue.get(i).is_terminal for i in ids)


def test_worker_runs_handlers_and_stores_results(queue: Queue) -> None:
    ids = [queue.enqueue("double", {"x": i})[0].id for i in range(5)]
    worker = Worker(queue, {"double": lambda job: job.payload["x"] * 2}, poll_interval=0.01)
    run_until(worker, lambda: all_terminal(queue, ids))
    assert [queue.get(i).result for i in ids] == [0, 2, 4, 6, 8]
    assert worker.stats.succeeded == 5


def test_failing_handler_retries_then_dead_letters(queue: Queue) -> None:
    queue.retry = RetryPolicy(base_seconds=0.01, max_seconds=0.05)  # keep the test fast
    job, _ = queue.enqueue("flaky", max_attempts=3)
    calls: list[int] = []

    def flaky(j: Job) -> None:
        calls.append(j.attempts)
        raise ValueError("nope")

    worker = Worker(queue, {"flaky": flaky}, poll_interval=0.01)
    run_until(worker, lambda: queue.get(job.id).is_terminal)
    final = queue.get(job.id)
    assert final.state is JobState.DEAD
    assert calls == [1, 2, 3]
    assert final.errors == 3 and final.last_error == "ValueError: nope"


def test_handler_that_succeeds_on_retry(queue: Queue) -> None:
    queue.retry = RetryPolicy(base_seconds=0.01, max_seconds=0.05)
    job, _ = queue.enqueue("second-time-lucky")

    def handler(j: Job) -> int:
        if j.attempts == 1:
            raise TimeoutError("downstream timeout")
        return j.attempts

    run_until(
        Worker(queue, {"second-time-lucky": handler}, poll_interval=0.01),
        lambda: queue.get(job.id).is_terminal,
    )
    final = queue.get(job.id)
    assert final.state is JobState.SUCCEEDED and final.result == 2 and final.errors == 1


def test_unknown_task_fails_the_attempt(queue: Queue) -> None:
    job, _ = queue.enqueue("nobody-handles-this", max_attempts=1)
    run_until(Worker(queue, {}, poll_interval=0.01), lambda: queue.get(job.id).is_terminal)
    final = queue.get(job.id)
    assert final.state is JobState.DEAD
    assert "no handler registered" in final.last_error


def test_unserializable_result_fails_the_attempt(queue: Queue) -> None:
    job, _ = queue.enqueue("bad", max_attempts=1)
    worker = Worker(queue, {"bad": lambda j: object()}, poll_interval=0.01)
    run_until(worker, lambda: queue.get(job.id).is_terminal)
    assert queue.get(job.id).state is JobState.DEAD


def test_heartbeat_keeps_long_job_alive_while_reaper_runs(queue: Queue) -> None:
    """A job running 4x longer than its lease must not be reaped or run twice."""
    job, _ = queue.enqueue("slow")
    runs: list[int] = []

    def slow(j: Job) -> str:
        runs.append(j.attempts)
        time.sleep(2.0)
        return "done"

    worker = Worker(queue, {"slow": slow}, lease_seconds=0.5, poll_interval=0.01, reap_interval=0.1)
    # A second worker only reaps; if the lease lapsed it would re-run the job.
    reaper = Worker(queue, {"slow": slow}, poll_interval=0.05, reap_interval=0.1)
    stop = threading.Event()
    reaper_thread = threading.Thread(target=reaper.run, args=(stop,))
    reaper_thread.start()
    try:
        run_until(worker, lambda: queue.get(job.id).is_terminal)
    finally:
        stop.set()
        reaper_thread.join()
    assert runs == [1]
    assert queue.get(job.id).result == "done"


def test_cancelled_while_running_result_is_discarded(queue: Queue) -> None:
    job, _ = queue.enqueue("slow")
    started = threading.Event()

    def slow(j: Job) -> str:
        started.set()
        time.sleep(0.5)
        return "late"

    worker = Worker(queue, {"slow": slow}, lease_seconds=5, poll_interval=0.01)

    def cancel_once_started() -> bool:
        if started.is_set() and queue.get(job.id).state is JobState.RUNNING:
            queue.cancel(job.id)
        return worker.stats.lost_leases == 1

    run_until(worker, cancel_once_started)
    final = queue.get(job.id)
    assert final.state is JobState.CANCELLED and final.result is None


def test_run_max_jobs(queue: Queue) -> None:
    for _ in range(3):
        queue.enqueue("t")
    stats = Worker(queue, {"t": lambda j: None}, poll_interval=0.01).run(max_jobs=2)
    assert stats.succeeded == 2
    assert queue.metrics().depth[("default", "queued")] == 1
