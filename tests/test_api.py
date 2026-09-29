from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from pgqueue import Queue
from pgqueue.api import create_app


@pytest.fixture
def client(queue: Queue) -> Iterator[TestClient]:
    with TestClient(create_app(queue)) as c:
        yield c


def test_enqueue_and_get(client: TestClient) -> None:
    r = client.post("/jobs", json={"task": "email", "payload": {"to": "x"}, "priority": 3})
    assert r.status_code == 201
    job = r.json()
    assert job["state"] == "queued" and job["priority"] == 3
    assert "lease_token" not in job  # the fencing token never leaves the server

    r = client.get(f"/jobs/{job['id']}")
    assert r.status_code == 200 and r.json()["payload"] == {"to": "x"}


def test_duplicate_idempotency_key_returns_200_and_same_job(client: TestClient) -> None:
    body = {"task": "charge", "idempotency_key": "invoice-7"}
    first = client.post("/jobs", json=body)
    second = client.post("/jobs", json=body)
    assert (first.status_code, second.status_code) == (201, 200)
    assert first.json()["id"] == second.json()["id"]


def test_delay_seconds_sets_run_at(client: TestClient) -> None:
    job = client.post("/jobs", json={"task": "t", "delay_seconds": 60}).json()
    run_at = datetime.fromisoformat(job["run_at"])
    enqueued_at = datetime.fromisoformat(job["enqueued_at"])
    assert abs((run_at - enqueued_at).total_seconds() - 60) < 0.01


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"task": ""},
        {"task": "t", "max_attempts": 0},
        {"task": "t", "delay_seconds": -1},
        {"task": "t", "run_at": "2030-01-01T00:00:00"},  # no timezone
        {"task": "t", "run_at": "2030-01-01T00:00:00Z", "delay_seconds": 5},
    ],
)
def test_enqueue_validation(client: TestClient, body: dict) -> None:
    assert client.post("/jobs", json=body).status_code == 422


def test_get_missing_job_404(client: TestClient) -> None:
    assert client.get("/jobs/12345").status_code == 404


def test_cancel(client: TestClient, queue: Queue) -> None:
    job_id = client.post("/jobs", json={"task": "t"}).json()["id"]
    r = client.post(f"/jobs/{job_id}/cancel")
    assert r.status_code == 200 and r.json()["state"] == "cancelled"
    assert client.post(f"/jobs/{job_id}/cancel").status_code == 409
    assert client.post("/jobs/999/cancel").status_code == 404


def test_metrics_endpoint(client: TestClient, queue: Queue) -> None:
    client.post("/jobs", json={"task": "t"})
    client.post("/jobs", json={"task": "t"})
    [job] = queue.claim("w")
    queue.ack(job)
    r = client.get("/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain; version=0.0.4")
    text = r.text
    assert 'pgqueue_jobs{queue="default",state="queued"} 1' in text
    assert 'pgqueue_jobs{queue="default",state="succeeded"} 1' in text
    assert 'pgqueue_jobs_completed_total{queue="default"} 1' in text
    assert 'pgqueue_enqueue_to_start_seconds_count{queue="default"} 1' in text


def test_healthz(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}


@pytest.mark.parametrize(
    "raw",
    ['{"task": "t", "payload": {"a": NaN}}', '{"task": "t", "payload": {"a": "x\\u0000y"}}'],
    ids=["nan", "nul"],
)
def test_payload_jsonb_cannot_store_is_422_not_500(client: TestClient, raw: str) -> None:
    r = client.post("/jobs", content=raw, headers={"content-type": "application/json"})
    assert r.status_code == 422


def test_healthz_returns_503_when_database_is_unreachable() -> None:
    # Port 1 on localhost: connections are refused, so the pool never fills.
    with (
        Queue("host=127.0.0.1 port=1 dbname=x user=x connect_timeout=1") as dead,
        TestClient(create_app(dead)) as c,
    ):
        assert c.get("/healthz").status_code == 503
