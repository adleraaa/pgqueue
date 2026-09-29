"""FastAPI HTTP API for producers and operators.

Workers do not use HTTP: they talk to PostgreSQL directly through the worker
library, which keeps claim/ack on a single round trip each.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field, model_validator

from . import metrics
from .db import database_url
from .models import Job
from .queue import InvalidTransition, JobNotFound, Queue


class EnqueueRequest(BaseModel):
    task: str = Field(min_length=1, max_length=200)
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: str = Field(default="default", min_length=1, max_length=100)
    priority: int = Field(default=0, ge=-1000, le=1000, description="Higher runs first.")
    delay_seconds: float = Field(default=0.0, ge=0, le=365 * 24 * 3600)
    run_at: datetime | None = Field(default=None, description="Absolute start time (with tz).")
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)
    max_attempts: int = Field(default=5, ge=1, le=100)

    @model_validator(mode="after")
    def _one_schedule(self) -> EnqueueRequest:
        if self.run_at is not None and self.delay_seconds:
            raise ValueError("pass either run_at or delay_seconds, not both")
        if self.run_at is not None and self.run_at.tzinfo is None:
            raise ValueError("run_at must include a timezone offset")
        return self


def create_app(queue: Queue | None = None) -> FastAPI:
    """Build the app. Tests pass a Queue; otherwise one is opened from DATABASE_URL."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned = queue is None
        app.state.queue = queue or Queue(database_url(), max_size=10)
        try:
            yield
        finally:
            if owned:
                app.state.queue.close()

    app = FastAPI(title="pgqueue", version="0.1.0", lifespan=lifespan)

    def q(request: Request) -> Queue:
        return request.app.state.queue

    # Handlers are plain `def`: psycopg calls block, so FastAPI runs them in its
    # thread pool instead of on the event loop.

    @app.post("/jobs", response_model=Job, status_code=201)
    def enqueue(body: EnqueueRequest, request: Request, response: Response) -> Job:
        job, created = q(request).enqueue(
            body.task,
            body.payload,
            queue=body.queue,
            priority=body.priority,
            delay_seconds=body.delay_seconds,
            run_at=body.run_at,
            idempotency_key=body.idempotency_key,
            max_attempts=body.max_attempts,
        )
        if not created:
            response.status_code = 200  # duplicate idempotency key: existing job
        return job

    @app.get("/jobs/{job_id}", response_model=Job)
    def get_job(job_id: int, request: Request) -> Job:
        job = q(request).get(job_id)
        if job is None:
            raise HTTPException(404, f"job {job_id} not found")
        return job

    @app.post("/jobs/{job_id}/cancel", response_model=Job)
    def cancel_job(job_id: int, request: Request) -> Job:
        try:
            return q(request).cancel(job_id)
        except JobNotFound:
            raise HTTPException(404, f"job {job_id} not found") from None
        except InvalidTransition as exc:
            raise HTTPException(409, str(exc)) from None

    @app.get("/metrics")
    def get_metrics(request: Request) -> Response:
        return Response(metrics.render(q(request).metrics()), media_type=metrics.CONTENT_TYPE)

    @app.get("/healthz")
    def healthz(request: Request) -> dict[str, str]:
        with q(request).pool.connection() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}

    return app
