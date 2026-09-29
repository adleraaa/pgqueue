"""FastAPI HTTP API for producers and operators.

Workers do not use HTTP: they talk to PostgreSQL directly through the worker
library, so claim and ack are each one SQL statement rather than an HTTP call
plus a statement.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import psycopg
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg_pool import PoolTimeout
from pydantic import BaseModel, Field, field_validator, model_validator

from . import metrics
from .db import database_url
from .models import Job
from .queue import InvalidTransition, JobNotFound, Queue, to_jsonb


class EnqueueRequest(BaseModel):
    task: str = Field(min_length=1, max_length=200)
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: str = Field(default="default", min_length=1, max_length=100)
    priority: int = Field(default=0, ge=-1000, le=1000, description="Higher runs first.")
    delay_seconds: float = Field(default=0.0, ge=0, le=365 * 24 * 3600)
    run_at: datetime | None = Field(default=None, description="Absolute start time (with tz).")
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)
    max_attempts: int = Field(default=5, ge=1, le=100)

    @field_validator("payload")
    @classmethod
    def _storable_payload(cls, payload: dict[str, Any]) -> dict[str, Any]:
        # The JSON parser accepts NaN and "\u0000", which jsonb rejects; answer
        # 422 here instead of a 500 from the INSERT.
        to_jsonb(payload)
        return payload

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
        # A short pool timeout: if the database is down, requests fail after
        # 5 s instead of the 30 s default that workers use to ride out restarts.
        app.state.queue = queue or Queue(database_url(), max_size=10, timeout=5.0)
        try:
            yield
        finally:
            if owned:
                app.state.queue.close()

    app = FastAPI(title="pgqueue", version="0.1.0", lifespan=lifespan)

    def q(request: Request) -> Queue:
        return request.app.state.queue

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        # FastAPI's default handler echoes the offending input back, which fails
        # to serialize when that input is NaN (and turns the 422 into a 500).
        errors = [{k: v for k, v in e.items() if k != "input"} for e in exc.errors()]
        return JSONResponse({"detail": jsonable_encoder(errors)}, status_code=422)

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
        try:
            with q(request).pool.connection(timeout=2.0) as conn:
                conn.execute("SELECT 1")
        except (PoolTimeout, psycopg.OperationalError):
            raise HTTPException(503, "database unavailable") from None
        return {"status": "ok"}

    return app
