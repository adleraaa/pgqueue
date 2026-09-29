"""Data types shared by the queue, the worker and the HTTP API."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class JobState(StrEnum):
    QUEUED = "queued"  # waiting to run (including retries scheduled for later)
    RUNNING = "running"  # claimed by a worker that holds an unexpired lease
    SUCCEEDED = "succeeded"
    DEAD = "dead"  # gave up after max_attempts failed attempts
    CANCELLED = "cancelled"


TERMINAL_STATES = frozenset({JobState.SUCCEEDED, JobState.DEAD, JobState.CANCELLED})


class Job(BaseModel):
    id: int
    queue: str
    task: str
    payload: dict[str, Any]
    priority: int
    state: JobState
    idempotency_key: str | None
    attempts: int
    max_attempts: int
    errors: int
    run_at: datetime
    enqueued_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    wait_seconds: float | None
    locked_by: str | None
    # The fencing token is a capability for the worker that holds the lease;
    # it is never serialized into API responses.
    lease_token: UUID | None = Field(default=None, exclude=True)
    lease_expires_at: datetime | None
    last_error: str | None
    result: Any | None
    updated_at: datetime

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES
