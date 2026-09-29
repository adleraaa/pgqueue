from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest

from bench.localdb import database_url
from pgqueue import Queue, migrate


@pytest.fixture(scope="session")
def db_url() -> str:
    url = database_url("pgqueue_test")
    migrate(url)
    return url


@pytest.fixture
def queue(db_url: str) -> Iterator[Queue]:
    with psycopg.connect(db_url, autocommit=True) as conn:
        conn.execute("TRUNCATE jobs RESTART IDENTITY")
    with Queue(db_url, max_size=8) as q:
        yield q


@pytest.fixture
def sql(db_url: str) -> Iterator[psycopg.Connection]:
    """A raw autocommit connection for arranging state the public API cannot."""
    with psycopg.connect(db_url, autocommit=True) as conn:
        yield conn


@pytest.fixture
def expire_lease(sql: psycopg.Connection):
    """Pretend a job's lease ran out, without sleeping in the test."""

    def expire(job_id: int) -> None:
        sql.execute(
            "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = %s",
            (job_id,),
        )

    return expire
