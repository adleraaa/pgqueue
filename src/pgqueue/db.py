"""Database helpers: connection settings and a minimal SQL-file migration runner."""

from __future__ import annotations

import os
from importlib import resources

import psycopg

# Arbitrary constant; any process running migrations takes this advisory lock
# first, so an API container and a worker container starting at the same time
# cannot apply the same migration twice.
_MIGRATION_LOCK_ID = 72_430_001

DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/postgres"


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


def migration_files() -> list[tuple[str, str]]:
    """Return (filename, sql) pairs from the packaged migrations/ folder, in order."""
    folder = resources.files("pgqueue") / "migrations"
    files = [f for f in folder.iterdir() if f.name.endswith(".sql")]
    return [(f.name, f.read_text(encoding="utf-8")) for f in sorted(files, key=lambda f: f.name)]


def migrate(conninfo: str) -> list[str]:
    """Apply pending migrations. Returns the names of the files that were applied.

    Each file runs in its own transaction together with its bookkeeping row, so a
    failing migration leaves no partial schema behind.
    """
    applied: list[str] = []
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (_MIGRATION_LOCK_ID,))
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                " version text PRIMARY KEY,"
                " applied_at timestamptz NOT NULL DEFAULT now())"
            )
            done = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
            for name, sql in migration_files():
                if name in done:
                    continue
                with conn.transaction():
                    # No parameters, so psycopg sends the whole file as one
                    # simple query and multiple statements are allowed.
                    conn.execute(sql)  # type: ignore[arg-type]
                    conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (name,))
                applied.append(name)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (_MIGRATION_LOCK_ID,))
    return applied
