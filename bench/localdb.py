"""Find or start a PostgreSQL server for tests and benchmarks.

If DATABASE_URL is set (CI, docker-compose) it is used as the server. Otherwise
an embedded PostgreSQL from the `pixeltable-pgserver` package is started with
its data directory in ./.pgdata, so development works without Docker.

`python -m bench.localdb` starts the embedded server (if needed) and prints a
DATABASE_URL for it; `python -m bench.localdb stop` shuts it down.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

ROOT = Path(__file__).resolve().parent.parent
PGDATA = ROOT / ".pgdata"


def pg_ctl() -> Path:
    """Path of the pg_ctl binary bundled with pixeltable-pgserver."""
    from pixeltable_pgserver.utils import POSTGRES_VERSIONS, TARGET_POSTGRES_VERSION

    return POSTGRES_VERSIONS[TARGET_POSTGRES_VERSION] / "pg_ctl"


def server_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    try:
        import pixeltable_pgserver
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise RuntimeError(
            "Set DATABASE_URL or install the embedded server: pip install -e .[local]"
        ) from exc
    # cleanup_mode=None leaves the server running after this process exits, so
    # repeated test runs reuse it instead of paying the ~5 s startup each time.
    server = pixeltable_pgserver.get_server(PGDATA, cleanup_mode=None)
    return server.get_uri()


def database_url(dbname: str) -> str:
    """Return a conninfo for `dbname` on the server, creating the database if needed."""
    base = server_url()
    with psycopg.connect(base, autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)).fetchone()
        if not exists:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    return make_conninfo(base, dbname=dbname)


def stop() -> None:
    subprocess.run([str(pg_ctl()), "stop", "-D", str(PGDATA), "-m", "fast"], check=True)


if __name__ == "__main__":
    if sys.argv[1:] == ["stop"]:
        stop()
    else:
        print(server_url())
