"""Command line entry point: `pgqueue migrate | api | worker`.

The connection string comes from the DATABASE_URL environment variable.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import signal
import threading

from .db import database_url, migrate


def _load_handlers(spec: str) -> dict:
    """Import 'package.module:NAME' and return the handler mapping it names."""
    module_name, _, attr = spec.partition(":")
    if not attr:
        raise SystemExit(f"--handlers must look like module:NAME, got {spec!r}")
    return dict(getattr(importlib.import_module(module_name), attr))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="pgqueue")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("migrate", help="apply pending SQL migrations")

    api = sub.add_parser("api", help="run the HTTP API with uvicorn")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)

    worker = sub.add_parser("worker", help="run a worker process")
    worker.add_argument("--handlers", required=True, help="module:NAME of a {task: fn} dict")
    worker.add_argument("--queue", default="default")
    worker.add_argument("--lease", type=float, default=30.0, help="lease length in seconds")
    worker.add_argument("--poll", type=float, default=0.5, help="idle poll interval (s)")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    url = database_url()

    if args.command == "migrate":
        applied = migrate(url)
        print(f"applied {len(applied)} migration(s): {applied}")
    elif args.command == "api":
        import uvicorn

        from .api import create_app

        migrate(url)
        uvicorn.run(create_app(), host=args.host, port=args.port)
    elif args.command == "worker":
        from .queue import Queue
        from .worker import Worker

        migrate(url)
        stop = threading.Event()
        # SIGTERM/SIGINT: finish the current job, then exit (graceful shutdown).
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: stop.set())
        with Queue(url) as q:
            Worker(
                q,
                _load_handlers(args.handlers),
                queue_name=args.queue,
                lease_seconds=args.lease,
                poll_interval=args.poll,
            ).run(stop)


if __name__ == "__main__":
    main()
