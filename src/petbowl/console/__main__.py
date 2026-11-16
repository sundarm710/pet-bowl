"""python -m petbowl.console --ingest http://127.0.0.1:8100 --port 8200"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import uvicorn

from petbowl.console.app import create_console_app, default_pipeline


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="petbowl.console")
    p.add_argument("--ingest", default="http://127.0.0.1:8100")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8200)
    p.add_argument(
        "--db-url",
        default=os.environ.get(
            "PETBOWL_DB_URL", "postgresql://petbowl:petbowl@127.0.0.1:5432/petbowl"
        ),
    )
    p.add_argument(
        "--ack-log",
        default="/tmp/acks.txt",
        help="simulator --ack-log file to tail ('' to disable)",
    )
    p.add_argument(
        "--dead-letter",
        default="/tmp/dead.jsonl",
        help="simulator --dead-letter file to tail ('' to disable)",
    )
    a = p.parse_args(argv)
    nodes, edges = default_pipeline(
        a.ingest,
        a.db_url,
        f"http://{a.host}:{a.port}",
        Path(a.ack_log) if a.ack_log else None,
        Path(a.dead_letter) if a.dead_letter else None,
    )
    uvicorn.run(create_console_app(nodes, edges), host=a.host, port=a.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
