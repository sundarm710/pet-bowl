import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path

import psycopg


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


def wait_until(cond: Callable[[], bool], timeout: float, msg: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(0.05)
    raise AssertionError(msg)


def wait_healthy(port: int, proc: subprocess.Popen[bytes], timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise AssertionError(f"server exited early with code {proc.returncode}")
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=1) as r:
                if r.status == 200:
                    return
        except OSError:
            time.sleep(0.1)
    raise AssertionError("server did not become healthy")


def start_server(port: int, env: dict[str, str], log: Path) -> subprocess.Popen[bytes]:
    cmd = [
        sys.executable, "-m", "uvicorn", "petbowl.ingest.app:app_from_env",
        "--factory", "--host", "127.0.0.1", "--port", str(port),
        "--workers", "1", "--log-level", "warning",
    ]  # fmt: skip
    with log.open("ab") as f:
        return subprocess.Popen(cmd, env=env, stdout=f, stderr=f)


def ack_lines(path: Path) -> int:
    return path.read_text().count("\n") if path.exists() else 0


def test_kill_nine_the_api_under_load(db_url: str, pg_schema: str, tmp_path: Path) -> None:
    port = free_port()
    env = {
        **os.environ,
        "PETBOWL_DB_URL": db_url,
        "PETBOWL_DB_SCHEMA": pg_schema,
        "PETBOWL_KV_PATH": str(tmp_path / "state.wal"),
    }
    log = tmp_path / "server.log"
    acks, dead = tmp_path / "acks.txt", tmp_path / "dead.jsonl"
    server = start_server(port, env, log)
    sim: subprocess.Popen[str] | None = None
    try:
        wait_healthy(port, server)
        sim = subprocess.Popen(
            [
                sys.executable, "-m", "petbowl.simulator",
                "--devices", "50", "--hours", "1", "--seed", "3", "--speedup", "600",
                "--dup-rate", "0.02", "--ooo-rate", "0.05",
                "--sink", f"http://127.0.0.1:{port}/v1/events",
                "--ack-log", str(acks), "--dead-letter", str(dead),
            ],
            stderr=subprocess.PIPE,
            text=True,
        )  # fmt: skip
        for threshold in (500, 3000):
            wait_until(lambda t=threshold: ack_lines(acks) >= t, 30, "load did not progress")
            server.kill()  # SIGKILL: no shutdown hooks, no final fsync
            server.wait()
            time.sleep(0.5)
            server = start_server(port, env, log)
            wait_healthy(port, server)
        _, err = sim.communicate(timeout=90)
        assert sim.returncode == 0, err

        assert not dead.exists() or dead.read_text() == "", "events were dropped or rejected"
        m = re.search(r"(\d+) retries", err)
        assert m is not None and int(m.group(1)) > 0, "the kills never interrupted traffic"

        with psycopg.connect(db_url, options=f"-c search_path={pg_schema}") as conn:
            rows = conn.execute("SELECT device_id, seq FROM events").fetchall()
            max_by_device = {
                d: s for d, s in conn.execute(
                    "SELECT device_id, max(seq) FROM events GROUP BY device_id"
                ).fetchall()
            }  # fmt: skip
        db_ids = {f"{d}:{s}" for d, s in rows}
        acked = set(acks.read_text().split())
        assert len(db_ids) == len(rows), "duplicate rows"
        assert acked <= db_ids, "acknowledged events missing from Postgres"
        assert acked == db_ids, "events in Postgres that were never acknowledged"

        for device, max_seq in max_by_device.items():
            url = f"http://127.0.0.1:{port}/v1/devices/{device}/state"
            with urllib.request.urlopen(url, timeout=5) as r:
                assert json.load(r)["max_seq"] == max_seq, f"KV state wrong for {device}"
    finally:
        for p in (sim, server):
            if p is not None and p.poll() is None:
                p.kill()
                p.wait()