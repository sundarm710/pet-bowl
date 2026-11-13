import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
import pytest

URL = os.environ.get("PETBOWL_DB_URL", "postgresql://petbowl:petbowl@127.0.0.1:5432/petbowl")
SCHEMA_SQL = Path(__file__).parents[1] / "src/petbowl/ingest/schema.sql"


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    try:
        c = psycopg.connect(URL, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError:
        if os.environ.get("CI"):
            raise  # in CI a missing database is a failure, not a skip
        pytest.skip("postgres not running: make db-up")
    name = f"t_{uuid.uuid4().hex[:12]}"
    c.execute(f"CREATE SCHEMA {name}")
    c.execute(f"SET search_path TO {name}")
    c.execute(SCHEMA_SQL.read_text())
    try:
        yield c
    finally:
        c.execute(f"DROP SCHEMA {name} CASCADE")
        c.close()


def insert(
    conn: psycopg.Connection[Any],
    device: str,
    seq: int,
    kind: str = "weight_g",
    ts: str = "2026-01-01T00:00:00Z",
) -> int:
    cur = conn.execute(
        "INSERT INTO events (device_id, seq, ts, kind, value) VALUES (%s, %s, %s, %s, 1.0) "
        "ON CONFLICT (device_id, seq) DO NOTHING",
        (device, seq, ts, kind),
    )
    return cur.rowcount


def test_duplicate_insert_is_a_noop_and_rowcount_says_so(conn: psycopg.Connection[Any]) -> None:
    assert insert(conn, "d1", 1) == 1
    assert insert(conn, "d1", 1) == 0
    assert conn.execute("SELECT count(*) FROM events").fetchone() == (1,)


def test_same_seq_on_different_devices_is_distinct(conn: psycopg.Connection[Any]) -> None:
    assert insert(conn, "d1", 1) == 1
    assert insert(conn, "d2", 1) == 1


def test_unknown_kind_is_rejected_by_the_database(conn: psycopg.Connection[Any]) -> None:
    with pytest.raises(psycopg.errors.CheckViolation):
        insert(conn, "d1", 1, kind="nope")


def test_event_time_is_stored_as_an_instant(conn: psycopg.Connection[Any]) -> None:
    insert(conn, "d1", 1, ts="2026-01-01T05:30:00+05:30")
    row = conn.execute("SELECT ts FROM events").fetchone()
    assert row is not None
    assert row[0] == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
