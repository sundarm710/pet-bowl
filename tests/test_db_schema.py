from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest


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
