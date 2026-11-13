"""Postgres access for the ingest path. Callers own transactions."""

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from petbowl.common.events import Event

SCHEMA_SQL = Path(__file__).with_name("schema.sql")

INSERT_SQL = """
INSERT INTO events (device_id, seq, ts, kind, value)
SELECT * FROM unnest(%s::text[], %s::bigint[], %s::timestamptz[], %s::text[], %s::float8[])
ON CONFLICT (device_id, seq) DO NOTHING
RETURNING device_id, seq
"""


class PgEvents:
    """EventStore backed by Postgres: one transaction per batch."""

    def __init__(self, pool: AsyncConnectionPool[Any]) -> None:
        self._pool = pool

    async def insert(self, events: Sequence[Event]) -> list[bool]:
        async with self._pool.connection() as conn:
            return await insert_events(conn, events)


async def open_pool(
    url: str, *, schema: str | None = None, max_size: int = 4, timeout: float = 5.0
) -> AsyncConnectionPool[Any]:
    """schema pins search_path; tests use it to stay inside a throwaway schema."""
    kwargs: dict[str, Any] = {}
    if schema:
        kwargs["options"] = f"-c search_path={schema}"
    pool: AsyncConnectionPool[Any] = AsyncConnectionPool(
        url, min_size=1, max_size=max_size, kwargs=kwargs, timeout=timeout, open=False
    )
    await pool.open(wait=True)
    return pool


async def apply_schema(pool: AsyncConnectionPool[Any]) -> None:
    async with pool.connection() as conn:  # commits on clean exit
        await conn.execute(SCHEMA_SQL.read_text())


async def insert_events(conn: AsyncConnection[Any], events: Sequence[Event]) -> list[bool]:
    """Insert in one statement. Per event, in order: True if new, False if duplicate.

    A key repeated inside the batch is new once, then duplicate. The caller commits.
    """
    if not events:
        return []
    cur = await conn.execute(
        INSERT_SQL,
        (
            [e.device_id for e in events],
            [e.seq for e in events],
            [e.ts for e in events],
            [e.kind for e in events],
            [e.value for e in events],
        ),
    )
    inserted = {(d, s) for d, s in await cur.fetchall()}
    results: list[bool] = []
    for e in events:
        key = (e.device_id, e.seq)
        results.append(key in inserted)
        inserted.discard(key)  # a second copy in the same batch is a duplicate
    return results
