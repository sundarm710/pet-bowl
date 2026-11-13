import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg_pool import AsyncConnectionPool

from petbowl.common.events import Event, Kind
from petbowl.ingest.db import PgEvents, apply_schema, insert_events, open_pool

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def ev(device: str, seq: int, kind: Kind = "weight_g", value: float | None = 1.0) -> Event:
    return Event(
        device_id=device, seq=seq, ts=T0 + timedelta(seconds=30 * seq), kind=kind, value=value
    )


def run[T](url: str, schema: str, work: Callable[[AsyncConnectionPool[Any]], Awaitable[T]]) -> T:
    async def main() -> T:
        pool = await open_pool(url, schema=schema)
        try:
            await apply_schema(pool)
            return await work(pool)
        finally:
            await pool.close()

    return asyncio.run(main())


async def insert_commit(pool: AsyncConnectionPool[Any], events: list[Event]) -> list[bool]:
    async with pool.connection() as conn:
        return await insert_events(conn, events)


async def count(pool: AsyncConnectionPool[Any]) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM events")
        row = await cur.fetchone()
        assert row is not None
        return int(row[0])


def test_new_rows_then_the_same_batch_again(db_url: str, pg_schema: str) -> None:
    async def work(pool: AsyncConnectionPool[Any]) -> tuple[list[bool], list[bool], int]:
        batch = [ev("d1", 1), ev("d1", 2)]
        first = await insert_commit(pool, batch)
        second = await insert_commit(pool, batch)
        return first, second, await count(pool)

    assert run(db_url, pg_schema, work) == ([True, True], [False, False], 2)


def test_mixed_batch_reports_each_row(db_url: str, pg_schema: str) -> None:
    async def work(pool: AsyncConnectionPool[Any]) -> list[bool]:
        await insert_commit(pool, [ev("d1", 1)])
        return await insert_commit(pool, [ev("d1", 1), ev("d1", 2), ev("d2", 1)])

    assert run(db_url, pg_schema, work) == [False, True, True]


def test_duplicate_inside_one_batch(db_url: str, pg_schema: str) -> None:
    async def work(pool: AsyncConnectionPool[Any]) -> tuple[list[bool], int]:
        res = await insert_commit(pool, [ev("d1", 1), ev("d1", 1)])
        return res, await count(pool)

    assert run(db_url, pg_schema, work) == ([True, False], 1)


def test_null_values_round_trip(db_url: str, pg_schema: str) -> None:
    async def work(pool: AsyncConnectionPool[Any]) -> tuple[list[bool], object]:
        res = await insert_commit(
            pool, [ev("d1", 1, "heartbeat", None), ev("d1", 2, "heartbeat", None)]
        )
        async with pool.connection() as conn:
            cur = await conn.execute("SELECT value FROM events WHERE seq = 1")
            row = await cur.fetchone()
        assert row is not None
        return res, row[0]

    assert run(db_url, pg_schema, work) == ([True, True], None)


def test_empty_batch(db_url: str, pg_schema: str) -> None:
    async def work(pool: AsyncConnectionPool[Any]) -> list[bool]:
        return await insert_commit(pool, [])

    assert run(db_url, pg_schema, work) == []


def test_the_caller_owns_the_transaction(db_url: str, pg_schema: str) -> None:
    async def work(pool: AsyncConnectionPool[Any]) -> int:
        with contextlib.suppress(RuntimeError):
            async with pool.connection() as conn:
                await insert_events(conn, [ev("d1", 1)])
                raise RuntimeError("boom")  # the pool rolls back on exceptions
        return await count(pool)

    assert run(db_url, pg_schema, work) == 0


def test_apply_schema_is_idempotent(db_url: str, pg_schema: str) -> None:
    async def work(pool: AsyncConnectionPool[Any]) -> int:
        await apply_schema(pool)  # run() already applied it once
        return await count(pool)

    assert run(db_url, pg_schema, work) == 0


def test_pg_events_satisfies_the_store_contract(db_url: str, pg_schema: str) -> None:
    async def work(pool: AsyncConnectionPool[Any]) -> list[bool]:
        return await PgEvents(pool).insert([ev("d1", 1), ev("d1", 1)])

    assert run(db_url, pg_schema, work) == [True, False]
