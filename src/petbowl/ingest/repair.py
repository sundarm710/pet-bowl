"""Rebuild KV state from Postgres, the source of truth."""

from typing import Any

from psycopg_pool import AsyncConnectionPool

from petbowl.ingest.service import (
    DeviceState,
    StateStore,
    decode_state,
    encode_state,
    state_key,
    to_us,
)

_SQL = "SELECT device_id, max(seq), max(ts) FROM events GROUP BY device_id"


async def repair_state(pool: AsyncConnectionPool[Any], state: StateStore) -> int:
    """Raise each device's KV state to at least what Postgres holds. Returns devices changed."""
    async with pool.connection() as conn:
        cur = await conn.execute(_SQL)
        rows = await cur.fetchall()
    changed = 0
    for device_id, max_seq, max_ts in rows:
        truth = DeviceState(max_seq, to_us(max_ts))
        raw = state.get(state_key(device_id))
        have = decode_state(raw) if raw is not None else None
        merged = (
            truth
            if have is None
            else DeviceState(max(have.max_seq, truth.max_seq), max(have.max_ts_us, truth.max_ts_us))
        )
        if merged != have:
            state.put(state_key(device_id), encode_state(merged))
            changed += 1
    return changed
