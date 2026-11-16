"""Postgres node: read-only polling of the events table."""

from __future__ import annotations

import asyncio
from typing import Any

import psycopg

from petbowl.console.bus import Bus
from petbowl.console.nodes.base import BaseNode

QUERY_KINDS = "SELECT kind, count(*) FROM events GROUP BY kind"
QUERY_RECENT = (
    "SELECT device_id, seq, kind, value, ts, received_at FROM events "
    "ORDER BY received_at DESC LIMIT 20"
)
QUERY_LAG = (
    "SELECT count(*), max(received_at), extract(epoch FROM now() - max(received_at)) FROM events"
)


class PostgresNode(BaseNode):
    type = "postgres"

    def __init__(self, db_url: str, node_id: str = "postgres", interval_s: float = 1.0) -> None:
        super().__init__(node_id, "Postgres events")
        self.db_url = db_url
        self.interval = interval_s
        self.rows = 0
        self.by_kind: dict[str, int] = {}
        self.recent: list[dict[str, Any]] = []
        self.since_last_s: float | None = None
        self.error: str | None = None
        self._task: asyncio.Task[None] | None = None

    async def start(self, bus: Bus) -> None:
        await super().start(bus)
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()

    def _poll(self) -> tuple[int, dict[str, int], list[dict[str, Any]], float | None]:
        with psycopg.connect(self.db_url, connect_timeout=3, autocommit=True) as conn:
            conn.read_only = True
            total, _, lag = conn.execute(QUERY_LAG).fetchone() or (0, None, None)
            kinds = {str(k): int(n) for k, n in conn.execute(QUERY_KINDS).fetchall()}
            recent = [
                {
                    "device_id": r[0],
                    "seq": r[1],
                    "kind": r[2],
                    "value": r[3],
                    "ts": r[4].isoformat(),
                    "received_at": r[5].isoformat(),
                }
                for r in conn.execute(QUERY_RECENT).fetchall()
            ]
        return int(total), kinds, recent, float(lag) if lag is not None else None

    async def _loop(self) -> None:
        while True:
            try:
                rows, kinds, recent, lag = await asyncio.to_thread(self._poll)
                if rows != self.rows:
                    self.emit("pg.rows", rows=rows, delta=rows - self.rows)
                self.rows, self.by_kind, self.recent, self.since_last_s = rows, kinds, recent, lag
                self.error = None
            except Exception as exc:  # DB down must not kill the console
                self.error = str(exc).splitlines()[0][:200] if str(exc) else type(exc).__name__
            await asyncio.sleep(self.interval)

    def snapshot(self) -> dict[str, Any]:
        return {
            "health": "down" if self.error else "ok",
            "error": self.error,
            "rows": self.rows,
            "by_kind": self.by_kind,
            "seconds_since_last_insert": self.since_last_s,
            "recent": self.recent,
        }
