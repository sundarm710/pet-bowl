"""KV node: infers 'landed' by polling ingest's read-only device-state route.

The WAL is flock'd by the ingest process, so we never open it directly.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from petbowl.console.bus import Bus, TraceEvent
from petbowl.console.nodes.base import BaseNode
from petbowl.console.nodes.ingest import IngestNode, _get

MAX_WATCH = 500
BATCH = 50


class KvNode(BaseNode):
    type = "kv"

    def __init__(self, ingest: IngestNode, node_id: str = "kv", interval_s: float = 0.5) -> None:
        super().__init__(node_id, "KV state")
        self.ingest = ingest
        self.interval = interval_s
        self.expected: dict[str, int] = {}  # device -> highest acked seq
        self.state: dict[str, dict[str, Any]] = {}  # device -> {max_seq, max_ts, seen}
        self.errors = 0
        self.polls = 0
        self._order: list[str] = []
        self._cursor = 0
        self._task: asyncio.Task[None] | None = None

    async def start(self, bus: Bus) -> None:
        await super().start(bus)
        bus.on_event(self._observe)
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()

    def _observe(self, ev: TraceEvent) -> None:
        if ev.kind == "batch.ack":
            if ev.data.get("status") != 200:
                return
        elif ev.kind != "log.acked":  # ack-log tail from a terminal-launched simulator
            return
        for dev, seq in ev.data.get("devices", {}).items():
            if dev not in self.expected:
                if len(self.expected) >= MAX_WATCH:
                    continue
                self._order.append(dev)
            self.expected[dev] = max(self.expected.get(dev, -1), seq)

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            if not self._order:
                continue
            devs = [
                self._order[(self._cursor + i) % len(self._order)]
                for i in range(min(BATCH, len(self._order)))
            ]
            self._cursor = (self._cursor + len(devs)) % len(self._order)
            await asyncio.gather(*(self._poll(d) for d in devs))

    async def _poll(self, dev: str) -> None:
        self.polls += 1
        try:
            status, body = await asyncio.to_thread(_get, self.ingest.state_url(dev), 2.0)
        except OSError:
            self.errors += 1
            return
        if status != 200:
            return
        try:
            st = json.loads(body)
        except ValueError:
            return
        prev = self.state.get(dev, {}).get("max_seq", -1)
        if st["max_seq"] != prev:
            self.state[dev] = {
                "max_seq": st["max_seq"],
                "max_ts": st["max_ts"],
                "seen": time.time(),
            }
            self.emit(
                "kv.state",
                corr=f"{dev}:{st['max_seq']}",
                device_id=dev,
                max_seq=st["max_seq"],
                prev=prev,
                max_ts=st["max_ts"],
            )

    def snapshot(self) -> dict[str, Any]:
        behind = sum(
            1 for d, s in self.expected.items() if self.state.get(d, {}).get("max_seq", -1) < s
        )
        recent = sorted(self.state.items(), key=lambda kv: -kv[1]["seen"])[:50]
        return {
            "health": "ok" if self.errors == 0 or self.polls > self.errors else "down",
            "watched": len(self.expected),
            "devices_with_state": len(self.state),
            "behind": behind,
            "poll_errors": self.errors,
            "devices": [{"device_id": d, "expected": self.expected.get(d), **s} for d, s in recent],
        }
