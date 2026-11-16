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
MISS_LIMIT = 10  # 404s in a row before we stop asking about a device until it is acked again


class KvNode(BaseNode):
    type = "kv"

    def __init__(
        self,
        ingest: IngestNode,
        node_id: str = "kv",
        interval_s: float = 0.5,
        *,
        enabled: bool = True,
    ) -> None:
        super().__init__(node_id, "KV state")
        self.enabled = enabled  # off when ingest has no state route (queue mode)
        self.ingest = ingest
        self.interval = interval_s
        self.expected: dict[str, int] = {}  # device -> highest acked seq
        self.state: dict[str, dict[str, Any]] = {}  # device -> {max_seq, max_ts, seen}
        self.errors = 0
        self.polls = 0
        self.misses: dict[str, int] = {}
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
        if not self.enabled:
            return
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
            if seq > self.expected.get(dev, -1):
                self.misses.pop(dev, None)  # new ack: worth asking again
            self.expected[dev] = max(self.expected.get(dev, -1), seq)

    def _pending(self) -> list[str]:
        """Devices acked at a higher seq than KV has shown us, and not given up on."""
        return [
            d
            for d in self._order
            if self.state.get(d, {}).get("max_seq", -1) < self.expected[d]
            and self.misses.get(d, 0) < MISS_LIMIT
        ]

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            pending = self._pending()  # idle (nothing pending) means zero requests
            if not pending:
                continue
            self._cursor %= len(pending)
            devs = (pending[self._cursor :] + pending[: self._cursor])[:BATCH]
            self._cursor += len(devs)
            await asyncio.gather(*(self._poll(d) for d in devs))

    async def _poll(self, dev: str) -> None:
        self.polls += 1
        try:
            status, body = await asyncio.to_thread(_get, self.ingest.state_url(dev), 2.0)
        except OSError:
            self.errors += 1
            return
        if status == 404:
            self.misses[dev] = self.misses.get(dev, 0) + 1
            return
        if status != 200:
            return
        try:
            st = json.loads(body)
        except ValueError:
            return
        self.misses.pop(dev, None)
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
            "health": (
                "idle"
                if not self.enabled
                else "ok"
                if self.errors == 0 or self.polls > self.errors
                else "down"
            ),
            "watched": len(self.expected),
            "devices_with_state": len(self.state),
            "behind": behind,
            "gave_up": sum(1 for m in self.misses.values() if m >= MISS_LIMIT),
            "poll_errors": self.errors,
            "devices": [{"device_id": d, "expected": self.expected.get(d), **s} for d, s in recent],
        }
