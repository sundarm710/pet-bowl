"""Trace envelope and in-process pub/sub with a replay ring buffer."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

ENVELOPE_VERSION = 1


@dataclass(frozen=True, slots=True)
class TraceEvent:
    """The only structure nodes share. `corr` is `device_id:seq` when applicable."""

    node_id: str
    node_type: str
    kind: str
    data: dict[str, Any] = field(default_factory=dict)
    corr: str | None = None
    ts: float = field(default_factory=time.time)
    v: int = ENVELOPE_VERSION
    id: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class Bus:
    def __init__(self, ring: int = 5000) -> None:
        self._ring: deque[TraceEvent] = deque(maxlen=ring)
        self._subs: set[asyncio.Queue[TraceEvent]] = set()
        self._taps: list[Callable[[TraceEvent], None]] = []
        self._next_id = 1
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def publish(self, ev: TraceEvent) -> None:
        """Thread-safe: callable from worker threads."""
        loop = self._loop
        if loop is not None and loop.is_running():
            try:
                running = asyncio.get_running_loop()
            except RuntimeError:
                running = None
            if running is not loop:
                loop.call_soon_threadsafe(self._publish, ev)
                return
        self._publish(ev)

    def emit(
        self, node_id: str, node_type: str, kind: str, corr: str | None = None, **data: Any
    ) -> None:
        self.publish(TraceEvent(node_id, node_type, kind, data, corr))

    def _publish(self, ev: TraceEvent) -> None:
        ev = TraceEvent(
            ev.node_id, ev.node_type, ev.kind, ev.data, ev.corr, ev.ts, ev.v, self._next_id
        )
        self._next_id += 1
        self._ring.append(ev)
        for tap in self._taps:
            tap(ev)
        for q in list(self._subs):
            if q.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    q.get_nowait()  # drop oldest for slow consumers
            q.put_nowait(ev)

    def on_event(self, fn: Callable[[TraceEvent], None]) -> None:
        """Synchronous in-process observer (used by probes, e.g. the KV node)."""
        self._taps.append(fn)

    def replay(self, after_id: int = 0) -> list[TraceEvent]:
        return [e for e in self._ring if e.id > after_id]

    def subscribe(self) -> asyncio.Queue[TraceEvent]:
        q: asyncio.Queue[TraceEvent] = asyncio.Queue(maxsize=2000)
        self._subs.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[TraceEvent]) -> None:
        self._subs.discard(q)
