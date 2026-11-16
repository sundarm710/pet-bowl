"""Server-side node contract. One file per node type under console/nodes/."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from petbowl.console.bus import Bus


class NodePlugin(Protocol):
    id: str
    type: str
    label: str

    async def start(self, bus: Bus) -> None: ...
    async def stop(self) -> None: ...
    def snapshot(self) -> dict[str, Any]: ...
    def actions(self) -> dict[str, Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]]: ...


class RateWindow:
    """Items per second over a sliding window."""

    def __init__(self, window_s: float = 5.0) -> None:
        self.window_s = window_s
        self._pts: deque[tuple[float, int]] = deque()

    def add(self, n: int) -> None:
        self._pts.append((time.monotonic(), n))

    def rate(self) -> float:
        cutoff = time.monotonic() - self.window_s
        while self._pts and self._pts[0][0] < cutoff:
            self._pts.popleft()
        return sum(n for _, n in self._pts) / self.window_s


class BaseNode:
    type = "generic"

    def __init__(self, node_id: str, label: str | None = None) -> None:
        self.id = node_id
        self.label = label or node_id
        self.bus: Bus | None = None

    async def start(self, bus: Bus) -> None:
        self.bus = bus

    async def stop(self) -> None:
        return None

    def snapshot(self) -> dict[str, Any]:
        return {"health": "unknown"}

    def actions(self) -> dict[str, Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]]:
        return {}

    def emit(self, kind: str, corr: str | None = None, **data: Any) -> None:
        if self.bus is not None:
            self.bus.emit(self.id, self.type, kind, corr, **data)
