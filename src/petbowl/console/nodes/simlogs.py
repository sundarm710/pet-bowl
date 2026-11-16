"""Tail the simulator's own --ack-log / --dead-letter files.

This lets a simulator started from a terminal straight at ingest (:8100) show up in
the console without going through the tap. Both files are append-only and accumulate
across runs, so we start at the current end of file and only report new lines.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any

from petbowl.console.bus import Bus

MAX_READ = 4_000_000  # bytes per tick per file; the rest is picked up next tick
CORR_CAP = 200
SAMPLE = 10


class _Tail:
    def __init__(self, path: Path) -> None:
        self.path = path
        # Skip history already in the file; a file created later is read from 0.
        try:
            self.offset = path.stat().st_size
        except OSError:
            self.offset = 0
        self._partial = b""

    def read_new(self) -> list[bytes]:
        try:
            size = self.path.stat().st_size
        except OSError:
            self.offset, self._partial = 0, b""  # gone: read a recreated file from 0
            return []
        if size < self.offset:  # truncated/replaced
            self.offset, self._partial = 0, b""
        if size == self.offset:
            return []
        with self.path.open("rb") as f:
            f.seek(self.offset)
            chunk = f.read(MAX_READ)
        self.offset += len(chunk)
        *lines, self._partial = (self._partial + chunk).split(b"\n")
        return [ln for ln in lines if ln.strip()]


class LogTailer:
    def __init__(self, ack_log: Path | None, dead_letter: Path | None, node_id: str) -> None:
        self.node_id = node_id
        self.ack = _Tail(ack_log) if ack_log else None
        self.dead = _Tail(dead_letter) if dead_letter else None
        self.acked = 0
        self.dead_lettered = 0
        self.dead_reasons: Counter[str] = Counter()
        self.last_activity = 0.0
        self._task: asyncio.Task[None] | None = None
        self._bus: Bus | None = None

    @property
    def active(self) -> bool:
        return time.monotonic() - self.last_activity < 3.0 if self.last_activity else False

    def start(self, bus: Bus) -> None:
        self._bus = bus
        self._task = asyncio.create_task(self._loop())

    def stop(self) -> None:
        if self._task:
            self._task.cancel()

    async def _loop(self) -> None:
        while True:
            try:
                self.tick()
            except Exception as exc:  # a bad log line must never kill the console
                if self._bus:
                    self._bus.emit(self.node_id, "simulator", "log.error", error=repr(exc)[:200])
            await asyncio.sleep(0.5)

    def tick(self) -> None:
        assert self._bus is not None
        if self.ack:
            self._ack_lines(self.ack.read_new())
        if self.dead:
            self._dead_lines(self.dead.read_new())

    def _ack_lines(self, lines: list[bytes]) -> None:
        devices: dict[str, int] = {}
        corrs: list[str] = []
        n = 0
        for raw in lines:
            dev, _, seq = raw.decode("utf-8", "replace").strip().rpartition(":")
            if not dev or not seq.isdigit():
                continue
            n += 1
            devices[dev] = max(devices.get(dev, -1), int(seq))
            if len(corrs) < CORR_CAP:
                corrs.append(f"{dev}:{seq}")
        if not n:
            return
        self.acked += n
        self.last_activity = time.monotonic()
        assert self._bus is not None
        self._bus.emit(
            self.node_id, "simulator", "log.acked", count=n, corrs=corrs, devices=devices
        )

    def _dead_lines(self, lines: list[bytes]) -> None:
        reasons: Counter[str] = Counter()
        sample: list[dict[str, str]] = []
        n = 0
        for raw in lines:
            try:
                rec = json.loads(raw)
                reason, payload = str(rec.get("reason", "?"))[:80], str(rec.get("payload", ""))
            except ValueError:
                continue  # partial/corrupt line
            n += 1
            reasons[reason] += 1
            if len(sample) < SAMPLE:
                sample.append({"reason": reason, "payload": payload[:200]})
        if not n:
            return
        self.dead_lettered += n
        self.dead_reasons.update(reasons)
        self.last_activity = time.monotonic()
        assert self._bus is not None
        self._bus.emit(
            self.node_id, "simulator", "log.dead", count=n, reasons=dict(reasons), sample=sample
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "ack_log": str(self.ack.path) if self.ack else None,
            "dead_letter": str(self.dead.path) if self.dead else None,
            "acked": self.acked,
            "dead_lettered": self.dead_lettered,
            "dead_reasons": dict(self.dead_reasons.most_common(10)),
            "active": self.active,
        }
