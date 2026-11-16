"""Simulator node: spawns/stops `python -m petbowl.simulator` pointed at the tap."""

from __future__ import annotations

import asyncio
import contextlib
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from petbowl.console.bus import Bus
from petbowl.console.nodes.base import BaseNode
from petbowl.console.nodes.simlogs import LogTailer

# UI param -> (CLI flag, caster, default)
PARAMS: dict[str, tuple[str, type, Any]] = {
    "devices": ("--devices", int, 20),
    "hours": ("--hours", float, 1.0),
    "seed": ("--seed", int, 42),
    "speedup": ("--speedup", float, 600.0),
    "dup_rate": ("--dup-rate", float, 0.0),
    "ooo_rate": ("--ooo-rate", float, 0.0),
    "offline_rate": ("--offline-rate", float, 0.0),
    "skew_s": ("--skew-s", float, 0.0),
    "malformed_rate": ("--malformed-rate", float, 0.0),
}


def build_args(params: dict[str, Any], sink_url: str) -> list[str]:
    args = [sys.executable, "-m", "petbowl.simulator", "--sink", sink_url]
    for key, (flag, cast, default) in PARAMS.items():
        val = cast(params.get(key, default))
        if val < 0:
            raise ValueError(f"{key} must be >= 0")
        args += [flag, str(val)]
    return args


class SimulatorNode(BaseNode):
    type = "simulator"

    def __init__(
        self,
        tap_url: str,
        node_id: str = "simulator",
        *,
        ack_log: Path | None = None,
        dead_letter: Path | None = None,
    ) -> None:
        super().__init__(node_id, "Simulator")
        self.tailer = LogTailer(ack_log, dead_letter, node_id)
        self.tap_url = tap_url
        self.proc: asyncio.subprocess.Process | None = None
        self.params: dict[str, Any] = {k: d for k, (_, _, d) in PARAMS.items()}
        self.started_at: float | None = None
        self.exit_code: int | None = None
        self.logs: list[str] = []
        self._reader: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.returncode is None

    async def start(self, bus: Bus) -> None:
        await super().start(bus)
        self.tailer.start(bus)

    async def stop(self) -> None:
        self.tailer.stop()
        await self.act_stop({})

    async def act_start(self, params: dict[str, Any]) -> dict[str, Any]:
        if self.running:
            return {"ok": False, "error": "already running"}
        try:
            args = build_args(params, self.tap_url)
        except (ValueError, TypeError) as exc:
            return {"ok": False, "error": str(exc)}
        self.params = {k: params.get(k, self.params[k]) for k in PARAMS}
        self.logs.clear()
        self.exit_code = None
        self.proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )
        self.started_at = time.time()
        self.emit("sim.start", params=self.params, pid=self.proc.pid)
        self._reader = asyncio.create_task(self._read(self.proc))
        return {"ok": True, "pid": self.proc.pid}

    async def act_stop(self, _: dict[str, Any]) -> dict[str, Any]:
        if not self.running or self.proc is None:
            return {"ok": True, "note": "not running"}
        self.proc.terminate()
        try:
            await asyncio.wait_for(self.proc.wait(), 5)
        except TimeoutError:
            self.proc.kill()
            await self.proc.wait()
        return {"ok": True}

    async def _read(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stderr is not None
        async for raw in proc.stderr:
            line = raw.decode(errors="replace").rstrip()
            self.logs.append(line)
            del self.logs[:-200]
            self.emit("sim.log", line=line)
        code = await proc.wait()
        self.exit_code = code
        self.emit("sim.exit", code=code)

    def actions(self) -> dict[str, Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]]:
        return {"start": self.act_start, "stop": self.act_stop}

    def snapshot(self) -> dict[str, Any]:
        if self.running or self.tailer.active:
            health = "running"
        elif self.exit_code not in (None, 0, -15):
            health = "failed"
        else:
            health = "idle"
        return {
            "health": health,
            "pid": self.proc.pid if self.proc else None,
            "exit_code": self.exit_code,
            "params": self.params,
            "uptime_s": round(time.time() - self.started_at, 1)
            if self.running and self.started_at
            else 0,
            "param_schema": {k: {"flag": f, "default": d} for k, (f, _, d) in PARAMS.items()},
            "logs": self.logs[-30:],
            "external": self.tailer.snapshot(),
        }

    async def aclose(self) -> None:
        with contextlib.suppress(Exception):
            await self.stop()
