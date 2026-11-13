# src/petbowl/simulator/sinks.py
"""Where deliveries end up. The HTTP sink arrives in stage 3."""

import sys
from pathlib import Path
from typing import BinaryIO, Protocol

from petbowl.simulator.faults import Delivery
from petbowl.simulator.http_sink import HttpSink


class Sink(Protocol):
    def write(self, batch: list[Delivery]) -> None: ...
    def close(self) -> None: ...


class NullSink:
    """Discards everything but counts it. Measures the generator's own ceiling."""

    def __init__(self) -> None:
        self.items = 0
        self.nbytes = 0

    def write(self, batch: list[Delivery]) -> None:
        self.items += len(batch)
        self.nbytes += sum(len(d.payload) for d in batch)

    def close(self) -> None:
        pass


class StreamSink:
    """One payload per line, written as raw bytes so corrupted payloads survive."""

    def __init__(self, out: BinaryIO, *, owns: bool) -> None:
        self._out = out
        self._owns = owns

    def write(self, batch: list[Delivery]) -> None:
        if not batch:
            return
        self._out.write(b"\n".join(d.payload for d in batch) + b"\n")

    def close(self) -> None:
        self._out.flush()
        if self._owns:
            self._out.close()


def make_sink(spec: str, *, dead_letter: Path | None = None, ack_log: Path | None = None) -> Sink:
    kind, _, arg = spec.partition(":")
    if spec.startswith(("http://", "https://")):
        return HttpSink(spec, dead_letter=dead_letter, ack_log=ack_log)
    if kind == "null" and not arg:
        return NullSink()
    if kind == "stdout" and not arg:
        return StreamSink(sys.stdout.buffer, owns=False)
    if kind == "jsonl" and arg:
        path = Path(arg)
        path.parent.mkdir(parents=True, exist_ok=True)
        return StreamSink(path.open("wb"), owns=True)
    raise ValueError(f"bad sink spec {spec!r}: use null, stdout or jsonl:<path>")
