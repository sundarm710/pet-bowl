"""Ingest service: parse, store, classify. No HTTP, no driver imports."""

import struct
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol

from pydantic import ValidationError

from petbowl.common.events import Event

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_US = timedelta(microseconds=1)
_STATE = struct.Struct("<qq")  # max_seq, max event ts in microseconds since epoch


class EventStore(Protocol):
    async def insert(self, events: Sequence[Event]) -> list[bool]:
        """Durably store the events in one transaction. Per event: True if new."""
        ...


class StateStore(Protocol):
    def get(self, key: bytes) -> bytes | None: ...
    def put(self, key: bytes, value: bytes) -> None: ...


@dataclass(frozen=True, slots=True)
class DeviceState:
    max_seq: int
    max_ts_us: int


def state_key(device_id: str) -> bytes:
    return b"dev/" + device_id.encode()


def encode_state(s: DeviceState) -> bytes:
    return _STATE.pack(s.max_seq, s.max_ts_us)


def decode_state(raw: bytes) -> DeviceState:
    seq, ts = _STATE.unpack(raw)
    return DeviceState(seq, ts)


class Outcome(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class ItemResult:
    index: int  # position in the request, so the client can find the line
    outcome: Outcome
    detail: str = ""  # rejected: why. accepted: "" | "gap:N" | "late"


@dataclass(frozen=True, slots=True)
class BatchResult:
    items: list[ItemResult]

    def count(self, outcome: Outcome) -> int:
        return sum(1 for r in self.items if r.outcome is outcome)


def _reason(exc: ValidationError) -> str:
    parts = [
        f"{'.'.join(str(p) for p in err['loc']) or 'body'}: {err['msg']}"
        for err in exc.errors()[:3]
    ]
    return "; ".join(parts)[:200]


def _classify(state: DeviceState | None, seq: int) -> str:
    if state is None or seq == state.max_seq + 1:
        return ""
    if seq > state.max_seq + 1:
        return f"gap:{seq - state.max_seq - 1}"
    return "late"


def to_us(ts: datetime) -> int:
    return (ts - _EPOCH) // _US


def from_us(us: int) -> datetime:
    return _EPOCH + timedelta(microseconds=us)


def _advance(state: DeviceState | None, e: Event) -> DeviceState:
    ts_us = to_us(e.ts)
    if state is None:
        return DeviceState(e.seq, ts_us)
    return DeviceState(max(state.max_seq, e.seq), max(state.max_ts_us, ts_us))


async def process_batch(
    raw_items: Sequence[bytes], events: EventStore, state: StateStore
) -> BatchResult:
    results: list[ItemResult | None] = [None] * len(raw_items)

    parsed: list[tuple[int, Event]] = []
    for i, raw in enumerate(raw_items):
        try:
            parsed.append((i, Event.model_validate_json(raw)))
        except ValidationError as exc:
            results[i] = ItemResult(i, Outcome.REJECTED, _reason(exc))

    verdicts = await events.insert([e for _, e in parsed])  # committed when this returns

    before: dict[str, DeviceState | None] = {}
    current: dict[str, DeviceState | None] = {}
    for (i, e), is_new in zip(parsed, verdicts, strict=True):
        dev = e.device_id
        if dev not in current:
            stored = state.get(state_key(dev))
            before[dev] = current[dev] = decode_state(stored) if stored is not None else None
        if is_new:
            results[i] = ItemResult(i, Outcome.ACCEPTED, _classify(current[dev], e.seq))
        else:
            results[i] = ItemResult(i, Outcome.DUPLICATE)
        current[dev] = _advance(current[dev], e)  # idempotent, so duplicates may heal lag

    for dev, new in current.items():
        if new is not None and new != before[dev]:
            state.put(state_key(dev), encode_state(new))

    done = [r for r in results if r is not None]
    assert len(done) == len(raw_items)
    return BatchResult(done)
