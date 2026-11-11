# src/petbowl/simulator/faults.py
"""Fault injection: turns clean events into what a flaky network delivers."""

import heapq
import random
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from petbowl.common.events import Event


@dataclass(frozen=True)
class Delivery:
    at: datetime  # virtual time the message reaches the sink
    payload: bytes  # one JSON document, possibly corrupted


@dataclass(frozen=True)
class FaultConfig:
    seed: int = 0
    dup_rate: float = 0.0  # per event: chance of 1 to 3 extra copies
    ooo_rate: float = 0.0  # per event: chance of delayed delivery
    ooo_max_delay_s: float = 120.0
    offline_rate: float = 0.0  # per event: chance the device drops off the network
    offline_min_s: float = 300.0
    offline_max_s: float = 3600.0
    skew_s: float = 0.0  # per device: constant clock offset in [-skew_s, +skew_s]
    malformed_rate: float = 0.0  # per event: chance the payload is corrupted


def _corrupt(payload: bytes, rng: random.Random) -> bytes:
    mode = rng.randrange(3)
    if mode == 0:  # truncated JSON
        return payload[: rng.randrange(1, len(payload))]
    if mode == 1:  # valid JSON, schema violation
        return payload.replace(b'"kind":"', b'"kind":"x_')
    return b"\x00\xffnot json"  # garbage


def apply_faults(events: Iterator[Event], cfg: FaultConfig) -> Iterator[Delivery]:
    """Must be the first stage after `stream`: input is ordered by true time."""
    rng = random.Random(f"{cfg.seed}:faults")
    skews: dict[str, float] = {}
    offline_until: dict[str, datetime] = {}
    pending: list[tuple[datetime, int, bytes]] = []
    n = 0  # tie-break: equal delivery times stay FIFO

    def skew_of(device_id: str) -> float:
        if device_id not in skews:
            r = random.Random(f"{cfg.seed}:skew:{device_id}")
            skews[device_id] = r.uniform(-cfg.skew_s, cfg.skew_s)
        return skews[device_id]

    for e in events:
        t = e.ts  # true time, captured before skew
        if cfg.skew_s:
            e = e.model_copy(update={"ts": t + timedelta(seconds=skew_of(e.device_id))})

        payload = e.model_dump_json().encode()
        if rng.random() < cfg.malformed_rate:
            payload = _corrupt(payload, rng)

        # offline: hold everything until the window ends, then burst
        until = offline_until.get(e.device_id)
        if until is not None and t >= until:
            del offline_until[e.device_id]
            until = None
        if until is None and rng.random() < cfg.offline_rate:
            span = rng.uniform(cfg.offline_min_s, cfg.offline_max_s)
            until = t + timedelta(seconds=span)
            offline_until[e.device_id] = until
        at = until if until is not None else t

        # out-of-order: extra network delay
        if rng.random() < cfg.ooo_rate:
            at += timedelta(seconds=rng.uniform(1, cfg.ooo_max_delay_s))

        # duplicates: retries shortly after the original
        copies = 1 + (rng.randint(1, 3) if rng.random() < cfg.dup_rate else 0)
        for k in range(copies):
            extra = timedelta(seconds=rng.uniform(0, 5)) if k else timedelta(0)
            heapq.heappush(pending, (at + extra, n, payload))
            n += 1

        # every delay is >= 0, so anything due by now can no longer be overtaken
        while pending and pending[0][0] <= t:
            due, _, p = heapq.heappop(pending)
            yield Delivery(due, p)

    while pending:
        due, _, p = heapq.heappop(pending)
        yield Delivery(due, p)
