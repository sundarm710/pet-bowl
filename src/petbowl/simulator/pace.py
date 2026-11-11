# src/petbowl/simulator/pace.py
"""Release deliveries on a wall clock, in batches, at virtual-time speed."""

import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime

from petbowl.simulator.faults import Delivery


@dataclass
class PaceStats:
    items: int = 0
    batches: int = 0
    # worst (actual release - due): the generator or the sink falling behind
    max_lag_s: float = 0.0


def pace(
    items: Iterable[Delivery],
    virtual_start: datetime,
    speedup: float,
    stats: PaceStats | None = None,
    *,
    batch_max: int = 500,
    window_s: float = 0.02,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[list[Delivery]]:
    """speedup = virtual seconds per wall second. 0 means unpaced (max throughput)."""
    st = stats if stats is not None else PaceStats()
    unpaced = speedup <= 0
    wall_start = clock()

    def due_of(d: Delivery) -> float:
        if unpaced:
            return 0.0
        return wall_start + (d.at - virtual_start).total_seconds() / speedup

    def release(batch: list[Delivery], due: float) -> list[Delivery]:
        if not unpaced:
            wait = due - clock()
            if wait > 0:
                sleep(wait)
            else:
                st.max_lag_s = max(st.max_lag_s, -wait)
        st.items += len(batch)
        st.batches += 1
        return batch

    batch: list[Delivery] = []
    first_due = last_due = 0.0
    for d in items:
        due = due_of(d)
        if batch and (len(batch) >= batch_max or due - first_due > window_s):
            yield release(batch, last_due)
            batch = []
        if not batch:
            first_due = due
        batch.append(d)
        last_due = due
    if batch:
        yield release(batch, last_due)
