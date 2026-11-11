from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from petbowl.simulator.faults import Delivery
from petbowl.simulator.pace import PaceStats, pace

START = datetime(2026, 1, 1, tzinfo=UTC)


class FakeClock:
    def __init__(self) -> None:
        self.t = 100.0

    def now(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.t += s


def at(ms: int) -> Delivery:
    return Delivery(START + timedelta(milliseconds=ms), b"x")


def run(ds: list[Delivery], speedup: float, clk: FakeClock, **kw: Any) -> list[list[Delivery]]:
    return list(pace(ds, START, speedup, clock=clk.now, sleep=clk.sleep, **kw))


def test_unpaced_preserves_order_and_never_sleeps() -> None:
    clk = FakeClock()
    ds = [at(i) for i in range(1200)]
    batches = run(ds, 0, clk, batch_max=500)
    assert [d for b in batches for d in b] == ds
    assert [len(b) for b in batches] == [500, 500, 200]
    assert clk.t == 100.0


def test_paced_follows_schedule_without_drift() -> None:
    clk = FakeClock()
    ds = [at(1000 * i) for i in range(10)]  # one per virtual second
    batches = run(ds, 10, clk)  # 10x: 0.1s apart
    assert len(batches) == 10
    assert clk.t - 100.0 == pytest.approx(0.9)


def test_burst_is_not_throttled() -> None:
    clk = FakeClock()
    ds = [at(5000)] * 1000  # offline flush: same instant
    batches = run(ds, 1, clk, batch_max=500)
    assert [len(b) for b in batches] == [500, 500]
    assert clk.t - 100.0 == pytest.approx(5.0)  # waited once, then no more


def test_close_deliveries_share_a_batch() -> None:
    clk = FakeClock()
    ds = [at(10 * i) for i in range(12)]  # 10 ms apart
    batches = run(ds, 1, clk, window_s=0.025)
    assert [len(b) for b in batches] == [3, 3, 3, 3]


def test_lag_is_reported_when_consumer_is_slow() -> None:
    clk = FakeClock()
    st = PaceStats()
    ds = [at(1000 * i) for i in range(5)]
    for _ in pace(ds, START, 1, st, clock=clk.now, sleep=clk.sleep):
        clk.t += 5  # a sink slower than real time
    assert st.max_lag_s > 3
    assert st.items == 5
