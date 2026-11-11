from collections import defaultdict
from contextlib import suppress
from datetime import UTC, datetime, timedelta

from hypothesis import given, settings
from hypothesis import strategies as st

from petbowl.common.events import Event
from petbowl.simulator.device import make_fleet, stream
from petbowl.simulator.faults import Delivery, FaultConfig, apply_faults
from petbowl.simulator.pace import pace

START = datetime(2026, 1, 1, tzinfo=UTC)
SLOW = settings(max_examples=25, deadline=None)  # each example runs a simulation


class FakeClock:
    def __init__(self) -> None:
        self.t = 100.0

    def now(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.t += s


def clean(n: int, seed: int) -> list[Event]:
    return list(stream(make_fleet(n, seed, START), START + timedelta(hours=1)))


def faulted(n: int, seed: int, cfg: FaultConfig) -> list[Delivery]:
    events = stream(make_fleet(n, seed, START), START + timedelta(hours=1))
    return list(apply_faults(events, cfg))


def parse(ds: list[Delivery]) -> list[tuple[Delivery, Event]]:
    out = []
    for d in ds:
        with suppress(ValueError):  # malformed on purpose
            out.append((d, Event.model_validate_json(d.payload)))
    return out


fleets = st.tuples(st.integers(1, 4), st.integers(0, 10_000))  # (devices, seed)


def configs(max_malformed: float) -> st.SearchStrategy[FaultConfig]:
    return st.builds(
        FaultConfig,
        seed=st.integers(0, 10_000),
        dup_rate=st.floats(0.0, 0.3),
        ooo_rate=st.floats(0.0, 0.3),
        offline_rate=st.floats(0.0, 0.01),
        skew_s=st.floats(0.0, 60.0),
        malformed_rate=st.floats(0.0, max_malformed),
    )


@SLOW
@given(seed=st.integers(0, 10_000), n=st.integers(2, 8))
def test_device_invariants_hold_for_any_seed(seed: int, n: int) -> None:
    evs = clean(n, seed)
    by_dev: dict[str, list[Event]] = defaultdict(list)
    for e in evs:
        by_dev[e.device_id].append(e)
    for es in by_dev.values():
        assert [e.seq for e in es] == list(range(len(es)))
        assert [e.ts for e in es] == sorted(e.ts for e in es)
        assert all(e.value is not None and e.value >= 0 for e in es if e.kind != "heartbeat")
        bat = [e.value for e in es if e.kind == "battery_pct"]
        assert bat == sorted(bat, reverse=True)  # type: ignore[type-var]
    assert clean(1, seed) == by_dev["dev-00000"]  # isolation from fleet size


@SLOW
@given(fleet=fleets, cfg=configs(0.3))
def test_faults_never_invent_or_time_travel(fleet: tuple[int, int], cfg: FaultConfig) -> None:
    n, seed = fleet
    true_ts = {e.event_id: e.ts for e in clean(n, seed)}
    ds = faulted(n, seed, cfg)
    assert [d.at for d in ds] == sorted(d.at for d in ds)
    max_lag = timedelta(seconds=cfg.offline_max_s + cfg.ooo_max_delay_s + 6)
    for d, e in parse(ds):
        assert e.event_id in true_ts  # nothing invented
        assert timedelta(0) <= d.at - true_ts[e.event_id] <= max_lag


@SLOW
@given(fleet=fleets, cfg=configs(0.0))
def test_without_corruption_nothing_is_lost(fleet: tuple[int, int], cfg: FaultConfig) -> None:
    n, seed = fleet
    ids = {e.event_id for e in clean(n, seed)}
    ds = faulted(n, seed, cfg)
    parsed = parse(ds)
    assert len(parsed) == len(ds)
    payloads: dict[str, set[bytes]] = defaultdict(set)
    for d, e in parsed:
        payloads[e.event_id].add(d.payload)
    assert set(payloads) == ids  # every event arrived at least once
    assert all(len(p) == 1 for p in payloads.values())  # copies are byte-identical
    assert len(ds) <= 4 * len(ids)  # at most 3 extra copies each


@SLOW
@given(fleet=fleets, cfg=configs(0.3))
def test_faults_are_deterministic(fleet: tuple[int, int], cfg: FaultConfig) -> None:
    n, seed = fleet
    assert faulted(n, seed, cfg) == faulted(n, seed, cfg)


@SLOW
@given(
    offsets=st.lists(st.integers(0, 3_600_000), max_size=200).map(sorted),
    speedup=st.floats(0.5, 1000),
    batch_max=st.integers(1, 50),
)
def test_pace_keeps_order_and_is_never_early(
    offsets: list[int], speedup: float, batch_max: int
) -> None:
    clk = FakeClock()
    ds = [Delivery(START + timedelta(milliseconds=ms), b"x") for ms in offsets]
    out: list[Delivery] = []
    for b in pace(ds, START, speedup, batch_max=batch_max, clock=clk.now, sleep=clk.sleep):
        assert 0 < len(b) <= batch_max
        for d in b:
            due = 100.0 + (d.at - START).total_seconds() / speedup
            assert clk.t >= due - 1e-6  # never released early
            assert clk.t - due <= 0.02 + 1e-6  # at most one window late
        out.extend(b)
    assert out == ds