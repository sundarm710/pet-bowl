from datetime import UTC, datetime, timedelta

from petbowl.common.events import Event
from petbowl.simulator.device import make_fleet, stream

START = datetime(2026, 1, 1, tzinfo=UTC)


def run(n: int, seed: int, hours: int = 24) -> list[Event]:
    return list(stream(make_fleet(n, seed, START), START + timedelta(hours=hours)))


def of(events: list[Event], device_id: str) -> list[Event]:
    return [e for e in events if e.device_id == device_id]


def test_deterministic_per_seed() -> None:
    assert run(5, 42) == run(5, 42)
    assert run(5, 42) != run(5, 43)


def test_device_stream_independent_of_fleet_size() -> None:
    assert of(run(5, 42), "dev-00003") == of(run(50, 42), "dev-00003")


def test_global_time_order() -> None:
    ts = [e.ts for e in run(20, 1)]
    assert ts == sorted(ts)


def test_seq_dense_per_device() -> None:
    evs = run(10, 1)
    for i in range(10):
        seqs = [e.seq for e in of(evs, f"dev-{i:05d}")]
        assert seqs == list(range(len(seqs)))


def test_value_invariants() -> None:
    evs = run(10, 7, hours=48)
    assert all(e.value is not None and e.value >= 0 for e in evs if e.kind != "heartbeat")
    assert all(e.value is None for e in evs if e.kind == "heartbeat")
    for i in range(10):
        bat = [e.value for e in of(evs, f"dev-{i:05d}") if e.kind == "battery_pct"]
        assert bat == sorted(bat, reverse=True)  # type: ignore[type-var]


def test_pets_actually_eat() -> None:
    w = [e.value for e in of(run(3, 5), "dev-00000") if e.kind == "weight_g"]
    assert max(w) - min(w) > 25  # type: ignore[type-var]


def test_event_rate_per_device_day() -> None:
    n = len(of(run(5, 9), "dev-00002"))
    assert 4000 < n < 5200  # expect ~
