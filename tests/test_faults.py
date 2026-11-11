from collections import defaultdict
from datetime import UTC, datetime, timedelta

from petbowl.common.events import Event
from petbowl.simulator.device import make_fleet, stream
from petbowl.simulator.faults import Delivery, FaultConfig, apply_faults

START = datetime(2026, 1, 1, tzinfo=UTC)


def clean() -> list[Event]:
    return list(stream(make_fleet(20, 42, START), START + timedelta(hours=6)))


def deliveries(cfg: FaultConfig) -> list[Delivery]:
    evs = stream(make_fleet(20, 42, START), START + timedelta(hours=6))
    return list(apply_faults(evs, cfg))


def parse(ds: list[Delivery]) -> tuple[list[Event], int]:
    good: list[Event] = []
    bad = 0
    for d in ds:
        try:
            good.append(Event.model_validate_json(d.payload))
        except ValueError:  # pydantic ValidationError subclasses ValueError
            bad += 1
    return good, bad


def ids(evs: list[Event]) -> set[str]:
    return {e.event_id for e in evs}


ALL = FaultConfig(
    seed=1, dup_rate=0.05, ooo_rate=0.05, offline_rate=0.001, skew_s=30, malformed_rate=0.02
)


def test_zero_faults_is_exact_passthrough() -> None:
    ds = deliveries(FaultConfig())
    good, bad = parse(ds)
    assert bad == 0 and good == clean()
    assert [d.at for d in ds] == [e.ts for e in good]


def test_deterministic() -> None:
    assert deliveries(ALL) == deliveries(ALL)


def test_delivery_order_is_time_order() -> None:
    ats = [d.at for d in deliveries(ALL)]
    assert ats == sorted(ats)


def test_duplicates() -> None:
    base = clean()
    good, bad = parse(deliveries(FaultConfig(seed=1, dup_rate=0.1)))
    assert bad == 0 and ids(good) == ids(base)
    assert 1.1 * len(base) < len(good) < 1.3 * len(base)  # expect 1.2x


def test_out_of_order_loses_nothing() -> None:
    good, _ = parse(deliveries(FaultConfig(seed=1, ooo_rate=0.05)))
    assert len(good) == len(clean())
    seqs: dict[str, list[int]] = defaultdict(list)
    for g in good:
        seqs[g.device_id].append(g.seq)
    assert any(s != sorted(s) for s in seqs.values())


def test_offline_bursts() -> None:
    ds = deliveries(FaultConfig(seed=1, offline_rate=0.001))
    good, bad = parse(ds)
    assert bad == 0 and ids(good) == ids(clean())
    lags = [d.at - Event.model_validate_json(d.payload).ts for d in ds]
    assert min(lags) >= timedelta(0)
    assert max(lags) >= timedelta(seconds=300)


def test_skew_is_constant_per_device() -> None:
    good, _ = parse(deliveries(FaultConfig(seed=1, skew_s=30)))
    offsets: dict[str, set[timedelta]] = defaultdict(set)
    for g, c in zip(good, clean(), strict=True):
        assert g.event_id == c.event_id
        offsets[g.device_id].add(g.ts - c.ts)
    assert all(len(o) == 1 for o in offsets.values())
    assert all(abs(next(iter(o))) <= timedelta(seconds=30) for o in offsets.values())


def test_malformed_rate() -> None:
    ds = deliveries(FaultConfig(seed=1, malformed_rate=0.05))
    good, bad = parse(ds)
    assert len(good) + bad == len(ds)
    assert 0.03 < bad / len(ds) < 0.07
    assert ids(good) <= ids(clean())
