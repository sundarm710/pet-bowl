import io
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from petbowl.simulator.device import make_fleet, stream
from petbowl.simulator.faults import Delivery, FaultConfig, apply_faults
from petbowl.simulator.sinks import NullSink, StreamSink, make_sink

T = datetime(2026, 1, 1, tzinfo=UTC)


def batch(*payloads: bytes) -> list[Delivery]:
    return [Delivery(T, p) for p in payloads]


def test_null_counts_items_and_bytes() -> None:
    s = NullSink()
    s.write(batch(b"abc", b"de"))
    s.write(batch(b"f"))
    assert (s.items, s.nbytes) == (3, 6)


def test_stream_writes_one_line_per_payload() -> None:
    buf = io.BytesIO()
    s = StreamSink(buf, owns=False)
    s.write(batch(b'{"a":1}', b"garbage"))
    s.write(batch(b'{"b":2}'))
    assert buf.getvalue() == b'{"a":1}\ngarbage\n{"b":2}\n'


def test_empty_batch_writes_nothing() -> None:
    buf = io.BytesIO()
    StreamSink(buf, owns=False).write([])
    assert buf.getvalue() == b""


def test_jsonl_round_trip_creates_parent_dir(tmp_path: Path) -> None:
    p = tmp_path / "sub" / "out.jsonl"
    s = make_sink(f"jsonl:{p}")
    s.write(batch(b"x", b"y"))
    s.close()
    assert p.read_bytes() == b"x\ny\n"


def test_close_only_closes_owned_streams() -> None:
    owned, borrowed = io.BytesIO(), io.BytesIO()
    StreamSink(owned, owns=True).close()
    StreamSink(borrowed, owns=False).close()
    assert owned.closed and not borrowed.closed


def test_factory_kinds() -> None:
    assert isinstance(make_sink("null"), NullSink)
    assert isinstance(make_sink("stdout"), StreamSink)


@pytest.mark.parametrize("spec", ["", "null:x", "jsonl", "jsonl:", "http://x", "bogus"])
def test_bad_spec(spec: str) -> None:
    with pytest.raises(ValueError):
        make_sink(spec)


def test_fault_payloads_never_break_line_framing() -> None:
    evs = stream(make_fleet(5, 1, T), T + timedelta(hours=2))
    cfg = FaultConfig(seed=1, malformed_rate=0.3)
    assert all(b"\n" not in d.payload for d in apply_faults(evs, cfg))