import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from helpers import line

from petbowl.common.events import Event
from petbowl.ingest.service import (
    BatchResult,
    Outcome,
    decode_state,
    process_batch,
    state_key,
)
from petbowl.kv.store import KV

TS = "2026-01-01T00:00:00Z"
TS_US = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp()) * 1_000_000


class FakeEvents:
    def __init__(self) -> None:
        self.keys: set[tuple[str, int]] = set()

    async def insert(self, events: Sequence[Event]) -> list[bool]:
        out: list[bool] = []
        for e in events:
            k = (e.device_id, e.seq)
            out.append(k not in self.keys)
            self.keys.add(k)
        return out


class FakeState:
    def __init__(self) -> None:
        self.data: dict[bytes, bytes] = {}
        self.puts = 0

    def get(self, key: bytes) -> bytes | None:
        return self.data.get(key)

    def put(self, key: bytes, value: bytes) -> None:
        self.data[key] = value
        self.puts += 1


def run(raw: list[bytes], events: FakeEvents, state: FakeState) -> BatchResult:
    return asyncio.run(process_batch(raw, events, state))


def details(res: BatchResult) -> list[tuple[Outcome, str]]:
    return [(r.outcome, r.detail) for r in res.items]


def test_in_order_batch_is_accepted_with_one_state_write() -> None:
    ev, st = FakeEvents(), FakeState()
    res = run([line("d1", 1), line("d1", 2), line("d1", 3)], ev, st)
    assert details(res) == [(Outcome.ACCEPTED, "")] * 3
    assert st.puts == 1  # one put per changed device per batch, not per event
    state = decode_state(st.data[state_key("d1")])
    assert (state.max_seq, state.max_ts_us) == (3, TS_US)


def test_a_jump_in_seq_is_reported_as_a_gap() -> None:
    ev, st = FakeEvents(), FakeState()
    run([line("d1", 1)], ev, st)
    res = run([line("d1", 4)], ev, st)
    assert details(res) == [(Outcome.ACCEPTED, "gap:2")]


def test_an_event_below_the_max_is_late() -> None:
    ev, st = FakeEvents(), FakeState()
    res = run([line("d1", 3), line("d1", 2)], ev, st)
    assert details(res) == [(Outcome.ACCEPTED, ""), (Outcome.ACCEPTED, "late")]
    assert decode_state(st.data[state_key("d1")]).max_seq == 3  # late events never lower it


def test_resending_a_batch_is_all_duplicates_and_writes_nothing() -> None:
    ev, st = FakeEvents(), FakeState()
    batch = [line("d1", 1), line("d1", 2)]
    run(batch, ev, st)
    res = run(batch, ev, st)
    assert details(res) == [(Outcome.DUPLICATE, "")] * 2
    assert st.puts == 1


def test_a_duplicate_inside_one_batch() -> None:
    res = run([line("d1", 1), line("d1", 1)], FakeEvents(), FakeState())
    assert [r.outcome for r in res.items] == [Outcome.ACCEPTED, Outcome.DUPLICATE]


def test_bad_items_are_rejected_with_their_index_and_never_stored() -> None:
    ev, st = FakeEvents(), FakeState()
    batch = [line("d1", 1), b"{not json", line("d1", 2, kind="nope"), line("d1", 3)]
    res = run(batch, ev, st)
    assert [(r.index, r.outcome) for r in res.items] == [
        (0, Outcome.ACCEPTED),
        (1, Outcome.REJECTED),
        (2, Outcome.REJECTED),
        (3, Outcome.ACCEPTED),
    ]
    assert all(r.detail for r in res.items if r.outcome is Outcome.REJECTED)
    assert ev.keys == {("d1", 1), ("d1", 3)}
    assert (res.count(Outcome.ACCEPTED), res.count(Outcome.REJECTED)) == (2, 1 + 1)


def test_a_retry_heals_a_kv_that_lagged_behind_postgres() -> None:
    ev, st = FakeEvents(), FakeState()
    ev.keys = {("d1", 1), ("d1", 2), ("d1", 3)}  # committed, but the KV write never happened
    res = run([line("d1", 1), line("d1", 2), line("d1", 3)], ev, st)
    assert [r.outcome for r in res.items] == [Outcome.DUPLICATE] * 3
    assert decode_state(st.data[state_key("d1")]).max_seq == 3


def test_each_device_gets_its_own_state() -> None:
    ev, st = FakeEvents(), FakeState()
    run([line("d1", 5), line("d2", 1)], ev, st)
    assert decode_state(st.data[state_key("d1")]).max_seq == 5
    assert decode_state(st.data[state_key("d2")]).max_seq == 1
    assert st.puts == 2


def test_works_with_the_real_kv_and_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "state.wal"
    ev = FakeEvents()
    with KV(path) as kv:
        asyncio.run(process_batch([line("d1", 1), line("d1", 2)], ev, kv))
    with KV(path) as kv:
        raw = kv.get(state_key("d1"))
        assert raw is not None
        assert decode_state(raw).max_seq == 2
        res = asyncio.run(process_batch([line("d1", 5)], ev, kv))
    assert details(res) == [(Outcome.ACCEPTED, "gap:2")]
