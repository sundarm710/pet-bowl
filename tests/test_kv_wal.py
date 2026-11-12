import os
import tempfile
from itertools import accumulate
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from petbowl.kv import wal
from petbowl.kv.record import OP_DELETE, OP_PUT, Record, encode
from petbowl.kv.wal import Replay, WalError, WalWriter, recover, replay


def put(i: int, n: int = 0) -> Record:
    return Record(OP_PUT, f"dev-{i:05d}".encode(), b"v" * n)


RECORDS = [put(i, i * 7 % 40) for i in range(12)] + [Record(OP_DELETE, b"dev-00003")]
ENCODED = [encode(r) for r in RECORDS]
ENDS = list(accumulate(len(e) for e in ENCODED))
DATA = b"".join(ENCODED)


def run_replay(path: Path) -> tuple[list[Record], Replay]:
    out: list[Record] = []
    return out, replay(path, out.append)


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def count_fsyncs(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    calls: list[int] = []
    monkeypatch.setattr(wal, "_fsync", calls.append)
    return calls


def test_writer_then_replay_round_trip(tmp_path: Path) -> None:
    p = tmp_path / "wal.log"
    with WalWriter(p, "never") as w:
        for r in RECORDS:
            w.append(r)
    recs, res = run_replay(p)
    assert recs == RECORDS
    assert res == Replay(len(DATA), len(DATA), "eof")
    assert p.read_bytes() == DATA


def test_always_syncs_every_append(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = count_fsyncs(monkeypatch)
    with WalWriter(tmp_path / "wal.log", "always") as w:
        for r in RECORDS[:5]:
            w.append(r)
    assert len(calls) == 5  # nothing left dirty at close


def test_never_syncs_only_on_close(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = count_fsyncs(monkeypatch)
    with WalWriter(tmp_path / "wal.log", "never") as w:
        for r in RECORDS[:5]:
            w.append(r)
        assert calls == []
    assert len(calls) == 1


def test_interval_syncs_when_due(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = count_fsyncs(monkeypatch)
    clk = Clock()
    with WalWriter(tmp_path / "wal.log", "interval", interval_s=1.0, clock=clk) as w:
        clk.t = 0.5
        w.append(RECORDS[0])
        assert calls == []
        clk.t = 1.0
        w.append(RECORDS[1])
        assert len(calls) == 1
        clk.t = 1.4
        w.append(RECORDS[2])
        assert len(calls) == 1
    assert len(calls) == 2  # close flushes the dirty tail


def test_every_cut_recovers_the_longest_valid_prefix(tmp_path: Path) -> None:
    p = tmp_path / "wal.log"
    for cut in range(len(DATA) + 1):
        p.write_bytes(DATA[:cut])
        recs, res = run_replay(p)
        n = sum(1 for e in ENDS if e <= cut)
        assert recs == RECORDS[:n], cut
        assert res.valid_end == (ENDS[n - 1] if n else 0), cut
        assert res.stop == ("eof" if cut in (0, *ENDS) else "truncated"), cut


def test_a_flipped_bit_stops_replay_at_that_record(tmp_path: Path) -> None:
    p = tmp_path / "wal.log"
    k = 5
    start = ENDS[k - 1]
    for i in range(start * 8, ENDS[k] * 8):
        bad = bytearray(DATA)
        bad[i // 8] ^= 1 << (i % 8)
        p.write_bytes(bytes(bad))
        recs, res = run_replay(p)
        assert recs == RECORDS[:k], i
        assert res.valid_end == start, i
        assert res.stop in ("truncated", "corrupt"), i


def test_zero_filled_tail_is_discarded(tmp_path: Path) -> None:
    p = tmp_path / "wal.log"
    p.write_bytes(DATA + bytes(100))
    recs, res = run_replay(p)
    assert recs == RECORDS
    assert (res.valid_end, res.stop, res.discarded) == (len(DATA), "corrupt", 100)


def test_missing_file_is_an_empty_log(tmp_path: Path) -> None:
    recs, res = run_replay(tmp_path / "nope.log")
    assert recs == []
    assert res == Replay(0, 0, "eof")


def test_recover_cuts_the_torn_tail_so_new_appends_are_reachable(tmp_path: Path) -> None:
    p = tmp_path / "wal.log"
    p.write_bytes(DATA + ENCODED[0][:7])  # a half-written record
    got: list[Record] = []
    res = recover(p, got.append)
    assert got == RECORDS
    assert res.stop == "truncated"
    assert p.stat().st_size == len(DATA)
    with WalWriter(p, "always") as w:
        w.append(put(99))
    recs, res2 = run_replay(p)
    assert recs == [*RECORDS, put(99)]
    assert res2.stop == "eof"


def test_without_recover_a_record_appended_after_garbage_is_lost(tmp_path: Path) -> None:
    p = tmp_path / "wal.log"
    p.write_bytes(DATA + ENCODED[0][:7])
    with WalWriter(p, "always") as w:
        w.append(put(99))
    recs, _ = run_replay(p)
    assert put(99) not in recs  # why recover() must truncate first


def test_writer_refuses_appends_after_a_write_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    w = WalWriter(tmp_path / "wal.log", "never")

    def boom(fd: int, data: object) -> int:
        raise OSError("disk full")

    with monkeypatch.context() as m:
        m.setattr(os, "write", boom)
        with pytest.raises(OSError):
            w.append(put(1))
    with pytest.raises(WalError):
        w.append(put(2))
    w.close()


def test_fsync_failure_is_never_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(fd: int) -> None:
        raise OSError("EIO")

    monkeypatch.setattr(wal, "_fsync", boom)
    w = WalWriter(tmp_path / "wal.log", "always")
    with pytest.raises(OSError):
        w.append(put(1))
    with pytest.raises(WalError):
        w.append(put(2))
    w.close()


records = st.builds(
    Record, op=st.just(OP_PUT), key=st.binary(max_size=40), value=st.binary(max_size=200)
)


@settings(deadline=None)
@given(recs=st.lists(records, max_size=30), cut_frac=st.floats(0, 1))
def test_random_logs_recover_their_valid_prefix(recs: list[Record], cut_frac: float) -> None:
    enc = [encode(r) for r in recs]
    ends = list(accumulate(len(e) for e in enc))
    data = b"".join(enc)
    cut = int(len(data) * cut_frac)
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "wal.log"
        p.write_bytes(data[:cut])
        out: list[Record] = []
        res = replay(p, out.append)
    n = sum(1 for e in ends if e <= cut)
    assert out == recs[:n]
    assert res.valid_end == (ends[n - 1] if n else 0)
