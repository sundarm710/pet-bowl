import os
import tempfile
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from petbowl.kv import wal
from petbowl.kv.record import MAX_BODY, OP_PUT, Record, encode
from petbowl.kv.store import KV
from petbowl.kv.wal import WalError


def test_put_get_delete(tmp_path: Path) -> None:
    with KV(tmp_path / "kv.wal") as kv:
        assert kv.get(b"a") is None
        kv.put(b"a", b"1")
        assert kv.get(b"a") == b"1"
        kv.put(b"a", b"2")
        assert kv.get(b"a") == b"2"
        assert kv.delete(b"a") is True
        assert kv.get(b"a") is None
        assert kv.delete(b"a") is False
        assert len(kv) == 0


def test_state_survives_reopen(tmp_path: Path) -> None:
    p = tmp_path / "kv.wal"
    with KV(p) as kv:
        kv.put(b"a", b"1")
        kv.put(b"b", b"2")
        kv.put(b"a", b"3")
        kv.delete(b"b")
    with KV(p) as kv:
        assert dict(kv.items()) == {b"a": b"3"}
        assert kv.recovery.stop == "eof"


def test_deleting_a_missing_key_writes_nothing(tmp_path: Path) -> None:
    p = tmp_path / "kv.wal"
    with KV(p) as kv:
        kv.put(b"a", b"1")
        size = p.stat().st_size
        assert kv.delete(b"zzz") is False
        assert p.stat().st_size == size


def test_open_repairs_a_torn_tail(tmp_path: Path) -> None:
    p = tmp_path / "kv.wal"
    good = encode(Record(OP_PUT, b"a", b"1")) + encode(Record(OP_PUT, b"b", b"2"))
    torn = encode(Record(OP_PUT, b"c", b"3"))[:-2]
    p.write_bytes(good + torn)
    with KV(p) as kv:
        assert dict(kv.items()) == {b"a": b"1", b"b": b"2"}
        assert kv.recovery.stop == "truncated"
        assert kv.recovery.discarded == len(torn)
        kv.put(b"d", b"4")
    with KV(p) as kv:  # the new write is reachable because the tail was cut
        assert dict(kv.items()) == {b"a": b"1", b"b": b"2", b"d": b"4"}
        assert kv.recovery.stop == "eof"


OPS = [
    ("put", b"a", b"1"),
    ("put", b"b", b"2"),
    ("put", b"a", b"3"),
    ("delete", b"b", b""),
    ("delete", b"zz", b""),  # no-op, never logged
    ("put", b"c", b"x" * 30),
    ("put", b"b", b""),
    ("delete", b"a", b""),
    ("put", b"d", b"4"),
]


def test_every_cut_matches_the_model(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # logic test, not a durability test: skip the slow real fsync in recovery
    monkeypatch.setattr(wal, "_fsync", lambda fd: None)
    src = tmp_path / "src.wal"
    model: dict[bytes, bytes] = {}
    states: list[dict[bytes, bytes]] = [{}]  # model after each logged op
    ends: list[int] = [0]  # file size after each logged op
    with KV(src, "never") as kv:
        for op, k, v in OPS:
            if op == "put":
                kv.put(k, v)
                model[k] = v
            else:
                if not kv.delete(k):
                    continue
                del model[k]
            states.append(dict(model))
            ends.append(src.stat().st_size)
    data = src.read_bytes()

    p = tmp_path / "cut.wal"
    for cut in range(len(data) + 1):
        p.write_bytes(data[:cut])
        n = max(i for i, e in enumerate(ends) if e <= cut)
        with KV(p) as kv:
            assert dict(kv.items()) == states[n], cut


KEYS = st.sampled_from([b"a", b"b", b"c", b"d"])  # few keys, so overwrites are common
OP_LISTS = st.lists(
    st.one_of(
        st.tuples(st.just("put"), KEYS, st.binary(max_size=20)),
        st.tuples(st.just("delete"), KEYS, st.just(b"")),
    ),
    max_size=40,
)


@settings(deadline=None)
@given(ops=OP_LISTS)
def test_matches_a_plain_dict_across_reopen(ops: list[tuple[str, bytes, bytes]]) -> None:
    model: dict[bytes, bytes] = {}
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "kv.wal"
        with KV(p, "never") as kv:
            for op, k, v in ops:
                if op == "put":
                    kv.put(k, v)
                    model[k] = v
                else:
                    assert kv.delete(k) == (k in model)
                    model.pop(k, None)
                assert kv.get(k) == model.get(k)
        with KV(p) as kv:
            assert dict(kv.items()) == model


def test_a_second_opener_is_refused(tmp_path: Path) -> None:
    p = tmp_path / "kv.wal"
    with KV(p), pytest.raises(WalError):
        KV(p)
    KV(p).close()  # the lock was released on close


def test_a_failed_write_never_reaches_the_dict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kv = KV(tmp_path / "kv.wal", "never")
    kv.put(b"a", b"1")

    def boom(fd: int, data: object) -> int:
        raise OSError("disk full")

    with monkeypatch.context() as m:
        m.setattr(os, "write", boom)
        with pytest.raises(OSError):
            kv.put(b"a", b"2")
    assert kv.get(b"a") == b"1"
    with pytest.raises(WalError):
        kv.put(b"b", b"3")
    assert kv.get(b"a") == b"1"  # reads still work
    kv.close()


def test_oversized_value_is_rejected_before_anything_changes(tmp_path: Path) -> None:
    with KV(tmp_path / "kv.wal") as kv:
        with pytest.raises(ValueError):
            kv.put(b"a", b"x" * MAX_BODY)
        assert kv.get(b"a") is None
        kv.put(b"a", b"ok")  # still usable
        assert kv.get(b"a") == b"ok"
