import struct

import pytest
from hypothesis import given
from hypothesis import strategies as st

from petbowl.kv.record import (
    MAX_BODY,
    OP_DELETE,
    OP_PUT,
    CorruptRecord,
    Record,
    RecordError,
    TruncatedRecord,
    decode,
    encode,
)

SAMPLE = [
    Record(OP_PUT, b"dev-00001", b'{"max_seq": 41}'),
    Record(OP_PUT, b"", b""),
    Record(OP_DELETE, b"dev-00001"),
]


@pytest.mark.parametrize("rec", SAMPLE)
def test_round_trip(rec: Record) -> None:
    buf = encode(rec)
    assert decode(buf) == (rec, len(buf))


@given(
    key=st.binary(max_size=300),
    value=st.binary(max_size=1000),
    op=st.sampled_from([OP_PUT, OP_DELETE]),
)
def test_round_trip_any_bytes(key: bytes, value: bytes, op: int) -> None:
    rec = Record(op, key, b"" if op == OP_DELETE else value)
    buf = encode(rec)
    assert decode(buf) == (rec, len(buf))


def test_records_decode_back_to_back() -> None:
    buf = b"".join(encode(r) for r in SAMPLE)
    out, off = [], 0
    while off < len(buf):
        rec, off = decode(buf, off)
        out.append(rec)
    assert out == SAMPLE
    assert off == len(buf)


def test_every_truncation_is_detected() -> None:
    buf = encode(SAMPLE[0])
    for cut in range(len(buf)):
        with pytest.raises(TruncatedRecord):
            decode(buf[:cut])


def test_every_single_bit_flip_is_detected() -> None:
    buf = encode(SAMPLE[0])
    for i in range(len(buf) * 8):
        bad = bytearray(buf)
        bad[i // 8] ^= 1 << (i % 8)
        with pytest.raises(RecordError):
            decode(bytes(bad))


def test_zero_filled_tail_is_not_a_valid_record() -> None:
    with pytest.raises(CorruptRecord):
        decode(bytes(64))


def test_absurd_length_is_corrupt_not_truncated() -> None:
    bad = struct.pack("<II", 0, 0xFFFFFFFF) + b"x" * 10
    with pytest.raises(CorruptRecord):
        decode(bad)


def test_encode_rejects_what_decode_would_reject() -> None:
    with pytest.raises(ValueError):
        encode(Record(99, b"k"))
    with pytest.raises(ValueError):
        encode(Record(OP_DELETE, b"k", b"v"))
    with pytest.raises(ValueError):
        encode(Record(OP_PUT, b"k", b"x" * MAX_BODY))
