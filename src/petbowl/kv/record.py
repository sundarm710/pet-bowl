# src/petbowl/kv/record.py
"""WAL record format: encode and decode, no I/O.

Layout (little-endian):
    crc32     u32   over the body_len bytes plus the body
    body_len  u32
    body:
        op        u8    1 = PUT, 2 = DELETE
        key_len   u32
        key       key_len bytes
        value     the rest (empty for DELETE)
"""

import struct
import zlib
from dataclasses import dataclass

OP_PUT = 1
OP_DELETE = 2

HEADER = struct.Struct("<II")  # crc32, body_len
BODY_PREFIX = struct.Struct("<BI")  # op, key_len
MAX_BODY = 16 * 1024 * 1024


class RecordError(Exception):
    def __init__(self, offset: int, reason: str) -> None:
        super().__init__(f"offset {offset}: {reason}")
        self.offset = offset


class TruncatedRecord(RecordError):
    """Not enough bytes. Expected at the tail after a crash mid-write."""


class CorruptRecord(RecordError):
    """Bytes are there but wrong. Unexpected unless a write was torn."""


@dataclass(frozen=True)
class Record:
    op: int
    key: bytes
    value: bytes = b""


def encode(rec: Record) -> bytes:
    if rec.op not in (OP_PUT, OP_DELETE):
        raise ValueError(f"unknown op {rec.op}")
    if rec.op == OP_DELETE and rec.value:
        raise ValueError("DELETE carries no value")
    body = BODY_PREFIX.pack(rec.op, len(rec.key)) + rec.key + rec.value
    if len(body) > MAX_BODY:
        raise ValueError("record too large")
    crc = zlib.crc32(struct.pack("<I", len(body)) + body)
    return HEADER.pack(crc, len(body)) + body


def decode(buf: bytes, offset: int = 0) -> tuple[Record, int]:
    """Decode one record at offset. Returns (record, offset of the next one)."""
    body_start = offset + HEADER.size
    if len(buf) < body_start:
        raise TruncatedRecord(offset, "incomplete header")
    crc, body_len = HEADER.unpack_from(buf, offset)
    if not BODY_PREFIX.size <= body_len <= MAX_BODY:
        raise CorruptRecord(offset, f"implausible length {body_len}")
    end = body_start + body_len
    if len(buf) < end:
        raise TruncatedRecord(offset, "incomplete body")
    body = buf[body_start:end]
    if zlib.crc32(buf[offset + 4 : offset + 8] + body) != crc:
        raise CorruptRecord(offset, "checksum mismatch")
    op, key_len = BODY_PREFIX.unpack_from(body)
    key_end = BODY_PREFIX.size + key_len
    if op not in (OP_PUT, OP_DELETE) or key_end > body_len:
        raise CorruptRecord(offset, "invalid body")
    value = body[key_end:]
    if op == OP_DELETE and value:
        raise CorruptRecord(offset, "DELETE with a value")
    return Record(op, body[BODY_PREFIX.size : key_end], value), end
