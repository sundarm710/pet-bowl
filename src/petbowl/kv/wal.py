# src/petbowl/kv/wal.py
"""Append-only write-ahead log: durable appends, and recovery of the valid prefix."""

import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Self

from petbowl.kv.record import CorruptRecord, Record, TruncatedRecord, decode, encode

SyncPolicy = Literal["always", "interval", "never"]
CHUNK = 1 << 20


class WalError(Exception):
    pass


def _fsync(fd: int) -> None:
    if sys.platform == "darwin":
        import fcntl

        try:  # plain fsync on macOS does not flush the drive's own cache
            fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
        except OSError:  # some filesystems do not support it
            os.fsync(fd)
    else:
        os.fsync(fd)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class WalWriter:
    def __init__(
        self,
        path: Path,
        policy: SyncPolicy = "always",
        interval_s: float = 0.05,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        existed = path.exists()
        self._fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            if not existed:
                _fsync_dir(path.parent)  # make the new file's directory entry durable
        except OSError:
            os.close(self._fd)
            raise
        self._policy = policy
        self._interval_s = interval_s
        self._clock = clock
        self._last_sync = clock()
        self._dirty = False
        self._failed = False
        self._closed = False

    def append(self, rec: Record) -> None:
        """Returns once the record is with the OS, and on disk if the policy says so."""
        if self._closed:
            raise WalError("writer is closed")
        if self._failed:
            raise WalError("an earlier write or fsync failed: reopen and recover first")
        data = memoryview(encode(rec))
        try:
            while data:  # os.write may write less than asked
                data = data[os.write(self._fd, data) :]
            self._dirty = True
            if self._policy == "always" or (
                self._policy == "interval" and self._clock() - self._last_sync >= self._interval_s
            ):
                self.sync()
        except OSError:
            self._failed = True
            raise

    def sync(self) -> None:
        if self._dirty:
            try:
                _fsync(self._fd)
            except OSError:
                # never retry: after a failed fsync the kernel may drop the dirty
                # pages, so a second fsync can report success for data that is gone
                self._failed = True
                raise
            self._dirty = False
        self._last_sync = self._clock()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if not self._failed:
                self.sync()
        finally:
            os.close(self._fd)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


@dataclass(frozen=True)
class Replay:
    valid_end: int  # byte offset just after the last good record
    file_size: int
    stop: Literal["eof", "truncated", "corrupt"]

    @property
    def discarded(self) -> int:
        return self.file_size - self.valid_end


def replay(path: Path, apply: Callable[[Record], None]) -> Replay:
    """Feed every valid record, in order, to apply. Stops at the first bad byte."""
    try:
        f = path.open("rb")
    except FileNotFoundError:
        return Replay(0, 0, "eof")
    with f:
        size = os.fstat(f.fileno()).st_size
        buf = b""
        pos = 0  # parse position inside buf
        base = 0  # file offset of buf[0]
        stop: Literal["eof", "truncated", "corrupt"] = "eof"
        while True:
            try:
                rec, pos = decode(buf, pos)
            except TruncatedRecord:
                chunk = f.read(CHUNK)
                if chunk:
                    buf = buf[pos:] + chunk
                    base += pos
                    pos = 0
                    continue
                stop = "truncated" if pos < len(buf) else "eof"
                break
            except CorruptRecord:
                stop = "corrupt"
                break
            apply(rec)
    return Replay(base + pos, size, stop)


def recover(path: Path, apply: Callable[[Record], None]) -> Replay:
    """Replay, then cut off any torn or corrupt tail so new appends start cleanly."""
    result = replay(path, apply)
    if result.discarded:
        with path.open("r+b") as f:
            f.truncate(result.valid_end)
            f.flush()
            _fsync(f.fileno())
    return result
