# src/petbowl/kv/store.py
"""A tiny durable key-value store: a dict in memory, a write-ahead log on disk."""

import fcntl
import os
from pathlib import Path
from typing import Self

from petbowl.kv.record import OP_DELETE, OP_PUT, Record
from petbowl.kv.wal import Replay, SyncPolicy, WalError, WalWriter, recover


def _lock(path: Path) -> int:
    """Exclusive advisory lock. The kernel drops it if the process dies."""
    fd = os.open(path.with_name(path.name + ".lock"), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        raise WalError(f"{path} is already open (another process or store)") from None
    return fd


class KV:
    def __init__(self, path: Path, policy: SyncPolicy = "always", interval_s: float = 0.05) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_fd = _lock(path)  # 1. lock, so we never repair a live file
        try:
            self._data: dict[bytes, bytes] = {}
            self.recovery: Replay = recover(path, self._apply)  # 2. rebuild + cut torn tail
            self._wal = WalWriter(path, policy, interval_s)  # 3. only now, append
        except BaseException:
            os.close(self._lock_fd)
            raise
        self._closed = False

    def _apply(self, rec: Record) -> None:
        if rec.op == OP_PUT:
            self._data[rec.key] = rec.value
        else:
            self._data.pop(rec.key, None)

    def get(self, key: bytes) -> bytes | None:
        return self._data.get(key)

    def put(self, key: bytes, value: bytes) -> None:
        self._wal.append(Record(OP_PUT, key, value))  # log first ...
        self._data[key] = value  # ... then memory

    def delete(self, key: bytes) -> bool:
        if key not in self._data:
            return False  # nothing changes, so nothing is logged
        self._wal.append(Record(OP_DELETE, key))
        del self._data[key]
        return True

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, key: bytes) -> bool:
        return key in self._data

    def items(self) -> list[tuple[bytes, bytes]]:
        return list(self._data.items())

    def sync(self) -> None:
        self._wal.sync()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._wal.close()
        finally:
            os.close(self._lock_fd)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
