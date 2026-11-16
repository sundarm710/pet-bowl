from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol


class QueueError(Exception):
    pass


class PublishError(QueueError):
    pass


class ConsumeError(QueueError):
    pass


@dataclass(frozen=True, slots=True)
class Message:
    topic: str
    partition: int
    offset: int
    key: bytes
    value: bytes


class Publisher(Protocol):
    async def publish(self, topic: str, items: Sequence[tuple[bytes, bytes]]) -> None:
        """Return only when every item is durably stored (acks=all).

        Raise PublishError otherwise. Some items may still have been stored,
        so callers retry the whole batch and rely on idempotent writes.
        """

    async def close(self) -> None:
        """Flush and release resources."""


class Consumer(Protocol):
    def poll(self, max_messages: int, timeout_s: float) -> list[Message]:
        """Up to max_messages, waiting at most timeout_s. Empty list if none."""

    def commit(self, messages: Sequence[Message]) -> None:
        """Mark these messages, and everything before them in their partitions, as done."""

    def close(self) -> None:
        """Leave the group. Uncommitted messages will be redelivered."""


def next_offsets(messages: Sequence[Message]) -> dict[tuple[str, int], int]:
    """Offset to commit per (topic, partition): highest seen plus one."""
    out: dict[tuple[str, int], int] = {}
    for m in messages:
        k = (m.topic, m.partition)
        out[k] = max(out.get(k, 0), m.offset + 1)
    return out
