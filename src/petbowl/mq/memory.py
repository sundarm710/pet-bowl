from __future__ import annotations

import zlib
from collections.abc import Sequence

from petbowl.mq.base import ConsumeError, Message, PublishError, next_offsets


class MemoryBroker:
    """Partitioned logs and committed offsets, all in RAM."""

    def __init__(self) -> None:
        self._topics: dict[str, list[list[Message]]] = {}
        self._committed: dict[tuple[str, str, int], int] = {}

    def create_topic(self, name: str, partitions: int) -> None:
        self._topics.setdefault(name, [[] for _ in range(partitions)])

    def has_topic(self, name: str) -> bool:
        return name in self._topics

    def partitions(self, topic: str) -> int:
        return len(self._topics[topic])

    def append(self, topic: str, key: bytes, value: bytes) -> None:
        parts = self._topics[topic]
        p = zlib.crc32(key) % len(parts)
        parts[p].append(Message(topic, p, len(parts[p]), key, value))

    def read(self, topic: str, partition: int, offset: int, limit: int) -> list[Message]:
        return self._topics[topic][partition][offset : offset + limit]

    def committed(self, group: str, topic: str, partition: int) -> int:
        return self._committed.get((group, topic, partition), 0)

    def set_committed(self, group: str, topic: str, partition: int, offset: int) -> None:
        self._committed[(group, topic, partition)] = offset


class MemoryPublisher:
    def __init__(self, broker: MemoryBroker) -> None:
        self._broker = broker
        self.fail = False

    async def publish(self, topic: str, items: Sequence[tuple[bytes, bytes]]) -> None:
        if self.fail:
            raise PublishError("injected failure")
        if not self._broker.has_topic(topic):
            raise PublishError(f"unknown topic {topic}")
        for key, value in items:
            self._broker.append(topic, key, value)

    async def close(self) -> None:
        return None


class MemoryConsumer:
    """One consumer owning every partition. No rebalance modelling."""

    def __init__(self, broker: MemoryBroker, group: str, topics: Sequence[str]) -> None:
        self._broker = broker
        self._group = group
        self._pos: dict[tuple[str, int], int] = {}
        self._closed = False
        for t in topics:
            if not broker.has_topic(t):
                raise ConsumeError(f"unknown topic {t}")
            for p in range(broker.partitions(t)):
                self._pos[(t, p)] = broker.committed(group, t, p)

    def poll(self, max_messages: int, timeout_s: float) -> list[Message]:
        if self._closed:
            raise ConsumeError("consumer closed")
        out: list[Message] = []
        for (t, p), pos in self._pos.items():
            if len(out) >= max_messages:
                break
            got = self._broker.read(t, p, pos, max_messages - len(out))
            self._pos[(t, p)] = pos + len(got)
            out.extend(got)
        return out

    def commit(self, messages: Sequence[Message]) -> None:
        if self._closed:
            raise ConsumeError("consumer closed")
        for (t, p), off in next_offsets(messages).items():
            self._broker.set_committed(self._group, t, p, off)

    def close(self) -> None:
        self._closed = True
