import asyncio
import time
import uuid
from collections import defaultdict
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import pytest

from petbowl.mq import (
    Consumer,
    MemoryBroker,
    MemoryConsumer,
    MemoryPublisher,
    Message,
    Publisher,
    PublishError,
    next_offsets,
)
from petbowl.mq.kafka import KafkaConsumer, KafkaPublisher, create_topic


@dataclass
class Backend:
    publisher: Publisher
    topic: str
    make_consumer: Callable[[str], Consumer]


@pytest.fixture(params=["memory", "kafka"])
def backend(request: pytest.FixtureRequest) -> Iterator[Backend]:
    if request.param == "memory":
        topic = f"t_{uuid.uuid4().hex[:12]}"
        broker = MemoryBroker()
        broker.create_topic(topic, 3)
        yield Backend(
            MemoryPublisher(broker),
            topic,
            lambda g: MemoryConsumer(broker, g, [topic]),
        )
        return
    bootstrap: str = request.getfixturevalue("kafka_bootstrap")
    topic = request.getfixturevalue("topic")
    pub = KafkaPublisher(bootstrap)
    yield Backend(pub, topic, lambda g: KafkaConsumer(bootstrap, g, [topic]))
    asyncio.run(pub.close())


def group() -> str:
    return f"g_{uuid.uuid4().hex[:8]}"


def publish(b: Backend, items: list[tuple[bytes, bytes]]) -> None:
    asyncio.run(b.publisher.publish(b.topic, items))


def poll_n(c: Consumer, n: int, timeout_s: float = 20.0) -> list[Message]:
    out: list[Message] = []
    deadline = time.monotonic() + timeout_s
    while len(out) < n and time.monotonic() < deadline:
        out.extend(c.poll(n - len(out), 0.5))
    return out


def test_per_key_order_and_partition(backend: Backend) -> None:
    keys = [f"dev-{i:03d}".encode() for i in range(20)]
    items = [(k, str(seq).encode()) for seq in range(15) for k in keys]
    publish(backend, items)

    c = backend.make_consumer(group())
    try:
        msgs = poll_n(c, len(items))
    finally:
        c.close()
    assert len(msgs) == len(items)

    parts: dict[bytes, set[int]] = defaultdict(set)
    seqs: dict[bytes, list[int]] = defaultdict(list)
    for m in msgs:
        parts[m.key].add(m.partition)
        seqs[m.key].append(int(m.value))
    assert all(len(v) == 1 for v in parts.values())
    assert all(v == list(range(15)) for v in seqs.values())
    assert len({next(iter(v)) for v in parts.values()}) > 1


def test_commit_resumes(backend: Backend) -> None:
    publish(backend, [(b"dev-000", str(i).encode()) for i in range(30)])
    g = group()

    c1 = backend.make_consumer(g)
    try:
        first = poll_n(c1, 10)
        c1.commit(first)
    finally:
        c1.close()

    c2 = backend.make_consumer(g)
    try:
        rest = poll_n(c2, 20)
    finally:
        c2.close()

    assert [int(m.value) for m in first] == list(range(10))
    assert [int(m.value) for m in rest] == list(range(10, 30))


def test_uncommitted_is_redelivered(backend: Backend) -> None:
    publish(backend, [(b"dev-000", str(i).encode()) for i in range(30)])
    g = group()

    c1 = backend.make_consumer(g)
    try:
        first = poll_n(c1, 10)
    finally:
        c1.close()

    c2 = backend.make_consumer(g)
    try:
        again = poll_n(c2, 10)
    finally:
        c2.close()

    assert [int(m.value) for m in first] == list(range(10))
    assert [int(m.value) for m in again] == list(range(10))


def test_commit_everything_leaves_nothing(backend: Backend) -> None:
    keys = [f"dev-{i:03d}".encode() for i in range(10)]
    publish(backend, [(k, b"x") for _ in range(6) for k in keys])
    g = group()

    c1 = backend.make_consumer(g)
    try:
        everything = poll_n(c1, 60)
        assert len(everything) == 60
        c1.commit(everything)
    finally:
        c1.close()

    publish(backend, [(b"dev-000", b"tail")])
    c2 = backend.make_consumer(g)
    try:
        got = poll_n(c2, 1)
        extra = c2.poll(10, 1.0)
    finally:
        c2.close()
    assert [m.value for m in got] == [b"tail"]
    assert extra == []


def test_empty_publish_is_noop(backend: Backend) -> None:
    publish(backend, [])


def test_memory_publish_failure() -> None:
    broker = MemoryBroker()
    broker.create_topic("t", 1)
    pub = MemoryPublisher(broker)
    pub.fail = True
    with pytest.raises(PublishError):
        asyncio.run(pub.publish("t", [(b"k", b"v")]))


def test_kafka_publish_times_out_when_broker_unreachable() -> None:
    pub = KafkaPublisher("127.0.0.1:1", message_timeout_ms=1500)
    start = time.monotonic()
    try:
        with pytest.raises(PublishError):
            asyncio.run(pub.publish("t", [(b"k", b"v")]))
        assert time.monotonic() - start < 10
    finally:
        asyncio.run(pub.close(2.0))


def test_create_topic_is_idempotent(kafka_bootstrap: str, topic: str) -> None:
    create_topic(kafka_bootstrap, topic, 3)


def test_next_offsets_takes_max_per_partition() -> None:
    ms = [
        Message("t", 0, 5, b"", b""),
        Message("t", 0, 2, b"", b""),
        Message("t", 1, 0, b"", b""),
    ]
    assert next_offsets(ms) == {("t", 0): 6, ("t", 1): 1}
