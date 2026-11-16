import time
import uuid
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer


def make_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
        }
    )


def make_consumer(bootstrap: str, group: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def drain(c: Consumer, n: int, timeout_s: float = 20.0) -> list[Message]:
    out: list[Message] = []
    deadline = time.monotonic() + timeout_s
    while len(out) < n and time.monotonic() < deadline:
        msg = c.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(msg.error())
        out.append(msg)
    return out


def produce_ordered(bootstrap: str, topic: str, n: int, key: str = "dev-000") -> None:
    p = make_producer(bootstrap)
    for i in range(n):
        p.produce(topic, key=key, value=str(i).encode())
        p.poll(0)
    assert p.flush(20) == 0


def test_key_keeps_partition_and_order(kafka_bootstrap, topic):
    p = make_producer(kafka_bootstrap)
    acks: list[tuple[object, int, int]] = []

    def on_delivery(err, msg):
        acks.append((err, msg.partition(), msg.offset()))

    keys = [f"dev-{i:03d}" for i in range(20)]
    sent = 0
    for seq in range(15):
        for k in keys:
            p.produce(topic, key=k, value=str(seq).encode(), on_delivery=on_delivery)
            sent += 1
        p.poll(0)
    assert p.flush(20) == 0
    assert len(acks) == sent
    assert all(err is None for err, _, _ in acks)

    c = make_consumer(kafka_bootstrap, f"g_{uuid.uuid4().hex[:8]}")
    c.subscribe([topic])
    try:
        msgs = drain(c, sent)
    finally:
        c.close()
    assert len(msgs) == sent

    parts: dict[str, set[int]] = defaultdict(set)
    seqs: dict[str, list[int]] = defaultdict(list)
    for m in msgs:
        k = m.key().decode()
        parts[k].add(m.partition())
        seqs[k].append(int(m.value()))
    assert all(len(v) == 1 for v in parts.values())
    assert all(v == list(range(15)) for v in seqs.values())
    assert len({next(iter(v)) for v in parts.values()}) > 1


def test_committed_offset_resumes(kafka_bootstrap, topic):
    produce_ordered(kafka_bootstrap, topic, 30)
    group = f"g_{uuid.uuid4().hex[:8]}"

    c1 = make_consumer(kafka_bootstrap, group)
    c1.subscribe([topic])
    first = drain(c1, 10)
    assert [int(m.value()) for m in first] == list(range(10))
    c1.commit(message=first[-1], asynchronous=False)
    c1.close()

    c2 = make_consumer(kafka_bootstrap, group)
    c2.subscribe([topic])
    rest = drain(c2, 20)
    c2.close()
    assert [int(m.value()) for m in rest] == list(range(10, 30))


def test_uncommitted_is_redelivered(kafka_bootstrap, topic):
    produce_ordered(kafka_bootstrap, topic, 30)
    group = f"g_{uuid.uuid4().hex[:8]}"

    c1 = make_consumer(kafka_bootstrap, group)
    c1.subscribe([topic])
    first = drain(c1, 10)
    assert len(first) == 10
    c1.close()  # no commit

    c2 = make_consumer(kafka_bootstrap, group)
    c2.subscribe([topic])
    again = drain(c2, 10)
    c2.close()
    assert [int(m.value()) for m in again] == list(range(10))
