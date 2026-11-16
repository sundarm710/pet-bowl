from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from helpers import line  # MATCH: import it the way test_app.py does

from petbowl.ingest.queue_app import QueueSettings, create_queue_app
from petbowl.mq import MemoryBroker, MemoryConsumer, MemoryPublisher, Message

TOPIC = "t"


def nd(*items: str | bytes) -> bytes:
    return b"\n".join(x if isinstance(x, bytes) else x.encode() for x in items)


def drain(broker: MemoryBroker) -> list[Message]:
    return MemoryConsumer(broker, "g", [TOPIC]).poll(1000, 0)


@pytest.fixture
def rig() -> Iterator[tuple[TestClient, MemoryBroker, MemoryPublisher]]:
    broker = MemoryBroker()
    broker.create_topic(TOPIC, 3)
    pub = MemoryPublisher(broker)
    settings = QueueSettings(topic=TOPIC, max_items=4, max_body_bytes=2_000)
    with TestClient(create_queue_app(settings, publisher=pub)) as client:
        yield client, broker, pub


def test_accepts_and_publishes(rig) -> None:
    client, broker, _ = rig
    r = client.post("/v1/events", content=nd(line("dev-a", 1), line("dev-b", 1), line("dev-a", 2)))
    assert r.status_code == 200
    body = r.json()
    assert (body["accepted"], body["rejected"]) == (3, 0)

    msgs = drain(broker)
    assert sorted(m.key for m in msgs) == [b"dev-a", b"dev-a", b"dev-b"]
    a = [m.value for m in msgs if m.key == b"dev-a"]
    assert a == [nd(line("dev-a", 1)).strip(), nd(line("dev-a", 2)).strip()]


def test_bad_line_is_rejected_not_published(rig) -> None:
    client, broker, _ = rig
    r = client.post("/v1/events", content=nd(line("dev-a", 1), b'{"nope": 1}', line("dev-a", 2)))
    assert r.status_code == 200
    body = r.json()
    assert (body["accepted"], body["rejected"]) == (2, 1)
    assert body["rejects"][0]["index"] == 1
    assert len(drain(broker)) == 2


def test_all_invalid_batch_never_touches_the_queue(rig) -> None:
    client, broker, pub = rig
    pub.fail = True
    r = client.post("/v1/events", content=nd(b"nope", b'{"x": 1}'))
    assert r.status_code == 200
    body = r.json()
    assert (body["accepted"], body["rejected"]) == (0, 2)
    assert drain(broker) == []


def test_queue_failure_is_503_with_retry_after(rig) -> None:
    client, broker, pub = rig
    pub.fail = True
    r = client.post("/v1/events", content=nd(line("dev-a", 1)))
    assert r.status_code == 503
    assert r.headers["retry-after"] == "1"
    assert drain(broker) == []


@pytest.mark.parametrize("body", [b"", b"\n\n"])
def test_empty_batch_is_400(rig, body: bytes) -> None:
    client, _, _ = rig
    assert client.post("/v1/events", content=body).status_code == 400


def test_too_many_items_is_413(rig) -> None:
    client, _, _ = rig
    body = nd(*[line("dev-a", i) for i in range(5)])
    assert client.post("/v1/events", content=body).status_code == 413


def test_body_too_large_is_413(rig) -> None:
    client, _, _ = rig
    assert client.post("/v1/events", content=b"x" * 3000).status_code == 413
