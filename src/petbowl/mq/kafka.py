from __future__ import annotations

import asyncio
import contextlib
import threading
from collections.abc import Sequence
from typing import Any

from confluent_kafka import Consumer as KConsumer
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka import Producer as KProducer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from petbowl.mq.base import ConsumeError, Message, PublishError, QueueError, next_offsets


def create_topic(bootstrap: str, name: str, partitions: int, timeout_s: float = 10.0) -> None:
    """Idempotent. Auto topic creation is off, so the app creates what it needs."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    fut = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])[
        name
    ]
    try:
        fut.result(timeout_s)
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise QueueError(str(exc)) from exc


def _resolve(fut: asyncio.Future[None], exc: Exception | None) -> None:
    if fut.done():
        return
    if exc is None:
        fut.set_result(None)
    else:
        fut.set_exception(exc)


class KafkaPublisher:
    """Idempotent, acks=all. publish() resolves when the broker has acked every item."""

    def __init__(
        self,
        bootstrap: str,
        *,
        linger_ms: int = 5,
        message_timeout_ms: int = 10_000,
    ) -> None:
        self._p = KProducer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": linger_ms,
                "message.timeout.ms": message_timeout_ms,
            }
        )
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="kafka-poll", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            self._p.poll(0.05)

    @staticmethod
    def _callback(loop: asyncio.AbstractEventLoop, fut: asyncio.Future[None]) -> Any:
        def on_delivery(err: Any, _msg: Any) -> None:
            exc = PublishError(str(err)) if err is not None else None
            with contextlib.suppress(RuntimeError):  # loop already closed
                loop.call_soon_threadsafe(_resolve, fut, exc)

        return on_delivery

    async def publish(self, topic: str, items: Sequence[tuple[bytes, bytes]]) -> None:
        loop = asyncio.get_running_loop()
        futs: list[asyncio.Future[None]] = []
        produce_error: Exception | None = None
        for key, value in items:
            fut: asyncio.Future[None] = loop.create_future()
            try:
                self._p.produce(topic, key=key, value=value, on_delivery=self._callback(loop, fut))
            except (BufferError, KafkaException) as exc:
                produce_error = exc
                break
            futs.append(fut)
        results = await asyncio.gather(*futs, return_exceptions=True)
        if produce_error is not None:
            raise PublishError(f"produce failed: {produce_error}") from produce_error
        failures = [r for r in results if isinstance(r, BaseException)]
        if failures:
            raise PublishError(
                f"{len(failures)} of {len(futs)} \
            failed: {failures[0]}"
            ) from failures[0]

    async def close(self, timeout_s: float = 10.0) -> None:
        remaining = await asyncio.to_thread(self._p.flush, timeout_s)
        self._stop.set()
        await asyncio.to_thread(self._thread.join, 2.0)
        if remaining:
            raise PublishError(f"{remaining} messages unsent at close")


class KafkaConsumer:
    """Group member with manual commits. Auto commit is off on purpose."""

    def __init__(
        self,
        bootstrap: str,
        group: str,
        topics: Sequence[str],
        *,
        session_timeout_ms: int = 10_000,
    ) -> None:
        self._c = KConsumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": session_timeout_ms,
            }
        )
        self._c.subscribe(list(topics))

    def poll(self, max_messages: int, timeout_s: float) -> list[Message]:
        try:
            raw = self._c.consume(num_messages=max_messages, timeout=timeout_s)
        except KafkaException as exc:
            raise ConsumeError(str(exc)) from exc
        out: list[Message] = []
        for m in raw:
            err = m.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise ConsumeError(str(err))
            topic, partition, offset = m.topic(), m.partition(), m.offset()
            if topic is None or partition is None or offset is None:
                raise ConsumeError("message without a position")
            out.append(Message(topic, partition, offset, m.key() or b"", m.value() or b""))
        return out

    def commit(self, messages: Sequence[Message]) -> None:
        offsets = next_offsets(messages)
        if not offsets:
            return
        try:
            self._c.commit(
                offsets=[TopicPartition(t, p, o) for (t, p), o in offsets.items()],
                asynchronous=False,
            )
        except KafkaException as exc:
            raise ConsumeError(str(exc)) from exc

    def close(self) -> None:
        self._c.close()
