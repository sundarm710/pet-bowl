"""Kafka node: read-only metrics for the ingest topic, plus a message peek.

Nothing here joins a consumer group or commits offsets, so the console never
disturbs real consumers. Watermarks come from a non-subscribed probe consumer,
and consumer-group lag from the admin API.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from petbowl.console.bus import Bus
from petbowl.console.nodes.base import BaseNode, RateWindow

TIMEOUT_S = 3.0
GROUP_EVERY_S = 3.0  # group lag is two admin round trips, so poll it less often
HISTORY = 60  # rate samples kept for the sparkline
PEEK_MAX = 50


@dataclass
class TopicStats:
    exists: bool = False
    partitions: dict[int, tuple[int, int]] = field(default_factory=dict)  # p -> (low, high)

    @property
    def end_total(self) -> int:
        return sum(high for _, high in self.partitions.values())

    @property
    def retained(self) -> int:
        return sum(high - low for low, high in self.partitions.values())


def read_topic(consumer: Consumer, topic: str) -> TopicStats:
    """Partition watermarks. Raises on broker errors so the caller can mark health down."""
    md = consumer.list_topics(topic, timeout=TIMEOUT_S)
    tmd = md.topics.get(topic)
    if tmd is None or tmd.error is not None:
        return TopicStats(exists=False)
    stats = TopicStats(exists=True)
    for p in tmd.partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=TIMEOUT_S)
        stats.partitions[p] = (low, high)
    return stats


def group_lags(admin: AdminClient, topic: str, stats: TopicStats) -> dict[str, dict[str, Any]]:
    """Lag per consumer group that has committed offsets on the topic."""
    listing = admin.list_consumer_groups(request_timeout=TIMEOUT_S).result(TIMEOUT_S + 1)
    ids = [g.group_id for g in listing.valid]
    if not ids:
        return {}
    futs = admin.list_consumer_group_offsets(
        [
            ConsumerGroupTopicPartitions(g, [TopicPartition(topic, p) for p in stats.partitions])
            for g in ids
        ],
        request_timeout=TIMEOUT_S,
    )
    out: dict[str, dict[str, Any]] = {}
    for gid, fut in futs.items():
        try:
            res = fut.result(TIMEOUT_S + 1)
        except Exception:  # one unreadable group must not hide the others
            continue
        committed = {tp.partition: tp.offset for tp in res.topic_partitions if tp.topic == topic}
        if not any(off >= 0 for off in committed.values()):
            continue  # not a consumer of this topic (nothing committed on any partition)
        per: dict[int, dict[str, int | None]] = {}
        for p, (low, high) in stats.partitions.items():
            off = committed.get(p, -1)
            if off < 0:  # no commit yet: consumers start at `earliest`, so everything is owed
                per[p] = {"committed": None, "lag": high - low}
            else:
                per[p] = {"committed": off, "lag": max(0, high - off)}
        out[gid] = {"lag": sum(v["lag"] or 0 for v in per.values()), "partitions": per}
    return out


class KafkaNode(BaseNode):
    type = "kafka"

    def __init__(
        self,
        bootstrap: str,
        topic: str,
        node_id: str = "kafka",
        interval_s: float = 1.0,
    ) -> None:
        super().__init__(node_id, "Kafka")
        self.bootstrap = bootstrap
        self.topic = topic
        self.interval = interval_s
        self.stats = TopicStats()
        self.groups: dict[str, dict[str, Any]] = {}
        self.error: str | None = None
        self.baseline: int | None = None  # end offsets total at first sight
        self.rate = RateWindow()
        self.rate_history: deque[float] = deque(maxlen=HISTORY)
        self._last_end: int | None = None
        self._last_groups_at = 0.0
        self._probe: Consumer | None = None
        self._admin: AdminClient | None = None
        self._task: asyncio.Task[None] | None = None

    async def start(self, bus: Bus) -> None:
        await super().start(bus)
        conf: dict[str, str | int | float | bool] = {"bootstrap.servers": self.bootstrap}
        # Not subscribed: never joins a group, never commits.
        self._probe = Consumer(
            {
                **conf,
                "group.id": f"petbowl-console-probe-{uuid.uuid4().hex[:8]}",
                "enable.auto.commit": False,
            }
        )
        self._admin = AdminClient(conf)
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        if self._probe is not None:
            probe, self._probe = self._probe, None
            await asyncio.to_thread(probe.close)

    def _sample(self) -> tuple[TopicStats, dict[str, dict[str, Any]] | None]:
        assert self._probe is not None and self._admin is not None
        stats = read_topic(self._probe, self.topic)
        groups = None
        if stats.exists and time.monotonic() - self._last_groups_at >= GROUP_EVERY_S:
            groups = group_lags(self._admin, self.topic, stats)
            self._last_groups_at = time.monotonic()
        return stats, groups

    async def _loop(self) -> None:
        while True:
            try:
                stats, groups = await asyncio.to_thread(self._sample)
                self._apply(stats, groups)
                self.error = None
            except Exception as exc:  # broker down must not kill the console
                self.error = (str(exc).splitlines() or [type(exc).__name__])[0][:200]
            await asyncio.sleep(self.interval)

    def _apply(self, stats: TopicStats, groups: dict[str, dict[str, Any]] | None) -> None:
        self.stats = stats
        end = stats.end_total
        if self.baseline is None:
            self.baseline = end
        if self._last_end is not None and end > self._last_end:
            delta = end - self._last_end
            self.rate.add(delta)
            self.emit(
                "kafka.produced",
                delta=delta,
                end_offsets={p: h for p, (_, h) in stats.partitions.items()},
            )
        self._last_end = end
        self.rate_history.append(round(self.rate.rate(), 1))
        if groups is not None:
            for gid, g in groups.items():
                if self.groups.get(gid, {}).get("lag") != g["lag"]:
                    self.emit("kafka.lag", group=gid, lag=g["lag"])
            self.groups = groups

    async def act_peek(self, params: dict[str, Any]) -> dict[str, Any]:
        n = max(1, min(int(params.get("n", 10)), PEEK_MAX))
        try:
            msgs = await asyncio.to_thread(self._peek, n)
        except Exception as exc:
            return {"ok": False, "error": (str(exc).splitlines() or [type(exc).__name__])[0][:200]}
        return {"ok": True, "messages": msgs}

    def _peek(self, n: int) -> list[dict[str, Any]]:
        """Last n messages per partition, via a throwaway assigned (not subscribed) consumer."""
        c = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": f"petbowl-console-peek-{uuid.uuid4().hex[:8]}",
                "enable.auto.commit": False,
            }
        )
        try:
            stats = read_topic(c, self.topic)
            tps = [
                TopicPartition(self.topic, p, max(low, high - n))
                for p, (low, high) in stats.partitions.items()
                if high > low
            ]
            if not tps:
                return []
            c.assign(tps)
            want = sum(stats.partitions[tp.partition][1] - tp.offset for tp in tps)
            out: list[dict[str, Any]] = []
            deadline = time.monotonic() + 3.0
            while len(out) < want and time.monotonic() < deadline:
                for m in c.consume(num_messages=want - len(out), timeout=0.5):
                    if m.error() is not None:
                        continue
                    raw = (m.value() or b"").decode("utf-8", "replace")
                    try:
                        value: Any = json.loads(raw)
                    except ValueError:
                        value = raw[:200]
                    out.append(
                        {
                            "partition": m.partition(),
                            "offset": m.offset(),
                            "key": (m.key() or b"").decode("utf-8", "replace"),
                            "value": value,
                        }
                    )
            out.sort(key=lambda m: (m["partition"], m["offset"]))
            return out[-n:]
        finally:
            c.close()

    def actions(self) -> dict[str, Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]]:
        return {"peek": self.act_peek}

    def snapshot(self) -> dict[str, Any]:
        s = self.stats
        if self.error:
            health = "down"
        elif self._last_end is None:
            health = "unknown"
        else:
            health = "ok" if s.exists else "failed"  # reachable, but the topic is missing
        return {
            "health": health,
            "error": self.error,
            "bootstrap": self.bootstrap,
            "topic": self.topic,
            "topic_exists": s.exists,
            "rate_per_s": round(self.rate.rate(), 1),
            "rate_history": list(self.rate_history),
            "end_total": s.end_total,
            "retained": s.retained,
            "produced_since_start": (s.end_total - self.baseline)
            if self.baseline is not None
            else 0,
            "partitions": [
                {"partition": p, "low": low, "high": high, "count": high - low}
                for p, (low, high) in sorted(s.partitions.items())
            ],
            "groups": [{"group": g, **v} for g, v in sorted(self.groups.items())],
            "total_lag": sum(v["lag"] for v in self.groups.values()),
        }
