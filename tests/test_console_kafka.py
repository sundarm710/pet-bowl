from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from confluent_kafka import TopicPartition

from petbowl.console.app import default_pipeline
from petbowl.console.bus import Bus
from petbowl.console.nodes.ingest import IngestNode
from petbowl.console.nodes.kafka import KafkaNode, TopicStats, group_lags
from petbowl.console.nodes.kv import KvNode
from petbowl.console.topology import Edge, describe


def stats(**parts: tuple[int, int]) -> TopicStats:
    return TopicStats(exists=True, partitions={int(k[1:]): v for k, v in parts.items()})


class _Fut:
    def __init__(self, value: Any) -> None:
        self._v = value

    def result(self, _timeout: float | None = None) -> Any:
        return self._v


class FakeAdmin:
    def __init__(self, committed: dict[str, dict[int, int]]) -> None:
        self.committed = committed

    def list_consumer_groups(self, **_: Any) -> _Fut:
        return _Fut(SimpleNamespace(valid=[SimpleNamespace(group_id=g) for g in self.committed]))

    def list_consumer_group_offsets(self, reqs: list[Any], **_: Any) -> dict[str, _Fut]:
        out = {}
        for r in reqs:
            tps = [
                TopicPartition("petbowl.events", p, off)
                for p, off in self.committed[r.group_id].items()
            ]
            tps.append(TopicPartition("other.topic", 0, 5))  # must be ignored
            out[r.group_id] = _Fut(SimpleNamespace(topic_partitions=tps))
        return out


def test_topic_stats_totals() -> None:
    s = stats(p0=(10, 110), p1=(0, 50))
    assert s.end_total == 160
    assert s.retained == 150


def test_group_lag_per_partition_and_ignores_other_topics() -> None:
    s = stats(p0=(0, 100), p1=(0, 50), p2=(0, 10))
    admin = FakeAdmin({"consumers": {0: 90, 1: 50}, "fresh": {2: -1}})  # -1 = nothing committed
    lags = group_lags(admin, "petbowl.events", s)  # type: ignore[arg-type]
    assert lags["consumers"]["partitions"][0] == {"committed": 90, "lag": 10}
    # p2 has no commit yet, so the whole partition is owed (consumers start at earliest)
    assert lags["consumers"]["partitions"][2] == {"committed": None, "lag": 10}
    assert lags["consumers"]["lag"] == 20
    assert "fresh" not in lags  # no committed offsets on this topic yet


def test_kafka_node_rate_baseline_and_events() -> None:
    bus = Bus()
    node = KafkaNode("127.0.0.1:1", "petbowl.events")
    node.bus = bus
    node._apply(stats(p0=(0, 100), p1=(0, 100)), None)  # first sight: baseline, no event
    assert bus.replay() == []
    node._apply(stats(p0=(0, 130), p1=(0, 120)), {"g": {"lag": 7, "partitions": {}}})
    kinds = [e.kind for e in bus.replay()]
    assert kinds == ["kafka.produced", "kafka.lag"]
    assert bus.replay()[0].data["delta"] == 50
    snap = node.snapshot()
    assert snap["produced_since_start"] == 50
    assert snap["end_total"] == 250
    assert snap["total_lag"] == 7
    assert snap["health"] == "ok"
    assert [p["count"] for p in snap["partitions"]] == [130, 120]
    node._apply(stats(p0=(0, 130), p1=(0, 120)), {"g": {"lag": 7, "partitions": {}}})
    assert len(bus.replay()) == 2  # nothing new produced, same lag: no new events


def test_kafka_node_health_states() -> None:
    node = KafkaNode("127.0.0.1:1", "t")
    assert node.snapshot()["health"] == "unknown"
    node._apply(TopicStats(exists=False), None)
    assert node.snapshot()["health"] == "failed"  # reachable but topic missing
    node.error = "connection refused"
    assert node.snapshot()["health"] == "down"


def test_queue_mode_pipeline_shape(tmp_path: Path) -> None:
    nodes, edges = default_pipeline(
        "http://i:1", "postgresql://x", "http://c:2", mode="queue", kafka="k:9092", topic="t"
    )
    ids = [n.id for n in nodes]
    assert ids == ["simulator", "ingest", "kafka", "kv", "postgres"]
    by = {(e.src, e.dst): e for e in edges}
    assert not by[("ingest", "kafka")].planned
    assert by[("kafka", "postgres")].planned and by[("kafka", "kv")].planned
    desc = describe(nodes, edges)
    assert desc["edges"][-1]["planned"] is True
    kv = next(n for n in nodes if n.id == "kv")
    assert kv.snapshot()["health"] == "idle"


def test_direct_mode_pipeline_unchanged() -> None:
    nodes, edges = default_pipeline("http://i:1", "postgresql://x", "http://c:2")
    assert [n.id for n in nodes] == ["simulator", "ingest", "kv", "postgres"]
    assert all(isinstance(e, Edge) and not e.planned for e in edges)


def test_disabled_kv_ignores_acks() -> None:
    bus = Bus()
    kv = KvNode(IngestNode("http://i:1"), enabled=False)
    bus.on_event(kv._observe)
    bus.emit("ingest", "ingest", "batch.ack", status=200, devices={"d1": 3})
    assert kv.expected == {}
