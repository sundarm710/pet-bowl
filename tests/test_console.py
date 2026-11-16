from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from fastapi.testclient import TestClient

from petbowl.console.app import create_console_app
from petbowl.console.bus import Bus
from petbowl.console.nodes.base import BaseNode
from petbowl.console.nodes.ingest import IngestNode, parse_batch
from petbowl.console.nodes.simulator import build_args
from petbowl.console.topology import Edge, describe


class FakeNode(BaseNode):
    type = "queue"  # a type with no widget: must still describe fine

    def snapshot(self) -> dict[str, Any]:
        return {"health": "ok"}


def test_bus_replay_and_ids() -> None:
    bus = Bus(ring=3)
    for i in range(5):
        bus.emit("n", "t", "k", corr=f"d:{i}")
    ids = [e.id for e in bus.replay()]
    assert ids == [3, 4, 5]
    assert [e.id for e in bus.replay(after_id=4)] == [5]


def test_topology_validates_edges() -> None:
    a, b = FakeNode("a"), FakeNode("b")
    out = describe([a, b], [Edge("a", "b")])
    assert [n["id"] for n in out["nodes"]] == ["a", "b"]
    with pytest.raises(ValueError):
        describe([a], [Edge("a", "zzz")])


def test_parse_batch_counts_malformed() -> None:
    body = b'{"device_id":"d","seq":1}\nnot json\n{"x":1}\n\n'
    good, bad = parse_batch(body)
    assert len(good) == 1 and bad == 2


def test_build_args_rejects_negative() -> None:
    assert "--devices" in build_args({"devices": 3}, "http://x/tap")
    with pytest.raises(ValueError):
        build_args({"devices": -1}, "http://x/tap")


def test_tap_forwards_and_emits_ack(monkeypatch: pytest.MonkeyPatch) -> None:
    ack = {"accepted": 1, "duplicates": 0, "rejected": 0, "gaps": 0, "late": 0, "rejects": []}
    seen: dict[str, Any] = {}

    def fake_forward(
        url: str, body: bytes, ct: str, timeout: float
    ) -> tuple[int, dict[str, str], bytes]:
        seen["url"], seen["body"] = url, body
        return 200, {}, json.dumps(ack).encode()

    monkeypatch.setattr("petbowl.console.nodes.ingest._forward", fake_forward)
    monkeypatch.setattr("petbowl.console.nodes.ingest._get", lambda url, t: (200, b"{}"))
    ingest = IngestNode("http://up:1")
    app = create_console_app([ingest], [])
    body = b'{"device_id":"d1","seq":7,"kind":"heartbeat"}\n'
    with TestClient(app) as c:
        r = c.post("/tap/ingest", content=body)
        assert r.status_code == 200 and r.json()["accepted"] == 1
        assert seen["url"] == "http://up:1/v1/events" and seen["body"] == body
        kinds = [e.kind for e in app.state.bus.replay()]
        assert kinds == ["batch.sent", "batch.ack"]
        assert app.state.bus.replay()[0].data["corrs"] == ["d1:7"]
        assert ingest.snapshot()["counters"]["accepted"] == 1
        topo = c.get("/api/topology").json()
        assert topo["nodes"][0]["type"] == "ingest"
        assert c.post("/api/nodes/nope/actions/x").status_code == 404


def test_tap_upstream_down_returns_502(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*a: Any) -> Any:
        raise ConnectionRefusedError("down")

    monkeypatch.setattr("petbowl.console.nodes.ingest._forward", boom)
    monkeypatch.setattr("petbowl.console.nodes.ingest._get", lambda url, t: (503, b""))
    app = create_console_app([IngestNode("http://up:1")], [])
    with TestClient(app) as c:
        assert c.post("/tap/ingest", content=b'{"device_id":"d","seq":1}\n').status_code == 502


def test_kv_probe_emits_on_advance(monkeypatch: pytest.MonkeyPatch) -> None:
    from petbowl.console.nodes.kv import KvNode

    state = {"device_id": "d1", "max_seq": 4, "max_ts": "2026-01-01T00:00:00Z"}
    monkeypatch.setattr(
        "petbowl.console.nodes.kv._get", lambda url, t: (200, json.dumps(state).encode())
    )

    async def run() -> list[str]:
        bus = Bus()
        bus.bind(asyncio.get_running_loop())
        kv = KvNode(IngestNode("http://up:1"), interval_s=0.01)
        await kv.start(bus)
        bus.emit("ingest", "ingest", "batch.ack", status=200, devices={"d1": 4})
        await asyncio.sleep(0.2)
        await kv.stop()
        return [e.kind for e in bus.replay()]

    kinds = asyncio.run(run())
    assert kinds.count("kv.state") == 1  # unchanged state is not re-emitted


def test_log_tailer_skips_history_and_emits_new_lines(tmp_path: Any) -> None:
    from petbowl.console.nodes.simlogs import LogTailer

    ack, dead = tmp_path / "acks.txt", tmp_path / "dead.jsonl"
    ack.write_text("old-1:0\nold-1:1\n")
    dead.write_text(json.dumps({"reason": "old", "payload": "x"}) + "\n")
    bus = Bus()
    t = LogTailer(ack, dead, "simulator")
    t._bus = bus
    t.tick()
    assert bus.replay() == []  # history ignored

    with ack.open("a") as f:
        f.write("dev-1:5\ndev-1:6\ndev-2:3\ndev-3:")  # last line is partial
    with dead.open("a") as f:
        f.write(json.dumps({"reason": "bad kind", "payload": "{}"}) + "\n")
    t.tick()
    evs = {e.kind: e for e in bus.replay()}
    assert evs["log.acked"].data["count"] == 3
    assert evs["log.acked"].data["devices"] == {"dev-1": 6, "dev-2": 3}
    assert evs["log.dead"].data["reasons"] == {"bad kind": 1}

    with ack.open("a") as f:
        f.write("9\n")  # completes the partial line "dev-3:9"
    t.tick()
    assert bus.replay()[-1].data["devices"] == {"dev-3": 9}

    ack.write_text("")  # truncated between runs
    t.tick()
    with ack.open("a") as f:
        f.write("dev-9:1\n")
    t.tick()
    assert bus.replay()[-1].data["corrs"] == ["dev-9:1"]
    assert t.snapshot()["acked"] == 5


def test_kv_and_ingest_observe_log_events() -> None:
    from petbowl.console.nodes.kv import KvNode

    bus = Bus()
    ingest = IngestNode("http://up:1")
    kv = KvNode(ingest)
    bus.on_event(kv._observe)
    bus.on_event(ingest._observe_logs)
    bus.emit("simulator", "simulator", "log.acked", count=4, devices={"d1": 3})
    bus.emit("simulator", "simulator", "log.dead", count=2, reasons={"x": 2})
    assert kv.expected == {"d1": 3}
    assert ingest.via_logs == {"acked": 4, "dead_lettered": 2}
    assert ingest.reject_reasons == {"x": 2}


def test_kv_stops_polling_landed_and_missing_devices(monkeypatch: pytest.MonkeyPatch) -> None:
    from petbowl.console.nodes.kv import MISS_LIMIT, KvNode

    calls: list[str] = []

    def fake_get(url: str, t: float) -> tuple[int, bytes]:
        calls.append(url)
        if "d-missing" in url:  # e.g. WAL wiped: ingest has no state for it
            return 404, b""
        return 200, json.dumps({"max_seq": 4, "max_ts": "2026-01-01T00:00:00Z"}).encode()

    monkeypatch.setattr("petbowl.console.nodes.kv._get", fake_get)

    async def run() -> KvNode:
        bus = Bus()
        bus.bind(asyncio.get_running_loop())
        kv = KvNode(IngestNode("http://up:1"), interval_s=0.005)
        await kv.start(bus)
        bus.emit("i", "ingest", "batch.ack", status=200, devices={"d-ok": 4, "d-missing": 9})
        await asyncio.sleep(0.4)
        n = len(calls)
        await asyncio.sleep(0.2)
        assert len(calls) == n  # idle: no polling at all
        await kv.stop()
        return kv

    kv = asyncio.run(run())
    assert sum("d-ok" in c for c in calls) == 1  # landed -> never asked again
    assert sum("d-missing" in c for c in calls) == MISS_LIMIT
    assert kv.snapshot()["gave_up"] == 1
