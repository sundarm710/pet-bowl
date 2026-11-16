"""Ingest node: a tap proxy in front of the (unchanged) ingest API.

The simulator is pointed at the console's `/tap/ingest`; every batch is forwarded
verbatim and the ack is published to the bus, so nothing inside ingest is touched.
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.error
import urllib.request
from typing import Any

from petbowl.console.nodes.base import BaseNode, RateWindow

SAMPLE = 25
CORR_CAP = 200


def _forward(
    url: str, body: bytes, content_type: str, timeout: float
) -> tuple[int, dict[str, str], bytes]:
    req = urllib.request.Request(
        url, data=body, method="POST", headers={"Content-Type": content_type}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def _get(url: str, timeout: float) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:  # noqa: S310
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, b""


def parse_batch(body: bytes) -> tuple[list[dict[str, Any]], int]:
    """Best-effort parse of NDJSON. Returns (valid-looking events, malformed count)."""
    good: list[dict[str, Any]] = []
    bad = 0
    for line in body.split(b"\n"):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            bad += 1
            continue
        if isinstance(obj, dict) and "device_id" in obj and "seq" in obj:
            good.append(obj)
        else:
            bad += 1
    return good, bad


class IngestNode(BaseNode):
    type = "ingest"

    def __init__(self, ingest_url: str, node_id: str = "ingest", timeout: float = 10.0) -> None:
        super().__init__(node_id, "Ingest API")
        self.base = ingest_url.rstrip("/")
        self.timeout = timeout
        self.rate = RateWindow()
        self.c = dict(
            batches=0,
            items=0,
            accepted=0,
            duplicates=0,
            rejected=0,
            gaps=0,
            late=0,
            malformed_seen=0,
            http_4xx=0,
            http_5xx=0,
            upstream_errors=0,
        )
        self.last_latency_ms = 0.0
        self.via_logs = {"acked": 0, "dead_lettered": 0}
        self.reject_reasons: dict[str, int] = {}
        self.healthy: bool | None = None
        self._task: asyncio.Task[None] | None = None

    async def start(self, bus: Any) -> None:
        await super().start(bus)
        bus.on_event(self._observe_logs)
        self._task = asyncio.create_task(self._health_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()

    def _observe_logs(self, ev: Any) -> None:
        """Activity from a simulator that bypasses the tap, seen via its log files."""
        if ev.kind == "log.acked":
            self.via_logs["acked"] += ev.data["count"]
            self.rate.add(ev.data["count"])
        elif ev.kind == "log.dead":
            self.via_logs["dead_lettered"] += ev.data["count"]
            for reason, n in ev.data["reasons"].items():
                self.reject_reasons[reason] = self.reject_reasons.get(reason, 0) + n

    async def _health_loop(self) -> None:
        while True:
            try:
                status, _ = await asyncio.to_thread(_get, f"{self.base}/healthz", 2.0)
                self.healthy = status == 200
            except Exception:
                self.healthy = False
            await asyncio.sleep(2.0)

    async def handle(self, body: bytes, content_type: str) -> tuple[int, dict[str, str], bytes]:
        events, malformed = parse_batch(body)
        devices: dict[str, int] = {}
        for e in events:
            try:
                devices[str(e["device_id"])] = max(
                    devices.get(str(e["device_id"]), -1), int(e["seq"])
                )
            except (TypeError, ValueError):
                malformed += 1
        corrs = [f"{e['device_id']}:{e['seq']}" for e in events[:CORR_CAP]]
        n_items = len(events) + malformed
        self.c["batches"] += 1
        self.c["items"] += n_items
        self.c["malformed_seen"] += malformed
        self.rate.add(n_items)
        self.emit(
            "batch.sent", items=n_items, malformed=malformed, corrs=corrs, sample=events[:SAMPLE]
        )

        t0 = time.perf_counter()
        try:
            status, headers, resp = await asyncio.to_thread(
                _forward, f"{self.base}/v1/events", body, content_type, self.timeout
            )
        except OSError as exc:
            self.c["upstream_errors"] += 1
            self.emit("batch.error", error=str(exc))
            return 502, {}, json.dumps({"detail": "upstream unreachable"}).encode()
        self.last_latency_ms = (time.perf_counter() - t0) * 1000

        ack: dict[str, Any] = {}
        if status == 200:
            try:
                ack = json.loads(resp)
            except ValueError:
                ack = {}
            for k in ("accepted", "duplicates", "rejected", "gaps", "late"):
                self.c[k] += int(ack.get(k, 0))
            for r in ack.get("rejects", []):
                reason = str(r.get("reason", "?"))[:80]
                self.reject_reasons[reason] = self.reject_reasons.get(reason, 0) + 1
        elif 400 <= status < 500:
            self.c["http_4xx"] += 1
        else:
            self.c["http_5xx"] += 1
        self.emit(
            "batch.ack",
            status=status,
            latency_ms=round(self.last_latency_ms, 1),
            devices=devices,
            ack={k: v for k, v in ack.items() if k != "rejects"},
            rejects=ack.get("rejects", [])[:20],
        )
        keep = {k: v for k, v in headers.items() if k.lower() == "retry-after"}
        return status, keep, resp

    def snapshot(self) -> dict[str, Any]:
        health = "unknown" if self.healthy is None else ("ok" if self.healthy else "down")
        return {
            "health": health,
            "rate_per_s": round(self.rate.rate(), 1),
            "counters": dict(self.c),
            "last_latency_ms": round(self.last_latency_ms, 1),
            "via_logs": dict(self.via_logs),
            "reject_reasons": dict(sorted(self.reject_reasons.items(), key=lambda kv: -kv[1])[:10]),
        }

    # used by the KV probe
    def state_url(self, device_id: str) -> str:
        return f"{self.base}/v1/devices/{device_id}/state"
