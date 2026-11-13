"""HTTP sink: wait for the ack, retry safely, dead-letter what can never succeed."""

import http.client
import json
import random
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from petbowl.simulator.faults import Delivery


class SinkError(RuntimeError):
    """A response the sink cannot act on. A bug in the setup, so it stops the run."""


@dataclass(frozen=True, slots=True)
class Response:
    status: int
    retry_after: float | None
    body: bytes


Transport = Callable[[bytes], Response]  # raises OSError on any network failure


class HttpTransport:
    """POSTs NDJSON over one keep-alive connection and reconnects after any failure."""

    def __init__(self, url: str, timeout: float) -> None:
        parts = urlsplit(url)
        self._https = parts.scheme == "https"
        self._host = parts.hostname or "localhost"
        self._port = parts.port or (443 if self._https else 80)
        self._path = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
        self._timeout = timeout
        self._conn: http.client.HTTPConnection | None = None

    def __call__(self, body: bytes) -> Response:
        if self._conn is None:
            cls = http.client.HTTPSConnection if self._https else http.client.HTTPConnection
            self._conn = cls(self._host, self._port, timeout=self._timeout)
        try:
            headers = {"Content-Type": "application/x-ndjson"}
            self._conn.request("POST", self._path, body=body, headers=headers)
            resp = self._conn.getresponse()
            data = resp.read()
        except (OSError, http.client.HTTPException) as exc:
            self.close()  # the next attempt opens a fresh connection
            raise OSError(str(exc)) from exc
        raw = resp.getheader("Retry-After")
        try:
            retry_after = float(raw) if raw else None
        except ValueError:
            retry_after = None
        return Response(resp.status, retry_after, data)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


@dataclass(slots=True)
class HttpStats:
    requests: int = 0
    batches: int = 0
    items: int = 0  # items the server answered for with a 200
    accepted: int = 0
    duplicates: int = 0
    rejected: int = 0
    retries: int = 0
    dropped: int = 0  # items given up on: delivery failed, never reached the server's verdict


def frame(item: bytes) -> bytes:
    """One item is one NDJSON line. A stray newline would shift every later reject index."""
    cleaned = item.replace(b"\r", b" ").replace(b"\n", b" ")
    return cleaned if cleaned.strip() else b"{}"


class HttpSink:
    def __init__(
        self,
        url: str,
        *,
        dead_letter: Path | None = None,
        ack_log: Path | None = None,
        timeout: float = 5.0,
        max_attempts: int = 8,
        backoff_base: float = 0.1,
        backoff_cap: float = 2.0,
        transport: Transport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self.stats = HttpStats()
        self._transport: Transport = transport or HttpTransport(url, timeout)
        self._dead_path = dead_letter
        self._ack_path = ack_log
        self._max_attempts = max_attempts
        self._base = backoff_base
        self._cap = backoff_cap
        self._sleep = sleep
        self._rng = rng or random.Random()

    def send_payloads(self, payloads: Sequence[bytes]) -> None:
        items = [frame(b) for b in payloads]
        if items:
            self.stats.batches += 1
            self._send(items)

    def write(self, batch: Sequence[Delivery]) -> None:
        self.send_payloads([d.payload for d in batch])

    def close(self) -> None:
        if isinstance(self._transport, HttpTransport):
            self._transport.close()
        s = self.stats
        print(
            f"http sink: {s.items} items, {s.batches} batches, {s.requests} requests, "
            f"{s.accepted} accepted, {s.duplicates} duplicate, {s.rejected} rejected, "
            f"{s.retries} retries, {s.dropped} dropped",
            file=sys.stderr,
        )

    def _send(self, items: list[bytes]) -> None:
        for attempt in range(self._max_attempts):
            try:
                resp = self._transport(b"\n".join(items))
            except OSError:
                self._backoff(attempt, None)
                continue
            self.stats.requests += 1
            if resp.status == 200:
                self._acked(items, resp.body)
                return
            if resp.status == 413 and len(items) > 1:
                mid = len(items) // 2
                self._send(items[:mid])
                self._send(items[mid:])
                return
            if resp.status in (400, 413):
                self._dead_letter([(i, f"HTTP {resp.status}") for i in items], dropped=True)
                return
            if resp.status >= 500:
                self._backoff(attempt, resp.retry_after)
                continue
            raise SinkError(f"unexpected HTTP {resp.status}: {resp.body[:200]!r}")
        reason = f"gave up after {self._max_attempts} attempts"
        self._dead_letter([(i, reason) for i in items], dropped=True)

    def _backoff(self, attempt: int, retry_after: float | None) -> None:
        if attempt + 1 >= self._max_attempts:
            return  # no sleep after the last attempt
        self.stats.retries += 1
        delay = min(self._cap, self._base * 2**attempt) * self._rng.uniform(0.5, 1.0)
        if retry_after is not None:
            delay = max(delay, min(retry_after, self._cap))
        self._sleep(delay)

    def _acked(self, items: list[bytes], body: bytes) -> None:
        data = json.loads(body)
        rejects = {r["index"]: r["reason"] for r in data["rejects"] if r["index"] < len(items)}
        s = self.stats
        s.items += len(items)
        s.accepted += data["accepted"]
        s.duplicates += data["duplicates"]
        s.rejected += data["rejected"]
        if rejects:
            self._dead_letter([(items[i], why) for i, why in rejects.items()], dropped=False)
        if self._ack_path is not None:
            ids = []
            for i, item in enumerate(items):
                if i in rejects:
                    continue
                try:
                    ev = json.loads(item)
                    ids.append(f"{ev['device_id']}:{ev['seq']}\n")
                except (ValueError, KeyError, TypeError):
                    continue  # an unlisted reject (past the server's cap of 100)
            with self._ack_path.open("a", encoding="utf-8") as f:
                f.writelines(ids)

    def _dead_letter(self, pairs: list[tuple[bytes, str]], *, dropped: bool) -> None:
        if dropped:
            self.stats.dropped += len(pairs)
        if self._dead_path is None:
            return
        with self._dead_path.open("a", encoding="utf-8") as f:
            for payload, reason in pairs:
                rec = {"reason": reason, "payload": payload.decode("utf-8", "backslashreplace")}
                f.write(json.dumps(rec) + "\n")
