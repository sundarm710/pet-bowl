import json
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from petbowl.simulator.http_sink import HttpSink, HttpTransport, Response, SinkError, frame


class Script:
    """A transport that replays scripted answers and records every body it was sent."""

    def __init__(self, *steps: Response | Exception) -> None:
        self.steps = list(steps)
        self.bodies: list[bytes] = []

    def __call__(self, body: bytes) -> Response:
        self.bodies.append(body)
        step = self.steps.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


def ok(
    accepted: int = 0, duplicates: int = 0, rejects: tuple[tuple[int, str], ...] = ()
) -> Response:
    body = {
        "accepted": accepted,
        "duplicates": duplicates,
        "rejected": len(rejects),
        "gaps": 0,
        "late": 0,
        "rejects": [{"index": i, "reason": why} for i, why in rejects],
    }
    return Response(200, None, json.dumps(body).encode())


def ev(seq: int, device: str = "d1") -> bytes:
    return json.dumps({"device_id": device, "seq": seq}).encode()


class Rig:
    def __init__(self, tmp_path: Path, script: Script, **kw: float) -> None:
        self.sleeps: list[float] = []
        self.dead = tmp_path / "dead.jsonl"
        self.acks = tmp_path / "acks.txt"
        self.script = script
        self.sink = HttpSink(
            "http://unused/v1/events",
            dead_letter=self.dead,
            ack_log=self.acks,
            transport=script,
            sleep=self.sleeps.append,
            rng=random.Random(1),
            **kw,  # type: ignore[arg-type]
        )

    def dead_reasons(self) -> list[str]:
        if not self.dead.exists():
            return []
        return [json.loads(ln)["reason"] for ln in self.dead.read_text().splitlines()]

    def ack_ids(self) -> list[str]:
        return self.acks.read_text().split() if self.acks.exists() else []


def test_a_200_is_an_ack_and_ids_are_logged(tmp_path: Path) -> None:
    r = Rig(tmp_path, Script(ok(accepted=2)))
    r.sink.send_payloads([ev(1), ev(2)])
    assert r.script.bodies == [ev(1) + b"\n" + ev(2)]
    assert (r.sink.stats.accepted, r.sink.stats.requests, r.sink.stats.retries) == (2, 1, 0)
    assert r.ack_ids() == ["d1:1", "d1:2"]
    assert r.sleeps == []


def test_503_is_retried_with_the_same_batch_and_honors_retry_after(tmp_path: Path) -> None:
    r = Rig(tmp_path, Script(Response(503, 1.0, b""), ok(accepted=2)))
    r.sink.send_payloads([ev(1), ev(2)])
    assert r.script.bodies[0] == r.script.bodies[1]
    assert r.sink.stats.retries == 1
    assert r.sleeps[0] >= 1.0


def test_a_network_error_is_retried(tmp_path: Path) -> None:
    r = Rig(tmp_path, Script(OSError("reset"), OSError("timeout"), ok(accepted=1)))
    r.sink.send_payloads([ev(1)])
    assert (r.sink.stats.retries, r.sink.stats.accepted, r.sink.stats.dropped) == (2, 1, 0)


def test_backoff_stays_within_base_and_cap_and_does_not_sleep_after_the_last_attempt(
    tmp_path: Path,
) -> None:
    r = Rig(tmp_path, Script(*[OSError("down")] * 5), max_attempts=5)
    r.sink.send_payloads([ev(1)])
    assert len(r.sleeps) == 4
    assert 0.05 <= r.sleeps[0] <= 0.1
    assert all(s <= 2.0 for s in r.sleeps)


def test_giving_up_dead_letters_the_batch_and_counts_it_dropped(tmp_path: Path) -> None:
    r = Rig(tmp_path, Script(*[OSError("down")] * 3), max_attempts=3)
    r.sink.send_payloads([ev(1), ev(2)])
    assert r.sink.stats.dropped == 2
    assert r.dead_reasons() == ["gave up after 3 attempts"] * 2
    assert r.ack_ids() == []


def test_413_splits_the_batch_in_half(tmp_path: Path) -> None:
    items = [ev(i) for i in range(4)]
    r = Rig(tmp_path, Script(Response(413, None, b""), ok(accepted=2), ok(accepted=2)))
    r.sink.send_payloads(items)
    assert r.script.bodies[1] == b"\n".join(items[:2])
    assert r.script.bodies[2] == b"\n".join(items[2:])
    assert r.sink.stats.accepted == 4


def test_a_single_item_that_is_too_big_is_dead_lettered(tmp_path: Path) -> None:
    r = Rig(tmp_path, Script(Response(413, None, b"")))
    r.sink.send_payloads([ev(1)])
    assert r.sink.stats.dropped == 1
    assert r.dead_reasons() == ["HTTP 413"]


def test_rejects_go_to_the_dead_letter_file_and_not_the_ack_log(tmp_path: Path) -> None:
    r = Rig(tmp_path, Script(ok(accepted=2, rejects=((1, "kind: bad"),))))
    r.sink.send_payloads([ev(1), ev(2), ev(3)])
    assert r.dead_reasons() == ["kind: bad"]
    assert json.loads(r.dead.read_text())["payload"] == ev(2).decode()
    assert r.ack_ids() == ["d1:1", "d1:3"]
    assert (r.sink.stats.rejected, r.sink.stats.dropped) == (1, 0)


def test_duplicates_count_as_delivered(tmp_path: Path) -> None:
    r = Rig(tmp_path, Script(ok(duplicates=2)))
    r.sink.send_payloads([ev(1), ev(2)])
    assert r.sink.stats.duplicates == 2
    assert r.ack_ids() == ["d1:1", "d1:2"]


def test_an_unexpected_status_stops_the_run(tmp_path: Path) -> None:
    r = Rig(tmp_path, Script(Response(404, None, b"nope")))
    with pytest.raises(SinkError):
        r.sink.send_payloads([ev(1)])


def test_frame_keeps_one_item_on_one_nonblank_line() -> None:
    assert frame(b'{"a":1}\n') == b'{"a":1} '
    assert frame(b"a\r\nb") == b"a  b"
    assert frame(b"") == b"{}"
    assert frame(b"\n\n") == b"{}"


def test_real_transport_round_trip_and_retry_after() -> None:
    received: list[bytes] = []
    status = {"code": 200}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"  # keep-alive

        def do_POST(self) -> None:
            received.append(self.rfile.read(int(self.headers["Content-Length"])))
            body = b'{"accepted": 1}'
            self.send_response(status["code"])
            if status["code"] == 503:
                self.send_header("Retry-After", "3")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    transport = HttpTransport(f"http://127.0.0.1:{server.server_port}/v1/events", timeout=2.0)
    try:
        assert transport(b"a").status == 200
        assert transport(b"b").status == 200  # second request on the same connection
        status["code"] = 503
        r = transport(b"c")
        assert (r.status, r.retry_after) == (503, 3.0)
        assert received == [b"a", b"b", b"c"]
    finally:
        transport.close()
        server.shutdown()
        server.server_close()
