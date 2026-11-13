import json

TS = "2026-01-01T00:00:00Z"


def line(device: str, seq: int, kind: str = "weight_g") -> bytes:
    body = {"device_id": device, "seq": seq, "ts": TS, "kind": kind, "value": 1.0}
    return json.dumps(body).encode()
