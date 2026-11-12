"""Deterministic operation stream shared by the crash writer and the crash test."""

import random

KEYS = [f"dev-{i:02d}".encode() for i in range(8)]


def op(seed: int, n: int) -> tuple[str, bytes, bytes]:
    r = random.Random(f"{seed}:{n}")
    key = r.choice(KEYS)
    if r.random() < 0.25:
        return ("delete", key, b"")
    return ("put", key, f"v{n}".encode() * r.randint(1, 20))


def model_after(seed: int, count: int) -> dict[bytes, bytes]:
    state: dict[bytes, bytes] = {}
    for n in range(count):
        kind, k, v = op(seed, n)
        if kind == "put":
            state[k] = v
        else:
            state.pop(k, None)
    return state
