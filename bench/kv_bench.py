"""KV benchmark. Run: uv run python bench/kv_bench.py [--dir PATH]

Prints markdown tables to paste into docs/BASELINES.md.
"""

import argparse
import platform
import sys
import tempfile
import time
from pathlib import Path

from petbowl.kv.store import KV

VALUE = b"x" * 100  # roughly one small event's worth of state
KEYS = [f"dev-{i:04d}".encode() for i in range(1000)]  # 1000 live keys, the log keeps growing


def bench_put(path: Path, policy: str, n: int) -> tuple[float, float, float, float]:
    """Returns (puts/s, p50 us, p99 us, max us)."""
    lat: list[int] = []
    with KV(path, policy) as kv:  # type: ignore[arg-type]
        t0 = time.perf_counter()
        for i in range(n):
            s = time.perf_counter_ns()
            kv.put(KEYS[i % len(KEYS)], VALUE)
            lat.append(time.perf_counter_ns() - s)
        total = time.perf_counter() - t0
    lat.sort()
    return n / total, lat[n // 2] / 1e3, lat[int(n * 0.99)] / 1e3, lat[-1] / 1e3


def open_seconds(path: Path) -> float:
    s = time.perf_counter()
    with KV(path, "never"):  # opening = lock + recover (replay whole log) + open writer
        pass
    return time.perf_counter() - s


def bench_replay(path: Path, n: int) -> tuple[int, float, float]:
    """Returns (log bytes, best open seconds, records/s)."""
    with KV(path, "never") as kv:
        for i in range(n):
            kv.put(KEYS[i % len(KEYS)], VALUE)
    best = min(open_seconds(path) for _ in range(3))  # warm page cache
    return path.stat().st_size, best, n / best


def bench_get(path: Path, n: int) -> float:
    with KV(path, "never") as kv:
        for k in KEYS:
            kv.put(k, VALUE)
        t0 = time.perf_counter()
        for i in range(n):
            kv.get(KEYS[i % len(KEYS)])
        return n / (time.perf_counter() - t0)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=None, help="directory on the disk you care about")
    args = ap.parse_args()

    with tempfile.TemporaryDirectory(dir=args.dir) as d:
        root = Path(d)
        print(f"machine: {platform.platform()} {platform.machine()}, py {sys.version.split()[0]}")
        print(f"dir: {root} (on Linux /tmp may be tmpfs, where fsync is free; use --dir)")
        print()

        print("| policy | ops | puts/s | p50 us | p99 us | max us |")
        print("|---|---|---|---|---|---|")
        for policy, n in [("never", 200_000), ("interval", 200_000), ("always", 2_000)]:
            ops, p50, p99, mx = bench_put(root / f"put-{policy}.wal", policy, n)
            print(f"| {policy} | {n} | {ops:,.0f} | {p50:.1f} | {p99:.1f} | {mx:,.0f} |")
        print()

        print("| records | log size | open time | records/s |")
        print("|---|---|---|---|")
        for n in [10_000, 100_000, 1_000_000]:
            size, secs, rps = bench_replay(root / f"replay-{n}.wal", n)
            print(f"| {n:,} | {size / 1e6:.1f} MB | {secs * 1e3:,.0f} ms | {rps:,.0f} |")
        print()

        print(f"get: {bench_get(root / 'get.wal', 1_000_000):,.0f} gets/s (dict lookup)")


if __name__ == "__main__":
    main()
