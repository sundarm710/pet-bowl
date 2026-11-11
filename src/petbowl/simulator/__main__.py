# src/petbowl/simulator/__main__.py
"""CLI: python -m petbowl.simulator --devices 1000 --hours 24 --seed 42 --sink null"""

import argparse
import sys
import time
from datetime import datetime, timedelta

from petbowl.simulator.device import make_fleet, stream
from petbowl.simulator.faults import FaultConfig, apply_faults
from petbowl.simulator.pace import PaceStats, pace
from petbowl.simulator.sinks import make_sink

# fixed, never "now": a wall-clock default would break reproducibility
DEFAULT_START = "2026-01-01T00:00:00+00:00"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="petbowl.simulator", description="Petbowl telemetry simulator")
    p.add_argument("--devices", type=int, default=100)
    p.add_argument("--hours", type=float, default=24.0, help="virtual duration")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--speedup", type=float, default=0.0, help="virtual s per wall s; 0 = unpaced")
    p.add_argument("--sink", default="null", help="null | stdout | jsonl:<path>")
    p.add_argument("--start", default=DEFAULT_START, help="ISO 8601 with UTC offset")
    p.add_argument("--dup-rate", type=float, default=0.0)
    p.add_argument("--ooo-rate", type=float, default=0.0)
    p.add_argument("--offline-rate", type=float, default=0.0)
    p.add_argument("--skew-s", type=float, default=0.0)
    p.add_argument("--malformed-rate", type=float, default=0.0)
    return p


def validate(p: argparse.ArgumentParser, a: argparse.Namespace) -> datetime:
    if a.devices < 1:
        p.error("--devices must be >= 1")
    if a.hours <= 0:
        p.error("--hours must be > 0")
    if a.speedup < 0:
        p.error("--speedup must be >= 0")
    if a.skew_s < 0:
        p.error("--skew-s must be >= 0")
    for name in ("dup_rate", "ooo_rate", "offline_rate", "malformed_rate"):
        if not 0.0 <= getattr(a, name) <= 1.0:
            p.error(f"--{name.replace('_', '-')} must be in [0, 1]")
    try:
        start = datetime.fromisoformat(a.start)
    except ValueError:
        p.error(f"--start is not ISO 8601: {a.start!r}")
    if start.tzinfo is None:
        p.error("--start needs a UTC offset, e.g. 2026-01-01T00:00:00+00:00")
    return start


def main(argv: list[str] | None = None) -> int:
    p = build_parser()
    a = p.parse_args(argv)
    start = validate(p, a)
    try:
        sink = make_sink(a.sink)
    except ValueError as e:
        p.error(str(e))

    cfg = FaultConfig(
        seed=a.seed,
        dup_rate=a.dup_rate,
        ooo_rate=a.ooo_rate,
        offline_rate=a.offline_rate,
        skew_s=a.skew_s,
        malformed_rate=a.malformed_rate,
    )
    # lazy generators: one batch in flight at a time, so memory stays flat
    fleet = make_fleet(a.devices, a.seed, start)
    events = stream(fleet, start + timedelta(hours=a.hours))
    deliveries = apply_faults(events, cfg)

    print(f"config: {vars(a)}", file=sys.stderr)  # logs go to stderr, data to the sink
    stats = PaceStats()
    t0 = time.monotonic()
    try:
        for batch in pace(deliveries, start, a.speedup, stats):
            sink.write(batch)
    finally:
        sink.close()
    wall = time.monotonic() - t0

    rate = stats.items / wall if wall > 0 else 0.0
    print(
        f"done: {stats.items} items, {stats.batches} batches, {wall:.2f}s wall, "
        f"{rate:,.0f} items/s, max lag {stats.max_lag_s:.3f}s",
        file=sys.stderr,
    )
    if a.speedup > 0 and stats.max_lag_s > 1.0:
        print("WARNING: max lag > 1s, did not hold schedule (NA for load tests)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
