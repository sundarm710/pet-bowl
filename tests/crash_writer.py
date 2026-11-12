"""Applies ops forever, printing each op number once it is acknowledged.

Usage: python crash_writer.py <wal path> <policy> <seed> <start>
The test kills it with SIGKILL.
"""

import sys
from pathlib import Path

from crash_ops import op

from petbowl.kv.store import KV


def main() -> None:
    path, policy = Path(sys.argv[1]), sys.argv[2]
    seed, n = int(sys.argv[3]), int(sys.argv[4])
    kv = KV(path, policy)  # type: ignore[arg-type]
    while True:
        kind, k, v = op(seed, n)
        if kind == "put":
            kv.put(k, v)
        else:
            kv.delete(k)
        print(n, flush=True)  # acknowledged: the op is done, now tell the parent
        n += 1


if __name__ == "__main__":
    main()
