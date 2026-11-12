import random
import signal
import subprocess
import sys
from pathlib import Path

import pytest
from crash_ops import model_after

from petbowl.kv.store import KV
from petbowl.kv.wal import WalError

WRITER = Path(__file__).with_name("crash_writer.py")
SEED = 7


def spawn(path: Path, policy: str, start: int) -> subprocess.Popen[bytes]:
    cmd = [sys.executable, str(WRITER), str(path), policy, str(SEED), str(start)]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE)


def run_and_kill(path: Path, policy: str, start: int, kill_after: int) -> int:
    """Run the writer, SIGKILL it after kill_after acks. Returns the ops acknowledged so far."""
    proc = spawn(path, policy, start)
    assert proc.stdout is not None
    acked = start
    try:
        for line in proc.stdout:
            acked = int(line) + 1
            if acked - start >= kill_after:
                break
        proc.send_signal(signal.SIGKILL)
        for line in proc.stdout:  # acks already in the pipe were acknowledged before the kill
            acked = int(line) + 1
        proc.wait()
        assert proc.returncode == -signal.SIGKILL, "writer exited by itself"
    finally:
        proc.kill()
        proc.wait()
        proc.stdout.close()
    return acked


def verify(path: Path, acked: int) -> int:
    """Reopen after the crash. Returns how many ops the store now contains."""
    with KV(path) as kv:  # would raise WalError if the dead writer's lock were stale
        state = dict(kv.items())
        assert kv.recovery.stop != "corrupt", "a kill must never leave corrupt bytes"
    if state == model_after(SEED, acked):
        return acked
    assert state == model_after(SEED, acked + 1), (
        f"acknowledged writes lost or phantom data after {acked} acks"
    )
    return acked + 1  # the in-flight op had reached the log


@pytest.mark.parametrize(
    ("policy", "cycles", "max_acks"),
    [("never", 12, 300), ("interval", 12, 300), ("always", 4, 15)],
)
def test_kill_nine_loses_no_acknowledged_write(
    tmp_path: Path, policy: str, cycles: int, max_acks: int
) -> None:
    path = tmp_path / "kv.wal"
    rng = random.Random(policy)
    applied = 0  # ops the store is known to contain
    for _ in range(cycles):
        acked = run_and_kill(path, policy, applied, rng.randint(1, max_acks))
        applied = verify(path, acked)  # next cycle keeps appending to the recovered log


def test_second_process_is_refused_while_the_writer_lives_then_allowed(tmp_path: Path) -> None:
    path = tmp_path / "kv.wal"
    proc = spawn(path, "never", 0)
    assert proc.stdout is not None
    try:
        proc.stdout.readline()  # the writer is up and holds the lock
        with pytest.raises(WalError):
            KV(path)
    finally:
        proc.kill()
        proc.wait()
        proc.stdout.close()
    KV(path).close()  # the kernel released the lock when the process died
