from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from petbowl.common.events import Event
from petbowl.simulator.__main__ import main
from petbowl.simulator.device import make_fleet, stream

BASE = ["--devices", "5", "--hours", "2", "--seed", "42"]


def sim(path: Path, *extra: str) -> bytes:
    assert main([*BASE, *extra, "--sink", f"jsonl:{path}"]) == 0  # later flags win
    return path.read_bytes()


def test_same_seed_same_bytes(tmp_path: Path) -> None:
    a = sim(tmp_path / "a.jsonl")
    b = sim(tmp_path / "b.jsonl")
    assert a and a == b


def test_different_seed_differs(tmp_path: Path) -> None:
    assert sim(tmp_path / "a.jsonl") != sim(tmp_path / "b.jsonl", "--seed", "43")


def test_clean_run_matches_the_library(tmp_path: Path) -> None:
    lines = sim(tmp_path / "a.jsonl").splitlines()
    start = datetime(2026, 1, 1, tzinfo=UTC)
    expected = list(stream(make_fleet(5, 42, start), start + timedelta(hours=2)))
    assert [Event.model_validate_json(x) for x in lines] == expected


def test_faults_change_output(tmp_path: Path) -> None:
    clean = sim(tmp_path / "a.jsonl").count(b"\n")
    dup = sim(tmp_path / "b.jsonl", "--dup-rate", "0.2").count(b"\n")
    assert dup > clean


def test_paced_run_completes(tmp_path: Path) -> None:
    # 36 virtual seconds at 3600x is ~10 ms of wall time
    assert main(["--devices", "1", "--hours", "0.01", "--speedup", "3600", "--sink", "null"]) == 0


def test_logs_go_to_stderr_not_stdout(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    sim(tmp_path / "a.jsonl")
    out, err = capsys.readouterr()
    assert out == ""
    assert "'seed': 42" in err and "items/s" in err


@pytest.mark.parametrize(
    "args",
    [
        ["--devices", "0"],
        ["--hours", "0"],
        ["--speedup", "-1"],
        ["--skew-s", "-1"],
        ["--dup-rate", "2"],
        ["--sink", "bogus"],
        ["--start", "nope"],
        ["--start", "2026-01-01T00:00:00"],
    ],
)
def test_bad_args_exit_2(args: list[str]) -> None:
    with pytest.raises(SystemExit) as e:
        main(args)
    assert e.value.code == 2
