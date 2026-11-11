import subprocess
import sys

from petbowl.common.events import Event


def test_python_dash_m_streams_clean_jsonl_to_stdout() -> None:
    r = subprocess.run(
        [
            sys.executable,
            "-m",
            "petbowl.simulator",
            "--devices",
            "2",
            "--hours",
            "0.05",
            "--sink",
            "stdout",
        ],
        capture_output=True,
        check=True,
        timeout=60,
    )
    lines = r.stdout.splitlines()
    assert lines
    for line in lines:
        Event.model_validate_json(line)  # raises if stdout carried anything but data
    assert b"done:" in r.stderr
