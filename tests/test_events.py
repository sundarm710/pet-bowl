from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from petbowl.common.events import Event

T0 = datetime(2026, 1, 1, tzinfo=UTC)

def make(**kw: Any) -> Event:
    base: dict[str, Any] = dict(
        device_id="d1", seq=0, ts=T0, kind="weight_g", value=120.5
    )
    return Event(**{**base, **kw})


def test_round_trip() -> None:
    e = make()
    assert Event.model_validate_json(e.model_dump_json()) == e


def test_event_id_is_deterministic() -> None:
    assert make(seq=7).event_id == "d1:7"
    assert make(seq=7).event_id == make(seq=7, value=1.0).event_id


def test_rejects_naive_ts() -> None:
    with pytest.raises(ValidationError):
        make(ts=datetime(2026, 1, 1))


def test_rejects_negative_seq() -> None:
    with pytest.raises(ValidationError):
        make(seq=-1)


def test_rejects_unknown_kind() -> None:
    with pytest.raises(ValidationError):
        make(kind="temperature")


def test_rejects_unknown_field() -> None:
    with pytest.raises(ValidationError):
        Event.model_validate({**make().model_dump(), "extra": 1})
