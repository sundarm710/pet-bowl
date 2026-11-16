from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from pydantic import ValidationError

from petbowl.ingest.service import ItemResult, Outcome
from petbowl.common.events import Event


@dataclass(frozen=True, slots=True)
class Checked:
    valid: list[tuple[int, Event, bytes]]  # (index, event, raw line)
    rejects: list[ItemResult]


def _reason(exc: ValidationError) -> str:
    errs = exc.errors()
    if not errs:
        return "invalid event"
    loc = ".".join(str(p) for p in errs[0]["loc"])
    return f"{loc}: {errs[0]['msg']}" if loc else str(errs[0]["msg"])


def validate_lines(lines: Sequence[bytes]) -> Checked:
    valid: list[tuple[int, Event, bytes]] = []
    rejects: list[ItemResult] = []
    for i, raw in enumerate(lines):
        try:
            ev = Event.model_validate_json(raw)
        except ValidationError as exc:
            rejects.append(ItemResult(i, Outcome.REJECTED, _reason(exc)))
            continue
        valid.append((i, ev, raw))
    return Checked(valid, rejects)