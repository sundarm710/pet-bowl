# src/petbowl/common/events.py
"""Wire format shared by simulator, ingest API and consumers."""

from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

Kind = Literal["weight_g", "water_ml", "battery_pct", "heartbeat"]


class Event(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    device_id: str = Field(min_length=1, max_length=64)
    seq: int = Field(ge=0)  # per-device monotonic counter
    ts: AwareDatetime  # device clock, tz-aware
    kind: Kind
    value: float | None = None

    @property
    def event_id(self) -> str:
        """Deterministic id. Retries and duplicates collapse on it."""
        return f"{self.device_id}:{self.seq}"