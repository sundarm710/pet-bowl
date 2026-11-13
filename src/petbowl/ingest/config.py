"""Ingest service settings, read from the environment."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Self


@dataclass(frozen=True, slots=True)
class Settings:
    db_url: str
    kv_path: Path
    kv_policy: str = "interval"
    kv_interval_s: float = 0.1
    schema: str | None = None  # tests pin a throwaway schema
    max_body_bytes: int = 5_000_000
    max_items: int = 5_000

    @classmethod
    def from_env(cls) -> Self:
        return cls(
            db_url=os.environ.get(
                "PETBOWL_DB_URL", "postgresql://petbowl:petbowl@127.0.0.1:5432/petbowl"
            ),
            kv_path=Path(os.environ.get("PETBOWL_KV_PATH", "data/state.wal")),
            kv_policy=os.environ.get("PETBOWL_KV_POLICY", "interval"),
            kv_interval_s=float(os.environ.get("PETBOWL_KV_INTERVAL_S", "0.1")),
        )
