"""HTTP layer: wires the stores to process_batch and speaks the ack contract."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime

import psycopg
from fastapi import FastAPI, HTTPException, Request
from psycopg_pool import PoolTimeout
from pydantic import BaseModel

from petbowl.ingest.config import Settings
from petbowl.ingest.db import PgEvents, apply_schema, open_pool
from petbowl.ingest.repair import repair_state
from petbowl.ingest.service import Outcome, decode_state, from_us, process_batch, state_key
from petbowl.kv.store import KV

log = logging.getLogger("uvicorn.error")
MAX_REJECTS_RETURNED = 100


class Reject(BaseModel):
    index: int
    reason: str


class IngestResponse(BaseModel):
    accepted: int
    duplicates: int
    rejected: int
    gaps: int
    late: int
    rejects: list[Reject]


class DeviceStateOut(BaseModel):
    device_id: str
    max_seq: int
    max_ts: datetime


def create_app(settings: Settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        settings.kv_path.parent.mkdir(parents=True, exist_ok=True)
        kv = KV(settings.kv_path, settings.kv_policy, settings.kv_interval_s)  # type: ignore[arg-type]
        try:
            pool = await open_pool(settings.db_url, schema=settings.schema)
            try:
                await apply_schema(pool)
                fixed = await repair_state(pool, kv)
                log.info("state repair: %d device(s) corrected from Postgres", fixed)
                app.state.pool = pool
                app.state.kv = kv
                app.state.events = PgEvents(pool)
                yield
            finally:
                await pool.close()
        finally:
            kv.close()

    app = FastAPI(title="petbowl ingest", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        try:
            async with app.state.pool.connection() as conn:
                await conn.execute("SELECT 1")
        except (psycopg.OperationalError, PoolTimeout) as exc:
            raise HTTPException(status_code=503, detail="database unavailable") from exc
        return {"status": "ok"}

    @app.post("/v1/events")
    async def ingest(request: Request) -> IngestResponse:
        declared = request.headers.get("content-length", "")
        if declared.isdigit() and int(declared) > settings.max_body_bytes:
            raise HTTPException(status_code=413, detail="body too large")
        body = await request.body()
        if len(body) > settings.max_body_bytes:
            raise HTTPException(status_code=413, detail="body too large")
        lines = [ln for ln in body.split(b"\n") if ln.strip()]
        if not lines:
            raise HTTPException(status_code=400, detail="empty batch")
        if len(lines) > settings.max_items:
            raise HTTPException(status_code=413, detail="too many items")

        try:
            result = await process_batch(lines, app.state.events, app.state.kv)
        except (psycopg.OperationalError, PoolTimeout) as exc:
            raise HTTPException(
                status_code=503, detail="database unavailable", headers={"Retry-After": "1"}
            ) from exc

        rejects = [
            Reject(index=r.index, reason=r.detail)
            for r in result.items
            if r.outcome is Outcome.REJECTED
        ]
        accepted = [r for r in result.items if r.outcome is Outcome.ACCEPTED]
        return IngestResponse(
            accepted=len(accepted),
            duplicates=result.count(Outcome.DUPLICATE),
            rejected=len(rejects),
            gaps=sum(1 for r in accepted if r.detail.startswith("gap")),
            late=sum(1 for r in accepted if r.detail == "late"),
            rejects=rejects[:MAX_REJECTS_RETURNED],
        )

    @app.get("/v1/devices/{device_id}/state")
    async def device_state(device_id: str) -> DeviceStateOut:
        raw = app.state.kv.get(state_key(device_id))
        if raw is None:
            raise HTTPException(status_code=404, detail="unknown device")
        st = decode_state(raw)
        return DeviceStateOut(device_id=device_id, max_seq=st.max_seq, max_ts=from_us(st.max_ts_us))

    return app


def app_from_env() -> FastAPI:
    """Factory for uvicorn: reads settings from the environment."""
    return create_app(Settings.from_env())
