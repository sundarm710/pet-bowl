from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from fastapi import FastAPI, HTTPException, Request

from petbowl.ingest.app import IngestResponse
from petbowl.ingest.service import ItemResult
from petbowl.ingest.validate import validate_lines
from petbowl.mq import Publisher, PublishError, QueueError
from petbowl.mq.kafka import KafkaPublisher, create_topic

log = logging.getLogger("petbowl.ingest.queue_app")


@dataclass(frozen=True)
class QueueSettings:
    kafka_bootstrap: str = "127.0.0.1:9092"
    topic: str = "petbowl.events"
    partitions: int = 3
    max_body_bytes: int = 5_000_000
    max_items: int = 5_000


def from_env() -> QueueSettings:
    return QueueSettings(
        kafka_bootstrap=os.environ.get("PETBOWL_KAFKA", "127.0.0.1:9092"),
        topic=os.environ.get("PETBOWL_TOPIC", "petbowl.events"),
        partitions=int(os.environ.get("PETBOWL_TOPIC_PARTITIONS", "3")),
    )


def _reject(r: ItemResult) -> dict[str, object]:
    # MATCH: same shape as the rejects your stage 3 handler returns
    return {"index": r.index, "reason": r.detail}


def create_queue_app(settings: QueueSettings, publisher: Publisher | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pub = publisher
        if pub is None:
            await asyncio.to_thread(
                create_topic, settings.kafka_bootstrap, settings.topic, settings.partitions
            )
            pub = KafkaPublisher(settings.kafka_bootstrap)
        app.state.publisher = pub
        try:
            yield
        finally:
            try:
                await pub.close()
            except QueueError:
                log.exception("publisher close failed")

    app = FastAPI(lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.post("/v1/events")
    async def ingest(request: Request) -> IngestResponse:
        declared = request.headers.get("content-length")
        if declared is not None and int(declared) > settings.max_body_bytes:
            raise HTTPException(413, "body too large")
        body = await request.body()
        if len(body) > settings.max_body_bytes:
            raise HTTPException(413, "body too large")

        lines = [ln for ln in body.split(b"\n") if ln.strip()]  # MATCH: same framing as stage 3
        if not lines:
            raise HTTPException(400, "empty batch")
        if len(lines) > settings.max_items:
            raise HTTPException(413, "too many items")

        checked = validate_lines(lines)
        if checked.valid:
            items = [(ev.device_id.encode(), raw) for _, ev, raw in checked.valid]
            try:
                await request.app.state.publisher.publish(settings.topic, items)
            except PublishError as exc:
                log.warning("publish failed: %s", exc)
                raise HTTPException(503, "queue unavailable", headers={"Retry-After": "1"}) from exc

        return IngestResponse(
            accepted=len(checked.valid),
            duplicates=0,
            rejected=len(checked.rejects),
            gaps=0,
            late=0,
            rejects=[_reject(r) for r in checked.rejects[:100]],
        )

    return app


def app_from_env() -> FastAPI:
    return create_queue_app(from_env())
