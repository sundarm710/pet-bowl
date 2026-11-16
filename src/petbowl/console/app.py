"""Console server: topology, SSE stream, node actions, tap proxy, static UI."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from petbowl.console.bus import Bus
from petbowl.console.nodes.base import NodePlugin
from petbowl.console.nodes.ingest import IngestNode
from petbowl.console.nodes.kv import KvNode
from petbowl.console.nodes.postgres import PostgresNode
from petbowl.console.nodes.simulator import SimulatorNode
from petbowl.console.topology import Edge, describe

STATIC = Path(__file__).parent / "static"


def default_pipeline(
    ingest_url: str,
    db_url: str,
    console_url: str,
    ack_log: Path | None = None,
    dead_letter: Path | None = None,
) -> tuple[list[NodePlugin], list[Edge]]:
    sim = SimulatorNode(f"{console_url}/tap/ingest", ack_log=ack_log, dead_letter=dead_letter)
    ingest = IngestNode(ingest_url)
    kv = KvNode(ingest)
    pg = PostgresNode(db_url)
    nodes: list[NodePlugin] = [sim, ingest, kv, pg]
    edges = [
        Edge("simulator", "ingest", "NDJSON"),
        Edge("ingest", "postgres", "insert"),
        Edge("ingest", "kv", "state"),
    ]
    return nodes, edges


def create_console_app(nodes: list[NodePlugin], edges: list[Edge]) -> FastAPI:
    bus = Bus()
    by_id = {n.id: n for n in nodes}
    describe(nodes, edges)  # validate eagerly

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        bus.bind(asyncio.get_running_loop())
        for n in nodes:
            await n.start(bus)
        try:
            yield
        finally:
            for n in reversed(nodes):
                with contextlib.suppress(Exception):
                    await n.stop()

    app = FastAPI(title="petbowl console", lifespan=lifespan)
    app.state.bus = bus

    @app.get("/api/topology")
    async def topology() -> dict[str, Any]:
        return describe(nodes, edges)

    @app.get("/api/stream")
    async def stream(request: Request, after: int = 0) -> StreamingResponse:
        last = request.headers.get("last-event-id")
        start = int(last) if last and last.isdigit() else after

        async def gen() -> AsyncIterator[str]:
            q = bus.subscribe()
            try:
                seen = start
                for ev in bus.replay(start):
                    seen = ev.id
                    yield f"id: {ev.id}\ndata: {json.dumps(ev.to_dict())}\n\n"
                while not await request.is_disconnected():
                    try:
                        ev = await asyncio.wait_for(q.get(), 15)
                    except TimeoutError:
                        yield ": keepalive\n\n"
                        continue
                    if ev.id <= seen:
                        continue
                    seen = ev.id
                    yield f"id: {ev.id}\ndata: {json.dumps(ev.to_dict())}\n\n"
            finally:
                bus.unsubscribe(q)

        return StreamingResponse(
            gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"}
        )

    @app.post("/api/nodes/{node_id}/actions/{name}")
    async def action(node_id: str, name: str, request: Request) -> dict[str, Any]:
        node = by_id.get(node_id)
        fn = node.actions().get(name) if node else None
        if fn is None:
            raise HTTPException(404, "unknown node or action")
        try:
            body = await request.json()
        except ValueError:
            body = {}
        return await fn(body if isinstance(body, dict) else {})

    @app.post("/tap/ingest")
    async def tap(request: Request) -> Response:
        ingest = next((n for n in nodes if isinstance(n, IngestNode)), None)
        if ingest is None:
            raise HTTPException(404, "no ingest node")
        status, headers, body = await ingest.handle(
            await request.body(), request.headers.get("content-type", "application/x-ndjson")
        )
        return Response(
            content=body, status_code=status, headers=headers, media_type="application/json"
        )

    app.mount("/", StaticFiles(directory=STATIC, html=True), name="static")
    return app
