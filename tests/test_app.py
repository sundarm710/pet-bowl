from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import psycopg
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from helpers import line

from petbowl.ingest.app import create_app
from petbowl.ingest.config import Settings

NDJSON = {"content-type": "application/x-ndjson"}


@pytest.fixture
def settings(db_url: str, pg_schema: str, tmp_path: Path) -> Settings:
    return Settings(
        db_url=db_url, kv_path=tmp_path / "state.wal", kv_policy="never", schema=pg_schema
    )


@pytest.fixture
def app(settings: Settings) -> FastAPI:
    return create_app(settings)


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    with TestClient(app) as c:  # entering the context runs the lifespan
        yield c


def post(client: TestClient, *lines: bytes) -> dict[str, Any]:
    r = client.post("/v1/events", content=b"\n".join(lines), headers=NDJSON)
    assert r.status_code == 200, r.text
    body: dict[str, Any] = r.json()
    return body


def test_healthz(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}


def test_batch_is_accepted_and_state_is_readable(client: TestClient) -> None:
    body = post(client, line("d1", 1), line("d1", 2), line("d1", 3))
    assert (body["accepted"], body["duplicates"], body["rejected"]) == (3, 0, 0)
    state = client.get("/v1/devices/d1/state").json()
    assert state["max_seq"] == 3
    assert state["max_ts"].startswith("2026-01-01T00:00:00")


def test_resending_the_same_batch_is_all_duplicates(client: TestClient) -> None:
    batch = [line("d1", 1), line("d1", 2)]
    post(client, *batch)
    body = post(client, *batch)
    assert (body["accepted"], body["duplicates"]) == (0, 2)


def test_rejects_carry_index_and_reason(client: TestClient) -> None:
    body = post(client, line("d1", 1), b"{not json", line("d1", 2, kind="nope"), line("d1", 3))
    assert (body["accepted"], body["rejected"]) == (2, 2)
    assert [r["index"] for r in body["rejects"]] == [1, 2]
    assert all(r["reason"] for r in body["rejects"])


def test_a_gap_is_counted(client: TestClient) -> None:
    post(client, line("d1", 1))
    assert post(client, line("d1", 4))["gaps"] == 1


def test_unknown_device_is_404(client: TestClient) -> None:
    assert client.get("/v1/devices/nobody/state").status_code == 404


def test_empty_body_is_400(client: TestClient) -> None:
    assert client.post("/v1/events", content=b"\n\n", headers=NDJSON).status_code == 400


def test_too_many_items_is_413(settings: Settings) -> None:
    with TestClient(create_app(replace(settings, max_items=2))) as c:
        r = c.post("/v1/events", content=b"\n".join([line("d1", i) for i in range(3)]))
        assert r.status_code == 413


class DownEvents:
    async def insert(self, events: object) -> list[bool]:
        raise psycopg.OperationalError("connection lost")


def test_database_failure_is_503_with_retry_after(app: FastAPI, client: TestClient) -> None:
    app.state.events = DownEvents()
    r = client.post("/v1/events", content=line("d1", 1), headers=NDJSON)
    assert r.status_code == 503
    assert r.headers["retry-after"] == "1"


def test_kv_is_rebuilt_from_postgres_at_startup(settings: Settings) -> None:
    with TestClient(create_app(settings)) as c:
        post(c, line("d1", 1), line("d1", 2), line("d1", 3))
    settings.kv_path.unlink()  # the KV lost everything; Postgres kept the events
    with TestClient(create_app(settings)) as c:
        assert c.get("/v1/devices/d1/state").json()["max_seq"] == 3
