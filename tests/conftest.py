import contextlib
import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from confluent_kafka.admin import AdminClient, NewTopic

SCHEMA_SQL = Path(__file__).parents[1] / "src/petbowl/ingest/schema.sql"


@pytest.fixture(scope="session")
def db_url() -> str:
    return os.environ.get("PETBOWL_DB_URL", "postgresql://petbowl:petbowl@127.0.0.1:5432/petbowl")


@pytest.fixture
def pg_schema(db_url: str) -> Iterator[str]:
    """An empty throwaway schema, dropped after the test."""
    try:
        c = psycopg.connect(db_url, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError:
        if os.environ.get("CI"):
            raise  # in CI a missing database is a failure, not a skip
        pytest.skip("postgres not running: make db-up")
    name = f"t_{uuid.uuid4().hex[:12]}"
    c.execute(f"CREATE SCHEMA {name}")
    try:
        yield name
    finally:
        c.execute(f"DROP SCHEMA {name} CASCADE")
        c.close()


@pytest.fixture
def conn(db_url: str, pg_schema: str) -> Iterator[psycopg.Connection[Any]]:
    """Sync connection with the schema applied inside the throwaway schema."""
    c = psycopg.connect(db_url, autocommit=True, options=f"-c search_path={pg_schema}")
    c.execute(SCHEMA_SQL.read_text())
    try:
        yield c
    finally:
        c.close()


@pytest.fixture(scope="session")
def kafka_bootstrap() -> str:
    servers = os.environ.get("PETBOWL_KAFKA", "127.0.0.1:9092")
    try:
        AdminClient({"bootstrap.servers": servers}).list_topics(timeout=3)
    except Exception as exc:
        msg = f"no Kafka at {servers}: {exc}"
        if os.environ.get("CI"):
            pytest.fail(msg)
        pytest.skip(msg)
    return servers


@pytest.fixture
def topic(kafka_bootstrap: str):
    name = f"t_{uuid.uuid4().hex[:12]}"
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)])[name].result(10)
    yield name
    with contextlib.suppress(Exception):
        admin.delete_topics([name])[name].result(10)
