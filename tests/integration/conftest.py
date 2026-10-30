"""Fixtures for tests against a real PostgreSQL and RabbitMQ (marker `integration`)."""

import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from aio_pika.abc import AbstractRobustConnection
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.broker.connection import connect_broker
from task_platform.config import BrokerSettings, Settings
from task_platform.data.db import create_db_engine
from tests.integration.broker import TestBrokerError, load_test_broker_url
from tests.integration.database import TestDatabaseError, load_test_database_url
from tests.settings import UNREACHABLE_DATABASE_URL, make_test_broker_settings, make_test_settings

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"


def alembic_config(settings: Settings) -> Config:
    # No ini file, so env.py leaves pytest's logging setup alone.
    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS_DIR))
    config.attributes["settings"] = settings
    return config


@pytest.fixture(scope="session")
def test_database_url() -> str:
    try:
        return load_test_database_url()
    except TestDatabaseError as exc:
        pytest.fail(str(exc), pytrace=False)


@pytest.fixture
def test_settings(test_database_url: str) -> Settings:
    return make_test_settings(test_database_url)


@pytest.fixture(scope="session")
def migrated_database(test_database_url: str) -> None:
    # Sync on purpose: env.py calls asyncio.run(), which fails inside a running event loop.
    command.upgrade(alembic_config(make_test_settings(test_database_url)), "head")


@pytest.fixture
async def engine(migrated_database: None, test_settings: Settings) -> AsyncIterator[AsyncEngine]:
    # Created per test: asyncpg connections belong to the event loop that opened them.
    engine = create_db_engine(test_settings)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE jobs"))
        # Left behind only if a run was killed inside test_command_timeout_also_bounds_commit; it
        # would make every later INSERT sleep 2 s at COMMIT.
        await connection.execute(text("DROP TRIGGER IF EXISTS test_slow_commit ON jobs"))
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def test_broker_url() -> str:
    try:
        return load_test_broker_url()
    except TestBrokerError as exc:
        pytest.fail(str(exc), pytrace=False)


@pytest.fixture
def broker_settings(test_broker_url: str) -> BrokerSettings:
    # A queue of its own per test (ADR 0004 decision 9), so tests never see each other's messages
    # or a developer's, and can run while a local worker is consuming `jobs`. Broker tests never
    # touch PostgreSQL, so they do not need TEST_DATABASE_URL.
    return make_test_broker_settings(
        UNREACHABLE_DATABASE_URL, test_broker_url, jobs_queue_name=f"test-jobs-{uuid.uuid4().hex}"
    )


@pytest.fixture
async def broker_connection(
    broker_settings: BrokerSettings,
) -> AsyncIterator[AbstractRobustConnection]:
    # Per test, like the engine: the connection belongs to the event loop that opened it.
    connection = await connect_broker(broker_settings)
    try:
        yield connection
    finally:
        # A fresh channel: the test may have left its own channels closed by the broker. Deletes
        # only this test's queue; deleting a queue that was never declared is not an error.
        try:
            channel = await connection.channel()
            await channel.queue_delete(broker_settings.jobs_queue_name)
        finally:
            await connection.close()
