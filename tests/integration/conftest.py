"""Fixtures for tests against a real PostgreSQL database (marker `integration`)."""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.config import Settings
from task_platform.data.db import create_db_engine
from tests.integration.database import TestDatabaseError, load_test_database_url

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"


def make_test_settings(url: str, **overrides: object) -> Settings:
    # _env_file=None: a developer's .env must not change what the tests measure.
    return Settings(_env_file=None, database_url=url, **overrides)  # type: ignore[arg-type]


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
    yield engine
    await engine.dispose()
