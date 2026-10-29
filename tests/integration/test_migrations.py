import asyncio

import pytest
from alembic import command
from sqlalchemy import text

from task_platform.config import Settings
from task_platform.data.db import create_db_engine
from tests.integration.conftest import alembic_config

pytestmark = pytest.mark.integration


async def schema_objects(settings: Settings) -> set[str]:
    engine = create_db_engine(settings)
    try:
        async with engine.connect() as connection:
            tables = await connection.scalars(
                text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            )
            indexes = await connection.scalars(
                text("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
            )
            return set(tables) | set(indexes)
    finally:
        await engine.dispose()


async def migrate(settings: Settings, direction: str, target: str) -> None:
    # Alembic's env.py runs its own event loop, so it gets a thread of its own.
    step = command.upgrade if direction == "up" else command.downgrade
    await asyncio.to_thread(step, alembic_config(settings), target)


async def test_migrations_go_up_down_and_up_again(test_settings: Settings) -> None:
    jobs_objects = {"jobs", "pk_jobs", "ix_jobs_dispatch_due"}

    await migrate(test_settings, "up", "head")
    assert jobs_objects <= await schema_objects(test_settings)

    try:
        await migrate(test_settings, "down", "base")
        assert not jobs_objects & await schema_objects(test_settings)
    finally:
        # The test database is shared with the other integration tests: leave it at head even if
        # the downgrade failed, so only this test reports the failure.
        await migrate(test_settings, "up", "head")
    assert jobs_objects <= await schema_objects(test_settings)


async def test_dispatch_index_is_partial_on_due_unpublished_jobs(test_settings: Settings) -> None:
    await migrate(test_settings, "up", "head")
    engine = create_db_engine(test_settings)
    try:
        async with engine.connect() as connection:
            definition = await connection.scalar(
                text("SELECT indexdef FROM pg_indexes WHERE indexname = 'ix_jobs_dispatch_due'")
            )
    finally:
        await engine.dispose()

    assert definition is not None
    assert "(available_at)" in definition
    assert "WHERE ((status = 'queued'::text) AND (dispatched_at IS NULL))" in definition
