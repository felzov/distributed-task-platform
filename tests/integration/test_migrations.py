import asyncio

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

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
    jobs_objects = {
        "jobs",
        "pk_jobs",
        "ix_jobs_dispatch_due",
        "fake_notifications",
        "pk_fake_notifications",
        "job_attempts",
        "pk_job_attempts",
        "ix_jobs_failed",
    }

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


async def test_failed_index_is_partial_and_newest_first(test_settings: Settings) -> None:
    await migrate(test_settings, "up", "head")
    engine = create_db_engine(test_settings)
    try:
        async with engine.connect() as connection:
            definition = await connection.scalar(
                text("SELECT indexdef FROM pg_indexes WHERE indexname = 'ix_jobs_failed'")
            )
    finally:
        await engine.dispose()

    assert definition is not None
    assert "(updated_at DESC, id DESC)" in definition
    assert "WHERE (status = 'failed'::text)" in definition


async def test_existing_failed_jobs_are_indexed_by_0004_and_kept_by_its_downgrade(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    # The index is built over the rows already there, and the downgrade drops only the index.
    try:
        await migrate(test_settings, "down", "0003")
        assert "ix_jobs_failed" not in await schema_objects(test_settings)
        async with engine.begin() as connection:
            job_id = await connection.scalar(
                text(
                    "INSERT INTO jobs (type, payload_version, payload, max_attempts, status,"
                    " attempts, error_code) VALUES ('notification.send', 1, '{}', 5, 'failed', 5,"
                    " 'handler_error') RETURNING id"
                )
            )

        await migrate(test_settings, "up", "0004")
        async with engine.begin() as connection:
            # Planner off the table scan, so the plan shows whether the index can answer the list.
            await connection.execute(text("SET LOCAL enable_seqscan = off"))
            plan = "\n".join(
                await connection.scalars(
                    text(
                        "EXPLAIN SELECT id FROM jobs WHERE status = 'failed'"
                        " ORDER BY updated_at DESC, id DESC LIMIT 10"
                    )
                )
            )
            listed = await connection.scalar(
                text(
                    "SELECT id FROM jobs WHERE status = 'failed'"
                    " ORDER BY updated_at DESC, id DESC LIMIT 1"
                )
            )
        assert "ix_jobs_failed" in plan
        assert listed == job_id

        await migrate(test_settings, "down", "0003")
        assert "ix_jobs_failed" not in await schema_objects(test_settings)
        async with engine.connect() as connection:
            status = await connection.scalar(
                text("SELECT status FROM jobs WHERE id = :id"), {"id": job_id}
            )
        assert status == "failed"
    finally:
        # Shared test database: leave it at head, as the tests above do.
        await migrate(test_settings, "up", "head")


async def jobs_columns(settings: Settings) -> set[str]:
    engine = create_db_engine(settings)
    try:
        async with engine.connect() as connection:
            columns = await connection.scalars(
                text("SELECT column_name FROM information_schema.columns WHERE table_name = 'jobs'")
            )
            return set(columns)
    finally:
        await engine.dispose()


async def test_existing_jobs_survive_0002_and_its_downgrade(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    # `engine` empties the tables first. A running row left by an earlier test would lose its
    # lease in the downgrade and then fail ck_jobs_running_has_lease on the way back up.
    new_columns = {"lease_expires_at", "error_code", "result"}
    try:
        await migrate(test_settings, "down", "0001")
        assert not new_columns & await jobs_columns(test_settings)
        # A job saved before phase 3, as the API wrote it.
        async with engine.begin() as connection:
            job_id = await connection.scalar(
                text(
                    "INSERT INTO jobs (type, payload_version, payload, max_attempts)"
                    " VALUES ('notification.send', 1, '{}', 5) RETURNING id"
                )
            )

        await migrate(test_settings, "up", "0002")
        assert new_columns <= await jobs_columns(test_settings)
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        "SELECT status, lease_expires_at, error_code, result FROM jobs"
                        " WHERE id = :id"
                    ),
                    {"id": job_id},
                )
            ).one()
        # No backfill: the new columns start empty, and a queued job needs no lease.
        assert tuple(row) == ("queued", None, None, None)

        # The downgrade keeps the row and drops only what 0002 added.
        await migrate(test_settings, "down", "0001")
        assert not new_columns & await jobs_columns(test_settings)
        async with engine.connect() as connection:
            assert (
                await connection.scalar(
                    text("SELECT count(*) FROM jobs WHERE id = :id"), {"id": job_id}
                )
                == 1
            )
    finally:
        # Shared test database: leave it at head, as the test above does.
        await migrate(test_settings, "up", "head")


async def test_existing_jobs_survive_0003_and_its_downgrade(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    # ADR 0005 decision 7: no backfill. A job that already ran keeps its attempts count but gets no
    # attempt rows; the downgrade drops the history and keeps the job.
    try:
        await migrate(test_settings, "down", "0002")
        assert "job_attempts" not in await schema_objects(test_settings)
        async with engine.begin() as connection:
            job_id = await connection.scalar(
                text(
                    "INSERT INTO jobs (type, payload_version, payload, max_attempts, status,"
                    " attempts, result) VALUES ('notification.send', 1, '{}', 5, 'succeeded', 2,"
                    " '{}') RETURNING id"
                )
            )

        await migrate(test_settings, "up", "0003")
        async with engine.begin() as connection:
            job = (
                await connection.execute(
                    text("SELECT status, attempts FROM jobs WHERE id = :id"), {"id": job_id}
                )
            ).one()
            rows = await connection.scalar(
                text("SELECT count(*) FROM job_attempts WHERE job_id = :id"), {"id": job_id}
            )
            assert (tuple(job), rows) == (("succeeded", 2), 0)
            # A later attempt row, so the downgrade has history to drop.
            await connection.execute(
                text("INSERT INTO job_attempts (job_id, attempt) VALUES (:id, 3)"), {"id": job_id}
            )

        await migrate(test_settings, "down", "0002")
        assert "job_attempts" not in await schema_objects(test_settings)
        async with engine.connect() as connection:
            status = await connection.scalar(
                text("SELECT status FROM jobs WHERE id = :id"), {"id": job_id}
            )
        assert status == "succeeded"
    finally:
        # Shared test database: leave it at head, as the tests above do.
        await migrate(test_settings, "up", "head")
