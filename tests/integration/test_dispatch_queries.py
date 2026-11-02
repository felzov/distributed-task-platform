"""The dispatcher's queries: which jobs are due, and recording a confirmed publish (ADR 0002)."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from task_platform.data.jobs import mark_dispatched, select_due_job_ids
from tests.integration.rows import insert_job

pytestmark = pytest.mark.integration

# Hours away from now, so the test machine's clock and PostgreSQL's never disagree about them.
PAST = datetime.now(UTC) - timedelta(hours=3)
FUTURE = datetime.now(UTC) + timedelta(hours=3)


async def due_ids(engine: AsyncEngine, limit: int = 10) -> list[uuid.UUID]:
    async with async_sessionmaker(engine).begin() as session:
        return await select_due_job_ids(session, limit=limit)


async def job_row(engine: AsyncEngine, job_id: uuid.UUID) -> dict[str, Any]:
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
        return dict(result.mappings().one())


# Normal path


async def test_due_jobs_come_oldest_first_up_to_the_limit(engine: AsyncEngine) -> None:
    newest = await insert_job(engine, available_at=PAST + timedelta(minutes=2))
    oldest = await insert_job(engine, available_at=PAST)
    middle = await insert_job(engine, available_at=PAST + timedelta(minutes=1))

    async with async_sessionmaker(engine).begin() as session:
        # A scan of ix_jobs_dispatch_due returns rows in available_at order anyway; without index
        # scans they come in insertion order, so only the query's ORDER BY sorts them.
        await session.execute(text("SET LOCAL enable_indexscan = off"))
        await session.execute(text("SET LOCAL enable_bitmapscan = off"))
        assert await select_due_job_ids(session, limit=2) == [oldest["id"], middle["id"]]
        assert await select_due_job_ids(session, limit=10) == [
            oldest["id"],
            middle["id"],
            newest["id"],
        ]


async def test_a_job_is_due_from_its_available_at(engine: AsyncEngine) -> None:
    # The column default is now(): a job submitted through the API is due at once.
    job = await insert_job(engine)
    assert await due_ids(engine) == [job["id"]]


async def test_mark_dispatched_records_the_publish(engine: AsyncEngine) -> None:
    job = await insert_job(engine)

    async with async_sessionmaker(engine).begin() as session:
        assert await mark_dispatched(session, [job["id"]]) == [job["id"]]

    row = await job_row(engine, job["id"])
    # Both from the same now(), in a later transaction than the insert.
    assert row["dispatched_at"] == row["updated_at"]
    assert row["dispatched_at"] > row["created_at"]
    assert row["status"] == "queued"
    assert await due_ids(engine) == []


async def test_mark_dispatched_with_no_ids_does_nothing(engine: AsyncEngine) -> None:
    async with async_sessionmaker(engine).begin() as session:
        assert await mark_dispatched(session, []) == []


# Jobs that are not due


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"status": "running", "lease_expires_at": FUTURE}, id="running"),
        pytest.param({"status": "succeeded"}, id="succeeded"),
        pytest.param({"status": "failed"}, id="failed"),
        pytest.param({"dispatched_at": PAST}, id="already dispatched"),
        pytest.param({"available_at": FUTURE}, id="not available yet"),
    ],
)
async def test_jobs_that_are_not_due_are_left_out(
    engine: AsyncEngine, overrides: dict[str, Any]
) -> None:
    due = await insert_job(engine)
    await insert_job(engine, **overrides)

    assert await due_ids(engine) == [due["id"]]


async def test_mark_dispatched_leaves_jobs_that_are_no_longer_due_alone(
    engine: AsyncEngine,
) -> None:
    due = await insert_job(engine)
    dispatched = await insert_job(engine, dispatched_at=PAST)
    running = await insert_job(engine, status="running", lease_expires_at=FUTURE)

    async with async_sessionmaker(engine).begin() as session:
        updated = await mark_dispatched(session, [due["id"], dispatched["id"], running["id"]])

    assert updated == [due["id"]]
    assert (await job_row(engine, dispatched["id"]))["dispatched_at"] == PAST
    assert (await job_row(engine, running["id"]))["dispatched_at"] is None


# Concurrency


async def test_locked_jobs_are_skipped_not_waited_for(engine: AsyncEngine) -> None:
    first = await insert_job(engine, available_at=PAST)
    second = await insert_job(engine, available_at=PAST + timedelta(minutes=1))
    sessions = async_sessionmaker(engine)

    async with sessions.begin() as holder:
        # The first dispatcher's transaction holds the oldest job's lock until it ends.
        assert await select_due_job_ids(holder, limit=1) == [first["id"]]

        # Without SKIP LOCKED the second dispatcher would block on that lock until the
        # holder ends (here: never, so the timeout fails the test).
        async with asyncio.timeout(2), sessions.begin() as other:
            assert await select_due_job_ids(other, limit=10) == [second["id"]]

    # Once the holder's transaction has ended, its job is due again.
    assert await due_ids(engine) == [first["id"], second["id"]]
