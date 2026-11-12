"""Lost-message recovery's queries: find jobs published long ago and still queued, then mark them
unpublished again (ADR 0002 decision 7, ADR 0008 decision 2)."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from task_platform.data.jobs import (
    clear_lost_dispatches,
    select_due_job_ids,
    select_lost_message_job_ids,
)
from tests.integration.plans import generic_plan
from tests.integration.rows import insert_job

pytestmark = pytest.mark.integration

# Hours away from now, so the test machine's clock and PostgreSQL's never disagree about them.
PAST = datetime.now(UTC) - timedelta(hours=3)
# The threshold every test passes: one hour. PAST is older, RECENT younger.
LOST_AFTER_S = 3600.0
RECENT = datetime.now(UTC) - timedelta(minutes=5)


async def lost_ids(engine: AsyncEngine, *, limit: int = 100) -> list[uuid.UUID]:
    async with async_sessionmaker(engine).begin() as session:
        return await select_lost_message_job_ids(session, lost_after_s=LOST_AFTER_S, limit=limit)


async def clear(engine: AsyncEngine, job_ids: list[uuid.UUID]) -> list[uuid.UUID]:
    async with async_sessionmaker(engine).begin() as session:
        return await clear_lost_dispatches(session, job_ids, lost_after_s=LOST_AFTER_S)


async def job_row(engine: AsyncEngine, job_id: uuid.UUID) -> dict[str, Any]:
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
        return dict(result.mappings().one())


async def insert_published(
    engine: AsyncEngine, *, dispatched_at: datetime = PAST, **overrides: Any
) -> dict[str, Any]:
    """A queued job, due long ago, whose message went out at `dispatched_at`."""
    return await insert_job(
        engine, available_at=PAST - timedelta(hours=1), dispatched_at=dispatched_at, **overrides
    )


# Finding them


async def test_lost_messages_come_earliest_publish_first_up_to_the_limit(
    engine: AsyncEngine,
) -> None:
    later = await insert_published(engine, dispatched_at=PAST + timedelta(minutes=2))
    earliest = await insert_published(engine, dispatched_at=PAST)
    await insert_published(engine, dispatched_at=PAST + timedelta(minutes=5))

    assert await lost_ids(engine, limit=2) == [earliest["id"], later["id"]]


@pytest.mark.parametrize(
    "columns",
    [
        pytest.param({"dispatched_at": RECENT}, id="published within the threshold"),
        pytest.param({"dispatched_at": None}, id="not published yet"),
        # Published long ago, but no longer queued: only the status keeps them out.
        pytest.param({"status": "running", "attempts": 1, "lease_expires_at": PAST}, id="running"),
        pytest.param({"status": "succeeded", "attempts": 1}, id="succeeded"),
        pytest.param({"status": "failed", "attempts": 1}, id="failed"),
    ],
)
async def test_jobs_whose_message_is_not_overdue_are_left_out(
    engine: AsyncEngine, columns: dict[str, Any]
) -> None:
    await insert_published(engine, **columns)

    assert await lost_ids(engine) == []


async def test_locked_jobs_are_skipped_not_waited_for(engine: AsyncEngine) -> None:
    first = await insert_published(engine, dispatched_at=PAST)
    second = await insert_published(engine, dispatched_at=PAST + timedelta(minutes=1))
    sessions = async_sessionmaker(engine)

    async with sessions.begin() as holder:
        # One sweep's transaction holds the earliest job's lock until it ends.
        assert await select_lost_message_job_ids(holder, lost_after_s=LOST_AFTER_S, limit=1) == [
            first["id"]
        ]

        # Without SKIP LOCKED a second sweep would block on that lock until the holder ends (here:
        # never, so the timeout fails the test). It takes the other job, so no job is cleared twice.
        async with asyncio.timeout(2), sessions.begin() as other:
            assert await select_lost_message_job_ids(
                other, lost_after_s=LOST_AFTER_S, limit=10
            ) == [second["id"]]

    assert await lost_ids(engine) == [first["id"], second["id"]]


async def test_the_lost_message_query_keeps_its_index_under_a_generic_plan(
    engine: AsyncEngine,
) -> None:
    # The recovery loop runs this every pass, so its prepared statement soon gets a generic plan.
    # With the status bound as a parameter, that plan could not use the partial index.
    await insert_published(engine)

    plan = await generic_plan(
        engine,
        lambda session: select_lost_message_job_ids(session, lost_after_s=LOST_AFTER_S, limit=10),
    )

    assert "ix_jobs_dispatched" in plan


# Marking them unpublished


async def test_a_cleared_job_is_unpublished_and_due_for_the_next_poll(engine: AsyncEngine) -> None:
    job = await insert_published(engine)

    assert await clear(engine, [job["id"]]) == [job["id"]]

    row = await job_row(engine, job["id"])
    assert row["dispatched_at"] is None
    assert row["updated_at"] > job["updated_at"]
    # Nothing else changes: same status, attempt count, due time and code.
    unchanged = ("status", "attempts", "available_at", "error_code", "lease_expires_at")
    assert {column: row[column] for column in unchanged} == {
        column: job[column] for column in unchanged
    }
    async with async_sessionmaker(engine).begin() as session:
        assert await select_due_job_ids(session, limit=10) == [job["id"]]


@pytest.mark.parametrize(
    "columns",
    [
        # Published again by another dispatcher after a sweep read it, say.
        pytest.param({"dispatched_at": RECENT}, id="published within the threshold"),
        pytest.param({"dispatched_at": None}, id="already unpublished"),
        # Claimed meanwhile: its message was not lost after all.
        pytest.param({"status": "running", "attempts": 1, "lease_expires_at": PAST}, id="running"),
        pytest.param({"status": "succeeded", "attempts": 1}, id="succeeded"),
        pytest.param({"status": "failed", "attempts": 1}, id="failed"),
    ],
)
async def test_clearing_leaves_a_job_alone_unless_its_message_is_overdue(
    engine: AsyncEngine, columns: dict[str, Any]
) -> None:
    # Without the row lock a sweep takes: the update checks the conditions itself.
    job = await insert_published(engine, **columns)

    assert await clear(engine, [job["id"]]) == []

    assert await job_row(engine, job["id"]) == job


async def test_clearing_only_touches_the_ids_passed(engine: AsyncEngine) -> None:
    passed = await insert_published(engine)
    other = await insert_published(engine)

    assert await clear(engine, [passed["id"], uuid.uuid4()]) == [passed["id"]]
    assert await clear(engine, []) == []

    assert await job_row(engine, other["id"]) == other
