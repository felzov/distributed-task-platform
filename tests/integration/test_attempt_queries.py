"""Queries on job_attempts against a real PostgreSQL (ADR 0005 decision 5)."""

import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform.data.attempts import close_attempt, open_attempt, select_attempts
from task_platform.domain.attempts import AttemptOutcome
from task_platform.domain.failures import FailureCode
from tests.integration.rows import insert_attempt, insert_job

pytestmark = pytest.mark.integration


async def attempt_rows(engine: AsyncEngine, job_id: uuid.UUID) -> list[dict[str, Any]]:
    async with engine.connect() as connection:
        result = await connection.execute(
            text("SELECT * FROM job_attempts WHERE job_id = :id ORDER BY attempt"), {"id": job_id}
        )
        return [dict(row) for row in result.mappings()]


# open_attempt


async def test_opening_an_attempt_records_its_start(engine: AsyncEngine) -> None:
    job = await insert_job(engine)
    sessions = async_sessionmaker(engine)
    async with sessions.begin() as session:
        await open_attempt(session, job["id"], attempt=1)
        now = await session.scalar(text("SELECT now()"))

    [row] = await attempt_rows(engine, job["id"])
    # now() is the transaction's start, the same instant the claim's own update uses.
    assert row["attempt"] == 1
    assert row["started_at"] == now
    assert (row["finished_at"], row["outcome"], row["error_code"]) == (None, None, None)


async def test_each_attempt_gets_its_own_row(engine: AsyncEngine) -> None:
    job = await insert_job(engine)
    sessions = async_sessionmaker(engine)
    for attempt in (1, 2):
        async with sessions.begin() as session:
            await open_attempt(session, job["id"], attempt=attempt)

    assert [row["attempt"] for row in await attempt_rows(engine, job["id"])] == [1, 2]


async def test_an_attempt_rolled_back_with_its_transaction_leaves_no_row(
    engine: AsyncEngine,
) -> None:
    # The claim's transaction owns the insert: a claim that does not commit records nothing.
    job = await insert_job(engine)
    session: AsyncSession
    async with async_sessionmaker(engine)() as session:
        await open_attempt(session, job["id"], attempt=1)
        await session.rollback()

    assert await attempt_rows(engine, job["id"]) == []


async def test_opening_the_same_attempt_twice_fails(engine: AsyncEngine) -> None:
    job = await insert_job(engine)
    sessions = async_sessionmaker(engine)
    async with sessions.begin() as session:
        await open_attempt(session, job["id"], attempt=1)

    with pytest.raises(IntegrityError, match="pk_job_attempts"):
        async with sessions.begin() as session:
            await open_attempt(session, job["id"], attempt=1)
    assert len(await attempt_rows(engine, job["id"])) == 1


async def test_opening_an_attempt_of_an_unknown_job_fails(engine: AsyncEngine) -> None:
    with pytest.raises(IntegrityError, match="fk_job_attempts_job_id"):
        async with async_sessionmaker(engine).begin() as session:
            await open_attempt(session, uuid.uuid4(), attempt=1)


# close_attempt


async def opened(engine: AsyncEngine, *attempts: int) -> uuid.UUID:
    """A job with these attempts open, each opened in its own transaction."""
    job_id: uuid.UUID = (await insert_job(engine))["id"]
    sessions = async_sessionmaker(engine)
    for attempt in attempts:
        async with sessions.begin() as session:
            await open_attempt(session, job_id, attempt=attempt)
    return job_id


async def close(
    engine: AsyncEngine, job_id: uuid.UUID, attempt: int, failure: FailureCode | None
) -> bool:
    async with async_sessionmaker(engine).begin() as session:
        return await close_attempt(session, job_id, attempt=attempt, failure=failure)


async def close_reading_now(
    engine: AsyncEngine, job_id: uuid.UUID, failure: FailureCode | None
) -> datetime:
    """Close attempt 1 and return the closing transaction's now(), which finished_at must equal."""
    async with async_sessionmaker(engine).begin() as session:
        assert await close_attempt(session, job_id, attempt=1, failure=failure)
        now: datetime = await session.scalar(text("SELECT now()"))
    return now


async def test_a_successful_attempt_closes_without_a_code(engine: AsyncEngine) -> None:
    job_id = await opened(engine, 1)

    now = await close_reading_now(engine, job_id, None)
    [row] = await attempt_rows(engine, job_id)
    assert (row["outcome"], row["error_code"]) == ("succeeded", None)
    # PostgreSQL's clock in the finish's transaction (ADR 0001 decision 9), not started_at's.
    assert row["finished_at"] == now


@pytest.mark.parametrize("failure", list(FailureCode))
async def test_a_failed_attempt_closes_with_its_code(
    engine: AsyncEngine, failure: FailureCode
) -> None:
    job_id = await opened(engine, 1)

    now = await close_reading_now(engine, job_id, failure)
    [row] = await attempt_rows(engine, job_id)
    assert (row["outcome"], row["error_code"]) == ("failed", failure.value)
    assert row["finished_at"] == now


async def test_only_the_named_attempt_is_closed(engine: AsyncEngine) -> None:
    job_id = await opened(engine, 1, 2)
    # The same attempt numbers, so only the job_id condition keeps its attempt 2 open.
    other_job = await opened(engine, 1, 2)

    assert await close(engine, job_id, 2, FailureCode.HANDLER_ERROR)
    rows = await attempt_rows(engine, job_id) + await attempt_rows(engine, other_job)
    assert [(row["attempt"], row["outcome"]) for row in rows] == [
        (1, None),
        (2, "failed"),
        (1, None),
        (2, None),
    ]


async def test_a_closed_attempt_keeps_its_first_outcome(engine: AsyncEngine) -> None:
    job_id = await opened(engine, 1)
    assert await close(engine, job_id, 1, FailureCode.HANDLER_TIMEOUT)
    [before] = await attempt_rows(engine, job_id)

    assert not await close(engine, job_id, 1, None)
    assert await attempt_rows(engine, job_id) == [before]


@pytest.mark.parametrize(
    "missing",
    [
        pytest.param("attempt", id="attempt never opened (e.g. before migration 0003)"),
        pytest.param("job", id="unknown job"),
    ],
)
async def test_closing_an_attempt_without_a_row_changes_nothing(
    engine: AsyncEngine, missing: str
) -> None:
    job_id = await opened(engine, 1)
    target = job_id if missing == "attempt" else uuid.uuid4()

    assert not await close(engine, target, 2 if missing == "attempt" else 1, None)
    [row] = await attempt_rows(engine, job_id)
    assert row["outcome"] is None


async def test_outcome_constraint_lists_exactly_the_domain_outcomes(engine: AsyncEngine) -> None:
    # Catches drift in both directions, as for ck_jobs_status and JobStatus.
    async with engine.connect() as connection:
        definition = await connection.scalar(
            text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                " WHERE conname = 'ck_job_attempts_outcome'"
            )
        )
    assert definition is not None
    found = set(re.findall(r"'(\w+)'::text", definition))
    assert found == {outcome.value for outcome in AttemptOutcome}


# select_attempts

STARTED = datetime(2026, 1, 1, tzinfo=UTC)


async def selected(engine: AsyncEngine, job_id: uuid.UUID) -> list[tuple[Any, ...]]:
    async with async_sessionmaker(engine)() as session:
        return [tuple(row) for row in await select_attempts(session, job_id)]


async def test_a_jobs_attempts_are_returned_lowest_first_with_every_column(
    engine: AsyncEngine,
) -> None:
    job_id = (await insert_job(engine))["id"]
    finished = STARTED + timedelta(seconds=1)
    # Inserted out of order, so only ORDER BY can put them in order.
    await insert_attempt(engine, job_id=job_id, attempt=3, started_at=STARTED)
    await insert_attempt(
        engine,
        job_id=job_id,
        attempt=1,
        started_at=STARTED,
        finished_at=finished,
        outcome="failed",
        error_code=FailureCode.HANDLER_TIMEOUT,
    )
    await insert_attempt(
        engine,
        job_id=job_id,
        attempt=2,
        started_at=STARTED,
        finished_at=finished,
        outcome="succeeded",
    )

    assert await selected(engine, job_id) == [
        (1, STARTED, finished, "failed", "handler_timeout"),
        (2, STARTED, finished, "succeeded", None),
        # Still open: no finish, no outcome.
        (3, STARTED, None, None, None),
    ]


async def test_only_the_named_jobs_attempts_are_returned(engine: AsyncEngine) -> None:
    job_id = (await insert_job(engine))["id"]
    other_job = (await insert_job(engine))["id"]
    for attempt in (1, 2):
        await insert_attempt(engine, job_id=other_job, attempt=attempt)
    await insert_attempt(engine, job_id=job_id, attempt=1)

    assert [row[0] for row in await selected(engine, job_id)] == [1]


async def test_a_history_may_start_after_attempt_1(engine: AsyncEngine) -> None:
    # A job that was running when migration 0003 ran: its earlier attempts have no rows.
    job_id = (await insert_job(engine))["id"]
    for attempt in (4, 3):
        await insert_attempt(engine, job_id=job_id, attempt=attempt)

    assert [row[0] for row in await selected(engine, job_id)] == [3, 4]


@pytest.mark.parametrize("known", [True, False], ids=["job without attempts", "unknown job"])
async def test_no_attempts_is_an_empty_list(engine: AsyncEngine, known: bool) -> None:
    job_id = (await insert_job(engine))["id"] if known else uuid.uuid4()

    assert await selected(engine, job_id) == []
