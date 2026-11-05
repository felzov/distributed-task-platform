"""Queries on job_attempts against a real PostgreSQL (ADR 0005 decision 5)."""

import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform.data.attempts import open_attempt
from tests.integration.rows import insert_job

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
