"""`send_fake_notification`: one notification per key, as a provider that dedupes by key (ADR 0007
decision 8)."""

import asyncio
import contextlib
import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from task_platform.data.db import create_db_engine
from task_platform.data.fake_notifications import send_fake_notification
from tests.integration.locks import wait_until_blocked_by
from tests.integration.rows import insert_job
from tests.settings import make_test_settings

pytestmark = pytest.mark.integration


async def send(engine: AsyncEngine, key: str, job_id: uuid.UUID, attempt: int = 1) -> uuid.UUID:
    async with async_sessionmaker(engine).begin() as session:
        return await send_fake_notification(
            session, idempotency_key=key, job_id=job_id, attempt=attempt
        )


async def notification_rows(engine: AsyncEngine) -> list[dict[str, Any]]:
    async with engine.connect() as connection:
        result = await connection.execute(
            text("SELECT id, job_id, attempt, idempotency_key FROM fake_notifications")
        )
        return [dict(row) for row in result.mappings()]


# Normal path


async def test_a_new_key_records_one_notification_and_returns_its_id(engine: AsyncEngine) -> None:
    job = await insert_job(engine)

    notification_id = await send(engine, "key-1", job["id"], attempt=2)

    assert await notification_rows(engine) == [
        {"id": notification_id, "job_id": job["id"], "attempt": 2, "idempotency_key": "key-1"}
    ]


async def test_a_repeated_key_returns_the_first_notification_and_records_nothing(
    engine: AsyncEngine,
) -> None:
    # A retry, or a stale attempt finishing late: a later attempt calls with the same key.
    job = await insert_job(engine)
    first = await send(engine, "key-1", job["id"], attempt=1)

    again = await send(engine, "key-1", job["id"], attempt=2)

    assert again == first
    # The row still names the attempt that made the first call.
    assert [(row["id"], row["attempt"]) for row in await notification_rows(engine)] == [(first, 1)]


async def test_other_keys_record_notifications_of_their_own(engine: AsyncEngine) -> None:
    job = await insert_job(engine)
    other_job = await insert_job(engine)

    ids = {
        await send(engine, "key-1", job["id"]),
        await send(engine, "key-2", job["id"]),
        await send(engine, "key-3", other_job["id"]),
    }

    assert len(ids) == 3
    assert len(await notification_rows(engine)) == 3


# Failure path: concurrent calls with one key


async def test_concurrent_calls_with_one_key_record_one_notification(
    engine: AsyncEngine, test_database_url: str
) -> None:
    # A stale attempt and a new one reaching the provider at once, and more.
    job = await insert_job(engine)
    calls = 10
    wide = create_db_engine(make_test_settings(test_database_url, db_pool_size=calls))
    try:
        ids = await asyncio.gather(
            *(send(wide, "key-1", job["id"], attempt=n) for n in range(1, calls + 1))
        )
    finally:
        await wide.dispose()

    assert len(set(ids)) == 1
    [row] = await notification_rows(engine)
    assert row["id"] == ids[0]


class FirstCallError(Exception):
    pass


@pytest.mark.parametrize("first_commits", [True, False], ids=["first commits", "first rolls back"])
async def test_a_call_waits_for_an_uncommitted_call_with_its_key(
    engine: AsyncEngine, first_commits: bool
) -> None:
    # Step by step: the second call blocks on the first one's uncommitted key. If the first
    # commits, the second returns its notification; if it rolls back, the second records its own.
    job = await insert_job(engine)
    sessions = async_sessionmaker(engine)
    second_task: asyncio.Task[uuid.UUID] | None = None
    try:
        with contextlib.suppress(FirstCallError):
            async with sessions.begin() as first_session:
                first = await send_fake_notification(
                    first_session, idempotency_key="key-1", job_id=job["id"], attempt=1
                )
                first_pid = await first_session.scalar(text("SELECT pg_backend_pid()"))

                second_task = asyncio.create_task(send(engine, "key-1", job["id"], attempt=2))
                await wait_until_blocked_by(engine, first_pid)
                assert not second_task.done()
                if not first_commits:
                    # Leaving the block with an exception rolls the first call back.
                    raise FirstCallError

        # Set inside the block above, which mypy cannot follow through suppress().
        assert second_task is not None
        second = await asyncio.wait_for(second_task, timeout=5)
    finally:
        if second_task is not None and not second_task.done():
            second_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await second_task

    [row] = await notification_rows(engine)
    if first_commits:
        assert (second, row["id"], row["attempt"]) == (first, first, 1)
    else:
        assert second != first
        assert (row["id"], row["attempt"]) == (second, 2)
