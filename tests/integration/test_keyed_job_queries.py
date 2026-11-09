"""`insert_job_or_find_by_key`: one job per idempotency key, also under concurrency (ADR 0007)."""

import asyncio
import contextlib
import hashlib
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform.data.db import create_db_engine
from task_platform.data.jobs import KeyedJob, insert_job, insert_job_or_find_by_key
from tests.integration.rows import insert_job as insert_raw_job
from tests.settings import make_test_settings

pytestmark = pytest.mark.integration

PAYLOAD = {"recipient": "max@example.com", "subject": "Hi", "body": "Hello"}
# Any 32 bytes stand for a fingerprint here; computing one is the domain's job.
FIRST_HASH = hashlib.sha256(b"first").digest()
OTHER_HASH = hashlib.sha256(b"other").digest()


async def insert_keyed(session: AsyncSession, key: str = "key-1", **overrides: Any) -> KeyedJob:
    values: dict[str, Any] = {
        "job_type": "notification.send",
        "payload_version": 1,
        "payload": PAYLOAD,
        "max_attempts": 5,
        "request_hash": FIRST_HASH,
    } | overrides
    return await insert_job_or_find_by_key(session, idempotency_key=key, **values)


async def insert_keyed_once(engine: AsyncEngine, key: str = "key-1", **overrides: Any) -> KeyedJob:
    async with async_sessionmaker(engine).begin() as session:
        return await insert_keyed(session, key, **overrides)


async def job_rows(engine: AsyncEngine) -> list[dict[str, Any]]:
    async with engine.connect() as connection:
        result = await connection.execute(
            text("SELECT id, status, payload, idempotency_key, request_hash FROM jobs")
        )
        return [dict(row) for row in result.mappings()]


# Normal path


async def test_a_new_key_saves_the_job_with_its_key_and_fingerprint(engine: AsyncEngine) -> None:
    job = await insert_keyed_once(engine)

    assert job.created
    assert job.status == "queued"
    assert job.request_hash == FIRST_HASH
    assert await job_rows(engine) == [
        {
            "id": job.id,
            "status": "queued",
            "payload": PAYLOAD,
            "idempotency_key": "key-1",
            "request_hash": FIRST_HASH,
        }
    ]


async def test_a_used_key_returns_the_first_job_and_its_fingerprint(engine: AsyncEngine) -> None:
    first = await insert_keyed_once(engine)
    # A different request under the same key: nothing of it is saved, and the caller gets the
    # first request's fingerprint to compare with its own.
    second = await insert_keyed_once(
        engine, payload=PAYLOAD | {"body": "Other"}, request_hash=OTHER_HASH
    )

    assert second == KeyedJob(first.id, "queued", created=False, request_hash=FIRST_HASH)
    rows = await job_rows(engine)
    assert [(row["id"], row["payload"]) for row in rows] == [(first.id, PAYLOAD)]


async def test_a_used_key_returns_the_jobs_current_status(engine: AsyncEngine) -> None:
    first = await insert_keyed_once(engine)
    async with engine.begin() as connection:
        await connection.execute(
            text("UPDATE jobs SET status = 'succeeded', result = '{}' WHERE id = :id"),
            {"id": first.id},
        )

    again = await insert_keyed_once(engine)

    assert (again.id, again.status, again.created) == (first.id, "succeeded", False)


async def test_other_keys_and_keyless_jobs_get_jobs_of_their_own(engine: AsyncEngine) -> None:
    keyless = await insert_raw_job(engine)
    first = await insert_keyed_once(engine, "key-1")
    second = await insert_keyed_once(engine, "key-2")
    async with async_sessionmaker(engine).begin() as session:
        unkeyed_id, _ = await insert_job(
            session,
            job_type="notification.send",
            payload_version=1,
            payload=PAYLOAD,
            max_attempts=5,
        )

    assert first.created and second.created
    assert len({keyless["id"], first.id, second.id, unkeyed_id}) == 4


# Failure path: concurrent requests with one key


async def test_concurrent_inserts_with_one_key_save_one_job(
    engine: AsyncEngine, test_database_url: str
) -> None:
    # A pool as large as the number of requests, so all of them can be in flight at once.
    requests = 10
    wide = create_db_engine(make_test_settings(test_database_url, db_pool_size=requests))
    try:
        jobs = await asyncio.gather(*(insert_keyed_once(wide) for _ in range(requests)))
    finally:
        await wide.dispose()

    assert sum(job.created for job in jobs) == 1
    assert {job.id for job in jobs} == {jobs[0].id}
    assert {job.request_hash for job in jobs} == {FIRST_HASH}
    assert len(await job_rows(engine)) == 1


async def test_an_insert_waits_for_an_uncommitted_insert_of_its_key(engine: AsyncEngine) -> None:
    # ADR 0007 decision 5, step by step: the second INSERT blocks on the first transaction's
    # uncommitted key, and after its COMMIT finds that job instead of saving one.
    sessions = async_sessionmaker(engine)
    second_task: asyncio.Task[KeyedJob] | None = None
    try:
        async with sessions.begin() as first_session:
            first = await insert_keyed(first_session)
            assert first.created
            first_pid = await first_session.scalar(text("SELECT pg_backend_pid()"))

            second_task = asyncio.create_task(insert_keyed_once(engine, request_hash=OTHER_HASH))
            await wait_until_blocked_by(engine, first_pid)
            assert not second_task.done()
        # Leaving the block committed the first transaction.

        second = await asyncio.wait_for(second_task, timeout=5)
    finally:
        # On a failure above, stop the second insert before the fixture disposes the engine.
        if second_task is not None and not second_task.done():
            second_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await second_task

    assert second == KeyedJob(first.id, "queued", created=False, request_hash=FIRST_HASH)
    assert len(await job_rows(engine)) == 1


class FirstRequestError(Exception):
    pass


async def test_an_insert_saves_the_job_if_the_first_insert_of_its_key_rolls_back(
    engine: AsyncEngine,
) -> None:
    # ADR 0007 decision 5, the other branch: the transaction holding the key rolls back (its
    # request timed out, its client went away), so the waiting INSERT goes ahead and saves its own
    # job. A SELECT-then-INSERT would find nothing here and fail.
    sessions = async_sessionmaker(engine)
    second_task: asyncio.Task[KeyedJob] | None = None
    try:
        with pytest.raises(FirstRequestError):
            async with sessions.begin() as first_session:
                assert (await insert_keyed(first_session)).created
                first_pid = await first_session.scalar(text("SELECT pg_backend_pid()"))

                second_task = asyncio.create_task(
                    insert_keyed_once(engine, request_hash=OTHER_HASH)
                )
                await wait_until_blocked_by(engine, first_pid)
                assert not second_task.done()
                # Leaving the block with an exception rolls the first transaction back.
                raise FirstRequestError

        # Set inside the block above, which mypy cannot follow through pytest.raises.
        assert second_task is not None
        second = await asyncio.wait_for(second_task, timeout=5)
    finally:
        if second_task is not None and not second_task.done():
            second_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await second_task

    assert second.created
    assert second.request_hash == OTHER_HASH
    rows = await job_rows(engine)
    assert [(row["id"], row["request_hash"]) for row in rows] == [(second.id, OTHER_HASH)]


async def wait_until_blocked_by(engine: AsyncEngine, pid: int) -> None:
    """Return once a backend waits for a lock held by backend `pid`; fail after 5 s."""
    async with asyncio.timeout(5), engine.connect() as connection:
        while True:
            blocked = await connection.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity"
                    " WHERE CAST(:pid AS integer) = ANY(pg_blocking_pids(pid))"
                ),
                {"pid": pid},
            )
            if blocked:
                return
            # Each SELECT in its own snapshot of pg_stat_activity.
            await connection.rollback()
            await asyncio.sleep(0.02)


# Invalid input: other constraint violations still raise
#
# ON CONFLICT never absorbs a CHECK violation, whatever its target. The target (the key) matters
# only once jobs has a second unique constraint a client value can hit; today it has none (the
# primary key is generated by PostgreSQL), so no test here can tell the target apart.


async def test_other_constraint_violations_still_raise(engine: AsyncEngine) -> None:
    with pytest.raises(IntegrityError, match="ck_jobs_max_attempts_range"):
        await insert_keyed_once(engine, max_attempts=0)
    with pytest.raises(IntegrityError, match="ck_jobs_request_hash_length"):
        await insert_keyed_once(engine, request_hash=b"short")
    assert await job_rows(engine) == []
