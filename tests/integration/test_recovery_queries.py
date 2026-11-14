"""Lease recovery's queries: find expired leases, then requeue or fail (ADR 0008 decision 1)."""

import asyncio
import contextlib
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform.data.attempts import close_attempt, open_attempt
from task_platform.data.jobs import (
    claim_job,
    fail_expired_lease,
    mark_failed,
    mark_retry,
    mark_succeeded,
    requeue_expired_lease,
    select_expired_leases,
)
from task_platform.domain.failures import FailureCode
from tests.integration.locks import wait_until_blocked_by
from tests.integration.plans import generic_plan
from tests.integration.rows import insert_job

pytestmark = pytest.mark.integration

# Hours away from now, so the test machine's clock and PostgreSQL's never disagree about them.
PAST = datetime.now(UTC) - timedelta(hours=3)
FUTURE = datetime.now(UTC) + timedelta(hours=3)

# requeue_expired_lease or fail_expired_lease: (engine, job id, attempt) -> whether it applied.
Recover = Callable[[AsyncEngine, uuid.UUID, int], Awaitable[bool]]


async def requeue(engine: AsyncEngine, job_id: uuid.UUID, attempt: int) -> bool:
    async with async_sessionmaker(engine).begin() as session:
        return await requeue_expired_lease(session, job_id, attempt=attempt)


async def fail(engine: AsyncEngine, job_id: uuid.UUID, attempt: int) -> bool:
    async with async_sessionmaker(engine).begin() as session:
        return await fail_expired_lease(session, job_id, attempt=attempt)


async def expired_ids(engine: AsyncEngine, *, limit: int = 100) -> list[uuid.UUID]:
    async with async_sessionmaker(engine).begin() as session:
        return [row.id for row in await select_expired_leases(session, limit=limit)]


async def job_row(engine: AsyncEngine, job_id: uuid.UUID) -> dict[str, Any]:
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
        return dict(result.mappings().one())


async def insert_running(
    engine: AsyncEngine, *, lease_expires_at: datetime = PAST, attempts: int = 1, **overrides: Any
) -> dict[str, Any]:
    """A running job on `attempts`, as a claim leaves it, published long ago."""
    return await insert_job(
        engine,
        status="running",
        attempts=attempts,
        lease_expires_at=lease_expires_at,
        dispatched_at=PAST,
        **overrides,
    )


# Finding expired leases


async def test_expired_leases_come_earliest_first_up_to_the_limit(engine: AsyncEngine) -> None:
    later = await insert_running(engine, lease_expires_at=PAST + timedelta(minutes=2))
    earliest = await insert_running(engine, lease_expires_at=PAST, attempts=2, max_attempts=3)
    await insert_running(engine, lease_expires_at=PAST + timedelta(minutes=5))

    async with async_sessionmaker(engine).begin() as session:
        rows = await select_expired_leases(session, limit=2)

    # The attempt whose lease expired and the job's own budget, for the retry decision.
    assert [tuple(row) for row in rows] == [(earliest["id"], 2, 3), (later["id"], 1, 5)]


@pytest.mark.parametrize(
    "columns",
    [
        pytest.param({"status": "running", "lease_expires_at": FUTURE}, id="running, lease left"),
        # Rows the code never writes, since every finish clears the lease: only the status keeps
        # them out.
        pytest.param({"status": "queued", "attempts": 1, "lease_expires_at": PAST}, id="queued"),
        pytest.param(
            {"status": "succeeded", "attempts": 1, "lease_expires_at": PAST}, id="succeeded"
        ),
        pytest.param({"status": "failed", "attempts": 1, "lease_expires_at": PAST}, id="failed"),
    ],
)
async def test_jobs_without_an_expired_lease_are_left_out(
    engine: AsyncEngine, columns: dict[str, Any]
) -> None:
    await insert_job(engine, **({"attempts": 1} | columns))

    assert await expired_ids(engine) == []


async def test_locked_expired_leases_are_skipped_not_waited_for(engine: AsyncEngine) -> None:
    first = await insert_running(engine, lease_expires_at=PAST)
    second = await insert_running(engine, lease_expires_at=PAST + timedelta(minutes=1))
    sessions = async_sessionmaker(engine)

    async with sessions.begin() as holder:
        # One sweep's transaction holds the earliest job's lock until it ends.
        assert [row.id for row in await select_expired_leases(holder, limit=1)] == [first["id"]]

        # Without SKIP LOCKED a second sweep would block on that lock until the holder ends (here:
        # never, so the timeout fails the test). It takes the other job, so no job is recovered
        # twice (criterion 5).
        async with asyncio.timeout(2), sessions.begin() as other:
            assert [row.id for row in await select_expired_leases(other, limit=10)] == [
                second["id"]
            ]

    assert await expired_ids(engine) == [first["id"], second["id"]]


async def test_a_job_a_worker_is_finishing_is_skipped(engine: AsyncEngine) -> None:
    # A worker whose lease expired can still finish until the sweep takes the job (ADR 0008
    # decision 1). While its final update's transaction is open, the sweep skips the job.
    job = await insert_running(engine)
    sessions = async_sessionmaker(engine)

    async with sessions.begin() as worker:
        assert await mark_succeeded(worker, job["id"], claimed_attempt=1, result={"ok": True})
        async with asyncio.timeout(2), sessions.begin() as sweep:
            assert await select_expired_leases(sweep, limit=10) == []

    # Committed: the job is no longer running, so the sweep never finds it.
    assert await expired_ids(engine) == []
    assert (await job_row(engine, job["id"]))["status"] == "succeeded"


@pytest.mark.parametrize("requeued", [True, False], ids=["requeued", "failed"])
async def test_a_finish_that_waited_for_the_sweep_is_refused(
    engine: AsyncEngine, requeued: bool
) -> None:
    # The race ADR 0008 decision 1 relies on: the sweep holds the job's row lock, and a worker
    # whose lease expired tries to finish meanwhile. Its UPDATE waits for the lock; once the sweep
    # commits, PostgreSQL checks the finish's conditions again against the recovered row, and the
    # finish matches nothing.
    job = await insert_running(engine, attempts=1, max_attempts=2 if requeued else 1)
    sessions = async_sessionmaker(engine)
    finish: asyncio.Task[bool] | None = None
    try:
        async with sessions.begin() as sweep:
            [row] = await select_expired_leases(sweep, limit=10)
            recover = requeue_expired_lease if requeued else fail_expired_lease
            assert await recover(sweep, row.id, attempt=row.attempts)
            sweep_pid = await sweep.scalar(text("SELECT pg_backend_pid()"))

            finish = asyncio.create_task(_finish_attempt_1(engine, job["id"]))
            await wait_until_blocked_by(engine, sweep_pid)
            assert not finish.done()
        # Leaving the block committed the sweep.

        assert not await asyncio.wait_for(finish, timeout=5)
    finally:
        # On a failure above, stop the finish before the fixture disposes the engine.
        if finish is not None and not finish.done():
            finish.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await finish

    row_after = await job_row(engine, job["id"])
    assert (row_after["status"], row_after["error_code"], row_after["result"]) == (
        "queued" if requeued else "failed",
        "lease_expired",
        None,
    )


async def _finish_attempt_1(engine: AsyncEngine, job_id: uuid.UUID) -> bool:
    async with async_sessionmaker(engine).begin() as session:
        return await mark_succeeded(session, job_id, claimed_attempt=1, result={"ok": True})


async def test_the_expired_lease_query_keeps_its_index_under_a_generic_plan(
    engine: AsyncEngine,
) -> None:
    # The recovery loop runs this every pass, so its prepared statement soon gets a generic plan.
    # With the status bound as a parameter, that plan could not use the partial index.
    await insert_running(engine)

    plan = await generic_plan(engine, lambda session: select_expired_leases(session, limit=10))

    assert "ix_jobs_lease_expiry" in plan


# Requeue and fail


async def test_requeue_makes_the_job_due_now_and_unpublished(engine: AsyncEngine) -> None:
    job = await insert_running(engine, error_code="handler_error")

    assert await requeue(engine, job["id"], 1)

    row = await job_row(engine, job["id"])
    assert row["status"] == "queued"
    assert row["error_code"] == "lease_expired"
    # Due now, with no backoff: the same now() as the update.
    assert row["available_at"] == row["updated_at"]
    assert row["updated_at"] > job["updated_at"]
    assert row["dispatched_at"] is None
    assert row["lease_expires_at"] is None
    # The attempt was counted at its claim; recovery does not count it again or give it back.
    assert row["attempts"] == 1
    assert row["result"] is None


async def test_requeue_refuses_the_jobs_last_attempt(engine: AsyncEngine) -> None:
    job = await insert_running(engine, attempts=3, max_attempts=3)

    assert not await requeue(engine, job["id"], 3)

    assert await job_row(engine, job["id"]) == job


async def test_fail_ends_the_job_with_lease_expired(engine: AsyncEngine) -> None:
    job = await insert_running(engine, attempts=3, max_attempts=3)

    assert await fail(engine, job["id"], 3)

    row = await job_row(engine, job["id"])
    assert row["status"] == "failed"
    assert row["error_code"] == "lease_expired"
    assert row["lease_expires_at"] is None
    assert row["attempts"] == 3
    assert row["result"] is None
    assert row["updated_at"] > job["updated_at"]
    # Still published: a failed job is never dispatched again.
    assert row["dispatched_at"] == PAST


@pytest.mark.parametrize("recover", [requeue, fail])
@pytest.mark.parametrize(
    ("columns", "attempt"),
    [
        pytest.param({"lease_expires_at": FUTURE}, 1, id="lease left"),
        pytest.param({}, 2, id="another attempt"),
        pytest.param({"status": "queued", "lease_expires_at": PAST}, 1, id="queued"),
        pytest.param({"status": "succeeded", "lease_expires_at": PAST}, 1, id="succeeded"),
        pytest.param({"status": "failed", "lease_expires_at": PAST}, 1, id="failed"),
    ],
)
async def test_recovery_leaves_a_job_alone_unless_its_attempts_lease_expired(
    engine: AsyncEngine, recover: Recover, columns: dict[str, Any], attempt: int
) -> None:
    job = await insert_job(
        engine,
        **({"status": "running", "attempts": 1, "lease_expires_at": PAST} | columns),
    )

    assert not await recover(engine, job["id"], attempt)

    assert await job_row(engine, job["id"]) == job


async def test_the_attempt_row_is_closed_with_lease_expired(engine: AsyncEngine) -> None:
    # The service closes the attempt in the recovery's transaction with the existing query.
    job = await insert_running(engine)
    async with async_sessionmaker(engine).begin() as session:
        await open_attempt(session, job["id"], attempt=1)
    async with async_sessionmaker(engine).begin() as session:
        assert await requeue_expired_lease(session, job["id"], attempt=1)
        assert await close_attempt(session, job["id"], attempt=1, failure=FailureCode.LEASE_EXPIRED)

    async with engine.connect() as connection:
        row = (
            await connection.execute(
                text(
                    "SELECT outcome, error_code, finished_at IS NOT NULL AS finished"
                    " FROM job_attempts WHERE job_id = :id"
                ),
                {"id": job["id"]},
            )
        ).one()
    assert tuple(row) == ("failed", "lease_expired", True)


# A worker whose lease expired, finishing after the recovery


async def stale_finishes(session: AsyncSession, job_id: uuid.UUID) -> list[bool]:
    """Attempt 1's every possible finish; whether each applied."""
    return [
        await mark_succeeded(session, job_id, claimed_attempt=1, result={"ok": True}),
        await mark_failed(session, job_id, claimed_attempt=1, error_code=FailureCode.HANDLER_ERROR),
        await mark_retry(
            session, job_id, claimed_attempt=1, error_code=FailureCode.HANDLER_ERROR, delay_s=60
        ),
    ]


@pytest.mark.parametrize("claimed_again", [False, True], ids=["requeued", "claimed again"])
async def test_a_stale_worker_cannot_finish_a_recovered_job(
    engine: AsyncEngine, claimed_again: bool
) -> None:
    job = await insert_running(engine)
    assert await requeue(engine, job["id"], 1)
    if claimed_again:
        async with async_sessionmaker(engine).begin() as session:
            assert await claim_job(session, job["id"], lease_duration_s=30) is not None
    recovered = await job_row(engine, job["id"])

    async with async_sessionmaker(engine).begin() as session:
        assert await stale_finishes(session, job["id"]) == [False, False, False]

    assert await job_row(engine, job["id"]) == recovered


async def test_a_stale_worker_cannot_finish_a_job_failed_by_recovery(engine: AsyncEngine) -> None:
    job = await insert_running(engine, attempts=1, max_attempts=1)
    assert await fail(engine, job["id"], 1)
    failed = await job_row(engine, job["id"])

    async with async_sessionmaker(engine).begin() as session:
        assert await stale_finishes(session, job["id"]) == [False, False, False]

    assert await job_row(engine, job["id"]) == failed
