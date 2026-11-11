"""The worker's queries: claim, and the fenced finish or retry (ADR 0001)."""

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from task_platform.data.jobs import (
    ClaimedJobRow,
    claim_job,
    mark_failed,
    mark_retry,
    mark_succeeded,
)
from task_platform.domain.failures import FailureCode
from tests.integration.rows import insert_job

pytestmark = pytest.mark.integration

# Hours away from now, so the test machine's clock and PostgreSQL's never disagree about them.
PAST = datetime.now(UTC) - timedelta(hours=3)
FUTURE = datetime.now(UTC) + timedelta(hours=3)
LEASE_S = 30.0
RESULT = {"delivered": True}

# succeed, fail or retry: (engine, job id, claimed attempt) -> whether the write applied.
Finish = Callable[[AsyncEngine, uuid.UUID, int], Awaitable[bool]]


async def claim(engine: AsyncEngine, job_id: uuid.UUID) -> ClaimedJobRow | None:
    async with async_sessionmaker(engine).begin() as session:
        return await claim_job(session, job_id, lease_duration_s=LEASE_S)


async def succeed(engine: AsyncEngine, job_id: uuid.UUID, attempt: int) -> bool:
    async with async_sessionmaker(engine).begin() as session:
        return await mark_succeeded(session, job_id, claimed_attempt=attempt, result=RESULT)


async def fail(engine: AsyncEngine, job_id: uuid.UUID, attempt: int) -> bool:
    async with async_sessionmaker(engine).begin() as session:
        return await mark_failed(
            session, job_id, claimed_attempt=attempt, error_code=FailureCode.INVALID_PAYLOAD
        )


async def retry(
    engine: AsyncEngine,
    job_id: uuid.UUID,
    attempt: int,
    delay_s: float = 60.0,
    error_code: FailureCode = FailureCode.HANDLER_ERROR,
) -> bool:
    async with async_sessionmaker(engine).begin() as session:
        return await mark_retry(
            session, job_id, claimed_attempt=attempt, error_code=error_code, delay_s=delay_s
        )


async def job_row(engine: AsyncEngine, job_id: uuid.UUID) -> dict[str, Any]:
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
        return dict(result.mappings().one())


async def recover(engine: AsyncEngine, job_id: uuid.UUID) -> None:
    # The part of lease recovery the fence cares about, written directly so the lease need not
    # expire first. Recovery's own queries meet stale finishes in test_recovery_queries.py.
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "UPDATE jobs SET status = 'queued', lease_expires_at = NULL, dispatched_at = NULL"
                " WHERE id = :id"
            ),
            {"id": job_id},
        )


# The claim


async def test_claim_starts_an_attempt_with_a_lease(engine: AsyncEngine) -> None:
    job = await insert_job(engine, dispatched_at=PAST)

    claimed = await claim(engine, job["id"])

    assert claimed is not None
    assert tuple(claimed) == (1, 5, "notification.send", 1, {"recipient": "max@example.com"})
    row = await job_row(engine, job["id"])
    assert row["status"] == "running"
    assert row["attempts"] == 1
    # Both from the same now(): the lease is PostgreSQL's clock plus the duration.
    assert row["lease_expires_at"] - row["updated_at"] == timedelta(seconds=LEASE_S)
    assert row["updated_at"] > row["created_at"]
    assert row["dispatched_at"] == PAST


async def test_each_claim_counts_one_more_attempt(engine: AsyncEngine) -> None:
    # A job that already had two attempts, as phase 5 retries will leave it.
    job = await insert_job(engine, attempts=2)

    claimed = await claim(engine, job["id"])

    assert claimed is not None
    assert claimed.attempts == 3
    assert (await job_row(engine, job["id"]))["attempts"] == 3


async def test_claim_returns_the_jobs_own_attempt_budget(engine: AsyncEngine) -> None:
    # The budget stored at submission, not the current default_max_attempts (5 in the tests):
    # the retry decision must use the one the job was given (ADR 0005 decision 2).
    job = await insert_job(engine, max_attempts=3)

    claimed = await claim(engine, job["id"])

    assert claimed is not None
    assert (claimed.attempts, claimed.max_attempts) == (1, 3)


async def test_a_claimed_job_cannot_be_claimed_again(engine: AsyncEngine) -> None:
    # A duplicate delivery of the same message.
    job = await insert_job(engine)
    assert await claim(engine, job["id"]) is not None
    claimed_row = await job_row(engine, job["id"])

    assert await claim(engine, job["id"]) is None
    assert await job_row(engine, job["id"]) == claimed_row


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param(
            {"status": "running", "attempts": 1, "lease_expires_at": FUTURE}, id="running"
        ),
        pytest.param({"status": "succeeded", "attempts": 1}, id="succeeded"),
        pytest.param({"status": "failed", "attempts": 1}, id="failed"),
        # A stale message for a job that waits for its retry backoff (ADR 0001 decision 7).
        pytest.param({"available_at": FUTURE}, id="not available yet"),
        # Unreachable through ADR 0001's transitions, but nothing in the schema forbids it. Without
        # the guard, attempts + 1 breaks ck_jobs_attempts_within_max, and the worker's DB-error
        # path would requeue the message forever (nack is not counted, ADR 0004).
        pytest.param({"attempts": 5, "max_attempts": 5}, id="no attempts left"),
    ],
)
async def test_jobs_that_are_not_claimable_are_left_alone(
    engine: AsyncEngine, overrides: dict[str, Any]
) -> None:
    job = await insert_job(engine, **overrides)

    assert await claim(engine, job["id"]) is None
    assert await job_row(engine, job["id"]) == job


async def test_claiming_an_unknown_job_returns_none(engine: AsyncEngine) -> None:
    assert await claim(engine, uuid.uuid4()) is None


async def test_of_two_concurrent_claims_only_the_first_wins(engine: AsyncEngine) -> None:
    job = await insert_job(engine)
    sessions = async_sessionmaker(engine)

    second: asyncio.Task[ClaimedJobRow | None] | None = None
    try:
        async with sessions.begin() as first:
            assert await claim_job(first, job["id"], lease_duration_s=LEASE_S) is not None

            # The second claim reads `queued` too, then has to wait for the first's row lock.
            second = asyncio.create_task(claim(engine, job["id"]))
            async with asyncio.timeout(2):
                while not await lock_waiters(engine):
                    await asyncio.sleep(0.01)
            assert not second.done()
        # The first has committed: PostgreSQL evaluates the second's WHERE again, on the new row.

        async with asyncio.timeout(2):
            assert await second is None
        assert (await job_row(engine, job["id"]))["attempts"] == 1
    finally:
        # On a failure above, stop the second claim so it cannot outlive the test unawaited.
        if second is not None and not second.done():
            second.cancel()
            await asyncio.gather(second, return_exceptions=True)


async def lock_waiters(engine: AsyncEngine) -> int:
    async with engine.connect() as connection:
        count = await connection.scalar(
            text(
                # Only this test's claims, not lock waits from other sessions on the database.
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
                " AND wait_event_type = 'Lock' AND query LIKE 'UPDATE jobs SET status=%'"
            )
        )
        return int(count or 0)


# The fenced finish


async def test_mark_succeeded_finishes_the_attempt(engine: AsyncEngine) -> None:
    job = await insert_job(engine)
    await claim(engine, job["id"])
    claimed_at = (await job_row(engine, job["id"]))["updated_at"]

    assert await succeed(engine, job["id"], attempt=1)

    row = await job_row(engine, job["id"])
    assert row["status"] == "succeeded"
    assert row["result"] == RESULT
    assert row["error_code"] is None
    assert row["lease_expires_at"] is None
    assert row["attempts"] == 1
    assert row["updated_at"] > claimed_at


async def test_success_after_a_failed_attempt_clears_its_code(engine: AsyncEngine) -> None:
    # What a retry leaves: queued again, attempt 1 counted, its failure's code still on the job.
    job = await insert_job(engine, attempts=1, error_code="handler_error")
    await claim(engine, job["id"])

    assert await succeed(engine, job["id"], attempt=2)

    row = await job_row(engine, job["id"])
    assert (row["status"], row["error_code"], row["result"]) == ("succeeded", None, RESULT)


async def test_mark_failed_finishes_the_attempt(engine: AsyncEngine) -> None:
    job = await insert_job(engine)
    await claim(engine, job["id"])
    claimed_at = (await job_row(engine, job["id"]))["updated_at"]

    assert await fail(engine, job["id"], attempt=1)

    row = await job_row(engine, job["id"])
    assert row["status"] == "failed"
    assert row["error_code"] == "invalid_payload"
    assert row["result"] is None
    assert row["lease_expires_at"] is None
    assert row["attempts"] == 1
    assert row["updated_at"] > claimed_at


@pytest.mark.parametrize("finish", [succeed, fail, retry])
async def test_a_stale_attempt_cannot_finish_a_newer_one(
    engine: AsyncEngine, finish: Finish
) -> None:
    job = await insert_job(engine)
    await claim(engine, job["id"])
    # Attempt 1's lease expired; the job was recovered and claimed again.
    await recover(engine, job["id"])
    await claim(engine, job["id"])
    newer = await job_row(engine, job["id"])

    assert not await finish(engine, job["id"], 1)
    assert await job_row(engine, job["id"]) == newer

    # The fence is the attempt number, not a refusal of every write: attempt 2 can finish.
    assert await finish(engine, job["id"], 2)


@pytest.mark.parametrize("second_finish", [succeed, fail, retry])
async def test_a_finished_attempt_cannot_finish_again(
    engine: AsyncEngine, second_finish: Finish
) -> None:
    job = await insert_job(engine)
    await claim(engine, job["id"])
    assert await succeed(engine, job["id"], attempt=1)
    finished = await job_row(engine, job["id"])

    assert not await second_finish(engine, job["id"], 1)
    assert await job_row(engine, job["id"]) == finished


@pytest.mark.parametrize("finish", [succeed, fail, retry])
async def test_a_job_that_was_never_claimed_cannot_finish(
    engine: AsyncEngine, finish: Finish
) -> None:
    # attempts is 0, so only the status fence stops this write.
    job = await insert_job(engine)

    assert not await finish(engine, job["id"], 0)
    assert await job_row(engine, job["id"]) == job


@pytest.mark.parametrize("finish", [succeed, fail, retry])
async def test_finishing_an_unknown_job_returns_false(engine: AsyncEngine, finish: Finish) -> None:
    assert not await finish(engine, uuid.uuid4(), 1)


# The retry


@pytest.mark.parametrize("delay_s", [0.0, 1.5, 300.0])
async def test_mark_retry_requeues_the_job_after_its_delay(
    engine: AsyncEngine, delay_s: float
) -> None:
    # Published and claimed, as a delivered job is.
    job = await insert_job(engine, dispatched_at=PAST)
    await claim(engine, job["id"])
    claimed_at = (await job_row(engine, job["id"]))["updated_at"]

    assert await retry(engine, job["id"], attempt=1, delay_s=delay_s)

    row = await job_row(engine, job["id"])
    assert row["status"] == "queued"
    assert row["error_code"] == "handler_error"
    # Due from PostgreSQL's clock in the retry's own transaction, to the microsecond.
    assert row["available_at"] - row["updated_at"] == timedelta(seconds=delay_s)
    assert row["updated_at"] > claimed_at
    # Unpublished and unleased: the dispatcher publishes it again once it is due.
    assert (row["dispatched_at"], row["lease_expires_at"]) == (None, None)
    # The attempt that failed stays counted, so the next claim is attempt 2.
    assert (row["attempts"], row["result"]) == (1, None)


async def test_a_retry_replaces_an_earlier_failures_code(engine: AsyncEngine) -> None:
    # The job keeps its last failure's code while it waits (ADR 0005 decision 6), not its first.
    job = await insert_job(engine, attempts=1, error_code="handler_error")
    await claim(engine, job["id"])

    assert await retry(engine, job["id"], attempt=2, error_code=FailureCode.HANDLER_TIMEOUT)

    assert (await job_row(engine, job["id"]))["error_code"] == "handler_timeout"


async def test_a_requeued_job_is_claimed_again_as_the_next_attempt(engine: AsyncEngine) -> None:
    job = await insert_job(engine)
    await claim(engine, job["id"])
    assert await retry(engine, job["id"], attempt=1, delay_s=0.0)

    claimed = await claim(engine, job["id"])

    assert claimed is not None
    assert claimed.attempts == 2


async def test_a_requeued_job_waits_for_its_delay(engine: AsyncEngine) -> None:
    job = await insert_job(engine)
    await claim(engine, job["id"])
    assert await retry(engine, job["id"], attempt=1, delay_s=3600.0)

    assert await claim(engine, job["id"]) is None


async def test_the_last_attempt_cannot_be_requeued(engine: AsyncEngine) -> None:
    # attempts reached max_attempts: a queued job no claim could take. Only the budget refuses this
    # write; status and attempt match.
    job = await insert_job(engine, max_attempts=2, attempts=1)
    await claim(engine, job["id"])
    running = await job_row(engine, job["id"])
    assert running["attempts"] == 2

    assert not await retry(engine, job["id"], attempt=2)
    assert await job_row(engine, job["id"]) == running
