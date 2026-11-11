"""Every write in `data/jobs.py` against a job in each status: only transitions the domain allows.

The domain's table (`ALLOWED_TRANSITIONS`, ADR 0001 decision 6) and the SQL conditions in the data
layer are two descriptions of one rule. This matrix runs each write on each status: a write either
applies, moving the job only where the table allows, or leaves the row exactly as it was. Staying
in the same status is not a transition (ADR 0001 decision 2), so a re-claim of a running job or a
rewrite of a finished one counts as a violation even though the status does not change. Every row
is the same apart from its status and passes every other condition of the writes (due, attempts
left, not yet published, the claimed attempt, an expired lease), so only the status decides. Some
rows are therefore ones the code never writes, such as a queued job with a lease.
"""

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform.data.jobs import (
    claim_job,
    fail_expired_lease,
    mark_dispatched,
    mark_failed,
    mark_retry,
    mark_succeeded,
    requeue_expired_lease,
)
from task_platform.domain.failures import FailureCode
from task_platform.domain.job_status import ALLOWED_TRANSITIONS, JobStatus
from tests.integration.rows import insert_job

pytestmark = pytest.mark.integration

PAST = datetime.now(UTC) - timedelta(hours=3)
# The attempt every row is seeded with, and the one the fenced writes claim to own.
ATTEMPT = 1

Write = Callable[[AsyncSession, uuid.UUID], Awaitable[object]]


async def claim(session: AsyncSession, job_id: uuid.UUID) -> object:
    return await claim_job(session, job_id, lease_duration_s=30)


async def dispatch(session: AsyncSession, job_id: uuid.UUID) -> object:
    return await mark_dispatched(session, [job_id])


async def succeed(session: AsyncSession, job_id: uuid.UUID) -> object:
    return await mark_succeeded(session, job_id, claimed_attempt=ATTEMPT, result={"ok": True})


async def fail(session: AsyncSession, job_id: uuid.UUID) -> object:
    return await mark_failed(
        session, job_id, claimed_attempt=ATTEMPT, error_code=FailureCode.HANDLER_ERROR
    )


async def retry(session: AsyncSession, job_id: uuid.UUID) -> object:
    return await mark_retry(
        session,
        job_id,
        claimed_attempt=ATTEMPT,
        error_code=FailureCode.HANDLER_ERROR,
        delay_s=60,
    )


async def requeue_expired(session: AsyncSession, job_id: uuid.UUID) -> object:
    return await requeue_expired_lease(session, job_id, attempt=ATTEMPT)


async def fail_expired(session: AsyncSession, job_id: uuid.UUID) -> object:
    return await fail_expired_lease(session, job_id, attempt=ATTEMPT)


WRITES: dict[str, Write] = {
    "claim_job": claim,
    "mark_dispatched": dispatch,
    "mark_succeeded": succeed,
    "mark_failed": fail,
    "mark_retry": retry,
    "requeue_expired_lease": requeue_expired,
    "fail_expired_lease": fail_expired,
}

# Where each write applies, and the status it leaves. Every other pair must not touch the row. Kept
# apart from the domain table so the test also notices a write that stops applying where it should
# (a guard made too strict). mark_dispatched records a publish; it changes no status.
APPLIES: dict[tuple[str, JobStatus], JobStatus] = {
    ("claim_job", JobStatus.QUEUED): JobStatus.RUNNING,
    ("mark_dispatched", JobStatus.QUEUED): JobStatus.QUEUED,
    ("mark_succeeded", JobStatus.RUNNING): JobStatus.SUCCEEDED,
    ("mark_failed", JobStatus.RUNNING): JobStatus.FAILED,
    ("mark_retry", JobStatus.RUNNING): JobStatus.QUEUED,
    ("requeue_expired_lease", JobStatus.RUNNING): JobStatus.QUEUED,
    ("fail_expired_lease", JobStatus.RUNNING): JobStatus.FAILED,
}
# The only writes allowed to apply without a transition. Any other write that keeps the status
# while changing the row is a re-claim or a rewrite, even if APPLIES were edited to allow it.
STATUS_PRESERVING = frozenset({"mark_dispatched"})


async def seed(engine: AsyncEngine, status: JobStatus) -> uuid.UUID:
    columns: dict[str, Any] = {
        "status": status,
        "attempts": ATTEMPT,
        "available_at": PAST,
        # The schema requires a lease only for running, but every row gets one, so a finish fenced
        # by the lease instead of the status fails here. Expired, so lease recovery's condition
        # holds too; a worker's finish does not look at the lease, so it still applies (ADR 0008
        # decision 1: until the sweep runs, a worker whose lease expired can still finish).
        "lease_expires_at": PAST,
        # dispatched_at stays NULL on every row, finished ones too, so only mark_dispatched's status
        # condition can refuse them. A realistic published row would hide that condition.
    }
    return uuid.UUID(str((await insert_job(engine, **columns))["id"]))


async def stored(engine: AsyncEngine, job_id: uuid.UUID) -> dict[str, Any]:
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
        return dict(result.mappings().one())


@pytest.mark.parametrize("before", list(JobStatus))
@pytest.mark.parametrize("write", list(WRITES))
async def test_a_write_moves_a_job_only_where_the_domain_allows(
    engine: AsyncEngine, write: str, before: JobStatus
) -> None:
    job_id = await seed(engine, before)
    row_before = await stored(engine, job_id)

    async with async_sessionmaker(engine).begin() as session:
        await WRITES[write](session, job_id)

    row_after = await stored(engine, job_id)
    after = JobStatus(row_after["status"])
    if (write, before) in APPLIES:
        assert after == APPLIES[(write, before)]
        if write in STATUS_PRESERVING:
            assert after == before
        else:
            assert after in ALLOWED_TRANSITIONS[before], f"{before} -> {after}"
        assert row_after != row_before
    else:
        assert row_after == row_before
