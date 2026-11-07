"""Queries on the jobs table. Each one is an explicit statement; callers own the transaction."""

import uuid
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Row, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import ReturningUpdate

from task_platform.data.models import Job
from task_platform.domain.failures import FailureCode
from task_platform.domain.job_status import JobStatus


async def insert_job(
    session: AsyncSession,
    *,
    job_type: str,
    payload_version: int,
    payload: dict[str, Any],
    max_attempts: int,
) -> tuple[uuid.UUID, str]:
    """Insert a new job and return its id and status, both as PostgreSQL assigned them."""
    # status, attempts and the timestamps are left to the column defaults in migration 0001.
    statement = (
        insert(Job)
        .values(
            type=job_type,
            payload_version=payload_version,
            payload=payload,
            max_attempts=max_attempts,
        )
        .returning(Job.id, Job.status)
    )
    row = (await session.execute(statement)).one()
    return row.id, row.status


# id, type, payload_version, status, attempts, max_attempts, available_at, created_at, updated_at,
# error_code, result
JobStateRow = Row[
    uuid.UUID,
    str,
    int,
    str,
    int,
    int,
    datetime,
    datetime,
    datetime,
    str | None,
    dict[str, Any] | None,
]


async def select_job(session: AsyncSession, job_id: uuid.UUID) -> JobStateRow | None:
    """Return a job's state, or None if there is no such job."""
    # Named columns, never `payload`: at DEBUG, SQLAlchemy logs result rows, and hide_parameters
    # covers only bound parameters. dispatched_at is the dispatcher's business (ADR 0002).
    statement = select(
        Job.id,
        Job.type,
        Job.payload_version,
        Job.status,
        Job.attempts,
        Job.max_attempts,
        Job.available_at,
        Job.created_at,
        Job.updated_at,
        Job.error_code,
        Job.result,
    ).where(Job.id == job_id)
    return (await session.execute(statement)).one_or_none()


async def select_due_job_ids(session: AsyncSession, *, limit: int) -> list[uuid.UUID]:
    """Lock and return up to `limit` jobs that are due for dispatch, oldest `available_at` first.

    Due: `queued`, not yet published, `available_at` reached (ADR 0002). The rows stay locked
    until the caller's transaction ends; rows another transaction holds are skipped, not waited
    for, so concurrent dispatchers never get the same job.
    """
    # Ids only: the message carries nothing else, and result rows are logged at DEBUG.
    statement = (
        select(Job.id)
        .where(
            Job.status == JobStatus.QUEUED,
            Job.dispatched_at.is_(None),
            Job.available_at <= func.now(),
        )
        .order_by(Job.available_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return list(await session.scalars(statement))


async def mark_dispatched(session: AsyncSession, job_ids: list[uuid.UUID]) -> list[uuid.UUID]:
    """Record a confirmed publish for these jobs. Returns the ids of the rows updated.

    Only jobs that are still `queued` and unpublished are updated, even if a caller passes other
    ids. `available_at` is not checked again: the caller holds the row locks from
    `select_due_job_ids`, so it cannot have changed.
    """
    if not job_ids:
        return []
    statement = (
        update(Job)
        .where(Job.id.in_(job_ids), Job.status == JobStatus.QUEUED, Job.dispatched_at.is_(None))
        .values(dispatched_at=func.now(), updated_at=func.now())
        # The ids, not a count: the dispatcher names any confirmed job it could not mark.
        .returning(Job.id)
    )
    return list(await session.scalars(statement))


# attempts (the claimed attempt, used to fence later writes), max_attempts (the job's own budget,
# for the retry decision: never the current setting, which may have changed since submission),
# type, payload_version, payload
ClaimedJobRow = Row[int, int, str, int, dict[str, Any]]


async def claim_job(
    session: AsyncSession, job_id: uuid.UUID, *, lease_duration_s: float
) -> ClaimedJobRow | None:
    """Move a due `queued` job to `running` for one attempt, or return None if it is not claimable.

    None is the normal outcome of a duplicate or stale delivery: the job is not due yet, or
    another worker already has it or finished it (ADR 0001 decision 7). Of two concurrent claims,
    the second waits for the first's row lock and then finds the job no longer `queued`. A `queued`
    job with no attempts left also returns None instead of breaking the attempts CHECK. So does an
    unknown job: a caller that treats that case differently (ADR 0004) has to look the job up.

    A caller that gets a row opens that attempt's history row (`open_attempt`) in the same
    transaction, so every committed claim has one (ADR 0005 decision 5).
    """
    statement = (
        update(Job)
        .where(
            Job.id == job_id,
            Job.status == JobStatus.QUEUED,
            Job.available_at <= func.now(),
            # Retries and recovery requeue only below the budget (ADR 0001); the database does not
            # enforce that for queued rows, and a CHECK error here would look like a DB outage.
            Job.attempts < Job.max_attempts,
        )
        .values(
            status=JobStatus.RUNNING,
            attempts=Job.attempts + 1,
            # A duration from the caller, a timestamp from PostgreSQL's clock (ADR 0001 decision 9).
            lease_expires_at=func.now() + timedelta(seconds=lease_duration_s),
            updated_at=func.now(),
        )
        # The handler needs the payload. Result rows are logged at DEBUG (limitation, phase 8).
        .returning(Job.attempts, Job.max_attempts, Job.type, Job.payload_version, Job.payload)
    )
    return (await session.execute(statement)).one_or_none()


async def mark_succeeded(
    session: AsyncSession, job_id: uuid.UUID, *, claimed_attempt: int, result: dict[str, Any]
) -> bool:
    """Finish a claimed attempt as `succeeded`; False if that attempt is no longer current."""
    statement = _fenced_finish(job_id, claimed_attempt).values(
        status=JobStatus.SUCCEEDED, result=result
    )
    return (await session.scalar(statement)) is not None


async def mark_failed(
    session: AsyncSession, job_id: uuid.UUID, *, claimed_attempt: int, error_code: FailureCode
) -> bool:
    """Finish a claimed attempt as `failed`; False if that attempt is no longer current."""
    statement = _fenced_finish(job_id, claimed_attempt).values(
        status=JobStatus.FAILED, error_code=error_code
    )
    return (await session.scalar(statement)) is not None


async def mark_retry(
    session: AsyncSession,
    job_id: uuid.UUID,
    *,
    claimed_attempt: int,
    error_code: FailureCode,
    delay_s: float,
) -> bool:
    """Requeue a claimed attempt that failed, due `delay_s` from now (ADR 0005 decision 4).

    False if that attempt is no longer current, or if it was the job's last: a requeued job always
    has an attempt left to be claimed with. The dispatcher publishes it again once it is due.
    """
    statement = (
        _fenced_finish(job_id, claimed_attempt)
        # The claim checks the same budget, but a requeue that broke it would leave a queued job
        # no claim can take.
        .where(Job.attempts < Job.max_attempts)
        .values(
            status=JobStatus.QUEUED,
            # The job keeps its last failure's code while it waits (ADR 0005 decision 6).
            error_code=error_code,
            # A duration from the caller, a timestamp from PostgreSQL's clock (ADR 0001 decision 9).
            available_at=func.now() + timedelta(seconds=delay_s),
            # Unpublished again, so the dispatcher's select finds it once it is due.
            dispatched_at=None,
        )
    )
    return (await session.scalar(statement)) is not None


def _fenced_finish(job_id: uuid.UUID, claimed_attempt: int) -> ReturningUpdate[tuple[uuid.UUID]]:
    # Fenced (ADR 0001 decision 7): if the job was recovered and claimed again, or this attempt
    # already finished, no row matches and the stale write is discarded. Used by every write that
    # ends an attempt, a retry included. The callers add their own columns and conditions; UPDATE
    # merges repeated .values() and .where().
    return (
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.RUNNING, Job.attempts == claimed_attempt)
        # A finished attempt holds no lease.
        .values(lease_expires_at=None, updated_at=func.now())
        .returning(Job.id)
    )
