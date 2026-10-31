"""Queries on the jobs table. Each one is an explicit statement; callers own the transaction."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Row, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from task_platform.data.models import Job


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


# id, type, payload_version, status, attempts, max_attempts, available_at, created_at, updated_at
JobStateRow = Row[uuid.UUID, str, int, str, int, int, datetime, datetime, datetime]


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
            Job.status == "queued",
            Job.dispatched_at.is_(None),
            Job.available_at <= func.now(),
        )
        .order_by(Job.available_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return list(await session.scalars(statement))


async def mark_dispatched(session: AsyncSession, job_ids: list[uuid.UUID]) -> int:
    """Record a confirmed publish for these jobs. Returns how many rows were updated.

    Only jobs that are still `queued` and unpublished are updated, even if a caller passes other
    ids. `available_at` is not checked again: the caller holds the row locks from
    `select_due_job_ids`, so it cannot have changed.
    """
    if not job_ids:
        return 0
    statement = (
        update(Job)
        .where(Job.id.in_(job_ids), Job.status == "queued", Job.dispatched_at.is_(None))
        .values(dispatched_at=func.now(), updated_at=func.now())
        # RETURNING instead of rowcount, which the typed Result does not expose.
        .returning(Job.id)
    )
    return len((await session.scalars(statement)).all())
