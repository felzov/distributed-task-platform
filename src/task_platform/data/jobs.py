"""Queries on the jobs table. Each one is an explicit statement; callers own the transaction."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Row, insert, select
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
