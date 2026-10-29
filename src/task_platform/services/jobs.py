"""Job use cases."""

import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform.config import Settings
from task_platform.data.jobs import insert_job, select_job
from task_platform.domain.errors import JobNotFoundError
from task_platform.domain.job_status import JobStatus
from task_platform.domain.payloads import parse_payload


@dataclass(frozen=True)
class SubmittedJob:
    id: uuid.UUID
    status: JobStatus


@dataclass(frozen=True)
class JobState:
    """What a client may see of a job: everything except its payload and dispatched_at."""

    id: uuid.UUID
    type: str
    payload_version: int
    status: JobStatus
    attempts: int
    max_attempts: int
    available_at: datetime
    created_at: datetime
    updated_at: datetime


async def submit_job(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    job_type: str,
    payload_version: int,
    payload: object,
) -> SubmittedJob:
    """Validate a job and save it as `queued`. Returns only after the COMMIT has succeeded.

    Saved is not dispatched: the dispatcher publishes the job later (ADR 0002).
    """
    # Raises a DomainError before any database access.
    model = parse_payload(job_type, payload_version, payload, max_bytes=settings.max_payload_bytes)
    # The validated model is stored, never the raw input.
    async with session_factory.begin() as session:
        job_id, status = await insert_job(
            session,
            job_type=job_type,
            payload_version=payload_version,
            payload=model.model_dump(mode="json"),
            max_attempts=settings.default_max_attempts,
        )
    return SubmittedJob(id=job_id, status=JobStatus(status))


async def get_job(session_factory: async_sessionmaker[AsyncSession], job_id: uuid.UUID) -> JobState:
    """Read a job's current state from PostgreSQL, the source of truth. Raises JobNotFoundError."""
    # Read-only: closing the session rolls back the implicit transaction.
    async with session_factory() as session:
        row = await select_job(session, job_id)
    if row is None:
        raise JobNotFoundError()
    return JobState(
        id=row.id,
        type=row.type,
        payload_version=row.payload_version,
        status=JobStatus(row.status),
        attempts=row.attempts,
        max_attempts=row.max_attempts,
        available_at=row.available_at,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
