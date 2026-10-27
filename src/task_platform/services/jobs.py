"""Job use cases."""

import uuid
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform.config import Settings
from task_platform.data.jobs import insert_job
from task_platform.domain.job_status import JobStatus
from task_platform.domain.payloads import parse_payload


@dataclass(frozen=True)
class SubmittedJob:
    id: uuid.UUID
    status: JobStatus


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
