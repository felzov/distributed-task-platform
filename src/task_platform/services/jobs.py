"""Job use cases."""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform.config import Settings
from task_platform.data.jobs import insert_job, select_job
from task_platform.domain.errors import JobNotFoundError
from task_platform.domain.failures import FAILURE_MESSAGES, UNKNOWN_FAILURE_MESSAGE, FailureCode
from task_platform.domain.job_status import JobStatus
from task_platform.domain.payloads import parse_payload

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SubmittedJob:
    id: uuid.UUID
    status: JobStatus


@dataclass(frozen=True)
class Failure:
    """Why a failed job failed: a fixed code and its fixed message, never exception text."""

    code: str
    message: str


@dataclass(frozen=True)
class JobState:
    """What a client may see of a job: everything except its payload, dispatched_at and lease.

    `result` is set only for a `succeeded` job and `failure` only for a `failed` one, whatever the
    columns hold: once retries exist (phase 5), a job queued again may still carry an error code.
    """

    id: uuid.UUID
    type: str
    payload_version: int
    status: JobStatus
    attempts: int
    max_attempts: int
    available_at: datetime
    created_at: datetime
    updated_at: datetime
    result: dict[str, Any] | None
    failure: Failure | None


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
    status = JobStatus(row.status)
    return JobState(
        id=row.id,
        type=row.type,
        payload_version=row.payload_version,
        status=status,
        attempts=row.attempts,
        max_attempts=row.max_attempts,
        available_at=row.available_at,
        created_at=row.created_at,
        updated_at=row.updated_at,
        result=_result(job_id, row.result) if status is JobStatus.SUCCEEDED else None,
        failure=_failure(job_id, row.error_code) if status is JobStatus.FAILED else None,
    )


def _result(job_id: uuid.UUID, result: object) -> dict[str, Any] | None:
    # Handlers return a dict and mark_succeeded stores it, but no CHECK enforces that (D4). Any
    # other value would fail the response model, and that error's text quotes the value.
    if result is None:
        logger.warning("succeeded job %s has no result", job_id)
        return None
    if not isinstance(result, dict):
        logger.warning("succeeded job %s has a result that is not a JSON object", job_id)
        return None
    return result


def _failure(job_id: uuid.UUID, error_code: str | None) -> Failure | None:
    if error_code is None:
        # Not written by this code: mark_failed always sets a code (no CHECK enforces it, D4).
        logger.warning("failed job %s has no error code", job_id)
        return None
    try:
        message = FAILURE_MESSAGES[FailureCode(error_code)]
    except ValueError:
        # Not one of ours, e.g. written by a newer worker. Still a category, never input, so it is
        # safe to return; one bad row must not make the job unreadable.
        logger.warning("failed job %s has an unknown error code", job_id)
        message = UNKNOWN_FAILURE_MESSAGE
    return Failure(code=error_code, message=message)
