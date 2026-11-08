"""Job use cases."""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform.config import Settings
from task_platform.data.attempts import AttemptRow, select_attempts
from task_platform.data.jobs import JobStateRow, insert_job, select_failed_jobs, select_job
from task_platform.domain.attempts import AttemptOutcome
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


@dataclass(frozen=True)
class AttemptState:
    """One recorded attempt of a job. No outcome yet: still running, or its worker crashed."""

    attempt: int
    started_at: datetime
    finished_at: datetime | None
    outcome: AttemptOutcome | None
    # Set only for a failed attempt.
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
    return _job_state(row)


@dataclass(frozen=True)
class FailedJobsPage:
    jobs: list[JobState]
    # The last job's (updated_at, id) if more failed jobs follow, else None.
    next_after: tuple[datetime, uuid.UUID] | None


async def list_failed_jobs(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    limit: int,
    after: tuple[datetime, uuid.UUID] | None = None,
) -> FailedJobsPage:
    """One page of failed jobs, newest first; `after` is the previous page's `next_after`."""
    async with session_factory() as session:
        # One row more than the page, to know whether another page follows without a second query.
        rows = await select_failed_jobs(session, limit=limit + 1, before=after)
    shown = rows[:limit]
    more = len(rows) > limit
    return FailedJobsPage(
        jobs=[_job_state(row) for row in shown],
        next_after=(shown[-1].updated_at, shown[-1].id) if more else None,
    )


def _job_state(row: JobStateRow) -> JobState:
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
        result=_result(row.id, row.result) if status is JobStatus.SUCCEEDED else None,
        failure=_failure(row.id, row.error_code) if status is JobStatus.FAILED else None,
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
    message = _known_message(error_code)
    if message is None:
        logger.warning("failed job %s has an unknown error code", job_id)
    return Failure(code=error_code, message=message or UNKNOWN_FAILURE_MESSAGE)


def _known_message(error_code: str) -> str | None:
    """The fixed message of a code this code knows, else None.

    An unknown code, e.g. one written by a newer worker, is still a category, never input, so it is
    safe to return with a generic message: one bad row must not make a job unreadable.
    """
    try:
        return FAILURE_MESSAGES[FailureCode(error_code)]
    except ValueError:
        return None


async def get_attempts(
    session_factory: async_sessionmaker[AsyncSession], job_id: uuid.UUID
) -> list[AttemptState]:
    """A job's recorded attempts, lowest first. Raises JobNotFoundError.

    Empty for a job not claimed yet, and for one that ran before migration 0003. The first need
    not be attempt 1: attempts that started before migration 0003 have no row (ADR 0005
    decision 7).
    """
    async with session_factory() as session:
        rows = await select_attempts(session, job_id)
        # Only an empty history needs the job itself: a row's foreign key proves the job exists.
        # Jobs are never deleted, so asking after the attempts cannot give a wrong 404.
        known = bool(rows) or await select_job(session, job_id) is not None
    if not known:
        raise JobNotFoundError()
    return [_attempt(job_id, row) for row in rows]


def _attempt(job_id: uuid.UUID, row: AttemptRow) -> AttemptState:
    # ck_job_attempts_error_code_on_failure ties the code to a failed outcome, so unlike
    # jobs.error_code (see JobState) the column alone says whether the attempt failed.
    failure = None
    if row.error_code is not None:
        message = _known_message(row.error_code)
        if message is None:
            logger.warning("job %s attempt %d has an unknown error code", job_id, row.attempt)
        failure = Failure(code=row.error_code, message=message or UNKNOWN_FAILURE_MESSAGE)
    return AttemptState(
        attempt=row.attempt,
        started_at=row.started_at,
        finished_at=row.finished_at,
        outcome=None if row.outcome is None else AttemptOutcome(row.outcome),
        failure=failure,
    )
