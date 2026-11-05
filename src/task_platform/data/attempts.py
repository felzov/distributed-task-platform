"""Queries on the job_attempts table (ADR 0005 decision 5). Callers own the transaction."""

import uuid

from sqlalchemy import func, insert, update
from sqlalchemy.ext.asyncio import AsyncSession

from task_platform.data.models import JobAttempt
from task_platform.domain.attempts import AttemptOutcome
from task_platform.domain.failures import FailureCode


async def open_attempt(session: AsyncSession, job_id: uuid.UUID, *, attempt: int) -> None:
    """Record that `attempt` of the job has started; `started_at` is the transaction's `now()`.

    Meant for the claim's transaction, so the row exists exactly when the claim committed. A plain
    INSERT, not ON CONFLICT DO NOTHING: the claim counts each attempt number once, so a second row
    for it is a bug that must fail the claim, not vanish.
    """
    await session.execute(insert(JobAttempt).values(job_id=job_id, attempt=attempt))


async def close_attempt(
    session: AsyncSession, job_id: uuid.UUID, *, attempt: int, failure: FailureCode | None
) -> bool:
    """Record how an open attempt ended: succeeded if `failure` is None, else failed with it.

    Returns False if that attempt has no open row: it was already closed, or it started before
    migration 0003 created the table (ADR 0005 decision 7). The outcome follows from `failure`, so
    a caller cannot pass a combination that ck_job_attempts_error_code_on_failure would reject.
    """
    outcome = AttemptOutcome.SUCCEEDED if failure is None else AttemptOutcome.FAILED
    statement = (
        update(JobAttempt)
        .where(
            JobAttempt.job_id == job_id,
            JobAttempt.attempt == attempt,
            # Only an open attempt: a closed one keeps the outcome it was closed with.
            JobAttempt.finished_at.is_(None),
        )
        .values(finished_at=func.now(), outcome=outcome, error_code=failure)
        .returning(JobAttempt.attempt)
    )
    return (await session.scalar(statement)) is not None
