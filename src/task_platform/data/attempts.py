"""Queries on the job_attempts table (ADR 0005 decision 5). Callers own the transaction."""

import uuid

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from task_platform.data.models import JobAttempt


async def open_attempt(session: AsyncSession, job_id: uuid.UUID, *, attempt: int) -> None:
    """Record that `attempt` of the job has started; `started_at` is the transaction's `now()`.

    Meant for the claim's transaction, so the row exists exactly when the claim committed. A plain
    INSERT, not ON CONFLICT DO NOTHING: the claim counts each attempt number once, so a second row
    for it is a bug that must fail the claim, not vanish.
    """
    await session.execute(insert(JobAttempt).values(job_id=job_id, attempt=attempt))
