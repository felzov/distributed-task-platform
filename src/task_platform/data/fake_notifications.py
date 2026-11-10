"""Queries on the fake_notifications table: what the fake provider "sent" (see the README)."""

import uuid

from sqlalchemy import insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from task_platform.data.models import FakeNotification


async def insert_fake_notification(
    session: AsyncSession, *, job_id: uuid.UUID, attempt: int
) -> None:
    """Record one provider call. A second call for the same job and attempt adds a second row."""
    # No copy of the payload: the row only shows that, and by which attempt, a call happened.
    await session.execute(insert(FakeNotification).values(job_id=job_id, attempt=attempt))


async def send_fake_notification(
    session: AsyncSession, *, idempotency_key: str, job_id: uuid.UUID, attempt: int
) -> uuid.UUID:
    """Send one notification per key, as a provider that deduplicates by key does (ADR 0007).

    The first call with a key records it; every later call, whichever attempt or job makes it,
    returns that first row's id and records nothing. Two calls at once with one key: the second
    waits for the first one's transaction, then returns its row, or records its own if the first
    rolled back (as for job submissions, ADR 0007 decision 5; both cases tested).
    """
    # No copy of the payload: the row only shows that, and by which attempt, a call happened.
    statement = (
        pg_insert(FakeNotification)
        .values(idempotency_key=idempotency_key, job_id=job_id, attempt=attempt)
        .on_conflict_do_nothing(index_elements=[FakeNotification.idempotency_key])
        .returning(FakeNotification.id)
    )
    inserted = await session.scalar(statement)
    if inserted is not None:
        return inserted
    existing: uuid.UUID = (
        await session.execute(
            select(FakeNotification.id).where(FakeNotification.idempotency_key == idempotency_key)
        )
    ).scalar_one()
    return existing
