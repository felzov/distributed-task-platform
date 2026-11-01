"""Queries on the fake_notifications table: what the fake provider "sent" (see the README)."""

import uuid

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from task_platform.data.models import FakeNotification


async def insert_fake_notification(
    session: AsyncSession, *, job_id: uuid.UUID, attempt: int
) -> None:
    """Record one provider call. A second call for the same job and attempt adds a second row."""
    # No copy of the payload: the row only shows that, and by which attempt, a call happened.
    await session.execute(insert(FakeNotification).values(job_id=job_id, attempt=attempt))
