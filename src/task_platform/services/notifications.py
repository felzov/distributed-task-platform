"""The `notification.send` handler and its fake provider (P3, see the README)."""

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform.data.fake_notifications import insert_fake_notification
from task_platform.domain.payloads import NotificationSendV1, PayloadModel
from task_platform.services.execution import ClaimedJob


async def send_notification(
    session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
) -> dict[str, Any]:
    """Send the notification: the fake provider records the call in its own transaction.

    Committed before the job is finished, like a real provider's call, so a crash or a stale
    attempt in between leaves the call recorded and the job not finished by this attempt. Not
    idempotent: each attempt that gets here records one call (ADR 0001).
    """
    if not isinstance(payload, NotificationSendV1):
        raise TypeError("notification.send takes a NotificationSendV1 payload")
    async with session_factory.begin() as session:
        await insert_fake_notification(session, job_id=job.id, attempt=job.attempt)
    # Nothing from the payload: the result is returned by GET /jobs later (phase 4).
    return {"provider": "fake"}
