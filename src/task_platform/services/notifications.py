"""The `notification.send` handler and its fake provider (P3, see the README)."""

import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform.data.fake_notifications import send_fake_notification
from task_platform.domain.payloads import NOTIFICATION_SEND, NotificationSendV1, PayloadModel
from task_platform.services.execution import ClaimedJob


def notification_key(job_id: uuid.UUID) -> str:
    """The key the handler gives the provider: one per job, so every attempt is one notification.

    Not per attempt: a retry would then send again (ADR 0007 decision 8). The form is pinned by a
    test: after a change, a job retried across it would send a second notification.
    """
    return f"{NOTIFICATION_SEND}:{job_id}"


async def send_notification(
    session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
) -> dict[str, Any]:
    """Send the notification: the fake provider records it in its own transaction.

    Committed before the job is finished, like a real provider's call, so a crash or a stale
    attempt in between leaves the notification sent and the job not finished by this attempt.
    Every attempt of a job, current or stale, gives the provider the same key, so the job gets one
    notification however often this runs (ADR 0007 decisions 8 and 9).
    """
    if not isinstance(payload, NotificationSendV1):
        raise TypeError("notification.send takes a NotificationSendV1 payload")
    async with session_factory.begin() as session:
        notification_id = await send_fake_notification(
            session, idempotency_key=notification_key(job.id), job_id=job.id, attempt=job.attempt
        )
    # Nothing from the payload: the result is shown to clients by GET /jobs/{id}.
    return {"provider": "fake", "notification_id": str(notification_id)}
