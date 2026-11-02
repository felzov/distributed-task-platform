"""Publishing job messages with publisher confirms (ADR 0004 decision 3).

A job counts as published only when the broker has confirmed its message and routed it to a queue.
Everything else, a nack, a returned message, the timeout or any other error such as a lost
connection, leaves it unpublished, so the dispatcher publishes it again on a later poll.
"""

import asyncio
import logging
import uuid
from collections import Counter
from collections.abc import Sequence
from typing import Protocol

from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractConnection
from aiormq import spec

from task_platform import safe_logging
from task_platform.domain.messages import encode_job_message

logger = logging.getLogger(__name__)


class PublishTarget(Protocol):
    """The part of an aio-pika exchange the publisher uses; tests replace it."""

    async def publish(self, message: Message, routing_key: str, *, mandatory: bool) -> object: ...


class PublishNotAckedError(Exception):
    """The publish returned without an error, but not with the broker's ack."""


class JobPublisher:
    def __init__(self, exchange: PublishTarget, *, queue_name: str, timeout_s: float) -> None:
        self._exchange = exchange
        self._queue_name = queue_name
        self._timeout_s = timeout_s

    async def publish(self, job_ids: Sequence[uuid.UUID]) -> list[uuid.UUID]:
        """Publish one message per job, all at once; return the confirmed ids in input order.

        One timeout bounds the whole batch: the dispatcher holds the jobs' row locks meanwhile
        (ADR 0004 decision 8). A publish still unconfirmed then is cancelled; its message may still
        reach the queue, which at-least-once delivery allows.
        """
        if len(set(job_ids)) != len(job_ids):
            # One task per id: a repeated id would orphan its first publish. A caller bug, since
            # the due-job query returns primary keys.
            raise ValueError("repeated job id in the batch")
        tasks = {job_id: asyncio.create_task(self._publish_one(job_id)) for job_id in job_ids}
        try:
            if tasks:
                await asyncio.wait(tasks.values(), timeout=self._timeout_s)
        finally:
            # Also when the caller is cancelled: no publish outlives this call. Only for one
            # cancellation: a second one during the gather stops the wait (shutdown, phase 8).
            for task in tasks.values():
                task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)

        confirmed = []
        reasons: Counter[str] = Counter()
        for job_id, task in tasks.items():
            if task.cancelled():
                reason = "publish timeout"
            elif (error := task.exception()) is not None:
                # The class only: str() of a returned message's error includes the message.
                reason = safe_logging.class_name(error)
            else:
                confirmed.append(job_id)
                continue
            reasons[reason] += 1
            logger.debug("job message not confirmed: job %s, %s", job_id, reason)
        if reasons:
            # One line per batch, not per job: during a broker outage every publish of every poll
            # fails, and the jobs are published again anyway.
            logger.warning(
                "job messages not confirmed: %d of %d (%s)",
                reasons.total(),
                len(tasks),
                ", ".join(f"{reason}: {count}" for reason, count in sorted(reasons.items())),
            )
        return confirmed

    async def _publish_one(self, job_id: uuid.UUID) -> None:
        message = Message(
            encode_job_message(job_id),
            content_type="application/json",
            # Written to disk by the broker, so it survives a broker restart.
            delivery_mode=DeliveryMode.PERSISTENT,
        )
        # The default exchange routes by queue name. `mandatory`: a message no queue takes comes
        # back as returned instead of being dropped and acked.
        reply = await self._exchange.publish(message, self._queue_name, mandatory=True)
        # Checked here too, not only by the channel options: without confirms publish() returns
        # None, without on_return_raises it returns the returned message (ADR 0004 decision 3).
        if not isinstance(reply, spec.Basic.Ack):
            raise PublishNotAckedError


async def open_job_publisher(
    connection: AbstractConnection, *, queue_name: str, timeout_s: float
) -> JobPublisher:
    """Open the channel the publisher needs. Does not declare the queue (the dispatcher does)."""
    # With confirms, publish() returns only after the broker's ack and raises on a nack. Without
    # on_return_raises, a returned message is returned from publish() instead of raised, and the
    # job would count as published although no queue has it (ADR 0004 decision 3, observed).
    channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
    return JobPublisher(channel.default_exchange, queue_name=queue_name, timeout_s=timeout_s)
