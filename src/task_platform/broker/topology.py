"""The queues the dispatcher and the worker share (ADR 0004 decisions 1 and 2, ADR 0006)."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from aio_pika.abc import AbstractChannel, AbstractQueue, FieldValue

# RabbitMQ's own default, written down so that the queue's arguments state the whole policy.
DELIVERY_LIMIT: Final = 20
DEAD_LETTER_SUFFIX: Final = ".dead"

# Every argument below is fixed when RabbitMQ creates the queue: a later declaration with other
# values fails with PRECONDITION_FAILED, so both processes declare the queues from here only.
_QUORUM: Final[Mapping[str, str]] = MappingProxyType({"x-queue-type": "quorum"})


def dead_letter_queue_name(jobs_queue_name: str) -> str:
    return jobs_queue_name + DEAD_LETTER_SUFFIX


def jobs_queue_arguments(
    jobs_queue_name: str, *, delivery_limit: int = DELIVERY_LIMIT
) -> dict[str, FieldValue]:
    """The jobs queue's arguments (ADR 0006 decision 3)."""
    return {
        **_QUORUM,
        # The default exchange routes by queue name: no exchange of our own is needed.
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": dead_letter_queue_name(jobs_queue_name),
        # Not dropped if the dead-letter queue cannot take it yet (ADR 0006 decision 4).
        "x-dead-letter-strategy": "at-least-once",
        # Required by at-least-once; without a length limit it changes nothing for publishing.
        "x-overflow": "reject-publish",
        # An int: RabbitMQ compares an argument's type as well as its value.
        "x-delivery-limit": delivery_limit,
    }


async def declare_jobs_queue(
    channel: AbstractChannel, name: str, *, delivery_limit: int = DELIVERY_LIMIT
) -> AbstractQueue:
    """Declare the dead-letter queue, then the durable quorum queue for job messages.

    A no-op for queues that already exist as such. The dead-letter queue comes first, so its route
    exists before any message can die. A robust channel restores the queues in the order they were
    declared (read in aio-pika 10.2.1's robust_channel.py, not tested here). Tests lower
    `delivery_limit` on queues of their own.
    """
    await channel.declare_queue(dead_letter_queue_name(name), durable=True, arguments=dict(_QUORUM))
    return await channel.declare_queue(
        name, durable=True, arguments=jobs_queue_arguments(name, delivery_limit=delivery_limit)
    )
