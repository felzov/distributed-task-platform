"""The queue the dispatcher and the worker share (ADR 0004 decisions 1 and 2)."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from aio_pika.abc import AbstractChannel, AbstractQueue

# Fixed when RabbitMQ creates the queue: a later declaration with other arguments fails with
# PRECONDITION_FAILED, so both processes declare it from here.
JOBS_QUEUE_ARGUMENTS: Final[Mapping[str, str]] = MappingProxyType({"x-queue-type": "quorum"})


async def declare_jobs_queue(channel: AbstractChannel, name: str) -> AbstractQueue:
    """Declare the durable quorum queue for job messages. A no-op if it already exists as such."""
    return await channel.declare_queue(name, durable=True, arguments=dict(JOBS_QUEUE_ARGUMENTS))
