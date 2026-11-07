"""The worker's consumer and its wiring to PostgreSQL and RabbitMQ (ADR 0004 decisions 5 and 6).

`services.execution` decides what happened to the job; this module only answers the broker.
"""

import asyncio
import logging
from collections.abc import Awaitable, Mapping
from typing import Final, NoReturn, Protocol

from aio_pika.abc import AbstractConnection, AbstractQueue
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform import safe_logging
from task_platform.broker.connection import connect_broker
from task_platform.broker.topology import declare_jobs_queue
from task_platform.config import BrokerSettings
from task_platform.data.db import create_db_engine
from task_platform.domain.messages import InvalidMessageError, parse_job_message
from task_platform.services.execution import Handler, Outcome, execute_job
from task_platform.worker.handlers import HANDLERS

logger = logging.getLogger(__name__)

# One message at a time per process (ADR 0004 decision 5): more throughput means more workers.
PREFETCH_COUNT: Final = 1

# Not processed: the database could not take the claim or the final update. nack does not count
# towards the delivery limit (ADR 0004 decision 7). Every other outcome is final for its message
# and acked, RETRY_SCHEDULED included: the retry comes back as a new message (ADR 0005).
_NACKED: Final = frozenset({Outcome.CLAIM_UNAVAILABLE, Outcome.FINISH_UNAVAILABLE})


class Delivery(Protocol):
    """The part of aio-pika's incoming message the worker uses."""

    @property
    def body(self) -> bytes: ...

    async def ack(self, multiple: bool = False) -> None: ...

    async def nack(self, multiple: bool = False, requeue: bool = True) -> None: ...

    async def reject(self, requeue: bool = False) -> None: ...


async def handle_message(
    message: Delivery,
    session_factory: async_sessionmaker[AsyncSession],
    handlers: Mapping[str, Handler],
    settings: BrokerSettings,
) -> None:
    """Run the job a message names and answer the broker. Never raises except on cancellation."""
    try:
        job = parse_job_message(message.body)
    except InvalidMessageError as exc:
        # The details name fields and error types, never a value from the body.
        logger.warning("rejected an unparsable message: %s", ", ".join(exc.details))
        await _settle("reject", message.reject(requeue=False))
        return
    try:
        outcome = await execute_job(session_factory, job.job_id, handlers, settings)
    # A bug, such as a transition the domain refuses. Requeued with reject, which counts towards
    # the delivery limit, so a job that keeps hitting it cannot hold the queue forever; its
    # message is dropped after the limit, as for a worker that keeps crashing (ADR 0004).
    except Exception as exc:
        logger.error("job %s: unexpected %s", job.job_id, safe_logging.class_name(exc))
        await _settle("reject", message.reject(requeue=True))
        return
    if outcome is Outcome.CLAIM_UNAVAILABLE:
        # Without the wait, every worker would get its message straight back: a hot loop for as
        # long as the database is down.
        await asyncio.sleep(settings.worker_db_error_backoff_s)
    if outcome in _NACKED:
        await _settle("nack", message.nack(requeue=True))
    else:
        await _settle("ack", message.ack())


async def _settle(action: str, answer: Awaitable[None]) -> None:
    try:
        await answer
    # The channel or connection is gone. The broker requeues every unacknowledged message of a
    # closed channel, and its redelivery loses the claim, so keep consuming (ADR 0004 decision 6).
    except Exception as exc:
        logger.warning("could not %s a job message: %s", action, safe_logging.class_name(exc))


async def consume_forever(
    queue: AbstractQueue,
    session_factory: async_sessionmaker[AsyncSession],
    handlers: Mapping[str, Handler],
    settings: BrokerSettings,
) -> NoReturn:
    """Handle messages one at a time until cancelled."""
    # aio-pika 10.2.1's robust iterator consumed again after the broker cancelled the consumer
    # (queue deleted, tested) and after an 87 s broker outage (observed once, 2026-11-02). It can
    # still end: its source gives up after 60 s without a restored channel in one path, and it
    # ends when the connection is closed.
    async with queue.iterator() as messages:
        async for message in messages:
            await handle_message(message, session_factory, handlers, settings)
    # Then the process stops instead of idling with no consumer; no message or job is lost (the
    # broker requeues, the claim fences). Restarting by itself is phase 8.
    raise RuntimeError("the jobs queue iterator ended; the worker stops")


async def open_jobs_queue(
    connection: AbstractConnection, settings: BrokerSettings
) -> AbstractQueue:
    """A channel of its own with prefetch 1, and the jobs queue declared on it."""
    # Robust on a robust connection: the prefetch and the queue are restored with the channel.
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=PREFETCH_COUNT)
    return await declare_jobs_queue(channel, settings.jobs_queue_name)


async def run_worker(settings: BrokerSettings) -> NoReturn:
    """Consume until cancelled. Fails at start if the broker or the queue declaration does."""
    # As in the dispatcher: creating the engine does no I/O, so a database outage only fails
    # claims, which back off.
    engine = create_db_engine(settings)
    connection = await connect_broker(settings)
    try:
        queue = await open_jobs_queue(connection, settings)
        logger.info("worker started: queue %s", settings.jobs_queue_name)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        await consume_forever(queue, sessions, HANDLERS, settings)
    finally:
        await engine.dispose()
        await connection.close()
