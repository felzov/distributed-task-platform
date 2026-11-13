"""The worker's consumer and its wiring to PostgreSQL and RabbitMQ (ADR 0004 decisions 5 and 6).

`services.execution` decides what happened to the job; this module only answers the broker.
"""

import asyncio
import logging
from collections.abc import Awaitable, Mapping
from typing import Final, Protocol

from aio_pika.abc import AbstractConnection, AbstractQueue, AbstractQueueIterator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform import safe_logging
from task_platform.broker.topology import declare_jobs_queue
from task_platform.config import BrokerSettings
from task_platform.data.db import create_db_engine
from task_platform.domain.messages import InvalidMessageError, parse_job_message
from task_platform.services.execution import Handler, Outcome, execute_job
from task_platform.shutdown import StopSignals, cancel_and_wait, finish_unless_forced
from task_platform.startup import wait_for_dependencies
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
    # the delivery limit, so a job that keeps hitting it cannot hold the queue forever; past the
    # limit its message goes to the dead-letter queue, as for a worker that keeps crashing
    # (ADR 0004, ADR 0006).
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


class _Consumer:
    """Handles the queue's messages one at a time and knows whether it holds one."""

    def __init__(
        self,
        queue: AbstractQueue,
        session_factory: async_sessionmaker[AsyncSession],
        handlers: Mapping[str, Handler],
        settings: BrokerSettings,
    ) -> None:
        self._queue = queue
        self._session_factory = session_factory
        self._handlers = handlers
        self._settings = settings
        self._messages: AbstractQueueIterator | None = None
        self.holding = False
        # Messages handled to their end: a stop compares it to tell a finished job in hand.
        self.answered = 0
        self._stopping = False

    async def run(self) -> None:
        """Consume until `stop` was called and the held message is answered."""
        # aio-pika 10.2.1's robust iterator consumed again after the broker cancelled the consumer
        # (queue deleted, tested) and after an 87 s broker outage (observed once, 2026-11-02). It
        # can still end: its source gives up after 60 s without a restored channel in one path,
        # and it ends when the connection is closed.
        async with self._queue.iterator() as messages:
            self._messages = messages
            async for message in messages:
                self.holding = True
                try:
                    await handle_message(
                        message, self._session_factory, self._handlers, self._settings
                    )
                finally:
                    self.holding = False
                self.answered += 1
                # Before asking the iterator again: once the job's ack reached the broker ahead
                # of the cancel, the next message may already be on its way here.
                if self._stopping:
                    return
        if not self._stopping:
            # Then the process stops instead of idling with no consumer; no message or job is
            # lost (the broker requeues, the claim fences). Consuming again comes with the outage
            # handling (ADR 0008 decision 8).
            raise RuntimeError("the jobs queue iterator ended; the worker stops")

    def stop(self) -> None:
        """Take no message after the one in hand. Called in the same step that saw it held, so
        the job cannot end in between and the loop ask for another."""
        self._stopping = True

    async def cancel_at_broker(self) -> None:
        """Cancel the consumer at the broker: no new message arrives, and the held one can still
        be acked (ADR 0008, observed on RabbitMQ 4.3.7). A quorum queue keeps counting the
        cancelled consumer until the held message is answered (observed). Not bounded here:
        `_finish` does not wait for it once the job is answered."""
        if self._messages is None:
            return
        try:
            await self._messages.close()
        # The channel or connection is gone; the held message goes back with it anyway.
        except Exception as exc:
            logger.warning(
                "could not cancel the consumer at shutdown: %s", safe_logging.class_name(exc)
            )
        else:
            logger.info("worker stopping: consumer cancelled, no new message will arrive")


async def consume_until_stopped(
    queue: AbstractQueue,
    session_factory: async_sessionmaker[AsyncSession],
    handlers: Mapping[str, Handler],
    settings: BrokerSettings,
    signals: StopSignals,
) -> None:
    """Handle messages one at a time until a stop signal (ADR 0008 decision 5) or cancellation.

    On the first signal: without a held message, stop at once; with one, take no new message and
    let the held one finish (handler, final update, answer) for up to `shutdown_grace_s`. Past
    that, or on a second signal, its handler is cancelled: the message goes back to the queue when
    the connection closes, and lease recovery takes the job on.
    """
    if signals.stop.is_set():
        # A signal during start-up. Not subscribing at all: a subscribe cancelled midway leaves a
        # consumer nobody cancels, whose message comes back only with the connection, counted.
        logger.info("worker stopping: stopped before consuming")
        return
    consumer = _Consumer(queue, session_factory, handlers, settings)
    consuming = asyncio.create_task(consumer.run())
    stop_requested = asyncio.create_task(signals.stop.wait())
    try:
        await asyncio.wait({consuming, stop_requested}, return_when=asyncio.FIRST_COMPLETED)
        if consuming.done():
            # The iterator ended without a stop: raises.
            consuming.result()
            return
        if not consumer.holding:
            logger.info("worker stopping: no job in hand")
            return
        consumer.stop()
        answered = consumer.answered
        logger.info(
            "worker stopping: waiting up to %g s for the job in hand", settings.shutdown_grace_s
        )
        await finish_unless_forced(
            _finish(consumer, consuming), signals.force, settings.shutdown_grace_s
        )
        # Whether the job ended, not whether everything else did in time.
        if consumer.answered > answered:
            logger.info("worker stopped after the job in hand finished")
        else:
            logger.warning(
                "worker stopped before the job in hand finished: its message goes back to the"
                " queue and lease recovery takes the job on"
            )
    finally:
        stop_requested.cancel()
        await cancel_and_wait(consuming)


async def _finish(consumer: _Consumer, consuming: asyncio.Task[None]) -> None:
    """Wait for the job in hand while the consumer is cancelled at the broker beside it."""
    cancelling = asyncio.create_task(consumer.cancel_at_broker())
    try:
        # Cancelling this await cancels the consumer, and with it the handler.
        await consuming
    finally:
        # A cancel still on its way once the job is answered is not waited for (a link that
        # stopped passing traffic). Dropping it closes the channel: a message delivered after the
        # job's ack goes back with it, marked redelivered but not counted, as the cancel's own
        # nack would send it.
        await cancel_and_wait(cancelling)


async def open_jobs_queue(
    connection: AbstractConnection, settings: BrokerSettings
) -> AbstractQueue:
    """A channel of its own with prefetch 1, and the jobs queue declared on it."""
    # Robust on a robust connection: the prefetch and the queue are restored with the channel.
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=PREFETCH_COUNT)
    return await declare_jobs_queue(channel, settings.jobs_queue_name)


async def run_worker(settings: BrokerSettings, signals: StopSignals) -> None:
    """Wait for the database and the broker, then consume until a stop signal or cancellation.

    Raises StartupError on a configuration error at start (ADR 0008 decision 7); fails if the
    queue declaration does.
    """
    engine = create_db_engine(settings)
    try:
        connection = await wait_for_dependencies(engine, settings, signals.stop)
        if connection is None:
            logger.info("worker stopped during start-up")
            return
        try:
            queue = await open_jobs_queue(connection, settings)
            logger.info("worker started: queue %s", settings.jobs_queue_name)
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            await consume_until_stopped(queue, sessions, HANDLERS, settings, signals)
        finally:
            await connection.close()
    finally:
        await engine.dispose()
