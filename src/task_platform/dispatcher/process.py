"""The dispatcher's two loops and their wiring to PostgreSQL and RabbitMQ: the poll loop (ADR 0002
decision 2) and the recovery loop (ADR 0008 decision 3)."""

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any, NoReturn

from aio_pika.abc import AbstractConnection
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform import safe_logging
from task_platform.broker.connection import connect_broker
from task_platform.broker.publisher import JobPublisher, open_job_publisher
from task_platform.broker.queue_depth import ReadyCount
from task_platform.broker.topology import declare_jobs_queue
from task_platform.config import BrokerSettings
from task_platform.data.db import create_db_engine
from task_platform.services.dispatch import Publisher, dispatch_due_jobs
from task_platform.services.recovery import (
    QueueDepth,
    recover_expired_leases,
    recover_lost_messages,
)

logger = logging.getLogger(__name__)


async def poll_once(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: Publisher,
    settings: BrokerSettings,
) -> None:
    """One poll. A failed poll is logged, never raised: the next one tries again."""
    try:
        poll = await dispatch_due_jobs(session_factory, publisher, settings)
    # A database outage or a bug. The class and SQLSTATE only: an error's text can quote rows.
    except Exception as exc:
        logger.warning(
            "dispatch poll failed: %s (sqlstate %s)",
            safe_logging.class_name(exc),
            safe_logging.sqlstate(exc),
        )
        return
    if poll.due:
        logger.info("dispatched %d of %d due jobs", poll.dispatched, poll.due)


async def poll_forever(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: Publisher,
    settings: BrokerSettings,
) -> NoReturn:
    while True:
        await poll_once(session_factory, publisher, settings)
        await asyncio.sleep(settings.dispatch_poll_interval_s)


async def recover_once(
    session_factory: async_sessionmaker[AsyncSession],
    queue: QueueDepth,
    settings: BrokerSettings,
) -> None:
    """One recovery pass: the lease sweep, then the lost-message sweep. A failed sweep is logged,
    never raised: the other still runs, and the next pass tries both again."""
    # First, and whatever the broker's state: it needs only the database.
    try:
        lease = await recover_expired_leases(session_factory, settings)
    # As for a failed poll: the class and SQLSTATE only.
    except Exception as exc:
        logger.warning(
            "lease recovery failed: %s (sqlstate %s)",
            safe_logging.class_name(exc),
            safe_logging.sqlstate(exc),
        )
    else:
        if lease.expired:
            logger.info(
                "lease recovery: %d expired, %d requeued, %d failed",
                lease.expired,
                lease.requeued,
                lease.failed,
            )
    try:
        lost = await recover_lost_messages(session_factory, queue, settings)
    except Exception as exc:
        logger.warning(
            "lost-message recovery failed: %s (sqlstate %s)",
            safe_logging.class_name(exc),
            safe_logging.sqlstate(exc),
        )
    else:
        if lost.found:
            logger.info(
                "lost-message recovery: %d of %d marked to be published again",
                lost.cleared,
                lost.found,
            )


async def recover_forever(
    session_factory: async_sessionmaker[AsyncSession],
    queue: QueueDepth,
    settings: BrokerSettings,
) -> NoReturn:
    # The first pass at start: jobs left by a crash wait no longer than they must.
    while True:
        await recover_once(session_factory, queue, settings)
        await asyncio.sleep(settings.recovery_interval_s)


async def open_dispatch_publisher(
    connection: AbstractConnection, settings: BrokerSettings
) -> JobPublisher:
    """Declare the jobs queue and its dead-letter queue, then open the publisher with the validated
    publish timeout."""
    # Both processes declare the queue when they start (ADR 0004 decision 2). Not again later:
    # if the queue disappears while the dispatcher runs, publishes come back as returned and the
    # jobs stay due until a process declares it again.
    async with connection.channel() as channel:
        await declare_jobs_queue(channel, settings.jobs_queue_name)
    return await open_job_publisher(
        connection, queue_name=settings.jobs_queue_name, timeout_s=settings.publish_timeout_s
    )


async def run_dispatcher(settings: BrokerSettings) -> NoReturn:
    """Poll and recover until cancelled. Fails at start if the broker or the queue declaration
    does."""
    # First, and before the broker: creating the engine does no I/O, so a database outage only
    # fails polls, and a bad URL fails here with no connection left open.
    engine = create_db_engine(settings)
    connection = await connect_broker(settings)
    try:
        publisher = await open_dispatch_publisher(connection, settings)
        logger.info("dispatcher started: queue %s", settings.jobs_queue_name)
        # expire_on_commit=False, as in the API: no hidden I/O after COMMIT.
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        # A reader of its own, on a channel nothing else declares the queue on (ADR 0008
        # decision 2).
        queue = ReadyCount(connection, settings.jobs_queue_name)
        await poll_and_recover(sessions, publisher, queue, settings)
    finally:
        await engine.dispose()
        await connection.close()


async def poll_and_recover(
    sessions: async_sessionmaker[AsyncSession],
    publisher: Publisher,
    queue: QueueDepth,
    settings: BrokerSettings,
) -> NoReturn:
    """Run the poll loop and the recovery loop side by side until cancelled."""
    # Neither loop returns or raises (each logs its failures), so only cancellation ends the
    # group, and it cancels both. A bug that escaped one would cancel the other too.
    async with asyncio.TaskGroup() as loops:
        loops.create_task(_until_cancelled("poll", poll_forever(sessions, publisher, settings)))
        loops.create_task(_until_cancelled("recovery", recover_forever(sessions, queue, settings)))
    raise RuntimeError("the dispatcher's loops ended")


async def _until_cancelled(name: str, loop: Coroutine[Any, Any, NoReturn]) -> NoReturn:
    """Run one of the group's loops, letting only a cancellation that was asked for end it.

    A TaskGroup ignores a child that ends cancelled without being cancelled, and the other loop
    goes on: a CancelledError raised from inside would stop this loop with nothing logged. The
    one known source, aiormq's on a stuck connection, is skipped by the lost-message sweep; any
    other is turned into an error here, which stops the dispatcher.
    """
    try:
        await loop
    except asyncio.CancelledError:
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            raise
        logger.error("the %s loop ended with a cancellation nobody asked for", name)
        raise RuntimeError(f"the {name} loop ended with a cancellation nobody asked for") from None
