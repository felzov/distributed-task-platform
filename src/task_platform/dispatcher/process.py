"""The dispatcher's two loops and their wiring to PostgreSQL and RabbitMQ: the poll loop (ADR 0002
decision 2) and the recovery loop (ADR 0008 decision 3)."""

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

from aio_pika.abc import AbstractConnection, AbstractRobustConnection
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform import safe_logging
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
from task_platform.shutdown import (
    Ended,
    StopSignals,
    clean_up_within,
    finish_unless_forced,
    sleep_unless_stopped,
)
from task_platform.startup import wait_for_dependencies

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


async def poll_until_stopped(
    session_factory: async_sessionmaker[AsyncSession],
    publisher: Publisher,
    settings: BrokerSettings,
    stop: asyncio.Event,
) -> None:
    """Poll every `dispatch_poll_interval_s` until `stop` is set; a poll in hand ends first."""
    while not stop.is_set():
        await poll_once(session_factory, publisher, settings)
        await sleep_unless_stopped(settings.dispatch_poll_interval_s, stop)


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


async def recover_until_stopped(
    session_factory: async_sessionmaker[AsyncSession],
    queue: QueueDepth,
    settings: BrokerSettings,
    stop: asyncio.Event,
) -> None:
    """A recovery pass every `recovery_interval_s` until `stop` is set; a pass in hand ends
    first."""
    # The first pass at start: jobs left by a crash wait no longer than they must.
    while not stop.is_set():
        await recover_once(session_factory, queue, settings)
        await sleep_unless_stopped(settings.recovery_interval_s, stop)


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


async def run_dispatcher(settings: BrokerSettings, signals: StopSignals) -> None:
    """Wait for the database and the broker, then poll and recover until a stop signal or
    cancellation (ADR 0008 decision 6). Raises StartupError on a configuration error at start
    (ADR 0008 decision 7); fails if the queue declaration does.

    On the first signal the poll and the sweep in hand end with their COMMIT or ROLLBACK and no
    new ones start. A second signal cancels them: their transactions roll back, and jobs whose
    messages were already confirmed are published again on the next start (ADR 0002 decision 5).
    """
    engine = create_db_engine(settings)
    try:
        connection = await wait_for_dependencies(engine, settings, signals.stop)
        if connection is None:
            logger.info("dispatcher stopped during start-up")
            return
        await _dispatch_until_stopped(engine, connection, settings, signals)
    finally:
        await clean_up_within(
            "closing the database pool",
            engine.dispose(),
            settings.db_connect_timeout_s,
            signals.force,
        )


async def _dispatch_until_stopped(
    engine: AsyncEngine,
    connection: AbstractRobustConnection,
    settings: BrokerSettings,
    signals: StopSignals,
) -> None:
    try:
        publisher = await open_dispatch_publisher(connection, settings)
        logger.info("dispatcher started: queue %s", settings.jobs_queue_name)
        # expire_on_commit=False, as in the API: no hidden I/O after COMMIT.
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        # A reader of its own, on a channel nothing else declares the queue on (ADR 0008
        # decision 2).
        queue = ReadyCount(connection, settings.jobs_queue_name)
        work = poll_and_recover(sessions, publisher, queue, settings, signals.stop)
        ended = await finish_unless_forced(work, signals.force, timeout_s=None)
        if ended is Ended.FINISHED:
            logger.info("dispatcher stopped")
        elif ended is Ended.CANCELLED:
            logger.warning(
                "dispatcher stopped on a second signal: the poll or sweep in hand was cancelled"
                " and rolled back"
            )
        else:
            # Its cleanup failure is logged; the database rolls back what never committed.
            logger.warning(
                "dispatcher stopped on a second signal: the poll or sweep in hand was cancelled"
                " and its cleanup failed"
            )
    finally:
        await clean_up_within(
            "closing the broker connection",
            connection.close(),
            settings.broker_connect_timeout_s,
            signals.force,
        )


async def poll_and_recover(
    sessions: async_sessionmaker[AsyncSession],
    publisher: Publisher,
    queue: QueueDepth,
    settings: BrokerSettings,
    stop: asyncio.Event,
) -> None:
    """Run the poll loop and the recovery loop side by side until `stop` is set or cancelled."""
    # Neither loop raises (each logs its failures), so they end together on `stop`, or on
    # cancellation, which cancels both. A bug that escaped one would cancel the other too.
    async with asyncio.TaskGroup() as loops:
        loops.create_task(
            _until_cancelled("poll", poll_until_stopped(sessions, publisher, settings, stop))
        )
        loops.create_task(
            _until_cancelled("recovery", recover_until_stopped(sessions, queue, settings, stop))
        )


async def _until_cancelled(name: str, loop: Coroutine[Any, Any, None]) -> None:
    """Run one of the group's loops, letting only a stop or a cancellation that was asked for end
    it.

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
