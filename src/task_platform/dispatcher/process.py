"""The dispatcher's poll loop and its wiring to PostgreSQL and RabbitMQ (ADR 0002 decision 2)."""

import asyncio
import logging
from typing import NoReturn

from aio_pika.abc import AbstractConnection
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform import safe_logging
from task_platform.broker.connection import connect_broker
from task_platform.broker.publisher import JobPublisher, open_job_publisher
from task_platform.broker.topology import declare_jobs_queue
from task_platform.config import BrokerSettings
from task_platform.data.db import create_db_engine
from task_platform.services.dispatch import Publisher, dispatch_due_jobs

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


async def open_dispatch_publisher(
    connection: AbstractConnection, settings: BrokerSettings
) -> JobPublisher:
    """Declare the jobs queue, then open the publisher with the validated publish timeout."""
    # Both processes declare the queue when they start (ADR 0004 decision 2). Not again later:
    # if the queue disappears while the dispatcher runs, publishes come back as returned and the
    # jobs stay due until a process declares it again.
    async with connection.channel() as channel:
        await declare_jobs_queue(channel, settings.jobs_queue_name)
    return await open_job_publisher(
        connection, queue_name=settings.jobs_queue_name, timeout_s=settings.publish_timeout_s
    )


async def run_dispatcher(settings: BrokerSettings) -> NoReturn:
    """Poll until cancelled. Fails at start if the broker or the queue declaration does."""
    # First, and before the broker: creating the engine does no I/O, so a database outage only
    # fails polls, and a bad URL fails here with no connection left open.
    engine = create_db_engine(settings)
    connection = await connect_broker(settings)
    try:
        publisher = await open_dispatch_publisher(connection, settings)
        logger.info("dispatcher started: queue %s", settings.jobs_queue_name)
        # expire_on_commit=False, as in the API: no hidden I/O after COMMIT.
        await poll_forever(async_sessionmaker(engine, expire_on_commit=False), publisher, settings)
    finally:
        await engine.dispose()
        await connection.close()
