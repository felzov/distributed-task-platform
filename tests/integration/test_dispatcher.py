"""The dispatcher on real PostgreSQL and RabbitMQ: real publisher, poll loop, process wiring."""

import asyncio
import uuid
from collections.abc import Sequence
from typing import Any

import pytest
from aio_pika.abc import AbstractQueue, AbstractRobustConnection
from sqlalchemy import QueuePool, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform.broker.connection import connect_broker
from task_platform.broker.publisher import JobPublisher, open_job_publisher
from task_platform.broker.topology import declare_jobs_queue
from task_platform.config import BrokerSettings
from task_platform.data.db import create_db_engine
from task_platform.dispatcher import process
from task_platform.dispatcher.process import poll_forever, run_dispatcher
from task_platform.domain.messages import encode_job_message
from task_platform.services.dispatch import DispatchPoll, dispatch_due_jobs
from tests.integration.rows import insert_job
from tests.settings import make_test_broker_settings

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-7731"


@pytest.fixture
def settings(
    test_database_url: str, test_broker_url: str, broker_settings: BrokerSettings
) -> BrokerSettings:
    # The queue of broker_settings, which the broker_connection fixture deletes afterwards. Not
    # the default publish timeout, so the wiring test can tell it apart from one passed by mistake.
    return make_test_broker_settings(
        test_database_url,
        test_broker_url,
        jobs_queue_name=broker_settings.jobs_queue_name,
        dispatch_poll_interval_s=0.05,
        publish_timeout_s=1.5,
    )


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def dispatched_at(engine: AsyncEngine, job_id: uuid.UUID) -> Any:
    async with engine.connect() as connection:
        return await connection.scalar(
            text("SELECT dispatched_at FROM jobs WHERE id = :id"), {"id": job_id}
        )


async def wait_until_dispatched(engine: AsyncEngine, job_id: uuid.UUID) -> None:
    async with asyncio.timeout(5):
        while await dispatched_at(engine, job_id) is None:
            await asyncio.sleep(0.02)


def idle_pooled_connections(engine: AsyncEngine) -> int:
    pool = engine.pool
    # The engine's AsyncAdaptedQueuePool is a QueuePool, as in services/health.py.
    assert isinstance(pool, QueuePool)
    return pool.checkedin()


async def queued_bodies(queue: AbstractQueue) -> list[bytes]:
    bodies = []
    while (message := await queue.get(no_ack=True, fail=False)) is not None:
        bodies.append(message.body)
    return bodies


async def stop(task: asyncio.Task[Any]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# The poll with the real publisher (S4a's broker outcomes, S4b's transaction)


async def test_a_job_is_marked_only_once_a_queue_took_its_message(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
) -> None:
    job_id = (await insert_job(engine))["id"]
    publisher = await open_job_publisher(
        broker_connection, queue_name=settings.jobs_queue_name, timeout_s=settings.publish_timeout_s
    )

    # No queue yet: the broker returns the message, so the job stays due.
    assert await dispatch_due_jobs(sessions, publisher, settings) == DispatchPoll(1, 0)
    assert await dispatched_at(engine, job_id) is None

    queue = await declare_jobs_queue(await broker_connection.channel(), settings.jobs_queue_name)
    assert await dispatch_due_jobs(sessions, publisher, settings) == DispatchPoll(1, 1)
    assert await dispatched_at(engine, job_id) is not None
    # Exactly one message: the returned one never reached a queue.
    assert await queued_bodies(queue) == [encode_job_message(job_id)]


# The loop


class FailsOncePublisher:
    """Raises on the first batch, as a bug would; then confirms every job without publishing."""

    def __init__(self) -> None:
        self.calls = 0

    async def publish(self, job_ids: Sequence[uuid.UUID]) -> list[uuid.UUID]:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError(f"error text with {SENTINEL}")
        return list(job_ids)


async def test_the_loop_goes_on_after_a_failed_poll(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    job_id = (await insert_job(engine))["id"]
    publisher = FailsOncePublisher()

    loop = asyncio.create_task(poll_forever(sessions, publisher, settings))
    try:
        await wait_until_dispatched(engine, job_id)
    finally:
        await stop(loop)

    assert publisher.calls >= 2
    assert "dispatch poll failed: builtins.RuntimeError (sqlstate None)" in caplog.text
    # The class, never the error's text.
    assert SENTINEL not in caplog.text


# The process


async def test_the_dispatcher_declares_the_queue_and_publishes_due_jobs(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened: list[dict[str, Any]] = []

    async def recording_open_job_publisher(*args: Any, **kwargs: Any) -> JobPublisher:
        opened.append(kwargs)
        return await open_job_publisher(*args, **kwargs)

    monkeypatch.setattr(process, "open_job_publisher", recording_open_job_publisher)
    job_id = (await insert_job(engine))["id"]

    dispatcher = asyncio.create_task(run_dispatcher(settings))
    try:
        await wait_until_dispatched(engine, job_id)
    finally:
        await stop(dispatcher)

    # The validated setting, not some other number (ADR 0004 decision 8).
    assert opened == [{"queue_name": settings.jobs_queue_name, "timeout_s": 1.5}]
    # Passive: fails if the queue does not exist, so the dispatcher declared it.
    channel = await broker_connection.channel()
    queue = await channel.declare_queue(settings.jobs_queue_name, passive=True)
    assert await queued_bodies(queue) == [encode_job_message(job_id)]


async def test_cancelling_the_dispatcher_closes_its_connection_and_pool(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Ctrl-C cancels the dispatcher; nothing it opened may outlive it.
    connections: list[AbstractRobustConnection] = []
    engines: list[AsyncEngine] = []

    async def recording_connect_broker(settings: BrokerSettings) -> AbstractRobustConnection:
        connections.append(await connect_broker(settings))
        return connections[-1]

    def recording_create_db_engine(settings: BrokerSettings) -> AsyncEngine:
        engines.append(create_db_engine(settings))
        return engines[-1]

    monkeypatch.setattr(process, "connect_broker", recording_connect_broker)
    monkeypatch.setattr(process, "create_db_engine", recording_create_db_engine)
    job_id = (await insert_job(engine))["id"]

    dispatcher = asyncio.create_task(run_dispatcher(settings))
    try:
        # One poll done, so the pool holds a connection.
        await wait_until_dispatched(engine, job_id)
        assert idle_pooled_connections(engines[0]) == 1
    finally:
        await stop(dispatcher)

    assert connections[0].is_closed
    # Disposed: its pooled connection is closed and the pool is empty.
    assert idle_pooled_connections(engines[0]) == 0
