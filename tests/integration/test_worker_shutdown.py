"""The worker's stop on SIGTERM or SIGINT, on real PostgreSQL and RabbitMQ (ADR 0008 decision 5,
criterion 8)."""

import asyncio
import json
import logging
import signal
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import pytest
from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractRobustConnection
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform.broker.connection import connect_broker
from task_platform.broker.topology import declare_jobs_queue
from task_platform.config import BrokerSettings
from task_platform.domain.messages import encode_job_message
from task_platform.domain.payloads import NOTIFICATION_SEND, PayloadModel
from task_platform.services.execution import ClaimedJob, Handler
from task_platform.shutdown import StopSignals
from task_platform.worker.process import consume_until_stopped, open_jobs_queue
from tests.integration.processes import stop_with_signal
from tests.integration.rows import insert_job
from tests.settings import make_test_broker_settings

pytestmark = pytest.mark.integration

PAYLOAD = {"recipient": "max@example.com", "subject": "Hello", "body": "A message."}


@pytest.fixture
def settings(
    test_database_url: str, test_broker_url: str, broker_settings: BrokerSettings
) -> BrokerSettings:
    # The queue of broker_settings, which the broker_connection fixture deletes afterwards.
    return make_test_broker_settings(
        test_database_url,
        test_broker_url,
        jobs_queue_name=broker_settings.jobs_queue_name,
        shutdown_grace_s=5.0,
    )


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture(autouse=True)
def _info_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)


class GatedHandler:
    """A handler that runs until the test releases it, and records whether it was cancelled."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = False

    def handlers(self) -> Mapping[str, Handler]:
        async def handler(
            session_factory: async_sessionmaker[AsyncSession],
            job: ClaimedJob,
            payload: PayloadModel,
        ) -> dict[str, Any]:
            self.started.set()
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            return {"done": True}

        return {NOTIFICATION_SEND: handler}


async def queued_job(engine: AsyncEngine, connection: AbstractRobustConnection, queue: str) -> Any:
    job_id = (await insert_job(engine, payload=json.dumps(PAYLOAD)))["id"]
    async with connection.channel() as channel:
        await declare_jobs_queue(channel, queue)
        await channel.default_exchange.publish(
            Message(encode_job_message(job_id), delivery_mode=DeliveryMode.PERSISTENT),
            routing_key=queue,
        )
    return job_id


async def job_state(engine: AsyncEngine, job_id: uuid.UUID) -> tuple[str, int]:
    async with engine.connect() as connection:
        row = (
            await connection.execute(
                text("SELECT status, attempts FROM jobs WHERE id = :id"), {"id": job_id}
            )
        ).one()
        return row.status, row.attempts


async def queue_counts(connection: AbstractRobustConnection, queue: str) -> tuple[int, int]:
    """(ready messages, consumers), read on a channel of its own so nothing is cached."""
    async with connection.channel() as channel:
        result = (await channel.declare_queue(queue, passive=True)).declaration_result
        assert result.message_count is not None and result.consumer_count is not None
        return result.message_count, result.consumer_count


async def wait_for_counts(
    connection: AbstractRobustConnection, queue: str, counts: tuple[int, int]
) -> None:
    # A message a closed channel gives back reaches the quorum queue shortly after (R9).
    async with asyncio.timeout(10):
        while await queue_counts(connection, queue) != counts:
            await asyncio.sleep(0.05)


@asynccontextmanager
async def worker_connection(settings: BrokerSettings) -> AsyncIterator[AbstractRobustConnection]:
    """The worker's own connection: closing it gives back a message it still holds."""
    connection = await connect_broker(settings)
    try:
        yield connection
    finally:
        await connection.close()


# Stopped with a job in hand


async def test_a_stop_signal_lets_the_job_in_hand_finish_and_takes_no_new_message(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    name = settings.jobs_queue_name
    first = await queued_job(engine, broker_connection, name)
    second = await queued_job(engine, broker_connection, name)
    handler = GatedHandler()
    signals = StopSignals()

    async with worker_connection(settings) as connection:
        queue = await open_jobs_queue(connection, settings)
        worker = asyncio.create_task(
            consume_until_stopped(queue, sessions, handler.handlers(), settings, signals)
        )
        async with asyncio.timeout(5):
            await handler.started.wait()
        signals.received(signal.SIGTERM)
        # Cancelled at the broker before the job finishes; released only then, so the ack cannot
        # race the cancel.
        async with asyncio.timeout(5):
            while "consumer cancelled, no new message will arrive" not in caplog.text:
                await asyncio.sleep(0.02)
        assert not worker.done()

        handler.release.set()
        async with asyncio.timeout(5):
            await worker

        # Finished and acked, before the connection closes: nothing comes back from it.
        assert await job_state(engine, first) == ("succeeded", 1)
    # The second message was never delivered: prefetch 1, and the consumer was gone before the
    # ack. Delivered after the ack instead, it would have come back from the iterator's buffer
    # with a nack, marked redelivered.
    await wait_for_counts(broker_connection, name, (1, 0))
    async with broker_connection.channel() as channel:
        queue = await channel.declare_queue(name, passive=True)
        message = await queue.get(no_ack=True)
    assert (message.body, message.redelivered) == (encode_job_message(second), False)
    assert await job_state(engine, second) == ("queued", 0)
    assert "worker stopped after the job in hand finished" in caplog.text


@pytest.mark.parametrize("second_signal", [False, True], ids=["grace passed", "second signal"])
async def test_a_job_not_finished_in_time_is_cancelled_and_left_for_recovery(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
    second_signal: bool,
) -> None:
    name = settings.jobs_queue_name
    job_id = await queued_job(engine, broker_connection, name)
    handler = GatedHandler()
    signals = StopSignals()
    # A second signal must not wait for a long grace period.
    grace = settings.model_copy(update={"shutdown_grace_s": 30.0 if second_signal else 0.3})

    async with worker_connection(settings) as connection:
        queue = await open_jobs_queue(connection, grace)
        worker = asyncio.create_task(
            consume_until_stopped(queue, sessions, handler.handlers(), grace, signals)
        )
        async with asyncio.timeout(5):
            await handler.started.wait()
        signals.received(signal.SIGTERM)
        if second_signal:
            signals.received(signal.SIGINT)
        async with asyncio.timeout(5):
            await worker
        assert handler.cancelled

    # Unacked: back in the queue once the connection closed, counted as a delivery (ADR 0004).
    await wait_for_counts(broker_connection, name, (1, 0))
    # Still running with its lease: lease recovery takes it on, the attempt is used up.
    assert await job_state(engine, job_id) == ("running", 1)
    assert (
        "worker stopped before the job in hand finished: its message goes back to the queue and"
        " lease recovery takes the job on"
    ) in caplog.text


# Stopped idle


async def test_an_idle_worker_stops_at_once_and_cancels_its_consumer(
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    signals = StopSignals()
    async with worker_connection(settings) as connection:
        queue = await open_jobs_queue(connection, settings)
        worker = asyncio.create_task(
            consume_until_stopped(queue, sessions, GatedHandler().handlers(), settings, signals)
        )
        await wait_for_counts(broker_connection, settings.jobs_queue_name, (0, 1))

        signals.received(signal.SIGTERM)
        # Well within the grace period: nothing to wait for.
        async with asyncio.timeout(1):
            await worker
        # Gone at the broker before the connection closes, so no message can reach it.
        assert await queue_counts(broker_connection, settings.jobs_queue_name) == (0, 0)
    assert "worker stopping: no job in hand" in caplog.text


async def test_a_stop_before_consuming_starts_leaves_the_queue_untouched(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
) -> None:
    # A signal during start-up, before the consumer exists. Subscribing and then cancelling
    # mid-subscribe would hand the broker a consumer nobody cancels: its message would come back
    # only with the connection, as a counted delivery of a job no worker touched.
    name = settings.jobs_queue_name
    job_id = await queued_job(engine, broker_connection, name)
    handler = GatedHandler()
    signals = StopSignals()
    signals.received(signal.SIGTERM)

    async with worker_connection(settings) as connection:
        queue = await open_jobs_queue(connection, settings)
        async with asyncio.timeout(1):
            await consume_until_stopped(queue, sessions, handler.handlers(), settings, signals)
    assert not handler.started.is_set()

    await wait_for_counts(broker_connection, name, (1, 0))
    async with broker_connection.channel() as channel:
        message = await (await channel.declare_queue(name, passive=True)).get(no_ack=True)
    assert (message.body, message.redelivered) == (encode_job_message(job_id), False)
    assert await job_state(engine, job_id) == ("queued", 0)


# The process


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT], ids=["SIGTERM", "SIGINT"])
async def test_the_worker_process_exits_with_status_0_on_a_signal(
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    test_database_url: str,
    test_broker_url: str,
    signum: signal.Signals,
) -> None:
    # The signal handlers python -m task_platform.worker installs: without them SIGTERM would end
    # it with status -15 and SIGINT with a KeyboardInterrupt traceback.
    env = {
        "DATABASE_URL": test_database_url,
        "BROKER_URL": test_broker_url,
        "JOBS_QUEUE_NAME": settings.jobs_queue_name,
    }

    async def consuming(stderr: bytes) -> bool:
        # The queue exists and has the worker's consumer.
        try:
            return await queue_counts(broker_connection, settings.jobs_queue_name) == (0, 1)
        except Exception:
            return False

    status, stderr = await stop_with_signal("task_platform.worker", env, consuming, signum)

    assert status == 0
    # Not printed on failure: the child's errors could quote its settings.
    assert b"Traceback" not in stderr
