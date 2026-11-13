"""The worker on real PostgreSQL and RabbitMQ: what the broker does with each answer, duplicate
delivery, a database outage, a deleted queue and the process wiring (ADR 0004 decisions 5-7)."""

import asyncio
import json
import logging
import signal
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection
from sqlalchemy import QueuePool, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform import startup
from task_platform.broker.connection import connect_broker
from task_platform.broker.topology import dead_letter_queue_name, declare_jobs_queue
from task_platform.config import BrokerSettings
from task_platform.data.db import create_db_engine
from task_platform.domain.messages import encode_job_message
from task_platform.services.execution import Outcome, execute_job
from task_platform.shutdown import StopSignals
from task_platform.worker import process
from task_platform.worker.handlers import HANDLERS
from task_platform.worker.process import (
    Delivery,
    consume_until_stopped,
    open_jobs_queue,
    run_worker,
)
from tests.integration.rows import fake_notification_rows, fake_result, insert_job
from tests.settings import (
    UNREACHABLE_BROKER_URL,
    UNREACHABLE_DATABASE_URL,
    make_test_broker_settings,
    make_test_settings,
)

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-7731"
PAYLOAD = {"recipient": f"{SENTINEL}@example.com", "subject": "Hello", "body": "A message."}


@pytest.fixture
def settings(
    test_database_url: str, test_broker_url: str, broker_settings: BrokerSettings
) -> BrokerSettings:
    # The queue of broker_settings, which the broker_connection fixture deletes afterwards.
    return make_test_broker_settings(
        test_database_url,
        test_broker_url,
        jobs_queue_name=broker_settings.jobs_queue_name,
        worker_db_error_backoff_s=0.1,
    )


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
def delivery_counts(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """The `x-delivery-count` each delivery carried when the worker got it (0 if absent).

    Read inside the worker, not from the queue afterwards: stopping the worker closes its channel,
    which returns a message it still holds and counts that return too.
    """
    seen: list[object] = []
    handle_message = process.handle_message

    async def recording_handle_message(message: Delivery, *args: Any) -> None:
        headers = getattr(message, "headers", None) or {}
        seen.append(headers.get("x-delivery-count", 0))
        await handle_message(message, *args)

    monkeypatch.setattr(process, "handle_message", recording_handle_message)
    return seen


@pytest.fixture(autouse=True)
def _info_logs(caplog: pytest.LogCaptureFixture) -> None:
    # "No payload in logs at INFO+" needs INFO captured.
    caplog.set_level(logging.INFO)


async def insert_due_job(engine: AsyncEngine) -> uuid.UUID:
    job_id: uuid.UUID = (await insert_job(engine, payload=json.dumps(PAYLOAD)))["id"]
    return job_id


async def job_row(engine: AsyncEngine, job_id: uuid.UUID) -> dict[str, Any]:
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
        return dict(result.mappings().one())


async def notifications(engine: AsyncEngine, job_id: uuid.UUID) -> int:
    async with engine.connect() as connection:
        count = await connection.scalar(
            text("SELECT count(*) FROM fake_notifications WHERE job_id = :id"), {"id": job_id}
        )
        return int(count)


async def publish(
    connection: AbstractRobustConnection, queue_name: str, body: bytes, *, declare: bool = True
) -> None:
    async with connection.channel() as channel:
        if declare:
            await declare_jobs_queue(channel, queue_name)
        await channel.default_exchange.publish(
            Message(body, delivery_mode=DeliveryMode.PERSISTENT), routing_key=queue_name
        )


async def ready_messages(connection: AbstractRobustConnection, queue_name: str) -> int:
    async with connection.channel() as channel:
        queue = await channel.declare_queue(queue_name, passive=True)
        count = queue.declaration_result.message_count
        assert count is not None
        return count


async def wait_until_queue_exists(connection: AbstractRobustConnection, queue_name: str) -> None:
    async with asyncio.timeout(10):
        while True:
            try:
                await ready_messages(connection, queue_name)
                return
            # The passive declaration of a missing queue: the broker closes that channel.
            except Exception:
                await asyncio.sleep(0.05)


async def take(connection: AbstractRobustConnection, queue_name: str) -> AbstractIncomingMessage:
    async with connection.channel() as channel:
        queue = await channel.declare_queue(queue_name, passive=True)
        message = await queue.get(no_ack=True, fail=False)
        assert message is not None
        return message


async def take_dead_letter(
    connection: AbstractRobustConnection, queue_name: str
) -> AbstractIncomingMessage:
    """The one message in the queue's dead-letter queue, once RabbitMQ has moved it there."""
    dead = dead_letter_queue_name(queue_name)
    async with asyncio.timeout(10):
        while await ready_messages(connection, dead) != 1:
            await asyncio.sleep(0.05)
    return await take(connection, dead)


async def wait_for_ready(connection: AbstractRobustConnection, queue_name: str, count: int) -> None:
    """Wait until the queue holds exactly `count` ready messages; fail after 10 s."""
    async with asyncio.timeout(10):
        while await ready_messages(connection, queue_name) != count:
            await asyncio.sleep(0.05)


async def wait_for(answers: list[str], count: int) -> None:
    async with asyncio.timeout(10):
        while len(answers) < count:
            await asyncio.sleep(0.02)


@asynccontextmanager
async def worker(
    connection: AbstractRobustConnection,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
) -> AsyncIterator[None]:
    """A consumer on a channel of its own, as run_worker sets it up. Closing the channel on exit
    gives the broker back any message the worker had not answered."""
    queue = await open_jobs_queue(connection, settings)
    task = asyncio.create_task(
        consume_until_stopped(
            queue,
            lambda: open_jobs_queue(connection, settings),
            sessions,
            HANDLERS,
            settings,
            StopSignals(),
        )
    )
    try:
        yield
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await queue.channel.close()


# Acked outcomes: nothing left in the queue


async def test_a_delivered_job_succeeds_and_its_message_is_acked(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    answers: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    job_id = await insert_due_job(engine)
    await publish(broker_connection, settings.jobs_queue_name, encode_job_message(job_id))

    async with worker(broker_connection, sessions, settings):
        await wait_for(answers, 1)

    assert answers == ["ack"]
    row = await job_row(engine, job_id)
    [notification] = await fake_notification_rows(engine, job_id)
    assert (row["status"], row["attempts"], row["result"]) == (
        "succeeded",
        1,
        fake_result(notification),
    )
    assert await ready_messages(broker_connection, settings.jobs_queue_name) == 0
    assert SENTINEL not in caplog.text


async def test_unparsable_and_unknown_messages_leave_the_queue_and_the_worker_goes_on(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    answers: list[str],
) -> None:
    job_id = await insert_due_job(engine)
    for body in (b"not json", encode_job_message(uuid.uuid4()), encode_job_message(job_id)):
        await publish(broker_connection, settings.jobs_queue_name, body)

    async with worker(broker_connection, sessions, settings):
        await wait_for(answers, 3)

    # Rejected without requeue, so RabbitMQ moves it to the dead-letter queue (ADR 0006); the two
    # parsable messages are acked.
    assert answers == ["reject", "ack", "ack"]
    assert (await job_row(engine, job_id))["status"] == "succeeded"
    assert await ready_messages(broker_connection, settings.jobs_queue_name) == 0
    dead = await take_dead_letter(broker_connection, settings.jobs_queue_name)
    assert dead.body == b"not json"
    assert (dead.headers or {}).get("x-first-death-reason") in ("rejected", b"rejected")


async def test_two_workers_given_one_job_twice_run_it_once_and_ack_both(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    answers: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A duplicate publish (ADR 0002 decision 5). Each worker holds one message (prefetch 1); both
    # start their claim before either finishes, so the claim, not timing, decides.
    outcomes: list[Outcome] = []
    both_received = asyncio.Barrier(2)

    async def recording_execute_job(*args: Any) -> Outcome:
        await both_received.wait()
        outcome = await execute_job(*args)
        outcomes.append(outcome)
        return outcome

    monkeypatch.setattr(process, "execute_job", recording_execute_job)
    job_id = await insert_due_job(engine)
    for _ in range(2):
        await publish(broker_connection, settings.jobs_queue_name, encode_job_message(job_id))

    second = await connect_broker(settings)
    try:
        async with (
            worker(broker_connection, sessions, settings),
            worker(second, sessions, settings),
        ):
            await wait_for(answers, 2)
    finally:
        await second.close()

    assert sorted(outcomes) == sorted([Outcome.SUCCEEDED, Outcome.NOT_CLAIMED])
    assert answers == ["ack", "ack"]
    assert (await job_row(engine, job_id))["attempts"] == 1
    assert await notifications(engine, job_id) == 1
    assert await ready_messages(broker_connection, settings.jobs_queue_name) == 0


# Requeued outcomes: the message stays, counted or not


async def test_a_database_outage_keeps_the_message_without_counting_it(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    answers: list[str],
    delivery_counts: list[object],
) -> None:
    job_id = await insert_due_job(engine)
    before = await job_row(engine, job_id)
    await publish(broker_connection, settings.jobs_queue_name, encode_job_message(job_id))
    down = create_db_engine(make_test_settings(UNREACHABLE_DATABASE_URL))

    try:
        async with worker(broker_connection, async_sessionmaker(down), settings):
            # Twice: the nacked message came back and was tried again after the backoff.
            await wait_for(answers, 2)
    finally:
        await down.dispose()

    assert answers[:2] == ["nack", "nack"]
    # The redelivery after a nack is not counted towards the delivery limit (ADR 0004 decision 7).
    assert delivery_counts[:2] == [0, 0]
    assert await job_row(engine, job_id) == before
    # Still in the queue once the worker has stopped, and not dead-lettered: lost and
    # dead-lettered would look the same from the jobs queue alone. Waited for, not read once: a
    # delivery the worker still held at stop came back to the quorum queue up to ~0.1 s after
    # its channel closed (observed on RabbitMQ 4.3.7).
    await wait_for_ready(broker_connection, settings.jobs_queue_name, 1)
    assert (
        await ready_messages(broker_connection, dead_letter_queue_name(settings.jobs_queue_name))
        == 0
    )


async def test_an_unexpected_error_requeues_the_message_and_counts_it(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    answers: list[str],
    delivery_counts: list[object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def raises(*args: Any) -> Outcome:
        raise RuntimeError("a bug")

    monkeypatch.setattr(process, "execute_job", raises)
    job_id = await insert_due_job(engine)
    await publish(broker_connection, settings.jobs_queue_name, encode_job_message(job_id))

    async with worker(broker_connection, sessions, settings):
        await wait_for(answers, 2)

    assert answers[:2] == ["reject", "reject"]
    # Counted, so the delivery limit (20) ends it instead of an endless loop.
    assert delivery_counts[:2] == [0, 1]


async def test_a_message_that_keeps_hitting_a_bug_is_dead_lettered_past_the_limit(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    answers: list[str],
    delivery_counts: list[object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The real queue and its default limit of 20, not a lowered one: the 21st return goes over it
    # and RabbitMQ moves the message to the dead-letter queue (ADR 0006 decision 5).
    async def raises(*args: Any) -> Outcome:
        raise RuntimeError("a bug")

    monkeypatch.setattr(process, "execute_job", raises)
    job_id = await insert_due_job(engine)
    before = await job_row(engine, job_id)
    await publish(broker_connection, settings.jobs_queue_name, encode_job_message(job_id))

    async with worker(broker_connection, sessions, settings):
        dead = await take_dead_letter(broker_connection, settings.jobs_queue_name)

    assert answers == ["reject"] * 21
    assert delivery_counts == list(range(21))
    assert dead.body == encode_job_message(job_id)
    assert (dead.headers or {}).get("x-first-death-reason") in ("delivery_limit", b"delivery_limit")
    assert await ready_messages(broker_connection, settings.jobs_queue_name) == 0
    # The broker never decides the job's state: it waits for phase 8 recovery, as before.
    assert await job_row(engine, job_id) == before


async def test_a_message_held_by_a_stopped_worker_comes_back_counted(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A worker that dies mid-job: its channel closes with the message unacknowledged. This return
    # counts towards the delivery limit, unlike a nack (ADR 0004 decision 7).
    received = asyncio.Event()

    async def hangs(*args: Any) -> Outcome:
        received.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setattr(process, "execute_job", hangs)
    job_id = await insert_due_job(engine)
    await publish(broker_connection, settings.jobs_queue_name, encode_job_message(job_id))

    async with worker(broker_connection, sessions, settings), asyncio.timeout(5):
        await received.wait()

    # Back in the queue shortly after the channel closed, not at once (see the database outage
    # test above).
    await wait_for_ready(broker_connection, settings.jobs_queue_name, 1)
    message = await take(broker_connection, settings.jobs_queue_name)
    assert message.body == encode_job_message(job_id)
    assert (message.headers or {}).get("x-delivery-count") == 1


# The broker cancels the consumer


async def test_the_worker_consumes_again_after_its_queue_was_deleted(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    answers: list[str],
) -> None:
    # Deleting the queue makes RabbitMQ send basic.cancel to its consumer; aio-pika's robust
    # channel declares the queue again and consumes from it. The test only waits for the queue.
    first = await insert_due_job(engine)
    second = await insert_due_job(engine)
    await publish(broker_connection, settings.jobs_queue_name, encode_job_message(first))

    async with worker(broker_connection, sessions, settings):
        await wait_for(answers, 1)
        async with broker_connection.channel() as channel:
            await channel.queue_delete(settings.jobs_queue_name)
        await wait_until_queue_exists(broker_connection, settings.jobs_queue_name)
        await publish(
            broker_connection, settings.jobs_queue_name, encode_job_message(second), declare=False
        )
        await wait_for(answers, 2)

    assert answers == ["ack", "ack"]
    assert (await job_row(engine, second))["status"] == "succeeded"


# The process


def idle_pooled_connections(engine: AsyncEngine) -> int:
    pool = engine.pool
    assert isinstance(pool, QueuePool)
    return pool.checkedin()


async def test_the_worker_process_declares_the_queue_runs_jobs_and_cleans_up(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    answers: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connections: list[AbstractRobustConnection] = []
    engines: list[AsyncEngine] = []

    async def recording_connect_broker(settings: BrokerSettings) -> AbstractRobustConnection:
        connections.append(await connect_broker(settings))
        return connections[-1]

    def recording_create_db_engine(settings: BrokerSettings) -> AsyncEngine:
        engines.append(create_db_engine(settings))
        return engines[-1]

    monkeypatch.setattr(startup, "connect_broker", recording_connect_broker)
    monkeypatch.setattr(process, "create_db_engine", recording_create_db_engine)
    job_id = await insert_due_job(engine)

    task = asyncio.create_task(run_worker(settings, StopSignals()))
    try:
        # The worker declares the queue: before that, a publish would go nowhere.
        await wait_until_queue_exists(broker_connection, settings.jobs_queue_name)
        await publish(
            broker_connection, settings.jobs_queue_name, encode_job_message(job_id), declare=False
        )
        await wait_for(answers, 1)
        assert idle_pooled_connections(engines[0]) == 1
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert (await job_row(engine, job_id))["status"] == "succeeded"
    # Ctrl-C cancels the worker; nothing it opened may outlive it.
    assert connections[0].is_closed
    assert idle_pooled_connections(engines[0]) == 0


async def test_the_worker_waits_for_the_broker_at_start_until_a_stop(
    engine: AsyncEngine, test_database_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # An unreachable broker is an outage to wait out, not a reason to stop (ADR 0008 decision 7).
    caplog.set_level(logging.INFO)
    settings = make_test_broker_settings(
        test_database_url, UNREACHABLE_BROKER_URL, startup_retry_interval_s=0.05
    )
    signals = StopSignals()
    worker = asyncio.create_task(run_worker(settings, signals))
    async with asyncio.timeout(5):
        while caplog.text.count("broker not available at start") < 2:
            await asyncio.sleep(0.01)
    assert not worker.done()

    signals.received(signal.SIGTERM)
    async with asyncio.timeout(1):
        await worker
    assert "worker stopped during start-up" in caplog.text
