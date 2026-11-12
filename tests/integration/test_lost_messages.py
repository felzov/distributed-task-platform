"""One lost-message recovery sweep against real PostgreSQL and RabbitMQ (ADR 0002 decision 7, ADR
0008 decision 2)."""

import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractRobustConnection
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform import safe_logging
from task_platform.broker.queue_depth import ReadyCount
from task_platform.config import BrokerSettings
from task_platform.data.db import create_db_engine
from task_platform.dispatcher.process import open_dispatch_publisher, poll_once
from task_platform.domain.messages import encode_job_message
from task_platform.services.recovery import LostMessageSweep, recover_lost_messages
from tests.integration.rows import insert_job
from tests.settings import (
    UNREACHABLE_BROKER_URL,
    UNREACHABLE_DATABASE_URL,
    make_test_broker_settings,
)

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-7731"
# Hours away from now, so the test machine's clock and PostgreSQL's never disagree about them.
PAST = datetime.now(UTC) - timedelta(hours=3)
RECENT = datetime.now(UTC) - timedelta(minutes=5)


@pytest.fixture
def settings(
    test_database_url: str, test_broker_url: str, broker_settings: BrokerSettings
) -> BrokerSettings:
    # The queue of broker_settings, which the broker_connection fixture deletes afterwards. A one
    # hour threshold: PAST is older, RECENT younger. A short count bound for the hanging read.
    return make_test_broker_settings(
        test_database_url,
        test_broker_url,
        jobs_queue_name=broker_settings.jobs_queue_name,
        lost_message_after_s=3600.0,
        publish_timeout_s=0.2,
    )


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
async def ready_count(
    broker_connection: AbstractRobustConnection, settings: BrokerSettings
) -> ReadyCount:
    # Declared as the dispatcher does at start, empty.
    await open_dispatch_publisher(broker_connection, settings)
    return ReadyCount(broker_connection, settings.jobs_queue_name)


@pytest.fixture(autouse=True)
def _info_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)


async def insert_lost(engine: AsyncEngine, *, dispatched_at: datetime = PAST) -> uuid.UUID:
    """A queued job, due long ago, whose message went out at `dispatched_at` and never came back."""
    job_id: uuid.UUID = (
        await insert_job(
            engine,
            payload=json.dumps({"recipient": f"{SENTINEL}@example.com"}),
            available_at=PAST - timedelta(hours=1),
            dispatched_at=dispatched_at,
        )
    )["id"]
    return job_id


async def dispatched_at(engine: AsyncEngine, job_id: uuid.UUID) -> Any:
    async with engine.connect() as connection:
        return await connection.scalar(
            text("SELECT dispatched_at FROM jobs WHERE id = :id"), {"id": job_id}
        )


async def publish_other(connection: AbstractRobustConnection, queue_name: str) -> None:
    async with connection.channel() as channel:
        await channel.default_exchange.publish(
            Message(encode_job_message(uuid.uuid4()), delivery_mode=DeliveryMode.PERSISTENT),
            routing_key=queue_name,
        )


class FailingCount:
    async def ready_messages(self) -> int:
        raise RuntimeError(f"error text with {SENTINEL}")


class HangingCount:
    """A broker outage as a robust channel sees it: the read waits for the connection."""

    async def ready_messages(self) -> int:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class EmptyCount:
    async def ready_messages(self) -> int:
        return 0


class CancelledCount:
    """A read failed as aiormq fails a pending request when its heartbeat finds the connection
    stuck: with CancelledError, though nothing cancelled the task (observed on aiormq 7.2.4)."""

    async def ready_messages(self) -> int:
        raise asyncio.CancelledError


class StartedHangingCount:
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def ready_messages(self) -> int:
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


# Recovered


async def test_an_overdue_job_with_an_empty_queue_is_published_again(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    ready_count: ReadyCount,
    caplog: pytest.LogCaptureFixture,
) -> None:
    job_id = await insert_lost(engine)

    sweep = await recover_lost_messages(sessions, ready_count, settings)

    assert sweep == LostMessageSweep(ready=0, found=1, cleared=1)
    assert await dispatched_at(engine, job_id) is None
    [record] = [r for r in caplog.records if r.name == "task_platform.services.recovery"]
    assert (record.levelno, record.getMessage()) == (
        logging.ERROR,
        f"job {job_id}: no message ready 3600 s after its publish; marked to be published again",
    )
    # The dispatcher's next poll publishes it (criterion 6), and the guard now sees it waiting.
    publisher = await open_dispatch_publisher(broker_connection, settings)
    await poll_once(sessions, publisher, settings)
    assert await dispatched_at(engine, job_id) is not None
    async with asyncio.timeout(5):
        while await ready_count.ready_messages() != 1:
            await asyncio.sleep(0.05)
    async with broker_connection.channel() as channel:
        queue = await channel.declare_queue(settings.jobs_queue_name, passive=True)
        message = await queue.get(no_ack=True)
        assert message.body == encode_job_message(job_id)


async def test_one_sweep_clears_one_batch(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    ready_count: ReadyCount,
) -> None:
    for _ in range(3):
        await insert_lost(engine)
    small = settings.model_copy(update={"dispatch_batch_size": 2})

    assert await recover_lost_messages(sessions, ready_count, small) == LostMessageSweep(0, 2, 2)
    assert await recover_lost_messages(sessions, ready_count, small) == LostMessageSweep(0, 1, 1)


async def test_logs_carry_no_payload_even_at_debug(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    await insert_lost(engine)
    # After seeding: the helper's own INSERT returns the payload.
    caplog.set_level(logging.DEBUG)

    assert await recover_lost_messages(sessions, EmptyCount(), settings) == LostMessageSweep(
        0, 1, 1
    )
    assert SENTINEL not in caplog.text


async def test_a_failed_commit_logs_no_recovery_and_the_next_sweep_clears_the_job(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A deferred constraint trigger runs inside COMMIT: the clear itself succeeds, then COMMIT
    # fails. A line logged before COMMIT would report a job that was never changed.
    job_id = await insert_lost(engine)
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "CREATE OR REPLACE FUNCTION test_refuse_at_commit() RETURNS trigger"
                " LANGUAGE plpgsql AS $$ BEGIN"
                " RAISE EXCEPTION 'refused at commit' USING ERRCODE = 'check_violation';"
                " END $$"
            )
        )
        await connection.execute(text("DROP TRIGGER IF EXISTS test_refused_commit ON jobs"))
        await connection.execute(
            text(
                "CREATE CONSTRAINT TRIGGER test_refused_commit AFTER UPDATE ON jobs"
                " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW"
                " WHEN (OLD.dispatched_at IS NOT NULL AND NEW.dispatched_at IS NULL)"
                " EXECUTE FUNCTION test_refuse_at_commit()"
            )
        )
    try:
        with pytest.raises(DBAPIError) as refused:
            await recover_lost_messages(sessions, EmptyCount(), settings)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("DROP TRIGGER test_refused_commit ON jobs"))
            await connection.execute(text("DROP FUNCTION test_refuse_at_commit()"))

    # The trigger's error: deferred, so raised by COMMIT.
    assert safe_logging.sqlstate(refused.value) == "23514"
    assert "published again" not in caplog.text
    assert await dispatched_at(engine, job_id) is not None
    # Rolled back as a whole, so the next sweep finds the job again.
    assert await recover_lost_messages(sessions, EmptyCount(), settings) == LostMessageSweep(
        0, 1, 1
    )
    assert await dispatched_at(engine, job_id) is None


# Left alone


# One message is enough: the guard is "none ready", not "fewer than some".
@pytest.mark.parametrize("ready", [1, 2])
async def test_ready_messages_in_the_queue_skip_the_sweep(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    ready_count: ReadyCount,
    caplog: pytest.LogCaptureFixture,
    ready: int,
) -> None:
    # A backlog: the overdue job's message may only be waiting behind the others (ADR 0002's retry
    # storm), so nothing is published again.
    job_id = await insert_lost(engine)
    for _ in range(ready):
        await publish_other(broker_connection, settings.jobs_queue_name)

    sweep = await recover_lost_messages(sessions, ready_count, settings)

    assert sweep == LostMessageSweep(ready=ready, found=0, cleared=0)
    assert await dispatched_at(engine, job_id) is not None
    assert f"lost-message sweep skipped: {ready} messages ready in the jobs queue" in caplog.text


async def test_a_job_published_within_the_threshold_is_left_alone(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    ready_count: ReadyCount,
) -> None:
    job_id = await insert_lost(engine, dispatched_at=RECENT)

    assert await recover_lost_messages(sessions, ready_count, settings) == LostMessageSweep(0, 0, 0)
    assert await dispatched_at(engine, job_id) is not None


# The count not read


@pytest.mark.parametrize(
    ("queue", "error"),
    [
        pytest.param(FailingCount(), "builtins.RuntimeError", id="read fails"),
        # Bounded by publish_timeout_s; without the bound this test would hang, not fail.
        pytest.param(HangingCount(), "builtins.TimeoutError", id="read hangs"),
        # A stuck connection: skipped like any outage, not the end of the recovery loop.
        pytest.param(
            CancelledCount(), "asyncio.exceptions.CancelledError", id="read cancelled from inside"
        ),
    ],
)
async def test_a_count_not_read_skips_the_sweep(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
    queue: FailingCount | HangingCount | CancelledCount,
    error: str,
) -> None:
    job_id = await insert_lost(engine)

    async with asyncio.timeout(5):
        sweep = await recover_lost_messages(sessions, queue, settings)

    assert sweep == LostMessageSweep(ready=None, found=0, cleared=0)
    assert await dispatched_at(engine, job_id) is not None
    [record] = [r for r in caplog.records if r.name == "task_platform.services.recovery"]
    assert (record.levelno, record.getMessage()) == (
        logging.WARNING,
        f"lost-message sweep skipped: the jobs queue's ready count was not read ({error})",
    )
    # The class, never the error's text.
    assert SENTINEL not in caplog.text


async def test_a_requested_cancellation_ends_the_sweep(
    sessions: async_sessionmaker[AsyncSession], settings: BrokerSettings
) -> None:
    # Shutdown cancels the recovery loop during a read: that one must not be taken for a stuck
    # connection and swallowed.
    queue = StartedHangingCount()
    long_bound = settings.model_copy(update={"publish_timeout_s": 4.0})
    sweep = asyncio.create_task(recover_lost_messages(sessions, queue, long_bound))
    async with asyncio.timeout(5):
        await queue.started.wait()
    sweep.cancel()

    with pytest.raises(asyncio.CancelledError):
        await sweep


async def test_the_missing_jobs_queue_skips_the_sweep(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Deleted while the dispatcher runs, say: never declared again by recovery (ADR 0008
    # alternatives), so nothing is published again into a queue without its dead-letter queue.
    job_id = await insert_lost(engine)

    reader = ReadyCount(broker_connection, settings.jobs_queue_name)
    sweep = await recover_lost_messages(sessions, reader, settings)

    assert sweep == LostMessageSweep(ready=None, found=0, cleared=0)
    assert await dispatched_at(engine, job_id) is not None
    assert "ready count was not read (aiormq.exceptions.ChannelNotFoundEntity)" in caplog.text


async def test_a_database_error_is_raised_and_logs_no_recovery(
    settings: BrokerSettings, caplog: pytest.LogCaptureFixture
) -> None:
    down = create_db_engine(
        make_test_broker_settings(UNREACHABLE_DATABASE_URL, UNREACHABLE_BROKER_URL)
    )
    try:
        with pytest.raises(OSError):
            await recover_lost_messages(async_sessionmaker(down), EmptyCount(), settings)
    finally:
        await down.dispose()

    assert "published again" not in caplog.text
