"""The dispatcher on real PostgreSQL and RabbitMQ: real publisher, poll loop, process wiring."""

import asyncio
import logging
import signal
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from aio_pika.abc import AbstractQueue, AbstractRobustConnection
from sqlalchemy import QueuePool, event, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform import startup
from task_platform.broker.connection import connect_broker
from task_platform.broker.publisher import JobPublisher, open_job_publisher
from task_platform.broker.topology import declare_jobs_queue
from task_platform.config import BrokerSettings
from task_platform.data.db import create_db_engine
from task_platform.dispatcher import process
from task_platform.dispatcher.process import poll_until_stopped, recover_once, run_dispatcher
from task_platform.domain.messages import encode_job_message
from task_platform.services.dispatch import DispatchPoll, dispatch_due_jobs
from task_platform.shutdown import StopSignals
from tests.integration.processes import stop_with_signal
from tests.integration.rows import insert_job
from tests.settings import make_test_broker_settings

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-7731"
# Hours away from now, so the test machine's clock and PostgreSQL's never disagree about them.
PAST = datetime.now(UTC) - timedelta(hours=3)


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
        recovery_interval_s=0.05,
        lost_message_after_s=3600.0,
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

    loop = asyncio.create_task(poll_until_stopped(sessions, publisher, settings, asyncio.Event()))
    try:
        await wait_until_dispatched(engine, job_id)
    finally:
        await stop(loop)

    assert publisher.calls >= 2
    assert "dispatch poll failed: builtins.RuntimeError (sqlstate None)" in caplog.text
    # The class, never the error's text.
    assert SENTINEL not in caplog.text


# The recovery loop


class HangingCount:
    """The ready count as a broker outage leaves it: the read waits for the connection."""

    async def ready_messages(self) -> int:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


async def test_a_cut_broker_skips_the_lost_message_sweep_and_leases_are_still_recovered(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The lease sweep needs only the database (ADR 0008 decision 3). A real cut through a proxy
    # comes with the outage tests; here the read hangs as a robust channel's would.
    caplog.set_level(logging.INFO)
    crashed = (await insert_job(engine, status="running", attempts=1, lease_expires_at=PAST))["id"]
    lost = (await insert_job(engine, dispatched_at=PAST))["id"]
    short = settings.model_copy(update={"publish_timeout_s": 0.1})

    async with asyncio.timeout(5):
        await recover_once(sessions, HangingCount(), short)

    async with engine.connect() as connection:
        rows = await connection.execute(
            text("SELECT id, status, dispatched_at FROM jobs WHERE id IN (:a, :b)"),
            {"a": crashed, "b": lost},
        )
        state = {row.id: (row.status, row.dispatched_at is None) for row in rows}
    assert state == {crashed: ("queued", True), lost: ("queued", False)}
    assert "lease recovery: 1 expired, 1 requeued, 0 failed" in caplog.text
    assert (
        "lost-message sweep skipped: the jobs queue's ready count was not read"
        " (builtins.TimeoutError)"
    ) in caplog.text


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

    dispatcher = asyncio.create_task(run_dispatcher(settings, StopSignals()))
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


async def test_the_dispatcher_publishes_a_lost_message_again(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
) -> None:
    # Queued and published hours ago, with nothing in the queue: its message died before a claim.
    job_id = (await insert_job(engine, dispatched_at=PAST))["id"]

    dispatcher = asyncio.create_task(run_dispatcher(settings, StopSignals()))
    try:
        async with asyncio.timeout(5):
            # None in between: cleared, not published yet.
            while (published := await dispatched_at(engine, job_id)) is None or published <= PAST:
                await asyncio.sleep(0.02)
    finally:
        await stop(dispatcher)

    # Cleared by the recovery loop, then published and marked again by the poll loop: one message.
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
    # Every database connection the dispatcher's pool opens or closes. Counted by events, not by
    # the pool after dispose(), which replaces the pool with an empty one: a connection still
    # checked out at shutdown would never show there.
    pool_events = {"connect": 0, "close": 0}

    async def recording_connect_broker(settings: BrokerSettings) -> AbstractRobustConnection:
        connections.append(await connect_broker(settings))
        return connections[-1]

    def recording_create_db_engine(settings: BrokerSettings) -> AsyncEngine:
        engines.append(create_db_engine(settings))
        for name in pool_events:
            event.listen(engines[-1].sync_engine, name, count_pool_event(pool_events, name))
        return engines[-1]

    monkeypatch.setattr(startup, "connect_broker", recording_connect_broker)
    monkeypatch.setattr(process, "create_db_engine", recording_create_db_engine)
    job_id = (await insert_job(engine))["id"]

    dispatcher = asyncio.create_task(run_dispatcher(settings, StopSignals()))
    try:
        # One poll done, so the pool holds a connection; the recovery loop may hold a second.
        await wait_until_dispatched(engine, job_id)
        assert idle_pooled_connections(engines[0]) >= 1
    finally:
        await stop(dispatcher)

    assert connections[0].is_closed
    # Every connection it opened is closed: none left checked out, none left in the pool.
    assert pool_events["connect"] >= 1
    assert pool_events["close"] == pool_events["connect"]


def count_pool_event(counts: dict[str, int], name: str) -> Callable[..., None]:
    def count(*_: Any) -> None:
        counts[name] += 1

    return count


# Stopping (ADR 0008 decision 6, criterion 9)


class GatedPublisher:
    """The real publisher, entered only when the test lets it: a poll held open on demand."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0
        self.publisher: JobPublisher | None = None

    async def publish(self, job_ids: Sequence[uuid.UUID]) -> list[uuid.UUID]:
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        assert self.publisher is not None
        return await self.publisher.publish(job_ids)


@pytest.fixture
def gated(monkeypatch: pytest.MonkeyPatch) -> GatedPublisher:
    gate = GatedPublisher()
    open_real = process.open_dispatch_publisher

    async def open_gated(*args: Any) -> GatedPublisher:
        gate.publisher = await open_real(*args)
        return gate

    monkeypatch.setattr(process, "open_dispatch_publisher", open_gated)
    return gate


async def test_a_stop_signal_lets_the_poll_in_hand_commit_and_starts_no_other(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    gated: GatedPublisher,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    job_id = (await insert_job(engine))["id"]
    signals = StopSignals()

    dispatcher = asyncio.create_task(run_dispatcher(settings, signals))
    try:
        async with asyncio.timeout(5):
            await gated.entered.wait()
        # Mid-poll: the job's row is locked and its publish not yet confirmed.
        signals.received(signal.SIGTERM)
        gated.release.set()
        async with asyncio.timeout(5):
            await dispatcher
    finally:
        await stop_if_running(dispatcher)

    # The poll committed: the job is marked, and its one message is in the queue.
    assert await dispatched_at(engine, job_id) is not None
    channel = await broker_connection.channel()
    queue = await channel.declare_queue(settings.jobs_queue_name, passive=True)
    assert await queued_bodies(queue) == [encode_job_message(job_id)]
    assert gated.calls == 1
    assert "dispatcher stopped" in caplog.text


async def test_a_second_signal_cancels_the_poll_in_hand_and_rolls_it_back(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    gated: GatedPublisher,
    caplog: pytest.LogCaptureFixture,
) -> None:
    job_id = (await insert_job(engine))["id"]
    signals = StopSignals()

    dispatcher = asyncio.create_task(run_dispatcher(settings, signals))
    try:
        async with asyncio.timeout(5):
            await gated.entered.wait()
        signals.received(signal.SIGTERM)
        signals.received(signal.SIGINT)
        # Never released: only the second signal ends the poll.
        async with asyncio.timeout(5):
            await dispatcher
    finally:
        await stop_if_running(dispatcher)

    # Rolled back, not left open: the row lock is gone, so the next start can take the job.
    assert await dispatched_at(engine, job_id) is None
    async with engine.begin() as connection:
        locked = text("SELECT id FROM jobs WHERE id = :id FOR UPDATE NOWAIT")
        assert (await connection.execute(locked, {"id": job_id})).scalar_one() == job_id
    channel = await broker_connection.channel()
    queue = await channel.declare_queue(settings.jobs_queue_name, passive=True)
    assert await queued_bodies(queue) == []
    assert (
        "dispatcher stopped on a second signal: the poll or sweep in hand was cancelled and"
        " rolled back"
    ) in caplog.text


async def test_a_stop_signal_lets_the_recovery_sweep_in_hand_commit(
    engine: AsyncEngine, settings: BrokerSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A lost message's job (published hours ago, the queue empty): the sweep in hand clears it.
    job_id = (await insert_job(engine, dispatched_at=PAST))["id"]
    entered = asyncio.Event()
    release = asyncio.Event()
    recover_once = process.recover_once

    async def gated_recover_once(*args: Any) -> None:
        entered.set()
        await release.wait()
        await recover_once(*args)

    monkeypatch.setattr(process, "recover_once", gated_recover_once)
    signals = StopSignals()
    dispatcher = asyncio.create_task(run_dispatcher(settings, signals))
    try:
        async with asyncio.timeout(5):
            await entered.wait()
        signals.received(signal.SIGTERM)
        release.set()
        async with asyncio.timeout(5):
            await dispatcher
    finally:
        await stop_if_running(dispatcher)

    # Cleared and committed after the stop; no poll started after it to publish it again.
    assert await dispatched_at(engine, job_id) is None


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT], ids=["SIGTERM", "SIGINT"])
async def test_the_dispatcher_process_exits_with_status_0_on_a_signal(
    engine: AsyncEngine,
    settings: BrokerSettings,
    test_database_url: str,
    test_broker_url: str,
    signum: signal.Signals,
) -> None:
    # The signal handlers python -m task_platform.dispatcher installs: without them SIGTERM would
    # end it with status -15 and SIGINT with a KeyboardInterrupt traceback.
    job_id = (await insert_job(engine))["id"]
    env = {
        "DATABASE_URL": test_database_url,
        "BROKER_URL": test_broker_url,
        "JOBS_QUEUE_NAME": settings.jobs_queue_name,
    }

    async def polling(stderr: bytes) -> bool:
        return await dispatched_at(engine, job_id) is not None

    status, stderr = await stop_with_signal("task_platform.dispatcher", env, polling, signum)

    assert status == 0
    # Not printed on failure: the child's errors could quote its settings.
    assert b"Traceback" not in stderr


async def stop_if_running(task: asyncio.Task[Any]) -> None:
    if not task.done():
        task.cancel()
        await asyncio.wait({task})
