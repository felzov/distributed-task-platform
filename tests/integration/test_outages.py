"""Broker outages through a TCP proxy (ADR 0008 decisions 7, 8 and 10, criterion 10): the shared
RabbitMQ container keeps running; only the processes' connections to it are cut."""

import asyncio
import json
import logging
import signal
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from aio_pika.abc import AbstractRobustConnection
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform.broker.connection import connect_broker
from task_platform.broker.queue_depth import ReadyCount
from task_platform.broker.topology import declare_jobs_queue
from task_platform.config import BrokerSettings
from task_platform.data.attempts import open_attempt
from task_platform.dispatcher.process import recover_once, run_dispatcher
from task_platform.main import create_app
from task_platform.shutdown import StopSignals
from task_platform.worker.process import run_worker
from tests.app_client import app_client
from tests.integration.processes import child_process
from tests.integration.rows import fake_notification_rows, insert_job
from tests.proxy import TcpProxy, running_proxy
from tests.settings import make_test_broker_settings, make_test_settings

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-6620"
JOB = {
    "type": "notification.send",
    "payload_version": 1,
    "payload": {"recipient": f"{SENTINEL}@example.com", "subject": "Hi", "body": SENTINEL},
}
PAST = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture
def settings(
    test_database_url: str, test_broker_url: str, broker_settings: BrokerSettings
) -> BrokerSettings:
    # The queue of broker_settings, which the broker_connection fixture deletes afterwards.
    return make_test_broker_settings(
        test_database_url,
        test_broker_url,
        jobs_queue_name=broker_settings.jobs_queue_name,
        dispatch_poll_interval_s=0.1,
    )


@pytest.fixture(autouse=True)
def _info_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)


def through(proxy: TcpProxy, settings: BrokerSettings) -> BrokerSettings:
    """`settings` with the broker reached through the proxy."""
    url = proxy.url(settings.broker_url.get_secret_value())
    return settings.model_copy(update={"broker_url": SecretStr(url)})


async def job_row(engine: AsyncEngine, job_id: str | uuid.UUID) -> dict[str, Any]:
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
        return dict(result.mappings().one())


async def consumers(connection: AbstractRobustConnection, queue: str) -> int:
    async with connection.channel() as channel:
        result = (await channel.declare_queue(queue, passive=True)).declaration_result
        assert result.consumer_count is not None
        return result.consumer_count


async def wait_for_log(caplog: pytest.LogCaptureFixture, text: str, count: int = 1) -> None:
    async with asyncio.timeout(10):
        while caplog.text.count(text) < count:
            await asyncio.sleep(0.02)


async def test_a_broker_outage_delays_dispatch_and_nothing_claims_it_happened(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    test_database_url: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    name = settings.jobs_queue_name
    dispatcher_signals, worker_signals = StopSignals(), StopSignals()
    async with running_proxy(settings.broker_url.get_secret_value()) as proxy:
        cut_settings = through(proxy, settings)
        dispatcher = asyncio.create_task(run_dispatcher(cut_settings, dispatcher_signals))
        worker = asyncio.create_task(run_worker(cut_settings, worker_signals))
        try:
            await wait_for_log(caplog, "dispatcher started")
            async with asyncio.timeout(10):
                while await consumers(broker_connection, name) != 1:
                    await asyncio.sleep(0.05)

            proxy.cut()
            async with app_client(create_app(make_test_settings(test_database_url))) as client:
                # The API never talks to the broker (ADR 0002): a submit still succeeds.
                submitted = await client.post("/jobs", json=JOB)
                assert submitted.status_code == 202
                job_id = submitted.json()["id"]

                # Polls during the outage publish nothing, and say so.
                await wait_for_log(caplog, "dispatched 0 of 1 due jobs", count=2)
                row = await job_row(engine, job_id)
                assert (row["status"], row["dispatched_at"]) == ("queued", None)
                assert "dispatched 1 of 1 due jobs" not in caplog.text
                assert (await client.get(f"/jobs/{job_id}")).json()["status"] == "queued"

                # Both processes survive the outage and reconnect by themselves.
                assert not dispatcher.done() and not worker.done()
                proxy.restore()
                async with asyncio.timeout(30):
                    while (await client.get(f"/jobs/{job_id}")).json()["status"] != "succeeded":
                        await asyncio.sleep(0.1)
        finally:
            dispatcher_signals.received(signal.SIGTERM)
            worker_signals.received(signal.SIGTERM)
            async with asyncio.timeout(10):
                await asyncio.gather(dispatcher, worker)

    # Published once, run once by the same worker, which consumed again after the outage.
    assert caplog.text.count("dispatched 1 of 1 due jobs") == 1
    assert (await job_row(engine, job_id))["attempts"] == 1
    assert len(await fake_notification_rows(engine, job_id)) == 1
    assert "the jobs queue consumer" not in caplog.text
    async with broker_connection.channel() as channel:
        result = (await channel.declare_queue(name, passive=True)).declaration_result
        assert result.message_count == 0
    assert SENTINEL not in caplog.text


async def insert_crashed(engine: AsyncEngine) -> uuid.UUID:
    """A job whose worker claimed attempt 1, never finished it, and whose lease expired."""
    payload = json.dumps(JOB["payload"])
    job_id: uuid.UUID = (
        await insert_job(
            engine,
            payload=payload,
            status="running",
            attempts=1,
            lease_expires_at=PAST,
            dispatched_at=PAST,
        )
    )["id"]
    async with async_sessionmaker(engine).begin() as session:
        await open_attempt(session, job_id, attempt=1)
    return job_id


async def test_during_a_broker_cut_the_lease_sweep_runs_and_the_lost_message_sweep_waits(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A real cut, where the S3 test used a count that never answers: the read fails at once.
    sessions: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)
    async with broker_connection.channel() as channel:
        await declare_jobs_queue(channel, settings.jobs_queue_name)
    job_id = await insert_crashed(engine)
    # Published long ago and never claimed: its message may be lost, or may wait in the queue.
    lost_id = (await insert_job(engine, dispatched_at=PAST))["id"]
    skipped = "lost-message sweep skipped: the jobs queue's ready count was not read"

    async with running_proxy(settings.broker_url.get_secret_value()) as proxy:
        cut_settings = through(proxy, settings)
        connection = await connect_broker(cut_settings)
        try:
            queue = ReadyCount(connection, settings.jobs_queue_name)
            proxy.cut()
            await recover_once(sessions, queue, cut_settings)

            assert (await job_row(engine, job_id))["status"] == "queued"
            assert skipped in caplog.text
            # Without the count, a message still in the queue cannot be ruled out.
            assert (await job_row(engine, lost_id))["dispatched_at"] == PAST

            # Once the connection is back, the count is read again on the same reader.
            proxy.restore()
            async with asyncio.timeout(20):
                while True:
                    before = caplog.text.count(skipped)
                    await recover_once(sessions, queue, cut_settings)
                    if caplog.text.count(skipped) == before:
                        break
                    await asyncio.sleep(0.2)
            # The queue had no ready message: the job is published again by the next poll.
            assert (await job_row(engine, lost_id))["dispatched_at"] is None
        finally:
            await connection.close()


# A second signal on a link that stopped passing traffic (ADR 0008 decisions 5 and 6). Without
# it, an idle worker's consumer cancel waits for an answer that never comes until the heartbeat
# gives up (60 s by default), the broker connection's close for aio-pika's own 2 s, and the
# database pool's dispose for the driver's timeouts (about 10 s per pooled connection). All
# observed on a frozen link.

# The bound on each close on the frozen side: above aio-pika's own 2 s, so a process that exits
# well within it was stopped by the second signal, not by the bound.
FROZEN_CLOSE_BOUND_S = "3"
SECOND_SIGNAL_AFTER_S = 0.2


def frozen_env(
    proxy: TcpProxy, link: str, settings: BrokerSettings, database_url: str, **extra: str
) -> dict[str, str]:
    broker_url = settings.broker_url.get_secret_value()
    return {
        "DATABASE_URL": proxy.url(database_url) if link == "database" else database_url,
        "BROKER_URL": proxy.url(broker_url) if link == "broker" else broker_url,
        "JOBS_QUEUE_NAME": settings.jobs_queue_name,
        "BROKER_CONNECT_TIMEOUT_S": FROZEN_CLOSE_BOUND_S,
        "DB_CONNECT_TIMEOUT_S": FROZEN_CLOSE_BOUND_S,
        # One poll and one recovery pass at start, then nothing until the stop.
        "DISPATCH_POLL_INTERVAL_S": "3600",
        "RECOVERY_INTERVAL_S": "3600",
    } | extra


async def stop_twice(child: asyncio.subprocess.Process) -> tuple[int, float]:
    """SIGTERM, then SIGINT while the first stop waits on the frozen link; returns the exit status
    and the seconds from the SIGINT to the exit."""
    child.send_signal(signal.SIGTERM)
    await asyncio.sleep(SECOND_SIGNAL_AFTER_S)
    child.send_signal(signal.SIGINT)
    loop = asyncio.get_running_loop()
    start = loop.time()
    async with asyncio.timeout(15):
        status = await child.wait()
    return status, loop.time() - start


def assert_no_stray_errors(stderr: bytes, link: str) -> None:
    # Not printed on failure: the child's errors could quote its settings.
    assert b"Task was destroyed" not in stderr
    assert b"never retrieved" not in stderr
    if link == "broker":
        assert b"Traceback" not in stderr
    else:
        # SQLAlchemy records the close of a pooled connection that the stop cut short ("Exception
        # closing connection", a CancelledError in asyncpg's close) with a traceback.
        assert stderr.count(b"Traceback") == stderr.count(b"Exception closing connection")


@pytest.mark.parametrize(
    ("module", "link", "cut_short"),
    [
        ("task_platform.dispatcher", "broker", b"closing the broker connection"),
        ("task_platform.dispatcher", "database", b"closing the database pool"),
        ("task_platform.worker", "database", b"closing the database pool"),
        # The consumer cancel waits on the frozen link, and the second signal cancels it again;
        # the connection's close that follows ends by itself after aio-pika's 2 s.
        ("task_platform.worker", "broker", None),
    ],
    ids=["dispatcher-broker", "dispatcher-database", "worker-database", "worker-broker"],
)
async def test_a_second_signal_stops_an_idle_process_whose_link_froze(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    test_database_url: str,
    module: str,
    link: str,
    cut_short: bytes | None,
) -> None:
    name = settings.jobs_queue_name
    due_id = (await insert_job(engine))["id"]
    crashed_id = await insert_crashed(engine)
    upstream = settings.broker_url.get_secret_value() if link == "broker" else test_database_url
    async with running_proxy(upstream) as proxy:
        env = frozen_env(proxy, link, settings, test_database_url)
        async with child_process(module, env) as (child, stderr):
            # Idle: past its start-up, its first poll and recovery pass committed, with a pooled
            # database connection.
            async with asyncio.timeout(15):
                if module == "task_platform.worker":
                    # Its consumer at the broker; the job's message is never published.
                    while await consumers_or_none(broker_connection, name) != 1:
                        await asyncio.sleep(0.05)
                else:
                    while (await job_row(engine, due_id))["dispatched_at"] is None or (
                        await job_row(engine, crashed_id)
                    )["status"] != "queued":
                        await asyncio.sleep(0.05)
            await asyncio.sleep(0.5)

            proxy.freeze()
            status, elapsed = await stop_twice(child)

    assert status == 0
    if cut_short is None:
        assert elapsed < float(FROZEN_CLOSE_BOUND_S) + 0.5
    else:
        assert cut_short + b" cut short by the second signal" in stderr
        assert elapsed < 1.5
    assert_no_stray_errors(bytes(stderr), link)


async def test_a_second_signal_stops_a_dispatcher_whose_poll_is_stuck_on_a_frozen_database(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    test_database_url: str,
) -> None:
    due_id = (await insert_job(engine))["id"]
    async with running_proxy(test_database_url) as proxy:
        env = frozen_env(
            proxy,
            "database",
            settings,
            test_database_url,
            DISPATCH_POLL_INTERVAL_S="0.05",
            # Once forced, the pool's dispose still gets its bound: its idle connections, left
            # by the recovery pass, are cut at it.
            DB_CONNECT_TIMEOUT_S="0.5",
        )
        async with child_process("task_platform.dispatcher", env) as (child, stderr):
            async with asyncio.timeout(15):
                while (await job_row(engine, due_id))["dispatched_at"] is None:
                    await asyncio.sleep(0.05)
            proxy.freeze()
            # The next poll, 0.05 s later, waits on the frozen link.
            await asyncio.sleep(0.3)
            status, elapsed = await stop_twice(child)

    assert status == 0
    assert b"dispatcher stopped on a second signal: the poll or sweep in hand was cancelled" in (
        stderr
    )
    # SQLAlchemy closes the cancelled poll's connection inside asyncio.shield with a 2 s timeout
    # (sqlalchemy/connectors/asyncio.py and dialects/postgresql/asyncpg.py, 2.1.6): no cancel cuts
    # it. Then the dispose's 0.5 s bound (2.6 s in all, observed), and room for a loaded machine.
    assert elapsed < 4.0
    assert_no_stray_errors(bytes(stderr), "database")


async def consumers_or_none(connection: AbstractRobustConnection, queue: str) -> int | None:
    try:
        return await consumers(connection, queue)
    except Exception:
        return None
