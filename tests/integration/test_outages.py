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
