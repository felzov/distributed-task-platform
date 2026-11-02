"""A job's whole path on real PostgreSQL and RabbitMQ: `POST /jobs` saves it, one dispatcher poll
publishes it, a worker runs it, and `GET /jobs/{id}` reports it succeeded."""

import asyncio
import logging
from typing import Any

import httpx
import pytest
from aio_pika.abc import AbstractRobustConnection
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from task_platform.broker.connection import connect_broker
from task_platform.config import BrokerSettings
from task_platform.dispatcher.process import open_dispatch_publisher, poll_once
from task_platform.main import create_app
from task_platform.worker.handlers import HANDLERS
from task_platform.worker.process import consume_forever, open_jobs_queue
from tests.app_client import app_client
from tests.settings import make_test_broker_settings, make_test_settings

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-7731"
JOB = {
    "type": "notification.send",
    "payload_version": 1,
    "payload": {"recipient": f"{SENTINEL}@example.com", "subject": "Hi", "body": SENTINEL},
}


@pytest.fixture
def settings(
    test_database_url: str, test_broker_url: str, broker_settings: BrokerSettings
) -> BrokerSettings:
    # The queue of broker_settings, which the broker_connection fixture deletes afterwards.
    return make_test_broker_settings(
        test_database_url, test_broker_url, jobs_queue_name=broker_settings.jobs_queue_name
    )


async def job_row(engine: AsyncEngine, job_id: str) -> dict[str, Any]:
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
        return dict(result.mappings().one())


async def notification_attempts(engine: AsyncEngine, job_id: str) -> list[int]:
    async with engine.connect() as connection:
        rows = await connection.scalars(
            text("SELECT attempt FROM fake_notifications WHERE job_id = :id"), {"id": job_id}
        )
        return list(rows)


async def wait_until_succeeded(client: httpx.AsyncClient, job_id: str) -> dict[str, Any]:
    """Poll `GET /jobs/{id}` as a client would, until the job succeeded."""
    async with asyncio.timeout(10):
        while True:
            body: dict[str, Any] = (await client.get(f"/jobs/{job_id}")).json()
            if body["status"] == "succeeded":
                return body
            assert body["status"] in {"queued", "running"}
            await asyncio.sleep(0.05)


async def test_a_submitted_job_is_dispatched_run_once_and_reported_succeeded(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    settings: BrokerSettings,
    test_database_url: str,
    answers: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # No payload in logs at INFO+, for the API, dispatcher and worker code this test runs.
    caplog.set_level(logging.INFO)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    api_settings = make_test_settings(test_database_url)

    async with app_client(create_app(api_settings)) as client:
        submitted = await client.post("/jobs", json=JOB)
        assert submitted.status_code == 202
        job_id = submitted.json()["id"]

        # The dispatcher, one poll: the job goes out only once a queue confirmed it.
        publisher = await open_dispatch_publisher(broker_connection, settings)
        await poll_once(sessions, publisher, settings)
        assert (await job_row(engine, job_id))["dispatched_at"] is not None

        # The worker, on a connection of its own as in its own process.
        worker_connection = await connect_broker(settings)
        try:
            queue = await open_jobs_queue(worker_connection, settings)
            worker = asyncio.create_task(consume_forever(queue, sessions, HANDLERS, settings))
            try:
                status = await wait_until_succeeded(client, job_id)
                # The worker acks after the final update has committed, so `succeeded` can show
                # before the ack has gone out; stopping the worker then would requeue the message.
                async with asyncio.timeout(5):
                    while not answers:
                        await asyncio.sleep(0.02)
            finally:
                worker.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await worker
        finally:
            await worker_connection.close()

    assert answers == ["ack"]
    assert (status["attempts"], status["max_attempts"]) == (1, api_settings.default_max_attempts)
    row = await job_row(engine, job_id)
    assert row["result"] == {"provider": "fake"}
    assert (row["error_code"], row["lease_expires_at"]) == (None, None)
    # The fake provider was called exactly once.
    assert await notification_attempts(engine, job_id) == [1]
    # Acked: nothing left in the queue once the worker's connection is closed.
    async with broker_connection.channel() as channel:
        queue_state = await channel.declare_queue(settings.jobs_queue_name, passive=True)
        assert queue_state.declaration_result.message_count == 0
    assert SENTINEL not in caplog.text
