"""A job's whole path on real PostgreSQL and RabbitMQ: `POST /jobs` saves it, one dispatcher poll
publishes it, a worker runs it, and `GET /jobs/{id}` reports it succeeded. Then the same path with
failed attempts: retried after a backoff and published again, until they succeed or run out."""

import asyncio
import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from aio_pika.abc import AbstractRobustConnection
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform.broker.connection import connect_broker
from task_platform.config import BrokerSettings
from task_platform.dispatcher.process import open_dispatch_publisher, poll_forever, poll_once
from task_platform.domain.payloads import NOTIFICATION_SEND, PayloadModel
from task_platform.main import create_app
from task_platform.services.dispatch import Publisher
from task_platform.services.execution import ClaimedJob, Handler
from task_platform.services.notifications import send_notification
from task_platform.worker.handlers import HANDLERS
from task_platform.worker.process import consume_forever, open_jobs_queue
from tests.app_client import app_client
from tests.integration.rows import fake_notification_rows, fake_result
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
    # `answers` records an answer even when the broker call failed; the worker then logs it.
    assert "could not ack" not in caplog.text
    assert (status["attempts"], status["max_attempts"]) == (1, api_settings.default_max_attempts)
    # The client sees the handler's result through the API.
    row = await job_row(engine, job_id)
    assert (row["error_code"], row["lease_expires_at"]) == (None, None)
    # The fake provider was called exactly once, and its notification is the job's result.
    [notification] = await fake_notification_rows(engine, job_id)
    assert notification["attempt"] == 1
    assert (status["result"], status["failure"]) == (fake_result(notification), None)
    # Acked: nothing left in the queue once the worker's connection is closed.
    async with broker_connection.channel() as channel:
        queue_state = await channel.declare_queue(settings.jobs_queue_name, passive=True)
        assert queue_state.declaration_result.message_count == 0
    # The dispatcher's INFO line shows the capture worked, so the sentinel check is not vacuous.
    assert "dispatched 1 of 1 due jobs" in caplog.text
    assert SENTINEL not in caplog.text


# Retries

# Raised by the failing handler. Exception text never reaches the database or the logs at INFO+.
ERROR_SENTINEL = "exc-sentinel-5519"


@pytest.fixture
def retry_settings(settings: BrokerSettings) -> BrokerSettings:
    # Backoffs of at most 10 ms and a 50 ms poll interval, so three attempts take well under a
    # second; the backoff policy itself is tested in tests/unit/test_backoff.py.
    return settings.model_copy(
        update={
            "retry_base_delay_s": 0.01,
            "retry_max_delay_s": 0.01,
            "dispatch_poll_interval_s": 0.05,
        }
    )


def failing_handlers(succeeds_on: int | None) -> Mapping[str, Handler]:
    """The real handler, behind one that raises before it until attempt `succeeds_on`."""

    async def handler(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        if succeeds_on is None or job.attempt < succeeds_on:
            raise RuntimeError(ERROR_SENTINEL)
        return await send_notification(session_factory, job, payload)

    return {NOTIFICATION_SEND: handler}


@asynccontextmanager
async def running(
    broker_connection: AbstractRobustConnection,
    publisher: Publisher,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    handlers: Mapping[str, Handler],
) -> AsyncIterator[None]:
    """The dispatcher's poll loop and one worker on a connection of its own, for the block."""
    worker_connection = await connect_broker(settings)
    try:
        queue = await open_jobs_queue(worker_connection, settings)
        tasks = [
            asyncio.create_task(poll_forever(sessions, publisher, settings)),
            asyncio.create_task(consume_forever(queue, sessions, handlers, settings)),
        ]
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            # Raises whatever else the worker task ended with. The dispatcher's poll logs its
            # errors instead of raising them, so the tests check those through the logs.
            for task in tasks:
                with pytest.raises(asyncio.CancelledError):
                    await task
    finally:
        await worker_connection.close()


async def wait_until(client: httpx.AsyncClient, job_id: str, status: str) -> dict[str, Any]:
    """Poll `GET /jobs/{id}` until the job reaches `status`, a final one."""
    async with asyncio.timeout(10):
        while True:
            body: dict[str, Any] = (await client.get(f"/jobs/{job_id}")).json()
            if body["status"] == status:
                return body
            assert body["status"] in {"queued", "running"}
            await asyncio.sleep(0.05)


async def wait_for_answers(answers: list[str], count: int) -> None:
    # The ack goes out after the final update has committed (see the first test).
    async with asyncio.timeout(5):
        while len(answers) < count:
            await asyncio.sleep(0.02)


def warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Every line this project's code logged at WARNING or above, in order."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.name.startswith("task_platform") and record.levelno >= logging.WARNING
    ]


def handler_failures(job_id: str, attempts: range) -> list[str]:
    return [
        f"job {job_id} attempt {n} failed: handler_error (builtins.RuntimeError)" for n in attempts
    ]


async def stored_text(engine: AsyncEngine, job_id: str) -> str:
    """Every column the worker writes, of the job and its attempts, as text."""
    async with engine.connect() as connection:
        rows = await connection.execute(
            text(
                "SELECT j.status, j.error_code, j.result::text, a.*"
                " FROM jobs j JOIN job_attempts a ON a.job_id = j.id WHERE j.id = :id"
            ),
            {"id": job_id},
        )
        return repr(rows.all())


async def queued_messages(
    broker_connection: AbstractRobustConnection, settings: BrokerSettings
) -> int:
    async with broker_connection.channel() as channel:
        queue_state = await channel.declare_queue(settings.jobs_queue_name, passive=True)
        count = queue_state.declaration_result.message_count
        assert count is not None
        return count


async def test_a_job_that_fails_twice_is_retried_and_succeeds_on_attempt_3(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    retry_settings: BrokerSettings,
    test_database_url: str,
    answers: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    publisher = await open_dispatch_publisher(broker_connection, retry_settings)

    async with app_client(
        create_app(make_test_settings(test_database_url, default_max_attempts=3))
    ) as client:
        job_id = (await client.post("/jobs", json=JOB)).json()["id"]
        async with running(
            broker_connection, publisher, sessions, retry_settings, failing_handlers(3)
        ):
            status = await wait_until(client, job_id, "succeeded")
            await wait_for_answers(answers, 3)
        attempts_response = await client.get(f"/jobs/{job_id}/attempts")
    attempts = attempts_response.json()["attempts"]

    # Each attempt's message was acked: the two retries went back through the database and the
    # dispatcher, not through a broker requeue (ADR 0005 decision 4).
    assert answers == ["ack", "ack", "ack"]
    assert "could not ack" not in caplog.text
    assert (status["attempts"], status["max_attempts"]) == (3, 3)
    [notification] = await fake_notification_rows(engine, job_id)
    assert (status["result"], status["failure"]) == (fake_result(notification), None)
    assert [(a["attempt"], a["outcome"]) for a in attempts] == [
        (1, "failed"),
        (2, "failed"),
        (3, "succeeded"),
    ]
    assert [a["failure"] and a["failure"]["code"] for a in attempts] == [
        "handler_error",
        "handler_error",
        None,
    ]
    # Success cleared the last failure's code (ADR 0005 decision 6); the history keeps it.
    row = await job_row(engine, job_id)
    assert (row["error_code"], row["lease_expires_at"]) == (None, None)
    # Only attempt 3 got past the failure to the fake provider.
    assert await notification_attempts(engine, job_id) == [3]
    assert await queued_messages(broker_connection, retry_settings) == 0
    # One publish per attempt, and the worker's INFO line for each retry (both also show that the
    # capture worked, so the sentinel checks below are not vacuous).
    assert caplog.text.count("dispatched 1 of 1 due jobs") == 3
    for attempt in (1, 2):
        assert f"job {job_id} attempt {attempt} failed with handler_error; retry in" in caplog.text
    # Nothing else went wrong on the way: no failed poll, publish or ack, no stale or refused
    # finish, no retry refused for a spent budget.
    assert warnings(caplog) == handler_failures(job_id, range(1, 3))
    for sentinel in (SENTINEL, ERROR_SENTINEL):
        assert sentinel not in caplog.text
        assert sentinel not in attempts_response.text
    assert ERROR_SENTINEL not in await stored_text(engine, job_id)


def sending_then_failing_handlers(succeeds_on: int) -> Mapping[str, Handler]:
    """The real handler on every attempt, but attempts before `succeeds_on` fail after it."""

    async def handler(
        session_factory: async_sessionmaker[AsyncSession], job: ClaimedJob, payload: PayloadModel
    ) -> dict[str, Any]:
        result = await send_notification(session_factory, job, payload)
        if job.attempt < succeeds_on:
            raise RuntimeError(ERROR_SENTINEL)
        return result

    return {NOTIFICATION_SEND: handler}


async def test_a_job_whose_attempts_all_reach_the_provider_sends_one_notification(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    retry_settings: BrokerSettings,
    test_database_url: str,
    answers: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The repeat ADR 0007 exists for: each attempt calls the provider and then fails (as when the
    # provider answered but the worker's next step did not), so three attempts call it three times.
    caplog.set_level(logging.INFO)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    publisher = await open_dispatch_publisher(broker_connection, retry_settings)

    async with app_client(
        create_app(make_test_settings(test_database_url, default_max_attempts=3))
    ) as client:
        job_id = (await client.post("/jobs", json=JOB)).json()["id"]
        async with running(
            broker_connection, publisher, sessions, retry_settings, sending_then_failing_handlers(3)
        ):
            status = await wait_until(client, job_id, "succeeded")
            await wait_for_answers(answers, 3)

    assert answers == ["ack", "ack", "ack"]
    assert status["attempts"] == 3
    # One notification, from attempt 1, and the job's result names it.
    [notification] = await fake_notification_rows(engine, job_id)
    assert notification["attempt"] == 1
    assert (status["result"], status["failure"]) == (fake_result(notification), None)
    assert warnings(caplog) == handler_failures(job_id, range(1, 3))
    assert SENTINEL not in caplog.text


async def test_a_job_that_always_fails_stops_after_its_attempts(
    engine: AsyncEngine,
    broker_connection: AbstractRobustConnection,
    retry_settings: BrokerSettings,
    test_database_url: str,
    answers: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    publisher = await open_dispatch_publisher(broker_connection, retry_settings)

    async with app_client(
        create_app(make_test_settings(test_database_url, default_max_attempts=3))
    ) as client:
        job_id = (await client.post("/jobs", json=JOB)).json()["id"]
        async with running(
            broker_connection, publisher, sessions, retry_settings, failing_handlers(None)
        ):
            status = await wait_until(client, job_id, "failed")
            await wait_for_answers(answers, 3)
        attempts_response = await client.get(f"/jobs/{job_id}/attempts")
        failed_response = await client.get("/jobs?status=failed")
    attempts = attempts_response.json()["attempts"]

    assert answers == ["ack", "ack", "ack"]
    assert (status["attempts"], status["max_attempts"]) == (3, 3)
    # The job that ran out of attempts is the one dead letter of its kind: a failed row, listed
    # exactly as GET /jobs/{id} shows it (ADR 0006 decision 1).
    assert failed_response.json() == {"jobs": [status], "next_cursor": None}
    # Failed with the last attempt's code.
    assert status["failure"]["code"] == "handler_error"
    assert [(a["attempt"], a["outcome"]) for a in attempts] == [
        (1, "failed"),
        (2, "failed"),
        (3, "failed"),
    ]
    assert [a["failure"]["code"] for a in attempts] == ["handler_error"] * 3
    assert await notification_attempts(engine, job_id) == []
    # No further publish: with no worker left to consume, one more poll would leave its message
    # in the queue.
    await poll_once(sessions, publisher, retry_settings)
    assert await queued_messages(broker_connection, retry_settings) == 0
    assert (await job_row(engine, job_id))["status"] == "failed"
    assert caplog.text.count("dispatched 1 of 1 due jobs") == 3
    for attempt in (1, 2):
        assert f"job {job_id} attempt {attempt} failed with handler_error; retry in" in caplog.text
    assert f"job {job_id} attempt 3 failed with handler_error; retry in" not in caplog.text
    # The last attempt failed the job directly: a spent budget's retry, refused by the database and
    # failed by the fallback, would log an ERROR here (ADR 0004, update 2026-11-07).
    assert warnings(caplog) == handler_failures(job_id, range(1, 4))
    for sentinel in (SENTINEL, ERROR_SENTINEL):
        assert sentinel not in caplog.text
        assert sentinel not in attempts_response.text
        assert sentinel not in failed_response.text
    assert ERROR_SENTINEL not in await stored_text(engine, job_id)
