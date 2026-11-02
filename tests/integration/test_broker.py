"""RabbitMQ connection and the jobs queue against a real broker (ADR 0004 decisions 1 and 2)."""

import time
from urllib.parse import urlsplit

import pytest
from aio_pika.abc import AbstractRobustConnection
from aio_pika.exceptions import ChannelPreconditionFailed, ProbableAuthenticationError

from task_platform.broker.connection import connect_broker
from task_platform.broker.topology import declare_jobs_queue
from task_platform.config import BrokerSettings
from tests.settings import UNREACHABLE_DATABASE_URL, make_test_broker_settings

pytestmark = pytest.mark.integration

PASSWORD = "s3cr3t-sentinel-7731"


async def test_jobs_queue_declaration_is_repeatable(
    broker_connection: AbstractRobustConnection, broker_settings: BrokerSettings
) -> None:
    # The dispatcher and the worker both declare it at start, in either order.
    first = await declare_jobs_queue(
        await broker_connection.channel(), broker_settings.jobs_queue_name
    )
    second = await declare_jobs_queue(
        await broker_connection.channel(), broker_settings.jobs_queue_name
    )

    assert first.name == second.name == broker_settings.jobs_queue_name
    assert first.durable and second.durable
    assert second.declaration_result.message_count == 0


async def test_jobs_queue_is_a_quorum_queue(
    broker_connection: AbstractRobustConnection, broker_settings: BrokerSettings
) -> None:
    await declare_jobs_queue(await broker_connection.channel(), broker_settings.jobs_queue_name)

    # RabbitMQ compares the arguments of an existing queue: a classic declaration of the same name
    # is refused, which shows the queue really was created as a quorum queue.
    with pytest.raises(ChannelPreconditionFailed, match="x-queue-type"):
        await (await broker_connection.channel()).declare_queue(
            broker_settings.jobs_queue_name, durable=True
        )


async def test_heartbeat_setting_reaches_the_connection(test_broker_url: str) -> None:
    settings = make_test_broker_settings(
        UNREACHABLE_DATABASE_URL, test_broker_url, broker_heartbeat_s=17
    )
    connection = await connect_broker(settings)
    try:
        # The interval the client sent in Connection.Tune-Ok, so the broker's is overridden.
        assert connection.transport is not None
        assert connection.transport.connection.connection_tune.heartbeat == 17
    finally:
        await connection.close()


async def test_wrong_password_fails_at_once_without_leaking_it(
    test_broker_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("DEBUG")
    url = urlsplit(test_broker_url)
    host_and_port = url.netloc.rpartition("@")[2]
    wrong = url._replace(netloc=f"{url.username}:{PASSWORD}@{host_and_port}").geturl()
    settings = make_test_broker_settings(UNREACHABLE_DATABASE_URL, wrong)

    start = time.monotonic()
    with pytest.raises(ProbableAuthenticationError) as exc_info:
        await connect_broker(settings)

    # Refused by the broker, not retried until the connect timeout.
    assert time.monotonic() - start < settings.broker_connect_timeout_s
    assert PASSWORD not in str(exc_info.value)
    assert PASSWORD not in repr(exc_info.value)
    assert PASSWORD not in caplog.text
