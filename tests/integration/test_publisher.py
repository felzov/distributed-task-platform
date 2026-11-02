"""JobPublisher against a real RabbitMQ: what the broker confirms, returns and refuses."""

import time
import uuid

import pytest
from aio_pika import DeliveryMode
from aio_pika.abc import AbstractRobustConnection

from task_platform.broker.connection import connect_broker
from task_platform.broker.publisher import open_job_publisher
from task_platform.broker.topology import declare_jobs_queue
from task_platform.config import BrokerSettings
from task_platform.domain.messages import encode_job_message

pytestmark = pytest.mark.integration

TIMEOUT_S = 5.0


async def test_confirmed_jobs_are_in_the_queue_as_the_contract_says(
    broker_connection: AbstractRobustConnection, broker_settings: BrokerSettings
) -> None:
    queue = await declare_jobs_queue(
        await broker_connection.channel(), broker_settings.jobs_queue_name
    )
    publisher = await open_job_publisher(
        broker_connection, queue_name=broker_settings.jobs_queue_name, timeout_s=TIMEOUT_S
    )
    job_ids = [uuid.uuid4() for _ in range(3)]

    assert await publisher.publish(job_ids) == job_ids

    received = []
    for _ in job_ids:
        message = await queue.get(no_ack=True, fail=False)
        assert message is not None
        assert message.delivery_mode == DeliveryMode.PERSISTENT
        assert message.content_type == "application/json"
        received.append(message.body)
    assert sorted(received) == sorted(encode_job_message(job_id) for job_id in job_ids)
    assert await queue.get(no_ack=True, fail=False) is None


async def test_a_message_no_queue_takes_is_not_confirmed(
    broker_connection: AbstractRobustConnection,
    broker_settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The queue was never declared: the broker acks the publish but returns the message
    # (`mandatory`). Counting that ack as published would lose the job.
    publisher = await open_job_publisher(
        broker_connection, queue_name=broker_settings.jobs_queue_name, timeout_s=TIMEOUT_S
    )
    job_id = uuid.uuid4()

    assert await publisher.publish([job_id]) == []
    assert "job messages not confirmed: 1 of 1 (aiormq.exceptions.PublishError: 1)" in caplog.text


async def test_a_message_the_broker_refuses_is_not_confirmed(
    broker_connection: AbstractRobustConnection,
    broker_settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A queue that is always full and refuses new messages: the broker nacks every publish.
    channel = await broker_connection.channel()
    await channel.declare_queue(
        broker_settings.jobs_queue_name,
        # Durable: RabbitMQ 4 refuses transient non-exclusive queues.
        durable=True,
        arguments={"x-max-length": 0, "x-overflow": "reject-publish"},
    )
    publisher = await open_job_publisher(
        broker_connection, queue_name=broker_settings.jobs_queue_name, timeout_s=TIMEOUT_S
    )
    job_id = uuid.uuid4()

    assert await publisher.publish([job_id]) == []
    assert "job messages not confirmed: 1 of 1 (aiormq.exceptions.DeliveryError: 1)" in caplog.text


async def test_nothing_is_confirmed_over_a_closed_connection(
    broker_settings: BrokerSettings, caplog: pytest.LogCaptureFixture
) -> None:
    # Its own connection: the fixture's teardown needs an open one.
    connection = await connect_broker(broker_settings)
    publisher = await open_job_publisher(
        connection, queue_name=broker_settings.jobs_queue_name, timeout_s=TIMEOUT_S
    )
    job_id = uuid.uuid4()
    await connection.close()

    started = time.monotonic()
    assert await publisher.publish([job_id]) == []
    # Fails at once rather than waiting for the timeout.
    assert time.monotonic() - started < TIMEOUT_S
    assert "job messages not confirmed: 1 of 1 (" in caplog.text
