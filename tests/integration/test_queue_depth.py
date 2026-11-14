"""Reading the jobs queue's ready count on real RabbitMQ (ADR 0008 decision 2)."""

import asyncio

import aiormq
import pytest
from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractRobustConnection

from task_platform.broker.connection import connect_broker
from task_platform.broker.queue_depth import ReadyCount
from task_platform.broker.topology import declare_jobs_queue
from task_platform.config import BrokerSettings

pytestmark = pytest.mark.integration


async def publish(connection: AbstractRobustConnection, queue_name: str, count: int) -> None:
    async with connection.channel() as channel:
        for _ in range(count):
            await channel.default_exchange.publish(
                Message(b"{}", delivery_mode=DeliveryMode.PERSISTENT), routing_key=queue_name
            )


async def test_the_count_is_live_on_every_read(
    broker_connection: AbstractRobustConnection, broker_settings: BrokerSettings
) -> None:
    name = broker_settings.jobs_queue_name
    async with broker_connection.channel() as channel:
        await declare_jobs_queue(channel, name)
    reader = ReadyCount(broker_connection, name)

    # Read before and after publishing: a cached declare result would keep answering 0.
    assert await reader.ready_messages() == 0
    await publish(broker_connection, name, 3)
    assert await reader.ready_messages() == 3


async def test_a_message_held_by_a_consumer_is_not_counted(
    broker_connection: AbstractRobustConnection, broker_settings: BrokerSettings
) -> None:
    # The gap ADR 0008 accepts: a worker's unacked message is invisible to the guard.
    name = broker_settings.jobs_queue_name
    async with broker_connection.channel() as channel:
        queue = await declare_jobs_queue(channel, name)
        await publish(broker_connection, name, 2)
        held = await queue.get(no_ack=False)
        reader = ReadyCount(broker_connection, name)

        assert await reader.ready_messages() == 1

        await held.nack(requeue=True)
        async with asyncio.timeout(5):
            while await reader.ready_messages() != 2:
                await asyncio.sleep(0.05)


async def test_a_missing_queue_raises_and_leaves_no_channel_behind(
    broker_settings: BrokerSettings,
) -> None:
    # A connection of its own, so its channels are only the reader's.
    connection = await connect_broker(broker_settings)
    try:
        reader = ReadyCount(connection, broker_settings.jobs_queue_name)
        # Twice in a row: a robust channel the broker closed reopens itself within moments, so a
        # reader that only replaced closed channels would leave the first one open next to the
        # second.
        for _ in range(2):
            with pytest.raises(aiormq.exceptions.ChannelNotFoundEntity):
                await reader.ready_messages()
        await asyncio.sleep(0.5)
        assert open_channels(connection) == 0

        # Once the queue exists, the same reader reads it on a new channel.
        async with connection.channel() as channel:
            await declare_jobs_queue(channel, broker_settings.jobs_queue_name)
        assert await reader.ready_messages() == 0
        assert open_channels(connection) == 1
    finally:
        async with connection.channel() as channel:
            await channel.queue_delete(broker_settings.jobs_queue_name)
            await channel.queue_delete(broker_settings.jobs_queue_name + ".dead")
        await connection.close()


async def test_reads_reuse_one_channel(broker_settings: BrokerSettings) -> None:
    # One pass per recovery interval: a channel per read would pile up on the connection until
    # RabbitMQ refuses new ones (channel_max, 2047 by default).
    connection = await connect_broker(broker_settings)
    try:
        async with connection.channel() as channel:
            await declare_jobs_queue(channel, broker_settings.jobs_queue_name)
        reader = ReadyCount(connection, broker_settings.jobs_queue_name)
        for _ in range(3):
            assert await reader.ready_messages() == 0
        assert open_channels(connection) == 1
    finally:
        async with connection.channel() as channel:
            await channel.queue_delete(broker_settings.jobs_queue_name)
            await channel.queue_delete(broker_settings.jobs_queue_name + ".dead")
        await connection.close()


def open_channels(connection: AbstractRobustConnection) -> int:
    # The AMQP connection's own table of open channels, below aio-pika's robust layer.
    underlay = connection.transport
    assert underlay is not None
    channels = underlay.connection.channels.values()
    return sum(channel is not None and not channel.is_closed for channel in channels)
