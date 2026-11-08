"""The dead-letter queue against a real broker: what reaches it, with which reason, and what does
not (ADR 0006 decisions 2-5 and 7). The worker's own path into it is in test_worker.py."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import pytest
from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection
from aio_pika.exceptions import ChannelPreconditionFailed

from task_platform.broker.topology import (
    DELIVERY_LIMIT,
    dead_letter_queue_name,
    declare_jobs_queue,
    jobs_queue_arguments,
)
from task_platform.config import BrokerSettings
from task_platform.dispatcher.process import open_dispatch_publisher
from task_platform.worker.process import open_jobs_queue

pytestmark = pytest.mark.integration

BODY = b'{"message_version": 1, "job_id": "00000000-0000-0000-0000-000000000000"}'
LIMIT = 2


async def publish(connection: AbstractRobustConnection, queue_name: str, body: bytes) -> None:
    async with connection.channel() as channel:
        await channel.default_exchange.publish(
            Message(body, delivery_mode=DeliveryMode.PERSISTENT), routing_key=queue_name
        )


async def count(connection: AbstractRobustConnection, queue_name: str) -> int:
    async with connection.channel() as channel:
        queue = await channel.declare_queue(queue_name, passive=True)
        messages = queue.declaration_result.message_count
        assert messages is not None
        return messages


async def wait_for_count(
    connection: AbstractRobustConnection, queue_name: str, expected: int
) -> None:
    # Dead-lettering is asynchronous (about 0.3 s in the ADR 0006 spike): poll, never sleep.
    async with asyncio.timeout(10):
        while await count(connection, queue_name) != expected:
            await asyncio.sleep(0.05)


@asynccontextmanager
async def holding(
    connection: AbstractRobustConnection, queue_name: str
) -> AsyncIterator[AbstractIncomingMessage]:
    """The next message, unanswered, on a channel of its own that is closed on exit. Closing it
    returns the message to the queue if it was not answered, as when a worker dies."""
    async with connection.channel() as channel:
        queue = await channel.declare_queue(queue_name, passive=True)
        async with asyncio.timeout(10):
            while (message := await queue.get(no_ack=False, fail=False)) is None:
                await asyncio.sleep(0.05)
        yield message


async def take_dead_letter(
    connection: AbstractRobustConnection, queue_name: str
) -> AbstractIncomingMessage:
    dead = dead_letter_queue_name(queue_name)
    await wait_for_count(connection, dead, 1)
    async with connection.channel() as channel:
        queue = await channel.declare_queue(dead, passive=True)
        message = await queue.get(no_ack=True)
        assert message is not None
        return message


def death(message: AbstractIncomingMessage) -> dict[str, object]:
    """The reason and queue RabbitMQ recorded, as text."""
    headers = message.headers or {}
    entries = headers["x-death"]
    assert isinstance(entries, list) and len(entries) == 1
    entry = entries[0]
    assert isinstance(entry, dict)
    return {
        key: value.decode() if isinstance(value, bytes) else value
        for key, value in {
            "reason": entry["reason"],
            "queue": entry["queue"],
            "first reason": headers["x-first-death-reason"],
            "first queue": headers["x-first-death-queue"],
        }.items()
    }


def died(queue_name: str, reason: str) -> dict[str, object]:
    return {
        "reason": reason,
        "queue": queue_name,
        "first reason": reason,
        "first queue": queue_name,
    }


# The queues


async def test_both_queues_are_declared_and_declaring_again_is_a_no_op(
    broker_connection: AbstractRobustConnection, broker_settings: BrokerSettings
) -> None:
    name = broker_settings.jobs_queue_name
    for _ in range(2):
        await declare_jobs_queue(await broker_connection.channel(), name)

    assert dead_letter_queue_name(name) == f"{name}.dead"
    assert await count(broker_connection, dead_letter_queue_name(name)) == 0
    # The dead-letter queue is a quorum queue too: a classic declaration of it is refused.
    with pytest.raises(ChannelPreconditionFailed, match="x-queue-type"):
        await (await broker_connection.channel()).declare_queue(
            dead_letter_queue_name(name), durable=True
        )


async def declare_directly(connection: AbstractRobustConnection, settings: BrokerSettings) -> None:
    await declare_jobs_queue(await connection.channel(), settings.jobs_queue_name)


async def open_worker_queue(connection: AbstractRobustConnection, settings: BrokerSettings) -> None:
    await open_jobs_queue(connection, settings)


async def open_dispatcher(connection: AbstractRobustConnection, settings: BrokerSettings) -> None:
    await open_dispatch_publisher(connection, settings)


@pytest.mark.parametrize(
    "declare",
    [
        pytest.param(declare_directly, id="declare_jobs_queue"),
        pytest.param(open_worker_queue, id="worker start"),
        pytest.param(open_dispatcher, id="dispatcher start"),
    ],
)
async def test_a_jobs_queue_created_without_dead_lettering_fails_the_declaration(
    broker_connection: AbstractRobustConnection,
    broker_settings: BrokerSettings,
    declare: Callable[[AbstractRobustConnection, BrokerSettings], Awaitable[None]],
) -> None:
    # As created before phase 6: both processes stop at start instead of running against it,
    # until the queue is recreated once (ADR 0006 decision 7).
    await (await broker_connection.channel()).declare_queue(
        broker_settings.jobs_queue_name, durable=True, arguments={"x-queue-type": "quorum"}
    )

    with pytest.raises(ChannelPreconditionFailed, match="x-dead-letter-exchange"):
        await declare(broker_connection, broker_settings)


async def test_the_delivery_limit_is_rabbitmqs_default_and_an_int(
    broker_connection: AbstractRobustConnection, broker_settings: BrokerSettings
) -> None:
    name = broker_settings.jobs_queue_name
    await declare_jobs_queue(await broker_connection.channel(), name)

    assert jobs_queue_arguments(name)["x-delivery-limit"] == DELIVERY_LIMIT == 20
    # The same value as a string differs from the declared int.
    with pytest.raises(ChannelPreconditionFailed, match="x-delivery-limit"):
        await (await broker_connection.channel()).declare_queue(
            name, durable=True, arguments=jobs_queue_arguments(name) | {"x-delivery-limit": "20"}
        )


@pytest.mark.parametrize(
    ("argument", "expected", "other"),
    [
        pytest.param("x-dead-letter-strategy", "at-least-once", "at-most-once", id="strategy"),
        pytest.param("x-overflow", "reject-publish", "drop-head", id="overflow"),
    ],
)
async def test_the_queue_holds_at_least_once_dead_lettering(
    broker_connection: AbstractRobustConnection,
    broker_settings: BrokerSettings,
    argument: str,
    expected: str,
    other: str,
) -> None:
    # While the dead-letter queue exists, at-most-once dead-letters the same messages, so only the
    # queue's own arguments can show the strategy (ADR 0006 decision 4). RabbitMQ accepts a
    # declaration only if every argument matches the queue's.
    name = broker_settings.jobs_queue_name
    await declare_jobs_queue(await broker_connection.channel(), name)
    channel = await broker_connection.channel()

    # The queue holds exactly this value: a no-op, also if the argument were missing from
    # jobs_queue_arguments...
    await channel.declare_queue(
        name, durable=True, arguments=jobs_queue_arguments(name) | {argument: expected}
    )
    # ...and refused with any other.
    with pytest.raises(ChannelPreconditionFailed, match=argument):
        await channel.declare_queue(
            name, durable=True, arguments=jobs_queue_arguments(name) | {argument: other}
        )


# What reaches the dead-letter queue


async def test_a_message_rejected_without_requeue_is_dead_lettered_unchanged(
    broker_connection: AbstractRobustConnection, broker_settings: BrokerSettings
) -> None:
    name = broker_settings.jobs_queue_name
    await declare_jobs_queue(await broker_connection.channel(), name)
    await publish(broker_connection, name, BODY)

    async with holding(broker_connection, name) as message:
        await message.reject(requeue=False)
    dead = await take_dead_letter(broker_connection, name)

    assert death(dead) == died(name, "rejected")
    assert dead.body == BODY
    assert dead.delivery_mode == DeliveryMode.PERSISTENT
    assert await count(broker_connection, name) == 0


async def test_a_message_requeued_with_reject_is_dead_lettered_past_the_limit(
    broker_connection: AbstractRobustConnection, broker_settings: BrokerSettings
) -> None:
    name = broker_settings.jobs_queue_name
    await declare_jobs_queue(await broker_connection.channel(), name, delivery_limit=LIMIT)
    await publish(broker_connection, name, BODY)

    seen = []
    # LIMIT + 1 deliveries: the return of the last one goes over the limit.
    for _ in range(LIMIT + 1):
        async with holding(broker_connection, name) as message:
            seen.append((message.headers or {}).get("x-delivery-count", 0))
            await message.reject(requeue=True)
    dead = await take_dead_letter(broker_connection, name)

    assert seen == [0, 1, 2]
    assert death(dead) == died(name, "delivery_limit")
    assert await count(broker_connection, name) == 0


async def test_a_message_whose_channel_closes_unanswered_is_dead_lettered_past_the_limit(
    broker_connection: AbstractRobustConnection, broker_settings: BrokerSettings
) -> None:
    # What a worker that dies while holding the message looks like to the broker.
    name = broker_settings.jobs_queue_name
    await declare_jobs_queue(await broker_connection.channel(), name, delivery_limit=LIMIT)
    await publish(broker_connection, name, BODY)

    for _ in range(LIMIT + 1):
        async with holding(broker_connection, name):
            pass
    dead = await take_dead_letter(broker_connection, name)

    assert death(dead) == died(name, "delivery_limit")


async def test_a_nacked_message_is_never_dead_lettered(
    broker_connection: AbstractRobustConnection, broker_settings: BrokerSettings
) -> None:
    # A database outage is answered with nack, which does not count (ADR 0004 decision 7), so an
    # outage of any length keeps the message in the jobs queue.
    name = broker_settings.jobs_queue_name
    await declare_jobs_queue(await broker_connection.channel(), name, delivery_limit=LIMIT)
    await publish(broker_connection, name, BODY)

    for _ in range(3 * (LIMIT + 1)):
        async with holding(broker_connection, name) as message:
            await message.nack(requeue=True)

    assert await count(broker_connection, name) == 1
    assert await count(broker_connection, dead_letter_queue_name(name)) == 0
