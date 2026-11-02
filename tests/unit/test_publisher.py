"""JobPublisher's batch logic with a fake exchange: timeout, mixed outcomes, cancellation.

What the broker does (ack, nack, returned message) is tested against RabbitMQ in
tests/integration/test_publisher.py.
"""

import asyncio
import logging
import time
import uuid

import pytest
from aio_pika import Message
from aiormq import spec

from task_platform.broker.publisher import JobPublisher
from task_platform.domain.messages import parse_job_message
from task_platform.services.dispatch import Publisher

TIMEOUT_S = 0.2
# What publish() returns on a channel with confirms once the broker has taken the message.
ACK = spec.Basic.Ack()


class BrokerRefusedError(Exception):
    pass


class FakeExchange:
    """Acks every job, except those told to fail or to never get an answer.

    `reply` is what an answered publish returns.
    """

    def __init__(
        self,
        *,
        failing: frozenset[uuid.UUID] = frozenset(),
        silent: frozenset[uuid.UUID] = frozenset(),
        reply: object = ACK,
    ) -> None:
        self.failing = failing
        self.silent = silent
        self.reply = reply
        self.calls: list[tuple[uuid.UUID, str, bool]] = []
        self.cancelled: list[uuid.UUID] = []

    async def publish(self, message: Message, routing_key: str, *, mandatory: bool) -> object:
        job_id = parse_job_message(message.body).job_id
        self.calls.append((job_id, routing_key, mandatory))
        if job_id in self.failing:
            raise BrokerRefusedError("message body would be in here")
        if job_id in self.silent:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled.append(job_id)
                raise
        return self.reply


def ids(count: int) -> list[uuid.UUID]:
    return [uuid.uuid4() for _ in range(count)]


def publisher(exchange: FakeExchange) -> JobPublisher:
    return JobPublisher(exchange, queue_name="jobs", timeout_s=TIMEOUT_S)


async def test_every_job_gets_one_mandatory_message_to_the_queue() -> None:
    job_ids = ids(3)
    exchange = FakeExchange()

    assert await publisher(exchange).publish(job_ids) == job_ids
    assert sorted(exchange.calls) == sorted((job_id, "jobs", True) for job_id in job_ids)


def test_job_publisher_is_what_the_dispatch_service_takes() -> None:
    # Checked by mypy: the protocol is structural, so JobPublisher does not name it.
    service_publisher: Publisher = publisher(FakeExchange())
    assert isinstance(service_publisher, JobPublisher)


async def test_no_jobs_means_no_publish() -> None:
    exchange = FakeExchange()

    assert await publisher(exchange).publish([]) == []
    assert exchange.calls == []


async def test_a_repeated_job_is_refused_before_any_publish() -> None:
    # One task per id: a repeated id would leave its first publish running, never cancelled.
    job_id = uuid.uuid4()
    exchange = FakeExchange()

    with pytest.raises(ValueError, match="repeated job id"):
        await publisher(exchange).publish([job_id, uuid.uuid4(), job_id])
    assert exchange.calls == []


@pytest.mark.parametrize(
    "reply",
    [
        # What publish() returns on a channel opened without confirms.
        pytest.param(None, id="no confirms"),
        # On a channel without on_return_raises, a returned message comes back as a value.
        pytest.param(object(), id="returned message"),
    ],
)
async def test_a_publish_answered_without_an_ack_is_not_confirmed(
    reply: object, caplog: pytest.LogCaptureFixture
) -> None:
    # The ack check is the publisher's own, not only the channel options open_job_publisher sets.
    job_id = uuid.uuid4()
    exchange = FakeExchange(reply=reply)

    assert await publisher(exchange).publish([job_id]) == []
    assert f"job {job_id}, task_platform.broker.publisher.PublishNotAckedError" in caplog.text


async def test_only_confirmed_jobs_are_returned_in_input_order(
    caplog: pytest.LogCaptureFixture,
) -> None:
    first, failing, silent, last = ids(4)
    exchange = FakeExchange(failing=frozenset({failing}), silent=frozenset({silent}))

    confirmed = await publisher(exchange).publish([first, failing, silent, last])

    assert confirmed == [first, last]
    assert f"job message not confirmed: job {failing}, tests.unit.test_publisher." in caplog.text
    assert f"job message not confirmed: job {silent}, publish timeout" in caplog.text
    # The error's class, never its text.
    assert "would be in here" not in caplog.text


async def test_one_timeout_bounds_the_whole_batch() -> None:
    job_ids = ids(5)
    exchange = FakeExchange(silent=frozenset(job_ids))

    started = time.monotonic()
    assert await publisher(exchange).publish(job_ids) == []
    elapsed = time.monotonic() - started

    # Concurrent: five silent publishes take one timeout, not five (headroom for a slow runner).
    assert TIMEOUT_S <= elapsed < 3 * TIMEOUT_S
    assert sorted(exchange.cancelled) == sorted(job_ids)


async def test_cancelling_the_caller_cancels_every_publish() -> None:
    # Shutdown (phase 8) cancels the poll; no publish may outlive it unawaited.
    job_ids = ids(2)
    exchange = FakeExchange(silent=frozenset(job_ids))
    call = asyncio.create_task(
        JobPublisher(exchange, queue_name="jobs", timeout_s=60).publish(job_ids)
    )
    async with asyncio.timeout(1):  # a failure, not a hang, if the publishes never start
        while len(exchange.calls) < len(job_ids):
            await asyncio.sleep(0)

    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert sorted(exchange.cancelled) == sorted(job_ids)


def test_warnings_go_to_the_publisher_logger(caplog: pytest.LogCaptureFixture) -> None:
    # The dispatcher's logging setup (S4c) can then find them by name.
    job_id = uuid.uuid4()
    with caplog.at_level(logging.WARNING, logger="task_platform.broker.publisher"):
        asyncio.run(publisher(FakeExchange(failing=frozenset({job_id}))).publish([job_id]))
    assert [record.name for record in caplog.records] == ["task_platform.broker.publisher"]
