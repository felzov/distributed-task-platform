"""The first broker connection fails fast and within its timeout, before any broker is needed."""

import asyncio
import time

import pytest
from aio_pika.exceptions import AMQPConnectionError

from task_platform.broker.connection import connect_broker
from tests.settings import (
    UNREACHABLE_BROKER_URL,
    UNREACHABLE_DATABASE_URL,
    make_test_broker_settings,
)

PASSWORD = "s3cr3t-sentinel-7731"


async def test_unreachable_broker_fails_at_once() -> None:
    settings = make_test_broker_settings(UNREACHABLE_DATABASE_URL, UNREACHABLE_BROKER_URL)
    start = time.monotonic()
    # Not retried: connect_robust only reconnects a connection that was established once.
    with pytest.raises(AMQPConnectionError):
        await connect_broker(settings)
    assert time.monotonic() - start < settings.broker_connect_timeout_s


async def test_broker_that_never_answers_fails_after_the_connect_timeout(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("DEBUG")

    async def accept_and_stay_silent(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        # Reads the client's AMQP protocol header and never answers, until the client hangs up.
        await reader.read()
        # Close our end too: since Python 3.12, wait_closed() waits for every connection.
        writer.close()

    server = await asyncio.start_server(accept_and_stay_silent, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    settings = make_test_broker_settings(
        UNREACHABLE_DATABASE_URL,
        f"amqp://task_platform:{PASSWORD}@127.0.0.1:{port}/",
        broker_connect_timeout_s=0.3,
    )
    start = time.monotonic()
    try:
        # Without the connect timeout this test hangs instead of failing: aio-pika does not let
        # an outer asyncio.timeout cancel the first connect (see connect_broker). CI's job
        # timeout is the backstop for that.
        with pytest.raises(TimeoutError) as exc_info:
            await connect_broker(settings)
    finally:
        server.close()
        await server.wait_closed()
    elapsed = time.monotonic() - start
    assert 0.3 <= elapsed < 2.0
    assert PASSWORD not in repr(exc_info.value)
    assert PASSWORD not in caplog.text
