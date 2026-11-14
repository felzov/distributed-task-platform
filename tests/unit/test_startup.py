"""Start-up without a reachable database or broker (ADR 0008 decision 7): what is retried, what
stops the process, and how a stop signal ends the wait."""

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from itertools import pairwise

import pytest
from aiormq.exceptions import AuthenticationError

from task_platform import startup
from task_platform.config import BrokerSettings
from task_platform.data.db import create_db_engine
from task_platform.startup import (
    StartupError,
    wait_for_broker,
    wait_for_dependencies,
    wait_for_schema,
)
from tests.settings import (
    UNREACHABLE_BROKER_URL,
    UNREACHABLE_DATABASE_URL,
    make_test_broker_settings,
)

PASSWORD = "s3cr3t-sentinel-5512"


def settings_for(database_url: str, broker_url: str, **overrides: object) -> BrokerSettings:
    return make_test_broker_settings(
        database_url, broker_url, startup_retry_interval_s=0.05, **overrides
    )


async def wait_for_warnings(caplog: pytest.LogCaptureFixture, text: str, count: int) -> None:
    async with asyncio.timeout(5):
        while sum(text in r.getMessage() for r in caplog.records) < count:
            await asyncio.sleep(0.01)


@pytest.fixture(autouse=True)
def _warnings(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="task_platform")


def assert_paced(caplog: pytest.LogCaptureFixture, text: str, interval: float) -> None:
    """The WARNINGs naming `text` came at least `interval` apart: no hot loop."""
    times = [r.created for r in caplog.records if text in r.getMessage()]
    gaps = [b - a for a, b in pairwise(times)]
    assert gaps
    assert min(gaps) >= interval - 0.005, gaps


async def test_an_unreachable_database_is_retried_until_a_stop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    url = UNREACHABLE_DATABASE_URL.replace(":unused@", f":{PASSWORD}@")
    settings = settings_for(url, UNREACHABLE_BROKER_URL)
    engine = create_db_engine(settings)
    stop = asyncio.Event()
    try:
        waiting = asyncio.create_task(wait_for_schema(engine, settings, stop))
        await wait_for_warnings(caplog, "database not available at start", 3)
        stop.set()
        async with asyncio.timeout(1):
            assert await waiting is False
    finally:
        await engine.dispose()
    assert "builtins.ConnectionRefusedError" in caplog.text
    assert_paced(caplog, "database not available at start", 0.05)
    assert PASSWORD not in caplog.text


async def test_an_unreachable_broker_is_retried_until_a_stop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = settings_for(UNREACHABLE_DATABASE_URL, UNREACHABLE_BROKER_URL)
    stop = asyncio.Event()
    waiting = asyncio.create_task(wait_for_broker(settings, stop))
    await wait_for_warnings(caplog, "broker not available at start", 3)
    stop.set()
    async with asyncio.timeout(1):
        assert await waiting is None
    assert "aiormq.exceptions.AMQPConnectionError" in caplog.text
    assert_paced(caplog, "broker not available at start", 0.05)


async def test_the_broker_is_not_tried_before_the_schema_check_passes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A worker must not consume from a database whose schema it does not know.
    settings = settings_for(UNREACHABLE_DATABASE_URL, UNREACHABLE_BROKER_URL)
    engine = create_db_engine(settings)
    stop = asyncio.Event()
    try:
        waiting = asyncio.create_task(wait_for_dependencies(engine, settings, stop))
        await wait_for_warnings(caplog, "database not available at start", 2)
        stop.set()
        async with asyncio.timeout(1):
            assert await waiting is None
    finally:
        await engine.dispose()
    assert "broker" not in caplog.text


async def test_an_authentication_mechanism_the_broker_does_not_offer_stops(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    # An OSError subclass, like the outages that are retried.
    async def refused(settings: BrokerSettings) -> object:
        raise AuthenticationError(PASSWORD)

    monkeypatch.setattr(startup, "connect_broker", refused)
    settings = settings_for(UNREACHABLE_DATABASE_URL, UNREACHABLE_BROKER_URL)
    with pytest.raises(StartupError):
        async with asyncio.timeout(1):
            await wait_for_broker(settings, asyncio.Event())
    assert "could not connect to the broker at start: aiormq.exceptions.AuthenticationError" in (
        caplog.text
    )
    assert "retrying" not in caplog.text
    assert PASSWORD not in caplog.text


@pytest.fixture
async def silent_broker_url() -> AsyncIterator[str]:
    """A server that accepts and never answers, as a broker that stopped responding."""

    async def accept_and_stay_silent(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await reader.read()
        writer.close()

    server = await asyncio.start_server(accept_and_stay_silent, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"amqp://task_platform:{PASSWORD}@127.0.0.1:{port}/"
    finally:
        server.close()
        await server.wait_closed()


async def test_a_stop_lets_the_connection_attempt_in_progress_run_to_its_timeout(
    silent_broker_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Cancelling aio-pika's first connect would leave its reconnect task running (ADR 0008).
    settings = settings_for(
        UNREACHABLE_DATABASE_URL, silent_broker_url, broker_connect_timeout_s=0.3
    )
    stop = asyncio.Event()
    start = time.monotonic()
    waiting = asyncio.create_task(wait_for_broker(settings, stop))
    await asyncio.sleep(0.05)
    stop.set()
    async with asyncio.timeout(2):
        assert await waiting is None
    assert time.monotonic() - start >= 0.3
    # The attempt timed out (retryable), and no attempt followed the stop.
    assert caplog.text.count("broker not available at start: builtins.TimeoutError") == 1
    others = asyncio.all_tasks() - {asyncio.current_task()}
    assert not others, others
    assert PASSWORD not in caplog.text


async def test_a_malformed_broker_url_stops_without_a_retry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    url = f"amqp://task_platform:{PASSWORD}@127.0.0.1:not-a-port/"
    settings = settings_for(UNREACHABLE_DATABASE_URL, url)
    with pytest.raises(StartupError):
        async with asyncio.timeout(5):
            await wait_for_broker(settings, asyncio.Event())
    assert "could not connect to the broker at start: builtins.ValueError" in caplog.text
    assert "retrying" not in caplog.text
    assert PASSWORD not in caplog.text


async def test_a_stop_before_start_up_tries_nothing(caplog: pytest.LogCaptureFixture) -> None:
    settings = settings_for(UNREACHABLE_DATABASE_URL, UNREACHABLE_BROKER_URL)
    engine = create_db_engine(settings)
    stop = asyncio.Event()
    stop.set()
    try:
        assert await wait_for_dependencies(engine, settings, stop) is None
    finally:
        await engine.dispose()
    assert caplog.records == []
