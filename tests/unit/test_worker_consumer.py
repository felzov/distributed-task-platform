"""The worker's consumer without a broker: an iterator that ends by itself stops the worker, and a
stop's races with the job in hand and with the consumer's cancel (ADR 0008 decision 5)."""

import asyncio
import gc
import logging
import signal
from types import TracebackType
from typing import Any, cast

import pytest
from aio_pika.abc import AbstractQueue
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform.config import BrokerSettings
from task_platform.shutdown import StopSignals
from task_platform.worker import process
from task_platform.worker.process import consume_until_stopped
from tests.settings import (
    UNREACHABLE_BROKER_URL,
    UNREACHABLE_DATABASE_URL,
    make_test_broker_settings,
)

SENTINEL = "s3cr3t-sentinel-7731"


class EndedIterator:
    """A queue iterator the broker or aio-pika ended, with no stop requested."""

    async def __aenter__(self) -> "EndedIterator":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def __aiter__(self) -> "EndedIterator":
        return self

    async def __anext__(self) -> Any:
        raise StopAsyncIteration


class EndedQueue:
    def iterator(self) -> EndedIterator:
        return EndedIterator()


async def test_an_iterator_that_ends_without_a_stop_stops_the_worker() -> None:
    # Instead of idling with no consumer, until it consumes again (ADR 0008 decision 8).
    settings = make_test_broker_settings(UNREACHABLE_DATABASE_URL, UNREACHABLE_BROKER_URL)
    sessions = cast(async_sessionmaker[AsyncSession], None)

    with pytest.raises(RuntimeError, match="the jobs queue iterator ended"):
        await consume_until_stopped(
            cast(AbstractQueue, EndedQueue()), sessions, {}, settings, StopSignals()
        )


class FakeIterator:
    """Messages the test puts in; `close` is the broker-side cancel, as slow or as broken as the
    test makes it. Once closed, a waiting `__anext__` ends, as aio-pika's does (observed)."""

    def __init__(self, *, close_delay_s: float = 0.0, close_error: bool = False) -> None:
        self.messages: asyncio.Queue[str] = asyncio.Queue()
        self.closed = asyncio.Event()
        self.close_entered = asyncio.Event()
        self.close_delay_s = close_delay_s
        self.close_error = close_error

    async def __aenter__(self) -> "FakeIterator":
        return self

    async def __aexit__(self, *_: object) -> None:
        self.closed.set()

    def __aiter__(self) -> "FakeIterator":
        return self

    async def __anext__(self) -> str:
        message = asyncio.ensure_future(self.messages.get())
        closed = asyncio.ensure_future(self.closed.wait())
        try:
            await asyncio.wait({message, closed}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            closed.cancel()
            if not message.done():
                message.cancel()
        if message.done() and not message.cancelled():
            return message.result()
        raise StopAsyncIteration

    async def close(self) -> None:
        self.close_entered.set()
        await asyncio.sleep(self.close_delay_s)
        if self.close_error:
            raise ConnectionError("cancel failed")
        self.closed.set()


class FakeQueue:
    def __init__(self, iterator: FakeIterator) -> None:
        self._iterator = iterator

    def iterator(self) -> FakeIterator:
        return self._iterator


def handled_messages(monkeypatch: pytest.MonkeyPatch, job_s: float) -> list[str]:
    """Replace the job with one of `job_s` seconds; returns the messages it ran."""
    handled: list[str] = []

    async def handle_message(message: str, *_: object) -> None:
        handled.append(message)
        await asyncio.sleep(job_s)

    monkeypatch.setattr(process, "handle_message", handle_message)
    return handled


def grace(seconds: float) -> BrokerSettings:
    return make_test_broker_settings(
        UNREACHABLE_DATABASE_URL, UNREACHABLE_BROKER_URL, shutdown_grace_s=seconds
    )


def start(iterator: FakeIterator, settings: BrokerSettings, signals: StopSignals) -> Any:
    queue = cast(AbstractQueue, FakeQueue(iterator))
    sessions = cast(async_sessionmaker[AsyncSession], None)
    return asyncio.create_task(consume_until_stopped(queue, sessions, {}, settings, signals))


async def test_a_job_ending_as_the_stop_comes_takes_no_message_delivered_meanwhile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The job's ack reaches the broker before the cancel does, so the broker delivers the next
    # message: the worker must not run it.
    handled = handled_messages(monkeypatch, job_s=0)
    iterator = FakeIterator(close_delay_s=0.2)
    signals = StopSignals()
    worker = start(iterator, grace(1.0), signals)
    await iterator.messages.put("first")
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    signals.received(signal.SIGTERM)
    await asyncio.sleep(0.01)
    await iterator.messages.put("second")

    async with asyncio.timeout(5):
        await worker
    assert handled == ["first"]


async def test_a_slow_cancel_does_not_hold_up_a_job_that_finished(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # A cancel that hangs (a link that silently stopped passing traffic): the job finishes in
    # 0.1 s, so the stop does not wait out the grace period nor report the job as unfinished.
    caplog.set_level(logging.INFO)
    handled_messages(monkeypatch, job_s=0.1)
    iterator = FakeIterator(close_delay_s=1000)
    signals = StopSignals()
    worker = start(iterator, grace(5.0), signals)
    await iterator.messages.put("first")
    async with asyncio.timeout(1):
        while not iterator.messages.empty():
            await asyncio.sleep(0.01)
    signals.received(signal.SIGTERM)

    async with asyncio.timeout(2):
        await worker
    assert "worker stopped after the job in hand finished" in caplog.text
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


async def test_a_failed_cancel_still_lets_the_job_finish(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    handled = handled_messages(monkeypatch, job_s=0.2)
    iterator = FakeIterator(close_error=True)
    signals = StopSignals()
    worker = start(iterator, grace(5.0), signals)
    await iterator.messages.put("first")
    async with asyncio.timeout(1):
        while not iterator.messages.empty():
            await asyncio.sleep(0.01)
    signals.received(signal.SIGTERM)

    async with asyncio.timeout(2):
        await worker
    assert handled == ["first"]
    assert "could not cancel the consumer at shutdown: builtins.ConnectionError" in caplog.text
    assert "worker stopped after the job in hand finished" in caplog.text


class FailingCleanupIterator(FakeIterator):
    async def __aexit__(self, *_: object) -> None:
        raise OSError(f"cleanup failed quoting {SENTINEL}")


async def test_a_forced_stop_whose_cleanup_fails_logs_the_class_only(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Never retrieved, the error would reach asyncio's handler with its whole text.
    caplog.set_level(logging.INFO)
    handled_messages(monkeypatch, job_s=1000)
    iterator = FailingCleanupIterator()
    signals = StopSignals()
    worker = start(iterator, grace(5.0), signals)
    await iterator.messages.put("first")
    async with asyncio.timeout(1):
        while not iterator.messages.empty():
            await asyncio.sleep(0.01)
    signals.received(signal.SIGTERM)
    signals.received(signal.SIGINT)

    async with asyncio.timeout(2):
        await worker
    del worker
    gc.collect()
    await asyncio.sleep(0)

    assert "cleanup after a stop failed: builtins.OSError (sqlstate None)" in caplog.text
    assert SENTINEL not in caplog.text
    assert "never retrieved" not in caplog.text
