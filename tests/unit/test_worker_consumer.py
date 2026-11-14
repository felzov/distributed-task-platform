"""The worker's consumer without a broker: an iterator that ends by itself is replaced by a new
consumer (ADR 0008 decision 8), and a stop's races with the job in hand and with the consumer's
cancel (ADR 0008 decision 5)."""

import asyncio
import gc
import logging
import signal
from collections.abc import Awaitable, Callable
from itertools import pairwise
from types import TracebackType
from typing import Any, cast

import pytest
from aio_pika.abc import AbstractConnection, AbstractQueue
from aiormq.exceptions import ChannelPreconditionFailed
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform.config import BrokerSettings
from task_platform.shutdown import StopSignals
from task_platform.worker import process
from task_platform.worker.process import OpenQueue, consume_until_stopped, open_jobs_queue
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


class FailingIterator(EndedIterator):
    """A consumer the broker failed: its basic.consume (`__aenter__`) or a delivery raises."""

    def __init__(self, error: BaseException, *, on_enter: bool) -> None:
        self.error = error
        self.on_enter = on_enter

    async def __aenter__(self) -> "FailingIterator":
        if self.on_enter:
            raise self.error
        return self

    async def __anext__(self) -> Any:
        raise self.error


class FakeChannel:
    def __init__(self, close_error: BaseException | None = None, close_s: float = 0.0) -> None:
        self.closed = False
        self.close_error = close_error
        self.close_s = close_s

    async def close(self) -> None:
        await asyncio.sleep(self.close_s)
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


class FakeQueue:
    def __init__(
        self, iterator: FakeIterator | EndedIterator, channel: FakeChannel | None = None
    ) -> None:
        self._iterator = iterator
        self.channel = channel or FakeChannel()

    def iterator(self) -> FakeIterator | EndedIterator:
        return self._iterator


async def never_reopened() -> AbstractQueue:
    raise AssertionError("the queue was opened again")


def handled_messages(monkeypatch: pytest.MonkeyPatch, job_s: float) -> list[str]:
    """Replace the job with one of `job_s` seconds; returns the messages it ran."""
    handled: list[str] = []

    async def handle_message(message: str, *_: object) -> None:
        handled.append(message)
        await asyncio.sleep(job_s)

    monkeypatch.setattr(process, "handle_message", handle_message)
    return handled


def grace(seconds: float, **overrides: object) -> BrokerSettings:
    return make_test_broker_settings(
        UNREACHABLE_DATABASE_URL, UNREACHABLE_BROKER_URL, shutdown_grace_s=seconds, **overrides
    )


def start(
    iterator: FakeIterator | EndedIterator,
    settings: BrokerSettings,
    signals: StopSignals,
    reopen: Callable[[], Awaitable[AbstractQueue]] = never_reopened,
) -> Any:
    return start_on(FakeQueue(iterator), settings, signals, reopen)


def start_on(
    queue: FakeQueue,
    settings: BrokerSettings,
    signals: StopSignals,
    reopen: Callable[[], Awaitable[AbstractQueue]],
) -> Any:
    sessions = cast(async_sessionmaker[AsyncSession], None)
    return asyncio.create_task(
        consume_until_stopped(cast(AbstractQueue, queue), reopen, sessions, {}, settings, signals)
    )


# An iterator that ends without a stop


async def test_an_iterator_that_ends_without_a_stop_is_replaced_on_a_new_channel(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    handled = handled_messages(monkeypatch, job_s=0)
    old = FakeQueue(EndedIterator())
    new_iterator = FakeIterator()
    opened: list[FakeQueue] = []

    async def reopen() -> AbstractQueue:
        # The old channel is closed first: aio-pika would restore its consumer with it.
        assert old.channel.closed
        opened.append(FakeQueue(new_iterator))
        return cast(AbstractQueue, opened[-1])

    signals = StopSignals()
    worker = start_on(old, grace(1.0, startup_retry_interval_s=0.01), signals, reopen)
    await new_iterator.messages.put("after")
    async with asyncio.timeout(1):
        while handled != ["after"]:
            await asyncio.sleep(0.01)

    signals.received(signal.SIGTERM)
    async with asyncio.timeout(1):
        await worker
    assert len(opened) == 1
    assert "the jobs queue consumer ended; consuming again in 0.01 s" in caplog.text


async def test_a_jobs_queue_that_cannot_be_opened_again_is_retried(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    handled = handled_messages(monkeypatch, job_s=0)
    new_iterator = FakeIterator()
    attempts = 0

    async def reopen() -> AbstractQueue:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError(f"broker gone {SENTINEL}")
        return cast(AbstractQueue, FakeQueue(new_iterator))

    signals = StopSignals()
    worker = start(EndedIterator(), grace(1.0, startup_retry_interval_s=0.01), signals, reopen)
    await new_iterator.messages.put("after")
    async with asyncio.timeout(1):
        while handled != ["after"]:
            await asyncio.sleep(0.01)

    signals.received(signal.SIGTERM)
    async with asyncio.timeout(1):
        await worker
    assert attempts == 3
    assert (
        caplog.text.count(
            "could not open the jobs queue again: builtins.ConnectionError; retrying in 0.01 s"
        )
        == 2
    )
    assert SENTINEL not in caplog.text


async def test_a_stop_while_waiting_to_consume_again_stops_at_once() -> None:
    signals = StopSignals()
    worker = start(EndedIterator(), grace(1.0, startup_retry_interval_s=1000), signals)
    await asyncio.sleep(0.05)
    assert not worker.done()

    signals.received(signal.SIGTERM)
    async with asyncio.timeout(1):
        await worker


def reopening(*iterators: FakeIterator | EndedIterator) -> tuple[OpenQueue, list[FakeQueue]]:
    """A reopen that returns a queue with each iterator in turn; the list holds those opened."""
    opened: list[FakeQueue] = []
    pending = list(iterators)

    async def reopen() -> AbstractQueue:
        opened.append(FakeQueue(pending.pop(0)))
        return cast(AbstractQueue, opened[-1])

    return reopen, opened


async def handle_then_stop(
    worker: Any, signals: StopSignals, iterator: FakeIterator, handled: list[str]
) -> None:
    """Deliver one message on `iterator`, wait until it is handled, then stop the worker."""
    await iterator.messages.put("after")
    async with asyncio.timeout(1):
        while handled != ["after"]:
            await asyncio.sleep(0.01)
    signals.received(signal.SIGTERM)
    async with asyncio.timeout(1):
        await worker


@pytest.mark.parametrize(
    ("error", "on_enter", "reason"),
    [
        (ConnectionError(SENTINEL), True, "failed (builtins.ConnectionError)"),
        (ConnectionError(SENTINEL), False, "failed (builtins.ConnectionError)"),
        (asyncio.CancelledError(), False, "failed (a cancellation nobody asked for)"),
    ],
    ids=["basic.consume fails", "a delivery fails", "an unrequested cancellation"],
)
async def test_a_consumer_the_broker_failed_is_replaced(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    error: BaseException,
    on_enter: bool,
    reason: str,
) -> None:
    # During an outage, a basic.consume on a channel closed meanwhile raises, and aiormq fails a
    # pending request with a CancelledError nobody asked for: neither may end the worker.
    handled = handled_messages(monkeypatch, job_s=0)
    new_iterator = FakeIterator()
    reopen, opened = reopening(new_iterator)
    signals = StopSignals()
    failing = FailingIterator(error, on_enter=on_enter)
    worker = start(failing, grace(1.0, startup_retry_interval_s=0.01), signals, reopen)

    await handle_then_stop(worker, signals, new_iterator, handled)
    assert len(opened) == 1
    assert f"the jobs queue consumer {reason}; consuming again in 0.01 s" in caplog.text
    assert SENTINEL not in caplog.text


async def test_a_consumer_is_replaced_as_often_as_it_ends(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    handled = handled_messages(monkeypatch, job_s=0)
    first = FakeQueue(EndedIterator())
    last_iterator = FakeIterator()
    replacements = [FakeQueue(EndedIterator()), FakeQueue(last_iterator)]
    opened: list[FakeQueue] = []

    async def reopen() -> AbstractQueue:
        # Every channel given up on is closed before the next one opens.
        assert all(queue.channel.closed for queue in [first, *opened])
        opened.append(replacements.pop(0))
        return cast(AbstractQueue, opened[-1])

    signals = StopSignals()
    worker = start_on(first, grace(1.0, startup_retry_interval_s=0.01), signals, reopen)

    await handle_then_stop(worker, signals, last_iterator, handled)
    assert len(opened) == 2
    assert caplog.text.count("the jobs queue consumer ended; consuming again") == 2


@pytest.mark.parametrize(
    ("channel", "reason"),
    [
        (FakeChannel(close_error=ConnectionError(SENTINEL)), "builtins.ConnectionError"),
        (FakeChannel(close_s=1000), "builtins.TimeoutError"),
        # aiormq's, on a stuck link, though nothing cancelled the worker.
        (FakeChannel(close_error=asyncio.CancelledError()), "a cancellation nobody asked for"),
    ],
    ids=["close fails", "close not answered", "close fails with a cancellation"],
)
async def test_an_old_channel_that_does_not_close_does_not_stop_the_new_consumer(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    channel: FakeChannel,
    reason: str,
) -> None:
    handled = handled_messages(monkeypatch, job_s=0)
    new_iterator = FakeIterator()
    reopen, opened = reopening(new_iterator)
    signals = StopSignals()
    settings = grace(1.0, startup_retry_interval_s=0.01, broker_connect_timeout_s=0.05)
    worker = start_on(FakeQueue(EndedIterator(), channel), settings, signals, reopen)

    await handle_then_stop(worker, signals, new_iterator, handled)
    assert len(opened) == 1
    assert f"could not close the old jobs queue channel: {reason}" in caplog.text
    assert SENTINEL not in caplog.text


async def test_an_unrequested_cancellation_while_opening_again_is_retried(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    handled = handled_messages(monkeypatch, job_s=0)
    new_iterator = FakeIterator()
    attempts = 0

    async def reopen() -> AbstractQueue:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise asyncio.CancelledError
        return cast(AbstractQueue, FakeQueue(new_iterator))

    signals = StopSignals()
    worker = start(EndedIterator(), grace(1.0, startup_retry_interval_s=0.01), signals, reopen)

    await handle_then_stop(worker, signals, new_iterator, handled)
    assert attempts == 2
    assert (
        "could not open the jobs queue again: a cancellation nobody asked for; retrying in 0.01 s"
    ) in caplog.text


async def test_a_jobs_queue_whose_declaration_is_refused_stops_the_worker(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Its arguments changed (or the user's permissions): no retry fixes that.
    attempts = 0

    async def reopen() -> AbstractQueue:
        nonlocal attempts
        attempts += 1
        raise ChannelPreconditionFailed(SENTINEL)

    signals = StopSignals()
    worker = start(EndedIterator(), grace(1.0, startup_retry_interval_s=0.01), signals, reopen)
    with pytest.raises(ChannelPreconditionFailed):
        async with asyncio.timeout(1):
            await worker
    assert attempts == 1
    assert (
        "the jobs queue cannot be opened again: aiormq.exceptions.ChannelPreconditionFailed;"
        " the worker stops"
    ) in caplog.text
    assert "retrying" not in caplog.text
    assert SENTINEL not in caplog.text


async def test_attempts_to_consume_again_are_paced_by_the_retry_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Without the pause, a broker that keeps failing the consumer would get a hot loop.
    handled = handled_messages(monkeypatch, job_s=0)
    interval = 0.05
    loop = asyncio.get_running_loop()
    new_iterator = FakeIterator()
    tries: list[float] = []

    async def reopen() -> AbstractQueue:
        tries.append(loop.time())
        if len(tries) < 3:
            raise ConnectionError("broker gone")
        return cast(AbstractQueue, FakeQueue(new_iterator))

    signals = StopSignals()
    started = loop.time()
    worker = start(EndedIterator(), grace(1.0, startup_retry_interval_s=interval), signals, reopen)

    await handle_then_stop(worker, signals, new_iterator, handled)
    gaps = [b - a for a, b in pairwise([started, *tries])]
    assert len(gaps) == 3
    assert min(gaps) >= interval - 0.005, gaps


class DeclareRefusedChannel:
    def __init__(self) -> None:
        self.closed = False

    async def set_qos(self, prefetch_count: int) -> None:
        return None

    async def declare_queue(self, *_: object, **__: object) -> None:
        raise ChannelPreconditionFailed(SENTINEL)

    async def close(self) -> None:
        self.closed = True


class OneChannelConnection:
    def __init__(self, channel: DeclareRefusedChannel) -> None:
        self._channel = channel

    async def channel(self) -> DeclareRefusedChannel:
        return self._channel


async def test_opening_the_jobs_queue_closes_its_channel_when_the_declaration_fails() -> None:
    # A robust channel the broker closed reopens itself: left open, each failed attempt to
    # consume again would add one more.
    channel = DeclareRefusedChannel()
    connection = cast(AbstractConnection, OneChannelConnection(channel))
    with pytest.raises(ChannelPreconditionFailed):
        await open_jobs_queue(connection, grace(1.0))
    assert channel.closed


# A stop with a job in hand


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
