"""Stop signals and the bounded wait both processes stop with (ADR 0008 decisions 5 and 6)."""

import asyncio
import gc
import logging
import os
import signal

import pytest

from task_platform.shutdown import Ended, StopSignals, finish_unless_forced


def test_the_first_signal_asks_to_stop_the_second_to_stop_now_and_more_are_ignored(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    signals = StopSignals()

    signals.received(signal.SIGTERM)
    assert (signals.stop.is_set(), signals.force.is_set()) == (True, False)
    signals.received(signal.SIGINT)
    assert (signals.stop.is_set(), signals.force.is_set()) == (True, True)
    signals.received(signal.SIGTERM)
    # Ignored: nothing is undone either.
    assert (signals.stop.is_set(), signals.force.is_set()) == (True, True)

    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.INFO, "SIGTERM received: finishing the work in hand, then stopping"),
        (logging.WARNING, "SIGINT received again: stopping now"),
    ]


async def test_real_signals_reach_it_while_handled_and_the_defaults_come_back() -> None:
    signals = StopSignals()
    with signals.handled():
        os.kill(os.getpid(), signal.SIGTERM)
        async with asyncio.timeout(5):
            await signals.stop.wait()
        os.kill(os.getpid(), signal.SIGINT)
        async with asyncio.timeout(5):
            await signals.force.wait()

    assert signal.getsignal(signal.SIGTERM) == signal.SIG_DFL
    assert signal.getsignal(signal.SIGINT) == signal.default_int_handler


class Work:
    """Work that ends when released, and records whether it was cancelled and cleaned up."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.cancelled = False
        self.cleaned_up = False

    async def run(self) -> None:
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        finally:
            self.cleaned_up = True


async def test_work_that_ends_in_time_is_not_cancelled() -> None:
    work = Work()
    asyncio.get_running_loop().call_later(0.05, work.release.set)

    ended = await finish_unless_forced(work.run(), asyncio.Event(), timeout_s=5)
    assert (ended, work.cancelled) == (Ended.FINISHED, False)


async def test_its_error_is_raised() -> None:
    async def fails() -> None:
        raise RuntimeError("a bug")

    with pytest.raises(RuntimeError, match="a bug"):
        await finish_unless_forced(fails(), asyncio.Event(), timeout_s=5)


async def test_work_past_the_timeout_is_cancelled_and_cleaned_up_before_it_returns() -> None:
    work = Work()

    ended = await finish_unless_forced(work.run(), asyncio.Event(), timeout_s=0.05)
    assert (ended, work.cancelled, work.cleaned_up) == (Ended.CANCELLED, True, True)


async def test_force_cancels_the_work_without_waiting_for_the_timeout() -> None:
    work = Work()
    force = asyncio.Event()
    asyncio.get_running_loop().call_later(0.05, force.set)

    async with asyncio.timeout(5):
        ended = await finish_unless_forced(work.run(), force, timeout_s=None)
    assert (ended, work.cancelled) == (Ended.CANCELLED, True)


async def test_cancelling_the_caller_cancels_the_work() -> None:
    work = Work()
    caller = asyncio.create_task(finish_unless_forced(work.run(), asyncio.Event(), timeout_s=None))
    await asyncio.sleep(0.05)
    caller.cancel()

    with pytest.raises(asyncio.CancelledError):
        await caller
    assert (work.cancelled, work.cleaned_up) == (True, True)


async def test_work_whose_cleanup_fails_is_reported_by_class_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A ROLLBACK that fails while a poll is cancelled, say: its text can quote SQL and rows.
    async def cleanup_fails() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            raise OSError("rollback failed: SELECT payload FROM jobs WHERE ... s3cr3t")

    ended = await finish_unless_forced(cleanup_fails(), asyncio.Event(), timeout_s=0.05)
    gc.collect()
    await asyncio.sleep(0)

    assert ended is Ended.CLEANUP_FAILED
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.WARNING, "cleanup after a stop failed: builtins.OSError (sqlstate None)")
    ]
