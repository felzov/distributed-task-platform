"""Stop signals and the bounded wait both processes stop with (ADR 0008 decisions 5 and 6)."""

import asyncio
import gc
import logging
import os
import signal

import pytest

from task_platform.shutdown import (
    Ended,
    StopSignals,
    cancel_and_wait,
    clean_up_within,
    finish_unless_forced,
    sleep_unless_stopped,
)

SENTINEL = "s3cr3t-sentinel-4401"


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


# Cleanup a second signal cuts short


async def stuck_cleanup(cleanups: list[str]) -> None:
    """Work whose cleanup waits on an answer that never comes, as aio-pika's consumer cancel on a
    link that stopped passing traffic."""
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        cleanups.append("started")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleanups.append("cut short")
            raise


async def test_a_second_signal_cancels_a_stuck_cleanup_once_more() -> None:
    cleanups: list[str] = []
    task = asyncio.create_task(stuck_cleanup(cleanups))
    await asyncio.sleep(0)
    force = asyncio.Event()
    waiting = asyncio.create_task(cancel_and_wait(task, force))
    await asyncio.sleep(0.05)
    # Without the signal it would wait forever.
    assert not waiting.done()
    assert cleanups == ["started"]

    force.set()
    async with asyncio.timeout(1):
        assert await waiting is True
    assert cleanups == ["started", "cut short"]


async def test_a_cleanup_step_that_ends_in_time_runs_to_its_end_even_after_a_second_signal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The second signal cut the work short, not the steps after it: a healthy close still closes.
    force = asyncio.Event()
    force.set()
    closed = asyncio.Event()

    async def close() -> None:
        await asyncio.sleep(0.05)
        closed.set()

    await clean_up_within("closing", close(), timeout_s=1, force=force)
    assert closed.is_set()
    assert caplog.records == []


async def test_a_cleanup_step_past_its_bound_is_cancelled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A close on a link that stopped passing traffic: one cancel ends it (observed for aio-pika's
    # connection close and SQLAlchemy's dispose).
    cancelled = asyncio.Event()

    async def close() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    loop = asyncio.get_running_loop()
    start = loop.time()
    await clean_up_within("closing", close(), timeout_s=0.1, force=asyncio.Event())
    assert 0.1 <= loop.time() - start < 0.5
    assert cancelled.is_set()
    assert "closing did not end within 0.1 s; cut short" in caplog.text


async def test_a_second_signal_while_a_cleanup_step_runs_ends_it_at_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    force = asyncio.Event()
    cancelled = asyncio.Event()

    async def close() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    stepping = asyncio.create_task(clean_up_within("closing", close(), timeout_s=100, force=force))
    await asyncio.sleep(0.05)
    force.set()
    async with asyncio.timeout(1):
        await stepping
    assert cancelled.is_set()
    assert "closing cut short by the second signal" in caplog.text


async def test_a_cleanup_steps_own_error_is_logged_by_class_and_the_error_before_it_kept(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Raised from the caller's `finally`, it would replace the error that led there.
    async def close() -> None:
        raise ConnectionError(f"close failed {SENTINEL}")

    class OriginalError(Exception):
        pass

    with pytest.raises(OriginalError):
        try:
            raise OriginalError
        finally:
            await clean_up_within("closing", close(), timeout_s=1, force=asyncio.Event())
    assert "closing failed: builtins.ConnectionError (sqlstate None)" in caplog.text
    assert SENTINEL not in caplog.text


@pytest.mark.parametrize("forced_when", ["before", "during"])
async def test_once_forced_a_cleanup_that_takes_one_cancel_is_cancelled_again(
    forced_when: str,
) -> None:
    # Both signals can land before the cleanup starts (a fast double Ctrl-C), or the second one
    # while it runs: either way the stop does not wait on the link.
    cleanups: list[str] = []
    task = asyncio.create_task(stuck_cleanup(cleanups))
    await asyncio.sleep(0)
    force = asyncio.Event()
    if forced_when == "before":
        force.set()
    waiting = asyncio.create_task(cancel_and_wait(task, force))
    if forced_when == "during":
        await asyncio.sleep(0.05)
        assert not waiting.done()
        force.set()
    async with asyncio.timeout(1):
        assert await waiting is True
    assert cleanups == ["started", "cut short"]


async def test_a_forced_stop_cuts_the_cleanup_of_the_work_it_cancelled() -> None:
    # The second signal itself cancels the work: no later signal is left to cut its cleanup.
    cleanups: list[str] = []
    force = asyncio.Event()
    finishing = asyncio.create_task(finish_unless_forced(stuck_cleanup(cleanups), force, None))
    await asyncio.sleep(0.05)
    force.set()
    async with asyncio.timeout(1):
        assert await finishing is Ended.CANCELLED
    assert cleanups == ["started", "cut short"]


async def test_a_stop_ends_the_sleep_at_once() -> None:
    stop = asyncio.Event()
    sleeping = asyncio.create_task(sleep_unless_stopped(1000, stop))
    await asyncio.sleep(0.01)
    stop.set()
    async with asyncio.timeout(1):
        await sleeping
