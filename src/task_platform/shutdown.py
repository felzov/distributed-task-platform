"""Stopping the dispatcher and the worker on SIGTERM or SIGINT (ADR 0008 decisions 5 and 6).

The first signal asks for a graceful stop: finish what is in hand, start nothing new. The second
asks for an immediate one. Any further signal is ignored.
"""

import asyncio
import contextlib
import enum
import logging
import signal
from collections.abc import Coroutine, Iterator
from contextlib import contextmanager
from typing import Any

from task_platform import safe_logging

logger = logging.getLogger(__name__)

STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


class StopSignals:
    """`stop` is set by the first signal, `force` by the second."""

    def __init__(self) -> None:
        self.stop = asyncio.Event()
        self.force = asyncio.Event()

    def received(self, signum: int) -> None:
        name = signal.Signals(signum).name
        if not self.stop.is_set():
            logger.info("%s received: finishing the work in hand, then stopping", name)
            self.stop.set()
        elif not self.force.is_set():
            logger.warning("%s received again: stopping now", name)
            self.force.set()

    @contextmanager
    def handled(self) -> Iterator[None]:
        """Route SIGTERM and SIGINT to `received` while the block runs.

        Replaces Python's defaults: SIGTERM would end the process at once without cleanup, and
        SIGINT would cancel the main task through `asyncio.run`.
        """
        loop = asyncio.get_running_loop()
        for signum in STOP_SIGNALS:
            loop.add_signal_handler(signum, self.received, signum)
        try:
            yield
        finally:
            for signum in STOP_SIGNALS:
                loop.remove_signal_handler(signum)


async def sleep_unless_stopped(seconds: float, stop: asyncio.Event) -> None:
    """Sleep, unless `stop` is set first: a stop does not wait for the interval."""
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(seconds):
            await stop.wait()


class Ended(enum.Enum):
    """How work run by `finish_unless_forced` ended."""

    FINISHED = "finished"  # by itself; its error, if any, was raised
    CANCELLED = "cancelled"  # on force or timeout, its cleanup clean
    CLEANUP_FAILED = "cleanup failed"  # cancelled, and its cleanup raised (logged)


async def finish_unless_forced(
    work: Coroutine[Any, Any, None], force: asyncio.Event, timeout_s: float | None
) -> Ended:
    """Run `work` to its end, unless `force` is set or `timeout_s` passes first: then cancel it.

    Its error, if it ends by itself with one, is raised. Cancelling the caller cancels `work` too.
    """
    task = asyncio.create_task(work)
    forced = asyncio.create_task(force.wait())
    try:
        done, _ = await asyncio.wait(
            {task, forced}, timeout=timeout_s, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        forced.cancel()
        # Its cleanup (a ROLLBACK, a closed iterator) runs before the caller goes on.
        clean = await cancel_and_wait(task)
    if task in done:
        task.result()
        return Ended.FINISHED
    return Ended.CANCELLED if clean else Ended.CLEANUP_FAILED


async def cancel_and_wait(task: asyncio.Task[Any]) -> bool:
    """Cancel `task` unless it is done, and wait for it. False if its cleanup raised.

    The error is logged by class and SQLSTATE: left unread, asyncio would log it with its whole
    text, which can quote SQL and rows.
    """
    if task.done():
        return True
    task.cancel()
    await asyncio.wait({task})
    if task.cancelled() or (exc := task.exception()) is None:
        return True
    logger.warning(
        "cleanup after a stop failed: %s (sqlstate %s)",
        safe_logging.class_name(exc),
        safe_logging.sqlstate(exc),
    )
    return False
