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
from typing import Any, Final

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
        # Its cleanup (a ROLLBACK, a closed iterator) runs before the caller goes on, unless the
        # second signal cuts it short too.
        clean = await cancel_and_wait(task, force)
    if task in done:
        task.result()
        return Ended.FINISHED
    return Ended.CANCELLED if clean else Ended.CLEANUP_FAILED


# Once a stop is forced, how often a task that has not ended is cancelled again. A cleanup can
# take one cancel and then wait on a link that stopped passing traffic (aio-pika's consumer
# cancel does); each further cancel interrupts that wait. A healthy cleanup, a ROLLBACK or a
# consumer cancel answered by the broker, ends well within it.
RECANCEL_INTERVAL_S: Final = 0.1


async def cancel_and_wait(task: asyncio.Task[Any], force: asyncio.Event | None = None) -> bool:
    """Cancel `task` unless it is done, and wait for it. False if its cleanup raised; True if it
    was done already (its result is then its owner's to read).

    With `force`: once it is set, before or while the cleanup runs, the task is cancelled again
    every RECANCEL_INTERVAL_S until it ends (ADR 0008 decisions 5 and 6).

    The error is logged by class and SQLSTATE: left unread, asyncio would log it with its whole
    text, which can quote SQL and rows.
    """
    if task.done():
        return True
    task.cancel()
    if force is not None:
        if not force.is_set():
            forced = asyncio.ensure_future(force.wait())
            try:
                await asyncio.wait({task, forced}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                forced.cancel()
        while not task.done():
            await asyncio.wait({task}, timeout=RECANCEL_INTERVAL_S)
            if not task.done():
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


async def clean_up_within(
    what: str, cleanup: Coroutine[Any, Any, None], timeout_s: float, force: asyncio.Event
) -> None:
    """Run a cleanup step, such as closing a connection, for up to `timeout_s`; a second signal
    that comes while it runs ends it at once (ADR 0008 decisions 5 and 6). `what` names the step
    in the WARNING logged when it is cut short or fails.

    Without the bound, a close on a link that stopped passing traffic waits for the libraries'
    own timeouts: 2 s for aio-pika's connection close, about 10 s per pooled connection for the
    database pool's dispose (observed). Cut short, the step is cancelled: the server notices the
    connection gone by itself, and gives back the messages it held unacknowledged.

    Its own error is logged by class and SQLSTATE, not raised: every caller runs it in a
    `finally`, where raising would replace the error that led there (a StartupError's exit
    status 1 among them).
    """
    task = asyncio.create_task(cleanup)
    # Set already: it cut the work short, not this step, which still gets its time.
    forced = None if force.is_set() else asyncio.ensure_future(force.wait())
    waits: set[asyncio.Future[Any]] = {task} if forced is None else {task, forced}
    try:
        done, _ = await asyncio.wait(waits, timeout=timeout_s, return_when=asyncio.FIRST_COMPLETED)
    finally:
        if forced is not None:
            forced.cancel()
        await cancel_and_wait(task, force)
    if task in done:
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.warning(
                "%s failed: %s (sqlstate %s)",
                what,
                safe_logging.class_name(exc),
                safe_logging.sqlstate(exc),
            )
    elif forced is not None and forced in done:
        logger.warning("%s cut short by the second signal", what)
    else:
        logger.warning("%s did not end within %g s; cut short", what, timeout_s)
