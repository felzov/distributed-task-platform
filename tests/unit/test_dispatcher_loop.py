"""The dispatcher's loops without a database or broker: a failed poll or sweep is logged and the
loop goes on; a stop ends a loop after its pass; a loop cancelled from inside stops the
dispatcher."""

import asyncio
import logging
import uuid
from collections.abc import Sequence
from itertools import pairwise

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from task_platform.data.db import create_db_engine
from task_platform.dispatcher import process
from task_platform.dispatcher.process import (
    poll_and_recover,
    poll_until_stopped,
    recover_until_stopped,
)
from tests.settings import (
    UNREACHABLE_BROKER_URL,
    UNREACHABLE_DATABASE_URL,
    make_test_broker_settings,
)

# Long enough to measure: a refused connection fails within milliseconds.
INTERVAL_S = 0.05


class NeverCalledPublisher:
    async def publish(self, job_ids: Sequence[uuid.UUID]) -> list[uuid.UUID]:
        raise AssertionError("no job can be due without a database")


async def test_the_loop_keeps_polling_while_the_database_is_down(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = make_test_broker_settings(
        UNREACHABLE_DATABASE_URL, UNREACHABLE_BROKER_URL, dispatch_poll_interval_s=INTERVAL_S
    )
    engine = create_db_engine(settings)
    loop = asyncio.create_task(
        poll_until_stopped(
            async_sessionmaker(engine), NeverCalledPublisher(), settings, asyncio.Event()
        )
    )
    try:
        async with asyncio.timeout(5):
            while sum("dispatch poll failed" in r.getMessage() for r in caplog.records) < 3:
                await asyncio.sleep(0.01)
        # Three failed polls and still running: no failure ends the loop.
        assert not loop.done()
    finally:
        loop.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop
        await engine.dispose()

    failures = [r for r in caplog.records if "dispatch poll failed" in r.getMessage()]
    # A closed port: refused at once. The class only, as for the API's database errors.
    assert failures[0].getMessage() == (
        "dispatch poll failed: builtins.ConnectionRefusedError (sqlstate None)"
    )
    assert {r.levelno for r in failures} == {logging.WARNING}
    assert {r.name for r in failures} == {"task_platform.dispatcher.process"}
    # The interval between polls holds while every poll fails: no tight loop against a database
    # that is down. A lower bound only, so a slow machine cannot break it.
    gaps = [later.created - earlier.created for earlier, later in pairwise(failures)]
    assert min(gaps) >= INTERVAL_S * 0.9


class HangingCount:
    """The ready count as a broker outage leaves it: the read waits for the connection."""

    async def ready_messages(self) -> int:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


async def test_the_recovery_loop_keeps_sweeping_while_the_database_and_broker_are_down(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = make_test_broker_settings(
        UNREACHABLE_DATABASE_URL,
        UNREACHABLE_BROKER_URL,
        recovery_interval_s=INTERVAL_S,
        publish_timeout_s=0.05,
    )
    engine = create_db_engine(settings)

    def lease_failures() -> list[logging.LogRecord]:
        return [r for r in caplog.records if r.getMessage().startswith("lease recovery failed")]

    loop = asyncio.create_task(
        recover_until_stopped(async_sessionmaker(engine), HangingCount(), settings, asyncio.Event())
    )
    try:
        async with asyncio.timeout(5):
            while len(lease_failures()) < 3:
                await asyncio.sleep(0.01)
        assert not loop.done()
    finally:
        loop.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop
        await engine.dispose()

    # Every pass runs the lease sweep, though the previous pass's count read hung: a broker outage
    # never holds the loop beyond publish_timeout_s (ADR 0008 decision 2).
    failures = lease_failures()
    assert failures[0].getMessage() == (
        "lease recovery failed: builtins.ConnectionRefusedError (sqlstate None)"
    )
    assert {(r.levelno, r.name) for r in failures} == {
        (logging.WARNING, "task_platform.dispatcher.process")
    }
    skipped = [r for r in caplog.records if r.getMessage().startswith("lost-message sweep skipped")]
    assert len(skipped) >= 2
    assert skipped[0].getMessage() == (
        "lost-message sweep skipped: the jobs queue's ready count was not read"
        " (builtins.TimeoutError)"
    )
    gaps = [later.created - earlier.created for earlier, later in pairwise(failures)]
    assert min(gaps) >= INTERVAL_S * 0.9


class EmptyCount:
    async def ready_messages(self) -> int:
        return 0


async def test_a_failed_lease_sweep_does_not_stop_the_lost_message_sweep(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = make_test_broker_settings(
        UNREACHABLE_DATABASE_URL, UNREACHABLE_BROKER_URL, recovery_interval_s=INTERVAL_S
    )
    engine = create_db_engine(settings)
    loop = asyncio.create_task(
        recover_until_stopped(async_sessionmaker(engine), EmptyCount(), settings, asyncio.Event())
    )
    try:
        async with asyncio.timeout(5):
            while "lost-message recovery failed" not in caplog.text:
                await asyncio.sleep(0.01)
    finally:
        loop.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop
        await engine.dispose()

    # This project's lines only: a library's own log line in between must not break the order.
    messages = [r.getMessage() for r in caplog.records if r.name.startswith("task_platform.")]
    assert messages[:2] == [
        "lease recovery failed: builtins.ConnectionRefusedError (sqlstate None)",
        "lost-message recovery failed: builtins.ConnectionRefusedError (sqlstate None)",
    ]


async def test_a_loop_cancelled_from_inside_stops_the_dispatcher(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Otherwise the TaskGroup drops the ended recovery loop without a word and polling goes on:
    # stuck jobs would never be recovered again, with nothing in the logs. Injected at the pass,
    # since the lost-message sweep skips the one known source (aiormq on a stuck connection).
    async def cancelled_from_inside(*_: object) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(process, "recover_once", cancelled_from_inside)
    settings = make_test_broker_settings(
        UNREACHABLE_DATABASE_URL,
        UNREACHABLE_BROKER_URL,
        dispatch_poll_interval_s=INTERVAL_S,
        recovery_interval_s=INTERVAL_S,
    )
    engine = create_db_engine(settings)
    try:
        # Without the guard the poll loop runs on, so this times out.
        async with asyncio.timeout(5):
            with pytest.raises(ExceptionGroup) as stopped:
                await poll_and_recover(
                    async_sessionmaker(engine),
                    NeverCalledPublisher(),
                    EmptyCount(),
                    settings,
                    asyncio.Event(),
                )
    finally:
        await engine.dispose()

    [error] = stopped.value.exceptions
    assert type(error) is RuntimeError
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert [(r.name, r.getMessage()) for r in errors] == [
        (
            "task_platform.dispatcher.process",
            "the recovery loop ended with a cancellation nobody asked for",
        )
    ]


@pytest.mark.parametrize("loop_name", ["poll", "recovery"])
async def test_a_stop_ends_the_loop_after_its_pass_without_waiting_for_the_interval(
    caplog: pytest.LogCaptureFixture, loop_name: str
) -> None:
    # ADR 0008 decision 6: an hour between passes, yet the stop takes effect at once.
    settings = make_test_broker_settings(
        UNREACHABLE_DATABASE_URL,
        UNREACHABLE_BROKER_URL,
        dispatch_poll_interval_s=3600,
        recovery_interval_s=3600,
        publish_timeout_s=0.05,
    )
    engine = create_db_engine(settings)
    stop = asyncio.Event()
    sessions = async_sessionmaker(engine)
    if loop_name == "poll":
        work = poll_until_stopped(sessions, NeverCalledPublisher(), settings, stop)
        first_pass = "dispatch poll failed"
    else:
        work = recover_until_stopped(sessions, EmptyCount(), settings, stop)
        first_pass = "lost-message recovery failed"
    loop = asyncio.create_task(work)
    try:
        async with asyncio.timeout(5):
            while first_pass not in caplog.text:
                await asyncio.sleep(0.01)
        stop.set()
        async with asyncio.timeout(1):
            await loop
    finally:
        loop.cancel()
        await asyncio.wait({loop})
        await engine.dispose()

    # One pass only: the stop came during the pause, and no pass started after it.
    assert caplog.text.count(first_pass) == 1
