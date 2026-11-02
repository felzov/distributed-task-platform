"""The dispatcher's poll loop without a database: a failed poll is logged and the loop goes on."""

import asyncio
import logging
import uuid
from collections.abc import Sequence
from itertools import pairwise

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from task_platform.data.db import create_db_engine
from task_platform.dispatcher.process import poll_forever
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
        poll_forever(async_sessionmaker(engine), NeverCalledPublisher(), settings)
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
