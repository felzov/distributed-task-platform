"""One dispatcher poll against the real database, with a fake publisher (ADR 0002 decision 2).

From the service's side a nack, a returned message and a timeout look the same: the job is not
among the confirmed ids. What the broker does is tested in test_publisher.py.
"""

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform.config import BrokerSettings
from task_platform.services.dispatch import DispatchPoll, dispatch_due_jobs
from tests.integration.rows import insert_job
from tests.settings import UNREACHABLE_BROKER_URL, make_test_broker_settings

pytestmark = pytest.mark.integration

PAST = datetime.now(UTC) - timedelta(hours=3)
SENTINEL = "s3cr3t-sentinel-7731"


class FakePublisher:
    """Confirms every job except `unconfirmed`, plus `extra`; records each batch it is given.

    `during` runs while the batch is being published, that is while its rows are locked.
    """

    def __init__(
        self,
        *,
        unconfirmed: frozenset[uuid.UUID] = frozenset(),
        extra: Sequence[uuid.UUID] = (),
        during: Callable[[], Awaitable[object]] | None = None,
    ) -> None:
        self.unconfirmed = unconfirmed
        self.extra = extra
        self.during = during
        self.batches: list[list[uuid.UUID]] = []
        self.started = asyncio.Event()

    async def publish(self, job_ids: Sequence[uuid.UUID]) -> list[uuid.UUID]:
        self.batches.append(list(job_ids))
        self.started.set()
        if self.during is not None:
            await self.during()
        return [job_id for job_id in job_ids if job_id not in self.unconfirmed] + list(self.extra)


@pytest.fixture
def settings(test_database_url: str) -> BrokerSettings:
    # The service never connects to the broker itself; the publisher is a fake.
    return make_test_broker_settings(test_database_url, UNREACHABLE_BROKER_URL)


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def insert_due_jobs(engine: AsyncEngine, count: int) -> list[uuid.UUID]:
    """Due jobs, oldest first, each a minute apart."""
    return [
        (await insert_job(engine, available_at=PAST + timedelta(minutes=minute)))["id"]
        for minute in range(count)
    ]


async def dispatched_at(engine: AsyncEngine, job_ids: list[uuid.UUID]) -> list[datetime | None]:
    async with engine.connect() as connection:
        rows = await connection.execute(
            text("SELECT id, dispatched_at FROM jobs WHERE id = ANY(:ids)"), {"ids": job_ids}
        )
        by_id = {row.id: row.dispatched_at for row in rows}
    return [by_id[job_id] for job_id in job_ids]


# Normal path


async def test_confirmed_jobs_are_marked_dispatched(
    engine: AsyncEngine, sessions: async_sessionmaker[AsyncSession], settings: BrokerSettings
) -> None:
    job_ids = await insert_due_jobs(engine, 2)
    publisher = FakePublisher()

    assert await dispatch_due_jobs(sessions, publisher, settings) == DispatchPoll(2, 2)

    assert publisher.batches == [job_ids]
    assert None not in await dispatched_at(engine, job_ids)
    # Marked jobs are no longer due: the next poll publishes nothing.
    assert await dispatch_due_jobs(sessions, publisher, settings) == DispatchPoll(0, 0)
    assert len(publisher.batches) == 1


async def test_no_due_jobs_means_no_publish(
    engine: AsyncEngine, sessions: async_sessionmaker[AsyncSession], settings: BrokerSettings
) -> None:
    await insert_job(engine, available_at=datetime.now(UTC) + timedelta(hours=3))
    publisher = FakePublisher()

    assert await dispatch_due_jobs(sessions, publisher, settings) == DispatchPoll(0, 0)
    assert publisher.batches == []


async def test_one_poll_publishes_at_most_the_batch_size_oldest_first(
    engine: AsyncEngine, sessions: async_sessionmaker[AsyncSession], test_database_url: str
) -> None:
    oldest, middle, newest = await insert_due_jobs(engine, 3)
    settings = make_test_broker_settings(
        test_database_url, UNREACHABLE_BROKER_URL, dispatch_batch_size=2
    )
    publisher = FakePublisher()

    assert await dispatch_due_jobs(sessions, publisher, settings) == DispatchPoll(2, 2)
    assert await dispatch_due_jobs(sessions, publisher, settings) == DispatchPoll(1, 1)
    assert publisher.batches == [[oldest, middle], [newest]]


# Failure paths


@pytest.mark.parametrize("unconfirmed_count", [1, 3], ids=["one unconfirmed", "none confirmed"])
async def test_unconfirmed_jobs_stay_due_and_the_next_poll_publishes_them(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    unconfirmed_count: int,
) -> None:
    job_ids = await insert_due_jobs(engine, 3)
    unconfirmed = job_ids[-unconfirmed_count:]
    confirmed = job_ids[:-unconfirmed_count]

    first = FakePublisher(unconfirmed=frozenset(unconfirmed))
    assert await dispatch_due_jobs(sessions, first, settings) == DispatchPoll(3, len(confirmed))
    assert await dispatched_at(engine, unconfirmed) == [None] * unconfirmed_count
    assert None not in await dispatched_at(engine, confirmed)

    second = FakePublisher()
    assert await dispatch_due_jobs(sessions, second, settings) == DispatchPoll(
        unconfirmed_count, unconfirmed_count
    )
    assert second.batches == [unconfirmed]


async def test_a_database_error_after_confirmed_publishes_marks_nothing(
    engine: AsyncEngine, sessions: async_sessionmaker[AsyncSession], settings: BrokerSettings
) -> None:
    # The broker has the messages, but the database connection is lost before the mark: the jobs
    # are published again (a duplicate), never left unpublished.
    job_ids = await insert_due_jobs(engine, 2)

    async def drop_the_dispatchers_connection() -> None:
        async with engine.connect() as admin:
            # Only the poll's own backend: idle in its transaction, its last statement the due-job
            # select, so a developer's open session on the test database is left alone. The
            # timeout argument waits until that backend has exited.
            terminated = await admin.scalar(
                text(
                    "SELECT count(*) FILTER (WHERE pg_terminate_backend(pid, 5000))"
                    " FROM pg_stat_activity"
                    " WHERE datname = current_database() AND state = 'idle in transaction'"
                    " AND query LIKE 'SELECT jobs.id %FOR UPDATE SKIP LOCKED'"
                )
            )
        assert terminated == 1

    with pytest.raises(DBAPIError):
        await dispatch_due_jobs(
            sessions, FakePublisher(during=drop_the_dispatchers_connection), settings
        )

    assert await dispatched_at(engine, job_ids) == [None, None]
    again = FakePublisher()
    assert await dispatch_due_jobs(sessions, again, settings) == DispatchPoll(2, 2)
    assert again.batches == [job_ids]


async def test_a_confirmed_job_that_could_not_be_marked_is_logged_by_id(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    settings: BrokerSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The row locks make this impossible short of a bug, so the test fakes one: the publisher
    # confirms a job that was not in the batch and is no longer due.
    payload = f'{{"recipient": "{SENTINEL}@example.com"}}'
    due = (await insert_job(engine, payload=payload))["id"]
    stray = (await insert_job(engine, payload=payload, dispatched_at=PAST))["id"]
    caplog.set_level(logging.INFO)

    poll = await dispatch_due_jobs(sessions, FakePublisher(extra=[stray]), settings)

    assert poll == DispatchPoll(due=1, dispatched=1)
    assert f"confirmed jobs not marked dispatched: {stray}" in caplog.text
    assert str(due) not in caplog.text
    assert [record.name for record in caplog.records] == ["task_platform.services.dispatch"]
    # Ids only, never anything from the rows.
    assert SENTINEL not in caplog.text


async def test_a_confirmed_id_outside_the_batch_is_never_marked(
    engine: AsyncEngine,
    sessions: async_sessionmaker[AsyncSession],
    test_database_url: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A due job the poll did not publish: marking it would report a dispatch nobody observed,
    # and the job would wait for lost-message recovery (phase 8) instead of the next poll.
    published, foreign = await insert_due_jobs(engine, 2)
    settings = make_test_broker_settings(
        test_database_url, UNREACHABLE_BROKER_URL, dispatch_batch_size=1
    )

    poll = await dispatch_due_jobs(sessions, FakePublisher(extra=[foreign]), settings)

    assert poll == DispatchPoll(due=1, dispatched=1)
    assert None not in await dispatched_at(engine, [published])
    assert await dispatched_at(engine, [foreign]) == [None]
    assert f"confirmed jobs not marked dispatched: {foreign}" in caplog.text


# Concurrency


async def test_two_dispatchers_never_publish_the_same_job(
    engine: AsyncEngine, sessions: async_sessionmaker[AsyncSession], test_database_url: str
) -> None:
    oldest, middle, newest = await insert_due_jobs(engine, 3)
    settings = make_test_broker_settings(
        test_database_url, UNREACHABLE_BROKER_URL, dispatch_batch_size=2
    )
    release = asyncio.Event()
    first = FakePublisher(during=release.wait)
    second = FakePublisher()

    first_poll = asyncio.create_task(dispatch_due_jobs(sessions, first, settings))
    try:
        # The first poll holds its batch's row locks while it publishes.
        async with asyncio.timeout(2):
            await first.started.wait()
            # The second skips those rows instead of waiting for them.
            assert await dispatch_due_jobs(sessions, second, settings) == DispatchPoll(1, 1)
    finally:
        # Awaited here too, so a failed assertion above leaves no poll running past the test.
        release.set()
        first_result = await first_poll
    assert first_result == DispatchPoll(2, 2)

    assert first.batches == [[oldest, middle]]
    assert second.batches == [[newest]]
    assert None not in await dispatched_at(engine, [oldest, middle, newest])
