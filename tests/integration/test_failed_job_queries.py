"""`select_failed_jobs`: failed jobs only, newest first, in pages that neither skip nor repeat."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from task_platform.data.jobs import JobStateRow, select_failed_jobs
from tests.integration.plans import generic_plan
from tests.integration.rows import insert_job

pytestmark = pytest.mark.integration

FAILED_AT = datetime(2026, 1, 1, tzinfo=UTC)
MAX_PAGES = 10


async def insert_failed(engine: AsyncEngine, failed_at: datetime) -> uuid.UUID:
    row = await insert_job(
        engine, status="failed", attempts=1, error_code="handler_error", updated_at=failed_at
    )
    return uuid.UUID(str(row["id"]))


async def page(
    engine: AsyncEngine, limit: int, before: tuple[datetime, uuid.UUID] | None = None
) -> list[JobStateRow]:
    async with async_sessionmaker(engine)() as session:
        return await select_failed_jobs(session, limit=limit, before=before)


async def walk(engine: AsyncEngine, limit: int) -> list[list[uuid.UUID]]:
    """Every page, each one starting after the previous page's last row."""
    pages: list[list[uuid.UUID]] = []
    before = None
    # Bounded: a cursor that is ignored would return the same page forever.
    for _ in range(MAX_PAGES):
        rows = await page(engine, limit, before)
        if not rows:
            return pages
        pages.append([row.id for row in rows])
        before = (rows[-1].updated_at, rows[-1].id)
    pytest.fail(f"still more pages after {MAX_PAGES}")


async def test_failed_jobs_come_newest_first_with_their_state(engine: AsyncEngine) -> None:
    oldest = await insert_failed(engine, FAILED_AT)
    newest = await insert_failed(engine, FAILED_AT + timedelta(minutes=2))
    middle = await insert_failed(engine, FAILED_AT + timedelta(minutes=1))

    rows = await page(engine, limit=10)

    assert [row.id for row in rows] == [newest, middle, oldest]
    first = rows[0]
    assert (first.status, first.attempts, first.error_code) == ("failed", 1, "handler_error")
    assert first.updated_at == FAILED_AT + timedelta(minutes=2)
    # The same columns as select_job: never the payload.
    assert "payload" not in first._fields


async def test_only_failed_jobs_are_listed(engine: AsyncEngine) -> None:
    failed = await insert_failed(engine, FAILED_AT)
    await insert_job(engine)
    await insert_job(engine, status="succeeded", attempts=1, result='{"provider": "fake"}')
    await insert_job(
        engine,
        status="running",
        attempts=1,
        lease_expires_at=FAILED_AT + timedelta(days=365),
        error_code="handler_error",
    )

    assert [row.id for row in await page(engine, limit=10)] == [failed]


async def test_pages_split_jobs_that_failed_at_the_same_time_by_id(engine: AsyncEngine) -> None:
    # Five jobs failed in the same microsecond: only the id orders them, and every page boundary
    # falls between two of them.
    ids = [await insert_failed(engine, FAILED_AT) for _ in range(5)]
    later = await insert_failed(engine, FAILED_AT + timedelta(seconds=1))

    pages = await walk(engine, limit=2)

    assert [len(p) for p in pages] == [2, 2, 2]
    listed = [job_id for p in pages for job_id in p]
    assert listed == [later, *sorted(ids, reverse=True)]


async def test_a_page_after_the_last_row_is_empty(engine: AsyncEngine) -> None:
    only = await insert_failed(engine, FAILED_AT)

    rows = await page(engine, limit=1)
    assert [row.id for row in rows] == [only]
    assert await page(engine, limit=1, before=(rows[0].updated_at, rows[0].id)) == []


async def test_no_failed_jobs_is_an_empty_list(engine: AsyncEngine) -> None:
    await insert_job(engine)
    assert await page(engine, limit=10) == []


@pytest.mark.parametrize("before", [None, (FAILED_AT, uuid.uuid4())], ids=["first", "next"])
async def test_the_list_keeps_its_index_under_a_generic_plan(
    engine: AsyncEngine, before: tuple[datetime, uuid.UUID] | None
) -> None:
    # The statement select_failed_jobs really sends, planned as asyncpg's prepared statement can
    # be after five runs. With the status bound as a parameter the plan sorts the whole table.
    await insert_failed(engine, FAILED_AT)

    plan = await generic_plan(
        engine, lambda session: select_failed_jobs(session, limit=50, before=before)
    )

    assert "ix_jobs_failed" in plan
