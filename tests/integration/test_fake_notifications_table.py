"""The fake provider's record of calls (ADR 0004, migration 0002)."""

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.rows import insert_job

pytestmark = pytest.mark.integration

INSERT = text(
    "INSERT INTO fake_notifications (job_id, attempt) VALUES (:job_id, :attempt) RETURNING *"
)


async def test_defaults_come_from_postgres(engine: AsyncEngine) -> None:
    job = await insert_job(engine)
    async with engine.begin() as connection:
        row = (await connection.execute(INSERT, {"job_id": job["id"], "attempt": 1})).mappings()
        notification = row.one()

    assert notification["id"] is not None
    assert notification["created_at"].tzinfo is not None


async def test_calls_without_a_key_are_all_recorded(engine: AsyncEngine) -> None:
    # Rows recorded before migration 0006 have no key: NULLs are distinct, so their duplicates
    # stay (ADR 0007 decision 8).
    job = await insert_job(engine)
    async with engine.begin() as connection:
        for _ in range(2):
            await connection.execute(INSERT, {"job_id": job["id"], "attempt": 1})
        count = await connection.scalar(
            text("SELECT count(*) FROM fake_notifications WHERE job_id = :id"), {"id": job["id"]}
        )
    assert count == 2


async def test_unknown_job_is_rejected(engine: AsyncEngine) -> None:
    with pytest.raises(IntegrityError, match="fk_fake_notifications_job_id"):
        async with engine.begin() as connection:
            await connection.execute(INSERT, {"job_id": uuid.uuid4(), "attempt": 1})


async def test_attempt_must_be_positive(engine: AsyncEngine) -> None:
    job = await insert_job(engine)
    with pytest.raises(IntegrityError, match="ck_fake_notifications_attempt_positive"):
        async with engine.begin() as connection:
            await connection.execute(INSERT, {"job_id": job["id"], "attempt": 0})


@pytest.mark.parametrize("column", ["job_id", "attempt"])
async def test_required_columns_reject_null(engine: AsyncEngine, column: str) -> None:
    job = await insert_job(engine)
    values = {"job_id": job["id"], "attempt": 1} | {column: None}
    with pytest.raises(IntegrityError, match=f'column "{column}" .* violates not-null'):
        async with engine.begin() as connection:
            await connection.execute(INSERT, values)


async def test_a_key_is_used_once(engine: AsyncEngine) -> None:
    # Whichever job or attempt: one key is one notification (migration 0006).
    first_job = await insert_job(engine)
    second_job = await insert_job(engine)
    keyed = text(
        "INSERT INTO fake_notifications (job_id, attempt, idempotency_key)"
        " VALUES (:job_id, :attempt, 'key-1')"
    )
    async with engine.begin() as connection:
        await connection.execute(keyed, {"job_id": first_job["id"], "attempt": 1})
    for job_id, attempt in [(first_job["id"], 2), (second_job["id"], 1)]:
        with pytest.raises(IntegrityError, match="uq_fake_notifications_idempotency_key"):
            async with engine.begin() as connection:
                await connection.execute(keyed, {"job_id": job_id, "attempt": attempt})
