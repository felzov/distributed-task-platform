"""The jobs table enforces ADR 0001's invariants itself, independent of the application code."""

import re
import traceback
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.data.db import create_db_engine
from task_platform.domain.job_status import JobStatus
from tests.integration.database import make_test_settings

pytestmark = pytest.mark.integration


async def insert_job(engine: AsyncEngine, **overrides: Any) -> dict[str, Any]:
    """Insert one row with raw SQL (no ORM) and return it as a dict."""
    values: dict[str, Any] = {
        "type": "notification.send",
        "payload_version": 1,
        "payload": '{"recipient": "max@example.com"}',
        "max_attempts": 5,
    } | overrides
    columns = ", ".join(values)
    params = ", ".join(
        "CAST(:payload AS jsonb)" if column == "payload" else f":{column}" for column in values
    )
    async with engine.begin() as connection:
        result = await connection.execute(
            text(f"INSERT INTO jobs ({columns}) VALUES ({params}) RETURNING *"), values
        )
        return dict(result.mappings().one())


# Normal path


async def test_defaults_come_from_postgres(engine: AsyncEngine) -> None:
    row = await insert_job(engine)

    assert row["id"] is not None
    assert row["status"] == "queued"
    assert row["attempts"] == 0
    assert row["dispatched_at"] is None
    # now() is the transaction start time, so all three are the same instant.
    assert row["available_at"] == row["created_at"] == row["updated_at"]
    assert row["available_at"].tzinfo is not None


@pytest.mark.parametrize("status", list(JobStatus))
async def test_every_domain_status_is_accepted(engine: AsyncEngine, status: JobStatus) -> None:
    row = await insert_job(engine, status=status.value)
    assert row["status"] == status.value


async def test_status_constraint_lists_exactly_the_domain_statuses(engine: AsyncEngine) -> None:
    # Catches drift in both directions: a status added to the domain but not the CHECK, or the
    # other way round.
    async with engine.connect() as connection:
        definition = await connection.scalar(
            text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint"
                " WHERE conname = 'ck_jobs_status'"
            )
        )
    assert definition is not None
    assert set(re.findall(r"'(\w+)'::text", definition)) == {status.value for status in JobStatus}


@pytest.mark.parametrize(
    "overrides",
    [
        {"attempts": 5, "max_attempts": 5},
        {"max_attempts": 1},
        {"max_attempts": 20},
        {"type": "x"},
        {"type": "x" * 100},
    ],
)
async def test_boundary_values_are_accepted(engine: AsyncEngine, overrides: dict[str, Any]) -> None:
    await insert_job(engine, **overrides)


# Invalid input


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"status": "bogus"}, "ck_jobs_status"),
        ({"status": "QUEUED"}, "ck_jobs_status"),
        ({"attempts": -1}, "ck_jobs_attempts_non_negative"),
        ({"max_attempts": 0}, "ck_jobs_max_attempts_range"),
        ({"max_attempts": 21}, "ck_jobs_max_attempts_range"),
        ({"attempts": 6, "max_attempts": 5}, "ck_jobs_attempts_within_max"),
        ({"payload_version": 0}, "ck_jobs_payload_version_positive"),
        ({"type": ""}, "ck_jobs_type_length"),
        ({"type": "x" * 101}, "ck_jobs_type_length"),
    ],
)
async def test_constraint_violations_are_rejected(
    engine: AsyncEngine, overrides: dict[str, Any], constraint: str
) -> None:
    with pytest.raises(IntegrityError, match=constraint):
        await insert_job(engine, **overrides)


@pytest.mark.parametrize(
    "column",
    [
        "type",
        "payload_version",
        "payload",
        "max_attempts",
        # A CHECK passes when its expression is NULL, so only NOT NULL stops these.
        "status",
        "attempts",
        "available_at",
        "created_at",
        "updated_at",
    ],
)
async def test_required_columns_reject_null(engine: AsyncEngine, column: str) -> None:
    with pytest.raises(IntegrityError, match=f'column "{column}" .* violates not-null'):
        await insert_job(engine, **{column: None})


async def test_constraint_errors_hide_parameters_but_not_postgres_detail(
    engine: AsyncEngine,
) -> None:
    sentinel = "s3cr3t-sentinel-7731"
    with pytest.raises(IntegrityError) as exc_info:
        await insert_job(engine, status="bogus", payload=f'{{"body": "{sentinel}"}}')
    error = exc_info.value

    # hide_parameters=True keeps bound values out of SQLAlchemy's own message.
    assert "[SQL parameters hidden due to hide_parameters=True]" in str(error)
    assert sentinel not in str(error)
    # But the asyncpg exception chained underneath puts PostgreSQL's DETAIL ("Failing row
    # contains ...") in its message, so a printed traceback leaks the payload. Handlers must log
    # only the error class and SQLSTATE, never a traceback of a database error (phase 2 S2).
    assert sentinel in "".join(traceback.format_exception(error))


# Failure path


async def test_statement_timeout_cancels_slow_statements(test_database_url: str) -> None:
    engine = create_db_engine(make_test_settings(test_database_url, db_statement_timeout_ms=100))
    try:
        async with engine.connect() as connection:
            assert await connection.scalar(text("SHOW statement_timeout")) == "100ms"
            with pytest.raises(DBAPIError, match="statement timeout"):
                await connection.execute(text("SELECT pg_sleep(1)"))
    finally:
        await engine.dispose()
