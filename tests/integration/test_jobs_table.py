"""The jobs table enforces ADR 0001's invariants itself, independent of the application code."""

import re
import traceback
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.data.db import create_db_engine
from task_platform.domain.idempotency import MAX_IDEMPOTENCY_KEY_LENGTH
from task_platform.domain.job_status import JobStatus
from tests.integration.rows import insert_job
from tests.settings import make_test_settings

pytestmark = pytest.mark.integration

# Any instant will do: the CHECK asks only that a running job has a lease.
LEASE = {"lease_expires_at": datetime(2026, 1, 1, tzinfo=UTC)}
# Any 32 bytes will do: the CHECK asks only for a SHA-256's length (migration 0005).
HASH = bytes(32)


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
    # Since migration 0002 a running job needs a lease (ck_jobs_running_has_lease).
    lease = LEASE if status is JobStatus.RUNNING else {}
    row = await insert_job(engine, status=status.value, **lease)
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
        {"error_code": "x"},
        {"error_code": "x" * 100},
        # Not tied to status: a retried job keeps its last error while queued (phase 5).
        {"status": "queued", "error_code": "handler_failed"},
        {"status": "failed", **LEASE},
        {"result": '{"sent": true}'},
        {"idempotency_key": "k", "request_hash": HASH},
        # The domain's limit, so a limit changed without a migration fails here (ADR 0007).
        {"idempotency_key": "k" * MAX_IDEMPOTENCY_KEY_LENGTH, "request_hash": HASH},
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
        ({"error_code": ""}, "ck_jobs_error_code_length"),
        ({"error_code": "x" * 101}, "ck_jobs_error_code_length"),
        ({"status": "running"}, "ck_jobs_running_has_lease"),
        ({"idempotency_key": "k"}, "ck_jobs_idempotency_key_has_hash"),
        ({"request_hash": HASH}, "ck_jobs_idempotency_key_has_hash"),
        ({"idempotency_key": "", "request_hash": HASH}, "ck_jobs_idempotency_key_length"),
        (
            {"idempotency_key": "k" * (MAX_IDEMPOTENCY_KEY_LENGTH + 1), "request_hash": HASH},
            "ck_jobs_idempotency_key_length",
        ),
        ({"idempotency_key": "k", "request_hash": bytes(31)}, "ck_jobs_request_hash_length"),
        ({"idempotency_key": "k", "request_hash": bytes(33)}, "ck_jobs_request_hash_length"),
    ],
)
async def test_constraint_violations_are_rejected(
    engine: AsyncEngine, overrides: dict[str, Any], constraint: str
) -> None:
    with pytest.raises(IntegrityError, match=constraint):
        await insert_job(engine, **overrides)


async def test_jobs_without_a_key_do_not_conflict(engine: AsyncEngine) -> None:
    # NULLs are distinct in uq_jobs_idempotency_key (ADR 0007 decision 4).
    for _ in range(3):
        await insert_job(engine)
    await insert_job(engine, idempotency_key="k", request_hash=HASH)
    async with engine.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM jobs")) == 4


async def test_a_key_is_used_once(engine: AsyncEngine) -> None:
    await insert_job(engine, idempotency_key="k", request_hash=HASH)
    with pytest.raises(IntegrityError, match="uq_jobs_idempotency_key"):
        await insert_job(engine, idempotency_key="k", request_hash=bytes(range(32)))


async def test_keys_are_compared_byte_for_byte(engine: AsyncEngine) -> None:
    # A quoted key (as the IETF draft writes one) and a differently cased key are other keys.
    for key in ("k", '"k"', "K"):
        await insert_job(engine, idempotency_key=key, request_hash=HASH)


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
