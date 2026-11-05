"""The job_attempts table enforces ADR 0005 decision 5 itself, apart from the application code."""

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.rows import insert_job

pytestmark = pytest.mark.integration

# Any instant will do: the CHECKs ask only whether finished_at is set.
FINISHED = datetime(2026, 1, 1, tzinfo=UTC)


async def insert_attempt(engine: AsyncEngine, **overrides: Any) -> dict[str, Any]:
    """Insert one attempt row with raw SQL (no ORM) for a new job, and return it as a dict."""
    if "job_id" not in overrides:
        overrides["job_id"] = (await insert_job(engine))["id"]
    values: dict[str, Any] = {"attempt": 1} | overrides
    columns = ", ".join(values)
    params = ", ".join(f":{column}" for column in values)
    async with engine.begin() as connection:
        result = await connection.execute(
            text(f"INSERT INTO job_attempts ({columns}) VALUES ({params}) RETURNING *"), values
        )
        return dict(result.mappings().one())


# Normal path


async def test_a_new_attempt_is_open_and_started_by_postgres(engine: AsyncEngine) -> None:
    row = await insert_attempt(engine)

    assert row["started_at"].tzinfo is not None
    assert (row["finished_at"], row["outcome"], row["error_code"]) == (None, None, None)


@pytest.mark.parametrize(
    "closed",
    [
        pytest.param({"outcome": "succeeded"}, id="succeeded"),
        pytest.param({"outcome": "failed", "error_code": "handler_error"}, id="failed"),
        pytest.param({"outcome": "failed", "error_code": "x" * 100}, id="longest code"),
        pytest.param({"outcome": "failed", "error_code": "x"}, id="shortest code"),
    ],
)
async def test_closed_attempts_are_accepted(engine: AsyncEngine, closed: dict[str, Any]) -> None:
    row = await insert_attempt(engine, finished_at=FINISHED, **closed)
    assert row["outcome"] == closed["outcome"]


async def test_one_job_has_one_row_per_attempt_number(engine: AsyncEngine) -> None:
    first = await insert_attempt(engine)
    second = await insert_attempt(engine, job_id=first["job_id"], attempt=2)
    assert (first["attempt"], second["attempt"]) == (1, 2)


# Invalid rows


async def test_an_attempt_number_is_recorded_once_per_job(engine: AsyncEngine) -> None:
    first = await insert_attempt(engine)
    with pytest.raises(IntegrityError, match="pk_job_attempts"):
        await insert_attempt(engine, job_id=first["job_id"])


async def test_an_unknown_job_is_rejected(engine: AsyncEngine) -> None:
    with pytest.raises(IntegrityError, match="fk_job_attempts_job_id"):
        await insert_attempt(engine, job_id=uuid.uuid4())


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        pytest.param({"attempt": 0}, "ck_job_attempts_attempt_positive", id="attempt 0"),
        pytest.param(
            {"finished_at": FINISHED, "outcome": "queued"}, "ck_job_attempts_outcome", id="outcome"
        ),
        pytest.param(
            {"finished_at": FINISHED},
            "ck_job_attempts_finished_has_outcome",
            id="finished without outcome",
        ),
        pytest.param(
            {"outcome": "succeeded"},
            "ck_job_attempts_finished_has_outcome",
            id="outcome without finished",
        ),
        pytest.param(
            {"finished_at": FINISHED, "outcome": "failed"},
            "ck_job_attempts_error_code_on_failure",
            id="failed without code",
        ),
        pytest.param(
            {"finished_at": FINISHED, "outcome": "succeeded", "error_code": "handler_error"},
            "ck_job_attempts_error_code_on_failure",
            id="succeeded with code",
        ),
        pytest.param(
            {"error_code": "handler_error"},
            "ck_job_attempts_error_code_on_failure",
            id="open with code",
        ),
        pytest.param(
            {"finished_at": FINISHED, "outcome": "failed", "error_code": ""},
            "ck_job_attempts_error_code_length",
            id="empty code",
        ),
        pytest.param(
            {"finished_at": FINISHED, "outcome": "failed", "error_code": "x" * 101},
            "ck_job_attempts_error_code_length",
            id="code too long",
        ),
    ],
)
async def test_constraint_violations_are_rejected(
    engine: AsyncEngine, overrides: dict[str, Any], constraint: str
) -> None:
    with pytest.raises(IntegrityError, match=constraint):
        await insert_attempt(engine, **overrides)


@pytest.mark.parametrize("column", ["job_id", "attempt", "started_at"])
async def test_required_columns_reject_null(engine: AsyncEngine, column: str) -> None:
    # A CHECK passes when its expression is NULL, so only NOT NULL stops these.
    with pytest.raises(IntegrityError, match=f'column "{column}" .* violates not-null'):
        await insert_attempt(engine, **{column: None})
