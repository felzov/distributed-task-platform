"""`GET /jobs/{id}/attempts` against a real PostgreSQL: each recorded attempt, never the payload."""

import json
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.config import Settings
from task_platform.domain.failures import FAILURE_MESSAGES, UNKNOWN_FAILURE_MESSAGE, FailureCode
from task_platform.main import create_app
from tests.app_client import app_client
from tests.integration.rows import insert_attempt, insert_job

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-7731"
PAYLOAD = json.dumps({"recipient": "max@example.com", "subject": "Hi", "body": SENTINEL})

STARTED = datetime(2026, 1, 1, tzinfo=UTC)
FINISHED = STARTED + timedelta(seconds=1)


async def seed_job(engine: AsyncEngine, **columns: Any) -> uuid.UUID:
    """Insert a job whose payload holds the sentinel; return its id."""
    job: dict[str, Any] = await insert_job(engine, payload=PAYLOAD, **columns)
    return uuid.UUID(str(job["id"]))


async def seed_failed(
    engine: AsyncEngine, job_id: uuid.UUID, attempt: int, code: str = FailureCode.HANDLER_ERROR
) -> None:
    await insert_attempt(
        engine,
        job_id=job_id,
        attempt=attempt,
        started_at=STARTED,
        finished_at=FINISHED,
        outcome="failed",
        error_code=code,
    )


async def get(settings: Settings, job_id: uuid.UUID) -> httpx.Response:
    async with app_client(create_app(settings)) as client:
        return await client.get(f"/jobs/{job_id}/attempts")


async def read(settings: Settings, job_id: uuid.UUID) -> list[dict[str, Any]]:
    response = await get(settings, job_id)
    assert response.status_code == 200
    assert SENTINEL not in response.text
    attempts: list[dict[str, Any]] = response.json()["attempts"]
    return attempts


def failure(code: FailureCode) -> dict[str, str]:
    return {"code": code, "message": FAILURE_MESSAGES[code]}


async def test_a_running_jobs_history_lists_each_attempt_lowest_first(
    engine: AsyncEngine, test_settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    job_id = await seed_job(
        engine, status="running", attempts=3, lease_expires_at=STARTED + timedelta(minutes=5)
    )
    # Inserted out of order: the response's order must come from the query.
    await insert_attempt(engine, job_id=job_id, attempt=3, started_at=STARTED)
    await seed_failed(engine, job_id, 2)
    await seed_failed(engine, job_id, 1, FailureCode.HANDLER_TIMEOUT)

    # DEBUG also on SQLAlchemy's logger, which then logs every result row (see test_get_job.py).
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="sqlalchemy")
    response = await get(test_settings, job_id)

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"attempts"}
    attempts = body["attempts"]
    for attempt in attempts:
        assert set(attempt) == {"attempt", "started_at", "finished_at", "outcome", "failure"}
        # ISO 8601 with the offset, as in GET /jobs/{id}.
        assert datetime.fromisoformat(attempt["started_at"]) == STARTED
    assert [
        (a["attempt"], a["finished_at"] and datetime.fromisoformat(a["finished_at"]), a["outcome"])
        for a in attempts
    ] == [(1, FINISHED, "failed"), (2, FINISHED, "failed"), (3, None, None)]
    assert [a["failure"] for a in attempts] == [
        failure(FailureCode.HANDLER_TIMEOUT),
        failure(FailureCode.HANDLER_ERROR),
        # Still running: no outcome, no failure.
        None,
    ]
    assert SENTINEL not in response.text
    assert SENTINEL not in caplog.text
    # The DEBUG row logging really ran, so the check above has teeth.
    assert any(
        record.name.startswith("sqlalchemy.engine") and record.getMessage().startswith("Row")
        for record in caplog.records
    )


async def test_a_succeeded_attempt_has_no_failure(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    job_id = await seed_job(engine, status="succeeded", attempts=2, result='{"provider": "fake"}')
    await seed_failed(engine, job_id, 1)
    await insert_attempt(
        engine,
        job_id=job_id,
        attempt=2,
        started_at=STARTED,
        finished_at=FINISHED,
        outcome="succeeded",
    )

    attempts = await read(test_settings, job_id)

    assert [(a["outcome"], a["failure"]) for a in attempts] == [
        ("failed", failure(FailureCode.HANDLER_ERROR)),
        ("succeeded", None),
    ]


@pytest.mark.parametrize("code", list(FailureCode))
async def test_each_failure_code_has_its_fixed_message(
    engine: AsyncEngine, test_settings: Settings, code: FailureCode
) -> None:
    job_id = await seed_job(engine, status="failed", attempts=1, error_code=code)
    await seed_failed(engine, job_id, 1, code)

    assert [a["failure"] for a in await read(test_settings, job_id)] == [failure(code)]


async def test_a_job_not_claimed_yet_has_no_attempts(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    job_id = await seed_job(engine)

    assert await read(test_settings, job_id) == []


async def test_a_history_may_start_after_attempt_1(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    # A job that was running when migration 0003 ran: its earlier attempts have no rows, and the
    # API does not invent them.
    job_id = await seed_job(engine, status="queued", attempts=3)
    await seed_failed(engine, job_id, 3)

    assert [a["attempt"] for a in await read(test_settings, job_id)] == [3]


async def test_an_unknown_job_returns_404_without_the_id(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    # Another job's attempts must not answer for it.
    await seed_failed(engine, await seed_job(engine, attempts=1), 1)
    job_id = uuid.uuid4()

    response = await get(test_settings, job_id)

    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "job_not_found", "message": "job not found", "details": []}
    }
    assert str(job_id) not in response.text


async def test_an_unknown_error_code_is_shown_with_a_generic_message(
    engine: AsyncEngine, test_settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    job_id = await seed_job(engine, status="queued", attempts=1)
    await seed_failed(engine, job_id, 1, "from_a_newer_worker")

    with caplog.at_level(logging.WARNING, logger="task_platform"):
        attempts = await read(test_settings, job_id)

    assert attempts[0]["failure"] == {
        "code": "from_a_newer_worker",
        "message": UNKNOWN_FAILURE_MESSAGE,
    }
    assert f"job {job_id} attempt 1 has an unknown error code" in caplog.text
