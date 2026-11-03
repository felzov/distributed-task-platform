"""`GET /jobs/{id}` against a real PostgreSQL: the stored state, never the payload."""

import json
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.config import Settings
from task_platform.domain.failures import FAILURE_MESSAGES, UNKNOWN_FAILURE_MESSAGE, FailureCode
from task_platform.main import create_app
from tests.app_client import app_client
from tests.integration.rows import insert_job

pytestmark = pytest.mark.integration

SENTINEL = "s3cr3t-sentinel-7731"

JOB = {
    "type": "notification.send",
    "payload_version": 1,
    "payload": {"recipient": "max@example.com", "subject": "Hi", "body": SENTINEL},
}


async def test_submitted_job_is_read_back_without_its_payload(
    engine: AsyncEngine, test_settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    async with app_client(create_app(test_settings)) as client:
        job_id = (await client.post("/jobs", json=JOB)).json()["id"]
        # Only the GET is under test. DEBUG also on SQLAlchemy's logger (pinned to WARNING on
        # import), which then logs every statement and every result row.
        caplog.clear()
        caplog.set_level(logging.DEBUG)
        caplog.set_level(logging.DEBUG, logger="sqlalchemy")
        response = await client.get(f"/jobs/{job_id}")

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {
        "id",
        "type",
        "payload_version",
        "status",
        "attempts",
        "max_attempts",
        "available_at",
        "created_at",
        "updated_at",
        "result",
        "failure",
    }
    assert (body["result"], body["failure"]) == (None, None)
    assert SENTINEL not in response.text
    assert SENTINEL not in caplog.text
    # Guards the guard: SQLAlchemy's DEBUG row logging really ran, so the check above has teeth
    # (statement logging alone, at INFO, would not print the row).
    assert any(
        record.name.startswith("sqlalchemy.engine")
        and record.levelno == logging.DEBUG
        and record.getMessage().startswith("Row")
        for record in caplog.records
    )

    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
        row = result.mappings().one()
    assert body["id"] == str(row["id"])
    assert body["type"] == row["type"] == "notification.send"
    assert body["payload_version"] == row["payload_version"] == 1
    assert body["status"] == row["status"] == "queued"
    assert body["attempts"] == row["attempts"] == 0
    assert body["max_attempts"] == row["max_attempts"] == test_settings.default_max_attempts
    for column in ("available_at", "created_at", "updated_at"):
        # ISO 8601 with the offset, so a client cannot misread the time zone.
        assert datetime.fromisoformat(body[column]) == row[column]
        assert datetime.fromisoformat(body[column]).tzinfo is not None


async def test_unknown_job_returns_404_without_the_id(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    job_id = str(uuid.uuid4())
    async with app_client(create_app(test_settings)) as client:
        response = await client.get(f"/jobs/{job_id}")

    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "job_not_found", "message": "job not found", "details": []}
    }
    assert job_id not in response.text


# Rows as a worker would leave them. Each also holds the columns its status must not show, so the
# tests prove that `result` and `failure` follow the status, not the columns.
RESULT = {"provider": "fake"}
LEFTOVERS = {"result": json.dumps({"left": "over"}), "error_code": FailureCode.HANDLER_ERROR}


async def seed(engine: AsyncEngine, **columns: Any) -> str:
    """Insert a job whose payload holds the sentinel; return its id."""
    payload = json.dumps(JOB["payload"])
    return str((await insert_job(engine, payload=payload, **columns))["id"])


async def read(settings: Settings, job_id: str) -> httpx.Response:
    async with app_client(create_app(settings)) as client:
        response = await client.get(f"/jobs/{job_id}")
    assert response.status_code == 200
    assert SENTINEL not in response.text
    return response


async def test_a_succeeded_job_shows_its_result_and_no_failure(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    job_id = await seed(
        engine,
        status="succeeded",
        attempts=1,
        result=json.dumps(RESULT),
        error_code=FailureCode.HANDLER_ERROR,
    )

    body = (await read(test_settings, job_id)).json()

    assert (body["status"], body["result"], body["failure"]) == ("succeeded", RESULT, None)


@pytest.mark.parametrize(
    "stored",
    [f'["{SENTINEL}"]', f'"{SENTINEL}"', "1"],
    ids=["array", "string", "number"],
)
async def test_a_succeeded_job_with_a_non_object_result_shows_none(
    engine: AsyncEngine, test_settings: Settings, caplog: pytest.LogCaptureFixture, stored: str
) -> None:
    # Not a row this code writes (handlers return a dict), but nothing in the schema forbids it.
    job_id = await seed(engine, status="succeeded", attempts=1, result=stored)

    with caplog.at_level(logging.WARNING, logger="task_platform"):
        body = (await read(test_settings, job_id)).json()

    assert (body["status"], body["result"], body["failure"]) == ("succeeded", None, None)
    assert f"succeeded job {job_id} has a result that is not a JSON object" in caplog.text
    # The stored value never reaches the response or the log: nothing proves it is free of
    # payload data.
    assert SENTINEL not in caplog.text


async def test_a_succeeded_job_without_a_result_shows_none(
    engine: AsyncEngine, test_settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    job_id = await seed(engine, status="succeeded", attempts=1)

    with caplog.at_level(logging.WARNING, logger="task_platform"):
        body = (await read(test_settings, job_id)).json()

    assert (body["status"], body["result"], body["failure"]) == ("succeeded", None, None)
    assert f"succeeded job {job_id} has no result" in caplog.text


@pytest.mark.parametrize("code", list(FailureCode))
async def test_a_failed_job_shows_its_failure_and_no_result(
    engine: AsyncEngine, test_settings: Settings, code: FailureCode
) -> None:
    job_id = await seed(
        engine, status="failed", attempts=1, error_code=code, result=json.dumps(RESULT)
    )

    body = (await read(test_settings, job_id)).json()

    assert body["status"] == "failed"
    assert body["result"] is None
    assert body["failure"] == {"code": code, "message": FAILURE_MESSAGES[code]}


@pytest.mark.parametrize(
    "columns",
    [
        pytest.param({"status": "queued"}, id="queued"),
        pytest.param(
            {
                "status": "running",
                "attempts": 1,
                "lease_expires_at": datetime.now(UTC) + timedelta(minutes=5),
            },
            id="running",
        ),
    ],
)
async def test_an_unfinished_job_shows_neither_result_nor_failure(
    engine: AsyncEngine, test_settings: Settings, columns: dict[str, Any]
) -> None:
    job_id = await seed(engine, **columns, **LEFTOVERS)

    body = (await read(test_settings, job_id)).json()

    assert body["status"] == columns["status"]
    assert (body["result"], body["failure"]) == (None, None)


async def test_an_unknown_error_code_is_shown_with_a_generic_message(
    engine: AsyncEngine, test_settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    job_id = await seed(engine, status="failed", attempts=1, error_code="from_a_newer_worker")

    with caplog.at_level(logging.WARNING, logger="task_platform"):
        body = (await read(test_settings, job_id)).json()

    assert body["failure"] == {"code": "from_a_newer_worker", "message": UNKNOWN_FAILURE_MESSAGE}
    assert f"failed job {job_id} has an unknown error code" in caplog.text
    assert SENTINEL not in caplog.text


async def test_a_failed_job_without_an_error_code_shows_no_failure(
    engine: AsyncEngine, test_settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    # Not a row this code writes (mark_failed always sets a code), but nothing in the schema
    # forbids it.
    job_id = await seed(engine, status="failed", attempts=1)

    with caplog.at_level(logging.WARNING, logger="task_platform"):
        body = (await read(test_settings, job_id)).json()

    assert (body["status"], body["result"], body["failure"]) == ("failed", None, None)
    assert f"failed job {job_id} has no error code" in caplog.text
