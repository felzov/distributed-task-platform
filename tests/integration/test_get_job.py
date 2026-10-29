"""`GET /jobs/{id}` against a real PostgreSQL: the stored state, never the payload."""

import logging
import uuid
from datetime import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.config import Settings
from task_platform.main import create_app
from tests.app_client import app_client

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
    }
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
