"""`POST /jobs` against a real PostgreSQL: what a 202 promises is really in the table."""

import uuid

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform.config import Settings
from task_platform.data.models import Base
from tests.app_client import app_client
from tests.settings import make_test_settings

pytestmark = pytest.mark.integration

PAYLOAD = {"recipient": "max@example.com", "subject": "Hi", "body": "Hello"}


# Normal path


async def test_submitted_job_is_saved_as_queued(
    engine: AsyncEngine, test_database_url: str
) -> None:
    # Not the default of 5, so the row shows the value really comes from settings.
    settings = make_test_settings(test_database_url, default_max_attempts=3)
    async with app_client(settings) as client:
        response = await client.post(
            "/jobs",
            json={"type": "notification.send", "payload_version": 1, "payload": PAYLOAD},
        )

    assert response.status_code == 202
    job_id = uuid.UUID(response.json()["id"])
    assert response.json() == {"id": str(job_id), "status": "queued"}
    assert response.headers["location"] == f"/jobs/{job_id}"

    # Read through another connection: the row is visible, so it was committed before the 202.
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id})
        row = result.mappings().one()
    assert row["type"] == "notification.send"
    assert row["payload_version"] == 1
    assert row["payload"] == PAYLOAD
    assert row["status"] == "queued"
    assert row["attempts"] == 0
    assert row["max_attempts"] == 3
    assert row["dispatched_at"] is None
    assert row["available_at"] == row["created_at"]


async def test_each_submission_creates_its_own_job(
    engine: AsyncEngine, test_settings: Settings
) -> None:
    body = {"type": "notification.send", "payload_version": 1, "payload": PAYLOAD}
    async with app_client(test_settings) as client:
        first = await client.post("/jobs", json=body)
        second = await client.post("/jobs", json=body)

    assert first.json()["id"] != second.json()["id"]
    async with engine.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM jobs")) == 2


# The ORM model and the hand-written migration describe the same table


async def test_orm_model_matches_the_migrated_table(engine: AsyncEngine) -> None:
    def describe(connection: Connection) -> tuple[dict[str, bool], list[str]]:
        inspector = inspect(connection)
        columns = {column["name"]: column["nullable"] for column in inspector.get_columns("jobs")}
        return columns, inspector.get_pk_constraint("jobs")["constrained_columns"]

    async with engine.connect() as connection:
        db_columns, db_primary_key = await connection.run_sync(describe)

    table = Base.metadata.tables["jobs"]
    assert {column.name: column.nullable for column in table.columns} == db_columns
    assert [column.name for column in table.primary_key] == db_primary_key
