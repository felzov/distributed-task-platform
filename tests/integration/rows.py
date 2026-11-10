"""Rows written with raw SQL, bypassing the application code, to test the schema and queries."""

import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

# Passed as JSON text, cast by PostgreSQL.
JSONB_COLUMNS = frozenset({"payload", "result"})


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
        f"CAST(:{column} AS jsonb)" if column in JSONB_COLUMNS else f":{column}"
        for column in values
    )
    async with engine.begin() as connection:
        result = await connection.execute(
            text(f"INSERT INTO jobs ({columns}) VALUES ({params}) RETURNING *"), values
        )
        return dict(result.mappings().one())


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


async def fake_notification_rows(
    engine: AsyncEngine, job_id: uuid.UUID | str
) -> list[dict[str, Any]]:
    """The fake provider's rows for one job, oldest first."""
    async with engine.connect() as connection:
        result = await connection.execute(
            text(
                "SELECT id, attempt, idempotency_key FROM fake_notifications WHERE job_id = :id"
                " ORDER BY created_at, attempt"
            ),
            {"id": job_id},
        )
        return [dict(row) for row in result.mappings()]


def fake_result(notification: dict[str, Any]) -> dict[str, Any]:
    """The result `notification.send` stores for the job of this fake_notifications row."""
    return {"provider": "fake", "notification_id": str(notification["id"])}
