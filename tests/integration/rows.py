"""Rows written with raw SQL, bypassing the application code, to test the schema and queries."""

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
