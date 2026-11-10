"""Waiting for PostgreSQL lock waits, so a test can order two transactions without sleeping."""

import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


async def wait_until_blocked_by(engine: AsyncEngine, pid: int) -> None:
    """Return once a backend waits for a lock held by backend `pid`; fail after 5 s."""
    async with asyncio.timeout(5), engine.connect() as connection:
        while True:
            blocked = await connection.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity"
                    " WHERE CAST(:pid AS integer) = ANY(pg_blocking_pids(pid))"
                ),
                {"pid": pid},
            )
            if blocked:
                return
            # Each SELECT in its own snapshot of pg_stat_activity.
            await connection.rollback()
            await asyncio.sleep(0.02)
