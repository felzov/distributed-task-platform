"""Readiness: can this instance serve requests right now? (Liveness needs no service.)"""

import asyncio
import logging
from enum import StrEnum

from sqlalchemy import QueuePool
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from task_platform import safe_logging
from task_platform.data.schema import SCHEMA_REVISION, current_revisions

logger = logging.getLogger(__name__)


class ReadinessCheck(StrEnum):
    """A failed check; its name is safe to show (no revision ids, no error text)."""

    DATABASE = "database"
    MIGRATIONS = "migrations"


async def check_readiness(
    engine: AsyncEngine,
    *,
    timeout_s: float,
    expected_revision: str = SCHEMA_REVISION,
) -> ReadinessCheck | None:
    """Return the failed check, or None if ready. The broker is not checked (decision D1).

    Uses the app's own pool: a full pool means requests cannot be served either, so it counts
    as not ready. The timeout bounds the wait for a connection and both queries.
    """
    try:
        async with asyncio.timeout(timeout_s), engine.connect() as connection:
            revisions = await current_revisions(connection)
    # TimeoutError (the timeout above, asyncpg's timeouts) is an OSError subclass.
    except (SQLAlchemyError, OSError) as exc:
        # With the default settings the probe's timeout (2 s) fires before the pool's (5 s), so a
        # full pool and a slow database both end in builtins.TimeoutError; the pool usage helps
        # tell them apart.
        logger.warning(
            "not ready: database (%s, sqlstate %s, pool %s in use)",
            safe_logging.class_name(exc),
            safe_logging.sqlstate(exc),
            _pool_usage(engine),
        )
        return ReadinessCheck.DATABASE
    # Strict: a database ahead of this code is not ready either (decision 2026-10-27).
    if revisions != (expected_revision,):
        logger.warning("not ready: migrations (database is not at this code's revision)")
        return ReadinessCheck.MIGRATIONS
    return None


def _pool_usage(engine: AsyncEngine) -> str:
    pool = engine.pool
    # The app's engine uses AsyncAdaptedQueuePool, a QueuePool; other pools have no fixed size.
    if isinstance(pool, QueuePool):
        return f"{pool.checkedout()}/{pool.size()}"
    return "n/a"
