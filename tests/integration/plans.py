"""The plan PostgreSQL would use for a query's prepared statement once it switches to a generic
plan, which asyncpg's prepared statements can get after five runs."""

import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime

from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker


def _literal(value: object) -> str:
    # Only the parameter types the tested queries send; anything else fails the test loudly.
    if isinstance(value, bool):
        raise TypeError("no literal for a bool parameter")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        return f"'{value.isoformat()}'"
    if isinstance(value, uuid.UUID | str):
        return "'" + str(value).replace("'", "''") + "'"
    raise TypeError(f"no literal for a {type(value).__name__} parameter")


async def generic_plan(
    engine: AsyncEngine, run: Callable[[AsyncSession], Awaitable[object]]
) -> str:
    """EXPLAIN of the statement `run` sends, as a generic plan with table scans switched off.

    Captures the SQL and parameters the application really sends, so the plan belongs to the
    query under test, not to a hand-written copy of it.
    """
    sent: list[tuple[str, tuple[object, ...]]] = []

    def capture(
        conn: Connection, cursor: object, statement: str, parameters: object, *_: object
    ) -> None:
        assert isinstance(parameters, tuple)
        sent.append((statement, parameters))

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        async with async_sessionmaker(engine).begin() as session:
            await run(session)
            [(statement, parameters)] = sent
            await session.execute(text("SET LOCAL plan_cache_mode = force_generic_plan"))
            # Off, so the plan shows whether the index can serve the query at all; on a table this
            # small the planner would otherwise scan it whatever the indexes.
            await session.execute(text("SET LOCAL enable_seqscan = off"))
            await session.execute(text(f"PREPARE planned AS {statement}"))
            arguments = ", ".join(_literal(value) for value in parameters)
            execute = (
                f"EXPLAIN EXECUTE planned({arguments})" if arguments else "EXPLAIN EXECUTE planned"
            )
            plan = "\n".join(await session.scalars(text(execute)))
            await session.execute(text("DEALLOCATE planned"))
            return plan
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
