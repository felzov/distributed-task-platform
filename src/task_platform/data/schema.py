"""The schema revision this code was written for, and the one the database is at."""

from typing import Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

# The newest revision in migrations/versions/. Bump it with every new migration; a unit test
# checks that it equals Alembic's head.
SCHEMA_REVISION: Final = "0001"


async def current_revisions(connection: AsyncConnection) -> tuple[str, ...]:
    """The revisions recorded in alembic_version; empty if the database was never migrated."""
    # to_regclass instead of catching "relation does not exist": a failed statement would abort
    # the transaction, and this is not an error, just a database that is not ready.
    if await connection.scalar(text("SELECT to_regclass('alembic_version')")) is None:
        return ()
    result = await connection.scalars(text("SELECT version_num FROM alembic_version"))
    return tuple(result)
