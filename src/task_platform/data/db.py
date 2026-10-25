"""The async PostgreSQL engine (ADR 0003), with every timeout taken from settings."""

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from task_platform.config import Settings


def create_db_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(
        settings.database_url.get_secret_value(),
        pool_size=settings.db_pool_size,
        # pool_size is the hard cap on connections, not a soft target.
        max_overflow=0,
        pool_timeout=settings.db_pool_timeout_s,
        # Bound parameters hold job payloads; keep them out of exception messages and logs.
        hide_parameters=True,
        connect_args={
            # asyncpg: connect timeout in seconds.
            "timeout": settings.db_connect_timeout_s,
            # Applied by PostgreSQL to every statement on the connection.
            "server_settings": {"statement_timeout": str(settings.db_statement_timeout_ms)},
        },
    )
