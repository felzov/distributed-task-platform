"""Application factory: `uvicorn task_platform.main:create_app --factory` (see `make run`)."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker

from task_platform.api import jobs
from task_platform.api.errors import register_error_handlers
from task_platform.config import Settings
from task_platform.data.db import create_db_engine


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings if settings is not None else Settings()
    # Creating the engine does not connect: the first request does. So the app also starts while
    # PostgreSQL is down, and tests that never touch the database need none.
    engine = create_db_engine(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        await engine.dispose()

    app = FastAPI(title="distributed-task-platform", lifespan=lifespan)
    app.state.settings = settings
    app.state.engine = engine
    # expire_on_commit=False: reading an attribute after COMMIT must not trigger hidden I/O.
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    register_error_handlers(app)
    app.include_router(jobs.router)
    return app
