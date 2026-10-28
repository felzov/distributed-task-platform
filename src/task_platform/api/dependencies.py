"""FastAPI dependencies: the objects `create_app` stores on `app.state`."""

from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from task_platform.config import Settings


def _settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


def _engine(request: Request) -> AsyncEngine:
    engine: AsyncEngine = request.app.state.engine
    return engine


def _session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    session_factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    return session_factory


AppSettings = Annotated[Settings, Depends(_settings)]
# Only for connection-level checks (/ready); use cases go through SessionFactory.
AppEngine = Annotated[AsyncEngine, Depends(_engine)]
SessionFactory = Annotated[async_sessionmaker[AsyncSession], Depends(_session_factory)]
