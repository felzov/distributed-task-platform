"""FastAPI dependencies: the objects `create_app` stores on `app.state`."""

from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from task_platform.config import Settings


def _settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


def _session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    session_factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    return session_factory


AppSettings = Annotated[Settings, Depends(_settings)]
SessionFactory = Annotated[async_sessionmaker[AsyncSession], Depends(_session_factory)]
