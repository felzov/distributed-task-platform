"""An HTTP client wired to the app in-process, for API and integration tests."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx

from task_platform.config import Settings
from task_platform.main import create_app


@asynccontextmanager
async def app_client(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    # ASGITransport does not run lifespan events, so the engine is disposed here instead. It also
    # re-raises what the app re-raises, so an exception that escapes to the server fails the test.
    app = create_app(settings)
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
    finally:
        await app.state.engine.dispose()
