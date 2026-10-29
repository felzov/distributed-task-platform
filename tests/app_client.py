"""An HTTP client wired to the app in-process, for API and integration tests."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI


@asynccontextmanager
async def app_client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    # ASGITransport does not run lifespan events, so the engine is disposed here instead. It also
    # re-raises what the app re-raises, so an exception that escapes to the server fails the test.
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
    finally:
        await app.state.engine.dispose()
